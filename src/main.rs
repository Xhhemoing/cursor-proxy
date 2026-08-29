//! Cursor Fast Proxy RS — 极致性能 Rust 版 Cursor→OpenAI 网关.
//!
//! 只保留三样东西:
//! 1. 日志 (结构化 JSON, 每请求一行)
//! 2. 号池配置 (accounts.json, 轮询 + 并发控制)
//! 3. API key 简单鉴权 (config.json)
//!
//! 接口: 标准 OpenAI 兼容 /v1/chat/completions (流式 SSE + 非流式).
//! 上游: Cursor InferenceService/Stream (Connect 协议, hyper + rustls).

mod config;
mod cursor;
mod pool;
mod translate;
mod admin;
mod logbuf;
mod quota;
mod metrics;
mod upstream;
mod audit;
mod ratelimit;
pub mod error;

use std::sync::Arc;
use std::time::Instant;

use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{HeaderMap, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes;
use futures_util::stream::StreamExt;
use serde_json::{json, Value};
use subtle::ConstantTimeEq;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::config::AppConfig;
use crate::cursor::CursorClient;
use crate::pool::AccountPool;
use crate::translate::{upstream_to_openai_stream, upstream_to_openai_full, openai_error};

pub struct AppState {
    config: parking_lot::Mutex<AppConfig>,
    pool: AccountPool,
    cursor: CursorClient,
    log_buffer: std::sync::Arc<logbuf::LogBuffer>,
    upstreams: std::collections::HashMap<String, upstream::UpstreamClient>,
    key_usage: std::sync::Arc<quota::KeyUsageStore>,
    metrics: std::sync::Arc<metrics::Metrics>,
    audit: std::sync::Arc<audit::AuditLog>,
    rate_limiter: std::sync::Arc<ratelimit::RateLimiter>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 安装 rustls CryptoProvider
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    // 初始化日志
    tracing_subscriber::fmt()
        .json()
        .with_env_filter("info")
        .init();

    // 加载配置
    let config = AppConfig::load()?;
    let accounts = config::load_accounts()?;
    if accounts.is_empty() {
        tracing::warn!(event = "startup", "accounts.json empty; pool will accept admin upserts");
    }

    info!(
        event = "startup",
        port = config.port,
        accounts = accounts.len(),
        backend = %config.backend,
        "cursor-fast-proxy-rs starting"
    );

    // 初始化号池
    let pool = AccountPool::new(accounts, config.max_concurrency_per_account);

    // 初始化 Cursor 客户端
    let cursor = CursorClient::new(&config.backend, config.timeout_s)?;

    // 初始化上游客户端
    let mut upstreams = std::collections::HashMap::new();
    // 默认 Cursor 上游
    upstreams.insert(
        "cursor".to_string(),
        upstream::UpstreamClient::new_cursor(&config.backend, config.timeout_s)?,
    );
    // 可选 OpenAI 兼容上游 (从环境变量)
    if let Ok(openai_url) = std::env::var("CFP_OPENAI_URL") {
        let openai_key = std::env::var("CFP_OPENAI_KEY").unwrap_or_default();
        upstreams.insert(
            "openai".to_string(),
            upstream::UpstreamClient::new_openai(&openai_url, &openai_key, config.timeout_s)?,
        );
        info!(event = "upstream", kind = "openai", url = %openai_url, "openai upstream configured");
    }

    let state = Arc::new(AppState {
        config: parking_lot::Mutex::new(config.clone()),
        pool,
        cursor,
        log_buffer: std::sync::Arc::new(logbuf::LogBuffer::with_persist(
            1000,
            std::path::PathBuf::from(&config.log_file),
        )),
        upstreams,
        key_usage: std::sync::Arc::new(quota::KeyUsageStore::new()),
        metrics: std::sync::Arc::new(metrics::Metrics::new()),
        audit: std::sync::Arc::new(
            audit::AuditLog::new().unwrap_or_else(|e| {
                warn!(event = "audit_init", error = %e, "audit log unavailable, continuing without");
                // 无法打开审计文件时, 用一个指向 /dev/null 的句柄继续
                audit::AuditLog::new_at(std::path::PathBuf::from("/dev/null"))
                    .expect("/dev/null must open")
            }),
        ),
        rate_limiter: std::sync::Arc::new(ratelimit::RateLimiter::new()),
    });

    // 恢复历史用量
    state.key_usage.load_from_disk();

    // 定时落盘用量 (每 30s)
    {
        let usage = state.key_usage.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                tick.tick().await;
                usage.save_to_disk();
            }
        });
    }

    // 配置热重载: 监听 config.json 变更
    {
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = watch_config(state).await {
                warn!(event = "config_watch", error = %e, "config watcher exited");
            }
        });
    }

    // 构建路由
    let admin_routes = Router::new()
        .route("/admin", get(admin::admin_page))
        .route("/admin/api/pool", get(admin::api_pool_stats))
        .route("/admin/api/accounts", get(admin::api_accounts_list).post(admin::api_account_upsert))
        .route("/admin/api/accounts/:id", axum::routing::delete(admin::api_account_delete))
        .route("/admin/api/accounts/:id/toggle", post(admin::api_account_toggle))
        .route("/admin/api/accounts/:id/enabled", post(admin::api_account_set_enabled))
        .route("/admin/api/accounts/:id/cooldown/clear", post(admin::api_account_clear_cooldown))
        .route("/admin/api/accounts/:id/probe", post(admin::api_account_probe))
        .route("/admin/api/accounts/probe-all", post(admin::api_account_probe_all))
        .route("/admin/api/accounts/import", post(admin::api_accounts_import))
        .route("/admin/api/accounts/export", get(admin::api_accounts_export))
        .route("/admin/api/keys", get(admin::api_keys_list).post(admin::api_keys_add))
        .route("/admin/api/keys/:index", axum::routing::delete(admin::api_keys_delete).post(admin::api_keys_patch))
        .route("/admin/api/logs", get(admin::api_logs_recent))
        .route("/admin/api/settings", get(admin::api_settings_get).post(admin::api_settings_patch))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            admin_auth_mw,
        ));

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/v1/models", get(models_handler))
        .route("/v1/chat/completions", post(chat_handler))
        .merge(admin_routes)
        .with_state(state.clone());

    // 启动服务器 (注入 ConnectInfo 供限频取真实 IP)
    let listener = tokio::net::TcpListener::bind(format!("{}:{}", config.host, config.port)).await?;
    info!(event = "listening", addr = %listener.local_addr()?, "server ready");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal(state.clone()))
    .await?;

    // 优雅关闭: 落盘用量
    state.key_usage.save_to_disk();
    info!(event = "shutdown", "usage persisted, bye");
    Ok(())
}

async fn shutdown_signal(state: Arc<AppState>) {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    info!(event = "shutdown", "signal received, draining");
    drop(state);
}

/// 监听 config.json 变更并热替换内存配置 (api_keys/default_model 等即时生效)
async fn watch_config(state: Arc<AppState>) -> notify::Result<()> {
    use notify::{RecursiveMode, Watcher};
    let path = config::config_path();
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let mut watcher = notify::recommended_watcher(move |res| {
        let _ = tx.blocking_send(res);
    })?;
    // 监听父目录 (文件可能不存在, 且原子写是 rename)
    let dir = path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    watcher.watch(&dir, RecursiveMode::NonRecursive)?;
    let fname = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "config.json".into());
    info!(event = "config_watch", file = %fname, "watching config.json");
    while let Some(res) = rx.recv().await {
        match res {
            Ok(event) => {
                let touched = event.paths.iter().any(|p| {
                    p.file_name()
                        .map(|n| n.to_string_lossy() == fname)
                        .unwrap_or(false)
                });
                if !touched {
                    continue;
                }
                match AppConfig::load() {
                    Ok(new_cfg) => {
                        *state.config.lock() = new_cfg;
                        info!(event = "config_reload", "config.json reloaded");
                    }
                    Err(e) => {
                        warn!(event = "config_reload", error = %e, "reload failed, keep old");
                    }
                }
            }
            Err(e) => warn!(event = "config_watch", error = %e, "watch error"),
        }
    }
    Ok(())
}

/// API key 鉴权; 成功时返回命中的 key 字符串 (可能为空, 表示未启用鉴权).
/// 恒定时间比较防时序攻击; 过期 key 拒绝.
fn check_auth(headers: &HeaderMap, config: &AppConfig) -> Result<String, Response> {
    if config.api_keys.is_empty() {
        return Ok(String::new());
    }
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let token = auth.strip_prefix("Bearer ").unwrap_or("").trim();
    let token_bytes = token.as_bytes();
    for rec in &config.api_keys {
        let key_bytes = rec.key.as_bytes();
        // 长度不同直接跳过 (长度本身不是秘密)
        if key_bytes.len() != token_bytes.len() {
            continue;
        }
        if key_bytes.ct_eq(token_bytes).into() {
            if !rec.enabled {
                return Err((
                    StatusCode::UNAUTHORIZED,
                    Json(openai_error("API key disabled", "invalid_api_key", 401)),
                )
                    .into_response());
            }
            if rec.is_expired() {
                return Err((
                    StatusCode::UNAUTHORIZED,
                    Json(openai_error("API key expired", "invalid_api_key", 401)),
                )
                    .into_response());
            }
            return Ok(rec.key.clone());
        }
    }
    Err((
        StatusCode::UNAUTHORIZED,
        Json(openai_error("Invalid API key", "invalid_api_key", 401)),
    )
        .into_response())
}

/// GET /health
async fn health_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "pool": state.pool.stats(),
    }))
}

async fn metrics_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.render(&state.pool, &state.key_usage),
    )
}

/// Admin 鉴权 + IP 限频 (每 IP 60 req/min)
async fn admin_auth_mw(
    State(state): State<Arc<AppState>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    // IP 限频 (在鉴权之前, 防暴力破解)
    let ip = req
        .extensions()
        .get::<ConnectInfo<std::net::SocketAddr>>()
        .map(|ci| ci.0.ip())
        .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
    if !state.rate_limiter.allow(ip) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error": "rate limit exceeded, try later"})),
        )
            .into_response();
    }

    let config = state.config.lock().clone();
    let expected = if !config.admin_token.is_empty() {
        Some(config.admin_token.clone())
    } else if !config.api_keys.is_empty() {
        Some(config.api_keys[0].key.clone())
    } else {
        None
    };
    let Some(expected) = expected else {
        return next.run(req).await;
    };
    let headers = req.headers();
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .strip_prefix("Bearer ")
        .unwrap_or("")
        .trim()
        .to_string();
    let query = req.uri().query().unwrap_or("");
    let qtok = query
        .split('&')
        .find_map(|p| p.strip_prefix("token="))
        .unwrap_or("")
        .to_string();
    let exp = expected.as_bytes();
    let bearer_ok = bearer.len() == expected.len() && bool::from(bearer.as_bytes().ct_eq(exp));
    let qtok_ok = qtok.len() == expected.len() && bool::from(qtok.as_bytes().ct_eq(exp));
    if bearer_ok || qtok_ok {
        return next.run(req).await;
    }
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({"error": "admin auth required", "hint": "Bearer or ?token="})),
    )
        .into_response()
}

/// GET /v1/models
async fn models_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, Response> {
    let config = state.config.lock().clone();
    let _used_key = check_auth(&headers, &config)?;
    Ok(Json(json!({
        "object": "list",
        "data": [{
            "id": config.default_model,
            "object": "model",
            "created": chrono::Utc::now().timestamp(),
            "owned_by": "cursor-fast-proxy-rs",
        }],
    })))
}

/// POST /v1/chat/completions
async fn chat_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::ConnectInfo(addr): axum::extract::ConnectInfo<std::net::SocketAddr>,
    Json(body): Json<Value>,
) -> Result<Response, Response> {
    let client_ip = addr.ip().to_string();
    let config = state.config.lock().clone();
    let used_key = check_auth(&headers, &config)?;
    if !used_key.is_empty() {
        if let Some(rec) = config.api_keys.iter().find(|k| k.key == used_key) {
            let (tok, reqs) = state.key_usage.snapshot(&used_key);
            if let Err(msg) = quota::check_key_limits(rec, tok, reqs) {
                return Err((
                    StatusCode::PAYMENT_REQUIRED,
                    Json(openai_error(&msg, "insufficient_quota", 402)),
                )
                    .into_response());
            }
        }
    }

    let request_id = format!("req-{}", uuid::Uuid::new_v4().simple());
    let start = Instant::now();

    // 解析请求
    let messages = body
        .get("messages")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(openai_error("messages is required", "missing_messages", 400)),
            )
                .into_response()
        })?;
    let model = body
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or(&config.default_model)
        .to_string();
    let stream = body.get("stream").and_then(|v| v.as_bool()).unwrap_or(false);
    let max_tokens = body.get("max_tokens").and_then(|v| v.as_u64()).map(|v| v as u32);
    let temperature = body.get("temperature").and_then(|v| v.as_f64());

    // 选号
    let (mut account, _permit) = match state.pool.acquire().await {
        Ok(v) => v,
        Err(pool::AcquireError::Empty) => {
            state.metrics.observe_err();
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(openai_error("no eligible accounts in pool", "pool_empty", 503)),
            )
                .into_response());
        }
        Err(pool::AcquireError::Busy) => {
            state.metrics.observe_err();
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(openai_error("account pool busy", "pool_busy", 503)),
            )
                .into_response());
        }
    };
    let mut account_id = account.id.clone();

    // 构造 Cursor 请求
    let cursor_body = cursor::build_cursor_body(messages, &model, max_tokens, temperature);

    // 多上游路由：根据模型前缀选择上游
    let upstream_name = if model.starts_with("gpt-") || model.starts_with("o1-") || model.starts_with("o3-") {
        "openai"
    } else {
        "cursor"
    };

    let upstream = match state.upstreams.get(upstream_name) {
        Some(u) => u,
        None => {
            state.pool.release(&account_id, true, 30);
            state.metrics.observe_err();
            error!(
                event = "upstream_not_found",
                req_id = %request_id,
                upstream = %upstream_name,
                "upstream not configured"
            );
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(openai_error(&format!("upstream '{}' not configured", upstream_name), "upstream_error", 503)),
            )
                .into_response());
        }
    };

    // 请求失败自动重试：最多 3 次，每次切换账号
    const MAX_RETRIES: usize = 3;
    let mut last_error = String::new();
    let mut frames_opt = None;

    for attempt in 0..MAX_RETRIES {
        if attempt > 0 {
            // 重新获取账号（排除当前失败的账号）
            let retry_account = match state.pool.acquire().await {
                Ok((acc, permit)) => {
                    drop(permit);
                    acc
                }
                Err(_) => break, // 无可用账号，退出重试
            };
            // 更新账号信息用于重试
            account.access_token = retry_account.access_token;
            account.machine_id = retry_account.machine_id;
            account_id = retry_account.id.clone();
            info!(
                event = "retry",
                req_id = %request_id,
                attempt = attempt + 1,
                new_account = %account_id,
                "retrying with different account"
            );
        }

        // 调用上游（统一通过 UpstreamClient::stream，自动处理 Cursor/OpenAI 差异）
        let cursor_auth = match upstream {
            crate::upstream::UpstreamClient::Cursor(_) => Some((account.access_token.as_str(), account.machine_id.as_str())),
            crate::upstream::UpstreamClient::OpenAi(_) => None,
        };

        match upstream
            .stream(&model, messages, max_tokens, temperature, cursor_auth)
            .await
        {
            Ok(f) => {
                frames_opt = Some(f);
                break;
            }
            Err(e) => {
                last_error = e.to_string();
                state.pool.release(&account_id, true, 30);
                state.metrics.observe_err();
                error!(
                    event = "upstream_error",
                    req_id = %request_id,
                    upstream = %upstream_name,
                    account = %account_id,
                    attempt = attempt + 1,
                    error = %e,
                    "upstream stream failed"
                );
                if attempt == MAX_RETRIES - 1 {
                    return Err((
                        StatusCode::BAD_GATEWAY,
                        Json(openai_error(&format!("upstream error after {} retries: {}", MAX_RETRIES, last_error), "upstream_error", 502)),
                    )
                        .into_response());
                }
            }
        }
    }

    let frames = match frames_opt {
        Some(f) => f,
        None => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(openai_error("no available account for retry", "pool_exhausted", 503)),
            )
                .into_response());
        }
    };

    if stream {
        // 流式 SSE
        let (tx, mut rx) = mpsc::channel::<Result<Bytes, std::convert::Infallible>>(100);
        let pool = state.pool.clone();
        let aid = account_id.clone();
        let rid = request_id.clone();
        let model_clone = model.clone();
        let log_buf = state.log_buffer.clone();
        let key_usage = state.key_usage.clone();
        let billed_key = used_key.clone();
        let metrics = state.metrics.clone();

        tokio::spawn(async move {
            let mut input_tokens = 0u64;
            let mut output_tokens = 0u64;
            let mut stream = Box::pin(upstream_to_openai_stream(frames, &model_clone));
            while let Some(item) = stream.next().await {
                match item {
                    Ok((sse, usage)) => {
                        if let Some(u) = usage {
                            input_tokens = u.0;
                            output_tokens = u.1;
                        }
                        if tx.send(Ok(Bytes::from(sse))).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        error!(event = "stream_error", req_id = %rid, error = %e, "stream translate error");
                        break;
                    }
                }
            }
            drop(tx);
            let latency = start.elapsed().as_millis() as u64;
            let log_entry = serde_json::json!({
                "ts": chrono::Utc::now().to_rfc3339(),
                "req_id": rid,
                "model": model_clone,
                "account": aid,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "total_tokens": input_tokens + output_tokens,
                "latency_ms": latency,
                "status": 200,
                "stream": true,
                "client_ip": client_ip,
            });
            info!(event = "request", %log_entry, "request completed");
            log_buf.push(log_entry);
            if !billed_key.is_empty() {
                key_usage.add(&billed_key, input_tokens + output_tokens);
            }
            metrics.observe_ok(input_tokens + output_tokens);
            pool.record_success(&aid);
            // 成功时检查是否需要重新启用（连续错误已重置）
            pool.release(&aid, false, 0);
        });

        let body = Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx));
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .header("cache-control", "no-cache")
            .header("connection", "keep-alive")
            .header("x-request-id", &request_id)
            .body(body)
            .unwrap())
    } else {
        // 非流式
        match upstream_to_openai_full(frames, &model).await {
            Ok((result, usage)) => {
                let latency = start.elapsed().as_millis() as u64;
                let log_entry = serde_json::json!({
                    "ts": chrono::Utc::now().to_rfc3339(),
                    "req_id": request_id,
                    "model": model,
                    "account": account_id,
                    "input_tokens": usage.0,
                    "output_tokens": usage.1,
                    "total_tokens": usage.0 + usage.1,
                    "latency_ms": latency,
                    "status": 200,
                    "stream": false,
                    "client_ip": client_ip,
                });
                info!(event = "request", %log_entry, "request completed");
                state.log_buffer.push(log_entry);
                if !used_key.is_empty() {
                    state.key_usage.add(&used_key, usage.0 + usage.1);
                }
                state.metrics.observe_ok(usage.0 + usage.1);
                state.pool.record_success(&account_id);
                state.pool.release(&account_id, false, 0);
                Ok(Json(result).into_response())
            }
            Err(e) => {
                state.pool.release(&account_id, true, 10);
                state.metrics.observe_err();
                error!(
                    event = "translate_error",
                    req_id = %request_id,
                    account = %account_id,
                    error = %e,
                    "translate failed"
                );
                Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(openai_error(&e.to_string(), "internal_error", 500)),
                )
                    .into_response())
            }
        }
    }
}
