//! Cursor Connect 协议异步客户端: 帧编解码 + TLS + 流式读取.

use bytes::{Bytes, BytesMut};
use futures_util::stream::Stream;
use hyper::body::{Body as HttpBody, Incoming};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde_json::{json, Value};
use std::pin::Pin;
use std::task::{Context, Poll};
use uuid::Uuid;

type HttpsClient =
    Client<hyper_rustls::HttpsConnector<HttpConnector>, http_body_util::Full<hyper::body::Bytes>>;

pub const STREAM_PATH: &str = "/aiserver.v1.InferenceService/Stream";
pub const CLIENT_TYPE: &str = "sand";
pub const CLIENT_VERSION: &str = "0.18.0";
pub const KIMI_K3_CONTEXT_WINDOW: u32 = 1_048_576;
/// 客户端未传 max_tokens 时, 所有模型给长输出预算, 避免上游默认 8k 截断.
/// 128k 而非 32k: kimi-k3 官方支持长输出, 32k 对长代码/长文档场景仍频繁截断.
pub const DEFAULT_MAX_TOKENS: u32 = 131_072;
/// maxTokens 下限保护: 客户端传 <1024 时自动提升到 1024, 防止上游报错.
pub const MAX_TOKENS_FLOOR: u32 = 1024;
/// 客户端未显式传 max_mode 时的默认值。免费/未付费 Cursor 账号请求 maxMode=true 会被上游
/// 直接以 "Max mode is only available to paid users" 拒绝 (HTTP 200 + error 帧), 所以默认 false。
pub const DEFAULT_MAX_MODE: bool = false;
/// 上游连接池: 直连每 host 最大空闲连接数 (高并发吞吐关键)
pub const POOL_MAX_IDLE_DIRECT: usize = 1024;
/// 上游连接池: 代理每 host 最大空闲连接数 (代理通常带宽有限, 不盲目扩)
pub const POOL_MAX_IDLE_PROXY: usize = 128;

pub fn is_kimi_family(model: &str) -> bool {
    let m = model.to_ascii_lowercase();
    m == "kimi-k3" || m.starts_with("kimi-k3-")
}

/// 思考程度档位
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThinkingLevel {
    Low,
    High,
    Max,
}

/// 从模型名解析思考程度档位
/// kimi-k3-max → Max, kimi-k3-high → High, kimi-k3-low → Low, kimi-k3 → Max (默认)
pub fn thinking_level_from_model(model: &str) -> ThinkingLevel {
    let m = model.to_ascii_lowercase();
    if m == "kimi-k3-max" || m == "kimi-k3" {
        ThinkingLevel::Max
    } else if m == "kimi-k3-high" {
        ThinkingLevel::High
    } else if m == "kimi-k3-low" {
        ThinkingLevel::Low
    } else {
        // 默认 Max
        ThinkingLevel::Max
    }
}

/// 根据思考程度自动映射到具体模型名
/// 用于客户端传入 kimi-k3 时，根据请求特征自动选择 max/high/low
pub fn model_for_thinking_level(level: ThinkingLevel) -> &'static str {
    match level {
        ThinkingLevel::Max => "kimi-k3-max",
        ThinkingLevel::High => "kimi-k3-high",
        ThinkingLevel::Low => "kimi-k3-low",
    }
}

/// 从请求体自动识别思考程度
/// 规则：
/// 1. 显式指定 max_mode=true → Max
/// 2. 显式指定 thinking_level 字段 → 对应档位
/// 3. 根据 messages 复杂度自动推断：
///    - 有 tools 或 tool_choice → Max (需要强推理)
///    - 消息数 > 10 或总长度 > 100k 字符 → Max
///    - 消息数 > 5 或总长度 > 50k 字符 → High
///    - 其他 → Low
pub fn auto_detect_thinking_level(body: &Value) -> ThinkingLevel {
    // 1. 显式 max_mode
    if let Some(mm) = max_mode_from_request(body) {
        if mm {
            return ThinkingLevel::Max;
        }
    }
    // 2. 显式 thinking_level 字段
    if let Some(tl) = body.get("thinking_level").and_then(|v| v.as_str()) {
        match tl.to_ascii_lowercase().as_str() {
            "max" => return ThinkingLevel::Max,
            "high" => return ThinkingLevel::High,
            "low" => return ThinkingLevel::Low,
            _ => {}
        }
    }
    // 3. 根据 messages 复杂度推断
    let messages = body.get("messages").and_then(|v| v.as_array());
    let tools = body.get("tools");
    let tool_choice = body.get("tool_choice");
    
    if let Some(msgs) = messages {
        let msg_count = msgs.len();
        let total_len: usize = msgs.iter()
            .filter_map(|m| m.get("content").and_then(|c| c.as_str()))
            .map(|s| s.len())
            .sum();
        
        // 有工具调用 → Max
        if tools.is_some() || tool_choice.is_some() {
            return ThinkingLevel::Max;
        }
        // 长上下文 → Max
        if msg_count > 10 || total_len > 100_000 {
            return ThinkingLevel::Max;
        }
        // 中等上下文 → High
        if msg_count > 5 || total_len > 50_000 {
            return ThinkingLevel::High;
        }
    }
    ThinkingLevel::Low
}

/// 根据请求体自动选择 kimi-k3 变体模型
pub fn auto_select_kimi_model(body: &Value) -> &'static str {
    model_for_thinking_level(auto_detect_thinking_level(body))
}

/// 客户端省略 max_tokens 时的默认输出预算; 所有模型统一 32k, 避免上游默认 8k 截断.
pub fn default_max_tokens_for(_model: &str) -> Option<u32> {
    Some(DEFAULT_MAX_TOKENS)
}

/// 计算最终生效的 maxTokens: 客户端值优先, 无则默认 32k, 低于 floor 则提升到 floor.
pub fn effective_max_tokens(client_value: Option<u32>, _model: &str) -> u32 {
    let v = client_value.unwrap_or(DEFAULT_MAX_TOKENS);
    if v < MAX_TOKENS_FLOOR { MAX_TOKENS_FLOOR } else { v }
}

pub fn context_window_for(_model: &str) -> u32 {
    // 仅支持 kimi-k3 家族, 统一 1M 上下文
    KIMI_K3_CONTEXT_WINDOW
}
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
/// 解码缓冲上限: 允许一帧 + 余量.
const MAX_STREAM_BUF: usize = MAX_FRAME_BYTES + 64 * 1024;

#[derive(thiserror::Error, Debug)]
pub enum CursorError {
    #[error("upstream HTTP {0}: {1}")]
    Http(u16, String),
    #[error("upstream network: {0}")]
    Network(String),
    #[error("upstream decode: {0}")]
    Decode(String),
    #[error("upstream rejected: {0}")]
    Rejected(String),
}

/// Connect 帧: [flags:1][len:4 big-endian][payload]
pub fn connect_envelope(payload: &[u8], flags: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(flags);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// x-cursor-checksum: obfuscated timestamp + machine_id
pub fn checksum(machine_id: &str) -> String {
    use base64::Engine;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let kilo = now_ms / 1_000_000;
    let mut raw = [
        ((kilo >> 40) & 255) as u8,
        ((kilo >> 32) & 255) as u8,
        ((kilo >> 24) & 255) as u8,
        ((kilo >> 16) & 255) as u8,
        ((kilo >> 8) & 255) as u8,
        (kilo & 255) as u8,
    ];
    let mut last = 165u8;
    for (i, cur) in raw.iter_mut().enumerate() {
        let val = ((*cur ^ last) as usize + (i % 256)) as u8;
        *cur = val;
        last = val;
    }
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw) + machine_id
}

/// Cursor 请求头
pub fn cursor_headers(access_token: &str, machine_id: &str) -> Vec<(&'static str, String)> {
    vec![
        ("authorization", format!("Bearer {}", access_token)),
        ("x-cursor-checksum", checksum(machine_id)),
        ("x-cursor-client-type", CLIENT_TYPE.into()),
        ("x-cursor-client-version", CLIENT_VERSION.into()),
        ("x-sand-box-namespace", "prod".into()),
        ("x-ghost-mode", "true".into()),
        ("x-request-id", Uuid::new_v4().to_string()),
        ("user-agent", "cursor-fast-proxy-rs/0.1".into()),
    ]
}

/// 构造 Cursor Stream 请求体
pub fn build_cursor_body(
    messages: &[Value],
    model: &str,
    max_tokens: Option<u32>,
    temperature: Option<f64>,
) -> Value {
    build_cursor_body_with_tools(
        messages,
        model,
        max_tokens,
        temperature,
        None,
        None,
        None,
        None,
        None,
    )
}

/// 稳定 conversationId: 同一 session 在同一号上复用, Cursor 前缀缓存才能命中.
pub fn conversation_id_for(session_id: Option<&str>, account_id: Option<&str>) -> String {
    match (session_id, account_id) {
        (Some(sid), Some(aid)) if !sid.is_empty() => {
            Uuid::new_v5(&Uuid::NAMESPACE_URL, format!("cfp:{aid}:{sid}").as_bytes()).to_string()
        }
        (Some(sid), None) if !sid.is_empty() => {
            Uuid::new_v5(&Uuid::NAMESPACE_URL, format!("cfp:{sid}").as_bytes()).to_string()
        }
        _ => Uuid::new_v4().to_string(),
    }
}

/// 从请求体提取 maxMode 标志 (支持三种客户端传入方式)
pub fn max_mode_from_request(body: &Value) -> Option<bool> {
    // 1. snake_case: max_mode
    if let Some(v) = body.get("max_mode").and_then(|v| v.as_bool()) {
        return Some(v);
    }
    // 2. camelCase: maxMode (OpenAI 风格客户端实际发送的)
    if let Some(v) = body.get("maxMode").and_then(|v| v.as_bool()) {
        return Some(v);
    }
    // 3. Cursor 原生格式: requestedModel.maxMode
    if let Some(v) = body
        .get("requestedModel")
        .and_then(|r| r.get("maxMode"))
        .and_then(|v| v.as_bool())
    {
        return Some(v);
    }
    None
}

pub fn build_cursor_body_with_tools(
    messages: &[Value],
    model: &str,
    max_tokens: Option<u32>,
    temperature: Option<f64>,
    tools: Option<&Value>,
    tool_choice: Option<&Value>,
    parallel_tool_calls: Option<bool>,
    conversation_id: Option<&str>,
    max_mode: Option<bool>,
) -> Value {
    let mut cursor_msgs = crate::protocol::openai_messages_to_cursor(messages);
    // P2: tool_choice 完整支持 — "none" 时剥掉 tools, 其他情况注入 hint
    let omit_tools = crate::protocol::tool_choice_omits_tools(tool_choice);
    if let Some(hint) = crate::protocol::tool_choice_hint(tool_choice) {
        cursor_msgs.insert(
            0,
            json!({
                "role": "user",
                "system": true,
                "parts": {"parts": [{"text": {"text": hint}}]},
            }),
        );
    }

    let mut requested_model = json!({"modelId": model});
    // maxMode 默认关闭。2026-09-02 SM95 实测: 号池内全部账号 (含 kimi-k3-max / kimi-k3-low)
    // 都被上游以 "Max mode is only available to paid users" (resource_exhausted) 拒绝,
    // 默认 true 只会让每个请求多一次 max_mode_fallback 往返 (+1~3s) 且首帧空内容。
    // 需要 1M 上下文时由客户端显式传 max_mode / maxMode = true (付费号才有效)。
    let mm = max_mode.unwrap_or(DEFAULT_MAX_MODE);
    requested_model["maxMode"] = json!(mm);

    let mut body = json!({
        "requestedModel": requested_model,
        "conversationId": conversation_id
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string()),
        "messages": cursor_msgs,
        "stream": true,
    });
    // maxTokens: 客户端值优先, 无则默认 32k, 低于 floor 则提升到 floor
    let mt = effective_max_tokens(max_tokens, model);
    body["maxTokens"] = json!(mt);
    if let Some(t) = temperature {
        body["temperature"] = json!(t);
    }
    // P2: tool_choice="none" 时跳过 tools 字段
    if !omit_tools {
        let cursor_tools = crate::protocol::cursor_tools_from_client(tools);
        if !cursor_tools.is_empty() {
            body["tools"] = json!(cursor_tools);
            let mut cfg = json!({});
            if let Some(p) = parallel_tool_calls {
                cfg["parallelToolCalls"] = json!(p);
            }
            if !cfg.as_object().map(|o| o.is_empty()).unwrap_or(true) {
                body["modelConfig"] = cfg;
            }
        }
    }
    body
}

/// P2: 从完整客户端 body 构造 Cursor 请求 (含 response_format 提示注入).
/// 与 Python `build_cursor_request` 等价.
///
/// 注意: `body` 只用于读取 response_format/include 等客户端字段, 不会原样透传到上游.
pub fn build_cursor_body_full(
    body: &Value,
    messages: &[Value],
    model: &str,
    max_tokens: Option<u32>,
    temperature: Option<f64>,
    tools: Option<&Value>,
    tool_choice: Option<&Value>,
    parallel_tool_calls: Option<bool>,
    conversation_id: Option<&str>,
    max_mode: Option<bool>,
) -> Value {
    let mut cursor_msgs = crate::protocol::openai_messages_to_cursor(messages);
    // P2: tool_choice hint 注入到最前
    let omit_tools = crate::protocol::tool_choice_omits_tools(tool_choice);
    if let Some(hint) = crate::protocol::tool_choice_hint(tool_choice) {
        cursor_msgs.insert(
            0,
            json!({
                "role": "user",
                "system": true,
                "parts": {"parts": [{"text": {"text": hint}}]},
            }),
        );
    }
    // P2: response_format JSON schema 注入为 system 提示
    if let Some(rf_hint) = crate::protocol::response_format_hint(body) {
        cursor_msgs.insert(
            0,
            json!({
                "role": "user",
                "system": true,
                "parts": {"parts": [{"text": {"text": rf_hint}}]},
            }),
        );
    }

    let mut requested_model = json!({"modelId": model});
    // maxMode 默认关闭, 理由见 build_cursor_body_with_tools
    let mm = max_mode.unwrap_or(DEFAULT_MAX_MODE);
    requested_model["maxMode"] = json!(mm);

    let mut out = json!({
        "requestedModel": requested_model,
        "conversationId": conversation_id
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string()),
        "messages": cursor_msgs,
        "stream": true,
    });
    let mt = effective_max_tokens(max_tokens, model);
    out["maxTokens"] = json!(mt);
    if let Some(t) = temperature {
        out["temperature"] = json!(t);
    }
    if !omit_tools {
        let cursor_tools = crate::protocol::cursor_tools_from_client(tools);
        if !cursor_tools.is_empty() {
            out["tools"] = json!(cursor_tools);
            let mut cfg = json!({});
            if let Some(p) = parallel_tool_calls {
                cfg["parallelToolCalls"] = json!(p);
            }
            if !cfg.as_object().map(|o| o.is_empty()).unwrap_or(true) {
                out["modelConfig"] = cfg;
            }
        }
    }
    out
}

/// Cursor 异步客户端 (直连 / HTTP CONNECT / SOCKS5)
#[derive(Clone)]
pub struct CursorClient {
    client: HttpsClient,
    proxied: Option<ProxiedClient>,
    backend: String,
    timeout_s: u64,
    pub proxy_id: Option<String>,
}

/// 经 HTTP 代理 CONNECT 的客户端, 连接器类型与直连不同, 所以分开存.
type ProxyBody = http_body_util::Full<hyper::body::Bytes>;

#[derive(Clone)]
enum ProxiedClient {
    Http(
        Client<
            hyper_rustls::HttpsConnector<
                hyper_util::client::legacy::connect::proxy::Tunnel<HttpConnector>,
            >,
            ProxyBody,
        >,
    ),
    Socks5(
        Client<
            hyper_rustls::HttpsConnector<
                hyper_util::client::legacy::connect::proxy::SocksV5<HttpConnector>,
            >,
            ProxyBody,
        >,
    ),
}

impl ProxiedClient {
    async fn request(
        &self,
        req: hyper::Request<ProxyBody>,
    ) -> Result<hyper::Response<Incoming>, hyper_util::client::legacy::Error> {
        match self {
            ProxiedClient::Http(c) => c.request(req).await,
            ProxiedClient::Socks5(c) => c.request(req).await,
        }
    }
}

impl CursorClient {
    pub fn backend(&self) -> &str {
        &self.backend
    }

    pub fn http(
        &self,
    ) -> &Client<
        hyper_rustls::HttpsConnector<HttpConnector>,
        http_body_util::Full<hyper::body::Bytes>,
    > {
        &self.client
    }

    pub fn new(backend: &str, timeout_s: u64) -> anyhow::Result<Self> {
        Ok(Self {
            client: build_direct_client()?,
            proxied: None,
            backend: backend.to_string(),
            timeout_s,
            proxy_id: None,
        })
    }

    pub fn new_via_http_proxy(
        backend: &str,
        timeout_s: u64,
        proxy_id: &str,
        proxy_url: &str,
    ) -> anyhow::Result<Self> {
        let parsed =
            crate::proxypool::parse_proxy_url(proxy_url).map_err(|e| anyhow::anyhow!(e))?;
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        http.set_connect_timeout(Some(std::time::Duration::from_secs(10)));
        http.set_keepalive(Some(std::time::Duration::from_secs(30)));
        http.set_nodelay(true);
        let proxy_uri: hyper::Uri = format!("http://{}:{}", parsed.host, parsed.port)
            .parse()
            .map_err(|e| anyhow::anyhow!("proxy uri: {e}"))?;
        let proxied = if parsed.socks5 {
            let mut socks =
                hyper_util::client::legacy::connect::proxy::SocksV5::new(proxy_uri, http);
            if let Some(u) = parsed.user.as_deref().filter(|u| !u.is_empty()) {
                socks = socks.with_auth(u.to_string(), parsed.pass.clone().unwrap_or_default());
            }
            // 域名交给代理端解析, 避免本机 DNS 泄漏/被污染
            socks = socks.local_dns(false);
            let https = hyper_rustls::HttpsConnectorBuilder::new()
                .with_webpki_roots()
                .https_or_http()
                .enable_all_versions()
                .wrap_connector(socks);
            ProxiedClient::Socks5(
                Client::builder(TokioExecutor::new())
                    .pool_max_idle_per_host(POOL_MAX_IDLE_PROXY)
                    .pool_idle_timeout(std::time::Duration::from_secs(90))
                    .build(https),
            )
        } else {
            let mut tunnel =
                hyper_util::client::legacy::connect::proxy::Tunnel::new(proxy_uri, http);
            if let (Some(u), Some(p)) = (&parsed.user, &parsed.pass) {
                use base64::Engine;
                let token = base64::engine::general_purpose::STANDARD.encode(format!("{u}:{p}"));
                let hv = hyper::header::HeaderValue::from_str(&format!("Basic {token}"))
                    .map_err(|e| anyhow::anyhow!("proxy auth header: {e}"))?;
                tunnel = tunnel.with_auth(hv);
            }
            let https = hyper_rustls::HttpsConnectorBuilder::new()
                .with_webpki_roots()
                .https_or_http()
                .enable_all_versions()
                .wrap_connector(tunnel);
            ProxiedClient::Http(
                Client::builder(TokioExecutor::new())
                    .pool_max_idle_per_host(POOL_MAX_IDLE_PROXY)
                    .pool_idle_timeout(std::time::Duration::from_secs(90))
                    .build(https),
            )
        };
        Ok(Self {
            client: build_direct_client()?,
            proxied: Some(proxied),
            backend: backend.to_string(),
            timeout_s,
            proxy_id: Some(proxy_id.to_string()),
        })
    }

    /// 流式调用 Cursor Stream, 返回帧流
    pub async fn stream(
        &self,
        access_token: &str,
        machine_id: &str,
        body: &Value,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Value, CursorError>> + Send>>, CursorError> {
        let payload = serde_json::to_vec(body).map_err(|e| CursorError::Decode(e.to_string()))?;
        let envelope = connect_envelope(&payload, 0);

        let mut req = hyper::Request::builder()
            .method("POST")
            .uri(format!("{}{}", self.backend, STREAM_PATH))
            .header("content-type", "application/connect+json")
            .header("connect-protocol-version", "1")
            .header("connect-timeout-ms", (self.timeout_s * 1000).to_string())
            .header("accept", "application/connect+json")
            .header("accept-encoding", "identity");

        for (k, v) in cursor_headers(access_token, machine_id) {
            req = req.header(k, v);
        }

        let req = req
            .body(http_body_util::Full::new(hyper::body::Bytes::from(
                envelope,
            )))
            .map_err(|e| CursorError::Network(e.to_string()))?;

        let timeout = std::time::Duration::from_secs(self.timeout_s.max(1));
        let resp = tokio::time::timeout(timeout, self.request(req))
            .await
            .map_err(|_| {
                CursorError::Network(format!(
                    "upstream response headers timeout after {}s",
                    self.timeout_s
                ))
            })?
            .map_err(|e| CursorError::Network(e.to_string()))?;

        let status = resp.status().as_u16();
        if status >= 400 {
            let body_bytes = tokio::time::timeout(
                std::time::Duration::from_secs(15),
                http_body_util::BodyExt::collect(resp.into_body()),
            )
            .await
            .map_err(|_| CursorError::Network("error body read timeout".into()))?
            .map_err(|e| CursorError::Network(e.to_string()))?
            .to_bytes();
            let text = String::from_utf8_lossy(&body_bytes);
            return Err(CursorError::Http(status, text.chars().take(200).collect()));
        }

        // hyper Incoming → Bytes stream 适配
        use futures_util::StreamExt;
        use http_body_util::BodyExt;
        let byte_stream =
            BodyExt::into_data_stream(resp.into_body()).map(|r| r.map_err(|e| e.to_string()));
        let mut frames = Box::pin(FrameStream {
            body: Box::pin(byte_stream),
            buf: BytesMut::with_capacity(65536), // 64KB 初始缓冲, 减少大帧的 read 次数
        });
        // 业务错误常跟在 HTTP 200 后的首帧。偷看一帧，Max mode / 配额拒绝立刻变 Err，
        // 让 handler 能同号降级重试，而不是 SSE 空内容。
        match frames.as_mut().next().await {
            Some(Err(e)) => Err(e),
            None => Ok(Box::pin(futures_util::stream::empty())),
            Some(Ok(first)) => {
                if let Some(msg) = crate::translate::extract_cursor_error_message(&first) {
                    return Err(CursorError::Rejected(msg));
                }
                Ok(Box::pin(
                    futures_util::stream::once(async move { Ok(first) }).chain(frames),
                ))
            }
        }
    }

    pub async fn request(
        &self,
        req: hyper::Request<http_body_util::Full<hyper::body::Bytes>>,
    ) -> Result<hyper::Response<Incoming>, hyper_util::client::legacy::Error> {
        if let Some(p) = &self.proxied {
            p.request(req).await
        } else {
            self.client.request(req).await
        }
    }
}

fn build_direct_client() -> anyhow::Result<HttpsClient> {
    let mut http = HttpConnector::new();
    http.enforce_http(false);
    http.set_connect_timeout(Some(std::time::Duration::from_secs(10)));
    http.set_keepalive(Some(std::time::Duration::from_secs(30)));
    http.set_nodelay(true);
    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_all_versions()
        .wrap_connector(http);
    Ok(Client::builder(TokioExecutor::new())
        .pool_max_idle_per_host(POOL_MAX_IDLE_DIRECT)
        .pool_idle_timeout(std::time::Duration::from_secs(90))
        .build(https))
}

/// Connect 帧流解码器 (泛型 body stream)
struct FrameStream<S> {
    body: S,
    buf: BytesMut,
}

impl<S, E> Stream for FrameStream<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: std::fmt::Display,
{
    type Item = Result<Value, CursorError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // 先尝试从 buf 解析帧
            if self.buf.len() >= 5 {
                let flags = self.buf[0];
                let n = u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]])
                    as usize;
                if n > MAX_FRAME_BYTES {
                    return Poll::Ready(Some(Err(CursorError::Decode(format!(
                        "connect frame {n} bytes exceeds {MAX_FRAME_BYTES}"
                    )))));
                }
                if self.buf.len() >= 5 + n {
                    let payload = self.buf.split_to(5 + n).split_off(5);
                    let payload = if flags & 1 != 0 {
                        match decompress_gzip(&payload) {
                            Ok(p) => p,
                            Err(e) => return Poll::Ready(Some(Err(CursorError::Decode(e)))),
                        }
                    } else {
                        payload.to_vec()
                    };
                    let obj = if payload.is_empty() {
                        None
                    } else {
                        match serde_json::from_slice(&payload) {
                            Ok(v) => Some(v),
                            Err(e) => {
                                return Poll::Ready(Some(Err(CursorError::Decode(format!(
                                    "connect frame json: {e}"
                                )))))
                            }
                        }
                    };
                    if let Some(v) = obj {
                        return Poll::Ready(Some(Ok(v)));
                    }
                    if flags & 2 != 0 {
                        return Poll::Ready(None);
                    }
                    continue;
                }
            }
            // 从 body 读更多数据
            match Pin::new(&mut self.body).poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    if self.buf.len().saturating_add(chunk.len()) > MAX_STREAM_BUF {
                        return Poll::Ready(Some(Err(CursorError::Decode(format!(
                            "stream buffer {}+{} exceeds {MAX_STREAM_BUF}",
                            self.buf.len(),
                            chunk.len()
                        )))));
                    }
                    self.buf.extend_from_slice(&chunk);
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(CursorError::Network(e.to_string()))));
                }
                Poll::Ready(None) => {
                    if self.buf.is_empty() {
                        return Poll::Ready(None);
                    }
                    return Poll::Ready(Some(Err(CursorError::Decode(
                        "stream ended with partial frame".into(),
                    ))));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn decompress_gzip(data: &[u8]) -> Result<Vec<u8>, String> {
    use std::io::Read;
    let mut decoder = flate2::read::GzDecoder::new(data);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out).map_err(|e| e.to_string())?;
    Ok(out)
}

/// 刷新 access_token（OAuth 风格，支持自定义端点）
pub async fn refresh_access_token(
    client: &reqwest::Client,
    refresh_url: Option<&str>,
    backend: &str,
    refresh_token: &str,
) -> Result<(String, Option<u64>), String> {
    let url = refresh_url
        .map(|u| u.to_string())
        .unwrap_or_else(|| format!("{}/oauth/token", backend.trim_end_matches('/')));

    let resp = client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }))
        .send()
        .await
        .map_err(|e| format!("refresh request failed: {}", e))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("refresh failed {}: {}", status, text));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("refresh decode failed: {}", e))?;

    let access_token = body
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "no access_token in refresh response".to_string())?
        .to_string();

    let expires_in = body.get("expires_in").and_then(|v| v.as_u64()).map(|s| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            + s
    });

    Ok((access_token, expires_in))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kimi_family_gets_1m_and_long_output() {
        assert!(is_kimi_family("kimi-k3"));
        assert!(is_kimi_family("kimi-k3-max"));
        assert!(is_kimi_family("Kimi-K3-high"));
        assert!(!is_kimi_family("claude-sonnet-4-6"));
        assert_eq!(context_window_for("kimi-k3"), 1_048_576);
        assert_eq!(default_max_tokens_for("kimi-k3"), Some(131_072));
        assert_eq!(default_max_tokens_for("claude-sonnet-4-6"), Some(131_072));
    }

    #[test]
    fn thinking_level_from_model_test() {
        assert_eq!(thinking_level_from_model("kimi-k3"), ThinkingLevel::Max);
        assert_eq!(thinking_level_from_model("kimi-k3-max"), ThinkingLevel::Max);
        assert_eq!(thinking_level_from_model("kimi-k3-high"), ThinkingLevel::High);
        assert_eq!(thinking_level_from_model("kimi-k3-low"), ThinkingLevel::Low);
        assert_eq!(thinking_level_from_model("kimi-k3-unknown"), ThinkingLevel::Max);
    }

    #[test]
    fn model_for_thinking_level_test() {
        assert_eq!(model_for_thinking_level(ThinkingLevel::Max), "kimi-k3-max");
        assert_eq!(model_for_thinking_level(ThinkingLevel::High), "kimi-k3-high");
        assert_eq!(model_for_thinking_level(ThinkingLevel::Low), "kimi-k3-low");
    }

    #[test]
    fn auto_detect_thinking_level_explicit() {
        // max_mode=true → Max
        assert_eq!(
            auto_detect_thinking_level(&json!({"max_mode": true, "messages": []})),
            ThinkingLevel::Max
        );
        // thinking_level 字段
        assert_eq!(
            auto_detect_thinking_level(&json!({"thinking_level": "high", "messages": []})),
            ThinkingLevel::High
        );
        assert_eq!(
            auto_detect_thinking_level(&json!({"thinking_level": "low", "messages": []})),
            ThinkingLevel::Low
        );
    }

    #[test]
    fn auto_detect_thinking_level_tools() {
        // 有 tools → Max
        assert_eq!(
            auto_detect_thinking_level(&json!({
                "messages": [{"role": "user", "content": "hi"}],
                "tools": [{"type": "function", "function": {"name": "bash"}}]
            })),
            ThinkingLevel::Max
        );
    }

    #[test]
    fn auto_detect_thinking_level_long_context() {
        // 长消息 → Max
        let long_content = "x".repeat(100_001);
        assert_eq!(
            auto_detect_thinking_level(&json!({
                "messages": [{"role": "user", "content": long_content}]
            })),
            ThinkingLevel::Max
        );
        // 多消息 → High
        let msgs: Vec<Value> = (0..6).map(|i| json!({"role": "user", "content": format!("msg {}", i)})).collect();
        assert_eq!(
            auto_detect_thinking_level(&json!({"messages": msgs})),
            ThinkingLevel::High
        );
        // 短消息 → Low
        assert_eq!(
            auto_detect_thinking_level(&json!({
                "messages": [{"role": "user", "content": "hi"}]
            })),
            ThinkingLevel::Low
        );
    }

    #[test]
    fn auto_select_kimi_model_test() {
        assert_eq!(
            auto_select_kimi_model(&json!({"max_mode": true, "messages": []})),
            "kimi-k3-max"
        );
        assert_eq!(
            auto_select_kimi_model(&json!({"thinking_level": "high", "messages": []})),
            "kimi-k3-high"
        );
        assert_eq!(
            auto_select_kimi_model(&json!({
                "messages": [{"role": "user", "content": "hi"}]
            })),
            "kimi-k3-low"
        );
    }

    #[test]
    fn max_tokens_floor_protection() {
        assert_eq!(effective_max_tokens(None, "kimi-k3"), 131_072);
        assert_eq!(effective_max_tokens(Some(16), "kimi-k3"), 1024);
        assert_eq!(effective_max_tokens(Some(1024), "kimi-k3"), 1024);
        assert_eq!(effective_max_tokens(Some(4096), "kimi-k3"), 4096);
        assert_eq!(effective_max_tokens(Some(0), "claude-sonnet-4-6"), 1024);
    }

    #[test]
    fn max_mode_from_request_variants() {
        assert_eq!(max_mode_from_request(&json!({"max_mode": true})), Some(true));
        assert_eq!(max_mode_from_request(&json!({"maxMode": true})), Some(true));
        assert_eq!(
            max_mode_from_request(&json!({"requestedModel": {"maxMode": true}})),
            Some(true)
        );
        assert_eq!(max_mode_from_request(&json!({"max_mode": false})), Some(false));
        assert_eq!(max_mode_from_request(&json!({})), None);
    }

    #[test]
    fn max_mode_defaults_off() {
        let msgs = vec![json!({"role": "user", "content": "hi"})];
        let body = build_cursor_body_with_tools(
            &msgs, "kimi-k3", Some(128), None, None, None, None, None, None,
        );
        assert_eq!(body["requestedModel"]["maxMode"], false);
        let body_on = build_cursor_body_with_tools(
            &msgs, "kimi-k3", Some(128), None, None, None, None, None, Some(true),
        );
        assert_eq!(body_on["requestedModel"]["maxMode"], true);
    }

    #[test]
    fn tools_go_into_cursor_body() {
        let msgs = vec![json!({"role": "user", "content": "ls"})];
        let tools = json!([{
            "type": "function",
            "function": {"name": "bash", "description": "sh", "parameters": {"type": "object"}}
        }]);
        let body = build_cursor_body_with_tools(
            &msgs,
            "kimi-k3",
            Some(128),
            None,
            Some(&tools),
            Some(&json!("required")),
            Some(false),
            None,
            None,
        );
        assert_eq!(body["tools"][0]["name"], "bash");
        assert_eq!(body["modelConfig"]["parallelToolCalls"], false);
        assert!(body["messages"][0]["parts"]["parts"][0]["text"]["text"]
            .as_str()
            .unwrap()
            .contains("must call a tool"));
    }

    #[test]
    fn conversation_id_is_stable_for_same_session() {
        let a = conversation_id_for(Some("job-k3"), Some("acc-5"));
        let b = conversation_id_for(Some("job-k3"), Some("acc-5"));
        let c = conversation_id_for(Some("job-k3"), Some("acc-2"));
        assert_eq!(a, b);
        assert_ne!(a, c);
        let msgs = vec![json!({"role": "user", "content": "hi"})];
        let body =
            build_cursor_body_with_tools(&msgs, "kimi-k3", None, None, None, None, None, Some(&a), None);
        assert_eq!(body["conversationId"], a);
    }

    #[test]
    fn auto_select_kimi_model_real_scenarios() {
        // 真实场景：hi + max_tokens=16 → 应该映射到 low（短消息）
        assert_eq!(
            auto_select_kimi_model(&json!({
                "model": "kimi-k3",
                "messages": [{"role": "user", "content": "hi"}],
                "max_tokens": 16
            })),
            "kimi-k3-low"
        );
        // 真实场景：多轮对话 → 应该映射到 high
        assert_eq!(
            auto_select_kimi_model(&json!({
                "model": "kimi-k3",
                "messages": [
                    {"role": "user", "content": "hi"},
                    {"role": "assistant", "content": "a"},
                    {"role": "user", "content": "b"},
                    {"role": "assistant", "content": "c"},
                    {"role": "user", "content": "d"},
                    {"role": "assistant", "content": "e"},
                    {"role": "user", "content": "f"}
                ],
                "max_tokens": 16
            })),
            "kimi-k3-high"
        );
        // 真实场景：max_mode → 应该映射到 max
        assert_eq!(
            auto_select_kimi_model(&json!({
                "model": "kimi-k3",
                "messages": [{"role": "user", "content": "hi"}],
                "max_mode": true,
                "max_tokens": 16
            })),
            "kimi-k3-max"
        );
    }
}
