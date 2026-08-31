//! 设置与日志 API.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use crate::config;
use crate::AppState;

#[derive(Deserialize)]
pub struct LogsQuery {
    pub n: Option<usize>,
    pub model: Option<String>,
    pub account: Option<String>,
    pub status: Option<u16>,
    pub stream: Option<bool>,
    pub client_ip: Option<String>,
    /// 增量游标: 只返回 ID > after 的日志
    pub after: Option<u64>,
}

/// GET /admin/api/logs — 最近日志 (新→旧)，支持筛选 + 增量游标
pub async fn api_logs_recent(
    State(state): State<Arc<AppState>>,
    Query(q): Query<LogsQuery>,
) -> impl IntoResponse {
    let n = q.n.unwrap_or(50).clamp(1, 500);
    // 增量模式: 只取新日志
    if let Some(after) = q.after {
        let logs = state.log_buffer.query_after(after, n);
        let max_id = state.log_buffer.max_id();
        return Json(json!({
            "logs": logs,
            "count": logs.len(),
            "max_id": max_id,
            "incremental": true,
        }));
    }
    // 全量模式
    let mut filter = crate::logbuf::LogFilter::new();
    filter.limit = n;
    filter.model = q.model;
    filter.account = q.account;
    filter.status = q.status;
    filter.stream = q.stream;
    filter.client_ip = q.client_ip;
    let logs = state.log_buffer.query(&filter);
    let max_id = state.log_buffer.max_id();
    Json(json!({
        "logs": logs,
        "count": logs.len(),
        "max_id": max_id,
        "incremental": false,
    }))
}

/// GET /admin/api/settings
pub async fn api_settings_get(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let config = state.config.load();
    let mut view = config.public_view();
    if let Some(obj) = view.as_object_mut() {
        obj.insert("upstreams".into(), json!(state.upstreams.keys().collect::<Vec<_>>()));
        obj.insert("version".into(), json!(env!("CARGO_PKG_VERSION")));
        obj.insert(
            "restart_required_fields".into(),
            json!(["timeout_s", "max_concurrency_per_account", "backend", "host", "port"]),
        );
    }
    Json(view)
}

#[derive(Deserialize, Default)]
pub struct SettingsPatch {
    pub default_model: Option<String>,
    pub timeout_s: Option<u64>,
    pub max_concurrency_per_account: Option<usize>,
}

/// POST /admin/api/settings — 热更新可立即生效的字段, 其余落盘并提示重启
pub async fn api_settings_patch(
    State(state): State<Arc<AppState>>,
    Json(body): Json<SettingsPatch>,
) -> Response {
    if body.default_model.is_none()
        && body.timeout_s.is_none()
        && body.max_concurrency_per_account.is_none()
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "no fields to update"})),
        )
            .into_response();
    }
    if let Some(model) = &body.default_model {
        if model.trim().is_empty() || model.len() > 80 {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid default_model"})),
            )
                .into_response();
        }
    }
    if let Some(t) = body.timeout_s {
        if !(10..=3600).contains(&t) {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "timeout_s must be 10..3600"})),
            )
                .into_response();
        }
    }
    if let Some(c) = body.max_concurrency_per_account {
        if !(1..=128).contains(&c) {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "max_concurrency_per_account must be 1..128"})),
            )
                .into_response();
        }
    }

    let (snapshot, restart_required, changed) = {
        let mut config = state.config.lock();
        let mut changed: Vec<&str> = Vec::new();
        if let Some(model) = body.default_model {
            config.default_model = model.trim().to_string();
            changed.push("default_model");
        }
        let mut restart = Vec::new();
        if let Some(t) = body.timeout_s {
            config.timeout_s = t;
            restart.push("timeout_s");
        }
        if let Some(c) = body.max_concurrency_per_account {
            config.max_concurrency_per_account = c;
            restart.push("max_concurrency_per_account");
        }
        (config.clone(), restart, changed)
    };

    if let Err(e) = config::save_config(&snapshot) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response();
    }
    state.audit.settings_op(&changed);

    Json(json!({
        "status": "ok",
        "settings": snapshot.public_view(),
        "restart_required": restart_required,
    }))
    .into_response()
}

/// GET /admin/api/rpm — RPM 仪表盘 (全局 + 每 key + 每账号)
pub async fn api_rpm_dashboard(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let config = state.config.load();
    let global_rpm = state.metrics.global_rpm();

    // 每 key RPM
    let keys: Vec<serde_json::Value> = config
        .api_keys
        .iter()
        .enumerate()
        .map(|(i, rec)| {
            let rpm = state.metrics.key_rpm(&rec.key);
            json!({
                "index": i,
                "prefix": rec.key.chars().take(8).collect::<String>(),
                "name": rec.name,
                "rpm": rpm,
                "rpm_limit": rec.rpm_limit,
                "max_concurrency": rec.max_concurrency,
                "enabled": rec.enabled,
            })
        })
        .collect();

    // 每账号 RPM
    let accounts: Vec<serde_json::Value> = state
        .pool
        .account_rows()
        .into_iter()
        .map(|row| {
            let id = row["id"].as_str().unwrap_or("").to_string();
            let rpm = state.metrics.account_rpm(&id);
            json!({
                "id": id,
                "rpm": rpm,
                "inflight": row["inflight"],
                "max_concurrency": row["max_concurrency"],
                "enabled": row["enabled"],
            })
        })
        .collect();

    Json(json!({
        "global_rpm": global_rpm,
        "keys": keys,
        "accounts": accounts,
    }))
}
