//! 设置与日志 API.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;

/// 查询串布尔: 接受 true/false/1/0/yes/no (axum 默认 serde 只认 true/false)
fn de_opt_bool<'de, D>(d: D) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(d)?;
    Ok(s.and_then(|s| match s.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }))
}
use std::sync::Arc;

use crate::config;
use crate::AppState;

#[derive(Deserialize)]
pub struct LogsQuery {
    pub n: Option<usize>,
    pub model: Option<String>,
    pub account: Option<String>,
    pub status: Option<u16>,
    #[serde(default, deserialize_with = "de_opt_bool")]
    pub stream: Option<bool>,
    pub client_ip: Option<String>,
    pub key: Option<String>,
    pub kind: Option<String>,
    #[serde(default, deserialize_with = "de_opt_bool")]
    pub ok: Option<bool>,
    pub min_latency_ms: Option<u64>,
    pub q: Option<String>,
    /// 增量游标: 只返回 ID > after 的日志
    pub after: Option<u64>,
}

fn build_filter(q: &LogsQuery, limit: usize) -> crate::logbuf::LogFilter {
    let mut filter = crate::logbuf::LogFilter::new();
    filter.limit = limit;
    filter.model = q.model.clone().filter(|s| !s.is_empty());
    filter.account = q.account.clone().filter(|s| !s.is_empty());
    filter.status = q.status;
    filter.stream = q.stream;
    filter.client_ip = q.client_ip.clone().filter(|s| !s.is_empty());
    filter.key = q.key.clone().filter(|s| !s.is_empty());
    filter.kind = q.kind.clone().filter(|s| !s.is_empty());
    filter.ok = q.ok;
    filter.min_latency_ms = q.min_latency_ms;
    filter.q = q.q.clone().filter(|s| !s.is_empty());
    filter
}

fn filter_matches(f: &crate::logbuf::LogFilter, e: &serde_json::Value) -> bool {
    f.matches(e)
}

fn has_filter(q: &LogsQuery) -> bool {
    q.model.as_deref().map_or(false, |s| !s.is_empty())
        || q.account.as_deref().map_or(false, |s| !s.is_empty())
        || q.status.is_some()
        || q.stream.is_some()
        || q.client_ip.as_deref().map_or(false, |s| !s.is_empty())
        || q.key.as_deref().map_or(false, |s| !s.is_empty())
        || q.kind.as_deref().map_or(false, |s| !s.is_empty())
        || q.ok.is_some()
        || q.min_latency_ms.is_some()
        || q.q.as_deref().map_or(false, |s| !s.is_empty())
}

/// GET /admin/api/logs/stats — 对当前筛选结果做聚合 (缓冲区内, 最多 2000 条)
pub async fn api_logs_stats(
    State(state): State<Arc<AppState>>,
    Query(q): Query<LogsQuery>,
) -> impl IntoResponse {
    let filter = build_filter(&q, usize::MAX);
    let logs = state.log_buffer.query_all(&filter);
    let n = logs.len();
    let mut ok = 0usize;
    let mut tokens_in = 0u64;
    let mut tokens_out = 0u64;
    let mut cost_nano = 0i64;
    let mut lat: Vec<u64> = Vec::with_capacity(n);
    let mut by_model: std::collections::BTreeMap<String, (usize, usize, u64, i64)> = Default::default();
    let mut by_key: std::collections::BTreeMap<String, (usize, usize, u64, i64)> = Default::default();
    let mut by_status: std::collections::BTreeMap<u64, usize> = Default::default();
    let mut by_account: std::collections::BTreeMap<String, (usize, usize)> = Default::default();
    let g = |v: &serde_json::Value, k: &str| v.get(k).and_then(|x| x.as_u64()).unwrap_or(0);
    let gs = |v: &serde_json::Value, k: &str| v.get(k).and_then(|x| x.as_str()).unwrap_or("").to_string();
    for l in &logs {
        let st = g(l, "status");
        let is_ok = st == 200;
        if is_ok {
            ok += 1;
        }
        let ti = g(l, "input_tokens") + g(l, "cache_read_tokens") + g(l, "cache_write_tokens");
        let to = g(l, "output_tokens");
        let c = l.get("cost_nano").and_then(|x| x.as_i64()).unwrap_or(0);
        tokens_in += ti;
        tokens_out += to;
        cost_nano += c;
        lat.push(g(l, "latency_ms"));
        let m = by_model.entry(gs(l, "model")).or_default();
        m.0 += 1;
        if is_ok { m.1 += 1; }
        m.2 += ti + to;
        m.3 += c;
        let k = by_key.entry(gs(l, "key_name")).or_default();
        k.0 += 1;
        if is_ok { k.1 += 1; }
        k.2 += ti + to;
        k.3 += c;
        *by_status.entry(st).or_default() += 1;
        let a = by_account.entry(gs(l, "account")).or_default();
        a.0 += 1;
        if !is_ok { a.1 += 1; }
    }
    lat.sort_unstable();
    let pct = |p: f64| -> u64 {
        if lat.is_empty() { 0 } else { lat[((lat.len() - 1) as f64 * p).round() as usize] }
    };
    let avg = if lat.is_empty() { 0 } else { lat.iter().sum::<u64>() / lat.len() as u64 };
    let row4 = |(k, (n, ok, tok, cost)): (&String, &(usize, usize, u64, i64))| json!({
        "name": k, "requests": n, "ok": ok, "errors": n - ok, "tokens": tok,
        "cost": crate::billing::fmt_money(*cost),
    });
    let mut models: Vec<_> = by_model.iter().map(row4).collect();
    models.sort_by_key(|v| std::cmp::Reverse(v["requests"].as_u64().unwrap_or(0)));
    let mut keys: Vec<_> = by_key.iter().map(row4).collect();
    keys.sort_by_key(|v| std::cmp::Reverse(v["requests"].as_u64().unwrap_or(0)));
    let mut accounts: Vec<_> = by_account
        .iter()
        .map(|(k, (n, err))| json!({"name": k, "requests": n, "errors": err}))
        .collect();
    accounts.sort_by_key(|v| std::cmp::Reverse(v["requests"].as_u64().unwrap_or(0)));
    let first_ts = logs.last().and_then(|l| l.get("ts").cloned());
    let last_ts = logs.first().and_then(|l| l.get("ts").cloned());
    Json(json!({
        "count": n,
        "ok": ok,
        "errors": n - ok,
        "error_rate": if n == 0 { 0.0 } else { (n - ok) as f64 / n as f64 },
        "tokens_in": tokens_in,
        "tokens_out": tokens_out,
        "cost": crate::billing::fmt_money(cost_nano),
        "latency": {"avg_ms": avg, "p50_ms": pct(0.5), "p90_ms": pct(0.9), "p99_ms": pct(0.99), "max_ms": lat.last().copied().unwrap_or(0)},
        "by_model": models,
        "by_key": keys,
        "by_status": by_status.iter().map(|(k, v)| json!({"status": k, "count": v})).collect::<Vec<_>>(),
        "by_account": accounts,
        "range": {"from": first_ts, "to": last_ts},
        "buffer_len": state.log_buffer.len(),
    }))
}

/// GET /admin/api/logs/export?format=csv|jsonl — 导出当前筛选 (缓冲区内全部)
#[derive(Deserialize)]
pub struct ExportQuery {
    #[serde(flatten)]
    pub f: LogsQuery,
    pub format: Option<String>,
}

pub async fn api_logs_export(
    State(state): State<Arc<AppState>>,
    Query(q): Query<ExportQuery>,
) -> Response {
    let filter = build_filter(&q.f, usize::MAX);
    let logs = state.log_buffer.query_all(&filter);
    let fmt = q.format.unwrap_or_else(|| "csv".into());
    let ts = chrono::Utc::now().format("%Y%m%d-%H%M%S");
    if fmt == "jsonl" {
        let body: String = logs.iter().map(|l| l.to_string() + "\n").collect();
        return (
            [
                (axum::http::header::CONTENT_TYPE, "application/x-ndjson".to_string()),
                (axum::http::header::CONTENT_DISPOSITION, format!("attachment; filename=\"logs-{ts}.jsonl\"")),
            ],
            body,
        )
            .into_response();
    }
    let cols = [
        "ts", "req_id", "status", "model", "account", "kind", "key_name", "key_prefix", "stream",
        "input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens", "total_tokens",
        "cost", "latency_ms", "client_ip",
    ];
    let esc = |v: &serde_json::Value| -> String {
        let s = match v {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Null => String::new(),
            other => other.to_string(),
        };
        if s.contains([',', '"', '\n']) {
            format!("\"{}\"", s.replace('"', "\"\""))
        } else {
            s
        }
    };
    let mut out = String::from("\u{feff}");
    out.push_str(&cols.join(","));
    out.push('\n');
    for l in &logs {
        let row: Vec<String> = cols
            .iter()
            .map(|c| esc(l.get(*c).unwrap_or(&serde_json::Value::Null)))
            .collect();
        out.push_str(&row.join(","));
        out.push('\n');
    }
    (
        [
            (axum::http::header::CONTENT_TYPE, "text/csv; charset=utf-8".to_string()),
            (axum::http::header::CONTENT_DISPOSITION, format!("attachment; filename=\"logs-{ts}.csv\"")),
        ],
        out,
    )
        .into_response()
}

/// DELETE /admin/api/logs — 清空面板缓冲 (磁盘 proxy.log 不动)
pub async fn api_logs_clear(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let n = state.log_buffer.clear();
    state.audit.key_op("logs_clear", "*", json!({"cleared": n}));
    Json(json!({"ok": true, "cleared": n}))
}

/// GET /admin/api/logs — 最近日志 (新→旧)，支持筛选 + 增量游标
pub async fn api_logs_recent(
    State(state): State<Arc<AppState>>,
    Query(q): Query<LogsQuery>,
) -> impl IntoResponse {
    let n = q.n.unwrap_or(50).clamp(1, 500);
    // 增量模式: 只取新日志
    if let Some(after) = q.after {
        let logs = if has_filter(&q) {
            let filter = build_filter(&q, n);
            state
                .log_buffer
                .query_after(after, usize::MAX)
                .into_iter()
                .filter(|e| filter_matches(&filter, e))
                .take(n)
                .collect()
        } else {
            state.log_buffer.query_after(after, n)
        };
        let max_id = state.log_buffer.max_id();
        return Json(json!({
            "logs": logs,
            "count": logs.len(),
            "max_id": max_id,
            "incremental": true,
        }));
    }
    // 全量模式
    let filter = build_filter(&q, n);
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
        obj.insert(
            "upstreams".into(),
            json!(state.upstreams.keys().collect::<Vec<_>>()),
        );
        obj.insert("version".into(), json!(env!("CARGO_PKG_VERSION")));
        obj.insert(
            "restart_required_fields".into(),
            json!([
                "timeout_s",
                "max_concurrency_per_account",
                "backend",
                "host",
                "port"
            ]),
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
