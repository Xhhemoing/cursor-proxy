//! 出口代理池管理 API.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;

use crate::config;
use crate::proxypool::{self, ProxyAssignRule, ProxyNode};
use crate::AppState;

/// GET /admin/api/proxies
pub async fn api_proxies_get(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.proxies.overview())
}

#[derive(Deserialize, Default)]
pub struct ProxyPoolPatch {
    pub enabled: Option<bool>,
    pub require_proxy: Option<bool>,
    pub default_mode: Option<String>,
    pub probe_interval_s: Option<u64>,
    pub probe_timeout_ms: Option<u64>,
    pub fail_threshold: Option<u32>,
    pub nodes: Option<Vec<ProxyNode>>,
    pub rules: Option<Vec<ProxyAssignRule>>,
}

/// POST /admin/api/proxies — 热更新代理池配置
pub async fn api_proxies_patch(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ProxyPoolPatch>,
) -> Response {
    let snapshot = {
        let mut cfg = state.config.lock();
        if let Some(v) = body.enabled {
            cfg.proxy.enabled = v;
        }
        if let Some(v) = body.require_proxy {
            cfg.proxy.require_proxy = v;
        }
        if let Some(m) = body.default_mode {
            cfg.proxy.default_mode = proxypool::AssignMode::parse(&m);
        }
        if let Some(v) = body.probe_interval_s {
            cfg.proxy.probe_interval_s = v.min(3600);
        }
        if let Some(v) = body.probe_timeout_ms {
            cfg.proxy.probe_timeout_ms = v.clamp(500, 60_000);
        }
        if let Some(v) = body.fail_threshold {
            cfg.proxy.fail_threshold = v.clamp(1, 20);
        }
        if let Some(nodes) = body.nodes {
            let mut seen = std::collections::HashSet::new();
            let mut merged = Vec::new();
            for n in nodes {
                if n.id.trim().is_empty() {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error": "proxy id required"})),
                    )
                        .into_response();
                }
                if !seen.insert(n.id.clone()) {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error": format!("duplicate proxy id {}", n.id)})),
                    )
                        .into_response();
                }
                let mut node = n;
                if node.url.contains("***") || node.url.trim().is_empty() {
                    if let Some(old) = cfg.proxy.nodes.iter().find(|o| o.id == node.id) {
                        node.url = old.url.clone();
                    } else {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(json!({"error": format!("proxy {} missing url", node.id)})),
                        )
                            .into_response();
                    }
                }
                if let Err(e) = proxypool::parse_proxy_url(&node.url) {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error": format!("proxy {}: {e}", node.id)})),
                    )
                        .into_response();
                }
                merged.push(node);
            }
            cfg.proxy.nodes = merged;
        }
        if let Some(rules) = body.rules {
            cfg.proxy.rules = rules;
        }
        cfg.clone()
    };
    if let Err(e) = config::save_config(&snapshot) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response();
    }
    state.proxies.replace(snapshot.proxy.clone());
    state.cursor_factory.invalidate();
    state.audit.settings_op(&["proxy"]);
    Json(json!({"status": "ok", "proxy": state.proxies.overview()})).into_response()
}

#[derive(Deserialize, Default)]
pub struct ProxyImportBody {
    /// 每行一个代理, 见 proxypool::parse_proxy_line 支持的格式
    #[serde(default)]
    pub text: String,
    /// 无 scheme 的行默认协议: http / https
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub tags: Vec<String>,
    /// 自动编号前缀, 默认 "px"
    #[serde(default)]
    pub id_prefix: Option<String>,
    #[serde(default)]
    pub max_accounts: u32,
    #[serde(default)]
    pub note: String,
    /// true = 只解析不落盘, 用于预览
    #[serde(default)]
    pub dry_run: bool,
}

/// POST /admin/api/proxies/import — 批量导入代理节点 (文本粘贴)
pub async fn api_proxies_import(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ProxyImportBody>,
) -> Response {
    let scheme = body.kind.as_deref().unwrap_or("http");
    let default_kind = match scheme.to_ascii_lowercase().as_str() {
        "https" => proxypool::ProxyKind::Https,
        "socks5" | "socks5h" | "socks" => proxypool::ProxyKind::Socks5,
        _ => proxypool::ProxyKind::Http,
    };
    let prefix = body
        .id_prefix
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("px")
        .to_string();

    let existing = state.config.lock().proxy.nodes.clone();
    let mut seen_urls: std::collections::HashSet<String> = existing
        .iter()
        .map(|n| n.url.trim().to_string())
        .collect();
    let existing_ids: std::collections::HashSet<String> =
        existing.iter().map(|n| n.id.clone()).collect();
    // 编号从已有 prefix-N 的最大值之后继续
    let mut seq = existing
        .iter()
        .filter_map(|n| {
            n.id
                .strip_prefix(&prefix)
                .and_then(|r| r.strip_prefix('-'))
                .and_then(|r| r.parse::<u32>().ok())
        })
        .max()
        .unwrap_or(0);

    let mut added: Vec<ProxyNode> = Vec::new();
    let mut duplicates = 0usize;
    let mut errors: Vec<serde_json::Value> = Vec::new();
    for (i, raw) in body.text.lines().enumerate() {
        let url = match proxypool::parse_proxy_line(raw, scheme) {
            Ok(Some(u)) => u,
            Ok(None) => continue,
            Err(e) => {
                errors.push(json!({"line": i + 1, "text": raw.trim(), "error": e}));
                continue;
            }
        };
        if !seen_urls.insert(url.clone()) {
            duplicates += 1;
            continue;
        }
        let id = loop {
            seq += 1;
            let id = format!("{prefix}-{seq}");
            if !existing_ids.contains(&id) {
                break id;
            }
        };
        // 行自带 scheme 时以行为准
        let kind = match url.split("://").next().unwrap_or("") {
            "socks5" | "socks5h" | "socks" => proxypool::ProxyKind::Socks5,
            "https" => proxypool::ProxyKind::Https,
            "http" => proxypool::ProxyKind::Http,
            _ => default_kind,
        };
        added.push(ProxyNode {
            id,
            url,
            kind,
            region: body.region.trim().to_string(),
            tags: body.tags.iter().map(|t| t.trim().to_string()).filter(|t| !t.is_empty()).collect(),
            enabled: true,
            max_accounts: body.max_accounts,
            note: body.note.trim().to_string(),
        });
    }

    let preview: Vec<serde_json::Value> = added.iter().map(|n| n.sanitized()).collect();
    if body.dry_run {
        return Json(json!({
            "status": "ok", "dry_run": true,
            "added": added.len(), "duplicates": duplicates, "errors": errors,
            "nodes": preview,
        }))
        .into_response();
    }
    if added.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "no new proxies parsed", "duplicates": duplicates, "errors": errors})),
        )
            .into_response();
    }
    let snapshot = {
        let mut cfg = state.config.lock();
        cfg.proxy.nodes.extend(added.iter().cloned());
        cfg.clone()
    };
    if let Err(e) = config::save_config(&snapshot) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response();
    }
    state.proxies.replace(snapshot.proxy.clone());
    state.cursor_factory.invalidate();
    state.audit.settings_op(&["proxy_import"]);
    Json(json!({
        "status": "ok", "dry_run": false,
        "added": added.len(), "duplicates": duplicates, "errors": errors,
        "nodes": preview, "total": snapshot.proxy.nodes.len(),
    }))
    .into_response()
}

#[derive(Deserialize)]
pub struct ProbeBody {
    pub ids: Option<Vec<String>>,
}

/// POST /admin/api/proxies/probe — 探测出口连通性 + 出口 IP
pub async fn api_proxies_probe(
    State(state): State<Arc<AppState>>,
    body: Option<Json<ProbeBody>>,
) -> impl IntoResponse {
    let want: Option<Vec<String>> = body.and_then(|b| b.ids.clone());
    let results = probe_nodes(&state, want.as_deref()).await;
    Json(json!({"status": "ok", "probed": results.len(), "results": results}))
}

/// POST /admin/api/proxies/rebalance — 按当前规则重绑未手动指定的账号
pub async fn api_proxies_rebalance(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let rows = state.pool.account_rows();
    let mut ids = Vec::new();
    let mut tags = std::collections::HashMap::new();
    for row in rows {
        let id = row["id"].as_str().unwrap_or("").to_string();
        if id.is_empty() {
            continue;
        }
        // 手动绑定不动
        if row
            .get("proxy_id")
            .and_then(|v| v.as_str())
            .map(|s| !s.is_empty())
            .unwrap_or(false)
        {
            continue;
        }
        let t = row
            .get("tags")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        tags.insert(id.clone(), t);
        ids.push(id);
    }
    let n = state.proxies.rebalance(&ids, &tags);
    state.cursor_factory.invalidate();
    Json(json!({"status": "ok", "reassigned": n, "proxy": state.proxies.overview()}))
}

pub async fn probe_nodes(state: &Arc<AppState>, ids: Option<&[String]>) -> Vec<serde_json::Value> {
    use futures_util::stream::{self, StreamExt};
    let cfg = state.proxies.load();
    let timeout = std::time::Duration::from_millis(cfg.probe_timeout_ms.max(500));
    let nodes: Vec<ProxyNode> = cfg
        .nodes
        .iter()
        .filter(|n| ids.map(|want| want.iter().any(|id| id == &n.id)).unwrap_or(true))
        .cloned()
        .collect();
    stream::iter(nodes)
        .map(|n| {
            let state = Arc::clone(state);
            async move {
                let start = Instant::now();
                let result = probe_one(&n.url, timeout).await;
                let latency = start.elapsed().as_millis() as u64;
                match result {
                    Ok(ip) => {
                        state
                            .proxies
                            .record_probe(&n.id, true, Some(latency), Some(ip.clone()), None);
                        json!({"id": n.id, "ok": true, "latency_ms": latency, "egress_ip": ip})
                    }
                    Err(e) => {
                        state
                            .proxies
                            .record_probe(&n.id, false, Some(latency), None, Some(e.clone()));
                        json!({"id": n.id, "ok": false, "latency_ms": latency, "error": e})
                    }
                }
            }
        })
        .buffer_unordered(32)
        .collect()
        .await
}

async fn probe_one(url: &str, timeout: std::time::Duration) -> Result<String, String> {
    let parsed = proxypool::parse_proxy_url(url)?;
    let stream = proxypool::connect_via_proxy(&parsed, "api.ipify.org", 443, timeout).await?;
    let connector = tokio_rustls::TlsConnector::from(Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            })
            .with_no_client_auth(),
    ));
    let name = rustls::pki_types::ServerName::try_from("api.ipify.org")
        .map_err(|e| e.to_string())?
        .to_owned();
    let mut tls = tokio::time::timeout(timeout, connector.connect(name, stream))
        .await
        .map_err(|_| "tls handshake timeout".to_string())?
        .map_err(|e| format!("tls: {e}"))?;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    tls.write_all(b"GET / HTTP/1.1\r\nHost: api.ipify.org\r\nConnection: close\r\n\r\n")
        .await
        .map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    tokio::time::timeout(timeout, tls.read_to_end(&mut buf))
        .await
        .map_err(|_| "ipify read timeout".to_string())?
        .map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&buf);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").trim();
    if body.is_empty() {
        return Err("empty ipify body".into());
    }
    Ok(body.chars().take(64).collect())
}
