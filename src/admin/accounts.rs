//! 账号管理 API.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use crate::config::{self, Account};
use crate::AppState;

/// GET /admin/api/pool — 号池统计（支持 q/filter/sort/page，默认不全量返回账号）
#[derive(Deserialize, Default)]
pub struct PoolQuery {
    pub q: Option<String>,
    pub filter: Option<String>,
    pub sort: Option<String>,
    pub page: Option<usize>,
    pub page_size: Option<usize>,
    pub proxy_id: Option<String>,
    /// 1 = 兼容旧面板，返回全部账号
    pub all: Option<u8>,
}

pub async fn api_pool_stats(
    State(state): State<Arc<AppState>>,
    Query(q): Query<PoolQuery>,
) -> impl IntoResponse {
    if q.all == Some(1) && q.page.is_none() {
        return Json(state.pool.stats());
    }
    let mut stats = state.pool.query_accounts(
        q.q.as_deref().unwrap_or(""),
        q.filter.as_deref().unwrap_or("all"),
        q.sort.as_deref().unwrap_or("attention"),
        q.page.unwrap_or(1),
        q.page_size.unwrap_or(50),
        q.proxy_id.as_deref().unwrap_or(""),
    );
    enrich_proxy_bindings(&state, &mut stats);
    Json(stats)
}

fn enrich_proxy_bindings(state: &AppState, stats: &mut serde_json::Value) {
    if let Some(arr) = stats.get_mut("accounts").and_then(|v| v.as_array_mut()) {
        for acc in arr {
            let id = acc["id"].as_str().unwrap_or("").to_string();
            let manual = acc["proxy_id"].as_str().unwrap_or("").to_string();
            let assigned = if !manual.is_empty() {
                Some(manual.clone())
            } else {
                state.proxies.binding_of(&id)
            };
            acc["assigned_proxy"] = json!(assigned);
            acc["proxy_mode"] = json!(if !manual.is_empty() {
                "manual"
            } else if assigned.is_some() {
                "auto"
            } else {
                "direct"
            });
        }
    }
}

/// GET /admin/api/accounts — 账号列表
pub async fn api_accounts_list(
    State(state): State<Arc<AppState>>,
    Query(q): Query<PoolQuery>,
) -> impl IntoResponse {
    let mut stats = state.pool.query_accounts(
        q.q.as_deref().unwrap_or(""),
        q.filter.as_deref().unwrap_or("all"),
        q.sort.as_deref().unwrap_or("attention"),
        q.page.unwrap_or(1),
        q.page_size.unwrap_or(50),
        q.proxy_id.as_deref().unwrap_or(""),
    );
    enrich_proxy_bindings(&state, &mut stats);
    Json(json!({
        "accounts": stats.get("accounts").cloned().unwrap_or(json!([])),
        "total": stats.get("total_accounts").cloned().unwrap_or(json!(0)),
        "available": stats.get("available").cloned().unwrap_or(json!(0)),
        "filtered": stats.get("filtered").cloned().unwrap_or(json!(0)),
        "page": stats.get("page").cloned().unwrap_or(json!(1)),
        "pages": stats.get("pages").cloned().unwrap_or(json!(1)),
    }))
}

/// POST /admin/api/accounts/:id/toggle — 开关账号并写回 accounts.json
pub async fn api_account_toggle(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    let Some(enabled) = state.pool.toggle_account(&id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "account not found", "id": id})),
        )
            .into_response();
    };
    match config::persist_account_enabled(&id, enabled) {
        Ok(_) => {
            state.pool.rebuild_available_ids(); // 重建可用列表，确保开关立即生效
            state.audit.account_op("toggle", &id, json!({"enabled": enabled}));
            Json(json!({
                "status": "ok",
                "id": id,
                "enabled": enabled,
                "persisted": true,
            }))
            .into_response()
        }
        Err(e) => {
            // 回滚内存状态, 避免磁盘与运行时分叉
            let _ = state.pool.set_enabled(&id, !enabled);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": e.to_string(),
                    "id": id,
                    "persisted": false,
                })),
            )
                .into_response()
        }
    }
}

#[derive(Deserialize)]
pub struct SetEnabledBody {
    pub enabled: bool,
}

/// POST /admin/api/accounts/:id/enabled — 显式启用/禁用
pub async fn api_account_set_enabled(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<SetEnabledBody>,
) -> Response {
    if state.pool.set_enabled(&id, body.enabled).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "account not found", "id": id})),
        )
            .into_response();
    }
    match config::persist_account_enabled(&id, body.enabled) {
        Ok(_) => {
            state.pool.rebuild_available_ids(); // 重建可用列表，确保禁用立即生效
            state
                .audit
                .account_op("set_enabled", &id, json!({"enabled": body.enabled}));
            Json(json!({
                "status": "ok",
                "id": id,
                "enabled": body.enabled,
                "persisted": true,
            }))
            .into_response()
        }
        Err(e) => {
            let _ = state.pool.set_enabled(&id, !body.enabled);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string(), "persisted": false})),
            )
                .into_response()
        }
    }
}

/// POST /admin/api/accounts/:id/cooldown/clear
pub async fn api_account_clear_cooldown(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    if !state.pool.has_account(&id) {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "account not found", "id": id})),
        )
            .into_response();
    }
    state.pool.clear_cooldown(&id);
    state.audit.account_op("clear_cooldown", &id, json!({}));
    Json(json!({"status": "ok", "id": id, "cooldown": false})).into_response()
}

#[derive(Deserialize)]
pub struct AccountUpsertBody {
    pub id: String,
    pub access_token: String,
    #[serde(default)]
    pub machine_id: String,
    #[serde(default)]
    pub refresh_token: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub proxy_id: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
}

fn default_true() -> bool {
    true
}

/// POST /admin/api/accounts — 新增或覆盖账号 (热替换号池)
pub async fn api_account_upsert(
    State(state): State<Arc<AppState>>,
    Json(body): Json<AccountUpsertBody>,
) -> Response {
    let acc = Account {
        id: body.id.trim().to_string(),
        access_token: body.access_token.trim().to_string(),
        machine_id: body.machine_id.trim().to_string(),
        refresh_token: body.refresh_token,
        enabled: body.enabled,
        token_expires_at: None,
        refresh_url: None,
        proxy_id: body.proxy_id.filter(|s| !s.trim().is_empty()),
        tags: body.tags,
    };
    let existed = state.pool.has_account(&acc.id);
    match config::upsert_account(acc) {
        Ok(accounts) => {
            state.pool.replace_accounts(accounts);
            state.audit.account_op(
                if existed { "update" } else { "create" },
                &body.id,
                json!({}),
            );
            Json(json!({"status": "ok", "pool": state.pool.stats()})).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// DELETE /admin/api/accounts/:id
pub async fn api_account_delete(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match config::delete_account(&id) {
        Ok(accounts) => {
            state.pool.replace_accounts(accounts);
            state.audit.account_op("delete", &id, json!({}));
            Json(json!({"status": "ok", "id": id, "pool": state.pool.stats()})).into_response()
        }
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": e.to_string(), "id": id})),
        )
            .into_response(),
    }
}

/// POST /admin/api/accounts/:id/probe — 探测 Cursor Sand/Period 额度
pub async fn api_account_probe(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    let Some(acc) = state.pool.get_account(&id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "account not found", "id": id})),
        )
            .into_response();
    };
    let snap = state
        .cursor_factory
        .resolve_for(&state.proxies, &acc)
        .map(|(c, _)| c)
        .unwrap_or_else(|_| state.cursor.clone())
        .probe_quota(&acc.access_token, &acc.machine_id)
        .await;
    state.pool.set_quota(&id, snap.clone());
    Json(json!({"status": "ok", "id": id, "quota": snap})).into_response()
}

/// POST /admin/api/accounts/probe-all
pub async fn api_account_probe_all(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    use futures_util::stream::{self, StreamExt};
    let ids: Vec<String> = state
        .pool
        .account_rows()
        .into_iter()
        .filter_map(|row| row["id"].as_str().map(|s| s.to_string()))
        .filter(|id| !id.is_empty())
        .collect();
    let out: Vec<serde_json::Value> = stream::iter(ids)
        .map(|id| {
            let state = state.clone();
            async move {
                let Some(acc) = state.pool.get_account(&id) else {
                    return json!({"id": id, "error": "not found"});
                };
                let snap = state
                    .cursor_factory
                    .resolve_for(&state.proxies, &acc)
                    .map(|(c, _)| c)
                    .unwrap_or_else(|_| state.cursor.clone())
                    .probe_quota(&acc.access_token, &acc.machine_id)
                    .await;
                state.pool.set_quota(&id, snap.clone());
                json!({"id": id, "quota": snap})
            }
        })
        .buffer_unordered(50)
        .collect()
        .await;
    Json(json!({"status": "ok", "probed": out.len(), "results": out}))
}

/// POST /admin/api/accounts/import — 批量导入账号（JSON 数组或 CSV）
pub async fn api_accounts_import(
    State(state): State<Arc<AppState>>,
    body: String,
) -> Response {
    // 尝试解析为 JSON 数组
    let accounts: Vec<Account> = match serde_json::from_str::<Vec<Account>>(&body) {
        Ok(accs) => accs,
        Err(_) => {
            // 尝试 CSV 格式，自动检测列名
            let mut lines = body.lines();
            let header = match lines.next() {
                Some(h) => h.to_lowercase(),
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error": "empty CSV"})),
                    )
                        .into_response();
                }
            };
            let cols: Vec<&str> = header.split(',').map(|s| s.trim()).collect();

            // 检测列位置
            let id_idx = cols.iter().position(|c| *c == "id" || *c == "account_id");
            let token_idx = cols.iter().position(|c| *c == "access_token" || *c == "token");
            let mid_idx = cols.iter().position(|c| *c == "machine_id" || *c == "machine");
            let ref_idx = cols.iter().position(|c| *c == "refresh_token" || *c == "refresh");
            let enabled_idx = cols.iter().position(|c| *c == "enabled" || *c == "disabled");

            // 如果没有检测到列名，回退到默认顺序: id,access_token,machine_id,refresh_token,enabled
            let (id_idx, token_idx, mid_idx, ref_idx, enabled_idx) = match (id_idx, token_idx, mid_idx) {
                (Some(i), Some(t), Some(m)) => (i, t, m, ref_idx, enabled_idx),
                _ => (0, 1, 2, Some(3), Some(4)),
            };

            let mut accs = Vec::new();
            for line in lines {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let parts: Vec<&str> = line.split(',').collect();
                if parts.len() <= id_idx.max(token_idx).max(mid_idx) {
                    continue;
                }

                let id = parts.get(id_idx).map(|s| s.trim().to_string()).unwrap_or_default();
                let access_token = parts.get(token_idx).map(|s| s.trim().to_string()).unwrap_or_default();
                let machine_id = parts.get(mid_idx).map(|s| s.trim().to_string()).unwrap_or_default();
                let refresh_token = ref_idx.and_then(|i| parts.get(i)).map(|s| s.trim().to_string()).unwrap_or_default();

                // enabled 列处理: 支持 "true"/"false" 或 "disabled" 列（true 表示禁用）
                let enabled = match enabled_idx.and_then(|i| parts.get(i)) {
                    Some(s) => {
                        let s = s.trim().to_lowercase();
                        if s == "true" || s == "1" || s == "yes" {
                            // 如果列名是 disabled，则 true 表示禁用
                            if cols.get(enabled_idx.unwrap()) == Some(&"disabled") {
                                false
                            } else {
                                true
                            }
                        } else if s == "false" || s == "0" || s == "no" {
                            if cols.get(enabled_idx.unwrap()) == Some(&"disabled") {
                                true
                            } else {
                                false
                            }
                        } else {
                            true // 默认启用
                        }
                    }
                    None => true,
                };

                if !id.is_empty() && !access_token.is_empty() {
                    accs.push(Account {
                        id,
                        access_token,
                        machine_id,
                        refresh_token,
                        enabled,
                        token_expires_at: None,
                        refresh_url: None,
                        proxy_id: None,
                        tags: Vec::new(),
                    });
                }
            }
            if accs.is_empty() {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "invalid format, expected JSON array or CSV with header"})),
                )
                    .into_response();
            }
            accs
        }
    };

    let mut imported = 0;
    let mut errors = Vec::new();
    for acc in accounts {
        let acc_id = acc.id.clone();
        if acc.id.is_empty() || acc.access_token.is_empty() {
            errors.push(json!({"id": acc_id, "error": "id and access_token required"}));
            continue;
        }
        match config::upsert_account(acc) {
            Ok(accounts) => {
                state.pool.replace_accounts(accounts);
                imported += 1;
            }
            Err(e) => {
                errors.push(json!({"id": acc_id, "error": e.to_string()}));
            }
        }
    }

    state.audit.account_op("import", "batch", json!({"imported": imported, "errors": errors.len()}));
    Json(json!({
        "status": "ok",
        "imported": imported,
        "errors": errors,
        "pool": state.pool.stats(),
    }))
    .into_response()
}

/// GET /admin/api/accounts/export — 导出账号备份
pub async fn api_accounts_export(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let rows = state.pool.account_rows();
    Json(json!({
        "exported_at": chrono::Utc::now().to_rfc3339(),
        "count": rows.len(),
        "accounts": rows,
    }))
}

/// GET /admin/api/pool/health — 健康度仪表盘
pub async fn api_pool_health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let scores = state.pool.all_health_scores();
    let total = scores.len();
    let healthy = scores.iter().filter(|s| s["health"]["score"].as_u64().unwrap_or(0) >= 80).count();
    let degraded = scores.iter().filter(|s| {
        let score = s["health"]["score"].as_u64().unwrap_or(0);
        score >= 50 && score < 80
    }).count();
    let unhealthy = scores.iter().filter(|s| s["health"]["score"].as_u64().unwrap_or(0) < 50).count();
    
    Json(json!({
        "total": total,
        "healthy": healthy,
        "degraded": degraded,
        "unhealthy": unhealthy,
        "accounts": scores,
    }))
}

#[derive(Deserialize)]
pub struct BatchOpBody {
    pub ids: Vec<String>,
    pub op: String, // "enable", "disable", "delete", "clear_cooldown"
}

/// POST /admin/api/accounts/batch — 批量操作
pub async fn api_accounts_batch(
    State(state): State<Arc<AppState>>,
    Json(body): Json<BatchOpBody>,
) -> Response {
    let mut results = Vec::new();
    
    for id in &body.ids {
        let result = match body.op.as_str() {
            "enable" => {
                state.pool.set_enabled(id, true);
                json!({"id": id, "status": "enabled"})
            }
            "disable" => {
                state.pool.set_enabled(id, false);
                json!({"id": id, "status": "disabled"})
            }
            "clear_cooldown" => {
                let ok = state.pool.clear_cooldown(id);
                json!({"id": id, "status": if ok { "cooldown_cleared" } else { "not_found" }})
            }
            "delete" => {
                match config::delete_account(id) {
                    Ok(accounts) => {
                        state.pool.replace_accounts(accounts);
                        json!({"id": id, "status": "deleted"})
                    }
                    Err(e) => json!({"id": id, "error": e.to_string()}),
                }
            }
            _ => json!({"id": id, "error": "unknown op"}),
        };
        results.push(result);
    }
    
    state.audit.account_op("batch", "batch", json!({"op": body.op, "count": body.ids.len()}));
    Json(json!({"status": "ok", "results": results})).into_response()
}
