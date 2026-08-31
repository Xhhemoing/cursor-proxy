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
            // RPM
            acc["rpm"] = json!(state.metrics.account_rpm(&id));
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

/// POST /admin/api/accounts/import — 批量导入账号（JSON 数组 / CSV / 纯文本粘贴）
/// 支持格式:
///   JSON: [{"id":"...","access_token":"...","machine_id":"...",...}]
///   CSV:  自动检测列名 (id/name/email/access_token/refresh_token/machine_id/account_id/max_concurrency/disabled/enabled)
///   纯文本: 每行 id,access_token,machine_id[,refresh_token]
pub async fn api_accounts_import(
    State(state): State<Arc<AppState>>,
    body: String,
) -> Response {
    let body = body.trim();
    if body.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "empty body"})),
        )
            .into_response();
    }

    // 1. 尝试 JSON 数组
    if let Ok(accs) = serde_json::from_str::<Vec<Account>>(body) {
        return do_import_accounts(state, accs).await;
    }

    // 2. 尝试 CSV (带表头自动检测)
    let mut lines = body.lines();
    let first_line = lines.next().unwrap_or("");
    let header_lower = first_line.to_lowercase();
    let cols: Vec<&str> = header_lower.split(',').map(|s| s.trim()).collect();

    // 检测是否为 CSV 表头 (包含至少一个已知列名)
    let known_cols = ["id", "name", "email", "access_token", "token", "refresh_token", "refresh",
        "machine_id", "machine", "account_id", "max_concurrency", "disabled", "enabled"];
    let is_csv_header = cols.iter().any(|c| known_cols.contains(c));

    if is_csv_header {
        // 列名 -> 索引映射
        let col_idx = |names: &[&str]| -> Option<usize> {
            cols.iter().position(|c| names.contains(c))
        };

        let id_idx = col_idx(&["id", "account_id"]);
        let name_idx = col_idx(&["name"]);
        let token_idx = col_idx(&["access_token", "token"]);
        let mid_idx = col_idx(&["machine_id", "machine"]);
        let ref_idx = col_idx(&["refresh_token", "refresh"]);
        let enabled_idx = col_idx(&["enabled"]);
        let disabled_idx = col_idx(&["disabled"]);

        // id 和 access_token 必须存在
        let (Some(id_i), Some(token_i)) = (id_idx, token_idx) else {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "CSV header must contain 'id' and 'access_token' columns"})),
            )
                .into_response();
        };

        let mut accs = Vec::new();
        for line in lines {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split(',').collect();
            let get = |idx: Option<usize>| -> String {
                idx.and_then(|i| parts.get(i)).map(|s| s.trim().to_string()).unwrap_or_default()
            };

            let id = get(Some(id_i));
            let access_token = get(Some(token_i));
            let machine_id = get(mid_idx);
            let refresh_token = get(ref_idx);

            // enabled/disabled 逻辑: disabled=true → enabled=false; enabled=false → enabled=false
            let enabled = if let Some(di) = disabled_idx {
                let d = get(Some(di)).to_lowercase();
                !(d == "true" || d == "1" || d == "yes")
            } else if let Some(ei) = enabled_idx {
                let e = get(Some(ei)).to_lowercase();
                e != "false" && e != "0" && e != "no"
            } else {
                true
            };

            // 如果 id 为空但有 name，用 name 作为 id
            let id = if id.is_empty() { get(name_idx) } else { id };

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
                Json(json!({"error": "no valid accounts found in CSV"})),
            )
                .into_response();
        }
        return do_import_accounts(state, accs).await;
    }

    // 3. 纯文本粘贴: 每行 id,access_token,machine_id[,refresh_token]
    let mut accs = Vec::new();
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() < 3 {
            continue;
        }
        let id = parts[0].trim().to_string();
        let access_token = parts[1].trim().to_string();
        let machine_id = parts[2].trim().to_string();
        let refresh_token = parts.get(3).map(|s| s.trim().to_string()).unwrap_or_default();
        if !id.is_empty() && !access_token.is_empty() {
            accs.push(Account {
                id,
                access_token,
                machine_id,
                refresh_token,
                enabled: true,
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
            Json(json!({"error": "invalid format, expected JSON array, CSV with header, or lines of id,access_token,machine_id"})),
        )
            .into_response();
    }
    do_import_accounts(state, accs).await
}

async fn do_import_accounts(state: Arc<AppState>, accounts: Vec<Account>) -> Response {
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

#[derive(Deserialize)]
pub struct AccountsBatchEditBody {
    pub ids: Vec<String>,
    /// 要修改的字段 (None = 不改)
    pub enabled: Option<bool>,
    pub proxy_id: Option<Option<String>>,
    pub tags: Option<Vec<String>>,
}

/// POST /admin/api/accounts/batch-edit — 批量修改账号字段 (代理/标签/开关)
pub async fn api_accounts_batch_edit(
    State(state): State<Arc<AppState>>,
    Json(body): Json<AccountsBatchEditBody>,
) -> Response {
    let mut updated = 0usize;
    let mut errors = Vec::new();
    for id in &body.ids {
        // 先改内存
        if let Some(en) = body.enabled {
            if state.pool.set_enabled(id, en).is_none() {
                errors.push(json!({"id": id, "error": "not found"}));
                continue;
            }
        }
        // 持久化到 accounts.json
        let mut accounts = config::load_accounts().unwrap_or_default();
        let Some(acc) = accounts.iter_mut().find(|a| a.id == *id) else {
            errors.push(json!({"id": id, "error": "not found in config"}));
            continue;
        };
        if let Some(en) = body.enabled { acc.enabled = en; }
        if let Some(ref pid) = body.proxy_id { acc.proxy_id = pid.clone().filter(|s| !s.is_empty()); }
        if let Some(ref t) = body.tags { acc.tags = t.clone(); }
        match config::save_accounts(&accounts) {
            Ok(_) => {
                state.pool.replace_accounts(accounts);
                updated += 1;
            }
            Err(e) => {
                errors.push(json!({"id": id, "error": e.to_string()}));
            }
        }
    }
    state.pool.rebuild_available_ids();
    state.audit.account_op("batch_edit", "batch", json!({"updated": updated, "ids": body.ids}));
    Json(json!({"status": "ok", "updated": updated, "errors": errors})).into_response()
}
