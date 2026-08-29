//! 账号管理 API.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use crate::config::{self, Account};
use crate::AppState;

/// GET /admin/api/pool — 号池统计
pub async fn api_pool_stats(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.pool.stats())
}

/// GET /admin/api/accounts — 账号列表
pub async fn api_accounts_list(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let stats = state.pool.stats();
    Json(json!({
        "accounts": stats.get("accounts").cloned().unwrap_or(json!([])),
        "total": stats.get("total_accounts").cloned().unwrap_or(json!(0)),
        "available": stats.get("available").cloned().unwrap_or(json!(0)),
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
        .cursor
        .probe_quota(&acc.access_token, &acc.machine_id)
        .await;
    state.pool.set_quota(&id, snap.clone());
    Json(json!({"status": "ok", "id": id, "quota": snap})).into_response()
}

/// POST /admin/api/accounts/probe-all
pub async fn api_account_probe_all(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let rows = state.pool.account_rows();
    let mut out = Vec::new();
    for row in rows {
        let id = row["id"].as_str().unwrap_or("").to_string();
        if id.is_empty() {
            continue;
        }
        if let Some(acc) = state.pool.get_account(&id) {
            let snap = state
                .cursor
                .probe_quota(&acc.access_token, &acc.machine_id)
                .await;
            state.pool.set_quota(&id, snap.clone());
            out.push(json!({"id": id, "quota": snap}));
        }
    }
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
            // 尝试 CSV 格式: id,access_token,machine_id,refresh_token,enabled
            let mut accs = Vec::new();
            for line in body.lines().skip(1) { // 跳过标题行
                let parts: Vec<&str> = line.split(',').collect();
                if parts.len() >= 3 {
                    accs.push(Account {
                        id: parts[0].trim().to_string(),
                        access_token: parts[1].trim().to_string(),
                        machine_id: parts[2].trim().to_string(),
                        refresh_token: parts.get(3).map(|s| s.trim().to_string()).unwrap_or_default(),
                        enabled: parts.get(4).map(|s| s.trim() == "true").unwrap_or(true),
                        token_expires_at: None,
                        refresh_url: None,
                    });
                }
            }
            if accs.is_empty() {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "invalid format, expected JSON array or CSV"})),
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
