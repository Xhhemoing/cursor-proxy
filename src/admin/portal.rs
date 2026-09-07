//! 门户 API: /portal/api/* (用户端) + /admin/api/portal/* (后台管理).

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::portal::{portal, user_json, PortalUser};
use crate::AppState;

fn bad(msg: impl Into<String>) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({"error": msg.into()}))).into_response()
}

fn unauthorized() -> Response {
    (StatusCode::UNAUTHORIZED, Json(json!({"error": "未登录或会话过期"}))).into_response()
}

fn forbid() -> Response {
    (StatusCode::FORBIDDEN, Json(json!({"error": "账号已禁用"}))).into_response()
}

/// 从 Authorization: Bearer <token> 解析门户用户
fn auth_user(headers: &HeaderMap) -> Option<PortalUser> {
    let token = headers
        .get("authorization")?
        .to_str().ok()?
        .strip_prefix("Bearer ")?;
    portal().resolve_session(token)
}

// ── 用户端端点 ──

#[derive(Deserialize)]
pub struct LoginBody {
    pub username_or_id: String,
    pub password: String,
}

/// POST /portal/api/login
pub async fn api_portal_login(Json(b): Json<LoginBody>) -> Response {
    let Some(u) = portal().verify_password(&b.username_or_id, &b.password) else {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "用户名或密码错误"}))).into_response();
    };
    if !u.enabled {
        return forbid();
    }
    let token = portal().create_session(&u.id);
    Json(json!({"token": token, "user": user_json(&u)})).into_response()
}

/// POST /portal/api/logout
pub async fn api_portal_logout(headers: HeaderMap) -> Response {
    if let Some(t) = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
    {
        portal().destroy_session(t);
    }
    Json(json!({"ok": true})).into_response()
}

/// GET /portal/api/me
pub async fn api_portal_me(headers: HeaderMap) -> Response {
    let Some(u) = auth_user(&headers) else { return unauthorized() };
    Json(json!({"user": user_json(&u)})).into_response()
}

/// GET /portal/api/balance
pub async fn api_portal_balance(headers: HeaderMap) -> Response {
    let Some(u) = auth_user(&headers) else { return unauthorized() };
    let log = portal().balance_log(&u.id, 20);
    Json(json!({"balance_rmb": u.balance_rmb, "log": log})).into_response()
}

/// GET /portal/api/plans — 可购买套餐 (按用户组白名单过滤)
pub async fn api_portal_plans(headers: HeaderMap, State(state): State<Arc<AppState>>) -> Response {
    let Some(u) = auth_user(&headers) else { return unauthorized() };
    let whitelist = portal().visible_plan_ids(&u);
    let plans: Vec<Value> = state
        .card_store
        .list_plans()
        .into_iter()
        .filter(|p| p.enabled)
        .filter(|p| {
            whitelist
                .as_ref()
                .map_or(true, |w| w.is_empty() || w.contains(&p.id))
        })
        .map(|p| {
            json!({
                "id": p.id,
                "name": p.name,
                "price": p.price,
                "kind": format!("{:?}", p.kind),
                "face_usd": p.face_usd,
                "duration_hours": p.duration_hours,
                "max_concurrency": p.max_concurrency,
                "note": p.note,
            })
        })
        .collect();
    Json(json!({"plans": plans})).into_response()
}

#[derive(Deserialize)]
pub struct PurchaseBody {
    pub plan_id: String,
    pub stack_mode: Option<String>,
}

#[derive(Deserialize)]
pub struct BoostBody {
    pub card_key: String,
    pub slots: u32,
}

/// POST /portal/api/boost — 临时加并发 (按剩余时间折算)
pub async fn api_portal_boost(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(b): Json<BoostBody>,
) -> Response {
    let Some(u) = auth_user(&headers) else { return unauthorized() };
    if b.slots == 0 || b.slots > 4 {
        return bad("槽数 1-4");
    }
    let card = state.card_store.get_card(&b.card_key);
    let Some(card) = card else { return bad("卡不存在") };
    if card.owner != u.username {
        return (StatusCode::FORBIDDEN, Json(json!({"error": "不是你的卡"}))).into_response();
    }
    if !card.enabled || card.is_expired(crate::cards::now_unix()) {
        return bad("卡已过期或禁用");
    }
    let plan = state.card_store.get_plan(&card.plan_id).unwrap_or_default();
    let remaining_secs = card.remaining_secs(crate::cards::now_unix());
    let plan_hours = plan.expire_hours.unwrap_or(plan.duration_hours);
    let ratio = remaining_secs as f64 / (plan_hours * 3600) as f64;
    let price = plan.price * ratio * b.slots as f64;
    if u.balance_rmb < price {
        return (StatusCode::PAYMENT_REQUIRED, Json(json!({
            "error": format!("余额不足 (需 ¥{:.2}, 当前 ¥{:.2})", price, u.balance_rmb),
            "balance_rmb": u.balance_rmb,
            "price": price,
        }))).into_response();
    }
    // 扣费
    if let Err(e) = portal().add_balance(&u.id, -price, "boost", Some(b.card_key.clone()), "portal") {
        return bad(e);
    }
    // 发卡 (同套餐, 独立计时, 并发+slots)
    match state.card_store.issue_card(&plan.id, &u.username) {
        Ok(new_card) => {
            // 绑定 user_id + 标记为 boost 卡
            let _ = state.card_store.update_card(&new_card.card_key, |c| {
                c.user_id = Some(u.id.clone());
                c.parent_card_key = Some(b.card_key.clone());
                // boost 卡时长 = 主卡剩余时间
                c.expires_at = card.expires_at;
            });
            Json(json!({
                "ok": true,
                "card_key": new_card.card_key,
                "slots": b.slots,
                "price": price,
                "balance_rmb": portal().get_user(&u.id).map(|x| x.balance_rmb).unwrap_or(0.0),
            })).into_response()
        }
        Err(e) => {
            let _ = portal().add_balance(&u.id, price, "boost_refund", Some(b.card_key.clone()), "system");
            bad(format!("发卡失败已退款: {e}"))
        }
    }
}

/// POST /portal/api/purchase — 从余额扣费发卡
pub async fn api_portal_purchase(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Json(b): Json<PurchaseBody>,
) -> Response {
    let Some(u) = auth_user(&headers) else { return unauthorized() };
    let Some(plan) = state.card_store.get_plan(&b.plan_id) else {
        return bad("套餐不存在");
    };
    if !plan.enabled {
        return bad("套餐已下架");
    }

    // P1: 子套餐检查 (需已持有主套餐)
    if !plan.sub_plan_ids.is_empty() {
        // 这是子套餐, 检查是否已开通主套餐
        let has_parent = state.card_store.list_cards().iter().any(|c| {
            c.owner == u.username
                && c.enabled
                && !c.is_expired(crate::cards::now_unix())
                && state
                    .card_store
                    .get_plan(&c.plan_id)
                    .map(|p| p.sub_plan_ids.contains(&plan.id))
                    .unwrap_or(false)
        });
        if !has_parent {
            return (StatusCode::FORBIDDEN, Json(json!({"error": "子套餐需先开通主套餐"}))).into_response();
        }
    }

    // 组白名单检查
    if let Some(w) = portal().visible_plan_ids(&u) {
        if !w.is_empty() && !w.contains(&plan.id) {
            return (StatusCode::FORBIDDEN, Json(json!({"error": "该套餐未对你所在的用户组开放"}))).into_response();
        }
    }

    // P1: 叠加检查 (同套餐已持有, 按 stack_mode 处理)
    let existing = state.card_store.list_cards().into_iter().find(|c| {
        c.owner == u.username
            && c.plan_id == plan.id
            && c.enabled
            && !c.is_expired(crate::cards::now_unix())
    });
    if let Some(existing_card) = existing {
        match plan.stack_mode {
            crate::cards::StackMode::TimeMultiply => {
                // 时间叠加: 延长现有卡
                let hours = plan.expire_hours.unwrap_or(plan.duration_hours);
                let new_expires = existing_card.expires_at + hours * 3600;
                if let Err(e) = state.card_store.update_card(&existing_card.card_key, |c| {
                    c.expires_at = new_expires;
                }) {
                    return bad(e);
                }
                // 扣费
                if let Err(e) = portal().add_balance(&u.id, -plan.price, "purchase_extend", Some(plan.id.clone()), "portal") {
                    return bad(e);
                }
                return Json(json!({"ok": true, "card_key": existing_card.card_key, "extended": true, "new_expires_at": new_expires})).into_response();
            }
            crate::cards::StackMode::SlotMultiply => {
                // 槽位叠加: 发新卡 (并发+1)
                // 继续走正常发卡流程
            }
        }
    }

    if u.balance_rmb < plan.price {
        return (StatusCode::PAYMENT_REQUIRED, Json(json!({
            "error": format!("余额不足 (需 ¥{:.2}, 当前 ¥{:.2})", plan.price, u.balance_rmb),
            "balance_rmb": u.balance_rmb,
            "price": plan.price,
        }))).into_response();
    }
    // 扣费
    if let Err(e) = portal().add_balance(&u.id, -plan.price, "purchase_plan", Some(plan.id.clone()), "portal") {
        return bad(e);
    }
    // 发卡 (复用现有 issue_card)
    match state.card_store.issue_card(&plan.id, &u.username) {
        Ok(card) => {
            // P1: 绑定 user_id
            if let Err(e) = state.card_store.update_card(&card.card_key, |c| {
                c.user_id = Some(u.id.clone());
            }) {
                tracing::warn!(error = %e, "bind user_id failed (non-fatal)");
            }
            Json(json!({"ok": true, "card_key": card.card_key, "plan": plan.name})).into_response()
        }
        Err(e) => {
            // 发卡失败退款
            let _ = portal().add_balance(&u.id, plan.price, "purchase_refund", Some(plan.id.clone()), "system");
            bad(format!("发卡失败已退款: {e}"))
        }
    }
}

/// GET /portal/api/cards — 我的卡 (owner == username)
pub async fn api_portal_cards(headers: HeaderMap, State(state): State<Arc<AppState>>) -> Response {
    let Some(u) = auth_user(&headers) else { return unauthorized() };
    let now = crate::cards::now_unix();
    let cards: Vec<Value> = state
        .card_store
        .list_cards()
        .into_iter()
        .filter(|c| c.owner == u.username)
        .map(|c| {
            let plan = state.card_store.get_plan(&c.plan_id);
            json!({
                "card_key": c.card_key,
                "plan_id": c.plan_id,
                "plan_name": plan.as_ref().map(|p| p.name.clone()).unwrap_or_default(),
                "plan_hours": plan.as_ref().map(|p| p.expire_hours.unwrap_or(p.duration_hours)).unwrap_or(24),
                "enabled": c.enabled,
                "issued_at": c.issued_at,
                "expires_at": c.expires_at,
                "expired": c.expires_at > 0 && c.expires_at < now,
                "remaining_secs": if c.expires_at > 0 { c.expires_at.saturating_sub(now) } else { 0 },
                "paid_rmb": c.paid_rmb,
                "face_used_usd": c.face_used_micro as f64 / 1e6,
                "face_usd": plan.as_ref().map(|p| p.face_usd).unwrap_or(0.0),
            })
        })
        .collect();
    Json(json!({"cards": cards})).into_response()
}

/// GET /portal/api/aff — 邀请码 + 统计
pub async fn api_portal_aff(headers: HeaderMap) -> Response {
    let Some(u) = auth_user(&headers) else { return unauthorized() };
    let all = portal().list_users();
    let invited: Vec<Value> = all
        .iter()
        .filter(|x| x.invited_by.as_deref() == Some(&u.aff_code))
        .map(|x| json!({"username": x.username, "created_at": x.created_at}))
        .collect();
    Json(json!({
        "aff_code": u.aff_code,
        "invited_count": invited.len(),
        "invited": invited,
        "reward_total_rmb": 0.0,  // P1: 返佣流水
    })).into_response()
}

// ── 后台管理端点 (复用 admin token 鉴权, 路由挂 /admin/api/portal/*) ──

#[derive(Deserialize)]
pub struct CreateUserBody {
    pub username: String,
    pub password: String,
    pub group_id: Option<String>,
    pub initial_balance: Option<f64>,
    pub invited_by: Option<String>,
    pub note: Option<String>,
}

/// POST /admin/api/portal/users — 创建用户
pub async fn api_admin_portal_create_user(Json(b): Json<CreateUserBody>) -> Response {
    match portal().create_user(
        &b.username,
        &b.password,
        b.group_id,
        b.initial_balance.unwrap_or(0.0),
        b.invited_by,
    ) {
        Ok(u) => {
            if let Some(note) = b.note {
                if !note.is_empty() {
                    let _ = portal().update_user(&u.id, |x| x.note = note);
                }
            }
            Json(json!({"ok": true, "user": user_json(&u)})).into_response()
        }
        Err(e) => bad(e),
    }
}

/// GET /admin/api/portal/users — 用户列表
pub async fn api_admin_portal_list_users() -> Response {
    let users: Vec<Value> = portal().list_users().iter().map(user_json).collect();
    Json(json!({"users": users, "groups": portal().list_groups()})).into_response()
}

#[derive(Deserialize)]
pub struct UpdateUserBody {
    pub enabled: Option<bool>,
    pub group_id: Option<String>,
    pub note: Option<String>,
    pub password: Option<String>,
}

/// POST /admin/api/portal/users/:id — 编辑用户
pub async fn api_admin_portal_update_user(
    Path(id): Path<String>,
    Json(b): Json<UpdateUserBody>,
) -> Response {
    let r = portal().update_user(&id, |u| {
        if let Some(e) = b.enabled {
            u.enabled = e;
        }
        if let Some(g) = &b.group_id {
            u.group_id = if g.is_empty() { None } else { Some(g.clone()) };
        }
        if let Some(n) = &b.note {
            u.note = n.clone();
        }
    });
    match r {
        Ok(mut u) => {
            // 改密码单独走 (要哈希)
            if let Some(pw) = &b.password {
                if let Ok(h) = crate::portal::hash_password(pw) {
                    let _ = portal().update_user(&id, |x| x.password_hash = h);
                    u = portal().get_user(&id).unwrap_or(u);
                }
            }
            Json(json!({"ok": true, "user": user_json(&u)})).into_response()
        }
        Err(e) => bad(e),
    }
}

#[derive(Deserialize)]
pub struct AddBalanceBody {
    pub delta_rmb: f64,
    pub reason: Option<String>,
}

/// POST /admin/api/portal/users/:id/balance — 手动加余额
pub async fn api_admin_portal_add_balance(
    Path(id): Path<String>,
    Json(b): Json<AddBalanceBody>,
) -> Response {
    if b.delta_rmb == 0.0 || !b.delta_rmb.is_finite() {
        return bad("金额非法");
    }
    let reason = b.reason.unwrap_or_else(|| "admin_add".into());
    match portal().add_balance(&id, b.delta_rmb, &reason, None, "admin") {
        Ok(new_balance) => {
            Json(json!({"ok": true, "balance_rmb": new_balance})).into_response()
        }
        Err(e) => bad(e),
    }
}

#[derive(Deserialize)]
pub struct UpsertGroupBody {
    pub id: String,
    pub name: String,
    pub note: Option<String>,
    pub plan_whitelist: Option<Vec<String>>,
}

/// POST /admin/api/portal/groups — 创建/编辑用户组
pub async fn api_admin_portal_upsert_group(Json(b): Json<UpsertGroupBody>) -> Response {
    if b.id.trim().is_empty() {
        return bad("组 id 必填");
    }
    let g = crate::portal::UserGroup {
        id: b.id.trim().to_string(),
        name: b.name.trim().to_string(),
        note: b.note.unwrap_or_default(),
        plan_whitelist: b.plan_whitelist.unwrap_or_default(),
    };
    match portal().upsert_group(g) {
        Ok(()) => Json(json!({"ok": true})).into_response(),
        Err(e) => bad(e),
    }
}
