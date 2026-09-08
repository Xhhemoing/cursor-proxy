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

use crate::portal::{portal, user_json, KeyMode, PortalUser};
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
pub async fn api_portal_balance(headers: HeaderMap, State(state): State<Arc<AppState>>) -> Response {
    let Some(u) = auth_user(&headers) else { return unauthorized() };
    let log = portal().balance_log(&u.id, 20);
    let (credited, used) = state.card_store.quota_totals(&u.id);
    let quota_log = state.card_store.quota_log(&u.id, 20);
    Json(json!({
        "balance_rmb": u.balance_rmb,
        "log": log,
        "quota": {
            "credited_usd": credited as f64 / 1e6,
            "used_usd": used as f64 / 1e6,
            "remaining_usd": ((credited - used).max(0)) as f64 / 1e6,
        },
        "quota_log": quota_log,
    })).into_response()
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
                "price_per_lane_hour": p.price_per_lane_hour,
                "min_hours": p.min_hours,
                "billing_mode": format!("{:?}", p.billing_mode),
            })
        })
        .collect();
    Json(json!({"plans": plans})).into_response()
}

#[derive(Deserialize)]
pub struct PurchaseBody {
    pub plan_id: String,
    pub stack_mode: Option<String>,
    /// 费率模式: 购买时长 (小时)
    pub hours: Option<u64>,
    /// 费率模式: 并发槽
    pub slots: Option<u32>,
}

/// POST /portal/api/purchase — 从余额扣费: 定额→充钱包, 畅饮→发卡
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

    // ── 定额卡 → 充钱包, 不发卡 ──
    if plan.kind == crate::cards::PlanKind::Quota {
        if u.balance_rmb < plan.price {
            return (StatusCode::PAYMENT_REQUIRED, Json(json!({
                "error": format!("余额不足 (需 ¥{:.2}, 当前 ¥{:.2})", plan.price, u.balance_rmb),
                "balance_rmb": u.balance_rmb,
                "price": plan.price,
            }))).into_response();
        }
        if let Err(e) = portal().add_balance(&u.id, -plan.price, "purchase_quota", Some(plan.id.clone()), "portal") {
            return bad(e);
        }
        match state.card_store.quota_credit(&u.id, plan.face_usd, "purchase", Some(&plan.id), &u.username) {
            Ok(remaining) => {
                return Json(json!({
                    "ok": true,
                    "quota_credited": plan.face_usd,
                    "quota_remaining_usd": remaining,
                    "balance_rmb": portal().get_user(&u.id).map(|x| x.balance_rmb).unwrap_or(0.0),
                })).into_response();
            }
            Err(e) => {
                let _ = portal().add_balance(&u.id, plan.price, "purchase_refund", Some(plan.id.clone()), "system");
                return bad(format!("充值失败已退款: {e}"));
            }
        }
    }

    // ── 费率模式: 时长 × 并发 算价, 发卡不激活 ──
    let (price, hours, slots) = if plan.is_rate_mode() {
        let hours = b.hours.unwrap_or(plan.min_hours).max(plan.min_hours);
        let slots = b.slots.unwrap_or(1).clamp(1, plan.max_concurrency.max(1));
        (plan.rate_price(hours, slots), Some(hours), Some(slots))
    } else {
        (plan.price, None, None)
    };

    // P1: 叠加检查 (同套餐已持有, 按 stack_mode 处理) — 仅固定价模式 (费率模式每次购买独立卡)
    if !plan.is_rate_mode() {
        let existing = state.card_store.list_cards().into_iter().find(|c| {
            c.owner == u.username
                && c.plan_id == plan.id
                && c.enabled
                && !c.is_expired(crate::cards::now_unix())
        });
        if let Some(existing_card) = existing {
            match plan.stack_mode {
                crate::cards::StackMode::TimeMultiply => {
                    let hours = plan.expire_hours.unwrap_or(plan.duration_hours);
                    let new_expires = existing_card.expires_at + hours * 3600;
                    if let Err(e) = state.card_store.update_card(&existing_card.card_key, |c| {
                        c.expires_at = new_expires;
                    }) {
                        return bad(e);
                    }
                    if let Err(e) = portal().add_balance(&u.id, -price, "purchase_extend", Some(plan.id.clone()), "portal") {
                        return bad(e);
                    }
                    return Json(json!({"ok": true, "card_key": existing_card.card_key, "extended": true, "new_expires_at": new_expires})).into_response();
                }
                crate::cards::StackMode::SlotMultiply => {}
            }
        }
    }

    if u.balance_rmb < price {
        return (StatusCode::PAYMENT_REQUIRED, Json(json!({
            "error": format!("余额不足 (需 ¥{:.2}, 当前 ¥{:.2})", price, u.balance_rmb),
            "balance_rmb": u.balance_rmb,
            "price": price,
        }))).into_response();
    }
    if let Err(e) = portal().add_balance(&u.id, -price, "purchase_plan", Some(plan.id.clone()), "portal") {
        return bad(e);
    }
    match state.card_store.issue_card_full(&plan.id, &u.username, Some(price), hours, slots) {
        Ok(card) => {
            if let Err(e) = state.card_store.update_card(&card.card_key, |c| {
                c.user_id = Some(u.id.clone());
            }) {
                tracing::warn!(error = %e, "bind user_id failed (non-fatal)");
            }
            Json(json!({
                "ok": true,
                "card_key": card.card_key,
                "plan": plan.name,
                "price": price,
                "activated": card.activated(),
                "slots": card.slots,
                "duration_hours": card.duration_hours,
            })).into_response()
        }
        Err(e) => {
            let _ = portal().add_balance(&u.id, price, "purchase_refund", Some(plan.id.clone()), "system");
            bad(format!("发卡失败已退款: {e}"))
        }
    }
}

/// GET /portal/api/cards — 我的卡 (owner == username)
pub async fn api_portal_cards(headers: HeaderMap, State(state): State<Arc<AppState>>) -> Response {
    let Some(u) = auth_user(&headers) else { return unauthorized() };
    let now = crate::cards::now_unix();
    let keys = portal().list_keys(&u.id);
    let cards: Vec<Value> = state
        .card_store
        .list_cards()
        .into_iter()
        .filter(|c| c.owner == u.username)
        .map(|c| {
            let plan = state.card_store.get_plan(&c.plan_id);
            let bound_keys: Vec<&str> = keys
                .iter()
                .filter(|k| k.card_key.as_deref() == Some(c.card_key.as_str()))
                .map(|k| k.name.as_str())
                .collect();
            json!({
                "card_key": c.card_key,
                "plan_id": c.plan_id,
                "plan_name": plan.as_ref().map(|p| p.name.clone()).unwrap_or_default(),
                "plan_kind": plan.as_ref().map(|p| format!("{:?}", p.kind)).unwrap_or_default(),
                "plan_hours": crate::cards::card_effective_hours(&c, &plan.clone().unwrap_or_default()),
                "enabled": c.enabled,
                "issued_at": c.issued_at,
                "expires_at": c.expires_at,
                "activated": c.activated(),
                "expired": c.is_expired(now),
                "remaining_secs": c.remaining_secs(now),
                "paid_rmb": c.paid_rmb,
                "face_used_usd": c.face_used_micro as f64 / 1e6,
                "face_usd": plan.as_ref().map(|p| p.face_usd).unwrap_or(0.0),
                "slots": c.slots,
                "duration_hours": c.duration_hours,
                "max_concurrency": plan.as_ref().map(|p| p.max_concurrency).unwrap_or(1),
                "rate_mode": plan.as_ref().map(|p| p.is_rate_mode()).unwrap_or(false),
                "bound_keys": bound_keys,
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

// ── 用户 Key 管理 (uk-) ──

/// GET /portal/api/keys — 我的 key 列表
pub async fn api_portal_keys(headers: HeaderMap, State(state): State<Arc<AppState>>) -> Response {
    let Some(u) = auth_user(&headers) else { return unauthorized() };
    let now = crate::cards::now_unix();
    let keys: Vec<Value> = portal()
        .list_keys(&u.id)
        .iter()
        .map(|k| {
            let card_info = k.card_key.as_ref().and_then(|ck| {
                state.card_store.get_card(ck).map(|c| {
                    let plan = state.card_store.get_plan(&c.plan_id);
                    json!({
                        "card_key": c.card_key,
                        "plan_name": plan.as_ref().map(|p| p.name.clone()).unwrap_or_default(),
                        "activated": c.activated(),
                        "expired": c.is_expired(now),
                        "remaining_secs": c.remaining_secs(now),
                        "enabled": c.enabled,
                    })
                })
            });
            json!({
                "key": k.key,
                "name": k.name,
                "mode": match k.mode { KeyMode::Quota => "quota", KeyMode::Card => "card" },
                "card_key": k.card_key,
                "max_concurrency": k.max_concurrency,
                "enabled": k.enabled,
                "created_at": k.created_at,
                "card": card_info,
            })
        })
        .collect();
    Json(json!({"keys": keys})).into_response()
}

#[derive(Deserialize)]
pub struct CreateKeyBody {
    pub name: Option<String>,
    pub mode: String,
    pub card_key: Option<String>,
    pub max_concurrency: Option<u32>,
}

/// POST /portal/api/keys — 创建 key
pub async fn api_portal_create_key(headers: HeaderMap, State(state): State<Arc<AppState>>, Json(b): Json<CreateKeyBody>) -> Response {
    let Some(u) = auth_user(&headers) else { return unauthorized() };
    let mode = match b.mode.as_str() {
        "quota" => KeyMode::Quota,
        "card" => KeyMode::Card,
        _ => return bad("mode must be quota|card"),
    };
    // card 模式校验卡归属
    if mode == KeyMode::Card {
        let Some(ck) = b.card_key.as_deref() else { return bad("card_key required for card mode") };
        let Some(card) = state.card_store.get_card(ck) else { return bad("卡不存在") };
        if card.owner != u.username {
            return (StatusCode::FORBIDDEN, Json(json!({"error": "不是你的卡"}))).into_response();
        }
        if !card.enabled {
            return bad("卡已禁用");
        }
    }
    match portal().create_key(&u.id, b.name.as_deref().unwrap_or(""), mode, b.card_key.clone(), b.max_concurrency) {
        Ok(k) => Json(json!({"ok": true, "key": k.key})).into_response(),
        Err(e) => bad(e),
    }
}

#[derive(Deserialize)]
pub struct UpdateKeyBody {
    pub name: Option<String>,
    pub mode: Option<String>,
    pub card_key: Option<String>,
    pub max_concurrency: Option<u32>,
    pub enabled: Option<bool>,
}

/// POST /portal/api/keys/:key — 改绑定/改名/并发帽/停启
pub async fn api_portal_update_key(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(token): Path<String>,
    Json(b): Json<UpdateKeyBody>,
) -> Response {
    let Some(u) = auth_user(&headers) else { return unauthorized() };
    // 改 card 模式前先校验卡归属
    if let Some(ck) = b.card_key.as_deref() {
        if !ck.is_empty() {
            let Some(card) = state.card_store.get_card(ck) else { return bad("卡不存在") };
            if card.owner != u.username {
                return (StatusCode::FORBIDDEN, Json(json!({"error": "不是你的卡"}))).into_response();
            }
        }
    }
    let mode = b.mode.as_deref().and_then(|m| match m {
        "quota" => Some(KeyMode::Quota),
        "card" => Some(KeyMode::Card),
        _ => None,
    });
    let card_key = b.card_key.clone();
    match portal().update_key(&u.id, &token, |k| {
        if let Some(n) = &b.name { k.name = n.trim().chars().take(32).collect(); }
        if let Some(m) = mode { k.mode = m; }
        if card_key.is_some() { k.card_key = card_key.filter(|s| !s.trim().is_empty()); }
        if let Some(mc) = b.max_concurrency { k.max_concurrency = mc; }
        if let Some(e) = b.enabled { k.enabled = e; }
    }) {
        Ok(_) => Json(json!({"ok": true})).into_response(),
        Err(e) => bad(e),
    }
}

/// DELETE /portal/api/keys/:key
pub async fn api_portal_delete_key(headers: HeaderMap, Path(token): Path<String>) -> Response {
    let Some(u) = auth_user(&headers) else { return unauthorized() };
    match portal().delete_key(&u.id, &token) {
        Ok(()) => Json(json!({"ok": true})).into_response(),
        Err(e) => bad(e),
    }
}

// ── 畅饮卡操作 (激活/加时/改槽) ──

fn own_card(state: &AppState, u: &PortalUser, card_key: &str) -> Result<crate::cards::Card, Response> {
    let Some(card) = state.card_store.get_card(card_key) else {
        return Err(bad("卡不存在"));
    };
    if card.owner != u.username {
        return Err((StatusCode::FORBIDDEN, Json(json!({"error": "不是你的卡"}))).into_response());
    }
    Ok(card)
}

#[derive(Deserialize)]
pub struct CardOpBody {
    pub card_key: String,
}

/// POST /portal/api/cards/activate — 激活未激活卡
pub async fn api_portal_activate(headers: HeaderMap, State(state): State<Arc<AppState>>, Json(b): Json<CardOpBody>) -> Response {
    let Some(u) = auth_user(&headers) else { return unauthorized() };
    let card = match own_card(&state, &u, &b.card_key) { Ok(c) => c, Err(r) => return r };
    if !card.enabled { return bad("卡已禁用"); }
    match state.card_store.activate_card(&b.card_key) {
        Ok(c) => Json(json!({"ok": true, "expires_at": c.expires_at})).into_response(),
        Err(e) => bad(e),
    }
}

#[derive(Deserialize)]
pub struct CardExtendBody {
    pub card_key: String,
    pub hours: u64,
}

/// POST /portal/api/cards/extend — 加时, 按当前槽×费率扣余额
pub async fn api_portal_extend(headers: HeaderMap, State(state): State<Arc<AppState>>, Json(b): Json<CardExtendBody>) -> Response {
    let Some(u) = auth_user(&headers) else { return unauthorized() };
    if b.hours == 0 || b.hours > 24 * 90 { return bad("时长 1-2160 小时"); }
    let card = match own_card(&state, &u, &b.card_key) { Ok(c) => c, Err(r) => return r };
    if !card.enabled { return bad("卡已禁用"); }
    let Some(plan) = state.card_store.get_plan(&card.plan_id) else { return bad("套餐缺失") };
    let slots = card.effective_slots(&plan);
    // 计费: 费率模式按费率, 固定价模式按 (price / plan_hours) 折算
    let price = if plan.is_rate_mode() {
        plan.rate_price(b.hours, slots)
    } else {
        let plan_hours = crate::cards::card_effective_hours(&card, &plan) as f64;
        ((plan.price / plan_hours.max(1.0)) * b.hours as f64 * 100.0).round() / 100.0
    };
    if u.balance_rmb < price {
        return (StatusCode::PAYMENT_REQUIRED, Json(json!({
            "error": format!("余额不足 (需 ¥{:.2}, 当前 ¥{:.2})", price, u.balance_rmb),
            "balance_rmb": u.balance_rmb, "price": price,
        }))).into_response();
    }
    if let Err(e) = portal().add_balance(&u.id, -price, "card_extend", Some(b.card_key.clone()), "portal") {
        return bad(e);
    }
    match state.card_store.extend_card_hours(&b.card_key, b.hours) {
        Ok(c) => Json(json!({
            "ok": true, "expires_at": c.expires_at, "price": price,
            "balance_rmb": portal().get_user(&u.id).map(|x| x.balance_rmb).unwrap_or(0.0),
        })).into_response(),
        Err(e) => {
            let _ = portal().add_balance(&u.id, price, "card_extend_refund", Some(b.card_key.clone()), "system");
            bad(format!("加时失败已退款: {e}"))
        }
    }
}

#[derive(Deserialize)]
pub struct CardSlotsBody {
    pub card_key: String,
    pub slots: u32,
}

/// POST /portal/api/cards/slots — 增减并发槽. 增槽按剩余时间折算扣余额; 减槽立即生效不退款.
pub async fn api_portal_slots(headers: HeaderMap, State(state): State<Arc<AppState>>, Json(b): Json<CardSlotsBody>) -> Response {
    let Some(u) = auth_user(&headers) else { return unauthorized() };
    let card = match own_card(&state, &u, &b.card_key) { Ok(c) => c, Err(r) => return r };
    if !card.enabled { return bad("卡已禁用"); }
    let Some(plan) = state.card_store.get_plan(&card.plan_id) else { return bad("套餐缺失") };
    if b.slots == 0 || b.slots > plan.max_concurrency.max(1) {
        return bad(format!("槽数 1-{}", plan.max_concurrency.max(1)));
    }
    let now = crate::cards::now_unix();
    let cur = card.effective_slots(&plan);
    let new = b.slots;
    if new == cur {
        return Json(json!({"ok": true, "slots": cur, "price": 0.0})).into_response();
    }
    // 增槽: 费率 × Δ槽 × 剩余小时 (未激活卡按购买时长计)
    let price = if new > cur {
        let remaining_hours = if card.activated() {
            card.remaining_secs(now) as f64 / 3600.0
        } else {
            crate::cards::card_effective_hours(&card, &plan) as f64
        };
        let per_hour = if plan.is_rate_mode() {
            plan.price_per_lane_hour
        } else {
            plan.price / crate::cards::card_effective_hours(&card, &plan) as f64 / plan.max_concurrency.max(1) as f64
        };
        ((per_hour * (new - cur) as f64 * remaining_hours) * 100.0).round() / 100.0
    } else {
        0.0
    };
    if price > 0.0 {
        if u.balance_rmb < price {
            return (StatusCode::PAYMENT_REQUIRED, Json(json!({
                "error": format!("余额不足 (需 ¥{:.2}, 当前 ¥{:.2})", price, u.balance_rmb),
                "balance_rmb": u.balance_rmb, "price": price,
            }))).into_response();
        }
        if let Err(e) = portal().add_balance(&u.id, -price, "card_slots", Some(b.card_key.clone()), "portal") {
            return bad(e);
        }
    }
    match state.card_store.set_card_slots(&b.card_key, Some(new)) {
        Ok(c) => Json(json!({
            "ok": true, "slots": c.effective_slots(&plan), "price": price,
            "balance_rmb": portal().get_user(&u.id).map(|x| x.balance_rmb).unwrap_or(0.0),
        })).into_response(),
        Err(e) => {
            if price > 0.0 {
                let _ = portal().add_balance(&u.id, price, "card_slots_refund", Some(b.card_key.clone()), "system");
            }
            bad(format!("改槽失败已退款: {e}"))
        }
    }
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
pub struct AddQuotaBody {
    pub delta_usd: f64,
    pub reason: Option<String>,
}

/// POST /admin/api/portal/users/:id/quota — 手动调定额 (售后/补偿)
pub async fn api_admin_portal_add_quota(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(b): Json<AddQuotaBody>,
) -> Response {
    if b.delta_usd == 0.0 || !b.delta_usd.is_finite() {
        return bad("金额非法");
    }
    let reason = b.reason.unwrap_or_else(|| "admin_adjust".into());
    match state.card_store.quota_credit(&id, b.delta_usd, "admin_adjust", Some(&reason), "admin") {
        Ok(remaining) => Json(json!({"ok": true, "quota_remaining_usd": remaining})).into_response(),
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
