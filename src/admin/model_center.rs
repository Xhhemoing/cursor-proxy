//! 模型中心 admin API: 家族树聚合 + 家族规则 CRUD + 路由预览.
//!
//! 一个端点喂「模型中心」页 5 个 tab 的共享数据:
//! - `GET  /admin/api/models/families`         家族树 (变体/菜单形态/默认档/生效价/24h·7d·30d 统计/风控/组)
//! - `POST /admin/api/models/family-rule`      upsert 家族规则 {family, default_tier, menu, note}
//! - `DELETE /admin/api/models/family-rule/:f` 删除家族规则 (回落 Auto/BaseAndFast)
//! - `GET  /admin/api/models/route-preview?model=x[&thinking_level=][&reasoning_effort=][&max_mode=]`
//!                                             路由预览: 会发给 Cursor 的确切名字 + 价格 + 风控 + 闸门
//!
//! 统计口径 (2026-09-08 重写):
//! - 数据源只有账本 billing_records, 经 `analytics::load_reqs` 归一化 (face 由 token×官方价现算,
//!   不再用任何 SQL 里不存在的列 —— 旧 analytics_price_map 的 `SUM(face_usd)` 是死代码).
//! - 家族聚合 = 该家族全部上游真变体 ∪ 家族基名 ∪ 网关别名 的请求并集, 槽·时按
//!   `analytics::aggregate` 的 key 分流 sessionize 重算 (不是 SUM(latency)).
//! - 三档时间窗 (24h/7d/30d) 同一次扫描切出, 保证四张表数字一致.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::analytics as an;
use crate::cards;
use crate::models::{
    console_key, registry, DefaultTier, FamilyRule, MenuMode, ModelRegistry, GATEWAY_ALIASES,
};
use crate::AppState;

fn bad(msg: impl Into<String>) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({"error": msg.into()}))).into_response()
}

// ── 家族树 ─────────────────────────────────────────────────────────────

/// 一个时间窗的家族统计 (从 Agg 提取, 字段名与前端表格列对齐)
fn win_json(a: Option<&an::Agg>) -> Value {
    match a {
        Some(a) if a.requests > 0 => json!({
            "requests": a.requests,
            "errors": a.errors,
            "error_rate": if a.requests > 0 { a.errors as f64 / a.requests as f64 } else { 0.0 },
            "face_usd": a.face_usd,
            "lane_hours": a.lane_hours,
            "usd_per_lane_hour": a.usd_per_lane_hour(),
            "output_tokens": a.output_tokens,
        }),
        _ => json!({"requests": 0, "errors": 0, "error_rate": 0.0, "face_usd": 0.0,
                    "lane_hours": 0.0, "usd_per_lane_hour": null, "output_tokens": 0}),
    }
}

/// 家族成员判定: 真变体 (family_base == fam) ∪ 基名本身 ∪ 裸 grok ↔ cursor-grok 互认
fn family_member(fam: &str, model: &str) -> bool {
    if model == fam {
        return true;
    }
    if ModelRegistry::family_base(model) == fam {
        return true;
    }
    // cursor-grok-4.6-high 属于 grok-4.6 家族 (面板按客户惯用名展示)
    if let Some(stripped) = fam.strip_prefix("cursor-") {
        if ModelRegistry::family_base(model) == stripped {
            return true;
        }
    }
    if let Some(pref) = model.strip_prefix("cursor-") {
        if ModelRegistry::family_base(pref) == fam {
            return true;
        }
    }
    false
}

pub async fn api_models_families(State(state): State<Arc<AppState>>) -> Response {
    let reg = registry();
    let upstream = state.card_store.upstream_names();
    let rmb = state.card_store.cost_model().rmb_per_usd();
    let policy = state.card_store.risk_policy();

    // 1. 家族全集 = 上游名的 family_base ∪ 注册表手动条目的 family_base ∪ 账本 seen 的 family_base
    //    ∪ 网关别名 (kimi-k3 单列一族, 客户认知入口).
    //    cursor-XXX 与裸 XXX 合并为一族 (客户惯用名优先: 上游只有 cursor-grok-4.6-* 时,
    //    面板展示 grok-4.6 而不是 cursor-grok-4.6).
    let seen: Vec<String> = crate::admin::seen_models(&state)
        .into_iter()
        .map(|(m, _)| m)
        .collect();
    let upstream_bases: Vec<String> = {
        let mut v: Vec<String> = upstream
            .iter()
            .map(|m| ModelRegistry::family_base(m).to_string())
            .collect();
        v.sort();
        v.dedup();
        v
    };
    let canon = |b: &str| -> String {
        // 归一到客户惯用名: cursor-X 且裸 X 有内置价目 → X; 否则保留原名
        if let Some(stripped) = b.strip_prefix("cursor-") {
            if crate::cards::builtin_model_known(stripped) {
                return stripped.to_string();
            }
        }
        b.to_string()
    };
    let mut fams: Vec<String> = vec![];
    let mut push_fam = |f: String| {
        if !f.is_empty() && !fams.iter().any(|x| *x == f) {
            fams.push(f);
        }
    };
    for b in &upstream_bases {
        push_fam(canon(b));
    }
    for m in seen.iter() {
        push_fam(canon(ModelRegistry::family_base(m)));
    }
    for e in &reg.snapshot().models {
        push_fam(canon(ModelRegistry::family_base(&e.model)));
    }
    for a in GATEWAY_ALIASES {
        push_fam((*a).to_string());
    }
    fams.sort();

    // 2. 账本一次扫描, 按家族切三窗聚合 (face 由 token×官方价现算, 与消耗分析同口径)
    let now = chrono::Utc::now().timestamp_millis();
    let since_30d = now - 30 * 86_400_000;
    let reqs: Vec<an::Req> = match state.ledger.reader() {
        Ok(conn) => an::load_reqs(&conn, Some(since_30d), None, false, None, 2_000_000)
            .unwrap_or_default(),
        Err(_) => vec![],
    };
    let fam_of = |r: &an::Req| -> String {
        let b = ModelRegistry::family_base(&r.model);
        // 裸 grok-4.6 账本行并入 cursor-grok-4.6 家族 (若上游只有 cursor- 前缀);
        // 反向也并: cursor-grok-4.6-high 的账本行并入 grok-4.6 (若该家族在名单).
        if !fams.iter().any(|f| f == b) {
            let cb = format!("cursor-{b}");
            if fams.iter().any(|f| *f == cb) {
                return cb;
            }
        }
        if let Some(stripped) = b.strip_prefix("cursor-") {
            if fams.iter().any(|f| f == stripped) {
                return stripped.to_string();
            }
        }
        b.to_string()
    };
    let agg_30d = an::aggregate(&reqs, |r| vec![fam_of(r)], None);
    let cut_7d = now - 7 * 86_400_000;
    let cut_24h = now - 86_400_000;
    let reqs_7d: Vec<an::Req> = reqs.iter().filter(|r| r.start_ms >= cut_7d).cloned().collect();
    let reqs_24h: Vec<an::Req> = reqs.iter().filter(|r| r.start_ms >= cut_24h).cloned().collect();
    let agg_7d = an::aggregate(&reqs_7d, |r| vec![fam_of(r)], None);
    let agg_24h = an::aggregate(&reqs_24h, |r| vec![fam_of(r)], None);

    // 3. 客户菜单 (visible_models 同源, 菜单 tab 与家族行共用)
    let visible = reg.visible_models(&upstream, &seen);

    // 4. 组装每个家族
    let snap = reg.snapshot();
    let rows: Vec<Value> = fams
        .iter()
        .map(|fam| {
            let variants: Vec<&String> = upstream
                .iter()
                .filter(|u| family_member(fam, u))
                .collect();
            let rule = snap.family_rules.iter().find(|r| &r.family == fam);
            let tier = rule.map(|r| r.default_tier).unwrap_or_default();
            let menu = rule.map(|r| r.menu).unwrap_or_default();
            // 生效价: 注册表手动 > 内置; fast 行单独报 (×2 规则在 model_price 内)
            let (pi, po, pcr, pcw) = cards::model_price(fam);
            let manual = reg.get_exact(fam);
            let price_source = if manual.is_some() {
                "manual"
            } else if cards::model_price_known(fam) {
                "builtin"
            } else {
                "fallback"
            };
            // 风控规则 (最长前缀命中, 与 rule_for 同口径)
            let pace = policy.rule_for(fam);
            // 组
            let groups = reg.groups_of(fam);
            // 该家族在客户菜单里实际暴露的 id
            let client_ids: Vec<&String> = visible
                .iter()
                .filter(|v| ModelRegistry::family_base(v) == fam || v.as_str() == fam)
                .collect();
            let upstream_missing = !upstream.is_empty()
                && !ModelRegistry::upstream_present(fam, &upstream)
                // cursor-<fam> 在上游 = 存在 (裸名是客户惯用名, 路由会加回前缀)
                && !ModelRegistry::upstream_present(&format!("cursor-{fam}"), &upstream)
                && !GATEWAY_ALIASES.contains(&fam.as_str());
            let a30 = agg_30d.get(fam);
            json!({
                "family": fam,
                "variants": variants,
                "variant_count": variants.len(),
                "default_tier": tier,
                "menu": menu,
                "rule_note": rule.map(|r| r.note.clone()).unwrap_or_default(),
                "price": {"input": pi, "output": po, "cache_read": pcr, "cache_write": pcw,
                          "source": price_source, "console_key": console_key(fam)},
                "pace_rule": pace,
                "groups": groups,
                "client_ids": client_ids,
                "upstream_missing": upstream_missing,
                "enabled": manual.as_ref().map(|e| e.enabled).unwrap_or(true),
                "hidden": manual.as_ref().map(|e| e.hidden).unwrap_or(false),
                "known": manual.as_ref().map(|e| e.known).unwrap_or(false),
                "w24h": win_json(agg_24h.get(fam)),
                "w7d": win_json(agg_7d.get(fam)),
                "w30d": win_json(a30),
            })
        })
        .collect();

    Json(json!({
        "families": rows,
        "rmb_per_usd": rmb,
        "upstream_count": upstream.len(),
        "menu": visible,
    }))
    .into_response()
}

// ── 家族规则 CRUD ───────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct FamilyRuleBody {
    pub family: String,
    #[serde(default)]
    pub default_tier: Option<String>,
    #[serde(default)]
    pub menu: Option<String>,
    #[serde(default)]
    pub note: Option<String>,
}

fn parse_tier(s: &str) -> Result<DefaultTier, String> {
    match s {
        "auto" | "" => Ok(DefaultTier::Auto),
        "low" => Ok(DefaultTier::Low),
        "medium" => Ok(DefaultTier::Medium),
        "high" => Ok(DefaultTier::High),
        "max" => Ok(DefaultTier::Max),
        other => Err(format!("unknown default_tier '{other}' (auto|low|medium|high|max)")),
    }
}

fn parse_menu(s: &str) -> Result<MenuMode, String> {
    match s {
        "base_and_fast" | "" => Ok(MenuMode::BaseAndFast),
        "base" => Ok(MenuMode::Base),
        "all" => Ok(MenuMode::All),
        "hidden" => Ok(MenuMode::Hidden),
        other => Err(format!("unknown menu '{other}' (base_and_fast|base|all|hidden)")),
    }
}

pub async fn api_models_family_rule_set(
    State(state): State<Arc<AppState>>,
    Json(b): Json<FamilyRuleBody>,
) -> Response {
    let family = b.family.trim().to_string();
    if family.is_empty() {
        return bad("family required");
    }
    let reg = registry();
    let cur = reg.family_rule(&family).unwrap_or(FamilyRule {
        family: family.clone(),
        ..Default::default()
    });
    let rule = FamilyRule {
        family: family.clone(),
        default_tier: match b.default_tier.as_deref() {
            Some(s) => match parse_tier(s) {
                Ok(t) => t,
                Err(e) => return bad(e),
            },
            None => cur.default_tier,
        },
        menu: match b.menu.as_deref() {
            Some(s) => match parse_menu(s) {
                Ok(m) => m,
                Err(e) => return bad(e),
            },
            None => cur.menu,
        },
        note: b.note.unwrap_or(cur.note),
    };
    if let Err(e) = reg.upsert_family_rule(rule) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response();
    }
    state
        .audit
        .key_op("family_rule_set", &family, json!({}));
    Json(json!({"ok": true, "family": family})).into_response()
}

pub async fn api_models_family_rule_delete(
    State(state): State<Arc<AppState>>,
    Path(family): Path<String>,
) -> Response {
    let removed = registry().delete_family_rule(&family).unwrap_or(false);
    state.audit.key_op("family_rule_del", &family, json!({}));
    Json(json!({"ok": true, "removed": removed})).into_response()
}

// ── 路由预览 ────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct RoutePreviewQuery {
    pub model: Option<String>,
    pub thinking_level: Option<String>,
    pub reasoning_effort: Option<String>,
    pub max_mode: Option<bool>,
}

pub async fn api_models_route_preview(
    State(state): State<Arc<AppState>>,
    Query(q): Query<RoutePreviewQuery>,
) -> Response {
    let model = q.model.clone().unwrap_or_default();
    if model.is_empty() {
        return bad("model required");
    }
    let upstream = state.card_store.upstream_names();
    let reg = registry();
    // 构造一个合成 body 复用生产路由链 (显式档 > 家族默认 > 价格 > 启发式)
    let mut body = json!({"model": model, "messages": []});
    if let Some(t) = &q.thinking_level {
        body["thinking_level"] = json!(t);
    }
    if let Some(e) = &q.reasoning_effort {
        body["reasoning_effort"] = json!(e);
    }
    if let Some(m) = q.max_mode {
        body["max_mode"] = json!(m);
    }
    let price_map = state.analytics_price_map();
    let resolved = ModelRegistry::resolve_smart_model_with_price(
        &model,
        &body,
        &upstream,
        price_map.as_ref(),
    );
    let final_model = resolved.clone().unwrap_or_else(|| model.clone());
    let known = reg.model_known(&model, &upstream);
    let (pi, po, pcr, pcw) = cards::model_price(&final_model);
    let policy = state.card_store.risk_policy();
    let pace = policy.rule_for(&final_model);
    let fam = ModelRegistry::family_base(&model);
    let rule = reg.family_rule(fam);
    Json(json!({
        "input": model,
        "resolved": resolved,
        "final_model": final_model,
        "known": known,
        "gate": if known { "pass" } else { "404 unknown_model" },
        "family": fam,
        "family_rule": rule,
        "price": {"input": pi, "output": po, "cache_read": pcr, "cache_write": pcw},
        "pace_rule": pace,
        "upstream_exact": upstream.iter().any(|u| *u == model),
    }))
    .into_response()
}
