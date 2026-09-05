//! 套餐卡管理 API.
//!
//! 端点:
//!   GET    /admin/api/cards/plans            套餐列表
//!   POST   /admin/api/cards/plans            新增/更新套餐 (全字段可调: 类型/层级/并发/匀速/评分阈值/额度)
//!   DELETE /admin/api/cards/plans/:id        删除套餐 (有卡在用则拒)
//!   POST   /admin/api/cards/plans/seed       写入预置套餐 (?overwrite=1 覆盖同 id)
//!   GET    /admin/api/cards/presets          查看预置套餐 (不写入)
//!   GET/POST /admin/api/cards/cost-model     成本模型 (号价/周额度/可用周数 → ¥/面值$)
//!   GET    /admin/api/cards                  卡列表 + 实时状态 (档位/匀速/行为评分/余额)
//!   POST   /admin/api/cards/issue            开卡 (可批量, 可覆盖实收价)
//!   GET    /admin/api/cards/report           按卡汇总真实消耗 (billing.db)
//!   GET    /admin/api/cards/profit           利润报表: 按卡/套餐/日 收入-成本-毛利 (?from=&to=&group=card|plan|day)
//!   GET    /admin/api/cards/pricing-table    官方口径价格表 + 模型层级 (调价参考)
//!   POST   /admin/api/cards/:key/extend      续期 (小时)
//!   POST   /admin/api/cards/:key/revoke      吊销
//!   DELETE /admin/api/cards/:key             删除
//!   GET    /admin/api/cards/:key             单卡状态

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::cards::{self, CardPlan, CostModel, PlanKind};
use crate::AppState;

// ── 套餐 ──

pub async fn api_card_plans_list(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let plans = state.card_store.list_plans();
    Json(json!({ "plans": plans }))
}

pub async fn api_card_presets(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(json!({ "presets": CardPlan::presets() }))
}

#[derive(Deserialize)]
pub struct SeedQuery {
    #[serde(default)]
    pub overwrite: Option<u8>,
}

pub async fn api_card_plans_seed(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SeedQuery>,
) -> impl IntoResponse {
    let n = state.card_store.seed_presets(q.overwrite == Some(1));
    Json(json!({ "ok": true, "written": n, "plans": state.card_store.list_plans() }))
}

/// 套餐字段全部可选: 缺省取 CardPlan::default(); 更新已有套餐时缺省字段沿用旧值
#[derive(Deserialize, Default)]
pub struct PlanBody {
    pub id: String,
    pub name: Option<String>,
    pub price: Option<f64>,
    pub kind: Option<PlanKind>,
    pub face_usd: Option<f64>,
    pub tier: Option<String>,
    pub duration_hours: Option<u64>,
    pub max_concurrency: Option<u32>,
    pub rpm_limit: Option<u32>,
    pub fair_use_rpd: Option<u32>,
    pub degraded_concurrency: Option<u32>,
    pub daily_quota_usd: Option<f64>,
    pub soften_ratio: Option<f64>,
    pub pace_normal_tps: Option<u32>,
    pub pace_soften_tps: Option<u32>,
    pub pace_degraded_tps: Option<u32>,
    pub abuse_score_threshold: Option<u32>,
    pub model_prefixes: Option<Vec<String>>,
    pub note: Option<String>,
    pub enabled: Option<bool>,
}

pub async fn api_card_plan_upsert(
    State(state): State<Arc<AppState>>,
    Json(body): Json<PlanBody>,
) -> Response {
    let id = body.id.trim().to_string();
    if id.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "id required"})),
        )
            .into_response();
    }
    if let Some(t) = body.tier.as_deref() {
        if !t.is_empty()
            && ![
                cards::TIER_ECONOMY,
                cards::TIER_STANDARD,
                cards::TIER_FLAGSHIP,
            ]
            .contains(&t)
        {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "tier must be economy|standard|flagship|\"\""})),
            )
                .into_response();
        }
    }
    let mut plan = state.card_store.get_plan(&id).unwrap_or_else(|| CardPlan {
        id: id.clone(),
        ..CardPlan::default()
    });
    macro_rules! set {
        ($($f:ident),*) => { $( if let Some(v) = body.$f { plan.$f = v; } )* };
    }
    set!(
        name,
        price,
        kind,
        face_usd,
        tier,
        duration_hours,
        max_concurrency,
        rpm_limit,
        fair_use_rpd,
        degraded_concurrency,
        daily_quota_usd,
        soften_ratio,
        pace_normal_tps,
        pace_soften_tps,
        pace_degraded_tps,
        abuse_score_threshold,
        model_prefixes,
        note,
        enabled
    );
    if plan.kind == PlanKind::Quota && plan.face_usd <= 0.0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "quota plan requires face_usd > 0"})),
        )
            .into_response();
    }
    state.card_store.upsert_plan(plan.clone());
    Json(json!({ "ok": true, "plan": plan })).into_response()
}

pub async fn api_card_plan_delete(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    if state.card_store.delete_plan(&id) {
        Json(json!({ "ok": true })).into_response()
    } else {
        (
            StatusCode::CONFLICT,
            Json(json!({"error": "plan in use by cards or not found"})),
        )
            .into_response()
    }
}

// ── 成本模型 ──

pub async fn api_cost_model_get(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let cm = state.card_store.cost_model();
    Json(json!({ "cost_model": cm, "rmb_per_usd": cm.rmb_per_usd() }))
}

pub async fn api_cost_model_set(
    State(state): State<Arc<AppState>>,
    Json(cm): Json<CostModel>,
) -> Response {
    if cm.account_price_rmb <= 0.0 || cm.weekly_quota_usd <= 0.0 || cm.usable_weeks <= 0.0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "all fields must be > 0"})),
        )
            .into_response();
    }
    state.card_store.set_cost_model(cm.clone());
    Json(json!({ "ok": true, "cost_model": cm, "rmb_per_usd": cm.rmb_per_usd() })).into_response()
}

/// 官方口径价格表 + 层级, 供调价时对照
pub async fn api_pricing_table(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    let models = [
        "claude-fable-5-1-thinking-max",
        "claude-fable-5-1-thinking-high",
        "claude-fable-5",
        "claude-opus-5-fast",
        "claude-opus-5",
        "kimi-k3-max",
        "kimi-k3-high",
        "kimi-k3",
        "gpt-5.6-sol",
        "gpt-5.4-pro",
        "grok-4.6",
        "claude-sonnet-5",
    ];
    let rows: Vec<Value> = models
        .iter()
        .map(|m| {
            let (i, o, c, w) = cards::model_price(m);
            json!({
                "model": m,
                "tier": cards::model_tier(m),
                "input_per_m": i, "output_per_m": o, "cache_read_per_m": c, "cache_write_per_m": w,
                // 参考: 典型请求 (20k in / 4k out / 30k cr) 官方口径成本
                "typical_req_usd": cards::estimate_quota_cost(m, 0, 0, 0),
            })
        })
        .collect();
    Json(
        json!({ "models": rows, "tiers": [cards::TIER_ECONOMY, cards::TIER_STANDARD, cards::TIER_FLAGSHIP] }),
    )
}

// ── 卡 ──

pub async fn api_cards_list(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(json!({ "cards": state.card_store.list_status() }))
}

#[derive(Deserialize)]
pub struct IssueBody {
    pub plan_id: String,
    #[serde(default)]
    pub owner: String,
    /// 一次开几张 (默认 1, 上限 100)
    #[serde(default = "d1u")]
    pub count: u32,
    /// 实收价覆盖 (促销/议价). 缺省 = 套餐标价
    #[serde(default)]
    pub paid_rmb: Option<f64>,
}
fn d1u() -> u32 {
    1
}

pub async fn api_cards_issue(
    State(state): State<Arc<AppState>>,
    Json(body): Json<IssueBody>,
) -> Response {
    let count = body.count.clamp(1, 100);
    let mut issued = Vec::new();
    for _ in 0..count {
        match state
            .card_store
            .issue_card_priced(&body.plan_id, &body.owner, body.paid_rmb)
        {
            Ok(c) => issued.push(c),
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": e, "issued_so_far": issued.len()})),
                )
                    .into_response()
            }
        }
    }
    Json(json!({ "ok": true, "issued": issued })).into_response()
}

#[derive(Deserialize)]
pub struct ExtendBody {
    pub hours: u64,
}

pub async fn api_cards_extend(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    Json(body): Json<ExtendBody>,
) -> Response {
    match state.card_store.extend_card(&key, body.hours) {
        Ok(c) => Json(json!({ "ok": true, "card": c })).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, Json(json!({"error": e}))).into_response(),
    }
}

pub async fn api_cards_revoke(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Response {
    if state.card_store.revoke_card(&key) {
        Json(json!({ "ok": true })).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "card not found"})),
        )
            .into_response()
    }
}

pub async fn api_cards_delete(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Response {
    if state.card_store.delete_card(&key) {
        Json(json!({ "ok": true })).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "card not found"})),
        )
            .into_response()
    }
}

pub async fn api_cards_status(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Response {
    match state.card_store.card_status(&key) {
        Some(v) => Json(v).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "card not found"})),
        )
            .into_response(),
    }
}

/// 卡用量报表: 跨 billing.db, 按 card key 汇总真实 token 消耗。
pub async fn api_cards_report(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let conn = match state.ledger.reader() {
        Ok(c) => c,
        Err(e) => {
            return Json(json!({"error": format!("billing reader: {}", e)})).into_response();
        }
    };
    let mut stmt = match conn.prepare(
        "SELECT key_name, COUNT(*) n,
                SUM(input_tokens), SUM(output_tokens),
                SUM(cache_read_tokens), SUM(cache_write_tokens), AVG(latency_ms)
         FROM billing_records
         WHERE key_name LIKE 'card-%'
         GROUP BY key_name ORDER BY n DESC",
    ) {
        Ok(s) => s,
        Err(e) => return Json(json!({"error": format!("prepare: {}", e)})).into_response(),
    };
    let rows = stmt.query_map([], |r| {
        Ok(json!({
            "card_key": r.get::<_, String>(0)?,
            "requests": r.get::<_, i64>(1)?,
            "input_tokens": r.get::<_, i64>(2)?,
            "output_tokens": r.get::<_, i64>(3)?,
            "cache_read_tokens": r.get::<_, i64>(4)?,
            "cache_write_tokens": r.get::<_, i64>(5)?,
            "avg_latency_ms": r.get::<_, f64>(6)?,
        }))
    });
    match rows {
        Ok(mapped) => {
            let out: Vec<Value> = mapped.flatten().collect();
            Json(json!({ "cards": out })).into_response()
        }
        Err(e) => Json(json!({"error": format!("query: {}", e)})).into_response(),
    }
}

// ── 利润报表 ──

#[derive(Deserialize, Default)]
pub struct ProfitQuery {
    /// 起止 (本地时间 "YYYY-MM-DD" 或 unix 秒). 缺省 = 全部
    pub from: Option<String>,
    pub to: Option<String>,
    /// card | plan | day (默认 plan)
    pub group: Option<String>,
}

/// 利润报表. 成本 = 官方口径面值 × CostModel.rmb_per_usd; 收入 = 卡 paid_rmb (按开卡日计入).
///
/// 逐条重算面值 (用 cards::model_price 官方价), 不信 billing.db 里按客户价算的 cost_nano.
pub async fn api_cards_profit(
    State(state): State<Arc<AppState>>,
    Query(q): Query<ProfitQuery>,
) -> impl IntoResponse {
    let tz = state.card_store.tz_offset_minutes();
    let from_ms = q
        .from
        .as_deref()
        .and_then(|s| crate::billing::parse_time(s, tz, false));
    let to_ms =
        q.to.as_deref()
            .and_then(|s| crate::billing::parse_time(s, tz, true));
    let group = q.group.clone().unwrap_or_else(|| "plan".into());
    let cm = state.card_store.cost_model();
    let rmb_per_usd = cm.rmb_per_usd();

    let conn = match state.ledger.reader() {
        Ok(c) => c,
        Err(e) => {
            return Json(json!({"error": format!("billing reader: {}", e)})).into_response();
        }
    };
    let mut sql = String::from(
        "SELECT key_name, model, ts_ms, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, status
         FROM billing_records WHERE key_name LIKE 'card-%'",
    );
    let mut params: Vec<i64> = Vec::new();
    if let Some(f) = from_ms {
        sql.push_str(" AND ts_ms >= ?");
        params.push(f);
    }
    if let Some(t) = to_ms {
        sql.push_str(" AND ts_ms <= ?");
        params.push(t);
    }
    let mut stmt = match conn.prepare(&sql) {
        Ok(s) => s,
        Err(e) => return Json(json!({"error": format!("prepare: {}", e)})).into_response(),
    };
    let mut rows = match stmt.query(rusqlite::params_from_iter(params.iter())) {
        Ok(r) => r,
        Err(e) => return Json(json!({"error": format!("query: {}", e)})).into_response(),
    };

    // 卡 → (plan_id, owner, paid_rmb, issued_at)
    let cards: BTreeMap<String, cards::Card> = state
        .card_store
        .list_cards()
        .into_iter()
        .map(|c| (c.card_key.clone(), c))
        .collect();
    let plans: BTreeMap<String, CardPlan> = state
        .card_store
        .list_plans()
        .into_iter()
        .map(|p| (p.id.clone(), p))
        .collect();

    #[derive(Default)]
    struct Agg {
        requests: i64,
        errors: i64,
        face_usd: f64,
        in_tok: i64,
        out_tok: i64,
        cr_tok: i64,
        cw_tok: i64,
        by_model: BTreeMap<String, (i64, f64)>,
    }
    let mut per_card: BTreeMap<String, Agg> = BTreeMap::new();
    let mut per_card_day: BTreeMap<(String, String), Agg> = BTreeMap::new();
    while let Ok(Some(r)) = rows.next() {
        let key: String = r.get(0).unwrap_or_default();
        let model: String = r.get(1).unwrap_or_default();
        let ts_ms: i64 = r.get(2).unwrap_or(0);
        let it: i64 = r.get(3).unwrap_or(0);
        let ot: i64 = r.get(4).unwrap_or(0);
        let cr: i64 = r.get(5).unwrap_or(0);
        let cw: i64 = r.get(6).unwrap_or(0);
        let status: i64 = r.get(7).unwrap_or(200);
        let usd = if it == 0 && ot == 0 {
            0.0 // 失败/无 usage 不计面值
        } else {
            cards::estimate_quota_cost_full(&model, it as u64, ot as u64, cr as u64, cw as u64)
        };
        let day = crate::billing::fmt_local(ts_ms, tz)
            .chars()
            .take(10)
            .collect::<String>();
        for a in [
            per_card.entry(key.clone()).or_default(),
            per_card_day.entry((key.clone(), day)).or_default(),
        ] {
            a.requests += 1;
            if status >= 400 {
                a.errors += 1;
            }
            a.face_usd += usd;
            a.in_tok += it;
            a.out_tok += ot;
            a.cr_tok += cr;
            a.cw_tok += cw;
            let e = a.by_model.entry(model.clone()).or_insert((0, 0.0));
            e.0 += 1;
            e.1 += usd;
        }
    }

    // 收入归属: 卡开在区间内才计收入 (否则只计成本 — 昨天卖的卡今天还在烧)
    let in_range = |issued_at: u64| -> bool {
        let ms = issued_at as i64 * 1000;
        from_ms.map(|f| ms >= f).unwrap_or(true) && to_ms.map(|t| ms <= t).unwrap_or(true)
    };

    let card_row = |key: &str, a: &Agg, count_revenue: bool| -> Value {
        let c = cards.get(key);
        let paid = c.map(|c| c.paid_rmb).unwrap_or(0.0);
        let revenue = if count_revenue && c.map(|c| in_range(c.issued_at)).unwrap_or(false) {
            paid
        } else {
            0.0
        };
        let cost = a.face_usd * rmb_per_usd;
        let mut models: Vec<Value> = a
            .by_model
            .iter()
            .map(|(m, (n, u))| json!({"model": m, "requests": n, "face_usd": u}))
            .collect();
        models.sort_by(|x, y| {
            y["face_usd"]
                .as_f64()
                .unwrap_or(0.0)
                .partial_cmp(&x["face_usd"].as_f64().unwrap_or(0.0))
                .unwrap()
        });
        json!({
            "card_key": key,
            "owner": c.map(|c| c.owner.clone()),
            "plan_id": c.map(|c| c.plan_id.clone()),
            "issued_at": c.map(|c| c.issued_at),
            "paid_rmb": paid,
            "revenue_rmb": revenue,
            "requests": a.requests,
            "errors": a.errors,
            "face_usd": a.face_usd,
            "cost_rmb": cost,
            "profit_rmb": revenue - cost,
            "margin": if revenue > 0.0 { (revenue - cost) / revenue } else { Value::Null.as_f64().unwrap_or(f64::NAN) },
            "input_tokens": a.in_tok, "output_tokens": a.out_tok,
            "cache_read_tokens": a.cr_tok, "cache_write_tokens": a.cw_tok,
            "models": models,
        })
    };

    // 无流量但开在区间内的卡也要计收入 (卖了没用 = 纯利)
    let mut all_keys: Vec<String> = cards
        .values()
        .filter(|c| in_range(c.issued_at))
        .map(|c| c.card_key.clone())
        .collect();
    for k in per_card.keys() {
        if !all_keys.contains(k) {
            all_keys.push(k.clone());
        }
    }
    let empty = Agg::default();
    let card_rows: Vec<Value> = all_keys
        .iter()
        .map(|k| card_row(k, per_card.get(k).unwrap_or(&empty), true))
        .collect();

    let total_revenue: f64 = card_rows
        .iter()
        .map(|r| r["revenue_rmb"].as_f64().unwrap_or(0.0))
        .sum();
    let total_face: f64 = card_rows
        .iter()
        .map(|r| r["face_usd"].as_f64().unwrap_or(0.0))
        .sum();
    let total_cost = total_face * rmb_per_usd;
    let total_req: i64 = card_rows
        .iter()
        .map(|r| r["requests"].as_i64().unwrap_or(0))
        .sum();

    let grouped: Value = match group.as_str() {
        "card" => json!(card_rows),
        "day" => {
            // 按日: 收入按开卡日, 成本按消耗日
            let mut days: BTreeMap<String, (f64, f64, i64, i64)> = BTreeMap::new();
            for ((key, day), a) in &per_card_day {
                let e = days.entry(day.clone()).or_default();
                e.1 += a.face_usd;
                e.2 += a.requests;
                let _ = key;
            }
            for c in cards.values() {
                if in_range(c.issued_at) {
                    let day = crate::billing::fmt_local(c.issued_at as i64 * 1000, tz)
                        .chars()
                        .take(10)
                        .collect::<String>();
                    let e = days.entry(day).or_default();
                    e.0 += c.paid_rmb;
                    e.3 += 1;
                }
            }
            json!(days
                .iter()
                .map(|(d, (rev, face, req, issued))| {
                    let cost = face * rmb_per_usd;
                    json!({
                        "day": d, "cards_issued": issued, "revenue_rmb": rev, "requests": req,
                        "face_usd": face, "cost_rmb": cost, "profit_rmb": rev - cost,
                        "margin": if *rev > 0.0 { (rev - cost) / rev } else { f64::NAN },
                    })
                })
                .collect::<Vec<_>>())
        }
        _ => {
            let mut by_plan: BTreeMap<String, (f64, f64, i64, i64)> = BTreeMap::new();
            for r in &card_rows {
                let pid = r["plan_id"].as_str().unwrap_or("?").to_string();
                let e = by_plan.entry(pid).or_default();
                e.0 += r["revenue_rmb"].as_f64().unwrap_or(0.0);
                e.1 += r["face_usd"].as_f64().unwrap_or(0.0);
                e.2 += r["requests"].as_i64().unwrap_or(0);
                if r["revenue_rmb"].as_f64().unwrap_or(0.0) > 0.0 {
                    e.3 += 1;
                }
            }
            json!(by_plan
                .iter()
                .map(|(pid, (rev, face, req, n))| {
                    let cost = face * rmb_per_usd;
                    let p = plans.get(pid);
                    json!({
                        "plan_id": pid,
                        "plan_name": p.map(|p| p.name.clone()),
                        "kind": p.map(|p| p.kind),
                        "tier": p.map(|p| p.tier.clone()),
                        "list_price": p.map(|p| p.price),
                        "cards_sold": n,
                        "revenue_rmb": rev,
                        "requests": req,
                        "face_usd": face,
                        "cost_rmb": cost,
                        "profit_rmb": rev - cost,
                        "margin": if *rev > 0.0 { (rev - cost) / rev } else { f64::NAN },
                        "avg_face_per_card": if *n > 0 { face / *n as f64 } else { 0.0 },
                        "avg_cost_per_card_rmb": if *n > 0 { cost / *n as f64 } else { 0.0 },
                    })
                })
                .collect::<Vec<_>>())
        }
    };

    Json(json!({
        "range": { "from_ms": from_ms, "to_ms": to_ms, "tz_offset_minutes": tz },
        "cost_model": cm,
        "rmb_per_usd": rmb_per_usd,
        "totals": {
            "cards": card_rows.len(),
            "requests": total_req,
            "revenue_rmb": total_revenue,
            "face_usd": total_face,
            "cost_rmb": total_cost,
            "profit_rmb": total_revenue - total_cost,
            "margin": if total_revenue > 0.0 { (total_revenue - total_cost) / total_revenue } else { f64::NAN },
        },
        "group": group,
        "rows": grouped,
    }))
    .into_response()
}
