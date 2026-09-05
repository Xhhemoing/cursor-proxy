//! 消耗分析 API (零售版).
//!
//! 端点:
//!   GET /admin/api/analytics/consumption?from=&to=&gap=&scope=cards|all&key=
//!       按 模型 / 模型组 / 套餐 / 卡 的消耗 + 在线时长 + $/在线小时, 附小时时间轴与总计
//!   GET /admin/api/analytics/presence?window=
//!       每张卡 (和每个 api key) 现在在线/空闲/离线, 最近活动, 今日面值, 今日在线时长
//!   GET /admin/api/analytics/sessions?key=&from=&to=&gap=
//!       单卡/单 key 的在线时段明细 (每段: 起止 / 请求数 / 面值 / 用了哪些模型)
//!
//! 所有金额: face_usd = 官方面值 (逐条按当前价重算), cost_rmb = face × CostModel.rmb_per_usd.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::{
    extract::{Query, State},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::analytics::{self, Agg, Req};
use crate::AppState;

/// 一次分析最多读多少行 (30 天 × 高频也够; 超出说明该缩时间窗)
const MAX_ROWS: usize = 500_000;

fn err(e: impl std::fmt::Display) -> Response {
    Json(json!({"error": e.to_string()})).into_response()
}

#[derive(Deserialize, Default)]
pub struct ConsumptionQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    /// 会话间隔秒; 缺省/0 = 按每个 key 自身节奏自适应
    pub gap: Option<f64>,
    /// cards (默认, 只看套餐卡) | all (含 api key / 匿名)
    pub scope: Option<String>,
    /// 只看某张卡 / 某个 key 名
    pub key: Option<String>,
}

struct Ctx {
    from_ms: Option<i64>,
    to_ms: Option<i64>,
    tz: i32,
    gap: Option<f64>,
    rmb_per_usd: f64,
}

fn ctx(state: &AppState, from: Option<&str>, to: Option<&str>, gap: Option<f64>) -> Ctx {
    let tz = state.card_store.tz_offset_minutes();
    Ctx {
        from_ms: from.and_then(|s| crate::billing::parse_time(s, tz, false)),
        to_ms: to.and_then(|s| crate::billing::parse_time(s, tz, true)),
        tz,
        gap: gap.filter(|g| *g > 0.0),
        rmb_per_usd: state.card_store.cost_model().rmb_per_usd(),
    }
}

fn agg_rows(m: BTreeMap<String, Agg>, rmb: f64, id_key: &str) -> Vec<Value> {
    let mut rows: Vec<Value> = m
        .into_iter()
        .map(|(k, a)| {
            let mut v = a.to_json(rmb);
            v[id_key] = json!(k);
            v
        })
        .collect();
    rows.sort_by(|x, y| {
        y["face_usd"]
            .as_f64()
            .unwrap_or(0.0)
            .partial_cmp(&x["face_usd"].as_f64().unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    rows
}

pub async fn api_analytics_consumption(
    State(state): State<Arc<AppState>>,
    Query(q): Query<ConsumptionQuery>,
) -> Response {
    let c = ctx(&state, q.from.as_deref(), q.to.as_deref(), q.gap);
    let cards_only = q.scope.as_deref().unwrap_or("cards") != "all";
    let key = q.key.clone().filter(|k| !k.trim().is_empty());
    let ledger = state.ledger.clone();
    let (from_ms, to_ms) = (c.from_ms, c.to_ms);
    let reqs: Vec<Req> = match tokio::task::spawn_blocking(move || {
        let conn = ledger.reader()?;
        analytics::load_reqs(&conn, from_ms, to_ms, cards_only, key.as_deref(), MAX_ROWS)
    })
    .await
    {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => return err(e),
        Err(e) => return err(e),
    };
    let truncated = reqs.len() >= MAX_ROWS;

    // 卡 → 套餐 / 归属; 模型 → 组
    let cards: BTreeMap<String, crate::cards::Card> = state
        .card_store
        .list_cards()
        .into_iter()
        .map(|c| (c.card_key.clone(), c))
        .collect();
    let plans: BTreeMap<String, crate::cards::CardPlan> = state
        .card_store
        .list_plans()
        .into_iter()
        .map(|p| (p.id.clone(), p))
        .collect();
    let reg = crate::models::registry();
    let groups: BTreeMap<String, String> = reg
        .groups()
        .into_iter()
        .map(|g| (g.id.clone(), g.name.clone()))
        .collect();

    let by_model = analytics::aggregate(&reqs, |r| vec![r.model.clone()], c.gap);
    let by_group = analytics::aggregate(
        &reqs,
        |r| {
            let g = reg.groups_of(&r.model);
            if g.is_empty() {
                vec!["(未分组)".to_string()]
            } else {
                g
            }
        },
        c.gap,
    );
    let by_plan = analytics::aggregate(
        &reqs,
        |r| {
            if !r.is_card() {
                return vec!["(非套餐卡)".to_string()];
            }
            vec![cards
                .get(&r.key_name)
                .map(|c| c.plan_id.clone())
                .unwrap_or_else(|| "(已删卡)".to_string())]
        },
        c.gap,
    );
    let by_key = analytics::aggregate(&reqs, |r| vec![r.key_name.clone()], c.gap);
    let total = analytics::aggregate(&reqs, |_| vec!["all".to_string()], c.gap)
        .remove("all")
        .unwrap_or_default();

    let rmb = c.rmb_per_usd;
    let mut model_rows = agg_rows(by_model, rmb, "model");
    for r in &mut model_rows {
        let m = r["model"].as_str().unwrap_or("").to_string();
        r["groups"] = json!(reg.groups_of(&m));
        r["priced"] = json!(crate::cards::model_price_known(&m));
    }
    let mut group_rows = agg_rows(by_group, rmb, "group_id");
    for r in &mut group_rows {
        let id = r["group_id"].as_str().unwrap_or("").to_string();
        r["group_name"] = json!(groups.get(&id).cloned().unwrap_or(id));
    }
    let mut plan_rows = agg_rows(by_plan, rmb, "plan_id");
    for r in &mut plan_rows {
        let id = r["plan_id"].as_str().unwrap_or("").to_string();
        if let Some(p) = plans.get(&id) {
            r["plan_name"] = json!(p.name);
            r["kind"] = json!(p.kind);
            r["list_price"] = json!(p.price);
            r["duration_hours"] = json!(p.duration_hours);
            r["max_concurrency"] = json!(p.max_concurrency);
            // 一张卡在有效期内若「全程在线」的理论上限 vs 实测 $/在线小时 → 定价参考
            let per_h = r["usd_per_online_hour"].as_f64();
            r["rmb_per_online_hour"] = json!(per_h.map(|v| v * rmb));
            // 每卡平均: 面值 / 参与卡数
            let users = r["users"].as_u64().unwrap_or(0).max(1) as f64;
            r["avg_face_per_card"] = json!(r["face_usd"].as_f64().unwrap_or(0.0) / users);
            r["avg_online_hours_per_card"] =
                json!(r["online_hours"].as_f64().unwrap_or(0.0) / users);
        }
    }
    let mut key_rows = agg_rows(by_key, rmb, "key_name");
    for r in &mut key_rows {
        let k = r["key_name"].as_str().unwrap_or("").to_string();
        if let Some(cd) = cards.get(&k) {
            r["owner"] = json!(cd.owner);
            r["plan_id"] = json!(cd.plan_id);
            r["plan_name"] = json!(plans.get(&cd.plan_id).map(|p| p.name.clone()));
            r["paid_rmb"] = json!(cd.paid_rmb);
            r["expires_at"] = json!(cd.expires_at);
            r["enabled"] = json!(cd.enabled);
            let cost = r["cost_rmb"].as_f64().unwrap_or(0.0);
            r["profit_rmb"] = json!(cd.paid_rmb - cost);
        }
        r["is_card"] = json!(k.starts_with("card-"));
    }

    Json(json!({
        "range": { "from_ms": c.from_ms, "to_ms": c.to_ms, "tz_offset_minutes": c.tz },
        "scope": if cards_only { "cards" } else { "all" },
        "gap_secs": c.gap,
        "gap_mode": if c.gap.is_some() { "fixed" } else { "auto" },
        "rmb_per_usd": rmb,
        "rows_scanned": reqs.len(),
        "truncated": truncated,
        "totals": total.to_json(rmb),
        "by_model": model_rows,
        "by_group": group_rows,
        "by_plan": plan_rows,
        "by_key": key_rows,
        "hourly": analytics::hourly(&reqs, c.tz),
    }))
    .into_response()
}

#[derive(Deserialize, Default)]
pub struct PresenceQuery {
    /// 「在线」窗口秒 (最近多久内有请求算在线), 默认 300
    pub window: Option<i64>,
}

/// 实时在线: 每张卡 / 每个 key 的状态. 数据源 = 今日账本 + 卡运行时 in_flight.
pub async fn api_analytics_presence(
    State(state): State<Arc<AppState>>,
    Query(q): Query<PresenceQuery>,
) -> Response {
    let window = q.window.unwrap_or(300).clamp(30, 3600);
    let tz = state.card_store.tz_offset_minutes();
    let now_ms = chrono::Utc::now().timestamp_millis();
    let now_s = (now_ms / 1000) as u64;
    let day_start_ms = crate::cards::day_start(now_s, tz) as i64 * 1000;
    // 今日 + 往前 30min (跨零点时空闲判定还要看昨晚)
    let from_ms = day_start_ms.min(now_ms - analytics::IDLE_WINDOW_SECS * 1000);
    let ledger = state.ledger.clone();
    let reqs: Vec<Req> = match tokio::task::spawn_blocking(move || {
        let conn = ledger.reader()?;
        analytics::load_reqs(&conn, Some(from_ms), None, false, None, MAX_ROWS)
    })
    .await
    {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => return err(e),
        Err(e) => return err(e),
    };
    let rmb = state.card_store.cost_model().rmb_per_usd();
    let statuses: BTreeMap<String, Value> = state
        .card_store
        .list_status()
        .into_iter()
        .filter_map(|v| {
            let k = v["card_key"].as_str()?.to_string();
            Some((k, v))
        })
        .collect();

    // 按 key 分流
    let mut per_key: BTreeMap<String, Vec<Req>> = BTreeMap::new();
    for r in &reqs {
        per_key
            .entry(r.key_name.clone())
            .or_default()
            .push(r.clone());
    }
    let mut rows: Vec<Value> = Vec::new();
    // 有卡但今日无流量的也要列 (离线)
    for k in statuses.keys() {
        per_key.entry(k.clone()).or_default();
    }
    let mut online = 0usize;
    let mut idle = 0usize;
    for (k, rs) in per_key {
        let st = statuses.get(&k);
        // 已删除/过期且无流量的卡不列
        if rs.is_empty()
            && st
                .map(|s| {
                    s["expired"].as_bool().unwrap_or(false)
                        || !s["enabled"].as_bool().unwrap_or(true)
                })
                .unwrap_or(true)
        {
            continue;
        }
        let in_flight = st.and_then(|s| s["in_flight"].as_u64()).unwrap_or(0);
        let last_end = rs.iter().map(|r| r.end_ms).max();
        let pres = analytics::presence(last_end, in_flight, now_ms, window);
        match pres {
            analytics::Presence::Online => online += 1,
            analytics::Presence::Idle => idle += 1,
            _ => {}
        }
        let today: Vec<Req> = rs
            .iter()
            .filter(|r| r.end_ms >= day_start_ms)
            .cloned()
            .collect();
        let (sessions, gap) = analytics::sessionize(&today, None);
        let online_secs: f64 = sessions.iter().map(|s| s.secs()).sum();
        let face: f64 = today.iter().map(|r| r.face_usd).sum();
        let mut models: BTreeMap<String, u64> = BTreeMap::new();
        for r in &today {
            *models.entry(r.model.clone()).or_default() += 1;
        }
        let cur_session = sessions
            .last()
            .filter(|_| pres != analytics::Presence::Offline);
        rows.push(json!({
            "key_name": k,
            "is_card": k.starts_with("card-"),
            "owner": st.and_then(|s| s["owner"].as_str()),
            "plan_id": st.and_then(|s| s["plan_id"].as_str()),
            "plan_name": st.and_then(|s| s["plan_name"].as_str()),
            "expires_at": st.and_then(|s| s["expires_at"].as_u64()),
            "presence": pres,
            "in_flight": in_flight,
            "last_seen_ms": last_end,
            "idle_secs": last_end.map(|t| ((now_ms - t) / 1000).max(0)),
            "today_requests": today.len(),
            "today_errors": today.iter().filter(|r| !r.ok()).count(),
            "today_face_usd": face,
            "today_cost_rmb": face * rmb,
            "today_online_hours": online_secs / 3600.0,
            "today_sessions": sessions.len(),
            "session_gap_secs": gap,
            "current_session_start_ms": cur_session.map(|s| s.start_ms),
            "current_session_requests": cur_session.map(|s| s.requests),
            "current_session_face_usd": cur_session.map(|s| s.face_usd),
            "models_today": models,
            "throttle": st.and_then(|s| s["throttle"].as_str()),
        }));
    }
    rows.sort_by(|a, b| {
        let rank = |v: &Value| match v["presence"].as_str() {
            Some("online") => 0,
            Some("idle") => 1,
            _ => 2,
        };
        rank(a)
            .cmp(&rank(b))
            .then_with(|| b["last_seen_ms"].as_i64().cmp(&a["last_seen_ms"].as_i64()))
    });
    Json(json!({
        "now_ms": now_ms,
        "window_secs": window,
        "idle_window_secs": analytics::IDLE_WINDOW_SECS,
        "online": online,
        "idle": idle,
        "total": rows.len(),
        "rows": rows,
    }))
    .into_response()
}

#[derive(Deserialize, Default)]
pub struct SessionsQuery {
    pub key: String,
    pub from: Option<String>,
    pub to: Option<String>,
    pub gap: Option<f64>,
}

/// 单卡 / 单 key 在线时段明细
pub async fn api_analytics_sessions(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SessionsQuery>,
) -> Response {
    let c = ctx(&state, q.from.as_deref(), q.to.as_deref(), q.gap);
    let key = q.key.trim().to_string();
    if key.is_empty() {
        return err("key required");
    }
    let ledger = state.ledger.clone();
    let (from_ms, to_ms, k2) = (c.from_ms, c.to_ms, key.clone());
    let reqs: Vec<Req> = match tokio::task::spawn_blocking(move || {
        let conn = ledger.reader()?;
        analytics::load_reqs(&conn, from_ms, to_ms, false, Some(&k2), MAX_ROWS)
    })
    .await
    {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => return err(e),
        Err(e) => return err(e),
    };
    let (sessions, gap) = analytics::sessionize(&reqs, c.gap);
    // 每段用了哪些模型 / 忙时
    let mut out: Vec<Value> = Vec::with_capacity(sessions.len());
    for s in &sessions {
        let inside: Vec<&Req> = reqs
            .iter()
            .filter(|r| r.start_ms >= s.start_ms && r.end_ms <= s.end_ms)
            .collect();
        let mut models: BTreeMap<String, (u64, f64)> = BTreeMap::new();
        let mut busy_ms = 0i64;
        let mut errors = 0u64;
        for r in &inside {
            let e = models.entry(r.model.clone()).or_default();
            e.0 += 1;
            e.1 += r.face_usd;
            busy_ms += r.latency_ms();
            if !r.ok() {
                errors += 1;
            }
        }
        let secs = s.secs();
        out.push(json!({
            "start_ms": s.start_ms,
            "end_ms": s.end_ms,
            "start": crate::billing::fmt_local(s.start_ms, c.tz),
            "end": crate::billing::fmt_local(s.end_ms, c.tz),
            "secs": secs,
            "requests": s.requests,
            "errors": errors,
            "face_usd": s.face_usd,
            "cost_rmb": s.face_usd * c.rmb_per_usd,
            "busy_ratio": if secs > 0.0 { (busy_ms as f64 / 1000.0 / secs).min(10.0) } else { 0.0 },
            "usd_per_hour": if secs > 0.0 { Some(s.face_usd / (secs / 3600.0)) } else { None },
            "models": models.iter().map(|(m, (n, f))| json!({"model": m, "requests": n, "face_usd": f})).collect::<Vec<_>>(),
        }));
    }
    out.reverse(); // 最近的在前
    let total_face: f64 = sessions.iter().map(|s| s.face_usd).sum();
    let total_secs: f64 = sessions.iter().map(|s| s.secs()).sum();
    let card = state.card_store.card_status(&key);
    Json(json!({
        "key": key,
        "card": card,
        "range": { "from_ms": c.from_ms, "to_ms": c.to_ms, "tz_offset_minutes": c.tz },
        "gap_secs": gap,
        "gap_mode": if c.gap.is_some() { "fixed" } else { "auto" },
        "rmb_per_usd": c.rmb_per_usd,
        "totals": {
            "requests": reqs.len(),
            "sessions": sessions.len(),
            "online_hours": total_secs / 3600.0,
            "face_usd": total_face,
            "cost_rmb": total_face * c.rmb_per_usd,
            "usd_per_online_hour": if total_secs > 0.0 { Some(total_face / (total_secs / 3600.0)) } else { None },
        },
        "sessions": out,
    }))
    .into_response()
}
