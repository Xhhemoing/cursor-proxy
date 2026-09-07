//! 模型总控: 一行一个「定价单元」, 把定价 / 消耗 / 降速 / 风控规则聚合到一张表.
//!
//! 设计约束 (2026-09-07 定稿):
//! - 行键 = `models::console_key` (思考档折叠到家族基名, `-fast` ×2 独立行);
//!   这是**定价行**, 与 `visible_models` 客户菜单行**不同构** (fast 变体折叠), 漂移不隐藏 ——
//!   行内必须带 `client_ids` (客户实际看到的可见 id) 与 `variants` (变体数) 暴露差异.
//! - 成本/速度/tps 全部来自 `analytics::aggregate` 的 `Agg::to_json`/`speed_json`,
//!   不在此手写新 SQL (与消耗分析同口径, 复用 load_reqs 归一化).
//! - 风控规则列只读 (RiskPolicy POST 是整表替换, 就地编辑会清零既有策略).
//! - fast 行定价必须先录 `-fast` 专属条目 (console_key_priceable=false 时前端提示).

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::analytics as an;
use crate::cards;
use crate::models::{console_key, console_key_priceable, registry, GATEWAY_ALIASES};
use crate::AppState;

#[derive(Deserialize)]
pub struct ConsoleQuery {
    /// 时间窗: "24h" (默认) | "7d" | "30d"
    #[serde(default)]
    pub window: Option<String>,
}

fn window_ms(w: Option<&str>) -> i64 {
    let secs = match w {
        Some("7d") => 7 * 86_400,
        Some("30d") => 30 * 86_400,
        _ => 86_400, // 24h
    };
    chrono::Utc::now().timestamp_millis() - secs * 1000
}

pub async fn api_model_console(
    State(state): State<Arc<AppState>>,
    Query(q): Query<ConsoleQuery>,
) -> Response {
    let from_ms = window_ms(q.window.as_deref());

    // 1. 行全集: 定价单元 = visible_models (客户端可见) ∪ 注册表 manual ∪ 内置表 ∪ 账本 seen.
    //    幽灵 (SHADOW 价) 已在 candidate 层被剔除; upstream_missing 红标由上游名单判定.
    //    fast 变体经 visible_models 并入 (keep-fast), 经 console_key 折叠成 *-fast 派生行.
    //    行键 = 定价单元; 客户菜单行是 visible_models 原名 (fast 逐列), 两者不同构, 差异由
    //    行内 variants/client_ids 暴露, 不隐藏.
    let upstream = state.card_store.upstream_names();
    let seen: Vec<String> = crate::admin::seen_models(&state)
        .into_iter()
        .map(|(m, _)| m)
        .collect();
    let reg = registry();
    let visible = reg.visible_models(&upstream, &seen);
    let snap = reg.snapshot();
    let mut row_keys: Vec<String> = vec![];
    let mut push_key = |k: String| {
        if !row_keys.iter().any(|x| *x == k) {
            row_keys.push(k);
        }
    };
    for v in &visible {
        push_key(console_key(v).into_owned());
    }
    for e in &snap.models {
        push_key(console_key(&e.model).into_owned());
    }
    for (m, ..) in cards::builtin_table() {
        push_key(console_key(&m).into_owned());
    }
    for m in &seen {
        push_key(console_key(m).into_owned());
    }

    // 2. 消耗聚合: 按行键 dim, 复用 analytics (账本归一化/面值/车道时/tps 与消耗分析同口径).
    //    限速比 (paced_requests / requests) 与 ttft/tps 分布走 Agg::to_json/speed_json.
    let ledger = state.ledger.clone();
    let reqs: Vec<an::Req> = match tokio::task::spawn_blocking(move || {
        let conn = ledger.reader()?;
        an::load_reqs(&conn, Some(from_ms), None, false, None, 500_000)
    })
    .await
    {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response()
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response()
        }
    };
    let by_row = an::aggregate(&reqs, |r| vec![console_key(&r.model).into_owned()], None);
    let rmb_per_usd = state.card_store.cost_model().rmb_per_usd();
    let price_map = state.analytics_price_map();
    let policy = state.card_store.risk_policy();

    // 3. 组装每行
    let rows: Vec<Value> = row_keys
        .iter()
        .map(|key| {
            let agg = by_row.get(key);
            let (price_in, price_out, price_cr, price_cw) = cards::model_price(key);
            let manual = reg.get_exact(key);
            let price_source = if manual.is_some() {
                "manual"
            } else if cards::model_price_known(key) {
                "builtin"
            } else {
                "fallback"
            };
            let priceable = console_key_priceable(key);
            let upstream_missing = !upstream.is_empty()
                && !crate::models::ModelRegistry::upstream_present(key, &upstream)
                && !GATEWAY_ALIASES.contains(&key.as_str());
            // 该行实际向客户暴露的可见 id (漂移暴露)
            let client_ids: Vec<&String> = visible
                .iter()
                .filter(|v| console_key(v).as_ref() == key.as_str())
                .collect();
            // 该行的上游变体名 (供「变体数」列)
            let variants: Vec<&String> = upstream
                .iter()
                .filter(|u| console_key(u).as_ref() == key.as_str())
                .collect();
            // 风控规则 (只读: RiskPolicy POST 是整表替换, 就地改会清零)
            let rule = policy.rule_for(key).map(|r| {
                json!({
                    "prefix": r.prefix,
                    "exempt": r.exempt,
                    "pace_normal_tps": r.pace_normal_tps,
                    "pace_soften_tps": r.pace_soften_tps,
                    "pace_degraded_tps": r.pace_degraded_tps,
                    "ttft_floor_ms": r.ttft_floor_ms,
                    "speed_ratio": r.speed_ratio,
                    "max_output_tokens": r.max_output_tokens,
                    "note": r.note,
                })
            });
            // 同族建议档: 该行族内 $/lane·h 最低的变体 (lane_hours>=1h 计入)
            let cheapest_variant = price_map.as_ref().and_then(|pm| {
                variants
                    .iter()
                    .filter_map(|v| pm.get(*v).map(|p| (v, *p)))
                    .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
                    .map(|(v, p)| json!({"variant": v, "usd_per_lane_hour": p}))
            });
            let consumption = agg.map(|a| {
                let j = a.to_json(rmb_per_usd);
                // 数据不足 1h 的 $/lane·h 标为 null, 前端显示「数据不足」而非误导性数字
                let lane_ok = a.lane_hours >= 1.0;
                let s = &j["speed"];
                let tps_p50 = s["tps_p50"].as_f64();
                let upstream_tps = s["upstream_tps_p50"].as_f64();
                let paced_ratio = if a.requests > 0 {
                    a.paced_requests as f64 / a.requests as f64
                } else {
                    0.0
                };
                // #3 成本占比: 官方成本(元) / 该模型日均实收(元). 日均实收 = 窗口内 face_usd * rmb_per_usd / 天数.
                // >60% 红 >30% 黄 (用户定价经验: 成本超付费只作行为分, 60%/80% 档)
                let cost_ratio = {
                    let days = match q.window.as_deref() { Some("7d") => 7.0, Some("30d") => 30.0, _ => 1.0 };
                    let cost_rmb = a.face_usd * rmb_per_usd;
                    let daily_income_rmb = if days > 0.0 { cost_rmb / days } else { 0.0 };
                    // 收入近似: 该模型日均 face 对应的套餐日收难以精确归集, 用「成本占自身面值比」代替
                    // (面值=官方口径成本, 占比=1 即 100%) —— 前端展示为「成本/面值」而非虚构「成本/收入」
                    if cost_rmb > 0.0 && daily_income_rmb > 0.0 { Some(cost_rmb / daily_income_rmb) } else { None }
                };
                json!({
                    "requests": j["requests"],
                    "errors": j["errors"],
                    "face_usd": j["face_usd"],
                    "cost_rmb": j["cost_rmb"],
                    "lane_hours": j["lane_hours"],
                    "usd_per_lane_hour": if lane_ok { j["usd_per_lane_hour"].clone() } else { Value::Null },
                    "data_insufficient": !lane_ok,
                    "users": j["users"],
                    "avg_concurrency": j["avg_concurrency"],
                    "tps_p50": tps_p50,
                    "upstream_tps_p50": upstream_tps,
                    "ttft_p50_ms": s["ttft_p50_ms"],
                    "ttft_p90_ms": s["ttft_p90_ms"],
                    // 降速感知: 我们的压速 = 1 - tps_p50/upstream_tps_p50, 限速命中率 = paced/requests
                    "pace_ratio": upstream_tps.zip(tps_p50).map(|(u, c)| if u > 0.0 { (1.0 - c / u) * 100.0 } else { 0.0 }),
                    "paced_ratio": paced_ratio,
                    "cost_ratio": cost_ratio,
                })
            });
            json!({
                "key": key,
                "price": {
                    "input_per_m": price_in,
                    "output_per_m": price_out,
                    "cache_read_per_m": price_cr,
                    "cache_write_per_m": price_cw,
                    "source": price_source,
                    "priceable": priceable,
                },
                "enabled": manual.as_ref().map(|e| e.enabled).unwrap_or(true),
                "hidden": manual.as_ref().map(|e| e.hidden).unwrap_or(false),
                "upstream": manual.as_ref().map(|e| e.upstream).unwrap_or(false),
                "upstream_missing": upstream_missing,
                "groups": reg.groups_of(key),
                "variants": variants.len(),
                "variant_names": variants,
                "client_ids": client_ids,
                "consumption": consumption,
                "risk_rule": rule,
                "cheapest_variant": cheapest_variant,
            })
        })
        .collect();

    Json(json!({
        "window": q.window.as_deref().unwrap_or("24h"),
        "rows": rows,
        "rmb_per_usd": rmb_per_usd,
        "counts": {
            "rows": rows.len(),
            "priced": rows.iter().filter(|r| r["price"]["source"] != "fallback").count(),
            "ghost": rows.iter().filter(|r| r["upstream_missing"].as_bool().unwrap_or(false)).count(),
        },
    }))
    .into_response()
}
