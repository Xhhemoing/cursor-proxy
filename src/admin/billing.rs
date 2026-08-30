//! 计费管理接口: 账单明细 / 汇总 / 导出 / 价格与销售配置.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::billing::{self, Filter, GroupBy};
use crate::config::{self, ModelPrice, SalesRecord};
use crate::AppState;

fn normalize_currency(raw: &str) -> String {
    crate::config::normalize_currency(raw)
}

const MAX_PAGE: usize = 1000;
const MAX_EXPORT_ROWS: usize = 200_000;

fn bad(msg: impl Into<String>) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({"error": msg.into()}))).into_response()
}

fn internal(e: impl std::fmt::Display) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response()
}

/// 从 query string 构造筛选器. 支持:
/// from/to (unix 秒/毫秒 或 YYYY-MM-DD[ HH[:MM]]), key, sales, model, account, tag, stream, status, unpriced, q
fn parse_filter(q: &HashMap<String, String>, tz: i32) -> Result<Filter, Response> {
    let mut f = Filter::default();
    if let Some(s) = q.get("from").filter(|s| !s.trim().is_empty()) {
        f.from_ms = Some(billing::parse_time(s, tz, false).ok_or_else(|| bad("invalid from"))?);
    }
    if let Some(s) = q.get("to").filter(|s| !s.trim().is_empty()) {
        // to 为日期/小时时取该单位末尾 (闭区间语义)
        f.to_ms = Some(billing::parse_time(s, tz, true).ok_or_else(|| bad("invalid to"))?);
    }
    // 快捷: day=YYYY-MM-DD 或 hour=YYYY-MM-DD HH
    if let Some(d) = q.get("day").filter(|s| !s.trim().is_empty()) {
        f.from_ms = Some(billing::parse_time(d, tz, false).ok_or_else(|| bad("invalid day"))?);
        f.to_ms = Some(billing::parse_time(d, tz, true).ok_or_else(|| bad("invalid day"))?);
    }
    if let Some(h) = q.get("hour").filter(|s| !s.trim().is_empty()) {
        f.from_ms = Some(billing::parse_time(h, tz, false).ok_or_else(|| bad("invalid hour"))?);
        f.to_ms = Some(billing::parse_time(h, tz, true).ok_or_else(|| bad("invalid hour"))?);
    }
    f.key = q.get("key").cloned();
    f.sales = q.get("sales").cloned();
    f.model = q.get("model").cloned();
    f.account = q.get("account").cloned();
    f.tag = q.get("tag").cloned();
    f.q = q.get("q").cloned();
    if let Some(s) = q.get("stream").filter(|s| !s.is_empty()) {
        f.stream = Some(matches!(s.as_str(), "1" | "true"));
    }
    if let Some(s) = q.get("status").filter(|s| !s.is_empty()) {
        f.status = Some(s.parse().map_err(|_| bad("invalid status"))?);
    }
    f.unpriced = q
        .get("unpriced")
        .map(|s| matches!(s.as_str(), "1" | "true"))
        .unwrap_or(false);
    Ok(f)
}

/// GET /admin/api/billing/records — 明细分页
pub async fn api_billing_records(
    State(state): State<Arc<AppState>>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let tz = state.config.load().billing.tz_offset_minutes;
    let f = match parse_filter(&q, tz) {
        Ok(f) => f,
        Err(r) => return r,
    };
    let limit = q
        .get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(100usize)
        .clamp(1, MAX_PAGE);
    let offset = q.get("offset").and_then(|s| s.parse().ok()).unwrap_or(0usize);
    let ledger = state.ledger.clone();
    let res = tokio::task::spawn_blocking(move || {
        let conn = ledger.reader()?;
        billing::query_records(&conn, &f, limit, offset, tz)
    })
    .await;
    match res {
        Ok(Ok((rows, total))) => Json(json!({
            "rows": rows,
            "total": total,
            "limit": limit,
            "offset": offset,
            "tz_offset_minutes": tz,
        }))
        .into_response(),
        Ok(Err(e)) => internal(e),
        Err(e) => internal(e),
    }
}

/// GET /admin/api/billing/summary?group=day|hour|key|sales|model|account|tag|none
pub async fn api_billing_summary(
    State(state): State<Arc<AppState>>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let cfg = state.config.load();
    let tz = cfg.billing.tz_offset_minutes;
    let f = match parse_filter(&q, tz) {
        Ok(f) => f,
        Err(r) => return r,
    };
    let group = match GroupBy::parse(q.get("group").map(|s| s.as_str()).unwrap_or("")) {
        Some(g) => g,
        None => return bad("invalid group (day|hour|key|sales|model|account|tag|none)"),
    };
    let ledger = state.ledger.clone();
    let res = tokio::task::spawn_blocking(move || {
        let conn = ledger.reader()?;
        let rows = billing::summary(&conn, &f, group, tz)?;
        let total = billing::summary(&conn, &f, GroupBy::None, tz)?;
        Ok::<_, rusqlite::Error>((rows, total))
    })
    .await;
    match res {
        Ok(Ok((rows, total))) => {
            // 按销售分组时补上销售名字
            let rows: Vec<Value> = if group == GroupBy::Sales {
                rows.into_iter()
                    .map(|mut r| {
                        let sid = r["group"].as_str().unwrap_or("").to_string();
                        let name = cfg
                            .billing
                            .sales
                            .iter()
                            .find(|s| s.id == sid)
                            .map(|s| s.name.clone())
                            .unwrap_or_default();
                        r["sales_name"] = json!(name);
                        r
                    })
                    .collect()
            } else {
                rows
            };
            Json(json!({
                "group": q.get("group").cloned().unwrap_or_default(),
                "rows": rows,
                "total": total.into_iter().next().unwrap_or(json!({})),
                "currency": cfg.billing.currency,
                "tz_offset_minutes": tz,
            }))
            .into_response()
        }
        Ok(Err(e)) => internal(e),
        Err(e) => internal(e),
    }
}

/// GET /admin/api/billing/export — CSV
pub async fn api_billing_export(
    State(state): State<Arc<AppState>>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let tz = state.config.load().billing.tz_offset_minutes;
    let f = match parse_filter(&q, tz) {
        Ok(f) => f,
        Err(r) => return r,
    };
    let ledger = state.ledger.clone();
    let res = tokio::task::spawn_blocking(move || {
        let conn = ledger.reader()?;
        billing::export_csv(&conn, &f, MAX_EXPORT_ROWS, tz)
    })
    .await;
    match res {
        Ok(Ok(csv)) => {
            let fname = format!("billing-{}.csv", chrono::Utc::now().format("%Y%m%d-%H%M%S"));
            (
                [
                    (header::CONTENT_TYPE, "text/csv; charset=utf-8".to_string()),
                    (
                        header::CONTENT_DISPOSITION,
                        format!("attachment; filename=\"{}\"", fname),
                    ),
                ],
                // BOM 让 Excel 正确识别 UTF-8
                format!("\u{feff}{}", csv),
            )
                .into_response()
        }
        Ok(Err(e)) => internal(e),
        Err(e) => internal(e),
    }
}

/// GET /admin/api/billing/tags — 出现过的标签列表
pub async fn api_billing_tags(State(state): State<Arc<AppState>>) -> Response {
    let ledger = state.ledger.clone();
    let res = tokio::task::spawn_blocking(move || {
        let conn = ledger.reader()?;
        billing::distinct_tags(&conn)
    })
    .await;
    // 合并 key 配置里的标签 (可能还没产生账单)
    let cfg = state.config.load();
    let mut tags: Vec<String> = match res {
        Ok(Ok(t)) => t,
        Ok(Err(e)) => return internal(e),
        Err(e) => return internal(e),
    };
    for k in &cfg.api_keys {
        for t in &k.tags {
            if !tags.contains(t) {
                tags.push(t.clone());
            }
        }
    }
    tags.sort();
    Json(json!({"tags": tags})).into_response()
}

/// GET /admin/api/billing/stats — 写入端状态
pub async fn api_billing_stats(State(state): State<Arc<AppState>>) -> Response {
    Json(state.ledger.stats()).into_response()
}

/// GET /admin/api/billing/pricing — 价格表 + 销售 + 计费参数
pub async fn api_pricing_get(State(state): State<Arc<AppState>>) -> Response {
    let cfg = state.config.load();
    Json(pricing_view(&cfg.billing)).into_response()
}

fn pricing_view(b: &config::BillingConfig) -> Value {
    json!({
        "currency": b.currency,
        "tz_offset_minutes": b.tz_offset_minutes,
        "default_commission_bps": b.default_commission_bps,
        "reject_unpriced": b.reject_unpriced,
        "db_file": b.db_file,
        "prices": b.prices,
        "sales": b.sales,
    })
}

#[derive(Deserialize, Default)]
pub struct PricingPatch {
    pub currency: Option<String>,
    pub tz_offset_minutes: Option<i32>,
    pub default_commission_bps: Option<u32>,
    pub reject_unpriced: Option<bool>,
    /// 整表替换
    pub prices: Option<Vec<ModelPrice>>,
    /// 整表替换
    pub sales: Option<Vec<SalesRecord>>,
}

/// POST /admin/api/billing/pricing — 热更新, 立即对新请求生效 (历史账单不受影响)
pub async fn api_pricing_patch(
    State(state): State<Arc<AppState>>,
    Json(body): Json<PricingPatch>,
) -> Response {
    if let Some(p) = &body.prices {
        let mut seen = std::collections::HashSet::new();
        for r in p {
            let m = r.model.trim();
            if m.is_empty() || m.len() > 120 {
                return bad("price rule: model required (<=120 chars)");
            }
            for v in [r.input_per_m, r.output_per_m, r.cache_read_per_m, r.cache_write_per_m] {
                if !v.is_finite() || v < 0.0 {
                    return bad(format!("price rule '{}': prices must be >= 0", m));
                }
                if v > 1e9 {
                    return bad(format!("price rule '{}': price too large", m));
                }
            }
            if !seen.insert(m.to_string()) {
                return bad(format!("duplicate price rule '{}'", m));
            }
        }
    }
    if let Some(s) = &body.sales {
        let mut seen = std::collections::HashSet::new();
        for r in s {
            let id = r.id.trim();
            if id.is_empty() || id.len() > 64 {
                return bad("sales: id required (<=64 chars)");
            }
            if r.commission_bps > 10_000 {
                return bad(format!("sales '{}': commission_bps must be 0..10000", id));
            }
            if !seen.insert(id.to_string()) {
                return bad(format!("duplicate sales id '{}'", id));
            }
        }
    }
    if let Some(b) = body.default_commission_bps {
        if b > 10_000 {
            return bad("default_commission_bps must be 0..10000");
        }
    }
    if let Some(tz) = body.tz_offset_minutes {
        if !(-14 * 60..=14 * 60).contains(&tz) {
            return bad("tz_offset_minutes out of range");
        }
    }
    if let Some(c) = &body.currency {
        let n = normalize_currency(c);
        if n.is_empty() || n.len() > 8 {
            return bad("invalid currency");
        }
    }

    let mut changed: Vec<&str> = Vec::new();
    let snapshot = {
        let mut cfg = state.config.lock();
        let b = &mut cfg.billing;
        if let Some(c) = body.currency {
            b.currency = normalize_currency(&c);
            changed.push("currency");
        }
        if let Some(tz) = body.tz_offset_minutes {
            b.tz_offset_minutes = tz;
            changed.push("tz_offset_minutes");
        }
        if let Some(v) = body.default_commission_bps {
            b.default_commission_bps = v;
            changed.push("default_commission_bps");
        }
        if let Some(v) = body.reject_unpriced {
            b.reject_unpriced = v;
            changed.push("reject_unpriced");
        }
        if let Some(p) = body.prices {
            b.prices = p
                .into_iter()
                .map(|mut r| {
                    r.model = r.model.trim().to_string();
                    r
                })
                .collect();
            changed.push("prices");
        }
        if let Some(s) = body.sales {
            b.sales = s
                .into_iter()
                .map(|mut r| {
                    r.id = r.id.trim().to_string();
                    r
                })
                .collect();
            changed.push("sales");
        }
        cfg.clone()
    };
    if changed.is_empty() {
        return bad("no fields to update");
    }
    if let Err(e) = config::save_config(&snapshot) {
        return internal(e);
    }
    state.audit.settings_op(&changed);
    Json(json!({"status": "ok", "pricing": pricing_view(&snapshot.billing)})).into_response()
}
