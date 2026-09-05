//! 模型设置 admin API: 手动定价 / 层级 / 停用 + 模型组.
//!
//! - `GET  /admin/api/models`                 全部: 已手动设置的 + 内置表 + 账本里出现过的模型, 标注来源
//! - `POST /admin/api/models`                 upsert 单个 {model, input_per_m, output_per_m, cache_read_per_m, cache_write_per_m, tier, enabled, note}
//! - `POST /admin/api/models/bulk`            整表替换 {models:[...]}
//! - `POST /admin/api/models/import-builtin`  把内置官方表全部写成手动条目 (便于逐个改)
//! - `DELETE /admin/api/models/:model`        删除手动条目 (回落内置)
//! - `GET  /admin/api/models/groups` · `POST /admin/api/models/groups` · `DELETE /admin/api/models/groups/:id`
//! - `GET  /admin/api/models/resolve?model=x` 看某模型最终生效的价格/层级/所属组 (调试)

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::cards;
use crate::models::{registry, ModelEntry, ModelGroup};
use crate::AppState;

fn bad(msg: impl Into<String>) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({"error": msg.into()}))).into_response()
}

fn valid_tier(t: &str) -> bool {
    t.is_empty() || [cards::TIER_ECONOMY, cards::TIER_STANDARD, cards::TIER_FLAGSHIP].contains(&t)
}

fn validate_entry(e: &ModelEntry) -> Result<(), String> {
    let m = e.model.trim();
    if m.is_empty() || m.len() > 120 {
        return Err("model required (<=120 chars)".into());
    }
    for v in [
        e.input_per_m,
        e.output_per_m,
        e.cache_read_per_m,
        e.cache_write_per_m,
    ] {
        if !v.is_finite() || v < 0.0 || v > 1e6 {
            return Err(format!("model '{}': prices must be 0..1e6", m));
        }
    }
    if !valid_tier(&e.tier) {
        return Err(format!("model '{}': tier must be economy|standard|flagship|\"\"", m));
    }
    Ok(())
}

/// 账本里出现过的模型 (最近 30 天), 用于把「用过但没设置」的模型列出来
fn seen_models(state: &AppState) -> Vec<(String, u64)> {
    let Ok(conn) = state.ledger.reader() else {
        return vec![];
    };
    let since = chrono::Utc::now().timestamp_millis() - 30 * 86_400_000;
    let mut out = vec![];
    if let Ok(mut st) = conn.prepare(
        "SELECT model, COUNT(*) FROM billing_records WHERE ts_ms >= ?1 GROUP BY model ORDER BY 2 DESC",
    ) {
        if let Ok(rows) = st.query_map([since], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))) {
            for r in rows.flatten() {
                out.push((r.0, r.1.max(0) as u64));
            }
        }
    }
    out
}

fn row(model: &str, source: &str, seen: u64) -> Value {
    let reg = registry();
    let manual = reg.get_exact(model);
    let (i, o, c, w) = cards::model_price(model);
    json!({
        "model": model,
        "source": source, // manual | builtin | seen
        "input_per_m": i,
        "output_per_m": o,
        "cache_read_per_m": c,
        "cache_write_per_m": w,
        "tier": cards::model_tier(model),
        "tier_manual": manual.as_ref().map(|e| e.tier.clone()).unwrap_or_default(),
        "enabled": manual.as_ref().map(|e| e.enabled).unwrap_or(true),
        "note": manual.as_ref().map(|e| e.note.clone()).unwrap_or_default(),
        "groups": reg.groups_of(model),
        "requests_30d": seen,
    })
}

pub async fn api_models_list(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let reg = registry();
    let snap = reg.snapshot();
    let mut names: Vec<(String, &'static str)> = vec![];
    for e in &snap.models {
        names.push((e.model.clone(), "manual"));
    }
    for (m, ..) in cards::builtin_table() {
        if !names.iter().any(|(n, _)| *n == m) {
            names.push((m, "builtin"));
        }
    }
    let seen = seen_models(&state);
    for (m, _) in &seen {
        if !names.iter().any(|(n, _)| n == m) {
            names.push((m.clone(), "seen"));
        }
    }
    let rows: Vec<Value> = names
        .iter()
        .map(|(m, src)| {
            let n = seen.iter().find(|(x, _)| x == m).map(|(_, c)| *c).unwrap_or(0);
            row(m, src, n)
        })
        .collect();
    Json(json!({
        "models": rows,
        "manual_count": snap.models.len(),
        "groups": snap.groups,
        "tiers": [cards::TIER_ECONOMY, cards::TIER_STANDARD, cards::TIER_FLAGSHIP],
    }))
}

#[derive(Deserialize)]
pub struct EntryBody {
    pub model: String,
    pub input_per_m: Option<f64>,
    pub output_per_m: Option<f64>,
    pub cache_read_per_m: Option<f64>,
    pub cache_write_per_m: Option<f64>,
    pub tier: Option<String>,
    pub enabled: Option<bool>,
    pub note: Option<String>,
}

/// upsert: 缺省字段 = 已有手动条目的值, 否则当前生效值 (内置)
pub async fn api_models_upsert(
    State(state): State<Arc<AppState>>,
    Json(b): Json<EntryBody>,
) -> Response {
    let model = b.model.trim().to_string();
    if model.is_empty() {
        return bad("model required");
    }
    let reg = registry();
    let (i, o, c, w) = cards::model_price(&model);
    let cur = reg.get_exact(&model).unwrap_or(ModelEntry {
        model: model.clone(),
        input_per_m: i,
        output_per_m: o,
        cache_read_per_m: c,
        cache_write_per_m: w,
        tier: cards::model_tier(&model).to_string(),
        enabled: true,
        note: String::new(),
    });
    let e = ModelEntry {
        model: model.clone(),
        input_per_m: b.input_per_m.unwrap_or(cur.input_per_m),
        output_per_m: b.output_per_m.unwrap_or(cur.output_per_m),
        cache_read_per_m: b.cache_read_per_m.unwrap_or(cur.cache_read_per_m),
        cache_write_per_m: b.cache_write_per_m.unwrap_or(cur.cache_write_per_m),
        tier: b.tier.unwrap_or(cur.tier),
        enabled: b.enabled.unwrap_or(cur.enabled),
        note: b.note.unwrap_or(cur.note),
    };
    if let Err(m) = validate_entry(&e) {
        return bad(m);
    }
    if let Err(err) = reg.upsert_model(e) {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": err.to_string()}))).into_response();
    }
    state.audit.key_op("model_upsert", &model, json!({}));
    Json(row(&model, "manual", 0)).into_response()
}

#[derive(Deserialize)]
pub struct BulkBody {
    pub models: Vec<ModelEntry>,
}

pub async fn api_models_bulk(
    State(state): State<Arc<AppState>>,
    Json(b): Json<BulkBody>,
) -> Response {
    let mut seen = std::collections::HashSet::new();
    for e in &b.models {
        if let Err(m) = validate_entry(e) {
            return bad(m);
        }
        if !seen.insert(e.model.trim().to_string()) {
            return bad(format!("duplicate model '{}'", e.model));
        }
    }
    let models: Vec<ModelEntry> = b
        .models
        .into_iter()
        .map(|mut e| {
            e.model = e.model.trim().to_string();
            e
        })
        .collect();
    let n = models.len();
    if let Err(err) = registry().replace_models(models) {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": err.to_string()}))).into_response();
    }
    state.audit.key_op("models_bulk", "*", json!({"count": n}));
    Json(json!({"ok": true, "count": n})).into_response()
}

/// 把内置表导入成手动条目 (已有手动条目不覆盖, 除非 ?overwrite=1)
#[derive(Deserialize)]
pub struct ImportQuery {
    #[serde(default)]
    pub overwrite: Option<u8>,
}

pub async fn api_models_import_builtin(
    State(state): State<Arc<AppState>>,
    Query(q): Query<ImportQuery>,
) -> Response {
    let reg = registry();
    let overwrite = q.overwrite == Some(1);
    let mut n = 0usize;
    for (m, i, o, c, w, tier) in cards::builtin_table() {
        if !overwrite && reg.get_exact(&m).is_some() {
            continue;
        }
        let e = ModelEntry {
            model: m,
            input_per_m: i,
            output_per_m: o,
            cache_read_per_m: c,
            cache_write_per_m: w,
            tier: tier.to_string(),
            enabled: true,
            note: "builtin".into(),
        };
        if reg.upsert_model(e).is_ok() {
            n += 1;
        }
    }
    state.audit.key_op("models_import_builtin", "*", json!({"count": n}));
    Json(json!({"ok": true, "written": n})).into_response()
}

pub async fn api_models_delete(
    State(state): State<Arc<AppState>>,
    Path(model): Path<String>,
) -> Response {
    match registry().delete_model(&model) {
        Ok(true) => {
            state.audit.key_op("model_delete", &model, json!({}));
            Json(json!({"ok": true})).into_response()
        }
        Ok(false) => (StatusCode::NOT_FOUND, Json(json!({"error": "no manual entry"}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    }
}

#[derive(Deserialize)]
pub struct ResolveQuery {
    pub model: String,
}

pub async fn api_models_resolve(Query(q): Query<ResolveQuery>) -> impl IntoResponse {
    let m = q.model.trim();
    let reg = registry();
    let hit = reg.lookup(m);
    Json(json!({
        "model": m,
        "resolved_from": hit.as_ref().map(|e| e.model.clone()),
        "price": row(m, if hit.is_some() { "manual" } else { "builtin" }, 0),
        "disabled": reg.is_disabled(m),
    }))
}

// ── 模型组 ──

pub async fn api_groups_list() -> impl IntoResponse {
    let reg = registry();
    let groups: Vec<Value> = reg
        .groups()
        .into_iter()
        .map(|g| {
            // 展开: 组内成员在「已知模型」(手动+内置) 中命中了哪些
            let known: Vec<String> = {
                let snap = reg.snapshot();
                let mut v: Vec<String> = snap.models.iter().map(|e| e.model.clone()).collect();
                for (m, ..) in cards::builtin_table() {
                    if !v.contains(&m) {
                        v.push(m);
                    }
                }
                v
            };
            let resolved: Vec<&String> = known.iter().filter(|m| g.contains(m)).collect();
            json!({
                "id": g.id, "name": g.name, "members": g.members, "note": g.note,
                "resolved_known": resolved,
            })
        })
        .collect();
    Json(json!({"groups": groups}))
}

#[derive(Deserialize)]
pub struct GroupBody {
    pub id: String,
    pub name: Option<String>,
    pub members: Option<Vec<String>>,
    pub note: Option<String>,
}

pub async fn api_groups_upsert(
    State(state): State<Arc<AppState>>,
    Json(b): Json<GroupBody>,
) -> Response {
    let id = b.id.trim().to_string();
    if id.is_empty() || id.len() > 64 || !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        return bad("group id: 1-64 chars, [a-zA-Z0-9_-]");
    }
    let reg = registry();
    let cur = reg.get_group(&id).unwrap_or(ModelGroup {
        id: id.clone(),
        name: id.clone(),
        members: vec![],
        note: String::new(),
    });
    let members: Vec<String> = b
        .members
        .unwrap_or(cur.members)
        .into_iter()
        .map(|m| m.trim().to_string())
        .filter(|m| !m.is_empty())
        .collect();
    let g = ModelGroup {
        id: id.clone(),
        name: b.name.map(|n| n.trim().to_string()).filter(|n| !n.is_empty()).unwrap_or(cur.name),
        members,
        note: b.note.unwrap_or(cur.note),
    };
    if let Err(e) = reg.upsert_group(g.clone()) {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response();
    }
    state.audit.key_op("model_group_upsert", &id, json!({"members": g.members.len()}));
    Json(json!({"ok": true, "group": g})).into_response()
}

pub async fn api_groups_delete(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    // 引用检查: key / 套餐还在用的组不让删 (删了会让引用方全部 403)
    let cfg = state.config.load();
    let used_by_keys: Vec<String> = cfg
        .api_keys
        .iter()
        .filter(|k| k.model_groups.contains(&id))
        .map(|k| k.name.clone())
        .collect();
    let used_by_plans: Vec<String> = state
        .card_store
        .list_plans()
        .into_iter()
        .filter(|p| p.model_groups.contains(&id))
        .map(|p| p.id)
        .collect();
    if !used_by_keys.is_empty() || !used_by_plans.is_empty() {
        return (
            StatusCode::CONFLICT,
            Json(json!({"error": "group in use", "keys": used_by_keys, "plans": used_by_plans})),
        )
            .into_response();
    }
    match registry().delete_group(&id) {
        Ok(true) => {
            state.audit.key_op("model_group_delete", &id, json!({}));
            Json(json!({"ok": true})).into_response()
        }
        Ok(false) => (StatusCode::NOT_FOUND, Json(json!({"error": "group not found"}))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    }
}

// ── 上游可用模型 / litellm 一键同步 ──

/// GET /admin/api/models/upstream — 用号池第一个可用号拉 Cursor 官方可用模型列表
pub async fn api_models_upstream(State(state): State<Arc<AppState>>) -> Response {
    let acc = state
        .pool
        .accounts()
        .into_iter()
        .find(|a| a.enabled)
        .or_else(|| state.pool.accounts().into_iter().next());
    let Some(acc) = acc else {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "no accounts"}))).into_response();
    };
    match state.cursor.available_models(&acc.access_token, &acc.machine_id).await {
        Ok(v) => {
            let names = extract_model_names(&v);
            let reg = registry();
            let rows: Vec<Value> = names
                .iter()
                .map(|m| {
                    let manual = reg.get_exact(m).is_some();
                    json!({
                        "model": m,
                        "in_registry": manual,
                        "tier": cards::model_tier(m),
                        "price": cards::model_price(m),
                    })
                })
                .collect();
            Json(json!({"account": acc.id, "models": rows, "count": rows.len()})).into_response()
        }
        Err(e) => (StatusCode::BAD_GATEWAY, Json(json!({"error": e.to_string()}))).into_response(),
    }
}

/// AvailableModels 响应的模型名提取: 兼容 {"models":[{"name":..}|".."]} / {"modelIds":[..]} 等
fn extract_model_names(v: &Value) -> Vec<String> {
    let mut out: Vec<String> = vec![];
    let mut walk = |arr: &Vec<Value>| {
        for it in arr {
            let n = it
                .as_str()
                .map(|s| s.to_string())
                .or_else(|| it.get("name").and_then(|x| x.as_str()).map(|s| s.to_string()))
                .or_else(|| it.get("id").and_then(|x| x.as_str()).map(|s| s.to_string()));
            if let Some(n) = n {
                if !n.is_empty() && !out.contains(&n) {
                    out.push(n);
                }
            }
        }
    };
    for key in ["models", "modelIds", "model_ids", "availableModels", "available_models"] {
        if let Some(arr) = v.get(key).and_then(|x| x.as_array()) {
            walk(arr);
        }
    }
    out
}

const LITELLM_URLS: &[&str] = &[
    "https://fastly.jsdelivr.net/gh/BerriAI/litellm@main/model_prices_and_context_window.json",
    "https://cdn.jsdelivr.net/gh/BerriAI/litellm@main/model_prices_and_context_window.json",
    "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json",
];

/// Cursor 模型名 → litellm 目录名候选 (第一个命中即采用)
fn litellm_candidates(model: &str) -> Vec<String> {
    let mut c: Vec<String> = vec![model.to_string()];
    if let Some(base) = model.strip_suffix("-fast") {
        c.push(base.to_string());
    }
    if let Some(rest) = model.strip_prefix("cursor-") {
        c.push(rest.to_string());
    }
    // 家族映射 (与 Python 版 pricing.py FAMILIES 一致)
    for (prefix, vendor) in [
        ("claude-4.5-sonnet", "claude-sonnet-4-5"),
        ("claude-4.5-haiku", "claude-haiku-4-5"),
        ("claude-4.5-opus", "claude-opus-4-5"),
        ("gpt-5.6-sol", "gpt-5.6"),
        ("gpt-5", "gpt-5"),
        ("grok", "grok-4"),
    ] {
        if model.starts_with(prefix) {
            c.push(vendor.to_string());
        }
    }
    c
}

/// POST /admin/api/models/sync-litellm — 拉 litellm 价格表, 为已知模型填价.
/// 只填「注册表里没有手动条目」的模型; ?overwrite=1 才覆盖手动条目.
pub async fn api_models_sync_litellm(State(state): State<Arc<AppState>>, Query(q): Query<ImportQuery>) -> Response {
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()
    {
        Ok(c) => c,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": e.to_string()}))).into_response(),
    };
    let mut table: Option<Value> = None;
    let mut errs: Vec<String> = vec![];
    for url in LITELLM_URLS {
        match client.get(*url).send().await {
            Ok(r) if r.status().is_success() => match r.json::<Value>().await {
                Ok(v) => {
                    table = Some(v);
                    break;
                }
                Err(e) => errs.push(format!("{url}: parse {e}")),
            },
            Ok(r) => errs.push(format!("{url}: HTTP {}", r.status())),
            Err(e) => errs.push(format!("{url}: {e}")),
        }
    }
    let Some(table) = table else {
        return (StatusCode::BAD_GATEWAY, Json(json!({"error": "all litellm mirrors failed", "detail": errs}))).into_response();
    };
    let price_of = |name: &str| -> Option<(f64, f64, f64, f64)> {
        let e = table.get(name)?;
        let g = |k: &str| e.get(k).and_then(|v| v.as_f64()).unwrap_or(0.0) * 1e6; // $/token → $/1M
        Some((g("input_cost_per_token"), g("output_cost_per_token"), g("cache_read_input_token_cost"), g("cache_creation_input_token_cost")))
    };
    // 目标模型集合: 内置表 + 注册表已有 + 账本出现过的
    let mut targets: Vec<String> = cards::builtin_table().iter().map(|(m, ..)| m.clone()).collect();
    for e in &registry().snapshot().models {
        if !targets.contains(&e.model) {
            targets.push(e.model.clone());
        }
    }
    for (m, _) in seen_models(&state) {
        if !targets.contains(&m) {
            targets.push(m);
        }
    }
    let overwrite = q.overwrite == Some(1);
    let reg = registry();
    let mut filled: Vec<String> = vec![];
    let mut skipped_manual: Vec<String> = vec![];
    let mut no_price: Vec<String> = vec![];
    for m in &targets {
        if !overwrite && reg.get_exact(m).is_some() {
            skipped_manual.push(m.clone());
            continue;
        }
        let hit = litellm_candidates(m).iter().find_map(|cand| price_of(cand).map(|p| (cand.clone(), p)));
        match hit {
            Some((src, (i, o, c, w))) => {
                let cur = reg.get_exact(m);
                let e = ModelEntry {
                    model: m.clone(),
                    input_per_m: (i * 100.0).round() / 100.0,
                    output_per_m: (o * 100.0).round() / 100.0,
                    cache_read_per_m: (c * 100.0).round() / 100.0,
                    cache_write_per_m: (w * 100.0).round() / 100.0,
                    tier: cur.as_ref().map(|x| x.tier.clone()).unwrap_or_default(),
                    enabled: cur.as_ref().map(|x| x.enabled).unwrap_or(true),
                    note: format!("litellm:{src}"),
                };
                if reg.upsert_model(e).is_ok() {
                    filled.push(m.clone());
                }
            }
            None => no_price.push(m.clone()),
        }
    }
    state.audit.key_op("models_sync_litellm", "*", json!({"filled": filled.len()}));
    Json(json!({
        "ok": true,
        "filled": filled,
        "skipped_manual": skipped_manual,
        "no_price": no_price,
        "note": "litellm 是公开目录价, 与 Cursor 官方口径可能不同; 关键模型建议用官方仪表盘对账后手动改",
    }))
    .into_response()
}
