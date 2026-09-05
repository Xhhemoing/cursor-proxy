//! API key 管理 API.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

use crate::config::{self, ApiKeyRecord, AppConfig};
use crate::quota;
use crate::AppState;

/// GET /admin/api/keys — API key 列表 (脱敏 + 限额/用量 + RPM)
pub async fn api_keys_list(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let config = state.config.lock();
    let mut v = redacted_keys(&config, &state.key_usage);
    if let Some(arr) = v.get_mut("keys").and_then(|k| k.as_array_mut()) {
        for (i, item) in arr.iter_mut().enumerate() {
            if let Some(rec) = config.api_keys.get(i) {
                item["rpm"] = json!(state.metrics.key_rpm(&rec.key));
            }
        }
    }
    Json(v)
}

#[derive(Deserialize)]
pub struct AddKeyBody {
    /// 留空 = 服务端自动生成
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub token_limit: Option<u64>,
    #[serde(default)]
    pub request_limit: Option<u64>,
    #[serde(default)]
    pub expires_at: Option<i64>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub sales_id: Option<String>,
    #[serde(default)]
    pub rpm_limit: Option<u32>,
    #[serde(default)]
    pub max_concurrency: Option<u32>,
    /// 允许的模型组 id; 空 = 不限
    #[serde(default)]
    pub model_groups: Vec<String>,
}

/// 标签规范化: 去空白/去空/去重, 最多 20 个, 每个 ≤ 32 字符
pub fn normalize_tags(tags: Vec<String>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for t in tags {
        let t: String = t.trim().chars().take(32).collect();
        if t.is_empty() || out.contains(&t) {
            continue;
        }
        out.push(t);
        if out.len() >= 20 {
            break;
        }
    }
    out
}

/// 校验自定义 key 强度: >=16 字符, 非纯数字, 建议 sk- 前缀 (警告不拒绝)
fn validate_key_strength(key: &str) -> Result<(), String> {
    if key.len() < 16 {
        return Err("key must be at least 16 characters".into());
    }
    if key.chars().all(|c| c.is_ascii_digit()) {
        return Err("key must not be all digits".into());
    }
    Ok(())
}

/// POST /admin/api/keys — 追加 API key (支持自定义值/描述/过期)
/// 生成随机 key: `sk-cfp-` + 48 位 hex (192 bit 随机量, 来自两个 UUIDv4)
pub fn generate_key() -> String {
    let a = uuid::Uuid::new_v4().simple().to_string();
    let b = uuid::Uuid::new_v4().simple().to_string();
    format!("sk-cfp-{}{}", &a, &b[..16])
}

pub async fn api_keys_add(
    State(state): State<Arc<AppState>>,
    Json(body): Json<AddKeyBody>,
) -> Response {
    // key 留空 → 服务端自动生成; 响应里回传完整 key (仅此一次明文返回)
    let (key, generated) = {
        let k = body.key.trim().to_string();
        if k.is_empty() {
            (generate_key(), true)
        } else {
            (k, false)
        }
    };
    if let Err(msg) = validate_key_strength(&key) {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": msg}))).into_response();
    }
    let rec = ApiKeyRecord {
        key: key.clone(),
        name: body.name,
        description: body.description,
        enabled: true,
        token_limit: body.token_limit,
        request_limit: body.request_limit,
        expires_at: body.expires_at,
        tags: normalize_tags(body.tags),
        sales_id: body
            .sales_id
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        rpm_limit: body.rpm_limit,
        max_concurrency: body.max_concurrency,
        model_groups: body
            .model_groups
            .into_iter()
            .map(|g| g.trim().to_string())
            .filter(|g| !g.is_empty())
            .collect(),
    };
    let snapshot = {
        let mut config = state.config.lock();
        if config.api_keys.iter().any(|k| k.key == key) {
            return (
                StatusCode::CONFLICT,
                Json(json!({"error": "key already exists"})),
            )
                .into_response();
        }
        config.api_keys.push(rec);
        config.clone()
    };
    if let Err(e) = config::save_config(&snapshot) {
        let mut config = state.config.lock();
        config.api_keys.pop();
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response();
    }
    let prefix: String = key.chars().take(8).collect();
    state.audit.key_op(
        "create",
        &prefix,
        json!({"name": snapshot.api_keys.last().map(|k| k.name.clone())}),
    );
    Json(json!({
        "status": "ok",
        "key": key,
        "generated": generated,
        "index": snapshot.api_keys.len() - 1,
        "keys": redacted_keys(&snapshot, &state.key_usage),
    }))
    .into_response()
}

/// GET /admin/api/keys/:index/reveal — 返回完整 key (用于复制); 记审计
pub async fn api_keys_reveal(
    State(state): State<Arc<AppState>>,
    Path(index): Path<usize>,
) -> Response {
    let config = state.config.load();
    let Some(rec) = config.api_keys.get(index) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "key index not found"})),
        )
            .into_response();
    };
    let prefix: String = rec.key.chars().take(8).collect();
    state
        .audit
        .key_op("reveal", &prefix, json!({"index": index}));
    Json(json!({"index": index, "key": rec.key, "name": rec.name})).into_response()
}

#[derive(Deserialize, Default)]
pub struct PatchKeyBody {
    pub name: Option<String>,
    pub description: Option<String>,
    pub enabled: Option<bool>,
    pub token_limit: Option<Option<u64>>,
    pub request_limit: Option<Option<u64>>,
    pub expires_at: Option<Option<i64>>,
    pub tags: Option<Vec<String>>,
    /// Some(None) = 清除归属
    pub sales_id: Option<Option<String>>,
    pub rpm_limit: Option<Option<u32>>,
    pub max_concurrency: Option<Option<u32>>,
    /// Some(vec![]) = 清除限制 (不限)
    pub model_groups: Option<Vec<String>>,
}

/// POST /admin/api/keys/:index — 改名/描述/限额/启用/过期
pub async fn api_keys_patch(
    State(state): State<Arc<AppState>>,
    Path(index): Path<usize>,
    Json(body): Json<PatchKeyBody>,
) -> Response {
    let (snapshot, prefix) = {
        let mut config = state.config.lock();
        let Some(rec) = config.api_keys.get_mut(index) else {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "key index not found"})),
            )
                .into_response();
        };
        if let Some(name) = body.name {
            rec.name = name;
        }
        if let Some(desc) = body.description {
            rec.description = desc;
        }
        if let Some(enabled) = body.enabled {
            rec.enabled = enabled;
        }
        if let Some(lim) = body.token_limit {
            rec.token_limit = lim;
        }
        if let Some(lim) = body.request_limit {
            rec.request_limit = lim;
        }
        if let Some(exp) = body.expires_at {
            // 前端约定: 0 = 清除过期时间
            rec.expires_at = exp.filter(|&v| v > 0);
        }
        if let Some(tags) = body.tags {
            rec.tags = normalize_tags(tags);
        }
        if let Some(sid) = body.sales_id {
            rec.sales_id = sid.map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
        }
        if let Some(rpm) = body.rpm_limit {
            rec.rpm_limit = rpm.filter(|&v| v > 0);
        }
        if let Some(mc) = body.max_concurrency {
            rec.max_concurrency = mc.filter(|&v| v > 0);
        }
        if let Some(g) = body.model_groups {
            rec.model_groups = g
                .into_iter()
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect();
        }
        let prefix: String = rec.key.chars().take(8).collect();
        (config.clone(), prefix)
    };
    if let Err(e) = config::save_config(&snapshot) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response();
    }
    state
        .audit
        .key_op("patch", &prefix, json!({"index": index}));
    Json(json!({"status": "ok", "keys": redacted_keys(&snapshot, &state.key_usage)}))
        .into_response()
}

/// DELETE /admin/api/keys/:index
pub async fn api_keys_delete(
    State(state): State<Arc<AppState>>,
    Path(index): Path<usize>,
) -> Response {
    let removed = {
        let config = state.config.lock();
        if index >= config.api_keys.len() {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "key index not found"})),
            )
                .into_response();
        }
        config.api_keys[index].key.clone()
    };
    let snapshot = {
        let config = state.config.lock();
        let mut next = config.clone();
        next.api_keys.remove(index);
        next
    };
    if let Err(e) = config::save_config(&snapshot) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response();
    }
    *state.config.lock() = snapshot.clone();
    state.key_usage.remove(&removed);
    let prefix: String = removed.chars().take(8).collect();
    state
        .audit
        .key_op("delete", &prefix, json!({"index": index}));
    Json(json!({"status": "ok", "keys": redacted_keys(&snapshot, &state.key_usage)}))
        .into_response()
}

pub fn redacted_keys(config: &AppConfig, usage: &quota::KeyUsageStore) -> Value {
    let keys: Vec<Value> = config
        .api_keys
        .iter()
        .enumerate()
        .map(|(i, rec)| {
            let (tok, reqs) = usage.snapshot(&rec.key);
            quota::key_public(i, rec, tok, reqs)
        })
        .collect();
    json!({
        "keys": keys,
        "count": keys.len(),
    })
}

// ─── 批量导入 / 批量编辑 / 导出 ───

/// POST /admin/api/keys/import — 批量导入 API Key (JSON 数组或 CSV)
/// CSV 列: key,name,description,token_limit,request_limit,rpm_limit,max_concurrency,expires_at,tags,sales_id
pub async fn api_keys_import(State(state): State<Arc<AppState>>, body: String) -> Response {
    let records: Vec<ApiKeyRecord> = match serde_json::from_str::<Vec<ApiKeyRecord>>(&body) {
        Ok(recs) => recs,
        Err(_) => {
            // CSV 解析
            let mut lines = body.lines();
            let header = match lines.next() {
                Some(h) => h.to_lowercase(),
                None => {
                    return (StatusCode::BAD_REQUEST, Json(json!({"error": "empty CSV"})))
                        .into_response();
                }
            };
            let cols: Vec<&str> = header.split(',').map(|s| s.trim()).collect();
            let find = |names: &[&str]| names.iter().find_map(|n| cols.iter().position(|c| c == n));

            let key_idx = match find(&["key", "api_key", "token"]) {
                Some(i) => i,
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error": "CSV must have a 'key' column"})),
                    )
                        .into_response()
                }
            };
            let name_idx = find(&["name"]);
            let desc_idx = find(&["description", "desc"]);
            let tok_idx = find(&["token_limit", "tokens"]);
            let req_idx = find(&["request_limit", "requests"]);
            let rpm_idx = find(&["rpm_limit", "rpm"]);
            let conc_idx = find(&["max_concurrency", "concurrency"]);
            let exp_idx = find(&["expires_at", "expires"]);
            let tags_idx = find(&["tags"]);
            let sales_idx = find(&["sales_id", "sales"]);

            let mut recs = Vec::new();
            for line in lines {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let parts: Vec<&str> = line.split(',').collect();
                let get = |idx: Option<usize>| {
                    idx.and_then(|i| parts.get(i)).map(|s| s.trim().to_string())
                };
                let key = match get(Some(key_idx)) {
                    Some(k) if !k.is_empty() => k,
                    _ => continue,
                };
                let parse_u64 = |idx: Option<usize>| {
                    get(idx)
                        .and_then(|s| s.parse::<u64>().ok())
                        .filter(|&v| v > 0)
                };
                let parse_u32 = |idx: Option<usize>| {
                    get(idx)
                        .and_then(|s| s.parse::<u32>().ok())
                        .filter(|&v| v > 0)
                };
                let parse_i64 = |idx: Option<usize>| {
                    get(idx)
                        .and_then(|s| s.parse::<i64>().ok())
                        .filter(|&v| v > 0)
                };
                let tags = get(tags_idx)
                    .map(|s| {
                        s.split(';')
                            .map(|t| t.trim().to_string())
                            .filter(|t| !t.is_empty())
                            .collect()
                    })
                    .unwrap_or_default();
                recs.push(ApiKeyRecord {
                    key,
                    name: get(name_idx).unwrap_or_default(),
                    description: get(desc_idx).unwrap_or_default(),
                    enabled: true,
                    token_limit: parse_u64(tok_idx),
                    request_limit: parse_u64(req_idx),
                    expires_at: parse_i64(exp_idx),
                    tags,
                    sales_id: get(sales_idx).filter(|s| !s.is_empty()),
                    rpm_limit: parse_u32(rpm_idx),
                    max_concurrency: parse_u32(conc_idx),
                    model_groups: Vec::new(),
                });
            }
            if recs.is_empty() {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "no valid rows parsed"})),
                )
                    .into_response();
            }
            recs
        }
    };

    let mut imported = 0usize;
    let mut skipped = 0usize;
    let mut errors = Vec::new();
    // 事务性导入: 先验证全部, 再一次性写入
    {
        let mut config = state.config.lock();
        // 第一遍: 验证
        let mut valid = Vec::new();
        for rec in records {
            if rec.key.len() < 16 {
                errors.push(json!({"key_prefix": &rec.key[..rec.key.len().min(8)], "error": "key too short"}));
                continue;
            }
            if config.api_keys.iter().any(|k| k.key == rec.key) {
                skipped += 1;
                continue;
            }
            valid.push(rec);
        }
        // 第二遍: 一次性写入
        for rec in valid {
            config.api_keys.push(rec);
            imported += 1;
        }
        if imported > 0 {
            if let Err(e) = config::save_config(&config) {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        }
    }
    state.audit.key_op(
        "import",
        "batch",
        json!({"imported": imported, "skipped": skipped}),
    );
    Json(json!({"status": "ok", "imported": imported, "skipped": skipped, "errors": errors}))
        .into_response()
}

#[derive(Deserialize)]
pub struct KeysBatchEditBody {
    /// 按 index 批量
    pub indices: Vec<usize>,
    /// 要修改的字段 (None = 不改)
    pub enabled: Option<bool>,
    pub rpm_limit: Option<Option<u32>>,
    pub max_concurrency: Option<Option<u32>>,
    pub token_limit: Option<Option<u64>>,
    pub request_limit: Option<Option<u64>>,
    pub tags: Option<Vec<String>>,
    pub sales_id: Option<Option<String>>,
}

/// POST /admin/api/keys/batch-edit — 批量修改 key 字段
pub async fn api_keys_batch_edit(
    State(state): State<Arc<AppState>>,
    Json(body): Json<KeysBatchEditBody>,
) -> Response {
    let mut updated = 0usize;
    let snapshot = {
        let mut config = state.config.lock();
        for &idx in &body.indices {
            let Some(rec) = config.api_keys.get_mut(idx) else {
                continue;
            };
            if let Some(en) = body.enabled {
                rec.enabled = en;
            }
            if let Some(rpm) = body.rpm_limit {
                rec.rpm_limit = rpm.filter(|&v| v > 0);
            }
            if let Some(mc) = body.max_concurrency {
                rec.max_concurrency = mc.filter(|&v| v > 0);
            }
            if let Some(tl) = body.token_limit {
                rec.token_limit = tl.filter(|&v| v > 0);
            }
            if let Some(rl) = body.request_limit {
                rec.request_limit = rl.filter(|&v| v > 0);
            }
            if let Some(ref tags) = body.tags {
                rec.tags = normalize_tags(tags.clone());
            }
            if let Some(ref sid) = body.sales_id {
                rec.sales_id = sid
                    .clone()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty());
            }
            updated += 1;
        }
        config.clone()
    };
    if let Err(e) = config::save_config(&snapshot) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response();
    }
    state.audit.key_op(
        "batch_edit",
        "batch",
        json!({"updated": updated, "indices": body.indices}),
    );
    Json(json!({"status": "ok", "updated": updated, "keys": redacted_keys(&snapshot, &state.key_usage)})).into_response()
}

/// GET /admin/api/keys/export — 导出全部 key (含完整 key, 用于备份/迁移)
pub async fn api_keys_export(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let config = state.config.load();
    state
        .audit
        .key_op("export", "all", json!({"count": config.api_keys.len()}));
    Json(json!({
        "exported_at": chrono::Utc::now().to_rfc3339(),
        "count": config.api_keys.len(),
        "keys": config.api_keys,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_strength_rejects_weak() {
        assert!(validate_key_strength("short").is_err());
        assert!(validate_key_strength("1234567890123456").is_err());
        assert!(validate_key_strength("sk-abcdefgh12345678").is_ok());
    }

    #[test]
    fn generated_key_is_strong_and_unique() {
        let a = generate_key();
        let b = generate_key();
        assert!(a.starts_with("sk-cfp-"));
        assert_eq!(a.len(), "sk-cfp-".len() + 48);
        assert_ne!(a, b);
        assert!(validate_key_strength(&a).is_ok());
    }

    #[test]
    fn keys_are_redacted() {
        let cfg = AppConfig {
            host: "0.0.0.0".into(),
            port: 8800,
            api_keys: vec![ApiKeyRecord::from_raw("sk-secret-1234567".into())],
            backend: "https://api2.cursor.sh".into(),
            timeout_s: 600,
            log_file: "proxy.log".into(),
            default_model: "kimi-k3".into(),
            max_concurrency_per_account: 8,
            admin_token: String::new(),
            acquire_wait_ms: 0,
            billing: crate::config::BillingConfig::default(),
            proxy: crate::proxypool::ProxyPoolConfig::default(),
        };
        let usage = quota::KeyUsageStore::new();
        usage.add("sk-secret-1234567", 42);
        let v = redacted_keys(&cfg, &usage);
        assert_eq!(v["count"], 1);
        assert_eq!(v["keys"][0]["prefix"], "sk-secre");
        assert_eq!(v["keys"][0]["length"], 17);
        assert_eq!(v["keys"][0]["used_tokens"], 42);
        assert!(v.to_string().contains("sk-secre"));
        assert!(!v.to_string().contains("sk-secret-1234567"));
    }
}
