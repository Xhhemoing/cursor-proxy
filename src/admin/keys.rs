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

/// GET /admin/api/keys — API key 列表 (脱敏 + 限额/用量)
pub async fn api_keys_list(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let config = state.config.load();
    Json(redacted_keys(&config, &state.key_usage))
}

#[derive(Deserialize)]
pub struct AddKeyBody {
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
pub async fn api_keys_add(
    State(state): State<Arc<AppState>>,
    Json(body): Json<AddKeyBody>,
) -> Response {
    let key = body.key.trim().to_string();
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
    };
    let snapshot = {
        let mut config = (**state.config.load()).clone();
        if config.api_keys.iter().any(|k| k.key == key) {
            return (
                StatusCode::CONFLICT,
                Json(json!({"error": "key already exists"})),
            )
                .into_response();
        }
        config.api_keys.push(rec);
        config
    };
    if let Err(e) = config::save_config(&snapshot) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response();
    }
    // 热更新内存配置
    state.config.store(std::sync::Arc::new(snapshot.clone()));
    let prefix: String = key.chars().take(8).collect();
    state.audit.key_op(
        "create",
        &prefix,
        json!({"name": snapshot.api_keys.last().map(|k| k.name.clone())}),
    );
    Json(json!({"status": "ok", "keys": redacted_keys(&snapshot, &state.key_usage)})).into_response()
}

#[derive(Deserialize, Default)]
pub struct PatchKeyBody {
    pub name: Option<String>,
    pub description: Option<String>,
    pub enabled: Option<bool>,
    pub token_limit: Option<Option<u64>>,
    pub request_limit: Option<Option<u64>>,
    pub expires_at: Option<Option<i64>>,
}

/// POST /admin/api/keys/:index — 改名/描述/限额/启用/过期
pub async fn api_keys_patch(
    State(state): State<Arc<AppState>>,
    Path(index): Path<usize>,
    Json(body): Json<PatchKeyBody>,
) -> Response {
    let (snapshot, prefix) = {
        let mut config = (**state.config.load()).clone();
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
        let prefix: String = rec.key.chars().take(8).collect();
        (config, prefix)
    };
    if let Err(e) = config::save_config(&snapshot) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response();
    }
    state.config.store(std::sync::Arc::new(snapshot.clone()));
    state.audit.key_op("patch", &prefix, json!({"index": index}));
    Json(json!({"status": "ok", "keys": redacted_keys(&snapshot, &state.key_usage)})).into_response()
}

/// DELETE /admin/api/keys/:index
pub async fn api_keys_delete(
    State(state): State<Arc<AppState>>,
    Path(index): Path<usize>,
) -> Response {
    let removed = {
        let config = state.config.load();
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
        let mut next = (**state.config.load()).clone();
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
    state.config.store(std::sync::Arc::new(snapshot.clone()));
    state.key_usage.remove(&removed);
    let prefix: String = removed.chars().take(8).collect();
    state.audit.key_op("delete", &prefix, json!({"index": index}));
    Json(json!({"status": "ok", "keys": redacted_keys(&snapshot, &state.key_usage)})).into_response()
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
    fn keys_are_redacted() {
        let cfg = AppConfig {
            host: "0.0.0.0".into(),
            port: 8800,
            api_keys: vec![ApiKeyRecord::from_raw("sk-secret-1234567".into())],
            backend: "https://api2.cursor.sh".into(),
            timeout_s: 600,
            log_file: "proxy.log".into(),
            default_model: "grok-4.6".into(),
            max_concurrency_per_account: 8,
            admin_token: String::new(),
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
