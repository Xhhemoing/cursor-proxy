//! Cursor dashboard 额度探测 + 本地 API key 限额.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::ApiKeyRecord;
use crate::cursor::{cursor_headers, CursorClient, CursorError};

const DASHBOARD_PATH: &str = "/aiserver.v1.DashboardService";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QuotaSnapshot {
    pub plan: Option<String>,
    pub usage_percent: Option<f64>,
    pub has_available_usage: Option<bool>,
    pub auto_percent_used: Option<f64>,
    pub api_percent_used: Option<f64>,
    pub total_percent_used: Option<f64>,
    pub display_message: Option<String>,
    pub next_reset_at: Option<f64>,
    pub period_start_at: Option<f64>,
    pub billing_cycle_start_at: Option<f64>,
    pub billing_cycle_end_at: Option<f64>,
    pub checked_at: Option<f64>,
    pub error: Option<String>,
}

impl QuotaSnapshot {
    pub fn merge_keep_last_good(&mut self, incoming: QuotaSnapshot) {
        fn keep<T: Clone>(dst: &mut Option<T>, src: Option<T>) {
            if src.is_some() {
                *dst = src;
            }
        }
        keep(&mut self.plan, incoming.plan);
        keep(&mut self.usage_percent, incoming.usage_percent);
        keep(&mut self.has_available_usage, incoming.has_available_usage);
        keep(&mut self.auto_percent_used, incoming.auto_percent_used);
        keep(&mut self.api_percent_used, incoming.api_percent_used);
        keep(&mut self.total_percent_used, incoming.total_percent_used);
        keep(&mut self.display_message, incoming.display_message);
        keep(&mut self.next_reset_at, incoming.next_reset_at);
        keep(&mut self.period_start_at, incoming.period_start_at);
        keep(&mut self.billing_cycle_start_at, incoming.billing_cycle_start_at);
        keep(&mut self.billing_cycle_end_at, incoming.billing_cycle_end_at);
        if incoming.checked_at.is_some() {
            self.checked_at = incoming.checked_at;
        }
        self.error = incoming.error;
    }

    pub fn round_one_decimal(&mut self) {
        fn r(v: &mut Option<f64>) {
            if let Some(x) = v {
                *x = (*x * 10.0).round() / 10.0;
            }
        }
        r(&mut self.usage_percent);
        r(&mut self.auto_percent_used);
        r(&mut self.api_percent_used);
        r(&mut self.total_percent_used);
    }
}

pub fn as_unix(value: &Value) -> Option<f64> {
    match value {
        Value::Null => None,
        Value::Number(n) => {
            let mut number = n.as_f64()?;
            if number <= 0.0 {
                return None;
            }
            if number > 1e12 {
                number /= 1000.0;
            }
            Some(number)
        }
        Value::String(text) => {
            let text = text.trim();
            if text.is_empty() {
                return None;
            }
            if let Ok(mut number) = text.parse::<f64>() {
                if number <= 0.0 {
                    return None;
                }
                if number > 1e12 {
                    number /= 1000.0;
                }
                return Some(number);
            }
            let iso = if text.ends_with('Z') {
                format!("{}+00:00", &text[..text.len() - 1])
            } else {
                text.to_string()
            };
            chrono::DateTime::parse_from_rfc3339(text)
                .or_else(|_| chrono::DateTime::parse_from_rfc3339(&iso))
                .ok()
                .map(|dt| dt.timestamp() as f64)
                .or_else(|| {
                    chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f")
                        .or_else(|_| chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S"))
                        .ok()
                        .map(|dt| dt.and_utc().timestamp() as f64)
                })
        }
        _ => None,
    }
}

pub fn sand_usage_percent(value: &Value) -> Option<f64> {
    match value {
        Value::Null | Value::Bool(false) => None,
        Value::String(s) if s.is_empty() => None,
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn now_unix() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

pub fn parse_quota(sand: &Value, period: &Value) -> QuotaSnapshot {
    let mut out = QuotaSnapshot {
        checked_at: Some(now_unix()),
        ..Default::default()
    };
    if let Some(obj) = sand.as_object() {
        out.usage_percent = obj.get("usagePercent").and_then(sand_usage_percent);
        out.has_available_usage = obj.get("hasAvailableUsage").and_then(|v| v.as_bool());
        out.plan = obj
            .get("includedUsageSuperGrokPlan")
            .or_else(|| obj.get("grokPlanLabel"))
            .or_else(|| obj.get("planName"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        out.next_reset_at = obj
            .get("nextResetTimestampUtc")
            .or_else(|| obj.get("nextResetAt"))
            .or_else(|| obj.get("resetAt"))
            .and_then(as_unix);
        out.period_start_at = obj
            .get("currentPeriodStart")
            .or_else(|| obj.get("periodStart"))
            .and_then(as_unix);
    }
    if let Some(obj) = period.as_object() {
        let plan = obj.get("planUsage").and_then(|v| v.as_object());
        out.display_message = obj
            .get("displayMessage")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        if let Some(plan) = plan {
            out.auto_percent_used = plan.get("autoPercentUsed").and_then(sand_usage_percent);
            out.api_percent_used = plan.get("apiPercentUsed").and_then(sand_usage_percent);
            out.total_percent_used = plan.get("totalPercentUsed").and_then(sand_usage_percent);
            if out.plan.is_none() {
                out.plan = plan
                    .get("planName")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
            }
        }
        if out.plan.is_none() {
            out.plan = obj
                .get("planName")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
        out.billing_cycle_start_at = obj.get("billingCycleStart").and_then(as_unix);
        out.billing_cycle_end_at = obj.get("billingCycleEnd").and_then(as_unix);
        if out.next_reset_at.is_none() {
            out.next_reset_at = out.billing_cycle_end_at;
        }
        if out.period_start_at.is_none() {
            out.period_start_at = out.billing_cycle_start_at;
        }
    }
    out.round_one_decimal();
    out
}

impl CursorClient {
    pub async fn dashboard_call(
        &self,
        access_token: &str,
        machine_id: &str,
        name: &str,
    ) -> Result<Value, CursorError> {
        let payload = b"{}";
        let mut req = hyper::Request::builder()
            .method("POST")
            .uri(format!("{}{}/{}", self.backend(), DASHBOARD_PATH, name))
            .header("content-type", "application/json")
            .header("connect-protocol-version", "1")
            .header("connect-timeout-ms", "15000")
            .header("accept", "application/json")
            .header("accept-encoding", "identity");
        for (k, v) in cursor_headers(access_token, machine_id) {
            req = req.header(k, v);
        }
        let req = req
            .body(http_body_util::Full::new(hyper::body::Bytes::from(payload.as_slice())))
            .map_err(|e| CursorError::Network(e.to_string()))?;
        let resp = self
            .request(req)
            .await
            .map_err(|e| CursorError::Network(e.to_string()))?;
        let status = resp.status().as_u16();
        let body_bytes = http_body_util::BodyExt::collect(resp.into_body())
            .await
            .map_err(|e| CursorError::Network(e.to_string()))?
            .to_bytes();
        if status >= 400 {
            let text = String::from_utf8_lossy(&body_bytes);
            return Err(CursorError::Http(
                status,
                text.chars().take(200).collect(),
            ));
        }
        if let Ok(v) = serde_json::from_slice::<Value>(&body_bytes) {
            return Ok(v);
        }
        // Connect 单帧: [flags:1][len:4][payload]
        if body_bytes.len() >= 5 {
            let n = u32::from_be_bytes([body_bytes[1], body_bytes[2], body_bytes[3], body_bytes[4]])
                as usize;
            if body_bytes.len() >= 5 + n {
                if let Ok(v) = serde_json::from_slice::<Value>(&body_bytes[5..5 + n]) {
                    return Ok(v);
                }
            }
        }
        Err(CursorError::Decode("dashboard response not json".into()))
    }

    pub async fn probe_quota(
        &self,
        access_token: &str,
        machine_id: &str,
    ) -> QuotaSnapshot {
        let sand = self
            .dashboard_call(access_token, machine_id, "GetSandUsageStatus")
            .await;
        let period = self
            .dashboard_call(access_token, machine_id, "GetCurrentPeriodUsage")
            .await;
        let mut err = None;
        let sand_v = match sand {
            Ok(v) => v,
            Err(e) => {
                err = Some(e.to_string());
                Value::Null
            }
        };
        let period_v = match period {
            Ok(v) => v,
            Err(e) => {
                if err.is_none() {
                    err = Some(e.to_string());
                }
                Value::Null
            }
        };
        let mut snap = parse_quota(&sand_v, &period_v);
        snap.error = err.map(|s| s.chars().take(200).collect());
        snap
    }
}

pub struct KeyUsageStore {
    /// 无锁分片 map; 内部计数原子更新, 请求路径不再有全局互斥锁
    tokens: dashmap::DashMap<String, AtomicPair>,
}

struct AtomicPair {
    tokens: AtomicU64,
    requests: AtomicU64,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
struct UsageEntry {
    tokens: u64,
    requests: u64,
}

impl KeyUsageStore {
    pub fn new() -> Self {
        Self {
            tokens: dashmap::DashMap::new(),
        }
    }

    /// 从 usage.json 加载历史用量
    pub fn load_from_disk(&self) {
        let path = crate::config::usage_path();
        if !path.exists() {
            return;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            return;
        };
        let Ok(map) = serde_json::from_str::<HashMap<String, UsageEntry>>(&text) else {
            tracing::warn!(event = "usage_load", "usage.json malformed, ignoring");
            return;
        };
        for (k, v) in map {
            self.tokens.insert(
                k,
                AtomicPair {
                    tokens: AtomicU64::new(v.tokens),
                    requests: AtomicU64::new(v.requests),
                },
            );
        }
        tracing::info!(event = "usage_load", keys = self.tokens.len(), "usage restored from disk");
    }

    /// 落盘到 usage.json (原子写)
    pub fn save_to_disk(&self) {
        let map: HashMap<String, UsageEntry> = self
            .tokens
            .iter()
            .map(|r| {
                (
                    r.key().clone(),
                    UsageEntry {
                        tokens: r.tokens.load(Ordering::Relaxed),
                        requests: r.requests.load(Ordering::Relaxed),
                    },
                )
            })
            .collect();
        if let Ok(text) = serde_json::to_string_pretty(&map) {
            if let Err(e) = crate::config::atomic_write(&crate::config::usage_path(), &text) {
                tracing::warn!(event = "usage_save", error = %e, "usage persist failed");
            }
        }
    }

    pub fn add(&self, key: &str, tokens: u64) {
        // 快路径: key 已存在时只读分片锁 + 原子加, 无分配
        if let Some(p) = self.tokens.get(key) {
            p.tokens.fetch_add(tokens, Ordering::Relaxed);
            p.requests.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let entry = self.tokens.entry(key.to_string()).or_insert_with(|| AtomicPair {
            tokens: AtomicU64::new(0),
            requests: AtomicU64::new(0),
        });
        entry.tokens.fetch_add(tokens, Ordering::Relaxed);
        entry.requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self, key: &str) -> (u64, u64) {
        match self.tokens.get(key) {
            Some(p) => (
                p.tokens.load(Ordering::Relaxed),
                p.requests.load(Ordering::Relaxed),
            ),
            None => (0, 0),
        }
    }

    /// 全量快照 (用于 Prometheus 按 key 导出)
    pub fn snapshot_all(&self) -> Vec<(String, u64, u64)> {
        self.tokens
            .iter()
            .map(|r| {
                (
                    r.key().clone(),
                    r.tokens.load(Ordering::Relaxed),
                    r.requests.load(Ordering::Relaxed),
                )
            })
            .collect()
    }

    pub fn remove(&self, key: &str) {
        self.tokens.remove(key);
    }
}

pub fn key_public(index: usize, rec: &ApiKeyRecord, used_tokens: u64, used_requests: u64) -> Value {
    json!({
        "index": index,
        "prefix": rec.key.chars().take(8).collect::<String>(),
        "length": rec.key.len(),
        "name": rec.name,
        "description": rec.description,
        "enabled": rec.enabled,
        "expired": rec.is_expired(),
        "expires_at": rec.expires_at,
        "token_limit": rec.token_limit,
        "request_limit": rec.request_limit,
        "tags": rec.tags,
        "sales_id": rec.sales_id,
        "used_tokens": used_tokens,
        "used_requests": used_requests,
        "tokens_remaining": rec.token_limit.map(|lim| lim.saturating_sub(used_tokens)),
        "requests_remaining": rec.request_limit.map(|lim| lim.saturating_sub(used_requests)),
    })
}

pub fn check_key_limits(rec: &ApiKeyRecord, used_tokens: u64, used_requests: u64) -> Result<(), String> {
    if !rec.enabled {
        return Err("API key disabled".into());
    }
    if rec.is_expired() {
        return Err("API key expired".into());
    }
    if let Some(lim) = rec.token_limit {
        if used_tokens >= lim {
            return Err("token quota exhausted".into());
        }
    }
    if let Some(lim) = rec.request_limit {
        if used_requests >= lim {
            return Err("request quota exhausted".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sand_percent_is_already_percent() {
        assert_eq!(sand_usage_percent(&json!(0.486355)).unwrap(), 0.486355);
        let mut snap = parse_quota(
            &json!({
                "usagePercent": 0.486355,
                "hasAvailableUsage": true,
                "includedUsageSuperGrokPlan": "SuperGrok Heavy",
                "nextResetTimestampUtc": "2026-08-27T14:11:21.037Z",
                "currentPeriodStart": "2026-08-20T14:11:21.037Z"
            }),
            &json!({
                "displayMessage": "You've used 0% of your included usage",
                "planUsage": {"autoPercentUsed": 100.0, "apiPercentUsed": 2.2, "totalPercentUsed": 12.34},
                "billingCycleStart": "1787289010899",
                "billingCycleEnd": "1789881010899"
            }),
        );
        assert_eq!(snap.usage_percent, Some(0.5));
        assert_eq!(snap.auto_percent_used, Some(100.0));
        assert_eq!(snap.api_percent_used, Some(2.2));
        assert_eq!(snap.has_available_usage, Some(true));
        assert!(snap.next_reset_at.unwrap() > 1_700_000_000.0);
        assert!(snap.billing_cycle_end_at.unwrap() > 1_700_000_000.0);
        let mut keep = QuotaSnapshot {
            usage_percent: Some(9.0),
            ..Default::default()
        };
        snap.usage_percent = None;
        keep.merge_keep_last_good(snap);
        assert_eq!(keep.usage_percent, Some(9.0));
        assert_eq!(keep.has_available_usage, Some(true));
    }

    #[test]
    fn key_limit_blocks_when_exhausted() {
        let rec = ApiKeyRecord {
            key: "sk-test-aaaa".into(),
            name: "a".into(),
            description: String::new(),
            enabled: true,
            token_limit: Some(100),
            request_limit: Some(2),
            expires_at: None,
            tags: vec![],
            sales_id: None,
        };
        assert!(check_key_limits(&rec, 10, 1).is_ok());
        assert!(check_key_limits(&rec, 100, 1).unwrap_err().contains("token"));
        assert!(check_key_limits(&rec, 10, 2).unwrap_err().contains("request"));
        let mut disabled = rec.clone();
        disabled.enabled = false;
        assert!(check_key_limits(&disabled, 0, 0).is_err());
    }

    #[test]
    fn expired_key_blocked() {
        let rec = ApiKeyRecord {
            key: "sk-expired-aaaa".into(),
            name: String::new(),
            description: String::new(),
            enabled: true,
            token_limit: None,
            request_limit: None,
            expires_at: Some(1_000_000_000), // 2001 年, 已过期
            tags: vec![],
            sales_id: None,
        };
        assert!(rec.is_expired());
        assert!(check_key_limits(&rec, 0, 0).unwrap_err().contains("expired"));
        let future = ApiKeyRecord {
            expires_at: Some(4_000_000_000), // 远未来
            ..rec.clone()
        };
        assert!(!future.is_expired());
        assert!(check_key_limits(&future, 0, 0).is_ok());
    }

    #[test]
    fn usage_persist_roundtrip() {
        let dir = std::env::temp_dir().join(format!("cfp-usage-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("usage.json");
        std::env::set_var("CFP_USAGE", &path);
        let store = KeyUsageStore::new();
        store.add("sk-persist-test", 123);
        store.add("sk-persist-test", 7);
        store.save_to_disk();
        let store2 = KeyUsageStore::new();
        store2.load_from_disk();
        assert_eq!(store2.snapshot("sk-persist-test"), (130, 2));
        std::env::remove_var("CFP_USAGE");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
