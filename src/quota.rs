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
    /// 本地缓存时间戳 (unix 秒)
    #[serde(default)]
    pub cached_at: Option<f64>,
    /// 是否来自缓存 (非实时探测)
    #[serde(default)]
    pub from_cache: bool,
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
        keep(
            &mut self.billing_cycle_start_at,
            incoming.billing_cycle_start_at,
        );
        keep(
            &mut self.billing_cycle_end_at,
            incoming.billing_cycle_end_at,
        );
        if incoming.checked_at.is_some() {
            self.checked_at = incoming.checked_at;
        }
        self.error = incoming.error;
        // 缓存元数据: 新数据覆盖旧缓存标记
        if incoming.cached_at.is_some() {
            self.cached_at = incoming.cached_at;
        }
        self.from_cache = incoming.from_cache;
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
                        .or_else(|_| {
                            chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S")
                        })
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

/// 去掉上游套餐标签里的 Grok / SuperGrok 品牌字样, 只保留档位 (对客户端和面板都不暴露渠道).
pub fn sanitize_plan_label(raw: &str) -> String {
    // 大小写无关地抹掉品牌 token; 兼容空格/连字符/下划线分隔的 slug (如 "supergrok-heavy").
    let lower = raw.to_lowercase();
    let mut out = String::with_capacity(raw.len());
    let mut i = 0usize;
    let bytes = lower.as_bytes();
    let brands = [
        "supergrok",
        "super grok",
        "super-grok",
        "super_grok",
        "grok",
        "xai",
        "x.ai",
    ];
    'outer: while i < bytes.len() {
        for b in brands {
            if lower[i..].starts_with(b) {
                out.push(' ');
                i += b.len();
                continue 'outer;
            }
        }
        out.push(raw[i..].chars().next().unwrap());
        i += raw[i..].chars().next().unwrap().len_utf8();
    }
    // 归一化分隔符, 去掉品牌抹除后残留的边缘连字符/下划线
    let cleaned: Vec<String> = out
        .split(|c: char| c.is_whitespace() || c == '-' || c == '_')
        .map(|w| w.trim_matches(|c: char| c == '-' || c == '_'))
        .filter(|w| !w.is_empty())
        .map(|w| w.to_string())
        .collect();
    if cleaned.is_empty() {
        "Pro".to_string()
    } else {
        cleaned.join(" ")
    }
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
            .map(sanitize_plan_label);
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
                    .map(sanitize_plan_label);
            }
        }
        if out.plan.is_none() {
            out.plan = obj
                .get("planName")
                .and_then(|v| v.as_str())
                .map(sanitize_plan_label);
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
            .body(http_body_util::Full::new(hyper::body::Bytes::from(
                payload.as_slice(),
            )))
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
            return Err(CursorError::Http(status, text.chars().take(200).collect()));
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

    pub async fn probe_quota(&self, access_token: &str, machine_id: &str) -> QuotaSnapshot {
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
        tracing::info!(
            event = "usage_load",
            keys = self.tokens.len(),
            "usage restored from disk"
        );
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
        let entry = self
            .tokens
            .entry(key.to_string())
            .or_insert_with(|| AtomicPair {
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
        "rpm_limit": rec.rpm_limit,
        "max_concurrency": rec.max_concurrency,
        "tags": rec.tags,
        "sales_id": rec.sales_id,
        "used_tokens": used_tokens,
        "used_requests": used_requests,
        "tokens_remaining": rec.token_limit.map(|lim| lim.saturating_sub(used_tokens)),
        "requests_remaining": rec.request_limit.map(|lim| lim.saturating_sub(used_requests)),
    })
}

pub fn check_key_limits(
    rec: &ApiKeyRecord,
    used_tokens: u64,
    used_requests: u64,
) -> Result<(), String> {
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
    #[test]
    fn plan_label_hides_grok() {
        assert_eq!(super::sanitize_plan_label("SuperGrok Heavy"), "Heavy");
        assert_eq!(super::sanitize_plan_label("supergrok-heavy"), "heavy");
        assert_eq!(super::sanitize_plan_label("super_grok_pro"), "pro");
        assert_eq!(super::sanitize_plan_label("SuperGrok"), "Pro");
        assert_eq!(super::sanitize_plan_label("Grok Pro"), "Pro");
        assert_eq!(super::sanitize_plan_label("Ultra Plan"), "Ultra Plan");
        for l in [
            "SuperGrok Heavy",
            "supergrok-heavy",
            "SUPERGROK_HEAVY",
            "grok",
            "xAI Grok",
        ] {
            assert!(!super::sanitize_plan_label(l)
                .to_lowercase()
                .contains("grok"));
        }
    }

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
            rpm_limit: None,
            max_concurrency: None,
        };
        assert!(check_key_limits(&rec, 10, 1).is_ok());
        assert!(check_key_limits(&rec, 100, 1)
            .unwrap_err()
            .contains("token"));
        assert!(check_key_limits(&rec, 10, 2)
            .unwrap_err()
            .contains("request"));
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
            rpm_limit: None,
            max_concurrency: None,
        };
        assert!(rec.is_expired());
        assert!(check_key_limits(&rec, 0, 0)
            .unwrap_err()
            .contains("expired"));
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
        store.add("«redacted:sk-…»", 123);
        store.add("«redacted:sk-…»", 7);
        store.save_to_disk();
        let store2 = KeyUsageStore::new();
        store2.load_from_disk();
        assert_eq!(store2.snapshot("«redacted:sk-…»"), (130, 2));
        std::env::remove_var("CFP_USAGE");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

// ---------------------------------------------------------------------------
// 额度持久化 + 自动刷新
// ---------------------------------------------------------------------------

/// 额度历史存储 (SQLite)
/// 注意: rusqlite::Connection 非 Send, 所有操作通过 spawn_blocking 执行
pub struct QuotaStore {
    path: std::path::PathBuf,
}

impl QuotaStore {
    pub fn open(path: &std::path::Path) -> anyhow::Result<Self> {
        let db = rusqlite::Connection::open(path)?;
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS quota_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                plan TEXT,
                usage_percent REAL,
                has_available_usage INTEGER,
                auto_percent_used REAL,
                api_percent_used REAL,
                total_percent_used REAL,
                display_message TEXT,
                next_reset_at REAL,
                period_start_at REAL,
                billing_cycle_start_at REAL,
                billing_cycle_end_at REAL,
                checked_at REAL,
                error TEXT,
                created_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_quota_account ON quota_snapshots(account_id);
            CREATE INDEX IF NOT EXISTS idx_quota_created ON quota_snapshots(created_at);
            CREATE TABLE IF NOT EXISTS quota_latest (
                account_id TEXT PRIMARY KEY,
                snapshot TEXT NOT NULL,
                updated_at REAL NOT NULL
            );",
        )?;
        Ok(Self {
            path: path.to_path_buf(),
        })
    }

    fn open_conn(&self) -> anyhow::Result<rusqlite::Connection> {
        Ok(rusqlite::Connection::open(&self.path)?)
    }

    /// 保存快照到历史 + 更新最新缓存 (spawn_blocking 安全)
    pub async fn save(&self, account_id: &str, snap: &QuotaSnapshot) -> anyhow::Result<()> {
        let account_id = account_id.to_string();
        let mut snap = snap.clone();
        snap.cached_at = Some(now_unix());
        snap.from_cache = false;
        let json = serde_json::to_string(&snap)?;
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let db = rusqlite::Connection::open(&path)?;
            let now = now_unix();
            db.execute(
                "INSERT INTO quota_snapshots
                 (account_id, plan, usage_percent, has_available_usage, auto_percent_used,
                  api_percent_used, total_percent_used, display_message, next_reset_at,
                  period_start_at, billing_cycle_start_at, billing_cycle_end_at, checked_at, error, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                rusqlite::params![
                    account_id,
                    snap.plan,
                    snap.usage_percent,
                    snap.has_available_usage.map(|b| b as i32),
                    snap.auto_percent_used,
                    snap.api_percent_used,
                    snap.total_percent_used,
                    snap.display_message,
                    snap.next_reset_at,
                    snap.period_start_at,
                    snap.billing_cycle_start_at,
                    snap.billing_cycle_end_at,
                    snap.checked_at,
                    snap.error,
                    now,
                ],
            )?;
            db.execute(
                "INSERT OR REPLACE INTO quota_latest (account_id, snapshot, updated_at) VALUES (?1, ?2, ?3)",
                rusqlite::params![account_id, json, now],
            )?;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        Ok(())
    }

    /// 加载所有账号的最新额度缓存 (spawn_blocking 安全)
    pub async fn load_latest(&self) -> anyhow::Result<Vec<(String, QuotaSnapshot)>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let db = rusqlite::Connection::open(&path)?;
            let mut stmt = db.prepare("SELECT account_id, snapshot FROM quota_latest")?;
            let rows = stmt.query_map([], |row| {
                let account_id: String = row.get(0)?;
                let json: String = row.get(1)?;
                Ok((account_id, json))
            })?;
            let mut out = Vec::new();
            for row in rows {
                let (account_id, json) = row?;
                if let Ok(mut snap) = serde_json::from_str::<QuotaSnapshot>(&json) {
                    snap.from_cache = true;
                    out.push((account_id, snap));
                }
            }
            Ok::<_, anyhow::Error>(out)
        })
        .await?
    }

    /// 获取账号额度历史 (最近 N 条)
    pub async fn history(
        &self,
        account_id: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<QuotaSnapshot>> {
        let account_id = account_id.to_string();
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let db = rusqlite::Connection::open(&path)?;
            let mut stmt = db.prepare(
                "SELECT plan, usage_percent, has_available_usage, auto_percent_used,
                        api_percent_used, total_percent_used, display_message, next_reset_at,
                        period_start_at, billing_cycle_start_at, billing_cycle_end_at,
                        checked_at, error, created_at
                 FROM quota_snapshots WHERE account_id = ?1 ORDER BY created_at DESC LIMIT ?2",
            )?;
            let rows = stmt.query_map(rusqlite::params![account_id, limit as i64], |row| {
                Ok(QuotaSnapshot {
                    plan: row.get(0)?,
                    usage_percent: row.get(1)?,
                    has_available_usage: row.get::<_, Option<i32>>(2)?.map(|v| v != 0),
                    auto_percent_used: row.get(3)?,
                    api_percent_used: row.get(4)?,
                    total_percent_used: row.get(5)?,
                    display_message: row.get(6)?,
                    next_reset_at: row.get(7)?,
                    period_start_at: row.get(8)?,
                    billing_cycle_start_at: row.get(9)?,
                    billing_cycle_end_at: row.get(10)?,
                    checked_at: row.get(11)?,
                    error: row.get(12)?,
                    cached_at: row.get(13)?,
                    from_cache: false,
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            Ok::<_, anyhow::Error>(out)
        })
        .await?
    }

    /// 清理旧历史 (保留最近 N 天)
    pub async fn gc(&self, keep_days: u32) -> anyhow::Result<usize> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let db = rusqlite::Connection::open(&path)?;
            let cutoff = now_unix() - (keep_days as f64 * 86400.0);
            let n = db.execute(
                "DELETE FROM quota_snapshots WHERE created_at < ?1",
                rusqlite::params![cutoff],
            )?;
            Ok::<_, anyhow::Error>(n)
        })
        .await?
    }
}

/// 自动刷新配置
#[derive(Debug, Clone)]
pub struct AutoRefreshConfig {
    /// 刷新间隔 (秒), 默认 300 = 5 分钟
    pub interval_secs: u64,
    /// 并发探测数
    pub concurrency: usize,
    /// 是否启用
    pub enabled: bool,
}

impl Default for AutoRefreshConfig {
    fn default() -> Self {
        Self {
            interval_secs: 300,
            concurrency: 10,
            enabled: true,
        }
    }
}

/// 后台自动刷新循环
pub async fn auto_refresh_loop(
    state: std::sync::Arc<crate::AppState>,
    store: std::sync::Arc<QuotaStore>,
    config: AutoRefreshConfig,
) {
    if !config.enabled {
        tracing::info!(event = "quota_auto_refresh", "disabled, skipping");
        return;
    }
    // 立即执行一次 (手动触发或启动时)
    run_refresh_once(&state, &store, config.concurrency).await;
    // 如果 interval_secs == 0, 只执行一次 (手动触发模式)
    if config.interval_secs == 0 {
        return;
    }
    let mut tick =
        tokio::time::interval(std::time::Duration::from_secs(config.interval_secs.max(1)));
    loop {
        tick.tick().await;
        run_refresh_once(&state, &store, config.concurrency).await;
    }
}

/// 执行一次全量额度刷新
async fn run_refresh_once(
    state: &std::sync::Arc<crate::AppState>,
    store: &std::sync::Arc<QuotaStore>,
    concurrency: usize,
) {
    let start = std::time::Instant::now();
    let accounts: Vec<(String, crate::config::Account)> = state
        .pool
        .account_rows()
        .into_iter()
        .filter_map(|row| {
            let id = row["id"].as_str()?.to_string();
            state.pool.get_account(&id).map(|acc| (id, acc))
        })
        .collect();
    let total = accounts.len();
    tracing::info!(
        event = "quota_auto_refresh",
        total,
        "starting quota refresh"
    );

    use futures_util::stream::{self, StreamExt};
    let results: Vec<_> = stream::iter(accounts)
        .map(|(id, acc)| {
            let state = state.clone();
            let store = store.clone();
            async move {
                let snap = state
                    .cursor_factory
                    .resolve_for(&state.proxies, &acc)
                    .map(|(c, _)| c)
                    .unwrap_or_else(|_| state.cursor.clone())
                    .probe_quota(&acc.access_token, &acc.machine_id)
                    .await;
                let ok = snap.error.is_none();
                // 失败时 set_quota 会保留上次成功数据，磁盘也只写成功快照
                state.pool.set_quota(&id, snap.clone());
                if ok {
                    if let Err(e) = store.save(&id, &snap).await {
                        tracing::warn!(event = "quota_persist", account = %id, error = %e, "failed to save quota");
                    }
                }
                state.event_bus.publish(crate::admin::events::AdminEvent::new(
                    "quota_update",
                    serde_json::json!({
                        "account_id": id,
                        "quota": snap,
                    }),
                ));
                (id, ok)
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    let ok = results.iter().filter(|(_, ok)| *ok).count();
    let elapsed = start.elapsed();
    tracing::info!(
        event = "quota_auto_refresh",
        total,
        ok,
        fail = total - ok,
        elapsed_ms = elapsed.as_millis() as u64,
        "quota refresh done"
    );
}
