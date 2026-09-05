//! 配置加载: config.json + accounts.json.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default, deserialize_with = "deserialize_api_keys")]
    pub api_keys: Vec<ApiKeyRecord>,
    #[serde(default = "default_backend")]
    pub backend: String,
    #[serde(default = "default_timeout")]
    pub timeout_s: u64,
    #[serde(default = "default_log_file")]
    pub log_file: String,
    #[serde(default = "default_model")]
    pub default_model: String,
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency_per_account: usize,
    #[serde(default)]
    pub admin_token: String,
    /// 号池全部繁忙时最长排队等待毫秒数 (0 = 不排队直接 503)
    #[serde(default = "default_acquire_wait_ms")]
    pub acquire_wait_ms: u64,
    /// 计费: 模型价格表 / 销售分成 / 时区
    #[serde(default)]
    pub billing: BillingConfig,
    /// HTTP 出口代理池 + 账号分配规则
    #[serde(default)]
    pub proxy: crate::proxypool::ProxyPoolConfig,
}

/// 计费配置. 价格单位: 每 1M tokens 的货币金额 (最多 6 位小数, 内部转 micro 整数).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BillingConfig {
    /// SQLite 账本文件
    #[serde(default = "default_billing_db")]
    pub db_file: String,
    /// 日/小时分桶所用时区偏移 (分钟), 默认 +480 = Asia/Shanghai
    #[serde(default = "default_tz_offset")]
    pub tz_offset_minutes: i32,
    #[serde(default = "default_currency")]
    pub currency: String,
    /// key 未绑定销售 / 销售未配置分成比例时的默认分成 (万分比)
    #[serde(default)]
    pub default_commission_bps: u32,
    /// 未匹配到价格的模型是否拒绝请求 (true = 402; false = 记 0 元并标 unpriced)
    #[serde(default)]
    pub reject_unpriced: bool,
    #[serde(default)]
    pub prices: Vec<ModelPrice>,
    #[serde(default)]
    pub sales: Vec<SalesRecord>,
}

impl Default for BillingConfig {
    fn default() -> Self {
        Self {
            db_file: default_billing_db(),
            tz_offset_minutes: default_tz_offset(),
            currency: default_currency(),
            default_commission_bps: 0,
            reject_unpriced: false,
            prices: Vec::new(),
            sales: Vec::new(),
        }
    }
}

fn default_billing_db() -> String {
    "billing.db".into()
}
fn default_tz_offset() -> i32 {
    480
}
fn default_currency() -> String {
    "RMB".into()
}

/// CNY / 人民币 / usd 别名统一成 RMB / USD.
pub fn normalize_currency(raw: &str) -> String {
    let t = raw.trim();
    if t.is_empty() {
        return String::new();
    }
    if t == "人民币" || t == "¥" {
        return "RMB".into();
    }
    match t.to_ascii_uppercase().as_str() {
        "CNY" | "CNH" | "RMB" => "RMB".into(),
        "USD" | "US$" | "$" => "USD".into(),
        _ => t.to_string(),
    }
}

/// 模型价格规则. `model` 支持精确名或 `prefix*` 通配, `*` 为兜底.
/// 匹配优先级: 精确 > 最长前缀 > `*`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelPrice {
    pub model: String,
    /// 每 1M 输入 tokens 价格
    pub input_per_m: f64,
    /// 每 1M 输出 tokens 价格
    pub output_per_m: f64,
    /// 每 1M 缓存读 tokens 价格 (默认 0)
    #[serde(default)]
    pub cache_read_per_m: f64,
    /// 每 1M 缓存写 tokens 价格 (默认 0)
    #[serde(default)]
    pub cache_write_per_m: f64,
    #[serde(default)]
    pub note: String,
}

impl ModelPrice {
    /// 价格转 micro (1e-6) 整数; 6 位小数内无损
    pub fn input_micro(&self) -> u64 {
        money_to_micro(self.input_per_m)
    }
    pub fn output_micro(&self) -> u64 {
        money_to_micro(self.output_per_m)
    }
    pub fn cache_read_micro(&self) -> u64 {
        money_to_micro(self.cache_read_per_m)
    }
    pub fn cache_write_micro(&self) -> u64 {
        money_to_micro(self.cache_write_per_m)
    }
}

pub fn money_to_micro(v: f64) -> u64 {
    if !v.is_finite() || v <= 0.0 {
        return 0;
    }
    (v * 1_000_000.0).round() as u64
}

/// 销售人员 / 渠道
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SalesRecord {
    pub id: String,
    #[serde(default)]
    pub name: String,
    /// 分成比例 (万分比, 1500 = 15%)
    #[serde(default)]
    pub commission_bps: u32,
}

fn default_host() -> String {
    "0.0.0.0".into()
}
fn default_port() -> u16 {
    8800
}
fn default_backend() -> String {
    "https://api2.cursor.sh".into()
}
fn default_timeout() -> u64 {
    120  // 思考超时控制: 120s 强制返回, 避免算法设计等场景 243s 过长
}
fn default_log_file() -> String {
    "proxy.log".into()
}
fn default_model() -> String {
    "kimi-k3-max".into()
}
fn default_max_concurrency() -> usize {
    5
}
fn default_acquire_wait_ms() -> u64 {
    5000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyRecord {
    pub key: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub token_limit: Option<u64>,
    #[serde(default)]
    pub request_limit: Option<u64>,
    /// Unix 秒; None = 永不过期
    #[serde(default)]
    pub expires_at: Option<i64>,
    /// 标签 (客户/渠道/项目等), 用于账单筛选
    #[serde(default)]
    pub tags: Vec<String>,
    /// 归属销售 id (对应 billing.sales[].id)
    #[serde(default)]
    pub sales_id: Option<String>,
    /// 每分钟请求数限制 (RPM); None = 不限
    #[serde(default)]
    pub rpm_limit: Option<u32>,
    /// 并发请求数限制; None = 不限
    #[serde(default)]
    pub max_concurrency: Option<u32>,
    /// 允许访问的模型组 id 列表 (models.json groups); 空 = 不限
    #[serde(default)]
    pub model_groups: Vec<String>,
}

impl ApiKeyRecord {
    pub fn from_raw(key: String) -> Self {
        Self {
            key,
            name: String::new(),
            description: String::new(),
            enabled: true,
            token_limit: None,
            request_limit: None,
            expires_at: None,
            tags: Vec::new(),
            sales_id: None,
            rpm_limit: None,
            max_concurrency: None,
            model_groups: Vec::new(),
        }
    }

    pub fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(ts) => chrono::Utc::now().timestamp() >= ts,
            None => false,
        }
    }
}

fn deserialize_api_keys<'de, D>(deserializer: D) -> Result<Vec<ApiKeyRecord>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Null => Ok(vec![]),
        serde_json::Value::Array(items) => {
            let mut out = Vec::new();
            for item in items {
                match item {
                    serde_json::Value::String(s) => {
                        if !s.is_empty() {
                            out.push(ApiKeyRecord::from_raw(s));
                        }
                    }
                    serde_json::Value::Object(_) => {
                        let rec: ApiKeyRecord =
                            serde_json::from_value(item).map_err(serde::de::Error::custom)?;
                        if !rec.key.is_empty() {
                            out.push(rec);
                        }
                    }
                    _ => {}
                }
            }
            Ok(out)
        }
        _ => Err(serde::de::Error::custom("api_keys must be an array")),
    }
}

impl AppConfig {
    pub fn load() -> anyhow::Result<Self> {
        let path = config_path();
        if !path.exists() {
            return Ok(Self {
                host: default_host(),
                port: default_port(),
                api_keys: vec![],
                backend: default_backend(),
                timeout_s: default_timeout(),
                log_file: default_log_file(),
                default_model: default_model(),
                max_concurrency_per_account: default_max_concurrency(),
                admin_token: String::new(),
                acquire_wait_ms: default_acquire_wait_ms(),
                billing: BillingConfig::default(),
                proxy: crate::proxypool::ProxyPoolConfig::default(),
            });
        }
        let text = std::fs::read_to_string(&path)?;
        let mut config: AppConfig = serde_json::from_str(&text)?;
        if let Ok(host) = std::env::var("CFP_HOST") {
            config.host = host;
        }
        if let Ok(port) = std::env::var("CFP_PORT") {
            config.port = port.parse()?;
        }
        if let Ok(backend) = std::env::var("CFP_BACKEND") {
            config.backend = backend;
        }
        config.billing.currency = normalize_currency(&config.billing.currency);
        if config.billing.currency.is_empty() {
            config.billing.currency = default_currency();
        }
        Ok(config)
    }

    pub fn public_view(&self) -> serde_json::Value {
        serde_json::json!({
            "host": self.host,
            "port": self.port,
            "backend": self.backend,
            "timeout_s": self.timeout_s,
            "log_file": self.log_file,
            "default_model": self.default_model,
            "max_concurrency_per_account": self.max_concurrency_per_account,
            "acquire_wait_ms": self.acquire_wait_ms,
            "billing_currency": self.billing.currency,
            "billing_tz_offset_minutes": self.billing.tz_offset_minutes,
            "billing_price_rules": self.billing.prices.len(),
            "api_key_count": self.api_keys.len(),
            "admin_auth": !self.admin_token.is_empty() || !self.api_keys.is_empty(),
            "proxy_enabled": self.proxy.enabled,
            "proxy_nodes": self.proxy.nodes.len(),
            "proxy_require": self.proxy.require_proxy,
        })
    }
}

/// 配置容器: 读路径无锁 (ArcSwap, 每请求只是一次 Arc clone),
/// 写路径 copy-on-write 并用互斥锁串行化 (admin 低频操作).
pub struct ConfigCell {
    current: arc_swap::ArcSwap<AppConfig>,
    write_lock: parking_lot::Mutex<()>,
}

/// 可写 guard: 持有配置副本, drop 时原子发布. 用法与 Mutex guard 一致.
pub struct ConfigGuard<'a> {
    cell: &'a ConfigCell,
    data: Option<AppConfig>,
    _lock: parking_lot::MutexGuard<'a, ()>,
}

impl ConfigCell {
    pub fn new(cfg: AppConfig) -> Self {
        Self {
            current: arc_swap::ArcSwap::from_pointee(cfg),
            write_lock: parking_lot::Mutex::new(()),
        }
    }

    /// 热路径: 无锁读, 返回 Arc 快照.
    #[inline]
    pub fn load(&self) -> std::sync::Arc<AppConfig> {
        self.current.load_full()
    }

    /// 管理路径: 取可写 guard, 修改后 drop 即发布.
    pub fn lock(&self) -> ConfigGuard<'_> {
        let lock = self.write_lock.lock();
        let data = (**self.current.load()).clone();
        ConfigGuard {
            cell: self,
            data: Some(data),
            _lock: lock,
        }
    }
}

impl std::ops::Deref for ConfigGuard<'_> {
    type Target = AppConfig;
    fn deref(&self) -> &AppConfig {
        self.data.as_ref().expect("config guard data")
    }
}

impl std::ops::DerefMut for ConfigGuard<'_> {
    fn deref_mut(&mut self) -> &mut AppConfig {
        self.data.as_mut().expect("config guard data")
    }
}

impl Drop for ConfigGuard<'_> {
    fn drop(&mut self) {
        if let Some(data) = self.data.take() {
            self.cell.current.store(std::sync::Arc::new(data));
        }
    }
}

pub fn config_path() -> PathBuf {
    std::env::var("CFP_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("config.json"))
}

pub fn accounts_path() -> PathBuf {
    std::env::var("CFP_ACCOUNTS")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("accounts.json"))
}

pub fn usage_path() -> PathBuf {
    std::env::var("CFP_USAGE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("usage.json"))
}

pub fn audit_path() -> PathBuf {
    std::env::var("CFP_AUDIT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("audit.log"))
}

pub fn atomic_write(path: &Path, contents: &str) -> anyhow::Result<()> {
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, contents)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

pub fn save_config(config: &AppConfig) -> anyhow::Result<()> {
    let text = serde_json::to_string_pretty(config)?;
    atomic_write(&config_path(), &text)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub id: String,
    pub access_token: String,
    pub machine_id: String,
    #[serde(default)]
    pub refresh_token: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// access_token 过期时间（Unix 秒），None 表示未知/不过期
    #[serde(default)]
    pub token_expires_at: Option<u64>,
    /// 自定义 refresh 端点，为空则用默认 OAuth 风格
    #[serde(default)]
    pub refresh_url: Option<String>,
    /// 手动绑定的出口代理 id; 空 = 走自动规则
    #[serde(default)]
    pub proxy_id: Option<String>,
    /// 账号标签, 用于自动分配规则匹配
    #[serde(default)]
    pub tags: Vec<String>,
}

fn default_true() -> bool {
    true
}

pub fn load_accounts() -> anyhow::Result<Vec<Account>> {
    let path = accounts_path();
    if !path.exists() {
        return Ok(vec![]);
    }
    let text = std::fs::read_to_string(&path)?;
    let accounts: Vec<Account> = serde_json::from_str(&text)?;
    Ok(accounts
        .into_iter()
        .filter(|a| !a.access_token.is_empty() && !a.machine_id.is_empty())
        .collect())
}

pub fn save_accounts(accounts: &[Account]) -> anyhow::Result<()> {
    let out = serde_json::to_string_pretty(accounts)?;
    atomic_write(&accounts_path(), &out)
}

pub fn persist_account_enabled(id: &str, enabled: bool) -> anyhow::Result<bool> {
    let mut accounts = load_accounts()?;
    let mut found = false;
    for acc in &mut accounts {
        if acc.id == id {
            acc.enabled = enabled;
            found = true;
            break;
        }
    }
    if !found {
        anyhow::bail!("account {id} not found");
    }
    save_accounts(&accounts)?;
    Ok(enabled)
}

pub fn upsert_account(account: Account) -> anyhow::Result<Vec<Account>> {
    if account.id.trim().is_empty()
        || account.access_token.trim().is_empty()
        || account.machine_id.trim().is_empty()
    {
        anyhow::bail!("id, access_token, machine_id are required");
    }
    let mut accounts = load_accounts()?;
    if let Some(existing) = accounts.iter_mut().find(|a| a.id == account.id) {
        if !account.access_token.is_empty() {
            existing.access_token = account.access_token;
        }
        if !account.machine_id.is_empty() {
            existing.machine_id = account.machine_id;
        }
        existing.refresh_token = account.refresh_token;
        existing.enabled = account.enabled;
        existing.proxy_id = account.proxy_id;
        existing.tags = account.tags;
    } else {
        accounts.push(account);
    }
    save_accounts(&accounts)?;
    Ok(accounts)
}

pub fn delete_account(id: &str) -> anyhow::Result<Vec<Account>> {
    let mut accounts = load_accounts()?;
    let before = accounts.len();
    accounts.retain(|a| a.id != id);
    if accounts.len() == before {
        anyhow::bail!("account {id} not found");
    }
    save_accounts(&accounts)?;
    Ok(accounts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_temp_paths<F: FnOnce()>(f: F) {
        let _guard = ENV_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join(format!("cfp-cfg-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = dir.join("config.json");
        let acc = dir.join("accounts.json");
        std::env::set_var("CFP_CONFIG", &cfg);
        std::env::set_var("CFP_ACCOUNTS", &acc);
        f();
        std::env::remove_var("CFP_CONFIG");
        std::env::remove_var("CFP_ACCOUNTS");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn persist_toggle_keeps_disabled_accounts_on_disk() {
        with_temp_paths(|| {
            let accounts = vec![Account {
                id: "acc1".into(),
                access_token: "tok".into(),
                machine_id: "mid".into(),
                refresh_token: String::new(),
                enabled: true,
                token_expires_at: None,
                refresh_url: None,
                proxy_id: None,
                tags: Vec::new(),
            }];
            atomic_write(
                &accounts_path(),
                &serde_json::to_string_pretty(&accounts).unwrap(),
            )
            .unwrap();
            persist_account_enabled("acc1", false).unwrap();
            let loaded = load_accounts().unwrap();
            assert_eq!(loaded.len(), 1);
            assert!(!loaded[0].enabled);
        });
    }

    #[test]
    fn save_config_roundtrip_default_model() {
        with_temp_paths(|| {
            let mut cfg = AppConfig::load().unwrap();
            cfg.default_model = "kimi-k3".into();
            cfg.api_keys = vec![ApiKeyRecord::from_raw("sk-test-aaaa".into())];
            save_config(&cfg).unwrap();
            let loaded = AppConfig::load().unwrap();
            assert_eq!(loaded.default_model, "kimi-k3");
            assert_eq!(loaded.api_keys.len(), 1);
            let view = loaded.public_view();
            assert_eq!(view["api_key_count"], 1);
            assert!(view.get("api_keys").is_none());
        });
    }

    #[test]
    fn currency_aliases_unify_to_rmb() {
        assert_eq!(normalize_currency("cny"), "RMB");
        assert_eq!(normalize_currency("CNY"), "RMB");
        assert_eq!(normalize_currency("人民币"), "RMB");
        assert_eq!(normalize_currency("RMB"), "RMB");
        assert_eq!(normalize_currency("usd"), "USD");
        assert_eq!(normalize_currency(""), "");
    }
}
