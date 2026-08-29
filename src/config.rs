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
    600
}
fn default_log_file() -> String {
    "proxy.log".into()
}
fn default_model() -> String {
    "grok-4.6".into()
}
fn default_max_concurrency() -> usize {
    8
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
            "api_key_count": self.api_keys.len(),
            "admin_auth": !self.admin_token.is_empty() || !self.api_keys.is_empty(),
        })
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
            cfg.default_model = "grok-4.6".into();
            cfg.api_keys = vec![ApiKeyRecord::from_raw("sk-test-aaaa".into())];
            save_config(&cfg).unwrap();
            let loaded = AppConfig::load().unwrap();
            assert_eq!(loaded.default_model, "grok-4.6");
            assert_eq!(loaded.api_keys.len(), 1);
            let view = loaded.public_view();
            assert_eq!(view["api_key_count"], 1);
            assert!(view.get("api_keys").is_none());
        });
    }
}
