//! 审计日志: JSON Lines 追加写, 记录管理操作 (key/账号/设置变更).

use serde_json::{json, Value};
use std::sync::Mutex;

pub struct AuditLog {
    inner: Mutex<std::fs::File>,
}

impl AuditLog {
    pub fn new() -> std::io::Result<Self> {
        Self::new_at(crate::config::audit_path())
    }

    pub fn new_at(path: std::path::PathBuf) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            inner: Mutex::new(file),
        })
    }

    /// 记录一条审计事件. 失败仅 warn, 不阻塞请求.
    pub fn record(&self, action: &str, actor: &str, detail: Value) {
        use std::io::Write;
        let entry = json!({
            "ts": chrono::Utc::now().to_rfc3339(),
            "action": action,
            "actor": actor,
            "detail": detail,
        });
        let line = entry.to_string();
        let mut f = match self.inner.lock() {
            Ok(f) => f,
            Err(_) => return,
        };
        if let Err(e) = writeln!(f, "{}", line) {
            tracing::warn!(event = "audit_write", error = %e, "audit log write failed");
        }
    }

    /// 便捷方法: key 操作
    pub fn key_op(&self, op: &str, key_prefix: &str, extra: Value) {
        self.record(
            &format!("key.{}", op),
            "admin",
            json!({ "key_prefix": key_prefix, "extra": extra }),
        );
    }

    /// 便捷方法: 账号操作
    pub fn account_op(&self, op: &str, account_id: &str, extra: Value) {
        self.record(
            &format!("account.{}", op),
            "admin",
            json!({ "account_id": account_id, "extra": extra }),
        );
    }

    /// 便捷方法: 设置变更
    pub fn settings_op(&self, changed: &[&str]) {
        self.record(
            "settings.update",
            "admin",
            json!({ "changed": changed }),
        );
    }
}
