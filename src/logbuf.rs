//! 内存日志 ring buffer: 固定容量, 线程安全, 供管理面板读取.
//! 同时支持 JSONL 持久化到磁盘.

use std::collections::VecDeque;
use std::sync::Mutex;

pub struct LogBuffer {
    entries: Mutex<VecDeque<serde_json::Value>>,
    capacity: usize,
    /// JSONL 持久化文件路径，None 表示不持久化
    persist_path: Option<std::path::PathBuf>,
}

impl LogBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            persist_path: None,
        }
    }

    pub fn with_persist(capacity: usize, path: std::path::PathBuf) -> Self {
        Self {
            entries: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            persist_path: Some(path),
        }
    }

    pub fn push(&self, entry: serde_json::Value) {
        // 持久化到 JSONL
        if let Some(ref path) = self.persist_path {
            let line = serde_json::to_string(&entry).unwrap_or_default() + "\n";
            let _ = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .and_then(|mut f| std::io::Write::write_all(&mut f, line.as_bytes()));
        }

        let mut entries = self.entries.lock().unwrap();
        if entries.len() >= self.capacity {
            entries.pop_front();
        }
        entries.push_back(entry);
    }

    pub fn recent(&self, n: usize) -> Vec<serde_json::Value> {
        let entries = self.entries.lock().unwrap();
        entries.iter().rev().take(n).cloned().collect()
    }

    /// 按条件查询日志
    pub fn query(&self, filter: &LogFilter) -> Vec<serde_json::Value> {
        let entries = self.entries.lock().unwrap();
        entries
            .iter()
            .rev()
            .filter(|e| filter.matches(e))
            .take(filter.limit)
            .cloned()
            .collect()
    }

    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }
}

/// 日志查询过滤器
#[derive(Debug, Default)]
pub struct LogFilter {
    pub model: Option<String>,
    pub account: Option<String>,
    pub status: Option<u16>,
    pub stream: Option<bool>,
    pub client_ip: Option<String>,
    pub limit: usize,
}

impl LogFilter {
    pub fn new() -> Self {
        Self {
            limit: 100,
            ..Default::default()
        }
    }

    fn matches(&self, entry: &serde_json::Value) -> bool {
        if let Some(ref m) = self.model {
            if entry.get("model").and_then(|v| v.as_str()) != Some(m) {
                return false;
            }
        }
        if let Some(ref a) = self.account {
            if entry.get("account").and_then(|v| v.as_str()) != Some(a) {
                return false;
            }
        }
        if let Some(s) = self.status {
            if entry.get("status").and_then(|v| v.as_u64()) != Some(s as u64) {
                return false;
            }
        }
        if let Some(st) = self.stream {
            if entry.get("stream").and_then(|v| v.as_bool()) != Some(st) {
                return false;
            }
        }
        if let Some(ref ip) = self.client_ip {
            if entry.get("client_ip").and_then(|v| v.as_str()) != Some(ip) {
                return false;
            }
        }
        true
    }
}

impl Default for LogBuffer {
    fn default() -> Self {
        Self::new(1000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn recent_is_newest_first_and_caps() {
        let buf = LogBuffer::new(3);
        buf.push(json!({"n": 1}));
        buf.push(json!({"n": 2}));
        buf.push(json!({"n": 3}));
        buf.push(json!({"n": 4}));
        let recent = buf.recent(2);
        assert_eq!(recent[0]["n"], 4);
        assert_eq!(recent[1]["n"], 3);
        assert_eq!(buf.len(), 3);
    }
}
