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

    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
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
