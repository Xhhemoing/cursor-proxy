//! 内存日志 ring buffer: 固定容量, 线程安全, 供管理面板读取.
//! 异步批量 JSONL 持久化到磁盘.

use std::collections::VecDeque;
use std::sync::Mutex;
use tokio::sync::mpsc;

pub struct LogBuffer {
    entries: Mutex<VecDeque<serde_json::Value>>,
    capacity: usize,
    /// 异步持久化通道
    persist_tx: Option<mpsc::Sender<serde_json::Value>>,
}

impl LogBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            persist_tx: None,
        }
    }

    /// 创建带异步持久化的 LogBuffer
    /// 后台任务每 100 条或 1 秒批量写一次磁盘
    pub fn with_persist(capacity: usize, path: std::path::PathBuf) -> Self {
        let (tx, rx) = mpsc::channel(1000);
        // 启动异步落盘任务
        tokio::spawn(persist_worker(rx, path));
        Self {
            entries: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            persist_tx: Some(tx),
        }
    }

    pub fn push(&self, entry: serde_json::Value) {
        // 异步发送到持久化通道（非阻塞）
        if let Some(ref tx) = self.persist_tx {
            let _ = tx.try_send(entry.clone());
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

/// 异步批量落盘 worker
async fn persist_worker(mut rx: mpsc::Receiver<serde_json::Value>, path: std::path::PathBuf) {
    use tokio::io::AsyncWriteExt;

    let mut batch = Vec::with_capacity(100);
    let mut file = match tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
    {
        Ok(f) => f,
        Err(e) => {
            tracing::error!(event = "log_persist_error", error = %e, "failed to open log file");
            return;
        }
    };

    loop {
        // 批量接收，最多 100 条或超时 1 秒
        batch.clear();
        let n = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            rx.recv_many(&mut batch, 100),
        )
        .await
        .unwrap_or(0);

        if n == 0 && batch.is_empty() {
            // 超时且无数据，flush 后等待
            let _ = file.flush().await;
            continue;
        }

        // 批量写入
        for entry in batch.drain(..) {
            let line = serde_json::to_string(&entry).unwrap_or_default() + "\n";
            if let Err(e) = file.write_all(line.as_bytes()).await {
                tracing::error!(event = "log_persist_error", error = %e, "failed to write log");
                break;
            }
        }

        // 批量 flush
        if let Err(e) = file.flush().await {
            tracing::error!(event = "log_persist_error", error = %e, "failed to flush log");
        }
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
