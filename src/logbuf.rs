//! 内存日志 ring buffer: 固定容量, 线程安全, 供管理面板读取.
//! 同时支持 JSONL 持久化到磁盘 (专用后台线程 + BufWriter, 请求路径不做任何文件 I/O).

use std::collections::VecDeque;
use std::io::Write;
use std::sync::mpsc;
use std::sync::Mutex;
use std::time::Duration;

pub struct LogBuffer {
    entries: Mutex<VecDeque<serde_json::Value>>,
    capacity: usize,
    /// 持久化通道: 请求线程只做一次无锁 send, 写盘在专用线程
    persist_tx: Option<mpsc::Sender<String>>,
    /// 单调递增日志 ID (用于增量查询游标)
    next_id: std::sync::atomic::AtomicU64,
}

impl LogBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            persist_tx: None,
            next_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    pub fn with_persist(capacity: usize, path: std::path::PathBuf) -> Self {
        let (tx, rx) = mpsc::channel::<String>();
        std::thread::Builder::new()
            .name("log-writer".into())
            .spawn(move || persist_loop(path, rx))
            .expect("spawn log writer thread");
        Self {
            entries: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            persist_tx: Some(tx),
            next_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// 推入一条日志; `line` 为已序列化的 JSON (调用方通常已经有了, 避免二次序列化)
    pub fn push_with_line(&self, entry: serde_json::Value, line: String) {
        if let Some(tx) = &self.persist_tx {
            // 写线程挂了也不能影响请求
            let _ = tx.send(line);
        }
        let id = self.next_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut entry = entry;
        entry["_id"] = serde_json::json!(id);
        let mut entries = self.entries.lock().unwrap();
        if entries.len() >= self.capacity {
            entries.pop_front();
        }
        entries.push_back(entry);
    }

    pub fn push(&self, entry: serde_json::Value) {
        let line = serde_json::to_string(&entry).unwrap_or_default();
        self.push_with_line(entry, line);
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

    /// 增量查询: 只返回 ID > after_id 的新日志
    pub fn query_after(&self, after_id: u64, limit: usize) -> Vec<serde_json::Value> {
        let entries = self.entries.lock().unwrap();
        entries
            .iter()
            .rev()
            .filter(|e| e.get("_id").and_then(|v| v.as_u64()).unwrap_or(0) > after_id)
            .take(limit)
            .cloned()
            .collect()
    }

    /// 当前最大日志 ID
    pub fn max_id(&self) -> u64 {
        self.next_id.load(std::sync::atomic::Ordering::Relaxed).saturating_sub(1)
    }

    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }
}

fn log_max_bytes() -> u64 {
    std::env::var("CFP_LOG_MAX_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(512 * 1024 * 1024)
}

fn log_keep() -> usize {
    std::env::var("CFP_LOG_KEEP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3)
        .clamp(1, 20)
}

fn rotated_path(path: &std::path::Path, gen: usize) -> std::path::PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(format!(".{gen}"));
    std::path::PathBuf::from(s)
}

/// 按大小轮转: path -> path.1 -> path.2 ... 超出 keep 的丢掉.
fn rotate_log_file(path: &std::path::Path, keep: usize) {
    let oldest = rotated_path(path, keep);
    let _ = std::fs::remove_file(&oldest);
    for gen in (1..keep).rev() {
        let from = rotated_path(path, gen);
        let to = rotated_path(path, gen + 1);
        if from.exists() {
            let _ = std::fs::rename(&from, &to);
        }
    }
    if path.exists() {
        let _ = std::fs::rename(path, rotated_path(path, 1));
    }
}

fn maybe_rotate(
    path: &std::path::Path,
    max_bytes: u64,
    keep: usize,
    writer: &mut Option<std::io::BufWriter<std::fs::File>>,
) {
    let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    if size < max_bytes {
        return;
    }
    if let Some(w) = writer.take() {
        let _ = w.into_inner().map(|mut f| {
            let _ = f.flush();
            f
        });
    }
    rotate_log_file(path, keep);
}

/// 后台写盘循环: 句柄常驻 + BufWriter, 攒批或 200ms 空闲即 flush.
/// 文件超过 CFP_LOG_MAX_BYTES (默认 512MiB) 时轮转, 保留 CFP_LOG_KEEP 代 (默认 3).
fn persist_loop(path: std::path::PathBuf, rx: mpsc::Receiver<String>) {
    let max_bytes = log_max_bytes();
    let keep = log_keep();
    let open = || {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map(|f| std::io::BufWriter::with_capacity(64 * 1024, f))
    };
    let mut writer = open().ok();
    let mut pending = 0usize;
    let mut since_size_check = 0usize;
    loop {
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(line) => {
                if writer.is_none() {
                    writer = open().ok();
                }
                if let Some(w) = writer.as_mut() {
                    if w.write_all(line.as_bytes()).and_then(|_| w.write_all(b"\n")).is_err() {
                        writer = None; // 下次重新打开
                        continue;
                    }
                    pending += 1;
                    since_size_check += 1;
                    if pending >= 256 {
                        let _ = w.flush();
                        pending = 0;
                    }
                    if since_size_check >= 2048 {
                        since_size_check = 0;
                        maybe_rotate(&path, max_bytes, keep, &mut writer);
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if pending > 0 {
                    if let Some(w) = writer.as_mut() {
                        let _ = w.flush();
                    }
                    pending = 0;
                    maybe_rotate(&path, max_bytes, keep, &mut writer);
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                if let Some(w) = writer.as_mut() {
                    let _ = w.flush();
                }
                return;
            }
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

    #[test]
    fn persist_writes_jsonl() {
        let dir = std::env::temp_dir().join(format!("cfp-log-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("p.log");
        {
            let buf = LogBuffer::with_persist(10, path.clone());
            buf.push(json!({"a": 1}));
            buf.push(json!({"a": 2}));
            // drop → 通道关闭 → 写线程 flush 退出
        }
        // 等写线程收尾
        for _ in 0..50 {
            if std::fs::read_to_string(&path).map(|s| s.lines().count() == 2).unwrap_or(false) {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(text.lines().count(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rotate_log_file_keeps_generations() {
        let dir = std::env::temp_dir().join(format!("cfp-rot-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("p.log");
        std::fs::write(&path, b"gen0\n").unwrap();
        rotate_log_file(&path, 2);
        assert!(!path.exists());
        assert_eq!(std::fs::read_to_string(dir.join("p.log.1")).unwrap(), "gen0\n");
        std::fs::write(&path, b"gen1\n").unwrap();
        rotate_log_file(&path, 2);
        assert_eq!(std::fs::read_to_string(dir.join("p.log.1")).unwrap(), "gen1\n");
        assert_eq!(std::fs::read_to_string(dir.join("p.log.2")).unwrap(), "gen0\n");
        std::fs::write(&path, b"gen2\n").unwrap();
        rotate_log_file(&path, 2);
        assert_eq!(std::fs::read_to_string(dir.join("p.log.1")).unwrap(), "gen2\n");
        assert_eq!(std::fs::read_to_string(dir.join("p.log.2")).unwrap(), "gen1\n");
        assert!(!dir.join("p.log.3").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
