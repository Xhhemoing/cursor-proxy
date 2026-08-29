//! Admin 端点 IP 限频: 每 IP 每分钟固定窗口 60 次.

use dashmap::DashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const WINDOW_SECS: u64 = 60;
const MAX_PER_WINDOW: u64 = 60;

struct Bucket {
    window_start: AtomicU64,
    count: AtomicU64,
}

pub struct RateLimiter {
    buckets: DashMap<IpAddr, Bucket>,
}

impl RateLimiter {
    pub fn new() -> Self {
        Self {
            buckets: DashMap::new(),
        }
    }

    /// 检查 IP 是否允许通过; true = 放行, false = 超限
    pub fn allow(&self, ip: IpAddr) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let window = now / WINDOW_SECS;

        let entry = self.buckets.entry(ip).or_insert_with(|| Bucket {
            window_start: AtomicU64::new(window),
            count: AtomicU64::new(0),
        });

        let cur_window = entry.window_start.load(Ordering::Relaxed);
        if cur_window != window {
            // 尝试滑动窗口; 并发竞争下 CAS 失败也无妨, 下次再滑
            if entry
                .window_start
                .compare_exchange(cur_window, window, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                entry.count.store(0, Ordering::Relaxed);
            }
        }

        let n = entry.count.fetch_add(1, Ordering::Relaxed);
        n < MAX_PER_WINDOW
    }

    /// 定期清理过期 bucket (可选, 防内存膨胀)
    pub fn gc(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let cur_window = now / WINDOW_SECS;
        self.buckets
            .retain(|_, b| cur_window - b.window_start.load(Ordering::Relaxed) <= 2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn allows_up_to_limit() {
        let rl = RateLimiter::new();
        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        for _ in 0..MAX_PER_WINDOW {
            assert!(rl.allow(ip));
        }
        assert!(!rl.allow(ip));
    }

    #[test]
    fn different_ips_independent() {
        let rl = RateLimiter::new();
        let a = IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1));
        let b = IpAddr::V4(Ipv4Addr::new(2, 2, 2, 2));
        for _ in 0..MAX_PER_WINDOW {
            rl.allow(a);
        }
        assert!(rl.allow(b));
    }
}
