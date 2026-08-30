//! Prometheus 文本指标: 号池 + 请求计数 + RPM (滑动窗口).

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// 滑动窗口 RPM 计数器: 60 秒窗口, 按秒分桶.
/// 低开销: 每 key 60 个 AtomicU64 桶, 读时求和.
#[derive(Default)]
pub struct RpmTracker {
    /// key -> [60] 秒桶
    buckets: DashMap<String, [AtomicU64; 60]>,
}

impl RpmTracker {
    pub fn new() -> Self {
        Self { buckets: DashMap::new() }
    }

    /// 记录一次请求
    pub fn hit(&self, key: &str) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let bucket_idx = (now % 60) as usize;
        let entry = self
            .buckets
            .entry(key.to_string())
            .or_insert_with(|| std::array::from_fn(|_| AtomicU64::new(0)));
        let bucket = &entry[bucket_idx];
        // 如果桶里的时间戳不是当前秒, 先清零 (简化: 直接用秒数做 bucket 内容校验太复杂, 采用"秒对齐清零"策略)
        // 简化实现: 每个桶存的是 "秒数*1000000 + 计数" 不行, 原子操作复杂.
        // 更简单: 桶内存计数, 读取时只统计"最近60个桶中非 stale"的值 — 但需要存时间戳.
        // 最简方案: 桶内直接存计数, 每 60 秒滚动清零由 GC 线程做. 但 GC 不及时会多算.
        // 折中: 桶内 high 32 bit 存秒数, low 32 bit 存计数 — 单次 CAS 完成.
        let packed = ((now & 0xFFFF_FFFF) << 32) as u64;
        loop {
            let cur = bucket.load(Ordering::Relaxed);
            let cur_sec = (cur >> 32) as u64;
            let cur_cnt = cur & 0xFFFF_FFFF;
            let new_val = if cur_sec == now & 0xFFFF_FFFF {
                packed | (cur_cnt + 1)
            } else {
                packed | 1
            };
            if bucket
                .compare_exchange(cur, new_val, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    /// 读取最近 60 秒总请求数
    pub fn rpm(&self, key: &str) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let Some(entry) = self.buckets.get(key) else {
            return 0;
        };
        let mut total = 0u64;
        for bucket in entry.iter() {
            let cur = bucket.load(Ordering::Relaxed);
            let sec = (cur >> 32) as u64;
            let cnt = cur & 0xFFFF_FFFF;
            // 只统计 60 秒窗口内的桶
            if now.saturating_sub(sec) < 60 {
                total += cnt;
            }
        }
        total
    }

    /// 定期清理不活跃 key (可选)
    pub fn gc(&self, active_keys: &[String]) {
        let set: std::collections::HashSet<&str> = active_keys.iter().map(|s| s.as_str()).collect();
        self.buckets.retain(|k, _| set.contains(k.as_str()));
    }
}

#[derive(Default)]
pub struct Metrics {
    pub requests_total: AtomicU64,
    pub requests_ok: AtomicU64,
    pub requests_err: AtomicU64,
    pub tokens_total: AtomicU64,
    /// 全局 RPM 滑动窗口
    pub rpm_global: RpmTracker,
    /// 每 API Key RPM
    pub rpm_keys: RpmTracker,
    /// 每账号 RPM
    pub rpm_accounts: RpmTracker,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe_ok(&self, tokens: u64) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        self.requests_ok.fetch_add(1, Ordering::Relaxed);
        self.tokens_total.fetch_add(tokens, Ordering::Relaxed);
        self.rpm_global.hit("__global__");
    }

    pub fn observe_err(&self) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        self.requests_err.fetch_add(1, Ordering::Relaxed);
        self.rpm_global.hit("__global__");
    }

    /// 记录一次请求的 RPM (key + account 维度)
    pub fn observe_rpm(&self, api_key: &str, account_id: &str) {
        if !api_key.is_empty() {
            self.rpm_keys.hit(api_key);
        }
        if !account_id.is_empty() {
            self.rpm_accounts.hit(account_id);
        }
    }

    /// 全局 RPM
    pub fn global_rpm(&self) -> u64 {
        self.rpm_global.rpm("__global__")
    }

    /// 某 key 的 RPM
    pub fn key_rpm(&self, key: &str) -> u64 {
        self.rpm_keys.rpm(key)
    }

    /// 某账号的 RPM
    pub fn account_rpm(&self, account_id: &str) -> u64 {
        self.rpm_accounts.rpm(account_id)
    }

    pub fn render(
        &self,
        pool: &crate::pool::AccountPool,
        usage: &crate::quota::KeyUsageStore,
    ) -> String {
        let stats = pool.summary();
        let total = stats["total_accounts"].as_u64().unwrap_or(0);
        let avail = stats["available"].as_u64().unwrap_or(0);
        let inflight = stats["inflight"].as_u64().unwrap_or(0);
        let mut out = format!(
            "# HELP cfp_up 1 if process is serving\n# TYPE cfp_up gauge\ncfp_up 1\n\
# HELP cfp_pool_accounts Number of bot accounts\n# TYPE cfp_pool_accounts gauge\ncfp_pool_accounts {total}\n\
# HELP cfp_pool_available Eligible accounts (enabled, not cooling, quota ok)\n# TYPE cfp_pool_available gauge\ncfp_pool_available {avail}\n\
# HELP cfp_pool_inflight In-flight requests holding a permit\n# TYPE cfp_pool_inflight gauge\ncfp_pool_inflight {inflight}\n\
# HELP cfp_requests_total Chat completions seen\n# TYPE cfp_requests_total counter\ncfp_requests_total {}\n\
# HELP cfp_requests_ok Successful completions\n# TYPE cfp_requests_ok counter\ncfp_requests_ok {}\n\
# HELP cfp_requests_err Failed completions\n# TYPE cfp_requests_err counter\ncfp_requests_err {}\n\
# HELP cfp_tokens_total Tokens billed to API keys\n# TYPE cfp_tokens_total counter\ncfp_tokens_total {}\n\
# HELP cfp_rpm_global Requests per minute (global, 60s sliding window)\n# TYPE cfp_rpm_global gauge\ncfp_rpm_global {}\n",
            self.requests_total.load(Ordering::Relaxed),
            self.requests_ok.load(Ordering::Relaxed),
            self.requests_err.load(Ordering::Relaxed),
            self.tokens_total.load(Ordering::Relaxed),
            self.global_rpm(),
        );
        // 按 key 导出 (脱敏前缀, 防泄露完整 key)
        out.push_str(
            "# HELP cfp_key_tokens_total Tokens per API key (redacted prefix label)\n# TYPE cfp_key_tokens_total counter\n",
        );
        for (key, tokens, _reqs) in usage.snapshot_all() {
            let prefix: String = key.chars().take(8).collect();
            out.push_str(&format!("cfp_key_tokens_total{{key=\"{}\"}} {}\n", prefix, tokens));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Account;
    use crate::pool::AccountPool;

    #[test]
    fn render_contains_gauges() {
        let pool = AccountPool::new(
            vec![Account {
                id: "acc1".into(),
                access_token: "t".into(),
                machine_id: "m".into(),
                refresh_token: String::new(),
                enabled: true,
                token_expires_at: None,
                refresh_url: None,
                proxy_id: None,
                tags: Vec::new(),
            }],
            2,
        );
        let m = Metrics::new();
        m.observe_ok(12);
        let usage = crate::quota::KeyUsageStore::new();
        usage.add("sk-test-aaaa", 12);
        let text = m.render(&pool, &usage);
        assert!(text.contains("cfp_up 1"));
        assert!(text.contains("cfp_pool_accounts 1"));
        assert!(text.contains("cfp_tokens_total 12"));
        assert!(text.contains("cfp_requests_ok 1"));
        assert!(text.contains("cfp_key_tokens_total{key=\"sk-test-\"} 12"));
    }
}
