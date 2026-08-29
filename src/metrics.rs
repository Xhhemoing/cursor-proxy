//! Prometheus 文本指标: 号池 + 请求计数.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct Metrics {
    pub requests_total: AtomicU64,
    pub requests_ok: AtomicU64,
    pub requests_err: AtomicU64,
    pub tokens_total: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe_ok(&self, tokens: u64) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        self.requests_ok.fetch_add(1, Ordering::Relaxed);
        self.tokens_total.fetch_add(tokens, Ordering::Relaxed);
    }

    pub fn observe_err(&self) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        self.requests_err.fetch_add(1, Ordering::Relaxed);
    }

    pub fn render(
        &self,
        pool: &crate::pool::AccountPool,
        usage: &crate::quota::KeyUsageStore,
    ) -> String {
        let stats = pool.stats();
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
# HELP cfp_tokens_total Tokens billed to API keys\n# TYPE cfp_tokens_total counter\ncfp_tokens_total {}\n",
            self.requests_total.load(Ordering::Relaxed),
            self.requests_ok.load(Ordering::Relaxed),
            self.requests_err.load(Ordering::Relaxed),
            self.tokens_total.load(Ordering::Relaxed),
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
