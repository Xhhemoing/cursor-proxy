//! 号池: 无锁轮询 + 并发控制 + 失败冷却 + 动态开关 + 热编辑 + 会话一致性 + 健康评分.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::Semaphore;

use crate::config::Account;
use crate::quota::QuotaSnapshot;

/// 全局会话索引: session_id -> (account_id, last_seen)
/// last_seen 用于 TTL GC，防止内存无限增长
type SessionIndex = Arc<DashMap<String, (String, Instant)>>;

/// 会话索引 TTL: 2 小时不活跃即回收
const SESSION_TTL: Duration = Duration::from_secs(2 * 3600);
/// 每 1024 次插入触发一次 GC（摊销成本）
const SESSION_GC_EVERY: u64 = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcquireError {
    Empty,
    Busy,
}

pub enum AcquireTry {
    Got((Account, tokio::sync::OwnedSemaphorePermit)),
    Empty,
    Busy,
}

pub struct Slot {
    account: Account,
    sem: Arc<Semaphore>,
    cooldown_until: parking_lot::Mutex<Option<Instant>>,
}

impl Slot {
    fn is_cooling_down(&self) -> bool {
        self.cooldown_until
            .lock()
            .map(|until| until > Instant::now())
            .unwrap_or(false)
    }

    fn set_cooldown(&self, secs: u64) {
        *self.cooldown_until.lock() = Some(Instant::now() + Duration::from_secs(secs));
    }

    fn clear_cooldown(&self) {
        *self.cooldown_until.lock() = None;
    }

    fn cooldown_remaining(&self) -> Option<Duration> {
        self.cooldown_until
            .lock()
            .and_then(|until| until.checked_duration_since(Instant::now()))
    }
}

#[derive(Clone)]
pub struct AccountPool {
    /// 无锁号池: id -> Slot
    slots: Arc<DashMap<String, Arc<Slot>>>,
    /// 轮询索引（原子）
    rr_index: Arc<AtomicUsize>,
    /// 有序 id 列表（用于轮询，变更时重建）
    ordered_ids: Arc<parking_lot::RwLock<Vec<String>>>,
    /// 可用账号列表（arc-swap 无锁读，避免热路径 RwLock 竞争）
    available_ids: Arc<arc_swap::ArcSwap<Vec<String>>>,
    /// 账号开关状态
    disabled: Arc<DashMap<String, bool>>,
    /// 原子化统计
    stats: Arc<DashMap<String, AccountStats>>,
    /// 额度快照
    quotas: Arc<DashMap<String, QuotaSnapshot>>,
    /// 全局会话索引: session_id -> (account_id, last_seen)（避免遍历 slot 查找）
    session_index: SessionIndex,
    /// 会话索引插入计数（用于摊销 GC）
    session_insert_count: Arc<AtomicU64>,
    max_concurrency: usize,
}

#[derive(Debug, Default)]
pub struct AccountStats {
    pub requests: AtomicU64,
    pub errors: AtomicU64,
    pub consecutive_errors: AtomicU64,
    pub auto_disabled_count: AtomicU64,
}

impl AccountStats {
    pub fn error_rate(&self) -> f64 {
        let req = self.requests.load(Ordering::Relaxed);
        if req == 0 {
            0.0
        } else {
            self.errors.load(Ordering::Relaxed) as f64 / req as f64
        }
    }

    pub fn record_request(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        self.consecutive_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_success(&self) {
        self.consecutive_errors.store(0, Ordering::Relaxed);
    }

    pub fn consecutive_errors(&self) -> u64 {
        self.consecutive_errors.load(Ordering::Relaxed)
    }
}

/// 账号健康评分 (0-100)
#[derive(Debug, Clone, serde::Serialize)]
pub struct HealthScore {
    pub score: u8,
    pub error_rate: f64,
    pub consecutive_errors: u64,
    pub cooldown_active: bool,
    pub quota_exhausted: bool,
    pub enabled: bool,
}

impl AccountPool {
    pub fn new(accounts: Vec<Account>, max_concurrency: usize) -> Self {
        let max_concurrency = max_concurrency.max(1);
        let slots = DashMap::new();
        let disabled = DashMap::new();
        let stats = DashMap::new();
        let quotas = DashMap::new();
        let mut ordered = Vec::new();

        for acc in accounts {
            let id = acc.id.clone();
            stats.insert(id.clone(), AccountStats::default());
            disabled.insert(id.clone(), !acc.enabled);
            slots.insert(
                id.clone(),
                Arc::new(Slot {
                    account: acc,
                    sem: Arc::new(Semaphore::new(max_concurrency)),
                    cooldown_until: parking_lot::Mutex::new(None),
                }),
            );
            ordered.push(id);
        }

        Self {
            slots: Arc::new(slots),
            rr_index: Arc::new(AtomicUsize::new(0)),
            ordered_ids: Arc::new(parking_lot::RwLock::new(ordered)),
            available_ids: Arc::new(arc_swap::ArcSwap::from_pointee(Vec::new())),
            disabled: Arc::new(disabled),
            stats: Arc::new(stats),
            quotas: Arc::new(quotas),
            session_index: Arc::new(DashMap::new()),
            session_insert_count: Arc::new(AtomicU64::new(0)),
            max_concurrency,
        }
    }

    pub fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }

    /// 重建有序 id 列表（账号变更时调用）
    fn rebuild_ordered_ids(&self) {
        let mut ids: Vec<String> = self.slots.iter().map(|r| r.key().clone()).collect();
        ids.sort();
        *self.ordered_ids.write() = ids;
        self.rebuild_available_ids();
    }

    /// 重建可用账号列表（启用 + 非冷却 + 额度正常）
    /// 使用 arc-swap 原子替换，读侧完全无锁
    /// pub: admin API 禁用/启用账号后需要调用以确保立即生效
    pub fn rebuild_available_ids(&self) {
        let ids = self.ordered_ids.read();
        let available: Vec<String> = ids
            .iter()
            .filter(|id| {
                let enabled = !self.disabled.get(*id).map(|v| *v).unwrap_or(false);
                let cooling = self.slots.get(*id).map(|s| s.is_cooling_down()).unwrap_or(false);
                let quota_ok = !self.quota_blocks(id);
                enabled && !cooling && quota_ok
            })
            .cloned()
            .collect();
        self.available_ids.store(Arc::new(available));
    }

    pub fn replace_accounts(&self, accounts: Vec<Account>) {
        let mut seen = std::collections::HashSet::new();
        for acc in accounts {
            let id = acc.id.clone();
            seen.insert(id.clone());
            self.stats.entry(id.clone()).or_default();
            self.disabled.entry(id.clone()).or_insert(!acc.enabled);

            if let Some(mut slot) = self.slots.get_mut(&id) {
                // 更新现有账号信息，保留冷却和会话状态，但重建 semaphore 以应用新的 max_concurrency
                if let Some(s) = Arc::get_mut(&mut slot) {
                    s.account = acc;
                    s.sem = Arc::new(Semaphore::new(self.max_concurrency));
                }
            } else {
                self.slots.insert(
                    id.clone(),
                    Arc::new(Slot {
                        account: acc,
                        sem: Arc::new(Semaphore::new(self.max_concurrency)),
                        cooldown_until: parking_lot::Mutex::new(None),
                    }),
                );
            }
        }
        // 移除已删除的账号
        let to_remove: Vec<String> = self
            .slots
            .iter()
            .filter(|r| !seen.contains(r.key()))
            .map(|r| r.key().clone())
            .collect();
        for id in to_remove {
            self.slots.remove(&id);
            self.disabled.remove(&id);
            self.stats.remove(&id);
            self.quotas.remove(&id);
        }
        // 清理指向已删除账号的会话索引
        self.session_index.retain(|_, (aid, _)| seen.contains(aid));
        self.rebuild_ordered_ids();
    }

    pub fn get_account(&self, id: &str) -> Option<Account> {
        self.slots.get(id).map(|s| s.account.clone())
    }

    /// 会话一致性获取: 同一 session 优先路由到固定账号
    pub async fn acquire_by_session(
        &self,
        session_id: Option<&str>,
    ) -> Result<(Account, tokio::sync::OwnedSemaphorePermit), AcquireError> {
        match self.try_acquire_by_session(session_id) {
            AcquireTry::Got(pair) => Ok(pair),
            AcquireTry::Empty => Err(AcquireError::Empty),
            AcquireTry::Busy => {
                // 短暂退避后重试一次
                tokio::time::sleep(Duration::from_millis(10)).await;
                match self.try_acquire_by_session(session_id) {
                    AcquireTry::Got(pair) => Ok(pair),
                    AcquireTry::Empty => Err(AcquireError::Empty),
                    AcquireTry::Busy => Err(AcquireError::Busy),
                }
            }
        }
    }

    pub async fn acquire(&self) -> Result<(Account, tokio::sync::OwnedSemaphorePermit), AcquireError> {
        self.acquire_by_session(None).await
    }

    fn try_acquire_by_session(&self, session_id: Option<&str>) -> AcquireTry {
        // 1. 会话粘性: 通过全局索引 O(1) 查找，命中即刷新 last_seen
        if let Some(sid) = session_id {
            if let Some(mut entry) = self.session_index.get_mut(sid) {
                let (account_id, last_seen) = entry.value_mut();
                *last_seen = Instant::now();
                if let Some(slot) = self.slots.get(account_id) {
                    if self.is_slot_available(&slot) {
                        if let Ok(permit) = slot.sem.clone().try_acquire_owned() {
                            self.stats
                                .entry(slot.account.id.clone())
                                .or_default()
                                .record_request();
                            return AcquireTry::Got((slot.account.clone(), permit));
                        }
                    }
                }
            }
        }

        // 2. 可用账号列表轮询（arc-swap 无锁读，单次快照）
        let available = self.available_ids.load();
        let available = if available.is_empty() {
            // 可用列表为空 → 尝试重建一次（冷启动或全部冷却）
            drop(available);
            self.rebuild_available_ids();
            let rebuilt = self.available_ids.load();
            if rebuilt.is_empty() {
                return AcquireTry::Empty;
            }
            rebuilt
        } else {
            available
        };

        let start = self.rr_index.fetch_add(1, Ordering::Relaxed);
        let len = available.len();

        for i in 0..len {
            let idx = (start + i) % len;
            let id = &available[idx];

            if let Some(slot) = self.slots.get(id) {
                // 防御性检查：缓存可能过期（如禁用后未重建），确保 slot 真正可用
                if !self.is_slot_available(&slot) {
                    continue;
                }

                // 会话哈希优先（在可用列表中）
                if let Some(sid) = session_id {
                    let mut hasher = DefaultHasher::new();
                    sid.hash(&mut hasher);
                    if (hasher.finish() as usize) % len == idx {
                        if let Ok(permit) = slot.sem.clone().try_acquire_owned() {
                            // 绑定会话到账号（带 TTL 时间戳）
                            self.bind_session(sid, id);
                            self.stats
                                .entry(slot.account.id.clone())
                                .or_default()
                                .record_request();
                            return AcquireTry::Got((slot.account.clone(), permit));
                        }
                    }
                }

                // 普通轮询
                if let Ok(permit) = slot.sem.clone().try_acquire_owned() {
                    if let Some(sid) = session_id {
                        self.bind_session(sid, id);
                    }
                    self.stats
                        .entry(slot.account.id.clone())
                        .or_default()
                        .record_request();
                    return AcquireTry::Got((slot.account.clone(), permit));
                }
            }
        }

        // 可用列表非空但全部获取失败 → Busy
        AcquireTry::Busy
    }

    /// 绑定会话到账号，带摊销 GC（每 1024 次插入清理过期条目）
    fn bind_session(&self, sid: &str, account_id: &str) {
        self.session_index
            .insert(sid.to_string(), (account_id.to_string(), Instant::now()));
        let n = self.session_insert_count.fetch_add(1, Ordering::Relaxed);
        if n % SESSION_GC_EVERY == 0 {
            self.gc_session_index();
        }
    }

    /// 回收 TTL 过期的会话索引条目
    fn gc_session_index(&self) {
        let now = Instant::now();
        self.session_index
            .retain(|_, (_, last_seen)| now.duration_since(*last_seen) < SESSION_TTL);
    }

    /// 会话索引当前大小（监控用）
    pub fn session_index_len(&self) -> usize {
        self.session_index.len()
    }

    fn is_slot_available(&self, slot: &Arc<Slot>) -> bool {
        let id = &slot.account.id;
        let enabled = !self.disabled.get(id).map(|v| *v).unwrap_or(false);
        let cooling = slot.is_cooling_down();
        let quota_ok = !self.quota_blocks(id);
        enabled && !cooling && quota_ok
    }

    fn quota_blocks(&self, account_id: &str) -> bool {
        self.quotas
            .get(account_id)
            .map(|snap| {
                snap.has_available_usage == Some(false)
                    || snap.usage_percent.map(|p| p >= 99.5).unwrap_or(false)
            })
            .unwrap_or(false)
    }

    /// 释放账号: failed=true 增加错误计数并可能触发冷却
    pub fn release(&self, account_id: &str, failed: bool, cooldown_secs: u64) {
        if let Some(stats) = self.stats.get(account_id) {
            if failed {
                stats.record_error();
            } else {
                stats.record_success();
            }
        }

        if failed {
            if let Some(slot) = self.slots.get(account_id) {
                let secs = self.dynamic_cooldown_secs(account_id, cooldown_secs);
                slot.set_cooldown(secs);
            }
        }
    }

    /// 动态冷却: 错误率越高冷却越长，上限 5 分钟
    fn dynamic_cooldown_secs(&self, account_id: &str, base_secs: u64) -> u64 {
        let stats = match self.stats.get(account_id) {
            Some(s) => s,
            None => return base_secs,
        };
        let consecutive = stats.consecutive_errors();
        let rate = stats.error_rate();

        let mut secs = base_secs.max(5);
        if consecutive >= 3 {
            secs = secs.saturating_mul(2);
        }
        if rate > 0.5 {
            secs = secs.saturating_mul(2);
        }
        secs.min(300)
    }

    /// 检查是否应自动禁用（连续错误 >= threshold）
    pub fn should_auto_disable(&self, account_id: &str, threshold: u64) -> bool {
        self.stats
            .get(account_id)
            .map(|s| s.consecutive_errors() >= threshold)
            .unwrap_or(false)
    }

    /// 自动禁用账号
    pub fn auto_disable(&self, account_id: &str) -> bool {
        if let Some(mut disabled) = self.disabled.get_mut(account_id) {
            *disabled = true;
            if let Some(stats) = self.stats.get(account_id) {
                stats.auto_disabled_count.fetch_add(1, Ordering::Relaxed);
                stats.consecutive_errors.store(0, Ordering::Relaxed);
            }
            true
        } else {
            false
        }
    }

    /// 手动启用/禁用
    pub fn set_enabled(&self, account_id: &str, enabled: bool) -> Option<bool> {
        self.disabled.get_mut(account_id).map(|mut v| {
            let old = *v;
            *v = !enabled;
            old
        })
    }

    pub fn toggle_account(&self, account_id: &str) -> Option<bool> {
        self.disabled.get_mut(account_id).map(|mut v| {
            *v = !*v;
            !*v
        })
    }

    pub fn has_account(&self, account_id: &str) -> bool {
        self.slots.contains_key(account_id)
    }

    /// 清除冷却
    pub fn clear_cooldown(&self, account_id: &str) -> bool {
        if let Some(slot) = self.slots.get(account_id) {
            slot.clear_cooldown();
            true
        } else {
            false
        }
    }

    /// 设置额度快照
    pub fn set_quota(&self, account_id: &str, snap: QuotaSnapshot) {
        self.quotas.insert(account_id.to_string(), snap);
    }

    /// 获取额度快照
    pub fn get_quota(&self, account_id: &str) -> Option<QuotaSnapshot> {
        self.quotas.get(account_id).map(|r| r.clone())
    }

    /// 账号健康评分
    pub fn health_score(&self, account_id: &str) -> Option<HealthScore> {
        let slot = self.slots.get(account_id)?;
        let stats = self.stats.get(account_id)?;
        let enabled = !self.disabled.get(account_id).map(|v| *v).unwrap_or(false);
        let cooldown_active = slot.is_cooling_down();
        let quota_exhausted = self.quota_blocks(account_id);
        let error_rate = stats.error_rate();
        let consecutive = stats.consecutive_errors();

        let mut score: i32 = 100;
        if !enabled {
            score -= 50;
        }
        if cooldown_active {
            score -= 20;
        }
        if quota_exhausted {
            score -= 30;
        }
        score -= (error_rate * 30.0) as i32;
        score -= (consecutive.min(10) * 3) as i32;

        Some(HealthScore {
            score: score.max(0).min(100) as u8,
            error_rate,
            consecutive_errors: consecutive,
            cooldown_active,
            quota_exhausted,
            enabled,
        })
    }

    /// 所有账号健康评分
    pub fn all_health_scores(&self) -> Vec<serde_json::Value> {
        self.slots
            .iter()
            .filter_map(|entry| {
                let id = entry.key();
                self.health_score(id).map(|h| {
                    serde_json::json!({
                        "id": id,
                        "health": h,
                        "account": entry.value().account,
                    })
                })
            })
            .collect()
    }

    /// 统计信息
    pub fn stats(&self) -> serde_json::Value {
        let mut accounts = Vec::new();
        let mut available = 0usize;
        let mut total_requests = 0u64;
        let mut total_errors = 0u64;

        for entry in self.slots.iter() {
            let id = entry.key();
            let slot = entry.value();
            let enabled = !self.disabled.get(id).map(|v| *v).unwrap_or(false);
            let cooling = slot.is_cooling_down();
            let quota_ok = !self.quota_blocks(id);
            let is_available = enabled && !cooling && quota_ok;

            if is_available {
                available += 1;
            }

            let stats = self.stats.get(id).map(|s| {
                let req = s.requests.load(Ordering::Relaxed);
                let err = s.errors.load(Ordering::Relaxed);
                total_requests += req;
                total_errors += err;
                serde_json::json!({
                    "requests": req,
                    "errors": err,
                    "consecutive_errors": s.consecutive_errors(),
                    "error_rate": s.error_rate(),
                    "auto_disabled_count": s.auto_disabled_count.load(Ordering::Relaxed),
                })
            }).unwrap_or(serde_json::json!({}));

            let cooldown_remaining = slot.cooldown_remaining().map(|d| d.as_secs());

            accounts.push(serde_json::json!({
                "id": id,
                "enabled": enabled,
                "cooldown": cooling,
                "cooldown_remaining_secs": cooldown_remaining,
                "quota_ok": quota_ok,
                "available": is_available,
                "stats": stats,
                "health_score": self.health_score(id).map(|h| h.score),
            }));
        }

        serde_json::json!({
            "total_accounts": self.slots.len(),
            "available": available,
            "total_requests": total_requests,
            "total_errors": total_errors,
            "accounts": accounts,
        })
    }

    /// 账号列表（用于导出）
    pub fn account_rows(&self) -> Vec<serde_json::Value> {
        self.slots
            .iter()
            .map(|entry| {
                let acc = &entry.value().account;
                serde_json::json!({
                    "id": acc.id,
                    "access_token": acc.access_token,
                    "machine_id": acc.machine_id,
                    "refresh_token": acc.refresh_token,
                    "enabled": !self.disabled.get(&acc.id).map(|v| *v).unwrap_or(false),
                    "token_expires_at": acc.token_expires_at,
                })
            })
            .collect()
    }

    /// 记录成功
    pub fn record_success(&self, account_id: &str) {
        if let Some(stats) = self.stats.get(account_id) {
            stats.record_success();
        }
    }

    /// 获取冷却中的账号列表（用于定时探测）
    pub fn cooling_accounts(&self) -> Vec<(String, Account)> {
        self.slots
            .iter()
            .filter(|entry| entry.value().is_cooling_down())
            .map(|entry| (entry.key().clone(), entry.value().account.clone()))
            .collect()
    }

    /// 获取额度耗尽的账号列表
    pub fn quota_exhausted_accounts(&self) -> Vec<(String, Account)> {
        self.slots
            .iter()
            .filter(|entry| self.quota_blocks(entry.key()))
            .map(|entry| (entry.key().clone(), entry.value().account.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acc(id: &str, enabled: bool) -> Account {
        Account {
            id: id.into(),
            access_token: format!("token-{}", id),
            machine_id: format!("mid-{}", id),
            refresh_token: String::new(),
            enabled,
            token_expires_at: None,
            refresh_url: None,
        }
    }

    #[tokio::test]
    async fn acquire_round_robin() {
        let pool = AccountPool::new(vec![acc("a", true), acc("b", true)], 1);
        let (a1, _p1) = pool.acquire().await.unwrap();
        let (a2, _p2) = pool.acquire().await.unwrap();
        assert_ne!(a1.id, a2.id);
    }

    #[tokio::test]
    async fn session_stickiness() {
        let pool = AccountPool::new(vec![acc("a", true), acc("b", true)], 2);
        let (a1, _p1) = pool.acquire_by_session(Some("sess1")).await.unwrap();
        let (a2, _p2) = pool.acquire_by_session(Some("sess1")).await.unwrap();
        assert_eq!(a1.id, a2.id);
    }

    #[tokio::test]
    async fn disabled_account_skipped() {
        let pool = AccountPool::new(vec![acc("a", false), acc("b", true)], 1);
        let (a, _p) = pool.acquire().await.unwrap();
        assert_eq!(a.id, "b");
    }

    #[tokio::test]
    async fn cooldown_blocks_acquire() {
        let pool = AccountPool::new(vec![acc("a", true)], 1);
        pool.release("a", true, 30);
        assert!(pool.acquire().await.is_err());
    }

    #[tokio::test]
    async fn clear_cooldown_unblocks() {
        let pool = AccountPool::new(vec![acc("a", true)], 1);
        pool.release("a", true, 30);
        assert!(pool.clear_cooldown("a"));
        let (a, _p) = pool.acquire().await.unwrap();
        assert_eq!(a.id, "a");
    }

    #[tokio::test]
    async fn quota_exhausted_skipped() {
        let pool = AccountPool::new(vec![acc("a", true), acc("b", true)], 1);
        pool.set_quota(
            "a",
            QuotaSnapshot {
                has_available_usage: Some(false),
                usage_percent: Some(100.0),
                ..Default::default()
            },
        );
        let (a, _p) = pool.acquire().await.unwrap();
        assert_eq!(a.id, "b");
    }

    #[test]
    fn auto_disable_threshold() {
        let pool = AccountPool::new(vec![acc("a", true)], 1);
        for _ in 0..5 {
            pool.release("a", true, 10);
        }
        assert!(pool.should_auto_disable("a", 5));
        assert!(pool.auto_disable("a"));
        assert!(!pool.should_auto_disable("a", 5)); // 已禁用，consecutive 重置
    }

    #[test]
    fn health_score_calculation() {
        let pool = AccountPool::new(vec![acc("a", true)], 1);
        let score = pool.health_score("a").unwrap();
        assert_eq!(score.score, 100);
        assert!(score.enabled);
        assert!(!score.cooldown_active);
    }

    #[test]
    fn empty_pool_is_empty() {
        let pool = AccountPool::new(vec![], 1);
        assert!(matches!(pool.try_acquire_by_session(None), AcquireTry::Empty));
    }
}
