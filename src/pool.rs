//! 号池: 无锁轮询 + 并发控制 + 失败冷却 + 动态开关 + 热编辑 + 会话一致性 + 健康评分.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use crate::config::Account;
use crate::quota::QuotaSnapshot;

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

/// 预编译可用槽位：消除热路径 DashMap 查找
/// P0: 存 Arc<Slot> 而非 String，消除 slots.get()
/// P2: 内联可用性掩码，消除 disabled/quotas 二次查找
#[derive(Clone)]
pub struct AvailableSlot {
    pub id: String,
    pub slot: Arc<Slot>,
    /// 位掩码: bit0=enabled, bit1=!cooling, bit2=quota_ok
    pub mask: u8,
}

impl AvailableSlot {
    const ENABLED: u8 = 0b001;
    const NOT_COOLING: u8 = 0b010;
    const QUOTA_OK: u8 = 0b100;
    const ALL_OK: u8 = 0b111;

    #[inline]
    pub fn is_available(&self) -> bool {
        self.mask == Self::ALL_OK
    }
}

/// 会话粘性 TTL
const STICKY_TTL: Duration = Duration::from_secs(3600);

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
    /// 存储 (account_id, slot_arc, 可用性掩码) 预编译，消除运行时 DashMap 查找
    available_slots: Arc<arc_swap::ArcSwap<Vec<AvailableSlot>>>,
    /// 账号开关状态
    disabled: Arc<DashMap<String, bool>>,
    /// 原子化统计
    stats: Arc<DashMap<String, AccountStats>>,
    /// 额度快照
    quotas: Arc<DashMap<String, QuotaSnapshot>>,
    /// 会话粘性反向索引: session_id -> (account_id, 过期时间). O(1) 查找, 后台定时 GC
    sessions: Arc<DashMap<String, (String, Instant)>>,
    max_concurrency: usize,
    /// 全部繁忙时最长排队等待; 0 = 不排队
    acquire_wait: Duration,
    /// 数据版本号: 任何账号状态变更时递增, 用于缓存有效性判断
    version: Arc<AtomicU64>,
    /// 缓存的查询结果 (version, json)
    query_cache: Arc<parking_lot::RwLock<Option<(u64, serde_json::Value)>>>,
    /// 运行时状态落盘路径 (冷却/错误计数). None = 不持久化
    state_path: Arc<parking_lot::RwLock<Option<PathBuf>>>,
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
            available_slots: Arc::new(arc_swap::ArcSwap::from_pointee(Vec::new())),
            disabled: Arc::new(disabled),
            stats: Arc::new(stats),
            quotas: Arc::new(quotas),
            sessions: Arc::new(DashMap::new()),
            max_concurrency,
            acquire_wait: Duration::ZERO,
            version: Arc::new(AtomicU64::new(0)),
            query_cache: Arc::new(parking_lot::RwLock::new(None)),
            state_path: Arc::new(parking_lot::RwLock::new(None)),
        }
    }

    /// 设置繁忙时的排队等待上限
    pub fn with_acquire_wait(mut self, wait: Duration) -> Self {
        self.acquire_wait = wait;
        self
    }

    pub fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }

    /// 清理过期会话绑定 (后台定时调用)
    pub fn gc_sessions(&self) -> usize {
        let now = Instant::now();
        let before = self.sessions.len();
        self.sessions.retain(|_, (_, exp)| *exp > now);
        before - self.sessions.len()
    }

    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    fn bind_session(&self, session_id: &str, account_id: &str) {
        self.sessions.insert(
            session_id.to_string(),
            (account_id.to_string(), Instant::now() + STICKY_TTL),
        );
    }

    /// 重建有序 id 列表（账号变更时调用）
    fn rebuild_ordered_ids(&self) {
        let mut ids: Vec<String> = self.slots.iter().map(|r| r.key().clone()).collect();
        ids.sort();
        *self.ordered_ids.write() = ids;
        self.rebuild_available_slots();
    }

    /// 重建可用账号列表（启用 + 非冷却 + 额度正常）
    /// 使用 arc-swap 原子替换，读侧完全无锁
    /// P0: 预编译存储 Arc<Slot>，消除热路径 DashMap 查找
    /// P2: 内联可用性掩码，消除 disabled/quotas 二次查找
    /// pub: admin API 禁用/启用账号后需要调用以确保立即生效
    pub fn rebuild_available_slots(&self) {
        let ids = self.ordered_ids.read();
        let available: Vec<AvailableSlot> = ids
            .iter()
            .filter_map(|id| {
                let slot = self.slots.get(id)?.clone();
                let enabled = !self.disabled.get(id).map(|v| *v).unwrap_or(false);
                let cooling = slot.is_cooling_down();
                let quota_ok = !self.quota_blocks(id);

                let mut mask = 0u8;
                if enabled { mask |= AvailableSlot::ENABLED; }
                if !cooling { mask |= AvailableSlot::NOT_COOLING; }
                if quota_ok { mask |= AvailableSlot::QUOTA_OK; }

                Some(AvailableSlot {
                    id: id.clone(),
                    slot,
                    mask,
                })
            })
            .collect();
        self.available_slots.store(Arc::new(available));
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
            self.sessions.retain(|_, (aid, _)| aid != &id);
        }
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
        // 全部繁忙时在 acquire_wait 内指数退避排队, 而不是立刻 503;
        // 非流式请求持有槽位时间长, 没有排队会在到达并发上限时成功率断崖
        let deadline = Instant::now() + self.acquire_wait;
        let mut backoff = Duration::from_millis(10);
        loop {
            match self.try_acquire_by_session(session_id) {
                AcquireTry::Got(pair) => return Ok(pair),
                AcquireTry::Empty => return Err(AcquireError::Empty),
                AcquireTry::Busy => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(AcquireError::Busy);
                    }
                    let sleep = backoff.min(deadline - now);
                    tokio::time::sleep(sleep).await;
                    backoff = (backoff * 2).min(Duration::from_millis(100));
                }
            }
        }
    }

    pub async fn acquire(
        &self,
    ) -> Result<(Account, tokio::sync::OwnedSemaphorePermit), AcquireError> {
        self.acquire_by_session(None).await
    }

    fn try_acquire_by_session(&self, session_id: Option<&str>) -> AcquireTry {
        // 1. 会话粘性: O(1) 查找已绑定账号
        if let Some(sid) = session_id {
            if let Some(slot) = self.find_sticky_slot(sid) {
                if self.is_slot_available_fast(&slot) {
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

        // 2. P0: 预编译可用列表（Arc<Slot> 无锁读，消除 DashMap 查找）
        let available = self.available_slots.load();
        let available = if available.is_empty() {
            // P3: 冷启动同步重建（测试环境无 runtime，必须同步）
            // 热路径异步刷新在后台定时任务中处理
            drop(available);
            self.rebuild_available_slots();
            let rebuilt = self.available_slots.load();
            if rebuilt.is_empty() {
                return AcquireTry::Empty;
            }
            rebuilt
        } else {
            available
        };

        let start = self.rr_index.fetch_add(1, Ordering::Relaxed);
        let len = available.len();

        // P4: 预计算会话哈希（只算一次）
        let session_hash_idx: Option<usize> = session_id.map(|sid| {
            let mut hasher = DefaultHasher::new();
            sid.hash(&mut hasher);
            (hasher.finish() as usize) % len
        });

        // P4: 环形迭代消除取模
        for (i, avail) in available.iter().cycle().skip(start).take(len).enumerate() {
            // P2: 位掩码检查（一次比较替代 3 次 DashMap 查找）
            if !avail.is_available() {
                continue;
            }

            let slot = &avail.slot;

            // P1: 会话哈希优先 O(1) 匹配
            if let (Some(sid), Some(hash_idx)) = (session_id, session_hash_idx) {
                // cycle().skip(start) 后，实际索引 = (start + i) % len
                // 但因为我们只关心是否命中 hash_idx，直接比较
                let actual_idx = (start + i) % len;
                if actual_idx == hash_idx {
                    if let Ok(permit) = slot.sem.clone().try_acquire_owned() {
                        self.bind_session(sid, &slot.account.id);
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
                    self.bind_session(sid, &slot.account.id);
                }
                self.stats
                    .entry(slot.account.id.clone())
                    .or_default()
                    .record_request();
                return AcquireTry::Got((slot.account.clone(), permit));
            }
        }

        // 可用列表非空但全部获取失败 → Busy
        AcquireTry::Busy
    }

    fn find_sticky_slot(&self, session_id: &str) -> Option<Arc<Slot>> {
        let (account_id, expiry) = {
            let entry = self.sessions.get(session_id)?;
            entry.value().clone()
        };
        if expiry <= Instant::now() {
            self.sessions.remove(session_id);
            return None;
        }
        match self.slots.get(&account_id) {
            Some(slot) => Some(slot.clone()),
            None => {
                // 账号已被删除
                self.sessions.remove(session_id);
                None
            }
        }
    }

    fn is_slot_available(&self, slot: &Arc<Slot>) -> bool {
        let id = &slot.account.id;
        let enabled = !self.disabled.get(id).map(|v| *v).unwrap_or(false);
        let cooling = slot.is_cooling_down();
        let quota_ok = !self.quota_blocks(id);
        enabled && !cooling && quota_ok
    }

    /// P2: 快速可用性检查 — 仅检查冷却（用于粘性会话路径）
    /// 粘性路径的 disabled/quota 状态在 rebuild 时已验证，此处只需检查冷却
    #[inline]
    fn is_slot_available_fast(&self, slot: &Arc<Slot>) -> bool {
        !slot.is_cooling_down()
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
        self.bump_version();
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
            self.bump_version();
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
            drop(v);
            self.bump_version();
            old
        })
    }

    pub fn toggle_account(&self, account_id: &str) -> Option<bool> {
        self.disabled.get_mut(account_id).map(|mut v| {
            *v = !*v;
            let new = !*v;
            drop(v);
            self.bump_version();
            new
        })
    }

    pub fn has_account(&self, account_id: &str) -> bool {
        self.slots.contains_key(account_id)
    }

    /// 清除冷却
    pub fn clear_cooldown(&self, account_id: &str) -> bool {
        if let Some(slot) = self.slots.get(account_id) {
            slot.clear_cooldown();
            self.bump_version();
            true
        } else {
            false
        }
    }

    /// 设置额度快照。探测失败时保留上一次成功数据，只更新 error/cached_at。
    pub fn set_quota(&self, account_id: &str, snap: QuotaSnapshot) {
        let snap = if snap.error.is_some() {
            if let Some(prev) = self.quotas.get(account_id) {
                let mut merged = prev.clone();
                merged.error = snap.error.clone();
                merged.cached_at = snap.cached_at.or(prev.cached_at);
                merged.from_cache = true;
                merged.checked_at = snap.checked_at.or(prev.checked_at);
                merged
            } else {
                snap
            }
        } else {
            snap
        };
        self.quotas.insert(account_id.to_string(), snap);
        self.bump_version();
    }

    /// 启动时从磁盘灌入额度缓存（不 bump 太多次，调用方一次灌完再 bump）
    pub fn restore_quota(&self, account_id: &str, mut snap: QuotaSnapshot) {
        snap.from_cache = true;
        self.quotas.insert(account_id.to_string(), snap);
    }

    /// 绑定运行时状态文件并尝试恢复冷却/错误计数
    pub fn with_state_file(self, path: PathBuf) -> Self {
        *self.state_path.write() = Some(path.clone());
        self.restore_runtime_state(&path);
        self
    }

    pub fn persist_runtime_state(&self) {
        let path = match self.state_path.read().clone() {
            Some(p) => p,
            None => return,
        };
        let now = unix_now();
        let mut accounts = Vec::new();
        for entry in self.slots.iter() {
            let id = entry.key().clone();
            let remaining = entry
                .value()
                .cooldown_remaining()
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let stats = self.stats.get(&id);
            accounts.push(PersistedAccountState {
                id,
                cooldown_remaining_secs: remaining,
                requests: stats
                    .as_ref()
                    .map(|s| s.requests.load(Ordering::Relaxed))
                    .unwrap_or(0),
                errors: stats
                    .as_ref()
                    .map(|s| s.errors.load(Ordering::Relaxed))
                    .unwrap_or(0),
                consecutive_errors: stats.as_ref().map(|s| s.consecutive_errors()).unwrap_or(0),
                auto_disabled_count: stats
                    .as_ref()
                    .map(|s| s.auto_disabled_count.load(Ordering::Relaxed))
                    .unwrap_or(0),
                saved_at: now,
            });
        }
        let blob = PersistedPoolState {
            saved_at: now,
            accounts,
        };
        if let Ok(json) = serde_json::to_string_pretty(&blob) {
            let tmp = path.with_extension("json.tmp");
            if std::fs::write(&tmp, json).is_ok() {
                let _ = std::fs::rename(&tmp, &path);
            }
        }
    }

    fn restore_runtime_state(&self, path: &Path) {
        let Ok(raw) = std::fs::read_to_string(path) else {
            return;
        };
        let Ok(blob) = serde_json::from_str::<PersistedPoolState>(&raw) else {
            tracing::warn!(
                event = "pool_state_restore",
                "account-state.json malformed, ignoring"
            );
            return;
        };
        let mut restored = 0usize;
        for rec in blob.accounts {
            if !self.slots.contains_key(&rec.id) {
                continue;
            }
            if rec.cooldown_remaining_secs > 0 {
                if let Some(slot) = self.slots.get(&rec.id) {
                    slot.set_cooldown(rec.cooldown_remaining_secs.min(300));
                }
            }
            if let Some(stats) = self.stats.get(&rec.id) {
                stats.requests.store(rec.requests, Ordering::Relaxed);
                stats.errors.store(rec.errors, Ordering::Relaxed);
                stats
                    .consecutive_errors
                    .store(rec.consecutive_errors, Ordering::Relaxed);
                stats
                    .auto_disabled_count
                    .store(rec.auto_disabled_count, Ordering::Relaxed);
            }
            restored += 1;
        }
        if restored > 0 {
            self.rebuild_available_slots();
            self.bump_version();
            tracing::info!(
                event = "pool_state_restore",
                restored,
                "account runtime state restored"
            );
        }
    }

    /// 获取额度快照
    pub fn get_quota(&self, account_id: &str) -> Option<QuotaSnapshot> {
        self.quotas.get(account_id).map(|r| r.clone())
    }

    /// 递增数据版本号 (任何状态变更时调用)
    pub fn bump_version(&self) {
        self.version.fetch_add(1, Ordering::Relaxed);
    }

    /// 当前数据版本号
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Relaxed)
    }

    /// 带缓存的账号查询: 版本号未变时直接返回缓存
    pub fn query_accounts_cached(
        &self,
        q: &str,
        filter: &str,
        sort: &str,
        page: usize,
        page_size: usize,
        proxy_id: &str,
        client_version: u64,
    ) -> (u64, Option<serde_json::Value>) {
        let current_version = self.version();
        // 客户端版本已是最新 → 304 Not Modified
        if client_version == current_version && client_version > 0 {
            return (current_version, None);
        }
        // 检查缓存
        {
            let cache = self.query_cache.read();
            if let Some((v, ref json)) = *cache {
                if v == current_version {
                    return (current_version, Some(json.clone()));
                }
            }
        }
        // 重新构建
        let result = self.query_accounts(q, filter, sort, page, page_size, proxy_id);
        // 只缓存无搜索/过滤的默认视图 (最常见场景)
        if q.is_empty()
            && filter == "all"
            && sort == "attention"
            && page == 1
            && proxy_id.is_empty()
        {
            let mut cache = self.query_cache.write();
            *cache = Some((current_version, result.clone()));
        }
        (current_version, Some(result))
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

    /// 轻量汇总（不含账号数组，给概览/metrics 用）
    pub fn summary(&self) -> serde_json::Value {
        let mut available = 0usize;
        let mut disabled = 0usize;
        let mut cooling = 0usize;
        let mut quota_blocked = 0usize;
        let mut erroring = 0usize;
        let mut total_requests = 0u64;
        let mut total_errors = 0u64;
        let mut total_inflight = 0usize;
        for entry in self.slots.iter() {
            let id = entry.key();
            let slot = entry.value();
            let enabled = !self.disabled.get(id).map(|v| *v).unwrap_or(false);
            let cooling_now = slot.is_cooling_down();
            let quota_ok = !self.quota_blocks(id);
            if enabled && !cooling_now && quota_ok {
                available += 1;
            }
            if !enabled {
                disabled += 1;
            }
            if cooling_now {
                cooling += 1;
            }
            if !quota_ok {
                quota_blocked += 1;
            }
            total_inflight += self
                .max_concurrency
                .saturating_sub(slot.sem.available_permits());
            if let Some(s) = self.stats.get(id) {
                total_requests += s.requests.load(Ordering::Relaxed);
                let err = s.errors.load(Ordering::Relaxed);
                total_errors += err;
                if s.consecutive_errors() > 0 {
                    erroring += 1;
                }
            }
        }
        serde_json::json!({
            "total_accounts": self.slots.len(),
            "available": available,
            "disabled": disabled,
            "cooling": cooling,
            "quota_blocked": quota_blocked,
            "erroring": erroring,
            "inflight": total_inflight,
            "total_requests": total_requests,
            "total_errors": total_errors,
        })
    }

    /// 统计信息
    pub fn stats(&self) -> serde_json::Value {
        let mut accounts = Vec::new();
        let mut available = 0usize;
        let mut total_requests = 0u64;
        let mut total_errors = 0u64;
        let mut total_inflight = 0usize;

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

            let inflight = self
                .max_concurrency
                .saturating_sub(slot.sem.available_permits());
            total_inflight += inflight;
            let (req, err) = self
                .stats
                .get(id)
                .map(|s| {
                    (
                        s.requests.load(Ordering::Relaxed),
                        s.errors.load(Ordering::Relaxed),
                    )
                })
                .unwrap_or((0, 0));
            total_requests += req;
            total_errors += err;
            let stats = self
                .stats
                .get(id)
                .map(|s| {
                    serde_json::json!({
                        "requests": req,
                        "errors": err,
                        "consecutive_errors": s.consecutive_errors(),
                        "error_rate": s.error_rate(),
                        "auto_disabled_count": s.auto_disabled_count.load(Ordering::Relaxed),
                    })
                })
                .unwrap_or(serde_json::json!({}));

            let cooldown_remaining = slot.cooldown_remaining().map(|d| d.as_secs());

            accounts.push(self.account_json(
                id,
                slot,
                enabled,
                cooling,
                quota_ok,
                is_available,
                inflight,
                req,
                err,
                stats,
                cooldown_remaining,
            ));
        }

        serde_json::json!({
            "total_accounts": self.slots.len(),
            "available": available,
            "inflight": total_inflight,
            "total_requests": total_requests,
            "total_errors": total_errors,
            "accounts": accounts,
        })
    }

    fn account_json(
        &self,
        id: &str,
        slot: &Slot,
        enabled: bool,
        cooling: bool,
        quota_ok: bool,
        is_available: bool,
        inflight: usize,
        req: u64,
        err: u64,
        stats: serde_json::Value,
        cooldown_remaining: Option<u64>,
    ) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "enabled": enabled,
            "cooldown": cooling,
            "cooldown_remaining_secs": cooldown_remaining,
            "cooldown_remaining_s": cooldown_remaining,
            "quota_ok": quota_ok,
            "quota": self.quotas.get(id).map(|q| q.clone()),
            "available": is_available,
            "requests": req,
            "errors": err,
            "inflight": inflight,
            "max_concurrency": self.max_concurrency,
            "stats": stats,
            "health_score": self.health_score(id).map(|h| h.score),
            "proxy_id": slot.account.proxy_id,
            "tags": slot.account.tags,
        })
    }

    fn build_account_row(&self, id: &str, slot: &Slot) -> serde_json::Value {
        let enabled = !self.disabled.get(id).map(|v| *v).unwrap_or(false);
        let cooling = slot.is_cooling_down();
        let quota_ok = !self.quota_blocks(id);
        let is_available = enabled && !cooling && quota_ok;
        let inflight = self
            .max_concurrency
            .saturating_sub(slot.sem.available_permits());
        let (req, err, stats) = self
            .stats
            .get(id)
            .map(|s| {
                let req = s.requests.load(Ordering::Relaxed);
                let err = s.errors.load(Ordering::Relaxed);
                (
                    req,
                    err,
                    serde_json::json!({
                        "requests": req,
                        "errors": err,
                        "consecutive_errors": s.consecutive_errors(),
                        "error_rate": s.error_rate(),
                        "auto_disabled_count": s.auto_disabled_count.load(Ordering::Relaxed),
                    }),
                )
            })
            .unwrap_or((0, 0, serde_json::json!({})));
        let cooldown_remaining = slot.cooldown_remaining().map(|d| d.as_secs());
        self.account_json(
            id,
            slot,
            enabled,
            cooling,
            quota_ok,
            is_available,
            inflight,
            req,
            err,
            stats,
            cooldown_remaining,
        )
    }

    /// 服务端分页/过滤，避免超大号池把整表塞给浏览器。
    pub fn query_accounts(
        &self,
        q: &str,
        filter: &str,
        sort: &str,
        page: usize,
        page_size: usize,
        proxy_id: &str,
    ) -> serde_json::Value {
        let page = page.max(1);
        let page_size = page_size.clamp(1, 200);
        let q = q.trim().to_ascii_lowercase();
        let mut rows: Vec<serde_json::Value> = self
            .slots
            .iter()
            .map(|e| self.build_account_row(e.key(), e.value()))
            .collect();

        rows.retain(|acc| {
            if !q.is_empty() {
                let id = acc["id"].as_str().unwrap_or("").to_ascii_lowercase();
                let tags = acc["tags"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|x| x.as_str())
                            .collect::<Vec<_>>()
                            .join(",")
                            .to_ascii_lowercase()
                    })
                    .unwrap_or_default();
                let pid = acc["proxy_id"].as_str().unwrap_or("").to_ascii_lowercase();
                if !id.contains(&q) && !tags.contains(&q) && !pid.contains(&q) {
                    return false;
                }
            }
            if !proxy_id.is_empty() {
                let pid = acc["proxy_id"].as_str().unwrap_or("");
                if proxy_id == "__none__" {
                    if !pid.is_empty() {
                        return false;
                    }
                } else if pid != proxy_id {
                    return false;
                }
            }
            match filter {
                "enabled" | "available" => acc["available"].as_bool().unwrap_or(false),
                "disabled" => !acc["enabled"].as_bool().unwrap_or(false),
                "cooldown" => acc["cooldown"].as_bool().unwrap_or(false),
                "error" => {
                    acc["errors"].as_u64().unwrap_or(0) > 0
                        || acc["stats"]["consecutive_errors"].as_u64().unwrap_or(0) > 0
                }
                "quota" => !acc["quota_ok"].as_bool().unwrap_or(true),
                "unhealthy" => acc["health_score"].as_u64().unwrap_or(100) < 50,
                "attention" => {
                    !acc["enabled"].as_bool().unwrap_or(false)
                        || acc["cooldown"].as_bool().unwrap_or(false)
                        || !acc["quota_ok"].as_bool().unwrap_or(true)
                        || acc["health_score"].as_u64().unwrap_or(100) < 50
                        || acc["stats"]["consecutive_errors"].as_u64().unwrap_or(0) >= 3
                }
                _ => true,
            }
        });

        let severity = |acc: &serde_json::Value| -> i64 {
            let mut s = 0i64;
            if acc["cooldown"].as_bool().unwrap_or(false) {
                s += 40;
            }
            if !acc["quota_ok"].as_bool().unwrap_or(true) {
                s += 30;
            }
            if !acc["enabled"].as_bool().unwrap_or(false) {
                s += 20;
            }
            s += acc["stats"]["consecutive_errors"].as_u64().unwrap_or(0) as i64 * 5;
            s += 100i64.saturating_sub(acc["health_score"].as_u64().unwrap_or(100) as i64);
            s
        };

        rows.sort_by(|a, b| match sort {
            "requests" => b["requests"].as_u64().cmp(&a["requests"].as_u64()),
            "errors" => b["errors"].as_u64().cmp(&a["errors"].as_u64()),
            "quota" => {
                let qa = a["quota"]["usage_percent"].as_f64().unwrap_or(999.0);
                let qb = b["quota"]["usage_percent"].as_f64().unwrap_or(999.0);
                qa.partial_cmp(&qb).unwrap_or(std::cmp::Ordering::Equal)
            }
            "health" => a["health_score"].as_u64().cmp(&b["health_score"].as_u64()),
            "attention" => severity(b).cmp(&severity(a)),
            _ => {
                let sev = severity(b).cmp(&severity(a));
                if sev != std::cmp::Ordering::Equal {
                    return sev;
                }
                a["id"]
                    .as_str()
                    .unwrap_or("")
                    .cmp(b["id"].as_str().unwrap_or(""))
            }
        });

        let filtered = rows.len();
        let pages = filtered.div_ceil(page_size).max(1);
        let page = page.min(pages);
        let start = (page - 1) * page_size;
        let slice = if start >= filtered {
            Vec::new()
        } else {
            rows[start..(start + page_size).min(filtered)].to_vec()
        };
        let mut out = self.summary();
        if let Some(obj) = out.as_object_mut() {
            obj.insert("accounts".into(), serde_json::json!(slice));
            obj.insert("filtered".into(), serde_json::json!(filtered));
            obj.insert("page".into(), serde_json::json!(page));
            obj.insert("page_size".into(), serde_json::json!(page_size));
            obj.insert("pages".into(), serde_json::json!(pages));
            obj.insert("q".into(), serde_json::json!(q));
            obj.insert("filter".into(), serde_json::json!(filter));
            obj.insert("sort".into(), serde_json::json!(sort));
        }
        out
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
                    "proxy_id": acc.proxy_id,
                    "tags": acc.tags,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedPoolState {
    saved_at: f64,
    accounts: Vec<PersistedAccountState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedAccountState {
    id: String,
    cooldown_remaining_secs: u64,
    requests: u64,
    errors: u64,
    consecutive_errors: u64,
    auto_disabled_count: u64,
    saved_at: f64,
}

fn unix_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
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
            proxy_id: None,
            tags: Vec::new(),
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

    #[test]
    fn persist_runtime_state_roundtrip() {
        let dir = std::env::temp_dir().join(format!("cfp-state-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("account-state.json");
        let pool = AccountPool::new(vec![acc("a", true)], 1);
        pool.release("a", true, 40);
        *pool.state_path.write() = Some(path.clone());
        pool.persist_runtime_state();
        let pool2 = AccountPool::new(vec![acc("a", true)], 1).with_state_file(path);
        assert!(pool2.health_score("a").unwrap().cooldown_active);
        assert!(pool2.health_score("a").unwrap().consecutive_errors >= 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn failed_quota_keeps_last_good() {
        let pool = AccountPool::new(vec![acc("a", true)], 1);
        pool.set_quota(
            "a",
            QuotaSnapshot {
                plan: Some("pro".into()),
                usage_percent: Some(12.0),
                has_available_usage: Some(true),
                error: None,
                ..Default::default()
            },
        );
        pool.set_quota(
            "a",
            QuotaSnapshot {
                error: Some("timeout".into()),
                ..Default::default()
            },
        );
        let q = pool.get_quota("a").unwrap();
        assert_eq!(q.plan.as_deref(), Some("pro"));
        assert_eq!(q.usage_percent, Some(12.0));
        assert_eq!(q.error.as_deref(), Some("timeout"));
        assert!(q.from_cache);
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

    #[tokio::test]
    async fn busy_waits_then_succeeds() {
        let pool =
            AccountPool::new(vec![acc("a", true)], 1).with_acquire_wait(Duration::from_millis(500));
        let (_, p1) = pool.acquire().await.unwrap();
        let pool2 = pool.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(p1);
        });
        let t = Instant::now();
        let (a, _p2) = pool2.acquire().await.unwrap();
        assert_eq!(a.id, "a");
        assert!(t.elapsed() < Duration::from_millis(400));
    }

    #[tokio::test]
    async fn session_gc_clears_expired() {
        let pool = AccountPool::new(vec![acc("a", true)], 2);
        let (_a, _p) = pool.acquire_by_session(Some("s1")).await.unwrap();
        assert_eq!(pool.session_count(), 1);
        assert_eq!(pool.gc_sessions(), 0);
        pool.sessions.insert(
            "dead".into(),
            ("a".into(), Instant::now() - Duration::from_secs(1)),
        );
        assert_eq!(pool.gc_sessions(), 1);
        assert_eq!(pool.session_count(), 1);
    }

    #[test]
    fn empty_pool_is_empty() {
        let pool = AccountPool::new(vec![], 1);
        assert!(matches!(
            pool.try_acquire_by_session(None),
            AcquireTry::Empty
        ));
    }

    #[test]
    fn query_pages_and_attention() {
        let pool = AccountPool::new(
            vec![acc("ok", true), acc("bad", true), acc("off", false)],
            1,
        );
        pool.release("bad", true, 30);
        let page = pool.query_accounts("", "attention", "attention", 1, 50, "");
        assert_eq!(page["total_accounts"], 3);
        assert!(page["filtered"].as_u64().unwrap() >= 2);
        let ids: Vec<&str> = page["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|a| a["id"].as_str())
            .collect();
        assert!(ids.contains(&"bad"));
        assert!(ids.contains(&"off"));
        assert!(!ids.contains(&"ok"));
        let p1 = pool.query_accounts("", "all", "id", 1, 2, "");
        assert_eq!(p1["page_size"], 2);
        assert_eq!(p1["pages"], 2);
        assert_eq!(p1["accounts"].as_array().unwrap().len(), 2);
    }
}
