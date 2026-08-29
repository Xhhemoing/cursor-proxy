//! 号池: 轮询 + 并发控制 + 失败冷却 + 动态开关 + 热编辑.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::Semaphore;

use crate::config::Account;
use crate::quota::QuotaSnapshot;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcquireError {
    Empty,
    Busy,
}

enum AcquireTry {
    Got((Account, tokio::sync::OwnedSemaphorePermit)),
    Empty,
    Busy,
}

struct Slot {
    account: Account,
    sem: Arc<Semaphore>,
    cooldown_until: Option<Instant>,
}

pub struct AccountPool {
    slots: Mutex<Vec<Slot>>,
    disabled: Arc<DashMap<String, bool>>,
    rr_index: AtomicUsize,
    stats: Arc<DashMap<String, AccountStats>>,
    quotas: Arc<DashMap<String, QuotaSnapshot>>,
    max_concurrency: usize,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct AccountStats {
    pub requests: u64,
    pub errors: u64,
}

impl AccountPool {
    pub fn new(accounts: Vec<Account>, max_concurrency: usize) -> Self {
        let max_concurrency = max_concurrency.max(1);
        let disabled = Arc::new(DashMap::new());
        let stats = Arc::new(DashMap::new());
        let quotas = Arc::new(DashMap::new());
        let slots = accounts
            .into_iter()
            .map(|acc| {
                stats.insert(acc.id.clone(), AccountStats::default());
                disabled.insert(acc.id.clone(), !acc.enabled);
                Slot {
                    account: acc,
                    sem: Arc::new(Semaphore::new(max_concurrency)),
                    cooldown_until: None,
                }
            })
            .collect();
        Self {
            slots: Mutex::new(slots),
            disabled,
            rr_index: AtomicUsize::new(0),
            stats,
            quotas,
            max_concurrency,
        }
    }

    pub fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }

    pub fn replace_accounts(&self, accounts: Vec<Account>) {
        let mut slots = self.slots.lock().unwrap();
        let old: Vec<Slot> = std::mem::take(&mut *slots);
        let mut by_id: std::collections::HashMap<String, Slot> =
            old.into_iter().map(|s| (s.account.id.clone(), s)).collect();
        let mut next = Vec::new();
        for acc in accounts {
            self.stats.entry(acc.id.clone()).or_default();
            self.disabled.entry(acc.id.clone()).or_insert(!acc.enabled);
            if let Some(mut existing) = by_id.remove(&acc.id) {
                existing.account = acc;
                next.push(existing);
            } else {
                next.push(Slot {
                    account: acc,
                    sem: Arc::new(Semaphore::new(self.max_concurrency)),
                    cooldown_until: None,
                });
            }
        }
        *slots = next;
    }

    pub fn get_account(&self, id: &str) -> Option<Account> {
        self.slots
            .lock()
            .unwrap()
            .iter()
            .find(|s| s.account.id == id)
            .map(|s| s.account.clone())
    }

    pub async fn acquire(&self) -> Result<(Account, tokio::sync::OwnedSemaphorePermit), AcquireError> {
        for _ in 0..40 {
            match self.try_acquire_now() {
                AcquireTry::Got(found) => return Ok(found),
                AcquireTry::Empty => return Err(AcquireError::Empty),
                AcquireTry::Busy => {
                    let waiter = {
                        let slots = self.slots.lock().unwrap();
                        slots
                            .iter()
                            .find(|s| self.is_enabled(&s.account.id) && !self.quota_blocks(&s.account.id))
                            .map(|s| s.sem.clone())
                    };
                    match waiter {
                        Some(sem) => {
                            let _ = tokio::time::timeout(Duration::from_millis(50), sem.acquire()).await;
                        }
                        None => return Err(AcquireError::Empty),
                    }
                }
            }
        }
        Err(AcquireError::Busy)
    }

    fn quota_blocks(&self, account_id: &str) -> bool {
        let q = self.quota(account_id);
        if q.has_available_usage == Some(false) {
            return true;
        }
        if let Some(p) = q.usage_percent {
            if p >= 99.5 {
                return true;
            }
        }
        false
    }

    fn try_acquire_now(&self) -> AcquireTry {
        let now = Instant::now();
        let slots = self.slots.lock().unwrap();
        let n = slots.len();
        if n == 0 {
            return AcquireTry::Empty;
        }
        let start = self.rr_index.fetch_add(1, Ordering::Relaxed);

        // 收集所有 eligible 账号，按权重排序（剩余配额比例 × (1 - 错误率)）
        let mut candidates: Vec<(usize, f64)> = Vec::new();
        for i in 0..n {
            let idx = (start + i) % n;
            let acc_id = slots[idx].account.id.clone();
            if self.disabled.get(&acc_id).map(|v| *v).unwrap_or(false) {
                continue;
            }
            if self.quota_blocks(&acc_id) {
                continue;
            }
            if slots[idx]
                .cooldown_until
                .map(|until| until > now)
                .unwrap_or(false)
            {
                continue;
            }

            // 计算权重：剩余配额比例 × (1 - 错误率)
            let quota_weight = match self.quota(&acc_id).usage_percent {
                Some(p) => (100.0 - p).max(1.0) / 100.0, // 剩余比例，至少 1%
                None => 0.5, // 未知配额给中等权重
            };
            let error_rate = match self.stats.get(&acc_id) {
                Some(s) if s.requests > 0 => s.errors as f64 / s.requests as f64,
                _ => 0.0,
            };
            let weight = quota_weight * (1.0 - error_rate.min(0.9)); // 错误率上限 90%

            candidates.push((idx, weight));
        }

        if candidates.is_empty() {
            return AcquireTry::Empty;
        }

        // 按权重降序排序，优先选权重高的
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // 尝试按权重顺序获取信号量
        for (idx, _weight) in &candidates {
            let idx = *idx;
            let acc_id = slots[idx].account.id.clone();
            if let Ok(permit) = slots[idx].sem.clone().try_acquire_owned() {
                if let Some(mut s) = self.stats.get_mut(&acc_id) {
                    s.requests += 1;
                }
                return AcquireTry::Got((slots[idx].account.clone(), permit));
            }
        }

        AcquireTry::Busy
    }

    pub fn release(&self, account_id: &str, error: bool, cooldown_s: u64) {
        if let Some(mut s) = self.stats.get_mut(account_id) {
            if error {
                s.errors += 1;
            }
        }
        if error && cooldown_s > 0 {
            // 动态冷却：根据错误率调整冷却时间，错误率越高冷却越长
            let dynamic_cooldown = match self.stats.get(account_id) {
                Some(s) if s.requests > 0 => {
                    let error_rate = s.errors as f64 / s.requests as f64;
                    let base = cooldown_s as f64;
                    (base * (1.0 + error_rate * 2.0)).min(300.0) as u64 // 最高 5 分钟
                }
                _ => cooldown_s,
            };
            let mut slots = self.slots.lock().unwrap();
            if let Some(slot) = slots.iter_mut().find(|s| s.account.id == account_id) {
                slot.cooldown_until = Some(Instant::now() + Duration::from_secs(dynamic_cooldown));
            }
        }
    }

    pub fn has_account(&self, account_id: &str) -> bool {
        self.slots
            .lock()
            .unwrap()
            .iter()
            .any(|s| s.account.id == account_id)
    }

    pub fn toggle_account(&self, account_id: &str) -> Option<bool> {
        let mut disabled = self.disabled.get_mut(account_id)?;
        *disabled = !*disabled;
        Some(!*disabled)
    }

    pub fn set_enabled(&self, account_id: &str, enabled: bool) -> Option<bool> {
        let mut disabled = self.disabled.get_mut(account_id)?;
        *disabled = !enabled;
        Some(enabled)
    }

    pub fn clear_cooldown(&self, account_id: &str) -> bool {
        let mut slots = self.slots.lock().unwrap();
        if let Some(slot) = slots.iter_mut().find(|s| s.account.id == account_id) {
            slot.cooldown_until = None;
            true
        } else {
            false
        }
    }

    pub fn is_enabled(&self, account_id: &str) -> bool {
        !self.disabled.get(account_id).map(|v| *v).unwrap_or(false)
    }

    pub fn set_quota(&self, account_id: &str, incoming: QuotaSnapshot) {
        let mut entry = self
            .quotas
            .entry(account_id.to_string())
            .or_insert_with(QuotaSnapshot::default);
        entry.merge_keep_last_good(incoming);
    }

    pub fn quota(&self, account_id: &str) -> QuotaSnapshot {
        self.quotas
            .get(account_id)
            .map(|v| v.clone())
            .unwrap_or_default()
    }

    fn cooldown_remaining_s(until: Option<Instant>) -> u64 {
        until
            .and_then(|t| t.checked_duration_since(Instant::now()))
            .map(|d| d.as_secs().max(1))
            .unwrap_or(0)
    }

    pub fn account_rows(&self) -> Vec<serde_json::Value> {
        let slots = self.slots.lock().unwrap();
        slots
            .iter()
            .map(|slot| {
                let acc = &slot.account;
                let s = self.stats.get(&acc.id).map(|s| s.clone()).unwrap_or_default();
                let remaining = Self::cooldown_remaining_s(slot.cooldown_until);
                let inflight = self
                    .max_concurrency
                    .saturating_sub(slot.sem.available_permits());
                let q = self.quota(&acc.id);
                serde_json::json!({
                    "id": acc.id,
                    "machine_suffix": acc.machine_id.chars().rev().take(8).collect::<String>().chars().rev().collect::<String>(),
                    "requests": s.requests,
                    "errors": s.errors,
                    "cooldown": remaining > 0,
                    "cooldown_remaining_s": remaining,
                    "enabled": self.is_enabled(&acc.id),
                    "inflight": inflight,
                    "max_concurrency": self.max_concurrency,
                    "quota": q,
                })
            })
            .collect()
    }

    pub fn stats(&self) -> serde_json::Value {
        let rows = self.account_rows();
        let available = rows
            .iter()
            .filter(|r| r["enabled"].as_bool() == Some(true) && r["cooldown"].as_bool() != Some(true))
            .count();
        let inflight: usize = rows
            .iter()
            .map(|r| r["inflight"].as_u64().unwrap_or(0) as usize)
            .sum();
        serde_json::json!({
            "total_accounts": rows.len(),
            "available": available,
            "inflight": inflight,
            "max_concurrency_per_account": self.max_concurrency,
            "accounts": rows,
        })
    }
}

impl Clone for AccountPool {
    fn clone(&self) -> Self {
        Self {
            slots: Mutex::new({
                let slots = self.slots.lock().unwrap();
                slots
                    .iter()
                    .map(|s| Slot {
                        account: s.account.clone(),
                        sem: s.sem.clone(),
                        cooldown_until: s.cooldown_until,
                    })
                    .collect()
            }),
            disabled: self.disabled.clone(),
            rr_index: AtomicUsize::new(self.rr_index.load(Ordering::Relaxed)),
            stats: self.stats.clone(),
            quotas: self.quotas.clone(),
            max_concurrency: self.max_concurrency,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acc(id: &str, enabled: bool) -> Account {
        Account {
            id: id.into(),
            access_token: "tok".into(),
            machine_id: "machine-abcdef12".into(),
            refresh_token: String::new(),
            enabled,
            token_expires_at: None,
            refresh_url: None,
        }
    }

    #[test]
    fn stats_are_array_and_toggle_flips() {
        let pool = AccountPool::new(vec![acc("acc1", true), acc("acc2", false)], 4);
        let stats = pool.stats();
        assert_eq!(stats["total_accounts"], 2);
        assert_eq!(stats["available"], 1);
        let rows = stats["accounts"].as_array().unwrap();
        assert_eq!(rows[0]["id"], "acc1");
        assert_eq!(rows[0]["enabled"], true);
        assert_eq!(rows[1]["enabled"], false);
        assert_eq!(pool.toggle_account("acc1"), Some(false));
        assert!(!pool.is_enabled("acc1"));
        assert_eq!(pool.toggle_account("missing"), None);
    }

    #[test]
    fn replace_accounts_keeps_stats() {
        let pool = AccountPool::new(vec![acc("acc1", true)], 1);
        pool.release("acc1", true, 0);
        pool.replace_accounts(vec![acc("acc1", true), acc("acc3", true)]);
        let stats = pool.stats();
        assert_eq!(stats["total_accounts"], 2);
        assert_eq!(stats["accounts"][0]["errors"], 1);
        assert!(pool.has_account("acc3"));
    }

    #[test]
    fn exhausted_quota_skips_account() {
        let pool = AccountPool::new(vec![acc("acc1", true), acc("acc2", true)], 1);
        pool.set_quota(
            "acc1",
            crate::quota::QuotaSnapshot {
                has_available_usage: Some(false),
                usage_percent: Some(100.0),
                ..Default::default()
            },
        );
        match pool.try_acquire_now() {
            AcquireTry::Got((a, _)) => assert_eq!(a.id, "acc2"),
            other => panic!("expected acc2, got {:?}", std::mem::discriminant(&other)),
        }
    }

    #[test]
    fn empty_pool_is_empty() {
        let pool = AccountPool::new(vec![], 1);
        assert!(matches!(pool.try_acquire_now(), AcquireTry::Empty));
    }

    #[test]
    fn clear_cooldown_unblocks() {
        let pool = AccountPool::new(vec![acc("acc1", true)], 1);
        pool.release("acc1", true, 30);
        let stats = pool.stats();
        assert_eq!(stats["accounts"][0]["cooldown"], true);
        assert!(pool.clear_cooldown("acc1"));
        let stats = pool.stats();
        assert_eq!(stats["accounts"][0]["cooldown"], false);
    }
}
