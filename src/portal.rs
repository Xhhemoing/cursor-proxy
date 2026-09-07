//! 用户门户: 用户账号 / 余额 / 购买套餐 / 邀请返佣.
//!
//! 与 card key 解耦: 用户 (PortalUser) 可持多卡, 余额购买套餐, admin 手动加余额.
//! 设计: docs/portal-design.md (2026-09-07 冻结).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

// ── 数据模型 ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortalUser {
    /// 用户编号 (如 "u1001")
    pub id: String,
    /// 登录名 (唯一, 不区分大小写)
    pub username: String,
    /// argon2 哈希
    pub password_hash: String,
    /// 余额 (元)
    #[serde(default)]
    pub balance_rmb: f64,
    /// 我的邀请码 (唯一, 8 位)
    #[serde(default)]
    pub aff_code: String,
    /// 谁邀请的我 (aff_code)
    #[serde(default)]
    pub invited_by: Option<String>,
    /// 所属用户组
    #[serde(default)]
    pub group_id: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default)]
    pub note: String,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserGroup {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub note: String,
    /// 该组可见的套餐 id 白名单 (空 = 全部可见)
    #[serde(default)]
    pub plan_whitelist: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceLogEntry {
    pub user_id: String,
    pub delta_rmb: f64,
    pub balance_after: f64,
    pub reason: String,
    pub ref_id: Option<String>,
    pub ts_ms: i64,
    pub operator: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PortalData {
    #[serde(default)]
    pub users: Vec<PortalUser>,
    #[serde(default)]
    pub groups: Vec<UserGroup>,
    #[serde(default)]
    pub next_user_seq: u32,
}

// ── PortalStore ──

pub struct PortalStore {
    data: arc_swap::ArcSwap<PortalData>,
    path: PathBuf,
    /// session token → (user_id, expire_unix)
    sessions: DashMap<String, (String, u64)>,
}

static PORTAL: std::sync::OnceLock<Arc<PortalStore>> = std::sync::OnceLock::new();

/// 初始化全局 PortalStore (main.rs 启动时调用, 数据目录与 cards.json 同级).
pub fn init(path: &std::path::Path) -> Arc<PortalStore> {
    let p = path.with_file_name("portal.json");
    PORTAL
        .get_or_init(|| Arc::new(PortalStore::open(&p)))
        .clone()
}

/// 全局 PortalStore (未 init 时用临时路径, 仅单测).
pub fn portal() -> Arc<PortalStore> {
    PORTAL
        .get_or_init(|| {
            let p = std::env::temp_dir().join(format!("portal-test-{}.json", std::process::id()));
            Arc::new(PortalStore::open(&p))
        })
        .clone()
}

const SESSION_TTL_SECS: u64 = 86400; // 24h

impl PortalStore {
    pub fn open(path: &PathBuf) -> Self {
        let data = std::fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        Self {
            data: arc_swap::ArcSwap::from_pointee(data),
            path: path.clone(),
            sessions: DashMap::new(),
        }
    }

    fn save(&self, d: PortalData) -> anyhow::Result<()> {
        let tmp = self.path.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_string_pretty(&d)?)?;
        std::fs::rename(&tmp, &self.path)?;
        self.data.store(Arc::new(d));
        Ok(())
    }

    pub fn snapshot(&self) -> Arc<PortalData> {
        self.data.load_full()
    }

    // ── 用户 ──

    pub fn get_user(&self, id_or_name: &str) -> Option<PortalUser> {
        let d = self.data.load();
        let lower = id_or_name.to_ascii_lowercase();
        d.users
            .iter()
            .find(|u| u.id == id_or_name || u.username.to_ascii_lowercase() == lower)
            .cloned()
    }

    pub fn list_users(&self) -> Vec<PortalUser> {
        self.data.load().users.clone()
    }

    pub fn create_user(
        &self,
        username: &str,
        password: &str,
        group_id: Option<String>,
        initial_balance: f64,
        invited_by: Option<String>,
    ) -> Result<PortalUser, String> {
        let username = username.trim().to_string();
        if username.len() < 2 || username.len() > 32 {
            return Err("用户名 2-32 字符".into());
        }
        if password.len() < 6 {
            return Err("密码至少 6 位".into());
        }
        let mut d = (*self.data.load_full()).clone();
        if d.users
            .iter()
            .any(|u| u.username.eq_ignore_ascii_case(&username))
        {
            return Err("用户名已存在".into());
        }
        d.next_user_seq += 1;
        let id = format!("u{}", 1000 + d.next_user_seq);
        let hash = hash_password(password)?;
        let aff_code = gen_aff_code(&d.users);
        let now = now_unix();
        let u = PortalUser {
            id: id.clone(),
            username,
            password_hash: hash,
            balance_rmb: initial_balance,
            aff_code,
            invited_by,
            group_id,
            enabled: true,
            created_at: now,
            note: String::new(),
        };
        d.users.push(u.clone());
        self.save(d).map_err(|e| e.to_string())?;
        Ok(u)
    }

    pub fn update_user(
        &self,
        id: &str,
        f: impl FnOnce(&mut PortalUser),
    ) -> Result<PortalUser, String> {
        let mut d = (*self.data.load_full()).clone();
        let u = d
            .users
            .iter_mut()
            .find(|u| u.id == id)
            .ok_or("用户不存在")?;
        f(u);
        let out = u.clone();
        self.save(d).map_err(|e| e.to_string())?;
        Ok(out)
    }

    pub fn verify_password(&self, id_or_name: &str, password: &str) -> Option<PortalUser> {
        let u = self.get_user(id_or_name)?;
        if !u.enabled {
            return None;
        }
        verify_password(&u.password_hash, password).then_some(u)
    }

    // ── session ──

    pub fn create_session(&self, user_id: &str) -> String {
        let token = gen_token();
        let exp = now_unix() + SESSION_TTL_SECS;
        self.sessions
            .insert(token.clone(), (user_id.to_string(), exp));
        token
    }

    pub fn resolve_session(&self, token: &str) -> Option<PortalUser> {
        let (uid, exp) = self.sessions.get(token)?.clone();
        if now_unix() > exp {
            self.sessions.remove(token);
            return None;
        }
        self.get_user(&uid)
    }

    pub fn destroy_session(&self, token: &str) {
        self.sessions.remove(token);
    }

    // ── 余额 ──

    pub fn add_balance(
        &self,
        user_id: &str,
        delta: f64,
        reason: &str,
        ref_id: Option<String>,
        operator: &str,
    ) -> Result<f64, String> {
        if !delta.is_finite() {
            return Err("金额非法".into());
        }
        let mut new_balance = 0.0;
        self.update_user(user_id, |u| {
            tracing::info!(event = "add_balance", user_id, before = u.balance_rmb, delta, "balance update");
            u.balance_rmb = ((u.balance_rmb + delta) * 100.0).round() / 100.0;
            new_balance = u.balance_rmb;
        })?;
        // 流水写 cards.db (portal_balance_log 表)
        if let Err(e) = self.log_balance(user_id, delta, new_balance, reason, ref_id, operator) {
            tracing::warn!(error = %e, "balance log write failed (non-fatal)");
        }
        Ok(new_balance)
    }

    fn log_balance(
        &self,
        user_id: &str,
        delta: f64,
        after: f64,
        reason: &str,
        ref_id: Option<String>,
        operator: &str,
    ) -> rusqlite::Result<()> {
        let path = self.path.with_file_name("cards.db");
        let conn = rusqlite::Connection::open(&path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS portal_balance_log (
                id INTEGER PRIMARY KEY,
                user_id TEXT NOT NULL,
                delta_rmb REAL NOT NULL,
                balance_after REAL NOT NULL,
                reason TEXT NOT NULL,
                ref_id TEXT,
                ts_ms INTEGER NOT NULL,
                operator TEXT NOT NULL
            )",
            [],
        )?;
        conn.execute(
            "INSERT INTO portal_balance_log (user_id, delta_rmb, balance_after, reason, ref_id, ts_ms, operator)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                user_id,
                delta,
                after,
                reason,
                ref_id,
                now_unix() as i64 * 1000,
                operator
            ],
        )?;
        Ok(())
    }

    pub fn balance_log(&self, user_id: &str, limit: usize) -> Vec<Value> {
        let path = self.path.with_file_name("cards.db");
        let Ok(conn) = rusqlite::Connection::open(&path) else {
            return vec![];
        };
        let mut out = vec![];
        if let Ok(mut st) = conn.prepare(
            "SELECT delta_rmb, balance_after, reason, ref_id, ts_ms, operator
             FROM portal_balance_log WHERE user_id = ?1 ORDER BY id DESC LIMIT ?2",
        ) {
            if let Ok(rows) = st.query_map(rusqlite::params![user_id, limit as i64], |r| {
                Ok(json!({
                    "delta_rmb": r.get::<_, f64>(0)?,
                    "balance_after": r.get::<_, f64>(1)?,
                    "reason": r.get::<_, String>(2)?,
                    "ref_id": r.get::<_, Option<String>>(3)?,
                    "ts_ms": r.get::<_, i64>(4)?,
                    "operator": r.get::<_, String>(5)?,
                }))
            }) {
                for r in rows.flatten() {
                    out.push(r);
                }
            }
        }
        out
    }

    // ── 用户组 ──

    pub fn list_groups(&self) -> Vec<UserGroup> {
        self.data.load().groups.clone()
    }

    pub fn get_group(&self, id: &str) -> Option<UserGroup> {
        self.data.load().groups.iter().find(|g| g.id == id).cloned()
    }

    pub fn upsert_group(&self, g: UserGroup) -> Result<(), String> {
        let mut d = (*self.data.load_full()).clone();
        if let Some(existing) = d.groups.iter_mut().find(|x| x.id == g.id) {
            *existing = g;
        } else {
            d.groups.push(g);
        }
        self.save(d).map_err(|e| e.to_string())
    }

    /// 用户可见的套餐 id 列表 (组白名单过滤; 无组或组白名单为空 = 全部)
    pub fn visible_plan_ids(&self, user: &PortalUser) -> Option<Vec<String>> {
        let gid = user.group_id.as_ref()?;
        let g = self.get_group(gid)?;
        if g.plan_whitelist.is_empty() {
            None // 全部可见
        } else {
            Some(g.plan_whitelist)
        }
    }
}

// ── 密码 / token ──

pub fn hash_password(pw: &str) -> Result<String, String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(pw.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| e.to_string())
}

pub fn verify_password(hash: &str, pw: &str) -> bool {
    PasswordHash::new(hash)
        .ok()
        .and_then(|h| Argon2::default().verify_password(pw.as_bytes(), &h).ok())
        .is_some()
}

pub fn gen_token() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: [u8; 32] = rng.gen();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn gen_aff_code(existing: &[PortalUser]) -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    loop {
        let code: String = (0..8)
            .map(|_| {
                let n = rng.gen_range(0..36);
                if n < 10 {
                    (b'0' + n) as char
                } else {
                    (b'a' + n - 10) as char
                }
            })
            .collect();
        if !existing.iter().any(|u| u.aff_code == code) {
            return code;
        }
    }
}

pub fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ── 用户 JSON 脱敏 (不暴露 password_hash) ──

pub fn user_json(u: &PortalUser) -> Value {
    json!({
        "id": u.id,
        "username": u.username,
        "balance_rmb": u.balance_rmb,
        "aff_code": u.aff_code,
        "invited_by": u.invited_by,
        "group_id": u.group_id,
        "enabled": u.enabled,
        "created_at": u.created_at,
        "note": u.note,
    })
}

/// 余额日志序列化占位 (BTreeMap 备用)
pub type _UnusedMap = BTreeMap<String, String>;
