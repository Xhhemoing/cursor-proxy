//! 计费账本: 官方口径面值快照 + 整数金额 + SQLite 单写线程.
//!
//! 零售版口径 (2026-09-05 起): 账本记的 `cost_nano` 是 **官方面值 (美元, nano-$)**, 单价来自
//! `cards::model_price` (注册表手动定价 > 内置官方表), 与套餐卡结算同一张价格表 —— 不再有
//! 「客户价 / 销售分成」第二套价格. 收入侧只有卡的 `paid_rmb`, 成本侧只有 面值 × CostModel.
//!
//! 精度与一致性保证:
//! - 金额一律用整数 nano (1e-9 $) 存储与累加, 不经过浮点.
//!   价格按 "每 1M tokens 的 micro (1e-6 $)" 存, `tokens × price_micro` 得到 pico 的精确值,
//!   再半入舍到 nano: 单条误差 ≤ 0.5 nano, 可忽略且确定性可复算.
//! - 每条记录快照当时的单价, 事后改价不影响历史账单.
//! - `req_id` UNIQUE + INSERT OR IGNORE: 同一请求绝不会记两次.
//! - 写入走专用线程 + 事务批量提交 (WAL), 请求线程只做一次 channel send, 高并发下没有锁竞争.
//! - 查询用独立只读连接跑在 spawn_blocking, 不影响写入与请求.
//!
//! 表结构保留了旧版 `sales_id` / `commission_*` 列 (恒 NULL/0), 老库无需迁移.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use rusqlite::{params, Connection, OpenFlags};
use serde::Serialize;
use serde_json::{json, Value};

use crate::config::ApiKeyRecord;
pub use crate::translate::Usage;

pub const NANO_PER_UNIT: i64 = 1_000_000_000;

// ---------------------------------------------------------------------------
// 价格快照 (官方口径)
// ---------------------------------------------------------------------------

/// 请求时快照下来的官方单价 (micro-$ / 1M tokens)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceQuote {
    pub input_micro: u64,
    pub output_micro: u64,
    pub cache_read_micro: u64,
    pub cache_write_micro: u64,
    /// false = 模型不在任何价格表里, 用的是兜底价 (面板「消耗分析」会标「估算」)
    pub priced: bool,
}

/// $/1M (f64) → micro 整数; 非法/负数 → 0
pub fn per_m_to_micro(v: f64) -> u64 {
    if !v.is_finite() || v <= 0.0 {
        return 0;
    }
    (v * 1_000_000.0).round() as u64
}

/// 官方口径报价: 注册表手动定价 > 内置官方表 > 兜底 (priced=false)
pub fn quote(model: &str) -> PriceQuote {
    let (i, o, c, w) = crate::cards::model_price(model);
    PriceQuote {
        input_micro: per_m_to_micro(i),
        output_micro: per_m_to_micro(o),
        cache_read_micro: per_m_to_micro(c),
        cache_write_micro: per_m_to_micro(w),
        priced: crate::cards::model_price_known(model),
    }
}

/// 成本 (nano-$). tokens × 每 1M 单价 (micro) = pico, 半入舍到 nano. 全程整数.
#[inline]
pub fn cost_nano(u: &Usage, q: &PriceQuote) -> i64 {
    let pico = (u.input as u128) * (q.input_micro as u128)
        + (u.output as u128) * (q.output_micro as u128)
        + (u.cache_read as u128) * (q.cache_read_micro as u128)
        + (u.cache_write as u128) * (q.cache_write_micro as u128);
    ((pico + 500) / 1000).min(i64::MAX as u128) as i64
}

/// nano → 小数字符串 (9 位小数, 去尾零但至少 2 位)
pub fn fmt_money(nano: i64) -> String {
    let neg = nano < 0;
    let n = nano.unsigned_abs();
    let int = n / NANO_PER_UNIT as u64;
    let frac = n % NANO_PER_UNIT as u64;
    let mut frac_s = format!("{:09}", frac);
    while frac_s.len() > 2 && frac_s.ends_with('0') {
        frac_s.pop();
    }
    format!("{}{}.{}", if neg { "-" } else { "" }, int, frac_s)
}

/// key 的稳定标识: 16 位 hash. 用于筛选与分组 (前缀可能撞, hash 不会)
pub fn key_hash(key: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    format!("{:016x}", h.finish())
}

/// 请求开始时快照的计费上下文, 贯穿整个请求
#[derive(Debug, Clone)]
pub struct BillingCtx {
    pub key_hash: String,
    pub key_prefix: String,
    pub key_name: String,
    pub tags: Vec<String>,
    pub quote: PriceQuote,
}

impl BillingCtx {
    pub fn from_key(rec: Option<&ApiKeyRecord>, model: &str) -> Self {
        let (key_hash, key_prefix, key_name, tags) = match rec {
            Some(r) => (
                key_hash(&r.key),
                r.key.chars().take(8).collect(),
                r.name.clone(),
                r.tags.clone(),
            ),
            None => (
                "anonymous".into(),
                "-".into(),
                "(no auth)".into(),
                Vec::new(),
            ),
        };
        Self {
            key_hash,
            key_prefix,
            key_name,
            tags,
            quote: quote(model),
        }
    }
}

// ---------------------------------------------------------------------------
// 账本记录
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// 账本记录
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct BillingRecord {
    pub ts_ms: i64,
    pub req_id: String,
    pub key_hash: String,
    pub key_prefix: String,
    pub key_name: String,
    pub model: String,
    pub account: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
    pub input_price_micro: u64,
    pub output_price_micro: u64,
    pub cache_read_price_micro: u64,
    pub cache_write_price_micro: u64,
    pub priced: bool,
    /// 官方面值 (nano-$)
    pub cost_nano: i64,
    pub stream: bool,
    pub status: u16,
    pub latency_ms: u64,
    /// 首个内容帧到达耗时 (ms). 流式 = 客户端看到第一个字的等待; 非流式 = 整段延迟; 失败/无内容 = None
    pub ttft_ms: Option<u64>,
    pub client_ip: String,
    pub tags: Vec<String>,
}

impl BillingRecord {
    /// 面板「请求日志」条目 (与旧 log_entry 字段兼容, 多了 key/费用/失败状态)
    pub fn to_log_entry(&self) -> serde_json::Value {
        let is_card = self.key_name.starts_with("card-");
        serde_json::json!({
            "ts": chrono::DateTime::from_timestamp_millis(self.ts_ms)
                .map(|d| d.to_rfc3339())
                .unwrap_or_default(),
            "req_id": self.req_id,
            "model": self.model,
            "account": self.account,
            "key_name": self.key_name,
            "key_prefix": self.key_prefix,
            "kind": if is_card { "card" } else if self.key_name == "(no auth)" { "anon" } else { "key" },
            "tags": self.tags,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cache_read_tokens": self.cache_read_tokens,
            "cache_write_tokens": self.cache_write_tokens,
            "total_tokens": self.input_tokens + self.output_tokens + self.cache_read_tokens + self.cache_write_tokens,
            "cost": fmt_money(self.cost_nano),
            "cost_nano": self.cost_nano,
            "priced": self.priced,
            "latency_ms": self.latency_ms,
            "ttft_ms": self.ttft_ms,
            "output_tps": self.output_tps(),
            "status": self.status,
            "ok": self.status == 200,
            "stream": self.stream,
            "client_ip": self.client_ip,
        })
    }

    /// 输出速度 tok/s: 输出 token ÷ (总延迟 − 首字延迟). 无输出或时长为 0 → None
    pub fn output_tps(&self) -> Option<f64> {
        let gen_ms = self.latency_ms.saturating_sub(self.ttft_ms.unwrap_or(0));
        if self.output_tokens == 0 || gen_ms == 0 {
            None
        } else {
            Some(self.output_tokens as f64 / (gen_ms as f64 / 1000.0))
        }
    }

    /// 补首字延迟 (build 之后链式调用)
    pub fn with_ttft(mut self, ttft_ms: Option<u64>) -> Self {
        self.ttft_ms = ttft_ms;
        self
    }

    /// 由上下文 + 结果构造一条账单, 金额在此处一次算定
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        ctx: &BillingCtx,
        req_id: &str,
        model: &str,
        account: &str,
        usage: Usage,
        stream: bool,
        status: u16,
        latency_ms: u64,
        client_ip: &str,
    ) -> Self {
        // 面值按上游实际上报/本地估算的 token 结算; 有 usage 就计 (中断流也烧额度),
        // 无 usage 的失败请求 (503/502 无 token) 自然为 0.
        let cost = cost_nano(&usage, &ctx.quote);
        Self {
            ts_ms: now_ms(),
            req_id: req_id.to_string(),
            key_hash: ctx.key_hash.clone(),
            key_prefix: ctx.key_prefix.clone(),
            key_name: ctx.key_name.clone(),
            model: model.to_string(),
            account: account.to_string(),
            input_tokens: usage.input,
            output_tokens: usage.output,
            cache_read_tokens: usage.cache_read,
            cache_write_tokens: usage.cache_write,
            input_price_micro: ctx.quote.input_micro,
            output_price_micro: ctx.quote.output_micro,
            cache_read_price_micro: ctx.quote.cache_read_micro,
            cache_write_price_micro: ctx.quote.cache_write_micro,
            priced: ctx.quote.priced,
            cost_nano: cost,
            stream,
            status,
            latency_ms,
            ttft_ms: None,
            client_ip: client_ip.to_string(),
            tags: ctx.tags.clone(),
        }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// 写入端
// ---------------------------------------------------------------------------

enum Msg {
    Record(Box<BillingRecord>),
    Flush(mpsc::SyncSender<()>),
    Checkpoint,
}

pub struct Ledger {
    tx: mpsc::Sender<Msg>,
    db_path: PathBuf,
    /// 请求日志 ring buffer 镜像: 每条账单 (含失败) 同步进面板「请求日志」, 带 key/卡归属
    log_mirror: Option<Arc<crate::logbuf::LogBuffer>>,
    /// 已入队未落盘
    pending: Arc<AtomicU64>,
    /// 累计落盘
    written: Arc<AtomicU64>,
    /// 因 UNIQUE 冲突被忽略 (重复 req_id)
    ignored: Arc<AtomicU64>,
    /// 写失败
    failed: Arc<AtomicU64>,
}

const SCHEMA: &str = r#"
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA wal_autocheckpoint = 1000;
PRAGMA busy_timeout = 5000;
CREATE TABLE IF NOT EXISTS billing_records (
    id INTEGER PRIMARY KEY,
    ts_ms INTEGER NOT NULL,
    req_id TEXT NOT NULL UNIQUE,
    key_hash TEXT NOT NULL,
    key_prefix TEXT NOT NULL,
    key_name TEXT NOT NULL,
    sales_id TEXT,
    commission_bps INTEGER NOT NULL,
    model TEXT NOT NULL,
    account TEXT NOT NULL,
    input_tokens INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    input_price_micro INTEGER NOT NULL,
    output_price_micro INTEGER NOT NULL,
    priced INTEGER NOT NULL,
    cost_nano INTEGER NOT NULL,
    commission_nano INTEGER NOT NULL,
    stream INTEGER NOT NULL,
    status INTEGER NOT NULL,
    latency_ms INTEGER NOT NULL,
    client_ip TEXT NOT NULL,
    tags TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_br_ts ON billing_records(ts_ms);
CREATE INDEX IF NOT EXISTS idx_br_key_ts ON billing_records(key_hash, ts_ms);
CREATE INDEX IF NOT EXISTS idx_br_sales_ts ON billing_records(sales_id, ts_ms);
CREATE INDEX IF NOT EXISTS idx_br_model_ts ON billing_records(model, ts_ms);
CREATE TABLE IF NOT EXISTS billing_tags (
    record_id INTEGER NOT NULL,
    tag TEXT NOT NULL,
    PRIMARY KEY (record_id, tag)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_bt_tag ON billing_tags(tag, record_id);
"#;

impl Ledger {
    pub fn open(db_path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db_path = db_path.as_ref().to_path_buf();
        let conn = Connection::open(&db_path)?;
        conn.execute_batch(SCHEMA)?;
        migrate(&conn)?;
        let (tx, rx) = mpsc::channel::<Msg>();
        let pending = Arc::new(AtomicU64::new(0));
        let written = Arc::new(AtomicU64::new(0));
        let ignored = Arc::new(AtomicU64::new(0));
        let failed = Arc::new(AtomicU64::new(0));
        {
            let (pending, written, ignored, failed) = (
                pending.clone(),
                written.clone(),
                ignored.clone(),
                failed.clone(),
            );
            std::thread::Builder::new()
                .name("billing-writer".into())
                .spawn(move || writer_loop(conn, rx, pending, written, ignored, failed))?;
        }
        Ok(Self {
            tx,
            db_path,
            log_mirror: None,
            pending,
            written,
            ignored,
            failed,
        })
    }

    /// 绑定请求日志 ring buffer: 之后每条 record() 都镜像一份进面板日志
    pub fn with_log_mirror(mut self, buf: Arc<crate::logbuf::LogBuffer>) -> Self {
        self.log_mirror = Some(buf);
        self
    }

    /// 请求路径: 一次 send, 不阻塞
    pub fn record(&self, rec: BillingRecord) {
        if let Some(buf) = &self.log_mirror {
            buf.push(rec.to_log_entry());
        }
        self.pending.fetch_add(1, Ordering::Relaxed);
        if self.tx.send(Msg::Record(Box::new(rec))).is_err() {
            self.pending.fetch_sub(1, Ordering::Relaxed);
            self.failed.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                event = "billing_writer_dead",
                "billing writer thread gone; record lost"
            );
        }
    }

    /// 等待队列全部落盘 (关机 / 测试用)
    pub fn flush(&self, timeout: Duration) -> bool {
        let (ack_tx, ack_rx) = mpsc::sync_channel(1);
        if self.tx.send(Msg::Flush(ack_tx)).is_err() {
            return false;
        }
        ack_rx.recv_timeout(timeout).is_ok()
    }

    /// 请求 WAL 截断检查点; 写线程处理, 不阻塞请求路径.
    pub fn request_checkpoint(&self) {
        if self.tx.send(Msg::Checkpoint).is_err() {
            tracing::warn!(
                event = "billing_checkpoint",
                "writer gone, skip wal checkpoint"
            );
        }
    }

    pub fn stats(&self) -> Value {
        let size = std::fs::metadata(&self.db_path)
            .map(|m| m.len())
            .unwrap_or(0);
        json!({
            "db_file": self.db_path.display().to_string(),
            "db_bytes": size,
            "pending": self.pending.load(Ordering::Relaxed),
            "written": self.written.load(Ordering::Relaxed),
            "ignored_duplicates": self.ignored.load(Ordering::Relaxed),
            "failed": self.failed.load(Ordering::Relaxed),
        })
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// 只读连接 (查询用, 每次查询新开; SQLite 打开开销极小)
    pub fn reader(&self) -> rusqlite::Result<Connection> {
        let conn = Connection::open_with_flags(
            &self.db_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(Duration::from_secs(5))?;
        Ok(conn)
    }
}

/// 增量迁移: 老库补缓存列 (SQLite ALTER TABLE ADD COLUMN 带默认值, 对既有行安全)
fn migrate(conn: &Connection) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(billing_records)")?;
    let cols: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<rusqlite::Result<_>>()?;
    for (name, ddl) in [
        ("cache_read_tokens", "ALTER TABLE billing_records ADD COLUMN cache_read_tokens INTEGER NOT NULL DEFAULT 0"),
        ("cache_write_tokens", "ALTER TABLE billing_records ADD COLUMN cache_write_tokens INTEGER NOT NULL DEFAULT 0"),
        ("cache_read_price_micro", "ALTER TABLE billing_records ADD COLUMN cache_read_price_micro INTEGER NOT NULL DEFAULT 0"),
        ("cache_write_price_micro", "ALTER TABLE billing_records ADD COLUMN cache_write_price_micro INTEGER NOT NULL DEFAULT 0"),
        // 2026-09-05 之前 input_tokens 记的是 Cursor promptTokens (含 cacheRead+cacheWrite);
        // 之后 translate::extract_usage 已扣缓存. 老行默认 1 (含), 新行写 0 → 分析端按此归一化.
        ("input_incl_cache", "ALTER TABLE billing_records ADD COLUMN input_incl_cache INTEGER NOT NULL DEFAULT 1"),
        // 首字延迟 (ms), 老行 NULL
        ("ttft_ms", "ALTER TABLE billing_records ADD COLUMN ttft_ms INTEGER"),
    ] {
        if !cols.iter().any(|c| c == name) {
            conn.execute_batch(ddl)?;
            tracing::info!(event = "billing_migrate", column = name, "added column");
        }
    }
    // 消耗分析按 key_name (卡号/key 名) 分组 + 时间窗查询; 老库补索引 (幂等)
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_br_keyname_ts ON billing_records(key_name, ts_ms);",
    )?;
    Ok(())
}

fn writer_loop(
    mut conn: Connection,
    rx: mpsc::Receiver<Msg>,
    pending: Arc<AtomicU64>,
    written: Arc<AtomicU64>,
    ignored: Arc<AtomicU64>,
    failed: Arc<AtomicU64>,
) {
    const BATCH: usize = 500;
    let mut batch: Vec<BillingRecord> = Vec::with_capacity(BATCH);
    let mut flush_acks: Vec<mpsc::SyncSender<()>> = Vec::new();
    loop {
        // 阻塞等第一条
        let first = match rx.recv() {
            Ok(m) => m,
            Err(_) => break,
        };
        let mut disconnected = false;
        let mut checkpoint = false;
        match first {
            Msg::Record(r) => batch.push(*r),
            Msg::Flush(a) => flush_acks.push(a),
            Msg::Checkpoint => checkpoint = true,
        }
        // 再尽量多收一点, 最多等 20ms, 攒一批一个事务
        let deadline = std::time::Instant::now() + Duration::from_millis(20);
        while batch.len() < BATCH {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            if left.is_zero() {
                break;
            }
            match rx.recv_timeout(left) {
                Ok(Msg::Record(r)) => batch.push(*r),
                Ok(Msg::Flush(a)) => flush_acks.push(a),
                Ok(Msg::Checkpoint) => checkpoint = true,
                Err(mpsc::RecvTimeoutError::Timeout) => break,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }
        if !batch.is_empty() {
            let n = batch.len() as u64;
            match write_batch(&mut conn, &batch) {
                Ok(dups) => {
                    written.fetch_add(n - dups, Ordering::Relaxed);
                    ignored.fetch_add(dups, Ordering::Relaxed);
                }
                Err(e) => {
                    failed.fetch_add(n, Ordering::Relaxed);
                    tracing::error!(event = "billing_write_failed", error = %e, count = n, "billing batch write failed");
                }
            }
            pending.fetch_sub(n, Ordering::Relaxed);
            batch.clear();
        }
        if checkpoint {
            if let Err(e) = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);") {
                tracing::warn!(event = "billing_wal_checkpoint", error = %e, "wal checkpoint failed");
            } else {
                tracing::info!(event = "billing_wal_checkpoint", "wal truncated");
            }
        }
        for a in flush_acks.drain(..) {
            let _ = a.send(());
        }
        if disconnected {
            break;
        }
    }
}

/// 一个事务写一批; 返回被 UNIQUE 忽略的条数
fn write_batch(conn: &mut Connection, batch: &[BillingRecord]) -> rusqlite::Result<u64> {
    let tx = conn.transaction()?;
    let mut dups = 0u64;
    {
        let mut ins = tx.prepare_cached(
            "INSERT OR IGNORE INTO billing_records (
                ts_ms, req_id, key_hash, key_prefix, key_name, sales_id, commission_bps,
                model, account, input_tokens, output_tokens, input_price_micro, output_price_micro,
                priced, cost_nano, commission_nano, stream, status, latency_ms, client_ip, tags,
                cache_read_tokens, cache_write_tokens, cache_read_price_micro, cache_write_price_micro,
                input_incl_cache, ttft_ms
            ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,0,?26)",
        )?;
        let mut ins_tag = tx.prepare_cached(
            "INSERT OR IGNORE INTO billing_tags (record_id, tag) VALUES (?1, ?2)",
        )?;
        for r in batch {
            let tags_json = serde_json::to_string(&r.tags).unwrap_or_else(|_| "[]".into());
            let n = ins.execute(params![
                r.ts_ms,
                r.req_id,
                r.key_hash,
                r.key_prefix,
                r.key_name,
                Option::<String>::None, // sales_id (已废弃, 保留列)
                0i64,                   // commission_bps (已废弃)
                r.model,
                r.account,
                r.input_tokens as i64,
                r.output_tokens as i64,
                r.input_price_micro as i64,
                r.output_price_micro as i64,
                r.priced as i64,
                r.cost_nano,
                0i64, // commission_nano (已废弃)
                r.stream as i64,
                r.status as i64,
                r.latency_ms as i64,
                r.client_ip,
                tags_json,
                r.cache_read_tokens as i64,
                r.cache_write_tokens as i64,
                r.cache_read_price_micro as i64,
                r.cache_write_price_micro as i64,
                r.ttft_ms.map(|v| v as i64),
            ])?;
            if n == 0 {
                dups += 1;
                continue;
            }
            let id = tx.last_insert_rowid();
            for t in &r.tags {
                ins_tag.execute(params![id, t])?;
            }
        }
    }
    tx.commit()?;
    Ok(dups)
}

// ---------------------------------------------------------------------------
// 时间工具 (消耗分析 / 利润报表共用)
// ---------------------------------------------------------------------------

/// 时间解析: unix 秒 / unix 毫秒 / `YYYY-MM-DD` / `YYYY-MM-DD HH` / `YYYY-MM-DDTHH:MM[:SS]`
/// 日期字符串按 tz_offset 解释. 返回毫秒.
pub fn parse_time(s: &str, tz_offset_minutes: i32, end_of_unit: bool) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(n) = s.parse::<i64>() {
        // 13 位以上当毫秒
        return Some(if n > 100_000_000_000 { n } else { n * 1000 });
    }
    use chrono::{NaiveDate, NaiveDateTime};
    let off = chrono::FixedOffset::east_opt(tz_offset_minutes * 60)?;
    let norm = s.replace('T', " ");
    // "YYYY-MM-DD HH" (只有小时): chrono 不接受缺分钟, 手动补 ":00"
    let hour_only = norm.len() == 13
        && norm.as_bytes()[10] == b' '
        && norm[11..].chars().all(|c| c.is_ascii_digit());
    let (naive, unit_secs): (NaiveDateTime, i64) =
        if let Ok(d) = NaiveDate::parse_from_str(&norm, "%Y-%m-%d") {
            (d.and_hms_opt(0, 0, 0)?, 86400)
        } else if hour_only {
            (
                NaiveDateTime::parse_from_str(&format!("{norm}:00"), "%Y-%m-%d %H:%M").ok()?,
                3600,
            )
        } else if let Ok(dt) = NaiveDateTime::parse_from_str(&norm, "%Y-%m-%d %H:%M") {
            (dt, 60)
        } else if let Ok(dt) = NaiveDateTime::parse_from_str(&norm, "%Y-%m-%d %H:%M:%S") {
            (dt, 1)
        } else {
            return None;
        };
    let ts = naive.and_local_timezone(off).single()?.timestamp();
    let ts = if end_of_unit { ts + unit_secs } else { ts };
    Some(ts * 1000)
}

pub fn fmt_local(ts_ms: i64, tz_offset_minutes: i32) -> String {
    use chrono::TimeZone;
    let off = chrono::FixedOffset::east_opt(tz_offset_minutes * 60)
        .unwrap_or_else(|| chrono::FixedOffset::east_opt(0).unwrap());
    off.timestamp_millis_opt(ts_ms)
        .single()
        .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn u(i: u64, o: u64) -> Usage {
        Usage {
            input: i,
            output: o,
            cache_read: 0,
            cache_write: 0,
        }
    }

    #[test]
    fn money_is_exact_integer_math() {
        // $3 / 1M input, $15 / 1M output
        let q = PriceQuote {
            input_micro: 3_000_000,
            output_micro: 15_000_000,
            cache_read_micro: 300_000,
            cache_write_micro: 3_750_000,
            priced: true,
        };
        // 1234 in + 567 out → 1234*3 + 567*15 = 3702 + 8505 = 12207 micro = 0.012207
        let c = cost_nano(&u(1234, 567), &q);
        let cc = cost_nano(
            &Usage {
                input: 0,
                output: 0,
                cache_read: 1000,
                cache_write: 200,
            },
            &q,
        );
        assert_eq!(cc, 1_050_000);
        assert_eq!(c, 12_207_000);
        assert_eq!(fmt_money(c), "0.012207");
        assert_eq!(per_m_to_micro(0.000001), 1);
        assert_eq!(per_m_to_micro(2.5), 2_500_000);
        assert_eq!(per_m_to_micro(-1.0), 0);
        assert_eq!(fmt_money(0), "0.00");
        assert_eq!(fmt_money(1_000_000_000), "1.00");
        assert_eq!(fmt_money(-1_500_000_000), "-1.50");
        let big = cost_nano(&u(u64::MAX / 4, u64::MAX / 4), &q);
        assert_eq!(big, i64::MAX);
    }

    #[test]
    fn quote_uses_official_table_and_flags_unknown() {
        // 内置官方表里的模型: 有价且 priced
        let q = quote("kimi-k3-high");
        assert!(q.priced);
        assert!(q.input_micro > 0 && q.output_micro > 0);
        // -fast 变体 ×2
        let base = quote("claude-opus-5");
        let fast = quote("claude-opus-5-fast");
        assert_eq!(fast.input_micro, base.input_micro * 2);
        // 完全未知模型: 兜底价, priced=false
        let unk = quote("zzz-nonexistent-model-9");
        assert!(!unk.priced);
        assert!(unk.input_micro > 0);
    }

    #[test]
    fn parse_time_formats() {
        // 2026-01-01 00:00 +08:00 = 2025-12-31T16:00:00Z = 1767196800
        assert_eq!(
            parse_time("2026-01-01", 480, false),
            Some(1_767_196_800_000)
        );
        assert_eq!(
            parse_time("2026-01-01", 480, true),
            Some(1_767_196_800_000 + 86_400_000)
        );
        assert_eq!(
            parse_time("2026-01-01 08", 480, false),
            Some(1_767_196_800_000 + 8 * 3_600_000)
        );
        assert_eq!(
            parse_time("2026-01-01T08:30", 480, false),
            Some(1_767_196_800_000 + 8 * 3_600_000 + 30 * 60_000)
        );
        assert_eq!(
            parse_time("1767196800", 480, false),
            Some(1_767_196_800_000)
        );
        assert_eq!(
            parse_time("1767196800123", 480, false),
            Some(1_767_196_800_123)
        );
        assert_eq!(parse_time("garbage", 480, false), None);
    }

    fn tmp_db() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("cfp-bill-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("billing.db")
    }

    fn rec(
        ctx: &BillingCtx,
        req: &str,
        model: &str,
        inp: u64,
        out: u64,
        status: u16,
    ) -> BillingRecord {
        BillingRecord::build(
            ctx,
            req,
            model,
            "acc1",
            u(inp, out),
            false,
            status,
            100,
            "127.0.0.1",
        )
    }

    fn count(conn: &Connection, sql: &str) -> i64 {
        conn.query_row(sql, [], |r| r.get(0)).unwrap()
    }

    #[test]
    fn output_tps_excludes_ttft() {
        let ctx = BillingCtx::from_key(None, "kimi-k3");
        let u = Usage {
            input: 1,
            output: 200,
            cache_read: 0,
            cache_write: 0,
        };
        let r = BillingRecord::build(&ctx, "x", "kimi-k3", "a", u, true, 200, 5000, "");
        assert!((r.output_tps().unwrap() - 40.0).abs() < 1e-9); // 无 ttft: 200/5s
        let r = r.with_ttft(Some(1000));
        assert!((r.output_tps().unwrap() - 50.0).abs() < 1e-9); // 200/(5-1)s
        let r0 = BillingRecord::build(
            &ctx,
            "y",
            "kimi-k3",
            "a",
            Usage::default(),
            true,
            200,
            5000,
            "",
        );
        assert!(r0.output_tps().is_none());
    }

    #[test]
    fn migrate_adds_cache_columns_to_old_db() {
        let db = tmp_db();
        {
            let c = Connection::open(&db).unwrap();
            // 老 schema (无缓存列)
            c.execute_batch("CREATE TABLE billing_records (id INTEGER PRIMARY KEY, ts_ms INTEGER NOT NULL, req_id TEXT NOT NULL UNIQUE, key_hash TEXT NOT NULL, key_prefix TEXT NOT NULL, key_name TEXT NOT NULL, sales_id TEXT, commission_bps INTEGER NOT NULL, model TEXT NOT NULL, account TEXT NOT NULL, input_tokens INTEGER NOT NULL, output_tokens INTEGER NOT NULL, input_price_micro INTEGER NOT NULL, output_price_micro INTEGER NOT NULL, priced INTEGER NOT NULL, cost_nano INTEGER NOT NULL, commission_nano INTEGER NOT NULL, stream INTEGER NOT NULL, status INTEGER NOT NULL, latency_ms INTEGER NOT NULL, client_ip TEXT NOT NULL, tags TEXT NOT NULL);
                INSERT INTO billing_records VALUES (1, 1, 'old', 'h', 'p', 'n', NULL, 0, 'm', 'a', 10, 20, 0, 0, 0, 0, 0, 0, 200, 1, '', '[]');").unwrap();
        }
        let ledger = Ledger::open(&db).unwrap();
        let conn = ledger.reader().unwrap();
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM billing_records"), 1);
        assert_eq!(
            count(&conn, "SELECT cache_read_tokens FROM billing_records"),
            0
        );
        // 老行 input_incl_cache 默认 1 (input 含缓存); 新写入的行为 0
        assert_eq!(
            count(
                &conn,
                "SELECT input_incl_cache FROM billing_records WHERE req_id='old'"
            ),
            1
        );
        drop(conn);
        let ctx = BillingCtx::from_key(None, "kimi-k3");
        ledger.record(BillingRecord::build(
            &ctx,
            "new-row",
            "kimi-k3",
            "acc",
            Usage {
                input: 3,
                output: 1,
                cache_read: 100,
                cache_write: 0,
            },
            true,
            200,
            5,
            "",
        ));
        ledger.record(
            BillingRecord::build(
                &ctx,
                "ttft-row",
                "kimi-k3",
                "acc",
                Usage {
                    input: 3,
                    output: 200,
                    cache_read: 0,
                    cache_write: 0,
                },
                true,
                200,
                5000,
                "",
            )
            .with_ttft(Some(1000)),
        );
        assert!(ledger.flush(Duration::from_secs(5)));
        let conn = ledger.reader().unwrap();
        assert_eq!(
            count(
                &conn,
                "SELECT input_incl_cache FROM billing_records WHERE req_id='new-row'"
            ),
            0
        );
        assert_eq!(
            count(
                &conn,
                "SELECT ttft_ms FROM billing_records WHERE req_id='ttft-row'"
            ),
            1000
        );
        assert_eq!(
            count(
                &conn,
                "SELECT ttft_ms IS NULL FROM billing_records WHERE req_id='new-row'"
            ),
            1
        );
        let _ = std::fs::remove_dir_all(db.parent().unwrap());
    }

    #[test]
    fn ledger_roundtrip_dedup_and_face_cost() {
        let db = tmp_db();
        let ledger = Ledger::open(&db).unwrap();
        let mut key = ApiKeyRecord::from_raw("«redacted:sk-…»".into());
        key.name = "客户A".into();
        key.tags = vec!["vip".into(), "proj-x".into()];
        let ctx = BillingCtx::from_key(Some(&key), "kimi-k3-high");
        let q = ctx.quote;
        assert!(q.priced);

        ledger.record(rec(&ctx, "r1", "kimi-k3-high", 1000, 100, 200));
        ledger.record(rec(&ctx, "r2", "kimi-k3-high", 2000, 200, 200));
        ledger.record(rec(&ctx, "r2", "kimi-k3-high", 2000, 200, 200)); // 重复 req_id → 忽略
        ledger.record(rec(&ctx, "r3", "kimi-k3-high", 0, 0, 502)); // 失败无 usage → 0
        ledger.record(rec(&ctx, "r4", "kimi-k3-high", 500, 50, 504)); // 中断但有 token → 仍计面值
        assert!(ledger.flush(Duration::from_secs(5)));
        let st = ledger.stats();
        assert_eq!(st["written"], 4);
        assert_eq!(st["ignored_duplicates"], 1);
        assert_eq!(st["pending"], 0);

        let conn = ledger.reader().unwrap();
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM billing_records"), 4);
        let expect = cost_nano(&u(1000, 100), &q)
            + cost_nano(&u(2000, 200), &q)
            + cost_nano(&u(500, 50), &q);
        assert_eq!(
            count(&conn, "SELECT SUM(cost_nano) FROM billing_records"),
            expect
        );
        assert_eq!(
            count(
                &conn,
                "SELECT cost_nano FROM billing_records WHERE req_id='r3'"
            ),
            0
        );
        // 废弃列恒 NULL/0, 老面板/脚本读表不炸
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM billing_records WHERE sales_id IS NULL AND commission_nano = 0"), 4);
        assert_eq!(
            count(&conn, "SELECT COUNT(*) FROM billing_tags WHERE tag='vip'"),
            4
        );
        let _ = std::fs::remove_dir_all(db.parent().unwrap());
    }

    #[test]
    fn concurrent_writes_sum_exactly() {
        // 32 线程 × 500 条, 每条 1 输入 token; 总 token 恰好 16000, 无丢无重
        let db = tmp_db();
        let ledger = Arc::new(Ledger::open(&db).unwrap());
        let key = ApiKeyRecord::from_raw("sk-bbb...bbbb".into());
        let ctx = Arc::new(BillingCtx::from_key(Some(&key), "kimi-k3-high"));
        let per = cost_nano(&u(1, 0), &ctx.quote);
        let handles: Vec<_> = (0..32)
            .map(|t| {
                let l = ledger.clone();
                let c = ctx.clone();
                std::thread::spawn(move || {
                    for i in 0..500 {
                        l.record(rec(&c, &format!("t{t}-{i}"), "kimi-k3-high", 1, 0, 200));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert!(ledger.flush(Duration::from_secs(30)));
        let conn = ledger.reader().unwrap();
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM billing_records"), 16000);
        assert_eq!(
            count(&conn, "SELECT SUM(input_tokens) FROM billing_records"),
            16000
        );
        assert_eq!(
            count(&conn, "SELECT SUM(cost_nano) FROM billing_records"),
            per * 16000
        );
        assert_eq!(ledger.stats()["written"], 16000);
        ledger.request_checkpoint();
        assert!(ledger.flush(Duration::from_secs(5)));
        let wal = db.with_file_name("billing.db-wal");
        let wal_len = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
        assert!(wal_len < 8 * 1024 * 1024, "wal not truncated: {wal_len}");
        let _ = std::fs::remove_dir_all(db.parent().unwrap());
    }
}
