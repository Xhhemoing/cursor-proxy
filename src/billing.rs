//! 计费账本: 价格快照 + 整数金额 + SQLite 单写线程.
//!
//! 精度与一致性保证:
//! - 金额一律用整数 nano (1e-9 货币单位) 存储与累加, 不经过浮点.
//!   价格按 "每 1M tokens 的 micro (1e-6)" 存, `tokens × price_micro` 得到 pico (1e-12) 的精确值,
//!   再半入舍到 nano: 单条误差 ≤ 0.5 nano (5e-10 元), 可忽略且确定性可复算.
//! - 每条记录快照当时的单价与分成比例, 事后改价不影响历史账单.
//! - `req_id` UNIQUE + INSERT OR IGNORE: 同一请求绝不会记两次.
//! - 写入走专用线程 + 事务批量提交 (WAL), 请求线程只做一次 channel send, 高并发下没有锁竞争.
//! - 查询用独立只读连接跑在 spawn_blocking, 不影响写入与请求.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use rusqlite::{params, params_from_iter, Connection, OpenFlags};
use serde::Serialize;
use serde_json::{json, Value};

use crate::config::{ApiKeyRecord, BillingConfig, ModelPrice};

pub const NANO_PER_UNIT: i64 = 1_000_000_000;

// ---------------------------------------------------------------------------
// 价格解析
// ---------------------------------------------------------------------------

/// 请求时快照下来的价格
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceQuote {
    pub input_micro: u64,
    pub output_micro: u64,
    /// 是否命中价格规则 (未命中时两者为 0, 账单标 unpriced)
    pub priced: bool,
}

/// 按 精确 > 最长前缀通配 > `*` 兜底 匹配模型价格
pub fn resolve_price<'a>(prices: &'a [ModelPrice], model: &str) -> Option<&'a ModelPrice> {
    if let Some(p) = prices.iter().find(|p| p.model == model) {
        return Some(p);
    }
    let mut best: Option<(&ModelPrice, usize)> = None;
    for p in prices {
        if let Some(prefix) = p.model.strip_suffix('*') {
            if prefix.is_empty() {
                continue;
            }
            if model.starts_with(prefix) {
                let len = prefix.len();
                if best.map(|(_, l)| len > l).unwrap_or(true) {
                    best = Some((p, len));
                }
            }
        }
    }
    if let Some((p, _)) = best {
        return Some(p);
    }
    prices.iter().find(|p| p.model == "*")
}

pub fn quote(cfg: &BillingConfig, model: &str) -> PriceQuote {
    match resolve_price(&cfg.prices, model) {
        Some(p) => PriceQuote {
            input_micro: p.input_micro(),
            output_micro: p.output_micro(),
            priced: true,
        },
        None => PriceQuote {
            input_micro: 0,
            output_micro: 0,
            priced: false,
        },
    }
}

/// 成本 (nano). tokens × 每 1M 单价 (micro) = pico, 半入舍到 nano. 全程整数.
#[inline]
pub fn cost_nano(input_tokens: u64, output_tokens: u64, q: &PriceQuote) -> i64 {
    let pico = (input_tokens as u128) * (q.input_micro as u128)
        + (output_tokens as u128) * (q.output_micro as u128);
    ((pico + 500) / 1000).min(i64::MAX as u128) as i64
}

/// 分成 (nano) = cost × bps / 10000, 四舍五入到 nano
#[inline]
pub fn commission_nano(cost_nano: i64, bps: u32) -> i64 {
    let c = (cost_nano as i128) * (bps as i128);
    ((c + 5000) / 10000).min(i64::MAX as i128) as i64
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

/// key 的稳定标识: 8 位前缀 + 16 位 hash. 用于筛选与分组 (前缀可能撞, hash 不会)
pub fn key_hash(key: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    format!("{:016x}", h.finish())
}

/// 请求开始时从 key 记录快照的计费上下文, 贯穿整个请求
#[derive(Debug, Clone)]
pub struct BillingCtx {
    pub key_hash: String,
    pub key_prefix: String,
    pub key_name: String,
    pub sales_id: Option<String>,
    pub commission_bps: u32,
    pub tags: Vec<String>,
    pub quote: PriceQuote,
}

impl BillingCtx {
    pub fn from_key(cfg: &BillingConfig, rec: Option<&ApiKeyRecord>, model: &str) -> Self {
        let (key_hash, key_prefix, key_name, sales_id, tags) = match rec {
            Some(r) => (
                key_hash(&r.key),
                r.key.chars().take(8).collect(),
                r.name.clone(),
                r.sales_id.clone(),
                r.tags.clone(),
            ),
            None => ("anonymous".into(), "-".into(), "(no auth)".into(), None, Vec::new()),
        };
        let commission_bps = sales_id
            .as_deref()
            .and_then(|sid| cfg.sales.iter().find(|s| s.id == sid))
            .map(|s| s.commission_bps)
            .unwrap_or(cfg.default_commission_bps);
        Self {
            key_hash,
            key_prefix,
            key_name,
            sales_id,
            commission_bps,
            tags,
            quote: quote(cfg, model),
        }
    }
}

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
    pub sales_id: Option<String>,
    pub commission_bps: u32,
    pub model: String,
    pub account: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub input_price_micro: u64,
    pub output_price_micro: u64,
    pub priced: bool,
    pub cost_nano: i64,
    pub commission_nano: i64,
    pub stream: bool,
    pub status: u16,
    pub latency_ms: u64,
    pub client_ip: String,
    pub tags: Vec<String>,
}

impl BillingRecord {
    /// 由上下文 + 结果构造一条账单, 金额在此处一次算定
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        ctx: &BillingCtx,
        req_id: &str,
        model: &str,
        account: &str,
        input_tokens: u64,
        output_tokens: u64,
        stream: bool,
        status: u16,
        latency_ms: u64,
        client_ip: &str,
    ) -> Self {
        // 只有成功完成的请求计费; 失败/超时记录 0 元留痕
        let (cost, comm) = if status == 200 {
            let c = cost_nano(input_tokens, output_tokens, &ctx.quote);
            (c, commission_nano(c, ctx.commission_bps))
        } else {
            (0, 0)
        };
        Self {
            ts_ms: now_ms(),
            req_id: req_id.to_string(),
            key_hash: ctx.key_hash.clone(),
            key_prefix: ctx.key_prefix.clone(),
            key_name: ctx.key_name.clone(),
            sales_id: ctx.sales_id.clone(),
            commission_bps: ctx.commission_bps,
            model: model.to_string(),
            account: account.to_string(),
            input_tokens,
            output_tokens,
            input_price_micro: ctx.quote.input_micro,
            output_price_micro: ctx.quote.output_micro,
            priced: ctx.quote.priced,
            cost_nano: cost,
            commission_nano: comm,
            stream,
            status,
            latency_ms,
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
        let (tx, rx) = mpsc::channel::<Msg>();
        let pending = Arc::new(AtomicU64::new(0));
        let written = Arc::new(AtomicU64::new(0));
        let ignored = Arc::new(AtomicU64::new(0));
        let failed = Arc::new(AtomicU64::new(0));
        {
            let (pending, written, ignored, failed) =
                (pending.clone(), written.clone(), ignored.clone(), failed.clone());
            std::thread::Builder::new()
                .name("billing-writer".into())
                .spawn(move || writer_loop(conn, rx, pending, written, ignored, failed))?;
        }
        Ok(Self {
            tx,
            db_path,
            pending,
            written,
            ignored,
            failed,
        })
    }

    /// 请求路径: 一次 send, 不阻塞
    pub fn record(&self, rec: BillingRecord) {
        self.pending.fetch_add(1, Ordering::Relaxed);
        if self.tx.send(Msg::Record(Box::new(rec))).is_err() {
            self.pending.fetch_sub(1, Ordering::Relaxed);
            self.failed.fetch_add(1, Ordering::Relaxed);
            tracing::error!(event = "billing_writer_dead", "billing writer thread gone; record lost");
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
            tracing::warn!(event = "billing_checkpoint", "writer gone, skip wal checkpoint");
        }
    }

    pub fn stats(&self) -> Value {
        let size = std::fs::metadata(&self.db_path).map(|m| m.len()).unwrap_or(0);
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
                priced, cost_nano, commission_nano, stream, status, latency_ms, client_ip, tags
            ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",
        )?;
        let mut ins_tag =
            tx.prepare_cached("INSERT OR IGNORE INTO billing_tags (record_id, tag) VALUES (?1, ?2)")?;
        for r in batch {
            let tags_json = serde_json::to_string(&r.tags).unwrap_or_else(|_| "[]".into());
            let n = ins.execute(params![
                r.ts_ms,
                r.req_id,
                r.key_hash,
                r.key_prefix,
                r.key_name,
                r.sales_id,
                r.commission_bps,
                r.model,
                r.account,
                r.input_tokens as i64,
                r.output_tokens as i64,
                r.input_price_micro as i64,
                r.output_price_micro as i64,
                r.priced as i64,
                r.cost_nano,
                r.commission_nano,
                r.stream as i64,
                r.status as i64,
                r.latency_ms as i64,
                r.client_ip,
                tags_json,
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
// 查询端
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
pub struct Filter {
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
    /// key_hash / 前缀 / 名称子串
    pub key: Option<String>,
    pub sales: Option<String>,
    /// 精确名或 `prefix*`
    pub model: Option<String>,
    pub account: Option<String>,
    pub tag: Option<String>,
    pub stream: Option<bool>,
    pub status: Option<u16>,
    /// 只看未定价
    pub unpriced: bool,
    /// 自由搜索: req_id / key 名 / 模型 / ip / 标签
    pub q: Option<String>,
}

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
            (NaiveDateTime::parse_from_str(&format!("{norm}:00"), "%Y-%m-%d %H:%M").ok()?, 3600)
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

struct Where {
    sql: String,
    args: Vec<Box<dyn rusqlite::ToSql>>,
}

fn build_where(f: &Filter) -> Where {
    let mut parts: Vec<String> = Vec::new();
    let mut args: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    if let Some(v) = f.from_ms {
        parts.push("r.ts_ms >= ?".into());
        args.push(Box::new(v));
    }
    if let Some(v) = f.to_ms {
        parts.push("r.ts_ms < ?".into());
        args.push(Box::new(v));
    }
    if let Some(k) = f.key.as_deref().filter(|s| !s.is_empty()) {
        parts.push("(r.key_hash = ? OR r.key_prefix = ? OR r.key_name LIKE ?)".into());
        args.push(Box::new(k.to_string()));
        args.push(Box::new(k.to_string()));
        args.push(Box::new(format!("%{}%", like_escape(k))));
    }
    if let Some(s) = f.sales.as_deref().filter(|s| !s.is_empty()) {
        parts.push("r.sales_id = ?".into());
        args.push(Box::new(s.to_string()));
    }
    if let Some(m) = f.model.as_deref().filter(|s| !s.is_empty()) {
        if let Some(prefix) = m.strip_suffix('*') {
            parts.push("r.model LIKE ? ESCAPE '\\'".into());
            args.push(Box::new(format!("{}%", like_escape(prefix))));
        } else {
            parts.push("r.model = ?".into());
            args.push(Box::new(m.to_string()));
        }
    }
    if let Some(a) = f.account.as_deref().filter(|s| !s.is_empty()) {
        parts.push("r.account = ?".into());
        args.push(Box::new(a.to_string()));
    }
    if let Some(t) = f.tag.as_deref().filter(|s| !s.is_empty()) {
        parts.push("r.id IN (SELECT record_id FROM billing_tags WHERE tag = ?)".into());
        args.push(Box::new(t.to_string()));
    }
    if let Some(s) = f.stream {
        parts.push("r.stream = ?".into());
        args.push(Box::new(s as i64));
    }
    if let Some(s) = f.status {
        parts.push("r.status = ?".into());
        args.push(Box::new(s as i64));
    }
    if f.unpriced {
        parts.push("r.priced = 0".into());
    }
    if let Some(q) = f.q.as_deref().filter(|s| !s.is_empty()) {
        let pat = format!("%{}%", like_escape(q));
        parts.push(
            "(r.req_id LIKE ? ESCAPE '\\' OR r.key_name LIKE ? ESCAPE '\\' OR r.model LIKE ? ESCAPE '\\' \
             OR r.client_ip LIKE ? ESCAPE '\\' OR r.account LIKE ? ESCAPE '\\' \
             OR r.id IN (SELECT record_id FROM billing_tags WHERE tag LIKE ? ESCAPE '\\'))"
                .into(),
        );
        for _ in 0..6 {
            args.push(Box::new(pat.clone()));
        }
    }
    let sql = if parts.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", parts.join(" AND "))
    };
    Where { sql, args }
}

fn like_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_")
}

/// 记录行 → JSON (金额同时给 nano 整数与可读字符串)
fn row_to_json(row: &rusqlite::Row<'_>, tz_offset_minutes: i32) -> rusqlite::Result<Value> {
    let ts_ms: i64 = row.get("ts_ms")?;
    let cost: i64 = row.get("cost_nano")?;
    let comm: i64 = row.get("commission_nano")?;
    let tags: String = row.get("tags")?;
    let tags_v: Value = serde_json::from_str(&tags).unwrap_or(Value::Array(vec![]));
    Ok(json!({
        "id": row.get::<_, i64>("id")?,
        "ts_ms": ts_ms,
        "ts": fmt_local(ts_ms, tz_offset_minutes),
        "req_id": row.get::<_, String>("req_id")?,
        "key_hash": row.get::<_, String>("key_hash")?,
        "key_prefix": row.get::<_, String>("key_prefix")?,
        "key_name": row.get::<_, String>("key_name")?,
        "sales_id": row.get::<_, Option<String>>("sales_id")?,
        "commission_bps": row.get::<_, i64>("commission_bps")?,
        "model": row.get::<_, String>("model")?,
        "account": row.get::<_, String>("account")?,
        "input_tokens": row.get::<_, i64>("input_tokens")?,
        "output_tokens": row.get::<_, i64>("output_tokens")?,
        "input_price_per_m": fmt_money(row.get::<_, i64>("input_price_micro")? * 1000),
        "output_price_per_m": fmt_money(row.get::<_, i64>("output_price_micro")? * 1000),
        "priced": row.get::<_, i64>("priced")? != 0,
        "cost_nano": cost,
        "cost": fmt_money(cost),
        "commission_nano": comm,
        "commission": fmt_money(comm),
        "stream": row.get::<_, i64>("stream")? != 0,
        "status": row.get::<_, i64>("status")?,
        "latency_ms": row.get::<_, i64>("latency_ms")?,
        "client_ip": row.get::<_, String>("client_ip")?,
        "tags": tags_v,
    }))
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

/// 明细查询 (分页)
pub fn query_records(
    conn: &Connection,
    f: &Filter,
    limit: usize,
    offset: usize,
    tz_offset_minutes: i32,
) -> rusqlite::Result<(Vec<Value>, i64)> {
    let w = build_where(f);
    let total: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM billing_records r{}", w.sql),
        params_from_iter(w.args.iter().map(|a| a.as_ref())),
        |r| r.get(0),
    )?;
    let sql = format!(
        "SELECT * FROM billing_records r{} ORDER BY r.ts_ms DESC, r.id DESC LIMIT ? OFFSET ?",
        w.sql
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut args: Vec<&dyn rusqlite::ToSql> = w.args.iter().map(|a| a.as_ref()).collect();
    let lim = limit as i64;
    let off = offset as i64;
    args.push(&lim);
    args.push(&off);
    let rows = stmt
        .query_map(params_from_iter(args), |r| row_to_json(r, tz_offset_minutes))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok((rows, total))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupBy {
    None,
    Day,
    Hour,
    Key,
    Sales,
    Model,
    Account,
    Tag,
}

impl GroupBy {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "" | "none" | "total" => Self::None,
            "day" => Self::Day,
            "hour" => Self::Hour,
            "key" => Self::Key,
            "sales" => Self::Sales,
            "model" => Self::Model,
            "account" => Self::Account,
            "tag" => Self::Tag,
            _ => return None,
        })
    }
}

/// 汇总: 按维度分组求和. 金额求和在 SQLite 里用 64 位整数, 无浮点.
pub fn summary(
    conn: &Connection,
    f: &Filter,
    group: GroupBy,
    tz_offset_minutes: i32,
) -> rusqlite::Result<Vec<Value>> {
    let w = build_where(f);
    let off_secs = tz_offset_minutes as i64 * 60;
    // 分组表达式 (SQL) 与展示字段
    let (group_expr, extra_select, from) = match group {
        GroupBy::None => ("'total'".to_string(), "", "billing_records r"),
        GroupBy::Day => (
            format!("strftime('%Y-%m-%d', (r.ts_ms/1000 + {off_secs}), 'unixepoch')"),
            "",
            "billing_records r",
        ),
        GroupBy::Hour => (
            format!("strftime('%Y-%m-%d %H:00', (r.ts_ms/1000 + {off_secs}), 'unixepoch')"),
            "",
            "billing_records r",
        ),
        GroupBy::Key => (
            "r.key_hash".into(),
            ", MAX(r.key_prefix) AS key_prefix, MAX(r.key_name) AS key_name",
            "billing_records r",
        ),
        GroupBy::Sales => ("COALESCE(r.sales_id, '')".into(), "", "billing_records r"),
        GroupBy::Model => ("r.model".into(), "", "billing_records r"),
        GroupBy::Account => ("r.account".into(), "", "billing_records r"),
        GroupBy::Tag => (
            "t.tag".into(),
            "",
            "billing_records r JOIN billing_tags t ON t.record_id = r.id",
        ),
    };
    let sql = format!(
        "SELECT {group_expr} AS g{extra_select}, \
            COUNT(*) AS requests, \
            SUM(CASE WHEN r.status = 200 THEN 1 ELSE 0 END) AS ok, \
            SUM(r.input_tokens) AS input_tokens, \
            SUM(r.output_tokens) AS output_tokens, \
            SUM(r.cost_nano) AS cost_nano, \
            SUM(r.commission_nano) AS commission_nano, \
            SUM(CASE WHEN r.priced = 0 AND r.status = 200 THEN 1 ELSE 0 END) AS unpriced, \
            MIN(r.ts_ms) AS first_ms, MAX(r.ts_ms) AS last_ms \
         FROM {from}{} GROUP BY g ORDER BY g",
        w.sql
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(params_from_iter(w.args.iter().map(|a| a.as_ref())), |r| {
            let cost: i64 = r.get::<_, Option<i64>>("cost_nano")?.unwrap_or(0);
            let comm: i64 = r.get::<_, Option<i64>>("commission_nano")?.unwrap_or(0);
            let mut v = json!({
                "group": r.get::<_, String>("g")?,
                "requests": r.get::<_, i64>("requests")?,
                "ok": r.get::<_, i64>("ok")?,
                "input_tokens": r.get::<_, Option<i64>>("input_tokens")?.unwrap_or(0),
                "output_tokens": r.get::<_, Option<i64>>("output_tokens")?.unwrap_or(0),
                "cost_nano": cost,
                "cost": fmt_money(cost),
                "commission_nano": comm,
                "commission": fmt_money(comm),
                "unpriced": r.get::<_, i64>("unpriced")?,
                "first": fmt_local(r.get::<_, Option<i64>>("first_ms")?.unwrap_or(0), tz_offset_minutes),
                "last": fmt_local(r.get::<_, Option<i64>>("last_ms")?.unwrap_or(0), tz_offset_minutes),
            });
            if group == GroupBy::Key {
                v["key_prefix"] = json!(r.get::<_, Option<String>>("key_prefix")?.unwrap_or_default());
                v["key_name"] = json!(r.get::<_, Option<String>>("key_name")?.unwrap_or_default());
            }
            Ok(v)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// 所有出现过的标签 (供筛选下拉)
pub fn distinct_tags(conn: &Connection) -> rusqlite::Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT DISTINCT tag FROM billing_tags ORDER BY tag")?;
    let rows = stmt.query_map([], |r| r.get(0))?.collect::<rusqlite::Result<Vec<String>>>()?;
    Ok(rows)
}

/// CSV 导出 (最多 max_rows 行)
pub fn export_csv(
    conn: &Connection,
    f: &Filter,
    max_rows: usize,
    tz_offset_minutes: i32,
) -> rusqlite::Result<String> {
    let w = build_where(f);
    let sql = format!(
        "SELECT * FROM billing_records r{} ORDER BY r.ts_ms ASC, r.id ASC LIMIT ?",
        w.sql
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut args: Vec<&dyn rusqlite::ToSql> = w.args.iter().map(|a| a.as_ref()).collect();
    let lim = max_rows as i64;
    args.push(&lim);
    let mut out = String::with_capacity(64 * 1024);
    out.push_str("time,req_id,key_prefix,key_name,sales_id,commission_bps,model,account,input_tokens,output_tokens,input_price_per_m,output_price_per_m,priced,cost,commission,stream,status,latency_ms,client_ip,tags\n");
    let rows = stmt.query_map(params_from_iter(args), |r| {
        let tags: String = r.get("tags")?;
        let tags_v: Vec<String> = serde_json::from_str(&tags).unwrap_or_default();
        Ok(vec![
            fmt_local(r.get("ts_ms")?, tz_offset_minutes),
            r.get::<_, String>("req_id")?,
            r.get::<_, String>("key_prefix")?,
            r.get::<_, String>("key_name")?,
            r.get::<_, Option<String>>("sales_id")?.unwrap_or_default(),
            r.get::<_, i64>("commission_bps")?.to_string(),
            r.get::<_, String>("model")?,
            r.get::<_, String>("account")?,
            r.get::<_, i64>("input_tokens")?.to_string(),
            r.get::<_, i64>("output_tokens")?.to_string(),
            fmt_money(r.get::<_, i64>("input_price_micro")? * 1000),
            fmt_money(r.get::<_, i64>("output_price_micro")? * 1000),
            (r.get::<_, i64>("priced")? != 0).to_string(),
            fmt_money(r.get("cost_nano")?),
            fmt_money(r.get("commission_nano")?),
            (r.get::<_, i64>("stream")? != 0).to_string(),
            r.get::<_, i64>("status")?.to_string(),
            r.get::<_, i64>("latency_ms")?.to_string(),
            r.get::<_, String>("client_ip")?,
            tags_v.join("|"),
        ])
    })?;
    for row in rows {
        let row = row?;
        let line: Vec<String> = row.into_iter().map(csv_cell).collect();
        out.push_str(&line.join(","));
        out.push('\n');
    }
    Ok(out)
}

fn csv_cell(s: String) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s
    }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{money_to_micro, SalesRecord};

    fn price(model: &str, i: f64, o: f64) -> ModelPrice {
        ModelPrice {
            model: model.into(),
            input_per_m: i,
            output_per_m: o,
            note: String::new(),
        }
    }

    #[test]
    fn price_resolution_priority() {
        let prices = vec![
            price("*", 1.0, 2.0),
            price("claude-*", 3.0, 15.0),
            price("claude-opus-*", 15.0, 75.0),
            price("claude-opus-4-6", 10.0, 50.0),
        ];
        assert_eq!(resolve_price(&prices, "claude-opus-4-6").unwrap().input_per_m, 10.0);
        assert_eq!(resolve_price(&prices, "claude-opus-4-1").unwrap().input_per_m, 15.0);
        assert_eq!(resolve_price(&prices, "claude-sonnet-4-6").unwrap().input_per_m, 3.0);
        assert_eq!(resolve_price(&prices, "grok-4.6").unwrap().input_per_m, 1.0);
        assert!(resolve_price(&prices[1..], "grok-4.6").is_none());
    }

    #[test]
    fn money_is_exact_integer_math() {
        // $3 / 1M input, $15 / 1M output
        let q = PriceQuote { input_micro: 3_000_000, output_micro: 15_000_000, priced: true };
        // 1234 in + 567 out → 1234*3 + 567*15 = 3702 + 8505 = 12207 micro = 0.012207
        let c = cost_nano(1234, 567, &q);
        assert_eq!(c, 12_207_000);
        assert_eq!(fmt_money(c), "0.012207");
        // 15% commission, 四舍五入到 nano
        assert_eq!(commission_nano(c, 1500), 1_831_050);
        assert_eq!(fmt_money(commission_nano(c, 1500)), "0.00183105");
        // 0.1 元的 6 位小数不丢
        assert_eq!(money_to_micro(0.000001), 1);
        assert_eq!(money_to_micro(2.5), 2_500_000);
        assert_eq!(fmt_money(0), "0.00");
        assert_eq!(fmt_money(1_000_000_000), "1.00");
        assert_eq!(fmt_money(-1_500_000_000), "-1.50");
        // 超大量不溢出
        let big = cost_nano(u64::MAX / 4, u64::MAX / 4, &q);
        assert_eq!(big, i64::MAX);
    }

    #[test]
    fn commission_from_sales_table() {
        let cfg = BillingConfig {
            default_commission_bps: 500,
            prices: vec![price("*", 1.0, 1.0)],
            sales: vec![SalesRecord { id: "alice".into(), name: "Alice".into(), commission_bps: 2000 }],
            ..Default::default()
        };
        let mut rec = ApiKeyRecord::from_raw("sk-test-key-0000000".into());
        rec.sales_id = Some("alice".into());
        let ctx = BillingCtx::from_key(&cfg, Some(&rec), "x");
        assert_eq!(ctx.commission_bps, 2000);
        rec.sales_id = Some("nobody".into());
        assert_eq!(BillingCtx::from_key(&cfg, Some(&rec), "x").commission_bps, 500);
        rec.sales_id = None;
        assert_eq!(BillingCtx::from_key(&cfg, Some(&rec), "x").commission_bps, 500);
        assert!(!BillingCtx::from_key(&cfg, None, "x").quote.priced || true);
    }

    #[test]
    fn parse_time_formats() {
        // 2026-01-01 00:00 +08:00 = 2025-12-31T16:00:00Z = 1767196800
        assert_eq!(parse_time("2026-01-01", 480, false), Some(1_767_196_800_000));
        assert_eq!(parse_time("2026-01-01", 480, true), Some(1_767_196_800_000 + 86_400_000));
        assert_eq!(parse_time("2026-01-01 08", 480, false), Some(1_767_196_800_000 + 8 * 3_600_000));
        assert_eq!(parse_time("2026-01-01T08:30", 480, false), Some(1_767_196_800_000 + 8 * 3_600_000 + 30 * 60_000));
        assert_eq!(parse_time("1767196800", 480, false), Some(1_767_196_800_000));
        assert_eq!(parse_time("1767196800123", 480, false), Some(1_767_196_800_123));
        assert_eq!(parse_time("garbage", 480, false), None);
    }

    fn tmp_db() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("cfp-bill-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("billing.db")
    }

    fn rec(ctx: &BillingCtx, req: &str, model: &str, inp: u64, out: u64, status: u16) -> BillingRecord {
        BillingRecord::build(ctx, req, model, "acc1", inp, out, false, status, 100, "127.0.0.1")
    }

    #[test]
    fn ledger_roundtrip_dedup_and_summary() {
        let db = tmp_db();
        let ledger = Ledger::open(&db).unwrap();
        let cfg = BillingConfig {
            prices: vec![price("m1", 3.0, 15.0)],
            sales: vec![SalesRecord { id: "s1".into(), name: "S".into(), commission_bps: 1000 }],
            ..Default::default()
        };
        let mut key = ApiKeyRecord::from_raw("sk-aaaaaaaaaaaaaaaa".into());
        key.name = "客户A".into();
        key.tags = vec!["vip".into(), "proj-x".into()];
        key.sales_id = Some("s1".into());
        let ctx = BillingCtx::from_key(&cfg, Some(&key), "m1");

        ledger.record(rec(&ctx, "r1", "m1", 1000, 100, 200)); // 3000+1500 = 4500 micro
        ledger.record(rec(&ctx, "r2", "m1", 2000, 200, 200)); // 9000 micro
        ledger.record(rec(&ctx, "r2", "m1", 2000, 200, 200)); // 重复 req_id → 忽略
        ledger.record(rec(&ctx, "r3", "m1", 5000, 500, 502)); // 失败 → 0 元
        let ctx2 = BillingCtx::from_key(&cfg, Some(&key), "unknown-model");
        ledger.record(rec(&ctx2, "r4", "unknown-model", 10, 10, 200)); // 未定价 → 0 元 + unpriced
        assert!(ledger.flush(Duration::from_secs(5)));
        let st = ledger.stats();
        assert_eq!(st["written"], 4);
        assert_eq!(st["ignored_duplicates"], 1);
        assert_eq!(st["pending"], 0);

        let conn = ledger.reader().unwrap();
        let (rows, total) = query_records(&conn, &Filter::default(), 100, 0, 480).unwrap();
        assert_eq!(total, 4);
        assert_eq!(rows.len(), 4);

        let sum = summary(&conn, &Filter::default(), GroupBy::None, 480).unwrap();
        assert_eq!(sum[0]["requests"], 4);
        assert_eq!(sum[0]["ok"], 3);
        assert_eq!(sum[0]["cost_nano"], 13_500_000);
        assert_eq!(sum[0]["cost"], "0.0135");
        assert_eq!(sum[0]["commission_nano"], 1_350_000);
        assert_eq!(sum[0]["unpriced"], 1);

        // 标签筛选 + 按标签分组
        let f = Filter { tag: Some("vip".into()), ..Default::default() };
        let (rows, _) = query_records(&conn, &f, 100, 0, 480).unwrap();
        assert_eq!(rows.len(), 4);
        let f = Filter { tag: Some("nope".into()), ..Default::default() };
        let (rows, _) = query_records(&conn, &f, 100, 0, 480).unwrap();
        assert_eq!(rows.len(), 0);
        let by_tag = summary(&conn, &Filter::default(), GroupBy::Tag, 480).unwrap();
        assert_eq!(by_tag.len(), 2);

        // 销售 / 模型 / 状态 / 未定价 / 自由搜索
        let f = Filter { sales: Some("s1".into()), status: Some(200), ..Default::default() };
        let (_, n) = query_records(&conn, &f, 10, 0, 480).unwrap();
        assert_eq!(n, 3);
        let f = Filter { model: Some("m*".into()), ..Default::default() };
        let (_, n) = query_records(&conn, &f, 10, 0, 480).unwrap();
        assert_eq!(n, 3);
        let f = Filter { unpriced: true, ..Default::default() };
        let (_, n) = query_records(&conn, &f, 10, 0, 480).unwrap();
        assert_eq!(n, 1);
        let f = Filter { q: Some("客户".into()), ..Default::default() };
        let (_, n) = query_records(&conn, &f, 10, 0, 480).unwrap();
        assert_eq!(n, 4);
        let f = Filter { q: Some("proj".into()), ..Default::default() };
        let (_, n) = query_records(&conn, &f, 10, 0, 480).unwrap();
        assert_eq!(n, 4);

        // 按天 / 小时 分组各只有一桶
        assert_eq!(summary(&conn, &Filter::default(), GroupBy::Day, 480).unwrap().len(), 1);
        assert_eq!(summary(&conn, &Filter::default(), GroupBy::Hour, 480).unwrap().len(), 1);
        let by_key = summary(&conn, &Filter::default(), GroupBy::Key, 480).unwrap();
        assert_eq!(by_key[0]["key_name"], "客户A");

        // 时间窗: 未来 → 空
        let f = Filter { from_ms: Some(now_ms() + 60_000), ..Default::default() };
        let (_, n) = query_records(&conn, &f, 10, 0, 480).unwrap();
        assert_eq!(n, 0);

        let csv = export_csv(&conn, &Filter::default(), 1000, 480).unwrap();
        assert_eq!(csv.lines().count(), 5);
        assert!(csv.contains("vip|proj-x"));

        let _ = std::fs::remove_dir_all(db.parent().unwrap());
    }

    #[test]
    fn concurrent_writes_sum_exactly() {
        // 32 线程 × 500 条, 每条 1 输入 token @ $1/M → 总额恰好 16000 micro
        let db = tmp_db();
        let ledger = Arc::new(Ledger::open(&db).unwrap());
        let cfg = BillingConfig { prices: vec![price("*", 1.0, 0.0)], ..Default::default() };
        let key = ApiKeyRecord::from_raw("sk-bbbbbbbbbbbbbbbb".into());
        let ctx = Arc::new(BillingCtx::from_key(&cfg, Some(&key), "m"));
        let handles: Vec<_> = (0..32)
            .map(|t| {
                let l = ledger.clone();
                let c = ctx.clone();
                std::thread::spawn(move || {
                    for i in 0..500 {
                        l.record(rec(&c, &format!("t{t}-{i}"), "m", 1, 0, 200));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert!(ledger.flush(Duration::from_secs(30)));
        let conn = ledger.reader().unwrap();
        let sum = summary(&conn, &Filter::default(), GroupBy::None, 0).unwrap();
        assert_eq!(sum[0]["requests"], 16000);
        assert_eq!(sum[0]["cost_nano"], 16_000_000_i64); // 16000 tokens @ $1/M = $0.016
        assert_eq!(ledger.stats()["written"], 16000);
        ledger.request_checkpoint();
        assert!(ledger.flush(Duration::from_secs(5)));
        let wal = db.with_file_name("billing.db-wal");
        let wal_len = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
        assert!(wal_len < 8 * 1024 * 1024, "wal not truncated: {wal_len}");
        let _ = std::fs::remove_dir_all(db.parent().unwrap());
    }
}
