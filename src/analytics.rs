//! 消耗分析: 从 billing.db 的请求流推断「用户在线时段」, 再按 模型 / 模型组 / 套餐 / 卡 评估消耗.
//!
//! 核心口径 (零售版):
//! - **面值 $**: 逐条按 token 数 × 官方口径单价 (`cards::model_price`, 注册表 > 内置表) 重算,
//!   不信账本里历史 `cost_nano` (旧版按客户价记的). 无 usage 的失败请求面值 0.
//! - **成本 ¥** = 面值 × `CostModel.rmb_per_usd()` (号价 ÷ 可用面值).
//! - **在线时段 (session)**: 同一 key 的请求按开始时间排序, 相邻两条间隔 ≤ gap 视为同一次
//!   使用; 一次使用从首条请求开始到末条请求结束. `gap` 缺省按该 key 自身节奏自适应:
//!   取 < 1h 的相邻间隔的中位数 × 3, 夹在 [5min, 30min] —— 秒接的 agent 用户和慢聊的人类
//!   用同一个固定阈值都会判错, 所以按人算.
//! - **$/在线小时**: 该维度 (模型/组/套餐/卡) 的面值 ÷ 该维度自身请求流算出的在线小时.
//!   对模型来说就是「用户开着这个模型干活时, 每小时烧多少面值」—— 这是给套餐定价用的数.
//! - **忙时 (busy)**: Σ latency, 即上游真正在算的时间; 在线时长 ≥ 忙时.
//!
//! 请求开始时间 = `ts_ms - latency_ms` (账本 ts_ms 记的是请求结束落账的时刻).

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::{json, Value};

/// 一条账本行的分析视图
#[derive(Debug, Clone)]
pub struct Req {
    /// 请求开始 (unix ms)
    pub start_ms: i64,
    /// 请求结束 (unix ms)
    pub end_ms: i64,
    pub key_name: String,
    pub model: String,
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    pub status: u16,
    /// 官方面值 (美元)
    pub face_usd: f64,
}

impl Req {
    pub fn is_card(&self) -> bool {
        self.key_name.starts_with("card-")
    }
    pub fn ok(&self) -> bool {
        self.status == 200
    }
    pub fn latency_ms(&self) -> i64 {
        (self.end_ms - self.start_ms).max(0)
    }
}

/// 一次「在线使用」
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Session {
    pub start_ms: i64,
    pub end_ms: i64,
    pub requests: usize,
    pub face_usd: f64,
}

impl Session {
    pub fn secs(&self) -> f64 {
        ((self.end_ms - self.start_ms).max(0) as f64) / 1000.0
    }
}

pub const GAP_MIN_SECS: f64 = 300.0;
pub const GAP_MAX_SECS: f64 = 1800.0;
pub const GAP_DEFAULT_SECS: f64 = 600.0;

/// 自适应会话间隔: 相邻请求开始时间差 (< 1h 的) 的中位数 × 3, 夹在 [5min, 30min].
/// 样本 < 3 条间隔时退回默认 10min.
pub fn auto_gap_secs(sorted: &[Req]) -> f64 {
    let mut gaps: Vec<f64> = sorted
        .windows(2)
        .map(|w| (w[1].start_ms - w[0].start_ms).max(0) as f64 / 1000.0)
        .filter(|g| *g > 0.0 && *g < 3600.0)
        .collect();
    if gaps.len() < 3 {
        return GAP_DEFAULT_SECS;
    }
    gaps.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let med = gaps[gaps.len() / 2];
    (med * 3.0).clamp(GAP_MIN_SECS, GAP_MAX_SECS)
}

/// 把一条 key 的请求流 (任意顺序) 切成在线时段. `gap_secs = None` → 自适应.
pub fn sessionize(reqs: &[Req], gap_secs: Option<f64>) -> (Vec<Session>, f64) {
    let mut v: Vec<Req> = reqs.to_vec();
    v.sort_by_key(|r| (r.start_ms, r.end_ms));
    let gap = gap_secs.unwrap_or_else(|| auto_gap_secs(&v));
    let gap_ms = (gap * 1000.0) as i64;
    let mut out: Vec<Session> = Vec::new();
    for r in &v {
        match out.last_mut() {
            // 与上一时段的「最后活动」(末条结束 或 末条开始, 取大) 间隔 ≤ gap → 并入
            Some(s) if r.start_ms - s.end_ms.max(s.start_ms) <= gap_ms => {
                s.end_ms = s.end_ms.max(r.end_ms);
                s.requests += 1;
                s.face_usd += r.face_usd;
            }
            _ => out.push(Session {
                start_ms: r.start_ms,
                end_ms: r.end_ms.max(r.start_ms),
                requests: 1,
                face_usd: r.face_usd,
            }),
        }
    }
    (out, gap)
}

/// 一个维度值 (某模型 / 某组 / 某套餐 / 某卡) 的聚合
#[derive(Debug, Default, Clone, Serialize)]
pub struct Agg {
    pub requests: u64,
    pub errors: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
    pub face_usd: f64,
    /// Σ latency (小时) — 上游真正在算的时间
    pub busy_hours: f64,
    /// 在线时段合计 (小时), 按 key 分流 sessionize 后求和
    pub online_hours: f64,
    pub sessions: u64,
    /// 参与的 key / 卡 数
    pub users: u64,
    pub first_ms: Option<i64>,
    pub last_ms: Option<i64>,
}

impl Agg {
    pub fn add_req(&mut self, r: &Req) {
        self.requests += 1;
        if !r.ok() {
            self.errors += 1;
        }
        self.input_tokens += r.input;
        self.output_tokens += r.output;
        self.cache_read_tokens += r.cache_read;
        self.cache_write_tokens += r.cache_write;
        self.face_usd += r.face_usd;
        self.busy_hours += r.latency_ms() as f64 / 3_600_000.0;
        self.first_ms = Some(self.first_ms.map_or(r.start_ms, |f| f.min(r.start_ms)));
        self.last_ms = Some(self.last_ms.map_or(r.end_ms, |l| l.max(r.end_ms)));
    }
    pub fn usd_per_online_hour(&self) -> Option<f64> {
        (self.online_hours > 1e-6).then(|| self.face_usd / self.online_hours)
    }
    pub fn usd_per_busy_hour(&self) -> Option<f64> {
        (self.busy_hours > 1e-6).then(|| self.face_usd / self.busy_hours)
    }
    pub fn avg_face_per_req(&self) -> f64 {
        if self.requests == 0 {
            0.0
        } else {
            self.face_usd / self.requests as f64
        }
    }
    pub fn to_json(&self, rmb_per_usd: f64) -> Value {
        json!({
            "requests": self.requests,
            "errors": self.errors,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cache_read_tokens": self.cache_read_tokens,
            "cache_write_tokens": self.cache_write_tokens,
            "face_usd": self.face_usd,
            "cost_rmb": self.face_usd * rmb_per_usd,
            "avg_face_per_req": self.avg_face_per_req(),
            "busy_hours": self.busy_hours,
            "online_hours": self.online_hours,
            "sessions": self.sessions,
            "users": self.users,
            "usd_per_online_hour": self.usd_per_online_hour(),
            "usd_per_busy_hour": self.usd_per_busy_hour(),
            "rmb_per_online_hour": self.usd_per_online_hour().map(|v| v * rmb_per_usd),
            "first_ms": self.first_ms,
            "last_ms": self.last_ms,
        })
    }
}

/// 按维度聚合. `dim(req)` 返回该请求归属的维度值 (一个请求可属多个组).
/// 每个维度值内部再按 key 分流做 sessionize, online_hours = Σ 各 key 的时段时长.
pub fn aggregate<F>(reqs: &[Req], dim: F, gap_secs: Option<f64>) -> BTreeMap<String, Agg>
where
    F: Fn(&Req) -> Vec<String>,
{
    // dim → key → reqs
    let mut buckets: BTreeMap<String, BTreeMap<String, Vec<Req>>> = BTreeMap::new();
    for r in reqs {
        for d in dim(r) {
            buckets
                .entry(d)
                .or_default()
                .entry(r.key_name.clone())
                .or_default()
                .push(r.clone());
        }
    }
    let mut out = BTreeMap::new();
    for (d, per_key) in buckets {
        let mut a = Agg::default();
        for (_k, rs) in &per_key {
            for r in rs {
                a.add_req(r);
            }
            let (sessions, _) = sessionize(rs, gap_secs);
            a.sessions += sessions.len() as u64;
            a.online_hours += sessions.iter().map(|s| s.secs()).sum::<f64>() / 3600.0;
        }
        a.users = per_key.len() as u64;
        out.insert(d, a);
    }
    out
}

/// 小时桶: 活跃用户数 / 请求 / 面值. 用于「谁在什么时候在线」的时间轴.
/// 活跃 = 该小时内有请求 (开始或结束落在桶内) 的 distinct key.
pub fn hourly(reqs: &[Req], tz_offset_minutes: i32) -> Vec<Value> {
    let off_ms = tz_offset_minutes as i64 * 60_000;
    let mut m: BTreeMap<i64, (std::collections::BTreeSet<String>, u64, f64)> = BTreeMap::new();
    for r in reqs {
        let h = (r.end_ms + off_ms).div_euclid(3_600_000);
        let e = m.entry(h).or_default();
        e.0.insert(r.key_name.clone());
        e.1 += 1;
        e.2 += r.face_usd;
    }
    m.into_iter()
        .map(|(h, (users, n, face))| {
            let local_ms = h * 3_600_000 - off_ms;
            json!({
                "hour_ms": local_ms,
                "hour": crate::billing::fmt_local(local_ms, tz_offset_minutes)
                    .chars().take(13).collect::<String>(),
                "active_users": users.len(),
                "requests": n,
                "face_usd": face,
            })
        })
        .collect()
}

/// 单 key 的在线状态判定
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Presence {
    /// in_flight > 0 或 最近 `online_window` 内有请求
    Online,
    /// 最近 30min 内有请求但已超 online_window
    Idle,
    Offline,
}

pub const IDLE_WINDOW_SECS: i64 = 1800;

pub fn presence(
    last_end_ms: Option<i64>,
    in_flight: u64,
    now_ms: i64,
    online_window_secs: i64,
) -> Presence {
    if in_flight > 0 {
        return Presence::Online;
    }
    match last_end_ms {
        Some(t) if now_ms - t <= online_window_secs * 1000 => Presence::Online,
        Some(t) if now_ms - t <= IDLE_WINDOW_SECS * 1000 => Presence::Idle,
        _ => Presence::Offline,
    }
}

/// 从账本行构造 Req (面值按当前官方价重算)
pub fn req_from_row(
    ts_ms: i64,
    latency_ms: i64,
    key_name: String,
    model: String,
    input: u64,
    output: u64,
    cache_read: u64,
    cache_write: u64,
    status: u16,
) -> Req {
    let face_usd = if input == 0 && output == 0 {
        0.0
    } else {
        crate::cards::estimate_quota_cost_full(&model, input, output, cache_read, cache_write)
    };
    Req {
        start_ms: ts_ms - latency_ms.max(0),
        end_ms: ts_ms,
        key_name,
        model,
        input,
        output,
        cache_read,
        cache_write,
        status,
        face_usd,
    }
}

/// 读账本时间窗内的请求 (按开始时间升序). `cards_only` 只取 card- 前缀.
pub fn load_reqs(
    conn: &rusqlite::Connection,
    from_ms: Option<i64>,
    to_ms: Option<i64>,
    cards_only: bool,
    key: Option<&str>,
    limit: usize,
) -> rusqlite::Result<Vec<Req>> {
    let mut sql = String::from(
        "SELECT ts_ms, latency_ms, key_name, model, input_tokens, output_tokens,
                cache_read_tokens, cache_write_tokens, status
         FROM billing_records WHERE 1=1",
    );
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    if let Some(f) = from_ms {
        sql.push_str(" AND ts_ms >= ?");
        params.push(Box::new(f));
    }
    if let Some(t) = to_ms {
        sql.push_str(" AND ts_ms <= ?");
        params.push(Box::new(t));
    }
    if cards_only {
        sql.push_str(" AND key_name LIKE 'card-%'");
    }
    if let Some(k) = key {
        sql.push_str(" AND key_name = ?");
        params.push(Box::new(k.to_string()));
    }
    sql.push_str(" ORDER BY ts_ms DESC LIMIT ?");
    params.push(Box::new(limit as i64));
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params.iter()), |r| {
        Ok(req_from_row(
            r.get(0)?,
            r.get(1)?,
            r.get(2)?,
            r.get(3)?,
            r.get::<_, i64>(4)?.max(0) as u64,
            r.get::<_, i64>(5)?.max(0) as u64,
            r.get::<_, i64>(6)?.max(0) as u64,
            r.get::<_, i64>(7)?.max(0) as u64,
            r.get::<_, i64>(8)?.clamp(0, 999) as u16,
        ))
    })?;
    let mut v: Vec<Req> = rows.flatten().collect();
    v.sort_by_key(|r| (r.start_ms, r.end_ms));
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(key: &str, model: &str, start_s: i64, lat_s: i64, face: f64) -> Req {
        Req {
            start_ms: start_s * 1000,
            end_ms: (start_s + lat_s) * 1000,
            key_name: key.into(),
            model: model.into(),
            input: 100,
            output: 10,
            cache_read: 0,
            cache_write: 0,
            status: 200,
            face_usd: face,
        }
    }

    #[test]
    fn sessionize_splits_on_gap_and_uses_start_time() {
        // 0-10s, 20-30s (gap 10s) → 同一段; 2000s 后另一段
        let reqs = vec![
            r("k", "m", 20, 10, 1.0),
            r("k", "m", 0, 10, 1.0),
            r("k", "m", 2000, 5, 2.0),
        ];
        let (s, gap) = sessionize(&reqs, Some(600.0));
        assert_eq!(gap, 600.0);
        assert_eq!(s.len(), 2);
        assert_eq!(s[0].start_ms, 0);
        assert_eq!(s[0].end_ms, 30_000);
        assert_eq!(s[0].requests, 2);
        assert_eq!(s[0].face_usd, 2.0);
        assert_eq!(s[1].start_ms, 2_000_000);
        assert_eq!(s[1].secs(), 5.0);
    }

    #[test]
    fn sessionize_gap_measured_from_previous_end() {
        // 长请求 0-500s, 下一条 900s 开始: 距结束 400s ≤ 600 → 同段 (虽然距开始 900s)
        let reqs = vec![r("k", "m", 0, 500, 1.0), r("k", "m", 900, 10, 1.0)];
        let (s, _) = sessionize(&reqs, Some(600.0));
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].end_ms, 910_000);
    }

    #[test]
    fn auto_gap_adapts_to_rhythm() {
        // 秒接 agent: 间隔 20s → 中位 20 × 3 = 60 → 夹到 5min
        let fast: Vec<Req> = (0..10).map(|i| r("k", "m", i * 20, 5, 0.1)).collect();
        assert_eq!(auto_gap_secs(&fast), GAP_MIN_SECS);
        // 慢聊人类: 间隔 8min → 24min
        let slow: Vec<Req> = (0..10).map(|i| r("k", "m", i * 480, 5, 0.1)).collect();
        assert!((auto_gap_secs(&slow) - 1440.0).abs() < 1e-6);
        // 间隔 20min → 60min 夹到 30min
        let vslow: Vec<Req> = (0..10).map(|i| r("k", "m", i * 1200, 5, 0.1)).collect();
        assert_eq!(auto_gap_secs(&vslow), GAP_MAX_SECS);
        // 样本不足 → 默认
        assert_eq!(auto_gap_secs(&fast[..2]), GAP_DEFAULT_SECS);
        // 全部 ≥ 1h 的间隔被过滤 → 样本不足 → 默认
        let sparse: Vec<Req> = (0..10).map(|i| r("k", "m", i * 7200, 5, 0.1)).collect();
        assert_eq!(auto_gap_secs(&sparse), GAP_DEFAULT_SECS);
    }

    #[test]
    fn aggregate_by_model_computes_online_and_busy_hours() {
        // 两个用户各用 m1 一小时 (每 5min 一条, 各 12 条, 每条 60s), m2 只有 a 用 1 条
        let mut reqs = vec![];
        for u in ["a", "b"] {
            for i in 0..12 {
                reqs.push(r(u, "m1", i * 300, 60, 0.5));
            }
        }
        reqs.push(r("a", "m2", 10_000, 30, 3.0));
        let agg = aggregate(&reqs, |q| vec![q.model.clone()], Some(600.0));
        let m1 = &agg["m1"];
        assert_eq!(m1.requests, 24);
        assert_eq!(m1.users, 2);
        assert_eq!(m1.sessions, 2);
        // 每用户: 0 → 11*300+60 = 3360s = 0.9333h; 两人合计 1.8667h
        assert!((m1.online_hours - 2.0 * 3360.0 / 3600.0).abs() < 1e-9);
        assert!((m1.busy_hours - 24.0 * 60.0 / 3600.0).abs() < 1e-9);
        assert!((m1.face_usd - 12.0).abs() < 1e-9);
        let per_h = m1.usd_per_online_hour().unwrap();
        assert!((per_h - 12.0 / (2.0 * 3360.0 / 3600.0)).abs() < 1e-9);
        let m2 = &agg["m2"];
        assert_eq!(m2.sessions, 1);
        assert!((m2.online_hours - 30.0 / 3600.0).abs() < 1e-12);
        assert_eq!(m2.usd_per_online_hour().map(|v| v.round()), Some(360.0));
    }

    #[test]
    fn aggregate_multi_dim_membership() {
        let reqs = vec![
            r("a", "kimi-k3-high", 0, 10, 1.0),
            r("a", "claude-opus-5", 100, 10, 5.0),
        ];
        let agg = aggregate(
            &reqs,
            |q| {
                let mut v = vec!["all".to_string()];
                if q.model.starts_with("kimi") {
                    v.push("eco".into());
                }
                v
            },
            Some(600.0),
        );
        assert_eq!(agg["all"].requests, 2);
        assert_eq!(agg["eco"].requests, 1);
        assert!((agg["all"].face_usd - 6.0).abs() < 1e-9);
    }

    #[test]
    fn presence_rules() {
        let now = 1_000_000_000_000i64;
        assert_eq!(presence(None, 1, now, 300), Presence::Online);
        assert_eq!(presence(None, 0, now, 300), Presence::Offline);
        assert_eq!(presence(Some(now - 200_000), 0, now, 300), Presence::Online);
        assert_eq!(presence(Some(now - 600_000), 0, now, 300), Presence::Idle);
        assert_eq!(
            presence(Some(now - 3_600_000), 0, now, 300),
            Presence::Offline
        );
    }

    #[test]
    fn hourly_buckets_count_distinct_users() {
        let reqs = vec![
            r("a", "m", 0, 10, 1.0),
            r("a", "m", 100, 10, 1.0),
            r("b", "m", 200, 10, 1.0),
            r("b", "m", 4000, 10, 2.0),
        ];
        let h = hourly(&reqs, 0);
        assert_eq!(h.len(), 2);
        assert_eq!(h[0]["active_users"], 2);
        assert_eq!(h[0]["requests"], 3);
        assert_eq!(h[1]["active_users"], 1);
        assert_eq!(h[1]["face_usd"], 2.0);
        assert_eq!(h[0]["hour"], "1970-01-01 00");
    }

    #[test]
    fn req_from_row_zero_usage_is_free() {
        let q = req_from_row(
            10_000,
            2_000,
            "k".into(),
            "kimi-k3-high".into(),
            0,
            0,
            0,
            0,
            502,
        );
        assert_eq!(q.face_usd, 0.0);
        assert_eq!(q.start_ms, 8_000);
        assert!(!q.ok());
        let q = req_from_row(
            10_000,
            2_000,
            "k".into(),
            "kimi-k3-high".into(),
            1000,
            100,
            0,
            0,
            200,
        );
        assert!(q.face_usd > 0.0);
    }
}
