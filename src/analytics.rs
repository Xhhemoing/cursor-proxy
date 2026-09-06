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
//! - **并发槽 (lane)**: 一个用户同时开 N 路请求 (agent 并行 / 多窗口), 就占了 N 个并发槽.
//!   按区间划分把同 key 的请求贪心塞进最少的槽 (槽数 = 瞬时最大并发), 每个槽再各自
//!   sessionize —— `lane_hours` = Σ 各槽的在线时长, `peak_concurrency` = 槽数.
//!   一个人单路用 1h = 1 槽·时; 4 路并行 1h = 4 槽·时. **模型消耗按槽·时归一化**:
//! - **$/槽·时 (`usd_per_lane_hour`)** = 面值 ÷ lane_hours —— 「一路并发开着这个模型,
//!   每小时烧多少面值」. 这是给套餐定价用的主指标: 套餐上限 = max_concurrency × 时长 × $/槽·时.
//!   `usd_per_online_hour` (面值 ÷ 用户在线小时, 不管几路) 保留作对照; 两者之比 = 平均并发.
//! - **忙时 (busy)**: Σ latency, 即上游真正在算的时间; lane_hours ≥ 忙时 (槽在等人时也算在线).
//!
//! - **速度/延迟**: `ttft_ms` 首字延迟 (流式: 首个内容帧; 老行/非流式 NULL), `output_tps` =
//!   输出 token ÷ (总延迟 − 首字延迟). 每维度给 p50/p90 (TTFT) 与 p50/p10 (tok/s) —— 用户体验用
//!   分位数看, 不用均值 (长尾请求会把均值拉飞).
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
    /// 首字延迟 (ms); 老行 / 非流式 / 失败 = None
    pub ttft_ms: Option<i64>,
    /// 网关限速目标 tok/s (0 = 未限)
    pub pace_tps: u32,
    /// 因限速累计 sleep (ms)
    pub pace_wait_ms: i64,
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
    /// 输出速度 tok/s (只对成功且有输出的请求有意义). 有 ttft 就扣掉等待时间.
    pub fn output_tps(&self) -> Option<f64> {
        if !self.ok() || self.output == 0 {
            return None;
        }
        let gen_ms = self.latency_ms() - self.ttft_ms.unwrap_or(0).max(0);
        (gen_ms > 0).then(|| self.output as f64 / (gen_ms as f64 / 1000.0))
    }
    /// 上游原生速度 (剔掉网关 sleep). 未限速时 = output_tps
    pub fn upstream_tps(&self) -> Option<f64> {
        if !self.ok() || self.output == 0 {
            return None;
        }
        let gen_ms =
            self.latency_ms() - self.ttft_ms.unwrap_or(0).max(0) - self.pace_wait_ms.max(0);
        (gen_ms > 0).then(|| self.output as f64 / (gen_ms as f64 / 1000.0))
    }
}

/// 分位数 (线性最近秩, 输入无需有序). 空 → None
pub fn percentile(v: &[f64], p: f64) -> Option<f64> {
    if v.is_empty() {
        return None;
    }
    let mut s: Vec<f64> = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((s.len() as f64 - 1.0) * p.clamp(0.0, 1.0)).round() as usize;
    Some(s[idx.min(s.len() - 1)])
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

/// 区间划分: 把一条 key 的请求按开始时间贪心分配到并发槽 (一个槽内请求不重叠).
/// 选「结束最晚但仍 ≤ 本条开始」的槽 (best-fit), 没有就开新槽. 槽数 = 瞬时最大并发.
pub fn assign_lanes(reqs: &[Req]) -> Vec<Vec<Req>> {
    let mut v: Vec<Req> = reqs.to_vec();
    v.sort_by_key(|r| (r.start_ms, r.end_ms));
    let mut lanes: Vec<Vec<Req>> = Vec::new();
    let mut lane_end: Vec<i64> = Vec::new();
    for r in v {
        let mut best: Option<usize> = None;
        for (i, e) in lane_end.iter().enumerate() {
            if *e <= r.start_ms && best.map_or(true, |b| lane_end[b] < *e) {
                best = Some(i);
            }
        }
        match best {
            Some(i) => {
                lane_end[i] = r.end_ms.max(r.start_ms);
                lanes[i].push(r);
            }
            None => {
                lane_end.push(r.end_ms.max(r.start_ms));
                lanes.push(vec![r]);
            }
        }
    }
    lanes
}

/// 一条 key 的请求流 → (在线秒, 段数, 槽·秒, 槽数). 槽用与整体相同的 gap 切时段.
pub fn key_online_and_lanes(reqs: &[Req], gap_secs: Option<f64>) -> (f64, usize, f64, usize) {
    let (sessions, gap) = sessionize(reqs, gap_secs);
    let online: f64 = sessions.iter().map(|s| s.secs()).sum();
    let lanes = assign_lanes(reqs);
    let lane_secs: f64 = lanes
        .iter()
        .map(|l| {
            sessionize(l, Some(gap))
                .0
                .iter()
                .map(|s| s.secs())
                .sum::<f64>()
        })
        .sum();
    (online, sessions.len(), lane_secs, lanes.len())
}

/// 限速 what-if: 对一条 key 的请求流, 若把输出匀速压到 `pace_tps`, 会话会被拉长多少.
///
/// 逐条: 交付时长 = max(原生成时长, output/pace) — 多出来的 `added` 只有在下一条请求「等着上一条结束
/// 才发」(agent 循环, 间隔 < FAST_FOLLOW) 时才会真的拉长会话; 人类思考间隔通常 > added, 会话总长不变.
/// 面值不变 (token 数没少), 槽·时变长 → $/槽·时 下降; 对「按小时卖」的畅饮卡等价于省钱.
/// 返回 (原槽·秒, 新槽·秒, 被拉长的请求数). 已限速的行先剔掉现有 sleep 再算.
pub fn simulate_pace(reqs: &[Req], pace_tps: f64, gap_secs: Option<f64>) -> (f64, f64, usize) {
    if pace_tps <= 0.0 || reqs.is_empty() {
        let (_, _, lane, _) = key_online_and_lanes(reqs, gap_secs);
        return (lane, lane, 0);
    }
    let mut v: Vec<Req> = reqs.to_vec();
    v.sort_by_key(|r| (r.start_ms, r.end_ms));
    let (_, _, lane_before, _) = key_online_and_lanes(&v, gap_secs);
    // 逐条重排: 请求 i 的新开始 = 原开始 + 之前累计的 shift (若它是秒接的); 新结束 = 新开始 + 新时长
    let mut shift_ms: i64 = 0;
    let mut stretched = 0usize;
    let mut prev_end_orig: Option<i64> = None;
    let mut out: Vec<Req> = Vec::with_capacity(v.len());
    for r in &v {
        let follows_fast = prev_end_orig
            .map(|pe| (r.start_ms - pe).abs() < FAST_FOLLOW_MS)
            .unwrap_or(false);
        let ttft = r.ttft_ms.unwrap_or(0).max(0);
        let native_gen = (r.latency_ms() - ttft - r.pace_wait_ms.max(0)).max(0);
        let paced_gen = if r.output > 0 {
            ((r.output as f64 / pace_tps) * 1000.0) as i64
        } else {
            0
        };
        let new_gen = native_gen.max(paced_gen);
        let added = new_gen - native_gen;
        let new_start = r.start_ms + if follows_fast { shift_ms } else { 0 };
        if !follows_fast {
            // 人类停顿吸收了之前的拉长
            shift_ms = 0;
        }
        let new_end = new_start + ttft + new_gen;
        if added > 0 && follows_fast {
            stretched += 1;
        }
        shift_ms += added;
        prev_end_orig = Some(r.end_ms);
        let mut q = r.clone();
        q.start_ms = new_start;
        q.end_ms = new_end;
        out.push(q);
    }
    let (_, _, lane_after, _) = key_online_and_lanes(&out, gap_secs);
    (lane_before, lane_after, stretched)
}

/// 秒接阈值 (ms): 下一条请求在上一条结束后多久内到达算 agent 循环
pub const FAST_FOLLOW_MS: i64 = 3000;

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
    /// 并发槽在线时长合计 (小时): Σ_key Σ_槽 该槽的时段时长. 单路使用时 = online_hours
    pub lane_hours: f64,
    /// 所有 key 中的最大瞬时并发 (槽数)
    pub peak_concurrency: u64,
    /// 参与的 key / 卡 数
    pub users: u64,
    pub first_ms: Option<i64>,
    pub last_ms: Option<i64>,
    /// 首字延迟样本 (ms, 成功且有 ttft 的请求)
    #[serde(skip)]
    pub ttft_samples: Vec<f64>,
    /// 输出速度样本 (tok/s, 成功且 output ≥ 50 的请求 — 太短的答复算不出稳定速度)
    #[serde(skip)]
    pub tps_samples: Vec<f64>,
    /// 端到端延迟样本 (ms, 成功请求)
    #[serde(skip)]
    pub latency_samples: Vec<f64>,
    /// 上游原生速度样本 (剔限速 sleep)
    #[serde(skip)]
    pub upstream_tps_samples: Vec<f64>,
    /// 被限速的请求数 / 累计 sleep 小时
    pub paced_requests: u64,
    pub pace_wait_hours: f64,
}

/// 速度样本最少输出 token 数
pub const TPS_MIN_OUTPUT: u64 = 50;

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
        if r.ok() {
            self.latency_samples.push(r.latency_ms() as f64);
            if let Some(t) = r.ttft_ms.filter(|t| *t >= 0) {
                self.ttft_samples.push(t as f64);
            }
            if r.output >= TPS_MIN_OUTPUT {
                if let Some(tps) = r.output_tps() {
                    self.tps_samples.push(tps);
                }
                if let Some(tps) = r.upstream_tps() {
                    self.upstream_tps_samples.push(tps);
                }
            }
            if r.pace_tps > 0 {
                self.paced_requests += 1;
            }
            self.pace_wait_hours += r.pace_wait_ms.max(0) as f64 / 3_600_000.0;
        }
    }
    /// 速度/延迟摘要
    pub fn speed_json(&self) -> Value {
        json!({
            "ttft_p50_ms": percentile(&self.ttft_samples, 0.5),
            "ttft_p90_ms": percentile(&self.ttft_samples, 0.9),
            "ttft_samples": self.ttft_samples.len(),
            "tps_p50": percentile(&self.tps_samples, 0.5),
            "tps_p10": percentile(&self.tps_samples, 0.1),
            "tps_p90": percentile(&self.tps_samples, 0.9),
            "tps_samples": self.tps_samples.len(),
            "latency_p50_ms": percentile(&self.latency_samples, 0.5),
            "latency_p90_ms": percentile(&self.latency_samples, 0.9),
            // 上游原生速度 (剔限速): 与 tps_p50 之差 = 我们压掉的
            "upstream_tps_p50": percentile(&self.upstream_tps_samples, 0.5),
            "paced_requests": self.paced_requests,
            "pace_wait_hours": self.pace_wait_hours,
            // 限速把槽·时拉长了多少 → 等效降低的 $/槽·时 (lane_hours 已含 sleep 时间)
            "pace_lane_share": if self.lane_hours > 1e-6 { Some((self.pace_wait_hours / self.lane_hours).min(1.0)) } else { None },
        })
    }
    pub fn usd_per_online_hour(&self) -> Option<f64> {
        (self.online_hours > 1e-6).then(|| self.face_usd / self.online_hours)
    }
    /// 主定价指标: 一路并发开着该维度, 每小时烧的面值
    pub fn usd_per_lane_hour(&self) -> Option<f64> {
        (self.lane_hours > 1e-6).then(|| self.face_usd / self.lane_hours)
    }
    /// 平均并发 = 槽·时 ÷ 在线时
    pub fn avg_concurrency(&self) -> Option<f64> {
        (self.online_hours > 1e-6).then(|| self.lane_hours / self.online_hours)
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
            "lane_hours": self.lane_hours,
            "peak_concurrency": self.peak_concurrency,
            "avg_concurrency": self.avg_concurrency(),
            "users": self.users,
            "usd_per_lane_hour": self.usd_per_lane_hour(),
            "rmb_per_lane_hour": self.usd_per_lane_hour().map(|v| v * rmb_per_usd),
            "usd_per_online_hour": self.usd_per_online_hour(),
            "usd_per_busy_hour": self.usd_per_busy_hour(),
            "rmb_per_online_hour": self.usd_per_online_hour().map(|v| v * rmb_per_usd),
            "first_ms": self.first_ms,
            "last_ms": self.last_ms,
            "speed": self.speed_json(),
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
            let (online_secs, n_sessions, lane_secs, n_lanes) = key_online_and_lanes(rs, gap_secs);
            a.sessions += n_sessions as u64;
            a.online_hours += online_secs / 3600.0;
            a.lane_hours += lane_secs / 3600.0;
            a.peak_concurrency = a.peak_concurrency.max(n_lanes as u64);
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
        ttft_ms: None,
        pace_tps: 0,
        pace_wait_ms: 0,
    }
}

/// 老账本行 (`input_incl_cache=1`) 的 input_tokens 是 Cursor promptTokens (含缓存) →
/// 扣掉缓存只留未命中部分, 与新行同口径; 否则缓存按 input 全价重复计, 面值高估 4–7 倍.
pub fn normalize_input(input: u64, cache_read: u64, cache_write: u64, incl_cache: bool) -> u64 {
    if incl_cache {
        input.saturating_sub(cache_read + cache_write)
    } else {
        input
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
                cache_read_tokens, cache_write_tokens, status, input_incl_cache, ttft_ms,
                pace_tps, pace_wait_ms
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
        let input = r.get::<_, i64>(4)?.max(0) as u64;
        let cr = r.get::<_, i64>(6)?.max(0) as u64;
        let cw = r.get::<_, i64>(7)?.max(0) as u64;
        let incl: i64 = r.get(9)?;
        let ttft: Option<i64> = r.get(10)?;
        let pace_tps: i64 = r.get(11)?;
        let pace_wait: i64 = r.get(12)?;
        let mut q = req_from_row(
            r.get(0)?,
            r.get(1)?,
            r.get(2)?,
            r.get(3)?,
            normalize_input(input, cr, cw, incl != 0),
            r.get::<_, i64>(5)?.max(0) as u64,
            cr,
            cw,
            r.get::<_, i64>(8)?.clamp(0, 999) as u16,
        );
        q.ttft_ms = ttft;
        q.pace_tps = pace_tps.max(0) as u32;
        q.pace_wait_ms = pace_wait.max(0);
        Ok(q)
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
            ttft_ms: None,
            pace_tps: 0,
            pace_wait_ms: 0,
        }
    }

    #[test]
    fn simulate_pace_stretches_agent_loops_not_humans() {
        // agent: 10 条串行秒接, 每条 5s 生成 500 tok (100 tok/s), 间隔 1s
        let mut agent = vec![];
        for i in 0..10 {
            let mut q = r("a", "m", i * 6, 5, 1.0);
            q.output = 500;
            agent.push(q);
        }
        let (before, after, n) = simulate_pace(&agent, 25.0, Some(600.0));
        // 25 tok/s → 每条 20s, 多 15s × 10 = 150s
        assert_eq!(n, 9); // 首条无前驱不算「拉长」, 但 shift 照样累计
        assert!(
            (after - before - 150.0).abs() < 1.0,
            "before {before} after {after}"
        );
        // 人类: 同样 10 条, 但间隔 120s → 拉长被停顿吸收, 会话只在最后一条多 15s
        let mut human = vec![];
        for i in 0..10 {
            let mut q = r("h", "m", i * 125, 5, 1.0);
            q.output = 500;
            human.push(q);
        }
        let (b2, a2, n2) = simulate_pace(&human, 25.0, Some(600.0));
        assert_eq!(n2, 0);
        assert!((a2 - b2 - 15.0).abs() < 1.0, "b {b2} a {a2}");
        // pace 高于原生速度 → 不变
        let (b3, a3, _) = simulate_pace(&agent, 500.0, Some(600.0));
        assert!((a3 - b3).abs() < 1e-6);
    }

    #[test]
    fn upstream_tps_removes_pace_sleep() {
        // 10s 请求 (ttft 2s), 输出 200, 其中 4s 是限速 sleep → 客户端 25 tok/s, 上游 50 tok/s
        let mut q = r("k", "m", 0, 10, 1.0);
        q.output = 200;
        q.ttft_ms = Some(2000);
        q.pace_tps = 25;
        q.pace_wait_ms = 4000;
        assert!((q.output_tps().unwrap() - 25.0).abs() < 1e-9);
        assert!((q.upstream_tps().unwrap() - 50.0).abs() < 1e-9);
        let agg = aggregate(&[q], |x| vec![x.model.clone()], Some(600.0));
        let sp = agg["m"].speed_json();
        assert_eq!(sp["paced_requests"], 1);
        assert!((sp["upstream_tps_p50"].as_f64().unwrap() - 50.0).abs() < 1e-9);
        // lane_hours = 10s, sleep 4s → 40%
        assert!((sp["pace_lane_share"].as_f64().unwrap() - 0.4).abs() < 1e-6);
    }

    #[test]
    fn speed_percentiles_and_tps_exclude_ttft() {
        assert_eq!(percentile(&[], 0.5), None);
        assert_eq!(percentile(&[5.0, 1.0, 3.0], 0.5), Some(3.0));
        assert_eq!(percentile(&[5.0, 1.0, 3.0], 0.0), Some(1.0));
        assert_eq!(percentile(&[5.0, 1.0, 3.0], 1.0), Some(5.0));
        // 10s 请求, 200 输出, ttft 2s → 25 tok/s; 无 ttft → 20 tok/s
        let mut q = r("k", "m", 0, 10, 1.0);
        q.output = 200;
        assert!((q.output_tps().unwrap() - 20.0).abs() < 1e-9);
        q.ttft_ms = Some(2000);
        assert!((q.output_tps().unwrap() - 25.0).abs() < 1e-9);
        // 失败 / 无输出 / 输出太短 → 不进样本
        let mut bad = q.clone();
        bad.status = 502;
        assert!(bad.output_tps().is_none());
        let mut short = q.clone();
        short.output = 10;
        let agg = aggregate(
            &[q.clone(), bad, short],
            |x| vec![x.model.clone()],
            Some(600.0),
        );
        let sp = agg["m"].speed_json();
        assert_eq!(sp["tps_samples"], 1);
        assert_eq!(sp["ttft_samples"], 2);
        assert!((sp["tps_p50"].as_f64().unwrap() - 25.0).abs() < 1e-9);
        assert_eq!(sp["ttft_p50_ms"], 2000.0);
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
    fn lanes_count_concurrency_and_lane_hours_exceed_online_hours() {
        // 单路: 3 条串行 → 1 槽, lane_hours == online_hours
        let serial = vec![
            r("k", "m", 0, 10, 1.0),
            r("k", "m", 20, 10, 1.0),
            r("k", "m", 40, 10, 1.0),
        ];
        assert_eq!(assign_lanes(&serial).len(), 1);
        let (on, n, lane, k) = key_online_and_lanes(&serial, Some(600.0));
        assert_eq!((n, k), (1, 1));
        assert!((on - lane).abs() < 1e-9);
        assert!((on - 50.0).abs() < 1e-9);

        // 4 路并行 1h (每路每 5min 一条 60s 请求, 起点错开 0/15/30/45s):
        // 用户在线 ≈ 1h, 槽·时 ≈ 4h, 峰值并发 4
        let mut par = vec![];
        for lane in 0..4i64 {
            for i in 0..12 {
                par.push(r("k", "m", i * 300 + lane * 15, 60, 0.25));
            }
        }
        let lanes = assign_lanes(&par);
        assert_eq!(lanes.len(), 4);
        assert!(lanes.iter().all(|l| l.len() == 12));
        // 每槽内请求不重叠
        for l in &lanes {
            for w in l.windows(2) {
                assert!(w[1].start_ms >= w[0].end_ms);
            }
        }
        let (on, _, lane_secs, k) = key_online_and_lanes(&par, Some(600.0));
        assert_eq!(k, 4);
        assert!((on - 3405.0).abs() < 1e-9); // 0 → 11*300+45+60
        assert!(lane_secs > 3.9 * on && lane_secs < 4.0 * on + 1.0);

        let agg = aggregate(&par, |q| vec![q.model.clone()], Some(600.0));
        let m = &agg["m"];
        assert_eq!(m.peak_concurrency, 4);
        assert!((m.avg_concurrency().unwrap() - 3.95).abs() < 0.06);
        // 面值 12; $/槽·时 ≈ 12/3.78 ≈ 3.2, $/在线时 ≈ 12/0.946 ≈ 12.7
        let per_lane = m.usd_per_lane_hour().unwrap();
        let per_online = m.usd_per_online_hour().unwrap();
        assert!((per_online / per_lane - m.avg_concurrency().unwrap()).abs() < 1e-9);
        assert!(per_lane < 3.3 && per_lane > 3.1);
    }

    #[test]
    fn lane_best_fit_reuses_freed_lane() {
        // A 0-100, B 10-20 (并发 → 槽2), C 30-40 应回到槽2 (槽1 到 100 才空)
        let reqs = vec![
            r("k", "m", 0, 100, 1.0),
            r("k", "m", 10, 10, 1.0),
            r("k", "m", 30, 10, 1.0),
        ];
        let lanes = assign_lanes(&reqs);
        assert_eq!(lanes.len(), 2);
        assert_eq!(lanes[1].len(), 2);
    }

    #[test]
    fn normalize_input_only_for_old_rows() {
        assert_eq!(normalize_input(208_667, 208_204, 460, true), 3);
        assert_eq!(normalize_input(3, 208_204, 460, false), 3);
        assert_eq!(normalize_input(100, 500, 0, true), 0); // 不为负
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
