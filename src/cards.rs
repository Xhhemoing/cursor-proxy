//! 套餐卡系统: 按时间 + 并发限制计费的「无限调用」卡。
//!
//! 与 token 计费并行: 卡不替代 ApiKey，而是挂在现有 key 上的「通行证」，
//! 也可以独立发卡 (card_key 直接当 api key 用)。
//!
//! 亏本兜底: 每张卡带 fair_use_rpd (每日请求软上限)。超过后不停机，
//! 而是并发降到 degraded_concurrency，把单卡对上游号池的挤压锁死在低车道。

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// 今日起点 (unix 秒), 按 tz_offset_minutes 对齐自然日
pub fn day_start(now: u64, tz_offset_minutes: i32) -> u64 {
    let local = now as i64 + (tz_offset_minutes as i64) * 60;
    let day = local / 86400;
    (day * 86400 - (tz_offset_minutes as i64) * 60) as u64
}

// ─────────────────────────── 模型价格 (算额度消耗, 官方口径) ───────────────────────────

/// 高级额度价格表 ($/1M tokens): (input, output, cache_read, cache_write).
///
/// 2026-09-05 用 Cursor 官方仪表盘 `GetAggregatedUsageEvents` 按模型对账校准
/// (4 个号 × 周期内全部模型, 拟合误差 ≤5%):
/// - fable/opus/sol/grok/kimi 全部命中列表价 (ratio 1.00)
/// - `*-fast` 变体 = 基础价 × 2 (opus-5-thinking-xhigh-fast 实测恰好 2.00×)
/// - kimi-k3 官方 $3/$15/$0.3, 不是 Python pricing.json 的 $20/$100 (高估 6 倍)
/// 匹配: 精确 > 最长前缀 > 兜底; `-fast` 后缀单独乘 2.
static PREMIUM_PRICES: &[(&str, (f64, f64, f64, f64))] = &[
    ("gpt-5.4-pro", (30.00, 180.00, 3.00, 0.00)),
    ("gpt-5.6-cyber", (12.50, 75.00, 1.25, 15.62)),
    ("claude-fable-5", (10.00, 50.00, 1.00, 12.50)),
    ("fable-5", (10.00, 50.00, 1.00, 12.50)),
    ("claude-opus-5", (5.00, 25.00, 0.50, 6.25)),
    ("claude-opus-4", (5.00, 25.00, 0.50, 6.25)),
    ("opus-5", (5.00, 25.00, 0.50, 6.25)),
    ("gpt-5.6-sol", (4.00, 20.00, 0.40, 5.00)),
    ("gpt-5.6", (4.00, 20.00, 0.40, 5.00)),
    ("gpt-5.6-terra", (2.00, 12.00, 0.20, 2.50)),
    ("gpt-5.6-luna", (0.20, 1.20, 0.02, 0.25)),
    ("claude-sonnet-4", (3.00, 15.00, 0.30, 3.75)),
    ("claude-sonnet-5", (2.00, 10.00, 0.20, 2.50)),
    ("sonnet-5", (2.00, 10.00, 0.20, 2.50)),
    ("kimi-k3", (3.00, 15.00, 0.30, 0.00)),
    ("kimi-k2", (0.95, 4.00, 0.19, 0.00)),
    ("gpt-5.4", (2.50, 15.00, 0.25, 0.00)),
    ("gpt-5", (2.50, 15.00, 0.25, 0.00)),
    ("claude-4.5-haiku", (1.00, 5.00, 0.10, 1.25)),
    ("gemini-3", (0.75, 3.75, 0.07, 0.00)),
    ("glm-5", (1.40, 4.40, 0.28, 0.00)),
    ("grok-4.5", (2.00, 6.00, 0.30, 0.00)),
    ("grok-4.6", (2.00, 6.00, 0.50, 0.00)),
    ("cursor-grok-4.6", (2.00, 6.00, 0.50, 0.00)),
];
/// 内置表导出 (面板「恢复默认」/「导入内置」用): (model, in, out, cr, cw, tier)
pub fn builtin_table() -> Vec<(String, f64, f64, f64, f64, &'static str)> {
    PREMIUM_PRICES
        .iter()
        .map(|(m, (i, o, c, w))| (m.to_string(), *i, *o, *c, *w, builtin_model_tier(m)))
        .collect()
}

/// 兜底价 (未知模型按 opus 档)
const FALLBACK_PRICE: (f64, f64, f64, f64) = (5.0, 25.0, 0.5, 6.25);

/// 查模型价格: 注册表手动定价优先 (models.json, 面板可改), 否则内置官方表.
///
/// `-fast` 倍率规则 (B2 修复, 2026-09-05): 注册表 `lookup` 是最长前缀匹配, 只要面板录过
/// `claude-opus-5`, 请求 `claude-opus-5-fast` 也会命中它 —— 之前直接原值返回, fast 按半价记账.
/// 现在: 请求是 `-fast` 而命中的注册表条目本身不是 `-fast` 名 → 补 ×2 (官方 fast 变体统一 ×2);
/// 注册表条目本身就是 `-fast` 名 (面板专门给 fast 定价) → 所见即所得, 不再乘.
pub fn model_price(model: &str) -> (f64, f64, f64, f64) {
    if let Some(e) = crate::models::registry().lookup(model) {
        let mut p = (
            e.input_per_m,
            e.output_per_m,
            e.cache_read_per_m,
            e.cache_write_per_m,
        );
        if model.ends_with("-fast") && !e.model.ends_with("-fast") {
            p = (p.0 * 2.0, p.1 * 2.0, p.2 * 2.0, p.3 * 2.0);
        }
        return p;
    }
    builtin_model_price(model)
}

/// 内置官方价格表查询 (精确 > 最长前缀 > 兜底), `-fast` 变体 ×2
pub fn builtin_model_price(model: &str) -> (f64, f64, f64, f64) {
    let fast = model.ends_with("-fast");
    let base = model.strip_suffix("-fast").unwrap_or(model);
    let mut p = if let Some((_, p)) = PREMIUM_PRICES.iter().find(|(n, _)| *n == base) {
        *p
    } else {
        let mut best: Option<&(f64, f64, f64, f64)> = None;
        let mut best_len = 0;
        for (name, p) in PREMIUM_PRICES {
            if base.starts_with(name) && name.len() > best_len {
                best = Some(p);
                best_len = name.len();
            }
        }
        best.copied().unwrap_or(FALLBACK_PRICE)
    };
    if fast {
        p = (p.0 * 2.0, p.1 * 2.0, p.2 * 2.0, p.3 * 2.0);
    }
    p
}

/// 估算单请求烧掉的高级额度 (美元, 官方口径). token 数为 0 时用保守默认 (in=20k out=4k cr=30k).
pub fn estimate_quota_cost(
    model: &str,
    input_tok: u64,
    output_tok: u64,
    cache_read_tok: u64,
) -> f64 {
    estimate_quota_cost_full(model, input_tok, output_tok, cache_read_tok, 0)
}

pub fn estimate_quota_cost_full(
    model: &str,
    input_tok: u64,
    output_tok: u64,
    cache_read_tok: u64,
    cache_write_tok: u64,
) -> f64 {
    let (i, o, c, w) = model_price(model);
    let (inp, out, cr, cw) = if input_tok == 0 && output_tok == 0 {
        (20_000.0, 4_000.0, 30_000.0, 0.0)
    } else {
        (
            input_tok as f64,
            output_tok as f64,
            cache_read_tok as f64,
            cache_write_tok as f64,
        )
    };
    (inp * i + out * o + cr * c + cw * w) / 1_000_000.0
}

// ─────────────────────────── 成本模型 (利润报表用) ───────────────────────────

/// B1: 估算请求体的输入 token 数 (定额卡预扣用). 递归遍历所有字符串值
/// (messages / system / tools / input …), CJK ≈ 1 tok/字, ASCII ≈ 0.25 tok/字,
/// 与 `TokenPacer::estimate_tokens` 同一口径; 另加每条 message ~4 tok 结构开销.
pub fn estimate_request_input_tokens(body: &Value) -> u64 {
    fn walk(v: &Value, est: &mut f64) {
        match v {
            Value::String(s) => {
                for c in s.chars() {
                    *est += if c.is_ascii() { 0.25 } else { 1.0 };
                }
            }
            Value::Array(a) => {
                for x in a {
                    walk(x, est);
                }
            }
            Value::Object(o) => {
                *est += 4.0;
                for (_, x) in o {
                    walk(x, est);
                }
            }
            _ => {}
        }
    }
    let mut est = 0.0f64;
    // 只数会送上游的内容字段; 跳过 model/stream/temperature 等标量本来就不计
    for key in ["messages", "system", "tools", "input", "instructions", "prompt"] {
        if let Some(v) = body.get(key) {
            walk(v, &mut est);
        }
    }
    est.round() as u64
}

/// 订阅成本模型: 把「烧掉的官方面值 $」折成真实人民币成本.
/// Grok Heavy: ¥130/号, 高级额度 $1000/周重置, 号只用 2 周 → ¥130 / $2000 = ¥0.065/面值$.
/// 可在 admin 调整 (号价涨/用满 4 周 等).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostModel {
    /// 单号采购价 (元)
    pub account_price_rmb: f64,
    /// 每周高级额度面值 ($)
    pub weekly_quota_usd: f64,
    /// 每号实际能用的周数
    pub usable_weeks: f64,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            account_price_rmb: 130.0,
            weekly_quota_usd: 1000.0,
            usable_weeks: 2.0,
        }
    }
}

impl CostModel {
    /// ¥ / 面值 $
    pub fn rmb_per_usd(&self) -> f64 {
        let face = self.weekly_quota_usd * self.usable_weeks;
        if face <= 0.0 {
            0.0
        } else {
            self.account_price_rmb / face
        }
    }
    pub fn cost_rmb(&self, usd: f64) -> f64 {
        usd * self.rmb_per_usd()
    }
}

// ─────────────────────────── 模型分层 ───────────────────────────

/// 模型层级 (按 1 并发 $/h 烧速划分, 不按单次价格):
/// economy ≤ $12/h · standard ≤ $24/h · flagship 其余.
/// fable-5-1-thinking-max 单次最贵但周期 186s, 匀速下只 $10.8/h —— 所以层级要看烧速.
pub const TIER_ECONOMY: &str = "economy";
pub const TIER_STANDARD: &str = "standard";
pub const TIER_FLAGSHIP: &str = "flagship";

/// 默认层级表: (模型前缀, 层级). 最长前缀优先.
static MODEL_TIERS: &[(&str, &str)] = &[
    ("claude-fable-5-1-thinking", TIER_FLAGSHIP),
    ("claude-fable-5-thinking", TIER_FLAGSHIP),
    ("gpt-5.4-pro", TIER_FLAGSHIP),
    ("gpt-5.6-cyber", TIER_FLAGSHIP),
    ("claude-fable", TIER_STANDARD),
    ("fable", TIER_STANDARD),
    ("kimi-k3-max", TIER_STANDARD),
    ("claude-opus-5-fast", TIER_STANDARD),
    ("claude-opus-5-thinking-xhigh-fast", TIER_STANDARD),
    ("claude-opus", TIER_ECONOMY),
    ("opus", TIER_ECONOMY),
    ("gpt-5.6-sol", TIER_ECONOMY),
    ("gpt-5.6", TIER_ECONOMY),
    ("gpt-5", TIER_ECONOMY),
    ("kimi", TIER_ECONOMY),
    ("grok", TIER_ECONOMY),
    ("cursor-grok", TIER_ECONOMY),
    ("claude-sonnet", TIER_ECONOMY),
    ("sonnet", TIER_ECONOMY),
    ("gemini", TIER_ECONOMY),
    ("glm", TIER_ECONOMY),
];

/// 模型层级: 注册表手动设定优先, 否则内置前缀规则
pub fn model_tier(model: &str) -> &'static str {
    if let Some(e) = crate::models::registry().lookup(model) {
        match e.tier.as_str() {
            TIER_ECONOMY => return TIER_ECONOMY,
            TIER_STANDARD => return TIER_STANDARD,
            TIER_FLAGSHIP => return TIER_FLAGSHIP,
            _ => {}
        }
    }
    builtin_model_tier(model)
}

pub fn builtin_model_tier(model: &str) -> &'static str {
    let mut best: Option<&'static str> = None;
    let mut best_len = 0;
    for (prefix, tier) in MODEL_TIERS {
        if model.starts_with(prefix) && prefix.len() > best_len {
            best = Some(tier);
            best_len = prefix.len();
        }
    }
    // 未知模型按旗舰算 (保守: 宁可让便宜卡用不了, 不让贵模型漏进便宜卡)
    best.unwrap_or(TIER_FLAGSHIP)
}

fn tier_rank(tier: &str) -> u8 {
    match tier {
        TIER_ECONOMY => 0,
        TIER_STANDARD => 1,
        _ => 2,
    }
}

/// 卡层级是否允许该模型: 卡 tier ≥ 模型 tier
pub fn tier_allows(card_tier: &str, model: &str) -> bool {
    tier_rank(card_tier) >= tier_rank(model_tier(model))
}

// ─────────────────────────── 套餐模板 ───────────────────────────

/// 套餐类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanKind {
    /// 无限畅饮: 时长内不限次数, 靠并发 + 匀速流出 + 行为评分控成本
    Unlimited,
    /// 定额卡: 面值 face_usd (官方口径 $), 用完即 402; 有效期到 duration_hours
    Quota,
}

impl Default for PlanKind {
    fn default() -> Self {
        PlanKind::Unlimited
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CardPlan {
    pub id: String,
    /// 展示名, 如 "畅饮·标准 1并发"
    pub name: String,
    /// 售价 (元), 用于利润报表; 实际收款在系统外
    pub price: f64,
    /// 套餐类型
    #[serde(default)]
    pub kind: PlanKind,
    /// 定额卡面值 (官方口径 $). kind=Quota 时必填
    #[serde(default)]
    pub face_usd: f64,
    /// 模型层级: economy / standard / flagship (卡 tier ≥ 模型 tier 才放行). 空 = 不按层级限
    #[serde(default)]
    pub tier: String,
    /// 有效期 (小时), 从开卡时刻计
    pub duration_hours: u64,
    /// 正常并发上限
    pub max_concurrency: u32,
    /// 每分钟请求数上限 (RPM), 防脚本瞬间打满
    pub rpm_limit: u32,
    /// 公平使用帽: 每日请求软上限, 超过进压制档. 0 = 不按次数
    pub fair_use_rpd: u32,
    /// 压制档并发上限
    #[serde(default = "default_degraded_concurrency")]
    pub degraded_concurrency: u32,
    /// 每日高级额度预算 (官方口径 $). 超过进压制档. 0 = 不按额度
    #[serde(default)]
    pub daily_quota_usd: f64,
    /// 软化起点 (0.7 = 负载到 70% 进软化档)
    #[serde(default = "default_soften_ratio")]
    pub soften_ratio: f64,
    /// 匀速流出 tok/s — 正常档. 0 = 不限速 (直通上游 34–48 tok/s)
    #[serde(default = "default_pace_normal")]
    pub pace_normal_tps: u32,
    /// 匀速流出 tok/s — 软化档
    #[serde(default = "default_pace_soften")]
    pub pace_soften_tps: u32,
    /// 匀速流出 tok/s — 压制档 (仍 ≥ 人类阅读速度 ~6 tok/s, 无感)
    #[serde(default = "default_pace_degraded")]
    pub pace_degraded_tps: u32,
    /// 行为评分阈值: ≥ 此分进压制档 (0 = 关闭行为评分)
    #[serde(default = "default_abuse_threshold")]
    pub abuse_score_threshold: u32,
    /// 限定的模型前缀 (空 = 不限); 与 tier 同时生效 (都要过)
    #[serde(default)]
    pub model_prefixes: Vec<String>,
    /// 允许访问的模型组 id (models.json groups); 空 = 不限. 与 tier / model_prefixes 同时生效 (都要过)
    #[serde(default)]
    pub model_groups: Vec<String>,
    #[serde(default)]
    pub note: String,
    /// 是否启用
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_degraded_concurrency() -> u32 {
    1
}
fn default_soften_ratio() -> f64 {
    0.7
}
fn default_pace_normal() -> u32 {
    0 // 0 = 不限速 (用户要求: 一开始不限速, 需要时按套餐单独开)
}
fn default_pace_soften() -> u32 {
    0
}
fn default_pace_degraded() -> u32 {
    0
}
fn default_abuse_threshold() -> u32 {
    55
}
fn default_true() -> bool {
    true
}

impl Default for CardPlan {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: String::new(),
            price: 0.0,
            kind: PlanKind::Unlimited,
            face_usd: 0.0,
            tier: String::new(),
            duration_hours: 24,
            max_concurrency: 1,
            rpm_limit: 30,
            fair_use_rpd: 0,
            degraded_concurrency: 1,
            daily_quota_usd: 0.0,
            soften_ratio: 0.7,
            pace_normal_tps: default_pace_normal(),
            pace_soften_tps: default_pace_soften(),
            pace_degraded_tps: default_pace_degraded(),
            abuse_score_threshold: default_abuse_threshold(),
            model_prefixes: vec![],
            model_groups: vec![],
            note: String::new(),
            enabled: true,
        }
    }
}

impl CardPlan {
    /// 预置套餐 (2026-09-05 定价, 见 docs/day-card-pricing-20260905.md §4d/§4e)
    pub fn presets() -> Vec<CardPlan> {
        let unlimited = |id: &str, name: &str, price: f64, tier: &str, conc: u32| CardPlan {
            id: id.into(),
            name: name.into(),
            price,
            kind: PlanKind::Unlimited,
            tier: tier.into(),
            duration_hours: 24,
            max_concurrency: conc,
            rpm_limit: 30 * conc,
            note: "无限畅饮: 24h 不限次, 不限速 (pace_* 全 0); 行为评分 ≥55 仅降并发".into(),
            ..CardPlan::default()
        };
        let quota = |id: &str, name: &str, price: f64, face: f64| CardPlan {
            id: id.into(),
            name: name.into(),
            price,
            kind: PlanKind::Quota,
            face_usd: face,
            tier: String::new(),
            duration_hours: 24 * 7,
            max_concurrency: 4,
            rpm_limit: 120,
            pace_normal_tps: 0,
            abuse_score_threshold: 0,
            note: "定额卡: 官方口径面值, 7 天有效, 用完即止, 不限速".into(),
            ..CardPlan::default()
        };
        vec![
            unlimited("day-eco-1", "畅饮·经济 1并发", 19.9, TIER_ECONOMY, 1),
            unlimited("day-std-1", "畅饮·标准 1并发", 34.9, TIER_STANDARD, 1),
            unlimited("day-pro-1", "畅饮·旗舰 1并发", 49.9, TIER_FLAGSHIP, 1),
            unlimited("day-eco-2", "畅饮·经济 2并发", 35.9, TIER_ECONOMY, 2),
            unlimited("day-std-2", "畅饮·标准 2并发", 62.9, TIER_STANDARD, 2),
            unlimited("day-pro-2", "畅饮·旗舰 2并发", 89.9, TIER_FLAGSHIP, 2),
            quota("quota-50", "定额 $50", 15.0, 50.0),
            quota("quota-200", "定额 $200", 49.0, 200.0),
            quota("quota-500", "定额 $500", 99.0, 500.0),
            quota("quota-1000", "定额 $1000", 169.0, 1000.0),
        ]
    }
}

// ─────────────────────────── 卡实例 ───────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Card {
    /// 卡 key (客户端当 Bearer api key 用)
    pub card_key: String,
    /// 关联套餐
    pub plan_id: String,
    /// 归属标记 (客户名/订单号)
    #[serde(default)]
    pub owner: String,
    /// 开卡时间 (unix 秒)
    pub issued_at: u64,
    /// 到期时间 (unix 秒)
    pub expires_at: u64,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 实收价 (元). 开卡时从套餐 price 快照; 可在开卡时覆盖 (促销/议价). 利润报表用
    #[serde(default)]
    pub paid_rmb: f64,
    /// 定额卡已用面值 (micro-$, 持久化 — 重启不能清零余额)
    #[serde(default)]
    pub face_used_micro: u64,
}

impl Card {
    pub fn is_expired(&self, now: u64) -> bool {
        now >= self.expires_at
    }
    pub fn remaining_secs(&self, now: u64) -> u64 {
        self.expires_at.saturating_sub(now)
    }
}

// ─────────────────────────── 行为评分 (脚本识别) ───────────────────────────

/// 5 分钟格数 / 天
const SLOTS_PER_DAY: usize = 288;
/// 「秒接」阈值: 上一条响应结束到下一条请求到达 < 3s
const FAST_FOLLOW_SECS: u64 = 3;
/// 「无停顿」阈值: 全天最长空闲 < 30min
const NO_BREAK_SECS: u64 = 1800;

/// 每张卡的行为信号 (内存, 按自然日滚动). 全部 atomic, 请求路径无锁.
pub struct BehaviorSignals {
    /// 活跃 5 分钟格位图 (288 bit → 5 × u64)
    pub slots: [AtomicU64; 5],
    /// 上一条请求结束时间 (unix 秒). 0 = 无
    pub last_done_at: AtomicU64,
    /// 秒接次数 / 总有间隔次数
    pub fast_follow: AtomicU64,
    pub follow_total: AtomicU64,
    /// 今日最长空闲 (秒)
    pub max_idle: AtomicU64,
    /// 首条请求时间 (unix 秒)
    pub first_at: AtomicU64,
}

impl BehaviorSignals {
    fn new() -> Self {
        Self {
            slots: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
            last_done_at: AtomicU64::new(0),
            fast_follow: AtomicU64::new(0),
            follow_total: AtomicU64::new(0),
            max_idle: AtomicU64::new(0),
            first_at: AtomicU64::new(0),
        }
    }
    fn reset(&self) {
        for s in &self.slots {
            s.store(0, Ordering::Relaxed);
        }
        self.last_done_at.store(0, Ordering::Relaxed);
        self.fast_follow.store(0, Ordering::Relaxed);
        self.follow_total.store(0, Ordering::Relaxed);
        self.max_idle.store(0, Ordering::Relaxed);
        self.first_at.store(0, Ordering::Relaxed);
    }
    /// 请求到达
    fn on_arrive(&self, now: u64, day_start: u64) {
        let slot = ((now.saturating_sub(day_start)) / 300) as usize;
        if slot < SLOTS_PER_DAY {
            self.slots[slot / 64].fetch_or(1u64 << (slot % 64), Ordering::Relaxed);
        }
        let _ = self
            .first_at
            .compare_exchange(0, now, Ordering::Relaxed, Ordering::Relaxed);
        let last = self.last_done_at.load(Ordering::Relaxed);
        if last > 0 && now >= last {
            let gap = now - last;
            self.follow_total.fetch_add(1, Ordering::Relaxed);
            if gap < FAST_FOLLOW_SECS {
                self.fast_follow.fetch_add(1, Ordering::Relaxed);
            }
            self.max_idle.fetch_max(gap, Ordering::Relaxed);
        }
    }
    /// 请求结束
    fn on_done(&self, now: u64) {
        self.last_done_at.fetch_max(now, Ordering::Relaxed);
    }
    pub fn active_slots(&self) -> u32 {
        self.slots
            .iter()
            .map(|s| s.load(Ordering::Relaxed).count_ones())
            .sum()
    }
    pub fn fast_follow_ratio(&self) -> f64 {
        let t = self.follow_total.load(Ordering::Relaxed);
        if t < 10 {
            0.0
        } else {
            self.fast_follow.load(Ordering::Relaxed) as f64 / t as f64
        }
    }
    /// 活跃跨度 (小时): 首条到当前
    pub fn span_hours(&self, now: u64) -> f64 {
        let f = self.first_at.load(Ordering::Relaxed);
        if f == 0 {
            0.0
        } else {
            now.saturating_sub(f) as f64 / 3600.0
        }
    }
}

/// 行为评分 (0–100). 用 8.6 天 / 61k 条真实流量校准:
/// 个人用户全天 0–25 分; 二道贩子脚本 55–85 分.
/// 信号: 活跃格数(30) + 秒接率(30) + 长时无停顿(25) + 日消耗(15)
#[derive(Debug, Clone, Serialize)]
pub struct AbuseScore {
    pub score: u32,
    pub active_slots: u32,
    pub fast_follow_ratio: f64,
    pub span_hours: f64,
    pub max_idle_secs: u64,
    pub day_quota_usd: f64,
    pub reasons: Vec<String>,
}

pub fn abuse_score(sig: &BehaviorSignals, day_quota_usd: f64, now: u64) -> AbuseScore {
    let slots = sig.active_slots();
    let fast = sig.fast_follow_ratio();
    let span = sig.span_hours(now);
    let idle = sig.max_idle.load(Ordering::Relaxed);
    let mut score = 0u32;
    let mut reasons = Vec::new();
    if slots >= 200 {
        score += 30;
        reasons.push(format!("活跃 {}/288 格", slots));
    } else if slots >= 150 {
        score += 15;
        reasons.push(format!("活跃 {} 格", slots));
    }
    if fast >= 0.40 {
        score += 30;
        reasons.push(format!("秒接 {:.0}%", fast * 100.0));
    } else if fast >= 0.25 {
        score += 15;
        reasons.push(format!("秒接 {:.0}%", fast * 100.0));
    }
    if span >= 20.0 && idle < NO_BREAK_SECS && slots >= 100 {
        score += 25;
        reasons.push("20h+ 无 30min 停顿".into());
    } else if span >= 18.0 && slots >= 100 {
        score += 10;
        reasons.push(format!("{:.0}h 活跃", span));
    }
    if day_quota_usd >= 250.0 {
        score += 15;
        reasons.push(format!("日消耗 ${:.0}", day_quota_usd));
    }
    AbuseScore {
        score,
        active_slots: slots,
        fast_follow_ratio: fast,
        span_hours: span,
        max_idle_secs: idle,
        day_quota_usd,
        reasons,
    }
}

// ─────────────────────────── 运行时状态 ───────────────────────────

/// 每张卡的运行时计数 (内存, 重启清零今日计数 — 按自然日重置正好)
pub struct CardRuntime {
    /// 当前并发占用数
    pub in_flight: AtomicU64,
    /// 今日已用请求数 (按自然日重置)
    pub day_count: AtomicU64,
    /// 今日已烧高级额度 (美元 × 1e6, 用 atomic u64 存 micro-dollar)
    pub day_quota_micro: AtomicU64,
    /// 今日计数对应的自然日起点
    pub day_start: AtomicU64,
    /// 最近一分钟请求时间戳环形缓冲 (算 RPM)
    pub rpm: RpmBucket,
    /// 行为信号
    pub behavior: BehaviorSignals,
    /// B8: 并发车道释放通知 — 等待者 FIFO 排队而非自旋/立即 429
    pub lane_freed: tokio::sync::Notify,
    /// B7: 每卡共享的匀速器 — 该卡所有并发流喂同一个桶, 卡总输出 ≤ pace_tps
    /// (之前每请求各建一个桶, N 并发实际放出 N × pace, 限速形同虚设)
    pub pacer: std::sync::Mutex<Option<TokenPacer>>,
    /// B1: 定额卡在途预扣 (micro-$): 已 acquire 未 settle 的请求按估算预占余额,
    /// 防止并发 N 个大请求同时通过「余额 > 0」检查后集体透支
    pub hold_micro: AtomicU64,
}

/// 简易 RPM 桶: 固定窗口 60s
pub struct RpmBucket {
    pub window_start: AtomicU64,
    pub count: AtomicU64,
}

impl RpmBucket {
    pub fn new() -> Self {
        Self {
            window_start: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }
    /// 返回本次请求后当前窗口计数
    pub fn tick(&self, now: u64) -> u64 {
        let window = now / 60;
        let cur = self.window_start.load(Ordering::Relaxed);
        if cur != window {
            if self
                .window_start
                .compare_exchange(cur, window, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                self.count.store(0, Ordering::Relaxed);
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed) + 1
    }
    pub fn current(&self, now: u64) -> u64 {
        let window = now / 60;
        if self.window_start.load(Ordering::Relaxed) != window {
            0
        } else {
            self.count.load(Ordering::Relaxed)
        }
    }
}

// ─────────────────────────── 卡存储 ───────────────────────────

pub struct CardStore {
    plans: DashMap<String, CardPlan>,
    cards: DashMap<String, Card>,
    runtimes: DashMap<String, Arc<CardRuntime>>,
    cost_model: std::sync::RwLock<CostModel>,
    tz_offset_minutes: i32,
    path: std::path::PathBuf,
    /// B4: 定额卡余额账本 (cards.db, SQLite WAL). 热路径只写这一行, 不再每请求全量重写 cards.json.
    /// None = 打不开 DB (只读文件系统等), 退回 cards.json 全量写 (慢但不丢账).
    ledger: Option<std::sync::Mutex<rusqlite::Connection>>,
    /// 测试用: 覆盖 B8 排队超时
    lane_wait_override: Option<std::time::Duration>,
}

impl CardStore {
    pub fn open(path: impl AsRef<std::path::Path>, tz_offset_minutes: i32) -> Self {
        let path = path.as_ref().to_path_buf();
        let ledger = Self::open_ledger(&path.with_extension("db"));
        let store = Self {
            plans: DashMap::new(),
            cards: DashMap::new(),
            runtimes: DashMap::new(),
            cost_model: std::sync::RwLock::new(CostModel::default()),
            tz_offset_minutes,
            path,
            ledger,
            lane_wait_override: None,
        };
        store.load();
        store
    }

    /// 打开/建表 cards.db. 失败返回 None (调用方退回 JSON 全量写).
    fn open_ledger(db_path: &std::path::Path) -> Option<std::sync::Mutex<rusqlite::Connection>> {
        let conn = match rusqlite::Connection::open(db_path) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(event = "card_ledger_open", error = %e, path = %db_path.display(), "cards.db open failed; falling back to cards.json writes");
                return None;
            }
        };
        let init = conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             CREATE TABLE IF NOT EXISTS card_face_used (
                 card_key TEXT PRIMARY KEY,
                 face_used_micro INTEGER NOT NULL DEFAULT 0,
                 updated_at INTEGER NOT NULL DEFAULT 0
             );",
        );
        if let Err(e) = init {
            tracing::warn!(event = "card_ledger_init", error = %e, "cards.db schema init failed; falling back to cards.json writes");
            return None;
        }
        Some(std::sync::Mutex::new(conn))
    }

    /// B4: 定额卡扣款热路径 — 只 upsert 一行. 返回 true = 已落库 (无需再写 JSON).
    fn ledger_add_face_used(&self, key: &str, add_micro: u64, new_total_micro: u64) -> bool {
        let Some(m) = self.ledger.as_ref() else {
            return false;
        };
        let Ok(conn) = m.lock() else {
            return false;
        };
        let now = now_unix() as i64;
        // 写「累加后的总额」而不是 +=: 内存里 cards[key].face_used_micro 已是权威值,
        // DB 只是它的持久副本, 避免 DB 与内存因并发累加顺序不同而分叉.
        let r = conn.execute(
            "INSERT INTO card_face_used (card_key, face_used_micro, updated_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(card_key) DO UPDATE SET face_used_micro = excluded.face_used_micro, updated_at = excluded.updated_at",
            rusqlite::params![key, new_total_micro as i64, now],
        );
        match r {
            Ok(_) => true,
            Err(e) => {
                tracing::warn!(event = "card_ledger_write", error = %e, card = %key, add_micro, "cards.db write failed; falling back to cards.json");
                false
            }
        }
    }

    /// 启动时: 用 cards.db 覆盖 cards.json 里的 face_used_micro (DB 是权威; JSON 里的是旧版遗留/兜底).
    /// 反向: DB 没有但 JSON 有 (从旧版本升级) → 一次性写进 DB.
    fn ledger_sync_on_load(&self) {
        let Some(m) = self.ledger.as_ref() else {
            return;
        };
        let Ok(conn) = m.lock() else {
            return;
        };
        let mut from_db = std::collections::HashMap::<String, u64>::new();
        if let Ok(mut st) = conn.prepare("SELECT card_key, face_used_micro FROM card_face_used") {
            if let Ok(rows) = st.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))) {
                for (k, v) in rows.flatten() {
                    from_db.insert(k, v.max(0) as u64);
                }
            }
        }
        let now = now_unix() as i64;
        let mut migrated = 0usize;
        for mut c in self.cards.iter_mut() {
            match from_db.get(c.key()) {
                Some(&db_val) => {
                    // DB 权威. 但若 JSON 值更大 (DB 曾写失败退回 JSON), 取 max 不少收
                    let v = db_val.max(c.face_used_micro);
                    c.face_used_micro = v;
                    if v != db_val {
                        let _ = conn.execute(
                            "UPDATE card_face_used SET face_used_micro=?2, updated_at=?3 WHERE card_key=?1",
                            rusqlite::params![c.key(), v as i64, now],
                        );
                    }
                }
                None if c.face_used_micro > 0 => {
                    let _ = conn.execute(
                        "INSERT OR REPLACE INTO card_face_used (card_key, face_used_micro, updated_at) VALUES (?1, ?2, ?3)",
                        rusqlite::params![c.key(), c.face_used_micro as i64, now],
                    );
                    migrated += 1;
                }
                None => {}
            }
        }
        if migrated > 0 {
            tracing::info!(event = "card_ledger_migrate", migrated, "migrated face_used from cards.json into cards.db");
        }
    }

    pub fn cost_model(&self) -> CostModel {
        self.cost_model
            .read()
            .map(|c| c.clone())
            .unwrap_or_default()
    }
    pub fn set_cost_model(&self, cm: CostModel) {
        if let Ok(mut g) = self.cost_model.write() {
            *g = cm;
        }
        self.save();
    }
    pub fn tz_offset_minutes(&self) -> i32 {
        self.tz_offset_minutes
    }

    /// 写入预置套餐 (已存在的 id 不覆盖, 返回新增数)
    pub fn seed_presets(&self, overwrite: bool) -> usize {
        let mut n = 0;
        for p in CardPlan::presets() {
            if overwrite || !self.plans.contains_key(&p.id) {
                self.plans.insert(p.id.clone(), p);
                n += 1;
            }
        }
        if n > 0 {
            self.save();
        }
        n
    }

    fn load(&self) {
        let Ok(text) = std::fs::read_to_string(&self.path) else {
            return;
        };
        let Ok(data) = serde_json::from_str::<Value>(&text) else {
            tracing::warn!(event = "card_load", "cards.json malformed, starting empty");
            return;
        };
        if let Some(cm) = data
            .get("cost_model")
            .and_then(|v| serde_json::from_value::<CostModel>(v.clone()).ok())
        {
            if let Ok(mut g) = self.cost_model.write() {
                *g = cm;
            }
        }
        if let Some(plans) = data.get("plans").and_then(|v| v.as_array()) {
            for p in plans {
                if let Ok(plan) = serde_json::from_value::<CardPlan>(p.clone()) {
                    self.plans.insert(plan.id.clone(), plan);
                }
            }
        }
        if let Some(cards) = data.get("cards").and_then(|v| v.as_array()) {
            for c in cards {
                if let Ok(card) = serde_json::from_value::<Card>(c.clone()) {
                    self.cards.insert(card.card_key.clone(), card);
                }
            }
        }
        // B4: cards.db 里的 face_used 是权威, 覆盖/迁移 JSON 值
        self.ledger_sync_on_load();
        tracing::info!(
            event = "card_load",
            plans = self.plans.len(),
            cards = self.cards.len(),
            ledger = self.ledger.is_some(),
            "card store loaded"
        );
    }

    pub fn save(&self) {
        let plans: Vec<CardPlan> = self.plans.iter().map(|r| r.value().clone()).collect();
        let cards: Vec<Card> = self.cards.iter().map(|r| r.value().clone()).collect();
        let data = json!({ "cost_model": self.cost_model(), "plans": plans, "cards": cards });
        if let Ok(text) = serde_json::to_string_pretty(&data) {
            if let Err(e) = crate::config::atomic_write(&self.path, &text) {
                tracing::warn!(event = "card_save", error = %e, "card persist failed");
            }
        }
    }

    // ── 套餐 CRUD ──
    pub fn upsert_plan(&self, plan: CardPlan) {
        self.plans.insert(plan.id.clone(), plan);
        self.save();
    }
    pub fn delete_plan(&self, id: &str) -> bool {
        // 有卡在用就不删
        if self.cards.iter().any(|c| c.plan_id == id) {
            return false;
        }
        let ok = self.plans.remove(id).is_some();
        if ok {
            self.save();
        }
        ok
    }
    pub fn get_plan(&self, id: &str) -> Option<CardPlan> {
        self.plans.get(id).map(|r| r.clone())
    }
    pub fn list_plans(&self) -> Vec<CardPlan> {
        self.plans.iter().map(|r| r.clone()).collect()
    }

    /// 该套餐当前实际可调的模型 (tier/组/前缀三合一 + 全局停用过滤).
    /// 候选 = 注册表 ∪ 内置表 ∪ 账本出现过的模型; extra_seen 传 seen_models 结果.
    pub fn plan_models(&self, plan: &CardPlan, extra_seen: &[String]) -> Vec<Value> {
        crate::models::candidate_models(extra_seen)
            .into_iter()
            .filter_map(|(m, manual)| {
                let enabled = !crate::models::registry().is_disabled(&m);
                let allowed = crate::models::plan_allows_model(
                    &plan.tier,
                    &plan.model_groups,
                    &plan.model_prefixes,
                    &m,
                )
                .is_ok();
                if !enabled || !allowed {
                    return None;
                }
                let (i, o, c, w) = model_price(&m);
                Some(json!({
                    "model": m,
                    "tier": model_tier(&m),
                    "source": if manual { "manual" } else { "builtin" },
                    "input_per_m": i, "output_per_m": o,
                    "cache_read_per_m": c, "cache_write_per_m": w,
                }))
            })
            .collect()
    }

    // ── 卡 CRUD ──
    pub fn issue_card(&self, plan_id: &str, owner: &str) -> Result<Card, String> {
        self.issue_card_priced(plan_id, owner, None)
    }

    /// 开卡, 可覆盖实收价 (促销/议价). None = 用套餐标价
    pub fn issue_card_priced(
        &self,
        plan_id: &str,
        owner: &str,
        paid_rmb: Option<f64>,
    ) -> Result<Card, String> {
        let plan = self.get_plan(plan_id).ok_or("plan not found")?;
        if !plan.enabled {
            return Err("plan disabled".into());
        }
        if plan.kind == PlanKind::Quota && plan.face_usd <= 0.0 {
            return Err("quota plan requires face_usd > 0".into());
        }
        let now = now_unix();
        let card = Card {
            card_key: format!("card-{}", uuid::Uuid::new_v4().simple()),
            plan_id: plan_id.to_string(),
            owner: owner.to_string(),
            issued_at: now,
            expires_at: now + plan.duration_hours * 3600,
            enabled: true,
            paid_rmb: paid_rmb.unwrap_or(plan.price),
            face_used_micro: 0,
        };
        self.cards.insert(card.card_key.clone(), card.clone());
        self.save();
        Ok(card)
    }
    pub fn revoke_card(&self, key: &str) -> bool {
        let ok = if let Some(mut c) = self.cards.get_mut(key) {
            c.enabled = false;
            true
        } else {
            false
        };
        if ok {
            self.save();
        }
        ok
    }
    pub fn delete_card(&self, key: &str) -> bool {
        let ok = self.cards.remove(key).is_some();
        self.runtimes.remove(key);
        if ok {
            if let Some(Ok(conn)) = self.ledger.as_ref().map(|m| m.lock()) {
                let _ = conn.execute(
                    "DELETE FROM card_face_used WHERE card_key = ?1",
                    rusqlite::params![key],
                );
            }
            self.save();
        }
        ok
    }
    pub fn extend_card(&self, key: &str, hours: u64) -> Result<Card, String> {
        let mut c = self.cards.get_mut(key).ok_or("card not found")?;
        c.expires_at += hours * 3600;
        let out = c.clone();
        drop(c);
        self.save();
        Ok(out)
    }
    pub fn get_card(&self, key: &str) -> Option<Card> {
        self.cards.get(key).map(|r| r.clone())
    }
    pub fn list_cards(&self) -> Vec<Card> {
        self.cards.iter().map(|r| r.clone()).collect()
    }

    fn runtime(&self, key: &str) -> Arc<CardRuntime> {
        self.runtimes
            .entry(key.to_string())
            .or_insert_with(|| {
                Arc::new(CardRuntime {
                    in_flight: AtomicU64::new(0),
                    day_count: AtomicU64::new(0),
                    day_quota_micro: AtomicU64::new(0),
                    day_start: AtomicU64::new(0),
                    rpm: RpmBucket::new(),
                    behavior: BehaviorSignals::new(),
                    lane_freed: tokio::sync::Notify::new(),
                    pacer: std::sync::Mutex::new(None),
                    hold_micro: AtomicU64::new(0),
                })
            })
            .clone()
    }

    /// 自然日滚动: 跨天清零今日计数/额度/行为信号. acquire 与 settle 都要调.
    fn roll_day(&self, rt: &CardRuntime, now: u64) -> u64 {
        let ds = day_start(now, self.tz_offset_minutes);
        let cur_ds = rt.day_start.load(Ordering::Relaxed);
        if cur_ds != ds
            && rt
                .day_start
                .compare_exchange(cur_ds, ds, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            rt.day_count.store(0, Ordering::Relaxed);
            rt.day_quota_micro.store(0, Ordering::Relaxed);
            rt.behavior.reset();
        }
        ds
    }

    /// 计算当前档位 (不含并发闸门). 三个维度取最紧:
    /// 1. 负载比 load = max(额度比, 次数比) → ≥soften_ratio 软化, >1 压制
    /// 2. 行为评分 ≥ 阈值 → 压制; ≥ 阈值×0.6 → 软化
    fn eval_throttle(
        &self,
        plan: &CardPlan,
        rt: &CardRuntime,
        now: u64,
    ) -> (Throttle, f64, AbuseScore) {
        let day_used = rt.day_count.load(Ordering::Relaxed);
        let quota_used = rt.day_quota_micro.load(Ordering::Relaxed) as f64 / 1e6;
        let quota_ratio = if plan.daily_quota_usd > 0.0 {
            quota_used / plan.daily_quota_usd
        } else {
            0.0
        };
        let count_ratio = if plan.fair_use_rpd > 0 {
            day_used as f64 / plan.fair_use_rpd as f64
        } else {
            0.0
        };
        let load = quota_ratio.max(count_ratio);
        let score = abuse_score(&rt.behavior, quota_used, now);
        let mut t = if load > 1.0 {
            Throttle::Degraded
        } else if load >= plan.soften_ratio {
            Throttle::Soften
        } else {
            Throttle::Normal
        };
        if plan.abuse_score_threshold > 0 {
            let th = plan.abuse_score_threshold;
            let soft_th = (th as f64 * 0.6) as u32;
            if score.score >= th {
                t = Throttle::Degraded;
            } else if score.score >= soft_th && t == Throttle::Normal {
                t = Throttle::Soften;
            }
        }
        (t, load, score)
    }

    // ── 请求路径: 准入 + 并发令牌 ──

    /// 等待空闲车道的最长时间 (B8): 超时才 429. 期间不占 CPU, FIFO 唤醒.
    /// 对真人: 高峰期多等几秒; 对脚本: 无法靠高频重试抢车道 (每次都排到队尾).
    pub const LANE_WAIT_SECS: u64 = 30;

    /// 实际等待时长 (测试可缩短)
    fn lane_wait(&self) -> std::time::Duration {
        self.lane_wait_override
            .unwrap_or(std::time::Duration::from_secs(Self::LANE_WAIT_SECS))
    }

    /// 测试用: 缩短排队超时 (仅本 store 实例)
    #[cfg(test)]
    pub fn set_lane_wait(&mut self, d: std::time::Duration) {
        self.lane_wait_override = Some(d);
    }

    /// 校验卡 + 拿并发令牌。Ok 返回 (卡, 套餐, 令牌, 档位)。
    /// Err 返回 (HTTP 状态码, 错误信息)。
    ///
    /// `est_input_tok`: 请求体估算输入 token 数 (B1 预扣用), 0 = 未知按保守默认.
    /// 车道满时异步排队最多 `LANE_WAIT_SECS` 秒 (B8), 而不是自旋 3 次就 429.
    pub async fn acquire(
        &self,
        key: &str,
        model: &str,
        est_input_tok: u64,
    ) -> Result<(Card, CardPlan, CardPermit, Throttle), (u16, String)> {
        let (card, plan, rt, throttle, cap, hold) = self.admit(key, model, est_input_tok)?;
        // 并发闸门: 先试一次, 满则排队等释放通知 (Notify 是 FIFO 的)
        let deadline = tokio::time::Instant::now() + self.lane_wait();
        loop {
            if Self::try_take_lane(&rt, cap) {
                break;
            }
            let notified = rt.lane_freed.notified();
            // 拿到 notified future 后再复查一次, 避免释放发生在 check 与 await 之间而漏唤醒
            if Self::try_take_lane(&rt, cap) {
                break;
            }
            match tokio::time::timeout_at(deadline, notified).await {
                Ok(()) => continue,
                Err(_) => {
                    rt.day_count.fetch_sub(1, Ordering::Relaxed);
                    rt.hold_micro.fetch_sub(hold, Ordering::Relaxed);
                    return Err((
                        429,
                        format!(
                            "card concurrency limit exceeded (max {}, waited {:.0}s)",
                            cap,
                            self.lane_wait().as_secs_f64()
                        ),
                    ));
                }
            }
        }
        let pace_tps = Self::pace_for(&plan, throttle);
        // B7: 档位变化时重置该卡共享桶的速率 (同卡多流共用一个桶)
        if let Ok(mut g) = rt.pacer.lock() {
            match (&mut *g, pace_tps) {
                (_, 0) => *g = None,
                (Some(p), tps) if (p.tps - tps as f64).abs() < f64::EPSILON => {}
                (slot, tps) => *slot = TokenPacer::new(tps),
            }
        }
        Ok((
            card,
            plan,
            CardPermit {
                rt,
                throttle,
                pace_tps,
                model: model.to_string(),
                key: key.to_string(),
                hold_micro: hold,
                settled: false,
            },
            throttle,
        ))
    }

    /// 同步版 acquire (单测/非 async 上下文): 车道满立即 429, 不排队.
    #[cfg(test)]
    pub fn acquire_now(
        &self,
        key: &str,
        model: &str,
    ) -> Result<(Card, CardPlan, CardPermit, Throttle), (u16, String)> {
        let (card, plan, rt, throttle, cap, hold) = self.admit(key, model, 0)?;
        if !Self::try_take_lane(&rt, cap) {
            rt.day_count.fetch_sub(1, Ordering::Relaxed);
            rt.hold_micro.fetch_sub(hold, Ordering::Relaxed);
            return Err((
                429,
                format!("card concurrency limit exceeded (max {})", cap),
            ));
        }
        let pace_tps = Self::pace_for(&plan, throttle);
        Ok((
            card,
            plan,
            CardPermit {
                rt,
                throttle,
                pace_tps,
                model: model.to_string(),
                key: key.to_string(),
                hold_micro: hold,
                settled: false,
            },
            throttle,
        ))
    }

    fn try_take_lane(rt: &CardRuntime, cap: u32) -> bool {
        let cap = cap as u64;
        loop {
            let cur = rt.in_flight.load(Ordering::Acquire);
            if cur >= cap {
                return false;
            }
            if rt
                .in_flight
                .compare_exchange(cur, cur + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    fn pace_for(plan: &CardPlan, throttle: Throttle) -> u32 {
        match throttle {
            Throttle::Normal => plan.pace_normal_tps,
            Throttle::Soften => plan.pace_soften_tps,
            Throttle::Degraded => plan.pace_degraded_tps,
        }
    }

    /// B1: 单请求预扣估算 (micro-$). 输入按估算 token 计, 输出按 2k 保守估; 未知输入按保守默认.
    fn hold_estimate_micro(model: &str, est_input_tok: u64) -> u64 {
        let usd = if est_input_tok == 0 {
            estimate_quota_cost(model, 0, 0, 0)
        } else {
            // 预扣不算缓存命中 (无法预知), 输入全按 input 价; 输出保守 2k
            estimate_quota_cost(model, est_input_tok, 2_000, 0)
        };
        (usd * 1e6) as u64
    }

    /// 准入检查 (不含并发闸门): 卡/套餐状态、余额 (含在途预扣)、模型门禁、RPM、档位.
    /// 成功时 day_count 已 +1、hold 已加到 hold_micro —— 调用方拿不到车道必须回滚这两项.
    #[allow(clippy::type_complexity)]
    fn admit(
        &self,
        key: &str,
        model: &str,
        est_input_tok: u64,
    ) -> Result<(Card, CardPlan, Arc<CardRuntime>, Throttle, u32, u64), (u16, String)> {
        let now = now_unix();
        let card = self
            .get_card(key)
            .ok_or((401u16, "invalid card key".to_string()))?;
        if !card.enabled {
            return Err((403, "card revoked".into()));
        }
        if card.is_expired(now) {
            return Err((402, "card expired".into()));
        }
        let plan = self
            .get_plan(&card.plan_id)
            .ok_or((500, "plan missing for card".to_string()))?;
        if !plan.enabled {
            return Err((403, "plan disabled".into()));
        }
        let rt = self.runtime(key);
        // 定额卡: 余额 (扣除在途预扣) 用尽 → 402. 这是 B1 的核心: 并发 N 个请求同时到达时,
        // 每个都能看到前面请求的预扣, 不会集体通过「余额 > 0」检查后透支.
        let mut hold = 0u64;
        if plan.kind == PlanKind::Quota {
            let face_micro = (plan.face_usd * 1e6) as u64;
            let in_flight_hold = rt.hold_micro.load(Ordering::Relaxed);
            let committed = card.face_used_micro.saturating_add(in_flight_hold);
            if committed >= face_micro {
                return Err((
                    402,
                    format!(
                        "card quota exhausted (${:.2} / ${:.2}{})",
                        card.face_used_micro as f64 / 1e6,
                        plan.face_usd,
                        if in_flight_hold > 0 {
                            format!(", ${:.2} in flight", in_flight_hold as f64 / 1e6)
                        } else {
                            String::new()
                        }
                    ),
                ));
            }
            // 预扣额不超过剩余余额 (最后一笔允许把余额烧到 0, 不因估算偏大而拒绝)
            hold = Self::hold_estimate_micro(model, est_input_tok).min(face_micro - committed);
        }
        // 模型访问三合一 (tier / 前缀 / 模型组): 全部为空 = 不限, 任一非空都要过
        if let Err(reason) =
            crate::models::plan_allows_model(&plan.tier, &plan.model_groups, &plan.model_prefixes, model)
        {
            return Err((403, format!("{} (plan '{}')", reason, plan.id)));
        }
        let ds = self.roll_day(&rt, now);

        // RPM 闸门 (先于计数, 拒绝时不污染当日计数)
        let rpm_now = rt.rpm.tick(now);
        if rpm_now > plan.rpm_limit as u64 {
            return Err((
                429,
                format!("card RPM limit exceeded ({}/{})", rpm_now, plan.rpm_limit),
            ));
        }

        // 行为信号 + 档位判定 (在 day_count 递增前评估, 用的是截至上一条的状态)
        rt.behavior.on_arrive(now, ds);
        let (throttle, _load, _score) = self.eval_throttle(&plan, &rt, now);
        rt.day_count.fetch_add(1, Ordering::Relaxed);
        rt.hold_micro.fetch_add(hold, Ordering::Relaxed);

        let conc_cap = match throttle {
            Throttle::Normal => plan.max_concurrency.max(1),
            Throttle::Soften => (plan.max_concurrency / 2)
                .max(plan.degraded_concurrency)
                .max(1),
            Throttle::Degraded => plan.degraded_concurrency.max(1),
        };
        Ok((card, plan, rt, throttle, conc_cap, hold))
    }

    /// 请求完成后记账: 把实际 token 折算成官方口径额度累加到今日; 定额卡同时扣面值.
    /// 由 inference_handler 在拿到上游 usage 后调用. 返回本次折算美元.
    pub fn settle(
        &self,
        key: &str,
        model: &str,
        input_tok: u64,
        output_tok: u64,
        cache_read_tok: u64,
    ) -> f64 {
        self.settle_full(key, model, input_tok, output_tok, cache_read_tok, 0)
    }

    pub fn settle_full(
        &self,
        key: &str,
        model: &str,
        input_tok: u64,
        output_tok: u64,
        cache_read_tok: u64,
        cache_write_tok: u64,
    ) -> f64 {
        let cost = estimate_quota_cost_full(
            model,
            input_tok,
            output_tok,
            cache_read_tok,
            cache_write_tok,
        );
        self.settle_usd(key, cost);
        cost
    }

    /// 按已折算的美元结算 (B3: 中断路径用本地估算值调用). 幂等由调用方 (CardPermit) 保证.
    fn settle_usd(&self, key: &str, cost_usd: f64) {
        let rt = self.runtime(key);
        let now = now_unix();
        self.roll_day(&rt, now);
        rt.behavior.on_done(now);
        let micro = (cost_usd * 1e6) as u64;
        rt.day_quota_micro.fetch_add(micro, Ordering::Relaxed);
        // 定额卡: 持久化扣面值. B4: 热路径只 upsert cards.db 一行; DB 不可用才退回全量写 JSON
        let is_quota = self
            .get_card(key)
            .and_then(|c| self.get_plan(&c.plan_id))
            .map(|p| p.kind == PlanKind::Quota)
            .unwrap_or(false);
        if is_quota {
            let new_total = if let Some(mut c) = self.cards.get_mut(key) {
                c.face_used_micro = c.face_used_micro.saturating_add(micro);
                c.face_used_micro
            } else {
                return;
            };
            if !self.ledger_add_face_used(key, micro, new_total) {
                self.save();
            }
        }
    }

    /// 通过令牌结算 (推荐入口): 释放预扣 + 记账, 幂等 (第二次调用无效).
    /// 中断路径 (客户端断开/上游错误/超时) 也走这里, 传入本地估算的 usage —— 否则
    /// 客户端可以在 usage 帧之前掐断连接白嫖 (B3).
    pub fn settle_permit(
        &self,
        permit: &mut CardPermit,
        input_tok: u64,
        output_tok: u64,
        cache_read_tok: u64,
        cache_write_tok: u64,
    ) -> f64 {
        if permit.settled {
            return 0.0;
        }
        permit.settled = true;
        permit.release_hold();
        self.settle_full(
            &permit.key,
            &permit.model,
            input_tok,
            output_tok,
            cache_read_tok,
            cache_write_tok,
        )
    }

    /// 卡运行态快照 (管理面板)
    pub fn card_status(&self, key: &str) -> Option<Value> {
        let card = self.get_card(key)?;
        let plan = self.get_plan(&card.plan_id);
        let rt = self.runtime(key);
        let now = now_unix();
        let ds = day_start(now, self.tz_offset_minutes);
        let same_day = rt.day_start.load(Ordering::Relaxed) == ds;
        let day_used = if same_day {
            rt.day_count.load(Ordering::Relaxed)
        } else {
            0
        };
        let day_quota_usd = if same_day {
            rt.day_quota_micro.load(Ordering::Relaxed) as f64 / 1e6
        } else {
            0.0
        };
        let (throttle, load, score) = match plan.as_ref() {
            Some(p) if same_day => self.eval_throttle(p, &rt, now),
            _ => (Throttle::Normal, 0.0, abuse_score(&rt.behavior, 0.0, now)),
        };
        let cm = self.cost_model();
        let face_used = card.face_used_micro as f64 / 1e6;
        Some(json!({
            "card_key": card.card_key,
            "owner": card.owner,
            "plan_id": card.plan_id,
            "plan_name": plan.as_ref().map(|p| p.name.clone()),
            "plan_kind": plan.as_ref().map(|p| p.kind),
            "tier": plan.as_ref().map(|p| p.tier.clone()),
            "enabled": card.enabled,
            "issued_at": card.issued_at,
            "expires_at": card.expires_at,
            "remaining_secs": card.remaining_secs(now),
            "expired": card.is_expired(now),
            "paid_rmb": card.paid_rmb,
            "in_flight": rt.in_flight.load(Ordering::Relaxed),
            "day_used": day_used,
            "fair_use_rpd": plan.as_ref().map(|p| p.fair_use_rpd),
            "day_quota_usd": day_quota_usd,
            "day_cost_rmb": cm.cost_rmb(day_quota_usd),
            "daily_quota_usd": plan.as_ref().map(|p| p.daily_quota_usd).unwrap_or(0.0),
            "face_usd": plan.as_ref().map(|p| p.face_usd).unwrap_or(0.0),
            "face_used_usd": face_used,
            "face_left_usd": plan.as_ref().map(|p| (p.face_usd - face_used).max(0.0)).unwrap_or(0.0),
            "load": load,
            "throttle": throttle.as_str(),
            "pace_tps": plan.as_ref().map(|p| match throttle {
                Throttle::Normal => p.pace_normal_tps,
                Throttle::Soften => p.pace_soften_tps,
                Throttle::Degraded => p.pace_degraded_tps,
            }),
            "abuse": score,
            "rpm_now": rt.rpm.current(now),
        }))
    }

    pub fn list_status(&self) -> Vec<Value> {
        self.cards
            .iter()
            .filter_map(|c| self.card_status(c.key()))
            .collect()
    }
}

/// 档位 (隐蔽限流): 正常 → 软化 → 压制.
/// 2026-09-05 起动作只有两种: 并发上限 + 输出匀速 (tok/s). 不再注入延迟、不再截 max_tokens
/// —— 那两样会让 agent 客户端重试, 越限越贵 (实测 fable $/h +30%/+91%).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Throttle {
    Normal,
    Soften,
    Degraded,
}

impl Throttle {
    pub fn as_str(&self) -> &'static str {
        match self {
            Throttle::Normal => "normal",
            Throttle::Soften => "soften",
            Throttle::Degraded => "degraded",
        }
    }
}

/// 并发令牌: Drop 时释放一个车道 (并唤醒一个排队者). 携带本请求的匀速目标.
pub struct CardPermit {
    rt: Arc<CardRuntime>,
    throttle: Throttle,
    pace_tps: u32,
    model: String,
    key: String,
    /// B1: 本请求在 rt.hold_micro 里预占的 micro-$, 结算/丢弃时归还
    hold_micro: u64,
    /// 是否已结算 (幂等; 未结算就 Drop 说明调用方漏了 settle —— 仍归还 hold, 但记警告)
    settled: bool,
}

impl CardPermit {
    pub fn throttle(&self) -> Throttle {
        self.throttle
    }
    pub fn degraded(&self) -> bool {
        self.throttle != Throttle::Normal
    }
    pub fn model(&self) -> &str {
        &self.model
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    /// 本请求输出匀速目标 (tok/s). 0 = 不限
    pub fn pace_tps(&self) -> u32 {
        self.pace_tps
    }
    /// B7: 向该卡的共享匀速桶放一帧 (估算 token 数), 返回调用方应 sleep 的时长.
    /// 同卡所有并发流共用一个桶 → 卡总输出 ≤ pace_tps. pace=0 时恒 ZERO.
    pub fn pace_admit(&self, tokens: f64) -> std::time::Duration {
        if self.pace_tps == 0 || tokens <= 0.0 {
            return std::time::Duration::ZERO;
        }
        match self.rt.pacer.lock() {
            Ok(mut g) => match g.as_mut() {
                Some(p) => p.admit(tokens),
                None => std::time::Duration::ZERO,
            },
            Err(_) => std::time::Duration::ZERO,
        }
    }
    fn release_hold(&mut self) {
        if self.hold_micro > 0 {
            // 用 fetch_update 防止并发下减到负数 (u64 下溢)
            let h = self.hold_micro;
            let _ = self.rt.hold_micro.fetch_update(
                Ordering::AcqRel,
                Ordering::Relaxed,
                |cur| Some(cur.saturating_sub(h)),
            );
            self.hold_micro = 0;
        }
    }
}

impl Drop for CardPermit {
    fn drop(&mut self) {
        if !self.settled {
            // 未经 settle_permit 的丢弃: 归还预扣 (余额不能被永久锁住).
            // 正常路径不应走到这里 —— main.rs 的所有出口都必须 settle.
            self.release_hold();
            tracing::debug!(event = "card_permit_unsettled_drop", card = %self.key, "permit dropped without settle");
        }
        self.rt.in_flight.fetch_sub(1, Ordering::AcqRel);
        // B8: 唤醒一个排队者 (Notify::notify_one 会存一个 permit, 即使此刻没人在等也不丢)
        self.rt.lane_freed.notify_one();
    }
}

// ─────────────────────────── 匀速流出 (pacing) ───────────────────────────

/// 令牌桶式匀速器: 把上游 34–48 tok/s 的突发流按 `tps` 节奏放给客户端.
/// 帧内 token 数按字符估 (CJK ≈ 1 tok/字, ASCII ≈ 0.25 tok/字). 只对流式生效.
/// 允许 burst = 1s 的量, 首帧不等待 (TTFT 不受影响).
///
/// B7: 每卡一个实例 (挂在 CardRuntime.pacer), 同卡多流共享 —— 不再每请求各建一个.
/// 长时间空闲后 `sent` 会远低于 `allowed`, 相当于无限 burst; 用 `resync` 把基线拉回,
/// 保证空闲重来时最多只有 1s 的 burst.
pub struct TokenPacer {
    tps: f64,
    /// 已放出的 token 累计 (估算)
    sent: f64,
    started: Option<std::time::Instant>,
}

impl TokenPacer {
    pub fn new(tps: u32) -> Option<Self> {
        if tps == 0 {
            None
        } else {
            Some(Self {
                tps: tps as f64,
                sent: 0.0,
                started: None,
            })
        }
    }

    /// 估算一帧 SSE 里的输出 token 数: 只数 content/text/thinking 字段的值
    pub fn estimate_tokens(sse_frame: &str) -> f64 {
        // ASCII ≈ 4 字符/token (kimi 实测 712 tok ↔ 3/char 高估 1.5×); 非 ASCII (中日韩等) ≈ 1 token/字符 (opus 实测 219 汉字 = 246 tokens).
        // 只按 chars/3 会把中文低估 3 倍, pacer 形同虚设 (本机实测 48 tok/s 未被压到 25).
        let mut est = 0.0f64;
        let mut chars = 0usize;
        for key in [
            "\"content\":\"",
            "\"text\":\"",
            "\"thinking\":\"",
            "\"reasoning_content\":\"",
            "\"reasoning\":\"",
            "\"partial_json\":\"",
        ] {
            let mut rest = sse_frame;
            while let Some(i) = rest.find(key) {
                let after = &rest[i + key.len()..];
                // 到下一个未转义引号
                let mut end = 0usize;
                let bytes = after.as_bytes();
                let mut escaped = false;
                for (j, b) in bytes.iter().enumerate() {
                    if escaped {
                        escaped = false;
                        continue;
                    }
                    if *b == b'\\' {
                        escaped = true;
                        continue;
                    }
                    if *b == b'"' {
                        end = j;
                        break;
                    }
                }
                let seg = &after[..end];
                for c in seg.chars() {
                    chars += 1;
                    est += if c.is_ascii() { 0.25 } else { 1.0 };
                }
                rest = &after[end..];
            }
        }
        est.max(if chars > 0 { 1.0 } else { 0.0 })
    }

    /// 放一帧: 返回需要等待的时长 (调用方 sleep). 首帧/无内容帧不等待.
    pub fn admit(&mut self, tokens: f64) -> std::time::Duration {
        let now = std::time::Instant::now();
        let started = *self.started.get_or_insert(now);
        let elapsed = now.duration_since(started).as_secs_f64();
        // 空闲重同步 (B7 共享桶): 桶闲置越久 allowed 越大, 不收口的话一次空闲 60s 后可以
        // 无等待放出 60×tps 个 token. 若当前欠量已为负 (放得少于配额), 把基线拉回到
        // 「恰好还剩 1s burst」的位置.
        let allowed_before = self.tps * (elapsed + 1.0);
        if self.sent < allowed_before - self.tps {
            self.sent = allowed_before - self.tps;
        }
        self.sent += tokens;
        // 允许量 = 1s burst + tps × 已过时间
        let allowed = self.tps * (elapsed + 1.0);
        if self.sent <= allowed {
            std::time::Duration::ZERO
        } else {
            let wait = (self.sent - allowed) / self.tps;
            std::time::Duration::from_secs_f64(wait.min(5.0))
        }
    }
}

// ─────────────────────────── 测试 ───────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> CardStore {
        CardStore::open(
            std::env::temp_dir().join(format!("cards-test-{}.json", uuid::Uuid::new_v4())),
            480,
        )
    }

    fn plan(id: &str) -> CardPlan {
        CardPlan {
            id: id.into(),
            name: "测试卡".into(),
            price: 50.0,
            duration_hours: 24,
            max_concurrency: 2,
            rpm_limit: 100,
            fair_use_rpd: 5,
            degraded_concurrency: 1,
            abuse_score_threshold: 0, // 单测默认关行为评分, 单独测
            // 单测显式开 pacing 以验证档位→速率映射; 生产默认 0 = 不限速
            pace_normal_tps: 25,
            pace_soften_tps: 18,
            pace_degraded_tps: 12,
            ..CardPlan::default()
        }
    }

    #[test]
    fn issue_and_acquire() {
        let s = store();
        s.upsert_plan(plan("p1"));
        let c = s.issue_card("p1", "alice").unwrap();
        let (_, pl, permit, throttle) = s.acquire_now(&c.card_key, "kimi-k3").unwrap();
        assert_eq!(pl.max_concurrency, 2);
        assert_eq!(throttle, Throttle::Normal);
        assert_eq!(permit.pace_tps(), 25);
        drop(permit);
    }

    /// B4: 定额卡扣款走 cards.db 写穿, 不再每请求全量重写 cards.json; 重开从 DB 恢复余额.
    #[test]
    fn quota_settle_writes_db_not_json() {
        let s = store();
        let mut p = plan("q4");
        p.kind = PlanKind::Quota;
        p.face_usd = 5.0;
        p.fair_use_rpd = 0;
        s.upsert_plan(p);
        let c = s.issue_card("q4", "db").unwrap();
        assert!(s.ledger.is_some(), "test store must open cards.db");
        let json_path = s.path.clone();
        let db_path = json_path.with_extension("db");
        assert!(db_path.exists());
        let json_before = std::fs::read_to_string(&json_path).unwrap();
        // 10 次结算: JSON 不应变化 (只 issue 时写过一次), DB 应累加
        for _ in 0..10 {
            s.settle(&c.card_key, "claude-opus-5", 0, 0, 0); // $0.215 each
        }
        let json_after = std::fs::read_to_string(&json_path).unwrap();
        assert_eq!(json_before, json_after, "settle must not rewrite cards.json when db is available");
        assert_eq!(s.get_card(&c.card_key).unwrap().face_used_micro, 2_150_000);
        // 直接查 DB
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        let v: i64 = conn
            .query_row(
                "SELECT face_used_micro FROM card_face_used WHERE card_key=?1",
                rusqlite::params![c.card_key],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(v, 2_150_000);
        // 重开: JSON 里 face_used 还是 0 (从未重写), 但 DB 权威 → 恢复 2.15
        drop(s);
        let s2 = CardStore::open(&json_path, 480);
        assert_eq!(s2.get_card(&c.card_key).unwrap().face_used_micro, 2_150_000);
        // 删卡同时清 DB 行
        assert!(s2.delete_card(&c.card_key));
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM card_face_used WHERE card_key=?1",
                rusqlite::params![c.card_key],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 0);
    }

    /// B4: 从旧版升级 — JSON 里有 face_used 但 DB 没有 → 启动时迁入 DB; JSON 更大时取 max.
    #[test]
    fn quota_ledger_migrates_from_json_and_takes_max() {
        let s = store();
        let mut p = plan("q5");
        p.kind = PlanKind::Quota;
        p.face_usd = 5.0;
        s.upsert_plan(p);
        let c = s.issue_card("q5", "mig").unwrap();
        let json_path = s.path.clone();
        let db_path = json_path.with_extension("db");
        drop(s);
        // 模拟旧版: 手改 JSON 里的 face_used, 删掉 DB
        let mut data: Value = serde_json::from_str(&std::fs::read_to_string(&json_path).unwrap()).unwrap();
        data["cards"][0]["face_used_micro"] = json!(777_000);
        std::fs::write(&json_path, serde_json::to_string_pretty(&data).unwrap()).unwrap();
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(db_path.with_extension("db-wal"));
        let _ = std::fs::remove_file(db_path.with_extension("db-shm"));
        let s2 = CardStore::open(&json_path, 480);
        assert_eq!(s2.get_card(&c.card_key).unwrap().face_used_micro, 777_000);
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        let v: i64 = conn
            .query_row(
                "SELECT face_used_micro FROM card_face_used WHERE card_key=?1",
                rusqlite::params![c.card_key],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(v, 777_000, "must migrate json value into db");
        // DB 比 JSON 小 (DB 曾写失败退回 JSON 写): 取 max
        conn.execute(
            "UPDATE card_face_used SET face_used_micro=100 WHERE card_key=?1",
            rusqlite::params![c.card_key],
        )
        .unwrap();
        drop(s2);
        let s3 = CardStore::open(&json_path, 480);
        assert_eq!(s3.get_card(&c.card_key).unwrap().face_used_micro, 777_000);
    }

    /// B1: 定额卡并发透支 — 余额只够一笔时, 第二笔并发请求必须被在途预扣挡住.
    #[test]
    fn quota_hold_blocks_concurrent_overdraft() {
        let s = store();
        let mut p = plan("qh");
        p.kind = PlanKind::Quota;
        p.face_usd = 0.3; // opus 默认估算单笔 $0.215
        p.fair_use_rpd = 0;
        p.max_concurrency = 4;
        s.upsert_plan(p);
        let c = s.issue_card("qh", "hold").unwrap();
        // 第 1 笔: 余额 0.3 > 0, 放行并预扣 min(0.215, 0.3) = 0.215
        let (_, _, mut g1, _) = s.acquire_now(&c.card_key, "claude-opus-5").unwrap();
        let rt = s.runtime(&c.card_key);
        assert_eq!(rt.hold_micro.load(Ordering::Relaxed), 215_000);
        // 第 2 笔: face_used(0) + hold(0.215) = 0.215 < 0.3 → 还能放, 预扣被夹到剩余 0.085
        let (_, _, mut g2, _) = s.acquire_now(&c.card_key, "claude-opus-5").unwrap();
        assert_eq!(rt.hold_micro.load(Ordering::Relaxed), 300_000);
        // 第 3 笔: committed = 0.3 ≥ 0.3 → 402, 且错误里带 in flight
        match s.acquire_now(&c.card_key, "claude-opus-5") {
            Ok(_) => panic!("3rd concurrent request must be blocked by in-flight hold"),
            Err((code, msg)) => {
                assert_eq!(code, 402);
                assert!(msg.contains("in flight"), "{msg}");
            }
        }
        // 结算第 1 笔 (真实 $0.215): hold 释放 0.215, face_used += 0.215
        s.settle_permit(&mut g1, 0, 0, 0, 0);
        assert_eq!(rt.hold_micro.load(Ordering::Relaxed), 85_000);
        assert_eq!(s.get_card(&c.card_key).unwrap().face_used_micro, 215_000);
        // 幂等: 再结算一次无效
        assert_eq!(s.settle_permit(&mut g1, 0, 0, 0, 0), 0.0);
        assert_eq!(s.get_card(&c.card_key).unwrap().face_used_micro, 215_000);
        // 第 2 笔实际只用了很少 (100 out tok = $0.0025): hold 全部归还, 只扣实际
        s.settle_permit(&mut g2, 0, 100, 0, 0);
        assert_eq!(rt.hold_micro.load(Ordering::Relaxed), 0);
        assert_eq!(s.get_card(&c.card_key).unwrap().face_used_micro, 217_500);
        drop(g1);
        drop(g2);
        // 现在余额 0.0825 > 0 → 可以再来一笔
        assert!(s.acquire_now(&c.card_key, "claude-opus-5").is_ok());
    }

    /// B1: 未结算就 Drop 的令牌必须归还预扣, 否则余额被永久锁死.
    #[test]
    fn unsettled_permit_drop_releases_hold() {
        let s = store();
        let mut p = plan("qd");
        p.kind = PlanKind::Quota;
        p.face_usd = 5.0;
        p.fair_use_rpd = 0;
        s.upsert_plan(p);
        let c = s.issue_card("qd", "drop").unwrap();
        let rt = s.runtime(&c.card_key);
        {
            let (_, _, _g, _) = s.acquire_now(&c.card_key, "claude-opus-5").unwrap();
            assert!(rt.hold_micro.load(Ordering::Relaxed) > 0);
        }
        assert_eq!(rt.hold_micro.load(Ordering::Relaxed), 0);
        assert_eq!(rt.in_flight.load(Ordering::Relaxed), 0);
        assert_eq!(s.get_card(&c.card_key).unwrap().face_used_micro, 0);
    }

    /// B8: 车道满时排队等待, 释放后 FIFO 放行, 不再立即 429.
    #[tokio::test]
    async fn lane_wait_queues_then_proceeds() {
        let s = Arc::new(store());
        let mut p = plan("lq");
        p.max_concurrency = 1;
        p.fair_use_rpd = 0;
        s.upsert_plan(p);
        let c = s.issue_card("lq", "queue").unwrap();
        let key = c.card_key.clone();
        let (_, _, g1, _) = s.acquire(&key, "kimi-k3", 0).await.unwrap();
        // 第二个请求在后台排队 (把 permit 一起带回, 否则在 task 里就 drop 了)
        let s2 = s.clone();
        let k2 = key.clone();
        let waiter = tokio::spawn(async move {
            s2.acquire(&k2, "kimi-k3", 0).await.map(|(_, _, g, t)| (g, t))
        });
        // 等 300ms (远小于 30s 超时), 排队者仍在等
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(!waiter.is_finished(), "must still be queued while lane is held");
        // 释放车道 → 排队者被唤醒并拿到车道
        drop(g1);
        let (g2, t) = tokio::time::timeout(std::time::Duration::from_secs(2), waiter)
            .await
            .expect("waiter must wake promptly after release")
            .unwrap()
            .expect("queued request must be admitted");
        assert_eq!(t, Throttle::Normal);
        let rt = s.runtime(&key);
        assert_eq!(rt.in_flight.load(Ordering::Relaxed), 1);
        drop(g2);
        assert_eq!(rt.in_flight.load(Ordering::Relaxed), 0);
    }

    /// B8: 排队超时 → 429, 且 day_count / hold 回滚. (用 override 把 30s 缩到 200ms)
    #[tokio::test]
    async fn lane_wait_times_out_and_rolls_back() {
        let mut st = store();
        st.set_lane_wait(std::time::Duration::from_millis(200));
        let s = Arc::new(st);
        let mut p = plan("lt");
        p.max_concurrency = 1;
        p.fair_use_rpd = 0;
        p.kind = PlanKind::Quota;
        p.face_usd = 10.0;
        s.upsert_plan(p);
        let c = s.issue_card("lt", "timeout").unwrap();
        let key = c.card_key.clone();
        let (_, _, _g1, _) = s.acquire(&key, "claude-opus-5", 0).await.unwrap();
        let rt = s.runtime(&key);
        let hold_after_first = rt.hold_micro.load(Ordering::Relaxed);
        let day_after_first = rt.day_count.load(Ordering::Relaxed);
        let started = std::time::Instant::now();
        match s.acquire(&key, "claude-opus-5", 0).await {
            Ok(_) => panic!("must time out while lane is held"),
            Err((code, msg)) => {
                assert_eq!(code, 429);
                assert!(msg.contains("waited"), "{msg}");
            }
        }
        assert!(started.elapsed() >= std::time::Duration::from_millis(150), "must actually wait");
        assert_eq!(rt.hold_micro.load(Ordering::Relaxed), hold_after_first, "hold must roll back");
        assert_eq!(rt.day_count.load(Ordering::Relaxed), day_after_first, "day_count must roll back");
    }

    /// B7: 同卡两条并发流共用一个匀速桶 —— 合计放出量受 pace 约束, 而不是各自一份.
    #[tokio::test]
    async fn shared_pacer_caps_total_output_across_streams() {
        let s = store();
        let mut p = plan("sp");
        p.max_concurrency = 2;
        p.fair_use_rpd = 0;
        p.pace_normal_tps = 10;
        s.upsert_plan(p);
        let c = s.issue_card("sp", "pace").unwrap();
        let (_, _, g1, _) = s.acquire(&c.card_key, "kimi-k3", 0).await.unwrap();
        let (_, _, g2, _) = s.acquire(&c.card_key, "kimi-k3", 0).await.unwrap();
        assert_eq!(g1.pace_tps(), 10);
        // 流 1 用掉 1s burst (10 tok) 不等
        assert_eq!(g1.pace_admit(10.0), std::time::Duration::ZERO);
        // 流 2 紧接着再放 10 tok: 若各自一桶会是 ZERO; 共享桶下要等 ~1s
        let w = g2.pace_admit(10.0);
        assert!(w.as_secs_f64() > 0.9 && w.as_secs_f64() <= 1.0, "{w:?}");
        // pace=0 的卡: 恒不等
        let s0 = store();
        s0.upsert_plan(CardPlan { id: "np".into(), name: "np".into(), ..CardPlan::default() });
        let c0 = s0.issue_card("np", "x").unwrap();
        let (_, _, g0, _) = s0.acquire(&c0.card_key, "kimi-k3", 0).await.unwrap();
        assert_eq!(g0.pace_admit(1e6), std::time::Duration::ZERO);
    }

    /// B7: 共享桶空闲后不能无限 burst — 空闲重同步把基线拉回 1s burst.
    #[test]
    fn pacer_idle_resync_limits_burst() {
        let mut p = TokenPacer::new(10).unwrap();
        assert_eq!(p.admit(10.0), std::time::Duration::ZERO);
        // 模拟空闲 5s: 手动把 started 往前拨
        p.started = Some(std::time::Instant::now() - std::time::Duration::from_secs(5));
        // 空闲后 allowed = 10×6 = 60, sent = 10 → 未重同步可无等待放 50 tok;
        // 重同步后 sent 被拉到 50, 再放 10 → 恰好 60 不等, 再放 10 → 等 1s
        assert_eq!(p.admit(10.0), std::time::Duration::ZERO);
        let w = p.admit(10.0);
        assert!(w.as_secs_f64() > 0.9 && w.as_secs_f64() <= 1.0, "{w:?}");
    }

    #[test]
    fn default_plan_and_presets_have_no_pacing() {
        let d = CardPlan::default();
        assert_eq!((d.pace_normal_tps, d.pace_soften_tps, d.pace_degraded_tps), (0, 0, 0));
        for p in CardPlan::presets() {
            assert_eq!(p.pace_normal_tps, 0, "{}", p.id);
            assert_eq!(p.pace_soften_tps, 0, "{}", p.id);
            assert_eq!(p.pace_degraded_tps, 0, "{}", p.id);
        }
        // pace 0 → 不建 pacer
        assert!(TokenPacer::new(0).is_none());
    }

    #[test]
    fn plan_model_groups_gate() {
        // 全局注册表 (进程级单例): 建一个只含 kimi 的组
        crate::models::registry()
            .upsert_group(crate::models::ModelGroup {
                id: "test-kimi-only".into(),
                name: "kimi".into(),
                members: vec!["kimi-*".into()],
                note: String::new(),
            })
            .unwrap();
        let s = store();
        let mut p = plan("mg");
        p.fair_use_rpd = 0;
        p.model_groups = vec!["test-kimi-only".into()];
        s.upsert_plan(p);
        let c = s.issue_card("mg", "hank").unwrap();
        assert!(s.acquire_now(&c.card_key, "kimi-k3-high").is_ok());
        match s.acquire_now(&c.card_key, "claude-opus-5") {
            Err((403, msg)) => assert!(msg.contains("model groups"), "{msg}"),
            other => panic!("expected 403, got {:?}", other.map(|_| ())),
        }
        let _ = crate::models::registry().delete_group("test-kimi-only");
    }

    /// 三合一闸门: tier ∧ 前缀 ∧ 组 任一不过即拒; 全空 = 全放
    #[test]
    fn plan_allows_model_combination() {
        use crate::models::plan_allows_model as f;
        // 全空 = 不限
        assert!(f("", &[], &[], "anything-at-all").is_ok());
        // tier 单条件 (用唯一名, 避免与并行测试写入的注册表条目串扰)
        assert!(f("standard", &[], &[], "kimi-k3").is_ok());
        assert!(f("economy", &[], &[], "pam-flagship-zzz").is_err()); // 未知模型按旗舰算
        assert!(f("flagship", &[], &[], "pam-flagship-zzz").is_ok());
        // 前缀单条件
        assert!(f("", &[], &["kimi-".into()], "kimi-k3").is_ok());
        assert!(f("", &[], &["kimi-".into()], "gpt-5.6").is_err());
        // 组单条件
        crate::models::registry()
            .upsert_group(crate::models::ModelGroup {
                id: "test-pam".into(),
                name: "t".into(),
                members: vec!["kimi-*".into()],
                note: String::new(),
            })
            .unwrap();
        assert!(f("", &["test-pam".into()], &[], "kimi-k3").is_ok());
        assert!(f("", &["test-pam".into()], &[], "gpt-5.6").is_err());
        // 组合: tier 过但组不过 → 拒; 三者都过 → 放
        assert!(f("flagship", &["test-pam".into()], &["kimi-".into()], "kimi-k3").is_ok());
        assert!(f("flagship", &["test-pam".into()], &[], "claude-opus-5").is_err());
        assert!(f("economy", &["test-pam".into()], &["kimi-".into()], "kimi-k3").is_ok());
        let _ = crate::models::registry().delete_group("test-pam");
    }

    /// plan_models: 候选全集过闸门 + 全局停用过滤
    #[test]
    fn plan_models_respects_gate_and_disabled() {
        let s = store();
        let mut p = plan("pm");
        p.model_prefixes = vec!["kimi-".into()];
        let names: Vec<String> = s
            .plan_models(&p, &[])
            .iter()
            .filter_map(|v| v.get("model").and_then(|x| x.as_str()).map(|s| s.to_string()))
            .collect();
        assert!(!names.is_empty(), "builtin kimi models should be listed");
        assert!(names.iter().all(|m| m.starts_with("kimi-")), "prefix gate leaked: {:?}", names);
        // 全局停用后从清单消失
        crate::models::registry()
            .upsert_model(crate::models::ModelEntry {
                model: "kimi-k3".into(),
                input_per_m: 3.0,
                output_per_m: 15.0,
                cache_read_per_m: 0.3,
                cache_write_per_m: 0.0,
                tier: String::new(),
                enabled: false,
                upstream: false,
                note: String::new(),
            })
            .unwrap();
        let names2: Vec<String> = s
            .plan_models(&p, &[])
            .iter()
            .filter_map(|v| v.get("model").and_then(|x| x.as_str()).map(|s| s.to_string()))
            .collect();
        assert!(!names2.iter().any(|m| m == "kimi-k3"), "disabled model still listed");
        let _ = crate::models::registry().delete_model("kimi-k3");
    }

    #[test]
    fn concurrency_cap_enforced() {
        let s = store();
        s.upsert_plan(plan("p1"));
        let c = s.issue_card("p1", "bob").unwrap();
        let _g1 = s.acquire_now(&c.card_key, "kimi-k3").unwrap().2;
        let _g2 = s.acquire_now(&c.card_key, "kimi-k3").unwrap().2;
        match s.acquire_now(&c.card_key, "kimi-k3") {
            Ok(_) => panic!("3rd concurrent acquire must be rejected"),
            Err((code, _)) => assert_eq!(code, 429),
        }
    }

    #[test]
    fn fair_use_degrades_and_paces_down() {
        let s = store();
        s.upsert_plan(plan("p1")); // fair_use_rpd=5
        let c = s.issue_card("p1", "carol").unwrap();
        for _ in 0..5 {
            drop(s.acquire_now(&c.card_key, "kimi-k3").unwrap().2);
        }
        // 第 6 次: 5/5 = 1.0 → 软化 (load ≥ 0.7 且 ≤ 1.0)
        let (_, _, p6, t6) = s.acquire_now(&c.card_key, "kimi-k3").unwrap();
        assert_eq!(t6, Throttle::Soften);
        assert_eq!(p6.pace_tps(), 18);
        drop(p6);
        // 第 7 次: 6/5 > 1.0 → 压制, 匀速降到 12
        let (_, _, p7, t7) = s.acquire_now(&c.card_key, "kimi-k3").unwrap();
        assert_eq!(t7, Throttle::Degraded);
        assert_eq!(p7.pace_tps(), 12);
    }

    #[test]
    fn quota_soften_then_degrade() {
        let s = store();
        let mut p = plan("pq");
        p.fair_use_rpd = 0;
        p.daily_quota_usd = 1.0;
        p.soften_ratio = 0.7;
        s.upsert_plan(p);
        let c = s.issue_card("pq", "dave").unwrap();
        // opus-5 默认成本 (20k in, 4k out, 30k cr) = 0.1+0.1+0.015 = $0.215
        s.settle(&c.card_key, "claude-opus-5", 0, 0, 0);
        s.settle(&c.card_key, "claude-opus-5", 0, 0, 0);
        let (_, _, _, t0) = s.acquire_now(&c.card_key, "claude-opus-5").unwrap();
        assert_eq!(t0, Throttle::Normal, "43% 额度应正常");
        s.settle(&c.card_key, "claude-opus-5", 0, 0, 0);
        s.settle(&c.card_key, "claude-opus-5", 0, 0, 0);
        let (_, _, _, t1) = s.acquire_now(&c.card_key, "claude-opus-5").unwrap();
        assert_eq!(t1, Throttle::Soften, "86% 额度应进软化档");
        s.settle(&c.card_key, "claude-opus-5", 0, 0, 0);
        let (_, _, _, t2) = s.acquire_now(&c.card_key, "claude-opus-5").unwrap();
        assert_eq!(t2, Throttle::Degraded, ">100% 额度应进压制档");
    }

    #[test]
    fn official_prices_calibrated() {
        // 官方对账: kimi-k3 $3/$15/$0.3, 不是 $20/$100
        assert_eq!(model_price("kimi-k3-high"), (3.0, 15.0, 0.3, 0.0));
        // -fast 变体 ×2
        assert_eq!(model_price("claude-opus-5-fast"), (10.0, 50.0, 1.0, 12.5));
        assert_eq!(
            model_price("claude-opus-5-thinking-xhigh-fast"),
            (10.0, 50.0, 1.0, 12.5)
        );
        // cursor- 前缀 grok
        assert_eq!(model_price("cursor-grok-4.6-high"), (2.0, 6.0, 0.5, 0.0));
        // fable thinking 走 claude-fable-5 价
        assert_eq!(
            model_price("claude-fable-5-1-thinking-high"),
            (10.0, 50.0, 1.0, 12.5)
        );
        // 与官方账单核对: acc-3 kimi-k3-high in=13.71M out=1.46M cr=179.12M → $116.8
        let c = estimate_quota_cost_full("kimi-k3-high", 13_710_000, 1_460_000, 179_120_000, 0);
        assert!((c - 116.8).abs() < 1.5, "got {}", c);
    }

    /// B2 回归: 注册表按最长前缀命中时, `-fast` 请求不能按基础模型半价记账.
    #[test]
    fn registry_prefix_hit_keeps_fast_multiplier() {
        let reg = crate::models::registry();
        // 唯一前缀, 避免与其他测试/内置表串扰
        let base = "b2test-zeta-9";
        let fast = "b2test-zeta-9-fast";
        let mk = |m: &str, i: f64, o: f64| crate::models::ModelEntry {
            model: m.into(),
            input_per_m: i,
            output_per_m: o,
            cache_read_per_m: i / 10.0,
            cache_write_per_m: 0.0,
            tier: "standard".into(),
            enabled: true,
            upstream: false,
            note: String::new(),
        };
        reg.upsert_model(mk(base, 4.0, 20.0)).unwrap();
        // 面板只录了基础名: fast 请求前缀命中 → 必须 ×2
        assert_eq!(model_price(base), (4.0, 20.0, 0.4, 0.0));
        assert_eq!(model_price(fast), (8.0, 40.0, 0.8, 0.0));
        // 面板专门给 fast 定价: 所见即所得, 不再乘
        reg.upsert_model(mk(fast, 9.0, 45.0)).unwrap();
        assert_eq!(model_price(fast), (9.0, 45.0, 0.9, 0.0));
        assert_eq!(model_price(base), (4.0, 20.0, 0.4, 0.0));
        let _ = reg.delete_model(fast);
        let _ = reg.delete_model(base);
    }

    #[test]
    fn cost_model_rmb_per_usd() {
        let cm = CostModel::default();
        assert!((cm.rmb_per_usd() - 0.065).abs() < 1e-9);
        assert!((cm.cost_rmb(200.0) - 13.0).abs() < 1e-9);
    }

    #[test]
    fn tiers_by_burn_rate() {
        assert_eq!(model_tier("grok-4.6"), TIER_ECONOMY);
        assert_eq!(model_tier("claude-opus-5"), TIER_ECONOMY);
        assert_eq!(model_tier("kimi-k3-high"), TIER_ECONOMY);
        assert_eq!(model_tier("kimi-k3-max"), TIER_STANDARD);
        assert_eq!(model_tier("claude-fable-5"), TIER_STANDARD);
        assert_eq!(model_tier("claude-opus-5-fast"), TIER_STANDARD);
        assert_eq!(model_tier("claude-fable-5-1-thinking-high"), TIER_FLAGSHIP);
        assert_eq!(model_tier("totally-unknown-model"), TIER_FLAGSHIP);
        assert!(tier_allows(TIER_FLAGSHIP, "claude-fable-5-1-thinking-max"));
        assert!(tier_allows(TIER_STANDARD, "claude-fable-5"));
        assert!(!tier_allows(
            TIER_STANDARD,
            "claude-fable-5-1-thinking-high"
        ));
        assert!(!tier_allows(TIER_ECONOMY, "claude-fable-5"));
        assert!(tier_allows(TIER_ECONOMY, "grok-4.6"));
    }

    #[test]
    fn tier_gate_on_plan() {
        let s = store();
        let mut p = plan("eco");
        p.tier = TIER_ECONOMY.into();
        s.upsert_plan(p);
        let c = s.issue_card("eco", "erin").unwrap();
        assert!(s.acquire_now(&c.card_key, "grok-4.6").is_ok());
        match s.acquire_now(&c.card_key, "claude-fable-5-1-thinking-high") {
            Ok(_) => panic!("flagship model must be rejected on economy plan"),
            Err((code, msg)) => {
                assert_eq!(code, 403);
                assert!(msg.contains("flagship"), "{}", msg);
            }
        }
    }

    #[test]
    fn quota_card_deducts_and_exhausts() {
        let s = store();
        let mut p = plan("q");
        p.kind = PlanKind::Quota;
        p.face_usd = 0.5;
        p.fair_use_rpd = 0;
        s.upsert_plan(p);
        let c = s.issue_card("q", "frank").unwrap();
        assert!(s.acquire_now(&c.card_key, "claude-opus-5").is_ok());
        // 烧 $0.215 ×2 = $0.43 < 0.5 → 还能用
        s.settle(&c.card_key, "claude-opus-5", 0, 0, 0);
        s.settle(&c.card_key, "claude-opus-5", 0, 0, 0);
        assert!(s.acquire_now(&c.card_key, "claude-opus-5").is_ok());
        // 第 3 次 → $0.645 ≥ 0.5 → 402
        s.settle(&c.card_key, "claude-opus-5", 0, 0, 0);
        match s.acquire_now(&c.card_key, "claude-opus-5") {
            Ok(_) => panic!("exhausted quota card must 402"),
            Err((code, _)) => assert_eq!(code, 402),
        }
        // 余额持久化: 重开 store 仍是用尽
        let path = s.path.clone();
        drop(s);
        let s2 = CardStore::open(&path, 480);
        let c2 = s2.get_card(&c.card_key).unwrap();
        assert!(c2.face_used_micro >= 500_000);
    }

    #[test]
    fn quota_plan_requires_face() {
        let s = store();
        let mut p = plan("q0");
        p.kind = PlanKind::Quota;
        p.face_usd = 0.0;
        s.upsert_plan(p);
        assert!(s.issue_card("q0", "x").is_err());
    }

    #[test]
    fn abuse_score_human_vs_script() {
        // 人: 40 个活跃格, 秒接 10%, 8h → 0 分
        let sig = BehaviorSignals::new();
        let ds = 1_700_000_000u64;
        for i in 0..40u64 {
            let t = ds + i * 300 * 2; // 每 10 分钟一条
            sig.on_arrive(t, ds);
            sig.on_done(t + 60);
        }
        let s = abuse_score(&sig, 30.0, ds + 40 * 600);
        assert!(s.score < 30, "human scored {} {:?}", s.score, s.reasons);

        // 脚本: 220 格, 秒接 100%, 20h 无停顿, 日烧 $300 → ≥80
        let sig = BehaviorSignals::new();
        let mut t = ds;
        for _ in 0..1200 {
            sig.on_arrive(t, ds); // 到达
            t += 59; // 上游生成 59s
            sig.on_done(t); // 结束
            t += 1; // 1s 后秒接下一条
        }
        let s = abuse_score(&sig, 300.0, t);
        assert!(s.score >= 80, "script scored {} {:?}", s.score, s.reasons);
        assert!(s.active_slots >= 200);
    }

    #[test]
    fn abuse_threshold_forces_degraded() {
        let s = store();
        let mut p = plan("ab");
        p.fair_use_rpd = 0;
        p.abuse_score_threshold = 15; // 低阈值便于触发
        s.upsert_plan(p);
        let c = s.issue_card("ab", "grace").unwrap();
        // 制造秒接: 连续 acquire→settle 间隔 0s (同秒)
        for _ in 0..12 {
            let (_, _, g, _) = s.acquire_now(&c.card_key, "grok-4.6").unwrap();
            drop(g);
            s.settle(&c.card_key, "grok-4.6", 100, 10, 0);
        }
        // 秒接率 ≥ 40% → +30 ≥ 15 → 压制
        let (_, _, g, t) = s.acquire_now(&c.card_key, "grok-4.6").unwrap();
        assert_eq!(t, Throttle::Degraded);
        assert_eq!(g.pace_tps(), 12);
    }

    #[test]
    fn estimate_tokens_cjk_vs_ascii() {
        // 12 个 ASCII ≈ 3 token; 13 个汉字 ≈ 13 token
        let ascii = TokenPacer::estimate_tokens(
            r#"data: {"choices":[{"delta":{"content":"hello world!"}}]}"#,
        );
        let cjk = TokenPacer::estimate_tokens(
            r#"data: {"choices":[{"delta":{"content":"太阳系是以太阳为中心的天体"}}]}"#,
        );
        assert!((ascii - 3.0).abs() < 0.01, "{ascii}");
        assert!((cjk - 13.0).abs() < 0.01, "{cjk}");
        assert_eq!(
            TokenPacer::estimate_tokens(r#"data: {"choices":[{"delta":{"role":"assistant"}}]}"#),
            0.0
        );
    }

    #[test]
    fn pacer_math() {
        let mut p = TokenPacer::new(10).unwrap();
        // 1s burst: 前 10 token 不等
        assert_eq!(p.admit(10.0), std::time::Duration::ZERO);
        // 再来 20 token → 超出 20 → 等 2s
        let w = p.admit(20.0);
        assert!(w.as_secs_f64() > 1.9 && w.as_secs_f64() <= 2.0, "{:?}", w);
        assert!(TokenPacer::new(0).is_none());
        // 帧 token 估算
        let t = TokenPacer::estimate_tokens(
            r#"data: {"choices":[{"delta":{"content":"hello world foo"}}]}"#,
        );
        assert!((t - 3.75).abs() < 0.01, "{}", t); // 15 ASCII × 0.25
        assert_eq!(
            TokenPacer::estimate_tokens(r#"data: {"choices":[{"delta":{"role":"assistant"}}]}"#),
            0.0
        );
    }

    #[test]
    fn presets_seed_and_persist() {
        let s = store();
        let n = s.seed_presets(false);
        assert_eq!(n, 10);
        assert_eq!(s.seed_presets(false), 0, "已存在不重复写");
        let p = s.get_plan("day-std-1").unwrap();
        assert_eq!(p.tier, TIER_STANDARD);
        assert!((p.price - 34.9).abs() < 1e-9);
        let q = s.get_plan("quota-200").unwrap();
        assert_eq!(q.kind, PlanKind::Quota);
        assert_eq!(q.pace_normal_tps, 0);
        // 重载
        let path = s.path.clone();
        drop(s);
        let s2 = CardStore::open(&path, 480);
        assert_eq!(s2.list_plans().len(), 10);
        assert!((s2.cost_model().rmb_per_usd() - 0.065).abs() < 1e-9);
    }

    #[test]
    fn expired_card_rejected() {
        let s = store();
        s.upsert_plan(plan("p1"));
        let mut c = s.issue_card("p1", "eve").unwrap();
        c.expires_at = 1;
        s.cards.insert(c.card_key.clone(), c.clone());
        match s.acquire_now(&c.card_key, "kimi-k3") {
            Ok(_) => panic!("expired card must be rejected"),
            Err((code, _)) => assert_eq!(code, 402),
        }
    }

    #[test]
    fn day_start_rolls() {
        let ds1 = day_start(1_700_000_000, 480);
        let ds2 = day_start(1_700_000_000 + 86400, 480);
        assert_eq!(ds2 - ds1, 86400);
    }
}
