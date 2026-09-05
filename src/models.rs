//! 模型注册表: 手动定价 + 层级 + 模型组 (models.json).
//!
//! - 定价/层级: 面板可改, 覆盖 `cards.rs` 内置官方价格表 (内置表只做兜底与「恢复默认」).
//! - 模型组: 若干模型名/前缀/通配组成一组; API key 与套餐都可限定「只能访问哪些组」.
//! - 全局单例 (`registry()`), 热路径无锁读 (ArcSwap); 改动即落盘.

use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

/// 单个模型的手动设置
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelEntry {
    pub model: String,
    /// $/1M tokens, 官方口径
    pub input_per_m: f64,
    pub output_per_m: f64,
    #[serde(default)]
    pub cache_read_per_m: f64,
    #[serde(default)]
    pub cache_write_per_m: f64,
    /// 是否允许调用 (false = 全局停用, 所有 key/卡都 403)
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 该模型经上游 AvailableModels 确认存在 (面板「获取可用模型」标记).
    /// 仅作展示/提醒, 不参与闸门 —— 上游列表拉不到不代表模型不可用.
    #[serde(default)]
    pub upstream: bool,
    #[serde(default)]
    pub note: String,
}

fn default_true() -> bool {
    true
}

/// 模型组: 成员为模型名; 支持后缀 `*` 通配 (如 `claude-opus-*`), 精确名优先
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelGroup {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub members: Vec<String>,
    #[serde(default)]
    pub note: String,
}

impl ModelGroup {
    pub fn contains(&self, model: &str) -> bool {
        self.members.iter().any(|m| pattern_matches(m, model))
    }
}

/// `*` 只支持结尾通配; `*` 单独 = 全部
pub fn pattern_matches(pattern: &str, model: &str) -> bool {
    let p = pattern.trim();
    if p.is_empty() {
        return false;
    }
    if p == "*" {
        return true;
    }
    match p.strip_suffix('*') {
        Some(prefix) => model.starts_with(prefix),
        None => p == model,
    }
}

/// 剥掉一层官方变体后缀; 没有可剥的返回原名.
/// 顺序敏感: 先剥 -fast, 再剥思考档, 再剥 -thinking (claude-opus-5-thinking-max-fast 需三轮).
pub fn strip_variant_suffix(model: &str) -> &str {
    const SUFFIXES: &[&str] = &[
        "-fast",
        "-low",
        "-medium",
        "-high",
        "-xhigh",
        "-max",
        "-none",
        "-minimal",
        "-extra-high",
        "-thinking",
    ];
    for s in SUFFIXES {
        if let Some(base) = model.strip_suffix(s) {
            if !base.is_empty() {
                return base;
            }
        }
    }
    model
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RegistryData {
    #[serde(default)]
    pub models: Vec<ModelEntry>,
    #[serde(default)]
    pub groups: Vec<ModelGroup>,
}

pub struct ModelRegistry {
    data: ArcSwap<RegistryData>,
    path: PathBuf,
}

static REGISTRY: OnceLock<Arc<ModelRegistry>> = OnceLock::new();

/// 全局注册表; 未 init 时返回空注册表 (单测/工具场景) — 一切回落内置价格表
pub fn registry() -> Arc<ModelRegistry> {
    REGISTRY
        .get_or_init(|| Arc::new(ModelRegistry::empty()))
        .clone()
}

/// 进程启动时调用一次; 重复调用返回已存在的实例
pub fn init(path: &std::path::Path) -> Arc<ModelRegistry> {
    REGISTRY
        .get_or_init(|| Arc::new(ModelRegistry::open(path)))
        .clone()
}

impl ModelRegistry {
    fn empty() -> Self {
        Self {
            data: ArcSwap::from_pointee(RegistryData::default()),
            path: PathBuf::new(),
        }
    }

    pub fn open(path: &std::path::Path) -> Self {
        let data = std::fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str::<RegistryData>(&s).ok())
            .unwrap_or_default();
        Self {
            data: ArcSwap::from_pointee(data),
            path: path.to_path_buf(),
        }
    }

    pub fn snapshot(&self) -> Arc<RegistryData> {
        self.data.load_full()
    }

    fn save(&self, data: RegistryData) -> anyhow::Result<()> {
        if !self.path.as_os_str().is_empty() {
            let tmp = self.path.with_extension("json.tmp");
            std::fs::write(&tmp, serde_json::to_vec_pretty(&data)?)?;
            std::fs::rename(&tmp, &self.path)?;
        }
        self.data.store(Arc::new(data));
        Ok(())
    }

    // ── 模型 (定价/层级) ──

    /// 精确名 > 最长前缀命中 (注册表内条目名也当前缀用, 与内置表规则一致)
    pub fn lookup(&self, model: &str) -> Option<ModelEntry> {
        let d = self.data.load();
        if let Some(e) = d.models.iter().find(|e| e.model == model) {
            return Some(e.clone());
        }
        let mut best: Option<&ModelEntry> = None;
        for e in &d.models {
            if model.starts_with(&e.model) && best.map_or(true, |b| e.model.len() > b.model.len()) {
                best = Some(e);
            }
        }
        best.cloned()
    }

    /// 只看精确名 (面板编辑用)
    pub fn get_exact(&self, model: &str) -> Option<ModelEntry> {
        self.data
            .load()
            .models
            .iter()
            .find(|e| e.model == model)
            .cloned()
    }

    pub fn upsert_model(&self, entry: ModelEntry) -> anyhow::Result<()> {
        let mut d = (*self.data.load_full()).clone();
        match d.models.iter_mut().find(|e| e.model == entry.model) {
            Some(e) => *e = entry,
            None => d.models.push(entry),
        }
        d.models.sort_by(|a, b| a.model.cmp(&b.model));
        self.save(d)
    }

    /// 把一批模型名标记为「上游确认可用」; 已在注册表的条目只置位, 不新建条目
    /// (上游名可能带变体后缀, 盲目建行会稀释最长前缀匹配).
    /// 返回 (置位条数, 上游有但注册表没有的名字).
    pub fn mark_upstream(&self, names: &[String]) -> anyhow::Result<(usize, Vec<String>)> {
        let mut d = (*self.data.load_full()).clone();
        let mut missing: Vec<String> = vec![];
        let mut marked = 0usize;
        for n in names {
            match d.models.iter_mut().find(|e| &e.model == n) {
                Some(e) => {
                    if !e.upstream {
                        e.upstream = true;
                        marked += 1;
                    }
                }
                None => missing.push(n.clone()),
            }
        }
        if marked > 0 {
            self.save(d)?;
        }
        Ok((marked, missing))
    }

    pub fn delete_model(&self, model: &str) -> anyhow::Result<bool> {
        let mut d = (*self.data.load_full()).clone();
        let n = d.models.len();
        d.models.retain(|e| e.model != model);
        let removed = d.models.len() != n;
        self.save(d)?;
        Ok(removed)
    }

    /// 整表替换 (面板「保存全部」)
    pub fn replace_models(&self, models: Vec<ModelEntry>) -> anyhow::Result<()> {
        let mut d = (*self.data.load_full()).clone();
        d.models = models;
        d.models.sort_by(|a, b| a.model.cmp(&b.model));
        self.save(d)
    }

    // ── 模型组 ──

    pub fn groups(&self) -> Vec<ModelGroup> {
        self.data.load().groups.clone()
    }

    pub fn get_group(&self, id: &str) -> Option<ModelGroup> {
        self.data.load().groups.iter().find(|g| g.id == id).cloned()
    }

    pub fn upsert_group(&self, group: ModelGroup) -> anyhow::Result<()> {
        let mut d = (*self.data.load_full()).clone();
        match d.groups.iter_mut().find(|g| g.id == group.id) {
            Some(g) => *g = group,
            None => d.groups.push(group),
        }
        self.save(d)
    }

    pub fn delete_group(&self, id: &str) -> anyhow::Result<bool> {
        let mut d = (*self.data.load_full()).clone();
        let n = d.groups.len();
        d.groups.retain(|g| g.id != id);
        let removed = d.groups.len() != n;
        self.save(d)?;
        Ok(removed)
    }

    /// 鉴权: `allowed_groups` 为空 = 不限; 否则模型须落在任一组内.
    /// 引用了不存在的组 id 视为空组 (不放行), 避免删组后意外放开.
    pub fn allowed_by_groups(&self, allowed_groups: &[String], model: &str) -> bool {
        if allowed_groups.is_empty() {
            return true;
        }
        let d = self.data.load();
        allowed_groups.iter().any(|gid| {
            d.groups
                .iter()
                .find(|g| &g.id == gid)
                .map_or(false, |g| g.contains(model))
        })
    }

    /// 某模型属于哪些组 (面板展示)
    pub fn groups_of(&self, model: &str) -> Vec<String> {
        self.data
            .load()
            .groups
            .iter()
            .filter(|g| g.contains(model))
            .map(|g| g.id.clone())
            .collect()
    }

    /// 模型是否被全局停用 (注册表精确/前缀命中且 enabled=false)
    pub fn is_disabled(&self, model: &str) -> bool {
        self.lookup(model).map_or(false, |e| !e.enabled)
    }

    /// 模型家族基名: 循环剥官方变体后缀直到收敛.
    /// claude-opus-5-thinking-max-fast → claude-opus-5; gpt-5.4-mini-none → gpt-5.4-mini;
    /// kimi-k2.7-code / default / gemini-3-flash 这类无变体后缀的, 基名即自身.
    pub fn family_base(model: &str) -> &str {
        let mut m = model;
        loop {
            let s = strip_variant_suffix(m);
            if s == m {
                return m;
            }
            m = s;
        }
    }

    /// 客户端请求模型名 → 实际上游模型名 (智能思考强度路由).
    ///
    /// 客户端传家族基名 (如 claude-opus-5) 且上游名单里**没有**这个精确名时,
    /// 按思考强度在该家族的上游变体里挑一个:
    ///   thinking_level 显式字段 > reasoning_effort (OpenAI 习惯) > max_mode > 复杂度推断.
    /// 档位映射 (家族里没有该档变体时向 nearest 可用档回落):
    ///   Max → -max > -xhigh > -extra-high > -high
    ///   High → -high > -medium > -low > -xhigh
    ///   Low → -low > -minimal > -none > -medium
    /// 基名本身就在上游名单里 (gemini-3-flash / kimi-k2.7-code / claude-4-sonnet) → 原样放行.
    /// 客户端直接传了变体全名 (claude-opus-5-high-fast) → 原样放行, 不重写.
    /// 返回 None = 无法解析 (家族在上游名单里没有任何变体), 调用方原样透传.
    pub fn resolve_smart_model(
        model: &str,
        body: &serde_json::Value,
        upstream: &[String],
    ) -> Option<String> {
        if upstream.is_empty() || upstream.iter().any(|u| u == model) {
            return None;
        }
        let base = Self::family_base(model);
        if base != model {
            return None; // 客户端已带变体后缀, 尊重显式选择
        }
        // 家族须在上游名单里真有变体 (防止把任意字符串路由成不存在的模型)
        let pref = format!("{base}-");
        if !upstream
            .iter()
            .any(|u| u.starts_with(&pref) && Self::family_base(u) == base)
        {
            return None;
        }
        let level = crate::cursor::auto_detect_thinking_level(body);
        let pick = |cands: &[&str]| -> Option<String> {
            cands
                .iter()
                .map(|c| format!("{base}-{c}"))
                .find(|want| upstream.iter().any(|u| u == want))
        };
        use crate::cursor::ThinkingLevel::*;
        let resolved = match level {
            Max => pick(&["max", "xhigh", "extra-high", "high"]),
            High => pick(&["high", "medium", "low", "xhigh"]),
            Low => pick(&["low", "minimal", "none", "medium"]),
        };
        // 家族无任何档位变体 (如 default): 试试基名-fast, 再放弃
        resolved.or_else(|| {
            let fast = format!("{base}-fast");
            upstream.iter().any(|u| u == &fast).then_some(fast)
        })
    }

    /// 模型是否对客户端可见 (进 /v1/models 与套餐可调清单).
    /// 规则: 注册表条目看自己的 enabled; 非注册表模型要求「上游确认过」——
    /// 上游 AvailableModels 拉到的名字直接可见, 或能剥掉官方变体后缀
    /// (-low/-medium/-high/-xhigh/-max/-none/-minimal/-extra-high/-thinking[-*]/-fast)
    /// 落到上游名单里某个名字的家族基名上 (claude-opus-5-thinking-max-fast → …→ claude-opus-5).
    /// 内置表里的纯别名 (fable-5/opus-5/gpt-5/gemini-3/glm-5/kimi-k2…) 上游名单里
    /// 没有任何变体, 不再出现在列表里.
    pub fn is_visible(&self, model: &str, upstream: &[String]) -> bool {
        let d = self.data.load();
        if let Some(e) = d.models.iter().find(|e| e.model == model) {
            return e.enabled;
        }
        if upstream.iter().any(|u| u == model) {
            return true;
        }
        // 上游名单里存在「以 model 为基名」的变体 → 基名可见
        // (claude-opus-5 不在名单, 但 claude-opus-5-low 在)
        let pref = format!("{model}-");
        if upstream
            .iter()
            .any(|u| u.starts_with(&pref) && Self::family_base(u) == model)
        {
            return true;
        }
        // 剥变体后缀, 逐级向可见名收敛
        let mut m = model;
        loop {
            let stripped = strip_variant_suffix(m);
            if stripped == m {
                return false;
            }
            m = stripped;
            if upstream.iter().any(|u| u == m) {
                return true;
            }
            let pref = format!("{m}-");
            if upstream
                .iter()
                .any(|u| u.starts_with(&pref) && Self::family_base(u) == m)
            {
                return true;
            }
            if let Some(e) = d.models.iter().find(|e| e.model == m) {
                return e.enabled;
            }
        }
    }
    /// 客户端可见的模型清单: 注册表(enabled) ∪ 上游家族基名 ∪ 上游无变体独立模型.
    /// 变体名 (claude-opus-5-high-fast) 不进列表 —— 客户端选基名, 网关按思考强度
    /// 路由到具体变体 (resolve_smart_model). extra 里的账本 seen 名同样按家族折叠.
    pub fn visible_models(&self, upstream: &[String], extra: &[String]) -> Vec<String> {
        let d = self.data.load();
        let mut out: Vec<String> = vec![];
        let mut push = |id: String| {
            if !out.iter().any(|x| *x == id) {
                out.push(id);
            }
        };
        for e in &d.models {
            if e.enabled {
                push(e.model.clone());
            }
        }
        for m in upstream.iter().chain(extra.iter()) {
            let base = Self::family_base(m);
            if self.is_visible(base, upstream) {
                push(base.to_string());
            }
        }
        out
    }
}

/// 拒绝原因 (供 main.rs 组装 403 文案)
pub fn deny_reason(allowed_groups: &[String], model: &str) -> String {
    format!(
        "model '{}' is not in allowed model groups [{}]",
        model,
        allowed_groups.join(", ")
    )
}

/// 套餐模型闸门 (model_groups / model_prefixes).
/// 全部为空 = 不限; 任一非空项都必须通过. 返回 Err(人类可读原因) 表示拒绝.
pub fn plan_allows_model(
    groups: &[String],
    prefixes: &[String],
    model: &str,
) -> Result<(), String> {
    if !prefixes.is_empty() && !prefixes.iter().any(|p| model.starts_with(p.as_str())) {
        return Err(format!(
            "model '{}' not in prefixes {:?}",
            model, prefixes
        ));
    }
    if !registry().allowed_by_groups(groups, model) {
        return Err(deny_reason(groups, model));
    }
    Ok(())
}

/// 候选模型全集: 注册表 ∪ 内置表 ∪ 额外名 (如账本 seen).
/// 每项 (模型名, 是否注册表手动条目). 注册表同名优先 (带 enabled/tier 等手动设定).
pub fn candidate_models(extra: &[String]) -> Vec<(String, bool)> {
    candidate_models_extra(&[], extra)
}

/// 同 candidate_models, 额外并入上游确认名单 (upstream).
pub fn candidate_models_extra(upstream: &[String], extra: &[String]) -> Vec<(String, bool)> {
    let snap = registry().snapshot();
    let mut out: Vec<(String, bool)> = snap
        .models
        .iter()
        .map(|e| (e.model.clone(), true))
        .collect();
    for (m, ..) in crate::cards::builtin_table() {
        if !out.iter().any(|(n, _)| *n == m) {
            out.push((m, false));
        }
    }
    for m in upstream.iter().chain(extra.iter()) {
        if !out.iter().any(|(n, _)| n == m) {
            out.push((m.clone(), false));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reg() -> ModelRegistry {
        ModelRegistry::empty()
    }

    fn entry(m: &str, i: f64, o: f64, _tier: &str) -> ModelEntry {
        ModelEntry {
            model: m.into(),
            input_per_m: i,
            output_per_m: o,
            cache_read_per_m: 0.0,
            cache_write_per_m: 0.0,
            enabled: true,
            upstream: false,
            note: String::new(),
        }
    }

    #[test]
    fn pattern_rules() {
        assert!(pattern_matches("*", "anything"));
        assert!(pattern_matches("claude-opus-*", "claude-opus-5-fast"));
        assert!(!pattern_matches("claude-opus-*", "claude-fable-5"));
        assert!(pattern_matches("kimi-k3-high", "kimi-k3-high"));
        assert!(!pattern_matches("kimi-k3-high", "kimi-k3-high-fast"));
        assert!(!pattern_matches("", "x"));
    }

    #[test]
    fn strip_suffix_chains() {
        assert_eq!(strip_variant_suffix("claude-opus-5-thinking-max-fast"), "claude-opus-5-thinking-max");
        assert_eq!(strip_variant_suffix("claude-opus-5-thinking-max"), "claude-opus-5-thinking");
        assert_eq!(strip_variant_suffix("claude-opus-5-thinking"), "claude-opus-5");
        assert_eq!(strip_variant_suffix("claude-opus-5"), "claude-opus-5");
        assert_eq!(strip_variant_suffix("gpt-5.6-sol-none-fast"), "gpt-5.6-sol-none");
        assert_eq!(strip_variant_suffix("kimi-k3"), "kimi-k3");
    }

    #[test]
    fn family_base_collapses_variants() {
        assert_eq!(
            ModelRegistry::family_base("claude-opus-5-thinking-max-fast"),
            "claude-opus-5"
        );
        assert_eq!(ModelRegistry::family_base("gpt-5.4-mini-none"), "gpt-5.4-mini");
        assert_eq!(ModelRegistry::family_base("gemini-3-flash"), "gemini-3-flash");
        assert_eq!(ModelRegistry::family_base("kimi-k2.7-code"), "kimi-k2.7-code");
    }

    #[test]
    fn resolve_smart_model_picks_variant_by_thinking_level() {
        use serde_json::json;
        let upstream: Vec<String> = vec![
            "claude-opus-5-low",
            "claude-opus-5-high",
            "claude-opus-5-high-fast",
            "claude-opus-5-thinking-max",
            "gpt-5.4-low",
            "gpt-5.4-medium",
            "gpt-5.4-xhigh",
            "gemini-3-flash",
            "composer-2.5",
            "composer-2.5-fast",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect();
        // 显式 thinking_level 优先
        let b = json!({"model":"claude-opus-5","thinking_level":"low","messages":[]});
        assert_eq!(
            ModelRegistry::resolve_smart_model("claude-opus-5", &b, &upstream).as_deref(),
            Some("claude-opus-5-low")
        );
        // reasoning_effort (OpenAI 习惯)
        let b = json!({"model":"claude-opus-5","reasoning_effort":"high","messages":[]});
        assert_eq!(
            ModelRegistry::resolve_smart_model("claude-opus-5", &b, &upstream).as_deref(),
            Some("claude-opus-5-high")
        );
        // max_mode → Max, opus-5 无 -max 变体 → 回落 xhigh 无 → high
        let b = json!({"model":"claude-opus-5","max_mode":true,"messages":[]});
        assert_eq!(
            ModelRegistry::resolve_smart_model("claude-opus-5", &b, &upstream).as_deref(),
            Some("claude-opus-5-high")
        );
        // gpt-5.4 Max → 无 -max, 有 -xhigh
        let b = json!({"model":"gpt-5.4","max_mode":true,"messages":[]});
        assert_eq!(
            ModelRegistry::resolve_smart_model("gpt-5.4", &b, &upstream).as_deref(),
            Some("gpt-5.4-xhigh")
        );
        // 基名本身在上游名单 → 不重写
        let b = json!({"model":"gemini-3-flash","messages":[]});
        assert_eq!(ModelRegistry::resolve_smart_model("gemini-3-flash", &b, &upstream), None);
        // 客户端已传变体全名 → 不重写
        let b = json!({"model":"claude-opus-5-high-fast","messages":[]});
        assert_eq!(
            ModelRegistry::resolve_smart_model("claude-opus-5-high-fast", &b, &upstream),
            None
        );
        // 无档位变体的家族: 基名在上游名单里 → 不重写 (上游自己处理)
        let b = json!({"model":"composer-2.5","messages":[]});
        assert_eq!(ModelRegistry::resolve_smart_model("composer-2.5", &b, &upstream), None);
        // 基名不在但 -fast 在 → 路由到 -fast
        let b = json!({"model":"foo-9","messages":[]});
        let up2: Vec<String> = vec!["foo-9-fast".to_string()];
        assert_eq!(
            ModelRegistry::resolve_smart_model("foo-9", &b, &up2).as_deref(),
            Some("foo-9-fast")
        );
        // 上游名单里完全没有的家族 → 不路由 (原样透传, 让上游报错)
        let b = json!({"model":"no-such-family","thinking_level":"low","messages":[]});
        assert_eq!(ModelRegistry::resolve_smart_model("no-such-family", &b, &upstream), None);
        // 上游名单为空 → 不路由 (未拉过 AvailableModels 时行为同旧版)
        let b = json!({"model":"claude-opus-5","thinking_level":"low","messages":[]});
        assert_eq!(ModelRegistry::resolve_smart_model("claude-opus-5", &b, &[]), None);
    }

    #[test]
    fn visible_models_collapses_to_family_bases() {
        let reg = ModelRegistry::empty();
        let upstream: Vec<String> = vec![
            "claude-opus-5-low",
            "claude-opus-5-high",
            "claude-opus-5-thinking-max",
            "gpt-5.4-low-fast",
            "gpt-5.4-xhigh",
            "gemini-3-flash",
            "kimi-k2.7-code",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect();
        let vis = reg.visible_models(&upstream, &[]);
        assert_eq!(vis, vec!["claude-opus-5", "gpt-5.4", "gemini-3-flash", "kimi-k2.7-code"]);
        // 注册表手动条目 (enabled) 即使不在上游也列出
        reg.upsert_model(ModelEntry {
            model: "my-custom".into(),
            input_per_m: 1.0,
            output_per_m: 2.0,
            cache_read_per_m: 0.0,
            cache_write_per_m: 0.0,
            enabled: true,
            upstream: false,
            note: String::new(),
        })
        .unwrap();
        let vis2 = reg.visible_models(&upstream, &[]);
        assert!(vis2.iter().any(|m| m == "my-custom"));
        let _ = reg.delete_model("my-custom");
    }

    #[test]
    fn visibility_rules() {
        let r = reg();
        let up = vec!["kimi-k3-low".to_string(), "claude-opus-5-low".to_string()];
        // 上游名单直接可见
        assert!(r.is_visible("kimi-k3-low", &up));
        // 变体收敛: claude-opus-5-thinking-max-fast 的家族基名 claude-opus-5
        // 在上游有变体 (claude-opus-5-low) → 可见 (智能路由会接管)
        assert!(r.is_visible("claude-opus-5-thinking-max-fast", &up));
        // 但家族基名不在上游的纯别名仍不可见
        // 上游有 kimi-k3-low → kimi-k3-low-fast 剥 -fast 命中 → 可见
        assert!(r.is_visible("kimi-k3-low-fast", &up));
        // 纯别名 (上游不存在) 不可见
        assert!(!r.is_visible("fable-5", &up));
        assert!(!r.is_visible("gpt-5", &up));
        // 注册表条目看 enabled
        r.upsert_model(entry("my-custom", 1.0, 2.0, "")).unwrap();
        assert!(r.is_visible("my-custom", &[]));
        let mut e = entry("my-custom", 1.0, 2.0, "");
        e.enabled = false;
        r.upsert_model(e).unwrap();
        assert!(!r.is_visible("my-custom", &[]));
    }

    #[test]
    fn lookup_exact_then_longest_prefix() {
        let r = reg();
        r.upsert_model(entry("claude-opus", 1.0, 2.0, "economy"))
            .unwrap();
        r.upsert_model(entry("claude-opus-5-fast", 10.0, 20.0, "standard"))
            .unwrap();
        assert_eq!(r.lookup("claude-opus-5-fast").unwrap().input_per_m, 10.0);
        assert_eq!(r.lookup("claude-opus-5").unwrap().input_per_m, 1.0);
        assert!(r.lookup("gpt-5").is_none());
    }

    #[test]
    fn groups_gate() {
        let r = reg();
        r.upsert_group(ModelGroup {
            id: "cheap".into(),
            name: "便宜".into(),
            members: vec!["kimi-*".into(), "grok-4.6".into()],
            note: String::new(),
        })
        .unwrap();
        r.upsert_group(ModelGroup {
            id: "opus".into(),
            name: "opus".into(),
            members: vec!["claude-opus-*".into()],
            note: String::new(),
        })
        .unwrap();
        let none: Vec<String> = vec![];
        assert!(r.allowed_by_groups(&none, "claude-fable-5")); // 不限
        let cheap = vec!["cheap".to_string()];
        assert!(r.allowed_by_groups(&cheap, "kimi-k3-high"));
        assert!(r.allowed_by_groups(&cheap, "grok-4.6"));
        assert!(!r.allowed_by_groups(&cheap, "claude-opus-5"));
        let both = vec!["cheap".to_string(), "opus".to_string()];
        assert!(r.allowed_by_groups(&both, "claude-opus-5"));
        // 引用不存在的组 = 不放行
        let ghost = vec!["ghost".to_string()];
        assert!(!r.allowed_by_groups(&ghost, "kimi-k3-high"));
        assert_eq!(r.groups_of("claude-opus-5"), vec!["opus".to_string()]);
        assert!(r.delete_group("opus").unwrap());
        assert!(!r.allowed_by_groups(&both, "claude-opus-5"));
    }

    #[test]
    fn disabled_model() {
        let r = reg();
        let mut e = entry("gpt-5.4-pro", 30.0, 180.0, "flagship");
        e.enabled = false;
        r.upsert_model(e).unwrap();
        assert!(r.is_disabled("gpt-5.4-pro"));
        assert!(!r.is_disabled("kimi-k3"));
    }

    #[test]
    fn persist_roundtrip() {
        let dir =
            std::env::temp_dir().join(format!("cfp-models-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("models.json");
        let r = ModelRegistry::open(&p);
        r.upsert_model(entry("kimi-k3", 3.0, 15.0, "economy"))
            .unwrap();
        r.upsert_group(ModelGroup {
            id: "g".into(),
            name: "g".into(),
            members: vec!["kimi-*".into()],
            note: String::new(),
        })
        .unwrap();
        let r2 = ModelRegistry::open(&p);
        assert_eq!(r2.lookup("kimi-k3-high").unwrap().output_per_m, 15.0);
        assert_eq!(r2.groups().len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
