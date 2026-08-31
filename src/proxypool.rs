//! IP 代理池: 出口节点 + 账号分配规则 + 探测状态.
//!
//! 规则优先级:
//! 1. 账号手动绑定 `Account.proxy_id` (sticky 直到手动改)
//! 2. 自动规则: exclusive > hash > round_robin > least_accounts
//! 3. 未命中且 `require_proxy=false` 时直连

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProxyKind {
    #[default]
    Http,
    Https,
    Socks5,
}

impl ProxyKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ProxyKind::Http => "http",
            ProxyKind::Https => "https",
            ProxyKind::Socks5 => "socks5",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AssignMode {
    /// 账号哈希到可用节点, 同一账号尽量固定出口
    #[default]
    Hash,
    /// 轮询
    RoundRobin,
    /// 优先挂到当前绑定账号最少的节点
    LeastAccounts,
    /// 一号一 IP: 每个节点最多绑 max_accounts 个账号 (默认 1)
    Exclusive,
}

impl AssignMode {
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "round_robin" | "rr" | "roundrobin" => AssignMode::RoundRobin,
            "least" | "least_accounts" | "least-accounts" => AssignMode::LeastAccounts,
            "exclusive" | "1:1" | "one_to_one" => AssignMode::Exclusive,
            _ => AssignMode::Hash,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyNode {
    pub id: String,
    pub url: String,
    #[serde(default)]
    pub kind: ProxyKind,
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// 该节点最多绑定多少账号; 0 = 不限
    #[serde(default)]
    pub max_accounts: u32,
    #[serde(default)]
    pub note: String,
}

fn default_true() -> bool {
    true
}

impl ProxyNode {
    pub fn sanitized(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id,
            "url": mask_proxy_url(&self.url),
            "kind": self.kind,
            "region": self.region,
            "tags": self.tags,
            "enabled": self.enabled,
            "max_accounts": self.max_accounts,
            "note": self.note,
        })
    }
}

pub fn mask_proxy_url(url: &str) -> String {
    // http://user:pass@host:port → http://user:***@host:port
    if let Some((scheme, rest)) = url.split_once("://") {
        if let Some((creds, host)) = rest.split_once('@') {
            let user = creds.split(':').next().unwrap_or("user");
            return format!("{scheme}://{user}:***@{host}");
        }
        return format!("{scheme}://{rest}");
    }
    url.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyAssignRule {
    pub id: String,
    #[serde(default)]
    pub enabled: bool,
    /// 匹配账号 id 前缀; 空 = 匹配全部
    #[serde(default)]
    pub account_prefix: String,
    /// 匹配账号 tags (任一命中); 空 = 不限
    #[serde(default)]
    pub account_tags: Vec<String>,
    /// 限定节点 tags (任一命中); 空 = 全部启用节点
    #[serde(default)]
    pub proxy_tags: Vec<String>,
    /// 限定节点 region; 空 = 不限
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub mode: AssignMode,
    /// 优先级, 数字越大越先匹配
    #[serde(default)]
    pub priority: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyPoolConfig {
    #[serde(default)]
    pub enabled: bool,
    /// true = 没分到代理就拒绝请求; false = 回退直连
    #[serde(default)]
    pub require_proxy: bool,
    /// 自动分配默认模式
    #[serde(default)]
    pub default_mode: AssignMode,
    /// 后台探测间隔秒; 0 = 关闭
    #[serde(default = "default_probe_interval")]
    pub probe_interval_s: u64,
    /// 探测超时毫秒
    #[serde(default = "default_probe_timeout_ms")]
    pub probe_timeout_ms: u64,
    /// 连续失败几次标记 unhealthy
    #[serde(default = "default_fail_threshold")]
    pub fail_threshold: u32,
    #[serde(default)]
    pub nodes: Vec<ProxyNode>,
    #[serde(default)]
    pub rules: Vec<ProxyAssignRule>,
}

fn default_probe_interval() -> u64 {
    60
}
fn default_probe_timeout_ms() -> u64 {
    8000
}
fn default_fail_threshold() -> u32 {
    3
}

impl Default for ProxyPoolConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            require_proxy: false,
            default_mode: AssignMode::Hash,
            probe_interval_s: default_probe_interval(),
            probe_timeout_ms: default_probe_timeout_ms(),
            fail_threshold: default_fail_threshold(),
            nodes: Vec::new(),
            rules: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ProxyHealth {
    pub id: String,
    pub ok: bool,
    pub latency_ms: Option<u64>,
    pub egress_ip: Option<String>,
    pub error: Option<String>,
    pub consecutive_fails: u64,
    pub last_ok_unix: Option<u64>,
    pub last_fail_unix: Option<u64>,
    pub bound_accounts: usize,
}

#[derive(Default)]
struct NodeRuntime {
    consecutive_fails: AtomicU64,
    last_ok_unix: AtomicU64,
    last_fail_unix: AtomicU64,
    last_latency_ms: AtomicU64,
    last_ok: AtomicU64, // 1/0
    last_error: parking_lot::Mutex<Option<String>>,
    egress_ip: parking_lot::Mutex<Option<String>>,
}

impl NodeRuntime {
    fn snapshot(&self, id: &str, bound: usize) -> ProxyHealth {
        let ok = self.last_ok.load(Ordering::Relaxed) == 1
            || self.consecutive_fails.load(Ordering::Relaxed) == 0
                && self.last_ok_unix.load(Ordering::Relaxed) == 0;
        ProxyHealth {
            id: id.to_string(),
            ok,
            latency_ms: {
                let v = self.last_latency_ms.load(Ordering::Relaxed);
                if v == 0 {
                    None
                } else {
                    Some(v)
                }
            },
            egress_ip: self.egress_ip.lock().clone(),
            error: self.last_error.lock().clone(),
            consecutive_fails: self.consecutive_fails.load(Ordering::Relaxed),
            last_ok_unix: nz(self.last_ok_unix.load(Ordering::Relaxed)),
            last_fail_unix: nz(self.last_fail_unix.load(Ordering::Relaxed)),
            bound_accounts: bound,
        }
    }
}

fn cap_for(n: &ProxyNode, mode: AssignMode) -> Option<usize> {
    if mode == AssignMode::Exclusive {
        Some(if n.max_accounts == 0 {
            1
        } else {
            n.max_accounts as usize
        })
    } else if n.max_accounts == 0 {
        None
    } else {
        Some(n.max_accounts as usize)
    }
}

fn nz(v: u64) -> Option<u64> {
    if v == 0 {
        None
    } else {
        Some(v)
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub struct ProxyPool {
    cfg: arc_swap::ArcSwap<ProxyPoolConfig>,
    write: parking_lot::Mutex<()>,
    runtime: DashMap<String, Arc<NodeRuntime>>,
    /// account_id -> proxy_id 运行时绑定 (含自动分配结果)
    bindings: DashMap<String, String>,
    rr: AtomicUsize,
}

impl ProxyPool {
    pub fn new(cfg: ProxyPoolConfig) -> Self {
        let pool = Self {
            cfg: arc_swap::ArcSwap::from_pointee(cfg),
            write: parking_lot::Mutex::new(()),
            runtime: DashMap::new(),
            bindings: DashMap::new(),
            rr: AtomicUsize::new(0),
        };
        pool.sync_runtime();
        pool
    }

    pub fn load(&self) -> Arc<ProxyPoolConfig> {
        self.cfg.load_full()
    }

    pub fn replace(&self, cfg: ProxyPoolConfig) {
        let _g = self.write.lock();
        self.cfg.store(Arc::new(cfg));
        self.sync_runtime();
    }

    fn sync_runtime(&self) {
        let cfg = self.cfg.load();
        let mut seen = std::collections::HashSet::new();
        for n in &cfg.nodes {
            seen.insert(n.id.clone());
            self.runtime
                .entry(n.id.clone())
                .or_insert_with(|| Arc::new(NodeRuntime::default()));
        }
        self.runtime.retain(|k, _| seen.contains(k));
        // 节点被删则清绑定
        self.bindings.retain(|_, pid| seen.contains(pid));
    }

    pub fn set_binding(&self, account_id: &str, proxy_id: Option<&str>) {
        match proxy_id {
            Some(pid) if !pid.is_empty() => {
                self.bindings
                    .insert(account_id.to_string(), pid.to_string());
            }
            _ => {
                self.bindings.remove(account_id);
            }
        }
    }

    pub fn binding_of(&self, account_id: &str) -> Option<String> {
        self.bindings.get(account_id).map(|r| r.clone())
    }

    pub fn bound_count(&self, proxy_id: &str) -> usize {
        self.bindings.iter().filter(|r| r.value() == proxy_id).count()
    }

    pub fn node(&self, id: &str) -> Option<ProxyNode> {
        self.cfg.load().nodes.iter().find(|n| n.id == id).cloned()
    }

    pub fn is_enabled(&self) -> bool {
        self.cfg.load().enabled && !self.cfg.load().nodes.is_empty()
    }

    pub fn require_proxy(&self) -> bool {
        self.cfg.load().require_proxy
    }

    /// 解析账号应走的代理. None = 直连.
    pub fn resolve(
        &self,
        account_id: &str,
        account_proxy_id: Option<&str>,
        account_tags: &[String],
    ) -> Result<Option<ProxyNode>, String> {
        let cfg = self.cfg.load();
        if !cfg.enabled {
            return Ok(None);
        }
        if let Some(pid) = account_proxy_id.filter(|s| !s.is_empty()) {
            return match cfg.nodes.iter().find(|n| n.id == pid) {
                Some(n) if n.enabled => {
                    self.set_binding(account_id, Some(&n.id));
                    Ok(Some(n.clone()))
                }
                Some(_) => Err(format!("proxy '{pid}' is disabled")),
                None => Err(format!("proxy '{pid}' not found")),
            };
        }
        if let Some(pid) = self.binding_of(account_id) {
            if let Some(n) = cfg.nodes.iter().find(|x| x.id == pid && x.enabled) {
                if self.node_healthy(&n.id) {
                    return Ok(Some(n.clone()));
                }
            }
            // 绑定节点已挂, 丢掉自动绑定重新分配
            self.bindings.remove(account_id);
        }
        if let Some(n) = self.auto_assign(account_id, account_tags, &cfg) {
            self.set_binding(account_id, Some(&n.id));
            return Ok(Some(n));
        }
        if cfg.require_proxy {
            return Err("no eligible proxy for account".into());
        }
        Ok(None)
    }

    fn node_healthy(&self, id: &str) -> bool {
        let cfg = self.cfg.load();
        match self.runtime.get(id) {
            Some(rt) => rt.consecutive_fails.load(Ordering::Relaxed) < cfg.fail_threshold as u64,
            None => true,
        }
    }

    fn auto_assign(
        &self,
        account_id: &str,
        account_tags: &[String],
        cfg: &ProxyPoolConfig,
    ) -> Option<ProxyNode> {
        let mut rules: Vec<&ProxyAssignRule> = cfg.rules.iter().filter(|r| r.enabled).collect();
        rules.sort_by_key(|r| std::cmp::Reverse(r.priority));
        for rule in rules {
            if !rule.account_prefix.is_empty() && !account_id.starts_with(&rule.account_prefix) {
                continue;
            }
            if !rule.account_tags.is_empty()
                && !rule
                    .account_tags
                    .iter()
                    .any(|t| account_tags.iter().any(|a| a == t))
            {
                continue;
            }
            let cands = self.candidates(cfg, &rule.proxy_tags, &rule.region, rule.mode);
            if let Some(n) = self.pick(account_id, &cands, rule.mode) {
                return Some(n);
            }
        }
        let cands = self.candidates(cfg, &[], "", cfg.default_mode);
        self.pick(account_id, &cands, cfg.default_mode)
    }

    fn candidates(
        &self,
        cfg: &ProxyPoolConfig,
        tags: &[String],
        region: &str,
        mode: AssignMode,
    ) -> Vec<ProxyNode> {
        cfg.nodes
            .iter()
            .filter(|n| n.enabled)
            .filter(|n| self.node_healthy(&n.id))
            .filter(|n| region.is_empty() || n.region == region)
            .filter(|n| {
                tags.is_empty()
                    || tags.iter().any(|t| n.tags.iter().any(|nt| nt == t))
            })
            .filter(|n| {
                let Some(cap) = cap_for(n, mode) else {
                    return true;
                };
                let bound = self.bound_count(&n.id);
                bound < cap
            })
            .cloned()
            .collect()
    }

    fn pick(&self, account_id: &str, cands: &[ProxyNode], mode: AssignMode) -> Option<ProxyNode> {
        if cands.is_empty() {
            return None;
        }
        match mode {
            AssignMode::Hash => {
                let mut h = std::collections::hash_map::DefaultHasher::new();
                account_id.hash(&mut h);
                let idx = (h.finish() as usize) % cands.len();
                Some(cands[idx].clone())
            }
            AssignMode::RoundRobin => {
                let i = self.rr.fetch_add(1, Ordering::Relaxed) % cands.len();
                Some(cands[i].clone())
            }
            AssignMode::LeastAccounts | AssignMode::Exclusive => cands
                .iter()
                .min_by_key(|n| self.bound_count(&n.id))
                .cloned(),
        }
    }

    pub fn record_probe(
        &self,
        id: &str,
        ok: bool,
        latency_ms: Option<u64>,
        egress_ip: Option<String>,
        error: Option<String>,
    ) {
        let rt = self
            .runtime
            .entry(id.to_string())
            .or_insert_with(|| Arc::new(NodeRuntime::default()))
            .clone();
        if ok {
            rt.consecutive_fails.store(0, Ordering::Relaxed);
            rt.last_ok.store(1, Ordering::Relaxed);
            rt.last_ok_unix.store(now_unix(), Ordering::Relaxed);
            if let Some(ms) = latency_ms {
                rt.last_latency_ms.store(ms, Ordering::Relaxed);
            }
            *rt.egress_ip.lock() = egress_ip;
            *rt.last_error.lock() = None;
        } else {
            rt.consecutive_fails.fetch_add(1, Ordering::Relaxed);
            rt.last_ok.store(0, Ordering::Relaxed);
            rt.last_fail_unix.store(now_unix(), Ordering::Relaxed);
            *rt.last_error.lock() = error;
        }
    }

    pub fn overview(&self) -> serde_json::Value {
        let cfg = self.cfg.load();
        let mut nodes = Vec::new();
        let mut healthy = 0usize;
        let mut disabled = 0usize;
        for n in &cfg.nodes {
            let bound = self.bound_count(&n.id);
            let health = self
                .runtime
                .get(&n.id)
                .map(|rt| rt.snapshot(&n.id, bound))
                .unwrap_or_else(|| ProxyHealth {
                    id: n.id.clone(),
                    ok: n.enabled,
                    latency_ms: None,
                    egress_ip: None,
                    error: None,
                    consecutive_fails: 0,
                    last_ok_unix: None,
                    last_fail_unix: None,
                    bound_accounts: bound,
                });
            if !n.enabled {
                disabled += 1;
            } else if health.ok {
                healthy += 1;
            }
            let mut row = n.sanitized();
            if let Some(obj) = row.as_object_mut() {
                obj.insert("health".into(), serde_json::to_value(&health).unwrap_or_default());
                obj.insert("bound_accounts".into(), serde_json::json!(bound));
            }
            nodes.push(row);
        }
        let bindings: HashMap<String, String> = self
            .bindings
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect();
        let findings = self.judge(&cfg, healthy, disabled, &nodes);
        serde_json::json!({
            "enabled": cfg.enabled,
            "require_proxy": cfg.require_proxy,
            "default_mode": cfg.default_mode,
            "probe_interval_s": cfg.probe_interval_s,
            "probe_timeout_ms": cfg.probe_timeout_ms,
            "fail_threshold": cfg.fail_threshold,
            "total": cfg.nodes.len(),
            "healthy": healthy,
            "disabled": disabled,
            "unhealthy": cfg.nodes.len().saturating_sub(healthy + disabled),
            "bound_accounts": bindings.len(),
            "nodes": nodes,
            "rules": cfg.rules,
            "bindings": bindings,
            "judge": findings,
        })
    }

    /// 大规模池的异常判断：重复出口 IP、过载、未探测、死节点仍绑定。
    pub fn judge(
        &self,
        cfg: &ProxyPoolConfig,
        healthy: usize,
        disabled: usize,
        nodes: &[serde_json::Value],
    ) -> serde_json::Value {
        let mut findings: Vec<serde_json::Value> = Vec::new();
        let mut ip_map: HashMap<String, Vec<String>> = HashMap::new();
        let mut overloaded = 0usize;
        let mut never_probed = 0usize;
        let mut dead_bound = 0usize;
        let mut slow = 0usize;
        for n in nodes {
            let id = n["id"].as_str().unwrap_or("").to_string();
            let bound = n["bound_accounts"].as_u64().unwrap_or(0);
            let max = n["max_accounts"].as_u64().unwrap_or(0);
            let health = &n["health"];
            let ok = health["ok"].as_bool().unwrap_or(true);
            let fails = health["consecutive_fails"].as_u64().unwrap_or(0);
            let latency = health["latency_ms"].as_u64();
            if let Some(ip) = health["egress_ip"].as_str() {
                if !ip.is_empty() && ip != "—" {
                    ip_map.entry(ip.to_string()).or_default().push(id.clone());
                }
            } else if n["enabled"].as_bool().unwrap_or(true) {
                never_probed += 1;
            }
            if max > 0 && bound >= max {
                overloaded += 1;
            }
            if !ok && bound > 0 {
                dead_bound += 1;
                findings.push(serde_json::json!({
                    "level": "bad",
                    "code": "dead_bound",
                    "node": id,
                    "msg": format!("{id} 已失败仍绑 {bound} 个号，请求会打到死出口"),
                    "action": "rebalance",
                }));
            }
            if fails >= cfg.fail_threshold as u64 && n["enabled"].as_bool().unwrap_or(true) {
                findings.push(serde_json::json!({
                    "level": "warn",
                    "code": "unhealthy",
                    "node": id,
                    "msg": format!("{id} 连续失败 {fails} 次，已踢出自动分配"),
                    "action": "probe",
                }));
            }
            if let Some(ms) = latency {
                if ms >= 3000 {
                    slow += 1;
                    findings.push(serde_json::json!({
                        "level": "warn",
                        "code": "slow",
                        "node": id,
                        "msg": format!("{id} 延迟 {ms}ms"),
                        "action": "none",
                    }));
                }
            }
        }
        let mut shared_ip = 0usize;
        for (ip, ids) in &ip_map {
            if ids.len() > 1 {
                shared_ip += 1;
                findings.push(serde_json::json!({
                    "level": "warn",
                    "code": "shared_egress",
                    "node": ids.join(","),
                    "msg": format!("出口 IP {ip} 被 {} 个节点共用: {}", ids.len(), ids.join("/")),
                    "action": "none",
                }));
            }
        }
        if cfg.enabled && cfg.nodes.is_empty() {
            findings.push(serde_json::json!({
                "level": "bad",
                "code": "empty_pool",
                "msg": "代理池已开但没有节点",
                "action": "add_node",
            }));
        }
        if cfg.require_proxy && healthy == 0 && !cfg.nodes.is_empty() {
            findings.push(serde_json::json!({
                "level": "bad",
                "code": "no_healthy",
                "msg": "require_proxy 已开且没有健康节点，请求会被拒绝",
                "action": "probe",
            }));
        }
        if never_probed > 0 {
            findings.push(serde_json::json!({
                "level": "info",
                "code": "never_probed",
                "msg": format!("{never_probed} 个启用节点还没探测到出口 IP"),
                "action": "probe",
            }));
        }
        if overloaded > 0 {
            findings.push(serde_json::json!({
                "level": "warn",
                "code": "overloaded",
                "msg": format!("{overloaded} 个节点达到 max_accounts 上限"),
                "action": "rebalance",
            }));
        }
        let severity = if findings.iter().any(|f| f["level"] == "bad") {
            "bad"
        } else if findings.iter().any(|f| f["level"] == "warn") {
            "warn"
        } else {
            "ok"
        };
        findings.sort_by(|a, b| {
            let rank = |l: &str| match l {
                "bad" => 0,
                "warn" => 1,
                _ => 2,
            };
            rank(a["level"].as_str().unwrap_or("")).cmp(&rank(b["level"].as_str().unwrap_or("")))
        });
        serde_json::json!({
            "severity": severity,
            "healthy": healthy,
            "disabled": disabled,
            "shared_egress_groups": shared_ip,
            "overloaded": overloaded,
            "never_probed": never_probed,
            "dead_bound": dead_bound,
            "slow": slow,
            "findings": findings,
            "suggested_action": findings.first().and_then(|f| f["action"].as_str()).unwrap_or("none"),
        })
    }

    pub fn rebalance(&self, account_ids: &[String], tags_of: &HashMap<String, Vec<String>>) -> usize {
        let cfg = self.cfg.load();
        if !cfg.enabled {
            return 0;
        }
        self.bindings.clear();
        let mut n = 0usize;
        for id in account_ids {
            let tags = tags_of.get(id).cloned().unwrap_or_default();
            if self.auto_assign(id, &tags, &cfg).is_some() {
                n += 1;
            }
        }
        n
    }
}

/// 解析 http(s)://[user:pass@]host:port
#[derive(Debug, Clone)]
pub struct ParsedProxy {
    pub host: String,
    pub port: u16,
    pub user: Option<String>,
    pub pass: Option<String>,
    pub https: bool,
    pub kind: ProxyKind,
    /// socks5:// 或 socks5h://
    pub socks5: bool,
}

pub fn parse_proxy_url(url: &str) -> Result<ParsedProxy, String> {
    let url = url.trim();
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| "proxy url must be scheme://host:port".to_string())?;
    let scheme = scheme.to_ascii_lowercase();
    let kind = match scheme.as_str() {
        "http" => ProxyKind::Http,
        "https" => ProxyKind::Https,
        "socks5" | "socks5h" | "socks" => ProxyKind::Socks5,
        _ => return Err(format!("unsupported proxy scheme '{scheme}' (http/https/socks5)")),
    };
    let (creds, hostport) = if let Some((c, h)) = rest.split_once('@') {
        (Some(c), h)
    } else {
        (None, rest)
    };
    let hostport = hostport.trim_end_matches('/');
    let (host, port) = if let Some((h, p)) = hostport.rsplit_once(':') {
        let port: u16 = p
            .parse()
            .map_err(|_| format!("invalid proxy port '{p}'"))?;
        (h.trim_start_matches('[').trim_end_matches(']').to_string(), port)
    } else {
        let default_port = if kind == ProxyKind::Socks5 { 1080 } else if scheme == "https" { 443 } else { 80 };
        (hostport.to_string(), default_port)
    };
    if host.is_empty() {
        return Err("empty proxy host".into());
    }
    let (user, pass) = match creds {
        Some(c) => {
            let (u, p) = c.split_once(':').unwrap_or((c, ""));
            (Some(percent_decode(u)), Some(percent_decode(p)))
        }
        None => (None, None),
    };
    Ok(ParsedProxy {
        host,
        port,
        user,
        pass,
        https: scheme == "https",
        kind,
        socks5: kind == ProxyKind::Socks5,
    })
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(a), Some(b)) = (from_hex(bytes[i + 1]), from_hex(bytes[i + 2])) {
                out.push((a << 4) | b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn from_hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// 经 HTTP CONNECT 隧道连到 target_host:target_port, 再包 TLS.
pub async fn connect_via_http_proxy(
    proxy: &ParsedProxy,
    target_host: &str,
    target_port: u16,
    timeout: Duration,
) -> Result<tokio::net::TcpStream, String> {
    let connect_fut = tokio::net::TcpStream::connect((proxy.host.as_str(), proxy.port));
    let mut stream = tokio::time::timeout(timeout, connect_fut)
        .await
        .map_err(|_| format!("proxy {}:{} connect timeout", proxy.host, proxy.port))?
        .map_err(|e| format!("proxy {}:{} connect: {e}", proxy.host, proxy.port))?;
    stream.set_nodelay(true).ok();

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut req = format!(
        "CONNECT {target_host}:{target_port} HTTP/1.1\r\nHost: {target_host}:{target_port}\r\nProxy-Connection: Keep-Alive\r\n"
    );
    if let (Some(u), Some(p)) = (&proxy.user, &proxy.pass) {
        use base64::Engine;
        let token = base64::engine::general_purpose::STANDARD.encode(format!("{u}:{p}"));
        req.push_str(&format!("Proxy-Authorization: Basic {token}\r\n"));
    }
    req.push_str("\r\n");
    tokio::time::timeout(timeout, stream.write_all(req.as_bytes()))
        .await
        .map_err(|_| "proxy CONNECT write timeout".to_string())?
        .map_err(|e| format!("proxy CONNECT write: {e}"))?;

    let mut buf = vec![0u8; 4096];
    let mut n = 0usize;
    let deadline = Instant::now() + timeout;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err("proxy CONNECT response timeout".into());
        }
        let got = tokio::time::timeout(left, stream.read(&mut buf[n..]))
            .await
            .map_err(|_| "proxy CONNECT response timeout".to_string())?
            .map_err(|e| format!("proxy CONNECT read: {e}"))?;
        if got == 0 {
            return Err("proxy closed during CONNECT".into());
        }
        n += got;
        if let Some(pos) = find_headers_end(&buf[..n]) {
            let head = std::str::from_utf8(&buf[..pos]).unwrap_or("");
            let status = head
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(0);
            if !(200..300).contains(&status) {
                let line = head.lines().next().unwrap_or(head);
                return Err(format!("proxy CONNECT failed: {line}"));
            }
            return Ok(stream);
        }
        if n >= buf.len() {
            return Err("proxy CONNECT response too large".into());
        }
    }
}

fn find_headers_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4)
}

/// 把批量导入里的一行宽松地解析成规范代理 URL. 支持:
/// - `http://user:pass@host:port` / `https://...` / `socks5://...` (原样校验)
/// - `user:pass@host:port`
/// - `host:port`
/// - `host:port:user:pass`
/// - `host,port[,user,pass]` / 空格 / Tab / `;` / `|` 分隔
/// 空行与 `#` `//` 开头的注释行返回 Ok(None).
pub fn parse_proxy_line(line: &str, default_scheme: &str) -> Result<Option<String>, String> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') || line.starts_with("//") {
        return Ok(None);
    }
    // 行尾注释
    let line = line.split(" #").next().unwrap_or(line).trim();
    let scheme = match default_scheme.trim().to_ascii_lowercase().as_str() {
        "https" => "https",
        "socks5" | "socks5h" | "socks" => "socks5",
        _ => "http",
    };
    if line.contains("://") {
        parse_proxy_url(line)?;
        return Ok(Some(line.to_string()));
    }
    let build = |host: &str, port: &str, user: Option<&str>, pass: Option<&str>| -> Result<String, String> {
        let url = match user {
            Some(u) if !u.is_empty() => format!(
                "{scheme}://{}:{}@{}:{}",
                percent_encode_userinfo(u),
                percent_encode_userinfo(pass.unwrap_or("")),
                host.trim(),
                port.trim()
            ),
            _ => format!("{scheme}://{}:{}", host.trim(), port.trim()),
        };
        parse_proxy_url(&url)?;
        Ok(url)
    };
    // user:pass@host:port
    if let Some((creds, hp)) = line.rsplit_once('@') {
        let creds_ok = !creds.contains(|c: char| matches!(c, ',' | '\t' | ' ' | ';' | '|'))
            && creds.matches(':').count() <= 1;
        if let (true, Some((h, po))) = (creds_ok, hp.rsplit_once(':')) {
            if po.parse::<u16>().is_ok() {
                let (u, p) = creds.split_once(':').unwrap_or((creds, ""));
                return build(h, po, Some(u), Some(p)).map(Some);
            }
        }
    }
    // 分隔符列
    let cols: Vec<&str> = line
        .split(|c: char| matches!(c, ',' | '\t' | ' ' | ';' | '|'))
        .map(|c| c.trim())
        .filter(|c| !c.is_empty())
        .collect();
    match cols.len() {
        1 => {
            // host:port 或 host:port:user:pass
            let parts: Vec<&str> = cols[0].split(':').collect();
            match parts.len() {
                2 => build(parts[0], parts[1], None, None).map(Some),
                4 => build(parts[0], parts[1], Some(parts[2]), Some(parts[3])).map(Some),
                3 => build(parts[0], parts[1], Some(parts[2]), Some("")).map(Some),
                _ => Err("expect host:port or host:port:user:pass".into()),
            }
        }
        2 => {
            // host port  或  host:port user:pass
            if cols[0].contains(':') {
                let (h, po) = cols[0].split_once(':').unwrap();
                let (u, p) = cols[1].split_once(':').unwrap_or((cols[1], ""));
                build(h, po, Some(u), Some(p)).map(Some)
            } else {
                build(cols[0], cols[1], None, None).map(Some)
            }
        }
        3 => build(cols[0], cols[1], Some(cols[2]), Some("")).map(Some),
        4 => build(cols[0], cols[1], Some(cols[2]), Some(cols[3])).map(Some),
        n => Err(format!("unexpected {n} columns")),
    }
}

fn percent_encode_userinfo(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'!' | b'$'
            | b'&' | b'\'' | b'(' | b')' | b'*' | b'+' | b',' | b';' | b'=' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mask_hides_password() {
        assert_eq!(
            mask_proxy_url("http://alice:secret@1.2.3.4:8080"),
            "http://alice:***@1.2.3.4:8080"
        );
    }

    #[test]
    fn parse_http_proxy() {
        let p = parse_proxy_url("http://u:p%40x@10.0.0.1:3128").unwrap();
        assert_eq!(p.host, "10.0.0.1");
        assert_eq!(p.port, 3128);
        assert_eq!(p.user.as_deref(), Some("u"));
        assert_eq!(p.pass.as_deref(), Some("p@x"));
    }

    #[test]
    fn hash_is_sticky() {
        let pool = ProxyPool::new(ProxyPoolConfig {
            enabled: true,
            nodes: vec![
                ProxyNode {
                    id: "p1".into(),
                    url: "http://1.1.1.1:8080".into(),
                    kind: ProxyKind::Http,
                    region: "us".into(),
                    tags: vec![],
                    enabled: true,
                    max_accounts: 0,
                    note: String::new(),
                },
                ProxyNode {
                    id: "p2".into(),
                    url: "http://2.2.2.2:8080".into(),
                    kind: ProxyKind::Http,
                    region: "sg".into(),
                    tags: vec![],
                    enabled: true,
                    max_accounts: 0,
                    note: String::new(),
                },
            ],
            default_mode: AssignMode::Hash,
            ..Default::default()
        });
        let a = pool.resolve("acc-7", None, &[]).unwrap().unwrap();
        let b = pool.resolve("acc-7", None, &[]).unwrap().unwrap();
        assert_eq!(a.id, b.id);
        let forced = pool.resolve("acc-7", Some("p2"), &[]).unwrap().unwrap();
        assert_eq!(forced.id, "p2");
    }

    #[test]
    fn exclusive_caps_at_one() {
        let pool = ProxyPool::new(ProxyPoolConfig {
            enabled: true,
            default_mode: AssignMode::Exclusive,
            nodes: vec![ProxyNode {
                id: "only".into(),
                url: "http://1.1.1.1:8080".into(),
                kind: ProxyKind::Http,
                region: String::new(),
                tags: vec![],
                enabled: true,
                max_accounts: 1,
                note: String::new(),
            }],
            ..Default::default()
        });
        let first = pool.resolve("a", None, &[]).unwrap();
        assert!(first.is_some());
        let second = pool.resolve("b", None, &[]);
        assert!(second.unwrap().is_none());
    }

    #[test]
    fn judge_flags_shared_egress() {
        let pool = ProxyPool::new(ProxyPoolConfig {
            enabled: true,
            nodes: vec![
                ProxyNode {
                    id: "a".into(),
                    url: "http://1.1.1.1:8080".into(),
                    kind: ProxyKind::Http,
                    region: String::new(),
                    tags: vec![],
                    enabled: true,
                    max_accounts: 0,
                    note: String::new(),
                },
                ProxyNode {
                    id: "b".into(),
                    url: "http://2.2.2.2:8080".into(),
                    kind: ProxyKind::Http,
                    region: String::new(),
                    tags: vec![],
                    enabled: true,
                    max_accounts: 0,
                    note: String::new(),
                },
            ],
            ..Default::default()
        });
        pool.record_probe("a", true, Some(20), Some("9.9.9.9".into()), None);
        pool.record_probe("b", true, Some(20), Some("9.9.9.9".into()), None);
        let ov = pool.overview();
        assert_eq!(ov["judge"]["shared_egress_groups"], 1);
        assert_eq!(ov["judge"]["severity"], "warn");
    }

    #[test]
    fn import_line_formats() {
        let ok = |l: &str| parse_proxy_line(l, "http").unwrap().unwrap();
        assert_eq!(ok("http://u:p@1.2.3.4:8080"), "http://u:p@1.2.3.4:8080");
        assert_eq!(ok("1.2.3.4:8080"), "http://1.2.3.4:8080");
        assert_eq!(ok("1.2.3.4:8080:u:p"), "http://u:p@1.2.3.4:8080");
        assert_eq!(ok("u:p@1.2.3.4:8080"), "http://u:p@1.2.3.4:8080");
        assert_eq!(ok("1.2.3.4,8080,u,p"), "http://u:p@1.2.3.4:8080");
        assert_eq!(ok("1.2.3.4\t8080\tu\tp"), "http://u:p@1.2.3.4:8080");
        assert_eq!(ok("1.2.3.4 8080"), "http://1.2.3.4:8080");
        assert_eq!(ok("1.2.3.4:8080 u:p"), "http://u:p@1.2.3.4:8080");
        assert_eq!(ok("1.2.3.4:8080:u:p@ss"), "http://u:p%40ss@1.2.3.4:8080");
        assert_eq!(parse_proxy_line("1.2.3.4:8080", "https").unwrap().unwrap(), "https://1.2.3.4:8080");
        assert!(parse_proxy_line("", "http").unwrap().is_none());
        assert!(parse_proxy_line("# c", "http").unwrap().is_none());
        assert!(parse_proxy_line("1.2.3.4:abc", "http").is_err());
        assert_eq!(ok("socks5://u:p@1.2.3.4:1080"), "socks5://u:p@1.2.3.4:1080");
        assert_eq!(parse_proxy_line("1.2.3.4:1080:u:p", "socks5").unwrap().unwrap(), "socks5://u:p@1.2.3.4:1080");
        assert!(parse_proxy_line("ftp://1.2.3.4:21", "http").is_err());
        let sp = parse_proxy_url("socks5://1.2.3.4").unwrap();
        assert!(sp.socks5);
        assert_eq!(sp.port, 1080);
        let p = parse_proxy_url(&ok("1.2.3.4:8080:u:p@ss")).unwrap();
        assert_eq!(p.pass.as_deref(), Some("p@ss"));
    }
}
