//! 智能健康诊断引擎：聚合号池、额度、代理、日志、配置，输出分级诊断 + 可执行建议。
//!
//! 设计原则：
//! - **只读诊断**：不自动执行修复动作，只生成建议（自动处理由用户确认后走现有 API）。
//! - **维度覆盖**：IP（代理池）、账号（冷却/错误/额度/禁用）、调用（RPM/延迟/日志错误模式）。
//! - **分级输出**：critical / warning / info，按 severity 排序。
//! - **数据驱动**：所有判断基于现有 pool / quota / proxypool / logbuf / config 的真实数据。

use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::config::ConfigCell;
use crate::logbuf::LogBuffer;
use crate::pool::AccountPool;
use crate::proxypool::ProxyPool;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Severity::Info => write!(f, "info"),
            Severity::Warning => write!(f, "warning"),
            Severity::Critical => write!(f, "critical"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Finding {
    /// 维度: "ip" | "account" | "call" | "system" | "config"
    pub category: String,
    pub severity: Severity,
    /// 问题简述
    pub message: String,
    /// 相关对象（账号ID、代理ID、错误码等）
    pub context: serde_json::Value,
    /// 可执行建议（对应现有 admin API）
    pub suggestion: Option<String>,
    /// 建议的 API 调用路径（前端可一键执行）
    pub action: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthReport {
    pub ok: bool,
    pub score: u8,
    pub findings: Vec<Finding>,
    pub summary: HealthSummary,
    pub generated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthSummary {
    pub total_accounts: usize,
    pub healthy_accounts: usize,
    pub critical_count: usize,
    pub warning_count: usize,
    pub info_count: usize,
}

/// 诊断引擎：无状态，每次调用实时聚合数据
pub struct HealthEngine;

impl HealthEngine {
    /// 执行完整诊断
    pub async fn diagnose(
        pool: &AccountPool,
        proxy_pool: &Arc<ProxyPool>,
        logbuf: &Arc<LogBuffer>,
        config: &ConfigCell,
    ) -> HealthReport {
        let mut findings = Vec::new();

        // ── 1. 账号维度 ──
        Self::check_accounts(pool, &mut findings);

        // ── 2. IP / 代理维度 ──
        Self::check_proxies(proxy_pool, config, &mut findings).await;

        // ── 3. 调用 / 日志维度 ──
        Self::check_logs(logbuf, &mut findings);

        // ── 4. 系统 / 配置维度 ──
        Self::check_config(config, pool, &mut findings);

        // 排序：critical > warning > info
        findings.sort_by(|a, b| b.severity.cmp(&a.severity));

        let critical = findings.iter().filter(|f| f.severity == Severity::Critical).count();
        let warning = findings.iter().filter(|f| f.severity == Severity::Warning).count();
        let info = findings.iter().filter(|f| f.severity == Severity::Info).count();

        let summary_data = pool.summary();
        let total = summary_data["total_accounts"].as_u64().unwrap_or(0) as usize;
        let available = summary_data["available"].as_u64().unwrap_or(0) as usize;

        // 健康分: 基础 100, critical -20, warning -5, info -1, 可用账号比例加成
        let mut score: i32 = 100;
        score -= (critical * 20) as i32;
        score -= (warning * 5) as i32;
        score -= (info * 1) as i32;
        if total > 0 {
            let ratio = available as f64 / total as f64;
            score = (score as f64 * ratio.max(0.3)) as i32; // 至少保留 30% 基础分
        }
        let score = score.max(0).min(100) as u8;

        HealthReport {
            ok: critical == 0,
            score,
            findings,
            summary: HealthSummary {
                total_accounts: total,
                healthy_accounts: available,
                critical_count: critical,
                warning_count: warning,
                info_count: info,
            },
            generated_at: chrono::Utc::now().to_rfc3339(),
        }
    }

    /// 账号维度检测
    fn check_accounts(pool: &AccountPool, findings: &mut Vec<Finding>) {
        let summary = pool.summary();
        let total = summary["total_accounts"].as_u64().unwrap_or(0) as usize;
        let available = summary["available"].as_u64().unwrap_or(0) as usize;
        let disabled = summary["disabled"].as_u64().unwrap_or(0) as usize;
        let cooling = summary["cooling"].as_u64().unwrap_or(0) as usize;
        let quota_blocked = summary["quota_blocked"].as_u64().unwrap_or(0) as usize;
        let erroring = summary["erroring"].as_u64().unwrap_or(0) as usize;
        let total_requests = summary["total_requests"].as_u64().unwrap_or(0);
        let total_errors = summary["total_errors"].as_u64().unwrap_or(0);

        // 1.1 可用账号不足
        if total == 0 {
            findings.push(Finding {
                category: "account".into(),
                severity: Severity::Critical,
                message: "号池为空：没有配置任何账号".into(),
                context: serde_json::json!({"total": 0}),
                suggestion: Some("导入账号：使用批量导入或添加账号".into()),
                action: Some(serde_json::json!({
                    "method": "POST",
                    "path": "/admin/api/accounts/import",
                    "label": "导入账号"
                })),
            });
        } else if available == 0 {
            findings.push(Finding {
                category: "account".into(),
                severity: Severity::Critical,
                message: format!("号池耗尽：{} 个账号全部不可用", total),
                context: serde_json::json!({
                    "total": total,
                    "disabled": disabled,
                    "cooling": cooling,
                    "quota_blocked": quota_blocked,
                }),
                suggestion: Some("检查账号状态：清除冷却 / 刷新额度 / 启用禁用账号".into()),
                action: Some(serde_json::json!({
                    "method": "POST",
                    "path": "/admin/api/accounts/probe-all",
                    "label": "全部探测"
                })),
            });
        } else if available < total / 2 && total >= 4 {
            findings.push(Finding {
                category: "account".into(),
                severity: Severity::Warning,
                message: format!("可用账号不足半数：{}/{} 可用", available, total),
                context: serde_json::json!({"available": available, "total": total}),
                suggestion: Some("检查冷却和额度状态，考虑扩容账号".into()),
                action: None,
            });
        }

        // 1.2 高错误率
        if total_requests >= 10 {
            let error_rate = total_errors as f64 / total_requests as f64;
            if error_rate > 0.5 {
                findings.push(Finding {
                    category: "account".into(),
                    severity: Severity::Critical,
                    message: format!("账号错误率过高：{:.1}%（{}/{}）", error_rate * 100.0, total_errors, total_requests),
                    context: serde_json::json!({"error_rate": error_rate, "errors": total_errors, "requests": total_requests}),
                    suggestion: Some("大量账号可能已失效，建议全部探测或更换账号".into()),
                    action: Some(serde_json::json!({
                        "method": "POST",
                        "path": "/admin/api/accounts/probe-all",
                        "label": "全部探测"
                    })),
                });
            } else if error_rate > 0.2 {
                findings.push(Finding {
                    category: "account".into(),
                    severity: Severity::Warning,
                    message: format!("账号错误率偏高：{:.1}%", error_rate * 100.0),
                    context: serde_json::json!({"error_rate": error_rate}),
                    suggestion: Some("部分账号可能不稳定，关注错误日志".into()),
                    action: None,
                });
            }
        }

        // 1.3 冷却账号过多
        if cooling > 0 && cooling >= total / 2 && total >= 2 {
            findings.push(Finding {
                category: "account".into(),
                severity: Severity::Warning,
                message: format!("大量账号冷却中：{}/{}", cooling, total),
                context: serde_json::json!({"cooling": cooling, "total": total}),
                suggestion: Some("上游可能限流，检查请求频率或等待冷却结束".into()),
                action: None,
            });
        }

        // 1.4 额度耗尽账号过多
        if quota_blocked > 0 && quota_blocked >= total / 2 && total >= 2 {
            findings.push(Finding {
                category: "account".into(),
                severity: Severity::Warning,
                message: format!("大量账号额度耗尽：{}/{}", quota_blocked, total),
                context: serde_json::json!({"quota_blocked": quota_blocked, "total": total}),
                suggestion: Some("刷新额度状态，或等待额度重置".into()),
                action: Some(serde_json::json!({
                    "method": "POST",
                    "path": "/admin/api/quota/refresh",
                    "label": "刷新额度"
                })),
            });
        }

        // 1.5 错误账号详情（取前 5 个）
        if erroring > 0 {
            let all_scores = pool.all_health_scores();
            let mut error_accounts: Vec<_> = all_scores
                .iter()
                .filter(|a| a["health"]["consecutive_errors"].as_u64().unwrap_or(0) > 0)
                .collect();
            error_accounts.sort_by(|a, b| {
                b["health"]["consecutive_errors"]
                    .as_u64()
                    .unwrap_or(0)
                    .cmp(&a["health"]["consecutive_errors"].as_u64().unwrap_or(0))
            });
            let top: Vec<_> = error_accounts.iter().take(5).collect();
            if !top.is_empty() {
                let names: Vec<_> = top.iter().map(|a| a["id"].as_str().unwrap_or("?")).collect();
                findings.push(Finding {
                    category: "account".into(),
                    severity: if erroring >= 3 { Severity::Critical } else { Severity::Warning },
                    message: format!("{} 个账号连续错误：{}", erroring, names.join(", ")),
                    context: serde_json::json!({"erroring": erroring, "top_accounts": names}),
                    suggestion: Some("检查这些账号的 token 是否失效，或手动禁用后重新导入".into()),
                    action: None,
                });
            }
        }
    }

    /// IP / 代理维度检测
    async fn check_proxies(
        proxy_pool: &Arc<ProxyPool>,
        config: &ConfigCell,
        findings: &mut Vec<Finding>,
    ) {
        let cfg = config.load();
        let proxy_enabled = cfg.proxy.enabled;
        let require_proxy = cfg.proxy.require_proxy;
        let total_nodes = cfg.proxy.nodes.len();
        let enabled_nodes = cfg.proxy.nodes.iter().filter(|n| n.enabled).count();
        drop(cfg);

        // 2.1 代理池未启用但配置了节点
        if !proxy_enabled && total_nodes > 0 {
            findings.push(Finding {
                category: "ip".into(),
                severity: Severity::Info,
                message: format!("代理池未启用（{} 个节点已配置）", total_nodes),
                context: serde_json::json!({"enabled": false, "total_nodes": total_nodes}),
                suggestion: Some("启用代理池以分散出口 IP，降低封禁风险".into()),
                action: Some(serde_json::json!({
                    "method": "POST",
                    "path": "/admin/api/proxies",
                    "body": {"enabled": true},
                    "label": "启用代理池"
                })),
            });
        }

        // 2.2 require_proxy 但无可用节点
        if require_proxy && enabled_nodes == 0 {
            findings.push(Finding {
                category: "ip".into(),
                severity: Severity::Critical,
                message: "require_proxy=true 但无可用代理节点".into(),
                context: serde_json::json!({"require_proxy": true, "enabled_nodes": 0}),
                suggestion: Some("添加并启用代理节点，或关闭 require_proxy".into()),
                action: None,
            });
        }

        // 2.3 代理节点健康检查（复用现有 overview + judge）
        if proxy_enabled && enabled_nodes > 0 {
            let overview = proxy_pool.overview();
            let nodes = overview["nodes"].as_array().cloned().unwrap_or_default();
            let judge = &overview["judge"];

            // 从 judge 结果提取 critical/warning
            if let Some(judge_findings) = judge["findings"].as_array() {
                for jf in judge_findings {
                    let level = jf["level"].as_str().unwrap_or("info");
                    let code = jf["code"].as_str().unwrap_or("");
                    let msg = jf["msg"].as_str().unwrap_or("").to_string();
                    let action = jf["action"].as_str().unwrap_or("none");

                    let severity = match level {
                        "bad" => Severity::Critical,
                        "warn" => Severity::Warning,
                        _ => Severity::Info,
                    };

                    let suggestion = match action {
                        "rebalance" => Some("执行 rebalance 重新分配账号到健康节点".to_string()),
                        "probe" => Some("重新探测节点确认状态".to_string()),
                        "add_node" => Some("添加代理节点".to_string()),
                        _ => None,
                    };

                    let action_json = match action {
                        "rebalance" => Some(serde_json::json!({
                            "method": "POST",
                            "path": "/admin/api/proxies/rebalance",
                            "label": "重新分配"
                        })),
                        "probe" => Some(serde_json::json!({
                            "method": "POST",
                            "path": "/admin/api/proxies/probe",
                            "label": "重新探测"
                        })),
                        _ => None,
                    };

                    findings.push(Finding {
                        category: "ip".into(),
                        severity,
                        message: msg,
                        context: serde_json::json!({"code": code, "node": jf["node"]}),
                        suggestion,
                        action: action_json,
                    });
                }
            }

            // 2.4 出口 IP 集中度（从 nodes 提取）
            let mut ip_count: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
            for node in &nodes {
                if let Some(ip) = node["health"]["egress_ip"].as_str() {
                    if !ip.is_empty() && ip != "—" {
                        *ip_count.entry(ip.to_string()).or_insert(0) += 1;
                    }
                }
            }
            for (ip, count) in &ip_count {
                if *count > 3 {
                    findings.push(Finding {
                        category: "ip".into(),
                        severity: Severity::Warning,
                        message: format!("出口 IP 过度集中：{} 被 {} 个节点共享", ip, count),
                        context: serde_json::json!({"ip": ip, "count": count}),
                        suggestion: Some("增加不同出口 IP 的节点，降低单 IP 封禁风险".into()),
                        action: None,
                    });
                }
            }
        }
    }

    /// 调用 / 日志维度检测
    fn check_logs(logbuf: &Arc<LogBuffer>, findings: &mut Vec<Finding>) {
        let recent = logbuf.recent(100);
        if recent.is_empty() {
            return;
        }

        let mut status_4xx = 0usize;
        let mut status_5xx = 0usize;
        let mut status_429 = 0usize;
        let mut error_kinds: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

        for entry in &recent {
            if let Some(status) = entry["status"].as_u64() {
                if status == 429 {
                    status_429 += 1;
                } else if (400..500).contains(&status) {
                    status_4xx += 1;
                } else if status >= 500 {
                    status_5xx += 1;
                }
            }
            if let Some(kind) = entry["error_kind"].as_str() {
                *error_kinds.entry(kind.to_string()).or_insert(0) += 1;
            }
        }

        let total = recent.len();

        // 3.1 429 过多（上游限流）
        if status_429 > total / 10 && status_429 >= 3 {
            findings.push(Finding {
                category: "call".into(),
                severity: Severity::Warning,
                message: format!("近期 429 限流过多：{}/{} 请求", status_429, total),
                context: serde_json::json!({"status_429": status_429, "total": total}),
                suggestion: Some("上游正在限流，降低请求频率或等待恢复".into()),
                action: None,
            });
        }

        // 3.2 5xx 过多（上游故障）
        if status_5xx > total / 5 && status_5xx >= 2 {
            findings.push(Finding {
                category: "call".into(),
                severity: Severity::Critical,
                message: format!("近期 5xx 错误过多：{}/{} 请求", status_5xx, total),
                context: serde_json::json!({"status_5xx": status_5xx, "total": total}),
                suggestion: Some("上游服务可能故障，检查网络或联系服务商".into()),
                action: None,
            });
        }

        // 3.3 错误类型聚合
        for (kind, count) in &error_kinds {
            if *count >= total / 5 && *count >= 2 {
                findings.push(Finding {
                    category: "call".into(),
                    severity: if *count >= total / 3 { Severity::Critical } else { Severity::Warning },
                    message: format!("错误类型集中：{} 出现 {} 次", kind, count),
                    context: serde_json::json!({"error_kind": kind, "count": count}),
                    suggestion: Some(format!("针对 {} 类型错误进行专项排查", kind)),
                    action: None,
                });
            }
        }
    }

    /// 系统 / 配置维度检测
    fn check_config(
        config: &ConfigCell,
        pool: &AccountPool,
        findings: &mut Vec<Finding>,
    ) {
        let cfg = config.load();

        // 4.1 并发配置 vs 账号数
        let max_concurrency = cfg.max_concurrency_per_account;
        let total_accounts = pool.summary()["total_accounts"].as_u64().unwrap_or(0) as usize;
        if total_accounts > 0 && max_concurrency > total_accounts * 10 {
            findings.push(Finding {
                category: "config".into(),
                severity: Severity::Warning,
                message: format!("全局并发 {} 可能超过账号承载能力（{} 个账号）", max_concurrency, total_accounts),
                context: serde_json::json!({"max_concurrency": max_concurrency, "total_accounts": total_accounts}),
                suggestion: Some("考虑降低全局并发或增加账号数".into()),
                action: None,
            });
        }

        // 4.2 默认模型检查
        let default_model = &cfg.default_model;
        if default_model.is_empty() {
            findings.push(Finding {
                category: "config".into(),
                severity: Severity::Info,
                message: "默认模型未设置".into(),
                context: serde_json::json!({"default_model": ""}),
                suggestion: Some("设置默认模型以获得更好的路由体验".into()),
                action: Some(serde_json::json!({
                    "method": "POST",
                    "path": "/admin/api/settings",
                    "body": {"default_model": "kimi-k3"},
                    "label": "设为 kimi-k3"
                })),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn severity_ordering() {
        assert!(Severity::Critical > Severity::Warning);
        assert!(Severity::Warning > Severity::Info);
    }
}
