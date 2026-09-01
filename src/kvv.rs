//! Kimi Vendor Verifier (KVV) 验证逻辑
//!
//! 从 Python 版 server.py 移植的 K3 vendor 合规检查：
//! - `_kimi_is_thinking` — 检测 thinking 模式
//! - `_kimi_vendor_param_check` — 不可变参数校验 (temperature/top_p/penalty/n)
//! - `_kimi_vendor_request_validate` — 请求结构校验 (response_format / tool_choice / dynamic_tools)
//!
//! 所有校验失败返回 OpenAI 风格错误 JSON，与 Python 版格式完全一致。

use serde_json::{json, Value};

/// Kimi K3 不可变参数表：(name, thinking_allowed, non_thinking_allowed)
/// 与 Python 版 _KIMI_IMMUTABLE_PARAMS 完全一致
const KIMI_IMMUTABLE_PARAMS: &[(&str, &[f64], &[f64])] = &[
    ("temperature", &[0.0, 0.6, 1.0], &[0.6]),
    ("top_p", &[0.95], &[0.95]),
    ("presence_penalty", &[0.0], &[0.0]),
    ("frequency_penalty", &[0.0], &[0.0]),
    ("n", &[1.0], &[1.0]),
];

/// 检测请求是否处于 thinking 模式
/// 对应 Python: `_kimi_is_thinking(body)`
pub fn kimi_is_thinking(body: &Value) -> bool {
    // style 1: thinking.type == "enabled"
    if let Some(t) = body.get("thinking") {
        if let Some(obj) = t.as_object() {
            if obj.get("type").and_then(|v| v.as_str()) == Some("enabled") {
                return true;
            }
        }
    }
    // style 2: chat_template_kwargs.thinking (opensource style)
    if let Some(ctk) = body.get("chat_template_kwargs") {
        if let Some(obj) = ctk.as_object() {
            if obj.contains_key("thinking") {
                return obj
                    .get("thinking")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
            }
        }
    }
    // style 3: reasoning_effort presence implies thinking mode
    if body.get("reasoning_effort").is_some() {
        return true;
    }
    false
}

/// 构造 KVV 错误响应 (与 Python `_kimi_err` 格式一致)
pub fn kimi_err(message: &str, param: Option<&str>, code: &str) -> Value {
    let mut err = json!({
        "message": message,
        "type": "invalid_request_error",
        "code": code,
    });
    if let Some(p) = param {
        err["param"] = json!(p);
    }
    json!({ "error": err })
}

/// K3 vendor 不可变参数校验
/// 对应 Python: `_kimi_vendor_param_check(body)`
/// 返回 Some(error_json) 表示校验失败，None 表示通过
pub fn kimi_vendor_param_check(body: &Value) -> Option<Value> {
    if !body.is_object() {
        return None;
    }
    let thinking = kimi_is_thinking(body);
    for (name, allowed_think, allowed_non) in KIMI_IMMUTABLE_PARAMS {
        let val = match body.get(*name) {
            Some(v) => v,
            None => continue,
        };
        let allowed = if thinking { allowed_think } else { allowed_non };
        // 数值比较：支持 int/float
        let val_f64 = match val.as_f64() {
            Some(f) => f,
            None => {
                // 非数值直接不匹配
                let allowed_str: Vec<String> =
                    allowed.iter().map(|v| format_param_value(*v)).collect();
                let mode = if thinking { "thinking" } else { "non-thinking" };
                return Some(kimi_err(
                    &format!(
                        "Invalid value for '{}': {}. Kimi K3 vendor ({} mode) requires {} ∈ {{{}}}.",
                        name, val, mode, name, allowed_str.join(" / ")
                    ),
                    Some(name),
                    "invalid_parameter",
                ));
            }
        };
        let matched = allowed.iter().any(|a| (a - val_f64).abs() < f64::EPSILON);
        if !matched {
            let allowed_str: Vec<String> = allowed.iter().map(|v| format_param_value(*v)).collect();
            let mode = if thinking { "thinking" } else { "non-thinking" };
            return Some(kimi_err(
                &format!(
                    "Invalid value for '{}': {}. Kimi K3 vendor ({} mode) requires {} ∈ {{{}}}.",
                    name,
                    val_f64,
                    mode,
                    name,
                    allowed_str.join(" / ")
                ),
                Some(name),
                "invalid_parameter",
            ));
        }
    }
    None
}

/// 格式化参数值用于错误消息 (与 Python str() 行为一致)
fn format_param_value(v: f64) -> String {
    if v == v.trunc() && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{}", v)
    }
}

/// K3 vendor 请求结构校验 (response_format + tool_choice + dynamic_tools)
/// 对应 Python: `_kimi_vendor_request_validate(body)`
/// 返回 Some(error_json) 表示校验失败，None 表示通过
pub fn kimi_vendor_request_validate(body: &Value) -> Option<Value> {
    if !body.is_object() {
        return None;
    }

    // ---- response_format ----
    if let Some(rf) = body.get("response_format") {
        if !rf.is_object() {
            return Some(kimi_err(
                "response_format must be an object",
                Some("response_format"),
                "invalid_parameter",
            ));
        }
        let rtype = rf.get("type").and_then(|v| v.as_str());
        match rtype {
            Some("text") | Some("json_object") | Some("json_schema") => {}
            _ => {
                return Some(kimi_err(
                    &format!(
                        "Invalid response_format.type: {:?}. Must be one of text/json_object/json_schema.",
                        rtype
                    ),
                    Some("response_format.type"),
                    "invalid_parameter",
                ));
            }
        }
        if rtype == Some("json_schema") {
            let js = rf.get("json_schema");
            if js.is_none() {
                return Some(kimi_err(
                    "response_format.type='json_schema' requires a 'json_schema' field",
                    Some("response_format.json_schema"),
                    "invalid_parameter",
                ));
            }
            let js = js.unwrap();
            if !js.is_object() {
                return Some(kimi_err(
                    "json_schema must be an object",
                    Some("response_format.json_schema"),
                    "invalid_parameter",
                ));
            }
            if js.get("name").is_none() {
                return Some(kimi_err(
                    "json_schema requires 'name'",
                    Some("response_format.json_schema.name"),
                    "invalid_parameter",
                ));
            }
            if js.get("schema").is_none() {
                return Some(kimi_err(
                    "json_schema requires 'schema'",
                    Some("response_format.json_schema.schema"),
                    "invalid_parameter",
                ));
            }
            // strict 必须是 bool（默认 true）
            if let Some(strict) = js.get("strict") {
                if !strict.is_boolean() {
                    return Some(kimi_err(
                        &format!(
                            "json_schema.strict must be a boolean, got {}",
                            strict_type_name(strict)
                        ),
                        Some("response_format.json_schema.strict"),
                        "invalid_parameter",
                    ));
                }
            }
            if !js.get("schema").unwrap().is_object() {
                return Some(kimi_err(
                    "json_schema.schema must be an object",
                    Some("response_format.json_schema.schema"),
                    "invalid_parameter",
                ));
            }
        }
    }

    // ---- tool_choice ----
    let tc = body.get("tool_choice");
    let tools = body.get("tools");
    let has_dynamic_tools = |b: &Value| -> bool {
        b.get("messages")
            .and_then(|v| v.as_array())
            .map(|msgs| {
                msgs.iter().any(|m| {
                    m.get("role").and_then(|r| r.as_str()) == Some("system")
                        && m.get("tools")
                            .and_then(|t| t.as_array())
                            .map(|a| !a.is_empty())
                            .unwrap_or(false)
                })
            })
            .unwrap_or(false)
    };

    if let Some(tc) = tc {
        if tc.is_string() {
            let tc_str = tc.as_str().unwrap();
            match tc_str {
                "auto" | "none" | "required" => {}
                _ => {
                    return Some(kimi_err(
                        &format!(
                            "Invalid tool_choice: {:?}. Must be one of auto/none/required, or a named-function object.",
                            tc_str
                        ),
                        Some("tool_choice"),
                        "invalid_parameter",
                    ));
                }
            }
            if tc_str == "required" {
                let has_global = tools
                    .and_then(|t| t.as_array())
                    .map(|a| !a.is_empty())
                    .unwrap_or(false);
                if !has_global && !has_dynamic_tools(body) {
                    return Some(kimi_err(
                        "tool_choice='required' requires a non-empty 'tools' array (or dynamic tools in a system message)",
                        Some("tool_choice"),
                        "invalid_parameter",
                    ));
                }
            }
        } else if tc.is_object() {
            return Some(kimi_err(
                "Named-function tool_choice is not supported by Kimi K3 vendor.",
                Some("tool_choice"),
                "invalid_parameter",
            ));
        } else {
            return Some(kimi_err(
                &format!(
                    "tool_choice must be a string or object, got {}",
                    json_type_name(tc)
                ),
                Some("tool_choice"),
                "invalid_parameter",
            ));
        }
    }

    // ---- dynamic tools (K3 vendor extension) ----
    if let Some(msgs) = body.get("messages").and_then(|v| v.as_array()) {
        let mut global_names: std::collections::HashSet<String> = std::collections::HashSet::new();
        if let Some(tools_arr) = tools.and_then(|v| v.as_array()) {
            for t in tools_arr {
                if let Some(obj) = t.as_object() {
                    let name = obj
                        .get("function")
                        .and_then(|f| f.get("name"))
                        .or_else(|| obj.get("name"))
                        .and_then(|n| n.as_str());
                    if let Some(n) = name {
                        global_names.insert(n.to_string());
                    }
                }
            }
        }
        let mut seen_dynamic: std::collections::HashSet<String> = std::collections::HashSet::new();

        for m in msgs {
            if !m.is_object() {
                continue;
            }
            let role = m.get("role").and_then(|v| v.as_str()).unwrap_or("");
            let mt = m.get("tools");

            // tool role without tool_call_id
            if role == "tool" && m.get("tool_call_id").is_none() {
                return Some(kimi_err(
                    "A 'tool' role message must include 'tool_call_id'.",
                    Some("messages"),
                    "invalid_parameter",
                ));
            }

            let mt = match mt {
                Some(v) => v,
                None => continue,
            };

            // tools only allowed in system role
            if role != "system" {
                return Some(kimi_err(
                    &format!(
                        "Dynamic tools are only allowed in 'system' role messages, found in '{}'.",
                        role
                    ),
                    Some("messages"),
                    "invalid_parameter",
                ));
            }

            // tools must be array
            let mt_arr = match mt.as_array() {
                Some(a) => a,
                None => {
                    return Some(kimi_err(
                        &format!(
                            "'tools' in a system message must be an array, got {}.",
                            json_type_name(mt)
                        ),
                        Some("messages.tools"),
                        "invalid_parameter",
                    ));
                }
            };

            // content + tools both nonempty is rejected (K3 vendor quirk)
            let content = m.get("content");
            if let Some(c) = content {
                if c.as_str().map(|s| !s.trim().is_empty()).unwrap_or(false) && !mt_arr.is_empty() {
                    return Some(kimi_err(
                        "A system message with non-empty 'content' must not also declare dynamic 'tools'.",
                        Some("messages"),
                        "invalid_parameter",
                    ));
                }
            }

            // each tool must be object with type=function + function.{name,parameters}
            for t in mt_arr {
                if !t.is_object() {
                    return Some(kimi_err(
                        &format!(
                            "Dynamic tool item must be an object, got {}.",
                            json_type_name(t)
                        ),
                        Some("messages.tools"),
                        "invalid_parameter",
                    ));
                }
                if t.get("type").and_then(|v| v.as_str()) != Some("function") {
                    return Some(kimi_err(
                        &format!(
                            "Unsupported tool type: {:?}. Must be 'function'.",
                            t.get("type")
                        ),
                        Some("messages.tools.type"),
                        "invalid_parameter",
                    ));
                }
                let fn_obj = match t.get("function") {
                    Some(f) if f.is_object() => f,
                    _ => {
                        return Some(kimi_err(
                            "Tool requires a 'function' object.",
                            Some("messages.tools.function"),
                            "invalid_parameter",
                        ));
                    }
                };
                let name = match fn_obj.get("name").and_then(|v| v.as_str()) {
                    Some(n) if !n.is_empty() => n,
                    _ => {
                        return Some(kimi_err(
                            "Tool function requires a 'name'.",
                            Some("messages.tools.function.name"),
                            "invalid_parameter",
                        ));
                    }
                };
                if fn_obj.get("parameters").is_none() {
                    return Some(kimi_err(
                        "Tool function requires 'parameters'.",
                        Some("messages.tools.function.parameters"),
                        "invalid_parameter",
                    ));
                }
                // name format: ^[a-zA-Z_][a-zA-Z0-9_]{0,63}$
                if !is_valid_tool_name(name) {
                    return Some(kimi_err(
                        &format!(
                            "Invalid tool name: {:?}. Must match ^[a-zA-Z_][a-zA-Z0-9_]{{0,63}}$.",
                            name
                        ),
                        Some("messages.tools.function.name"),
                        "invalid_parameter",
                    ));
                }
                // duplicate detection (across dynamic and global)
                if seen_dynamic.contains(name) || global_names.contains(name) {
                    return Some(kimi_err(
                        &format!("Duplicate tool name: {:?}.", name),
                        Some("messages.tools.function.name"),
                        "invalid_parameter",
                    ));
                }
                seen_dynamic.insert(name.to_string());
            }
        }
    }

    None
}

/// 检查 tool name 是否符合 K3 命名规则
/// ^[a-zA-Z_][a-zA-Z0-9_]{0,63}$
fn is_valid_tool_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 64 {
        return false;
    }
    let first = name.chars().next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return false;
    }
    name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// JSON 值类型名（用于错误消息）
fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// strict 类型名（用于错误消息）
fn strict_type_name(v: &Value) -> &'static str {
    json_type_name(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_kimi_is_thinking() {
        // thinking.type = enabled
        assert!(kimi_is_thinking(&json!({"thinking": {"type": "enabled"}})));
        // chat_template_kwargs.thinking = true
        assert!(kimi_is_thinking(
            &json!({"chat_template_kwargs": {"thinking": true}})
        ));
        // reasoning_effort present
        assert!(kimi_is_thinking(&json!({"reasoning_effort": "low"})));
        // non-thinking
        assert!(!kimi_is_thinking(&json!({})));
        assert!(!kimi_is_thinking(
            &json!({"thinking": {"type": "disabled"}})
        ));
    }

    #[test]
    fn test_kimi_vendor_param_check_valid() {
        // thinking mode: temperature=0.6 允许
        assert!(kimi_vendor_param_check(&json!({
            "thinking": {"type": "enabled"},
            "temperature": 0.6
        }))
        .is_none());
        // non-thinking: temperature=0.6 允许
        assert!(kimi_vendor_param_check(&json!({"temperature": 0.6})).is_none());
        // top_p=0.95 允许
        assert!(kimi_vendor_param_check(&json!({"top_p": 0.95})).is_none());
    }

    #[test]
    fn test_kimi_vendor_param_check_invalid() {
        // non-thinking: temperature=0.0 不允许（只允许 0.6）
        let err = kimi_vendor_param_check(&json!({"temperature": 0.0}));
        assert!(err.is_some());
        assert!(err.unwrap()["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Invalid value for 'temperature'"));
        // thinking: temperature=0.3 不允许（只允许 0.0/0.6/1.0）
        let err = kimi_vendor_param_check(&json!({
            "thinking": {"type": "enabled"},
            "temperature": 0.3
        }));
        assert!(err.is_some());
    }

    #[test]
    fn test_kimi_vendor_request_validate_response_format() {
        // 有效
        assert!(kimi_vendor_request_validate(&json!({
            "response_format": {"type": "text"}
        }))
        .is_none());
        assert!(kimi_vendor_request_validate(&json!({
            "response_format": {"type": "json_object"}
        }))
        .is_none());
        assert!(kimi_vendor_request_validate(&json!({
            "response_format": {"type": "json_schema", "json_schema": {"name": "test", "schema": {}}}
        })).is_none());
        // 无效 type
        let err = kimi_vendor_request_validate(&json!({
            "response_format": {"type": "yaml"}
        }));
        assert!(err.is_some());
        // json_schema 缺 name
        let err = kimi_vendor_request_validate(&json!({
            "response_format": {"type": "json_schema", "json_schema": {"schema": {}}}
        }));
        assert!(err.is_some());
    }

    #[test]
    fn test_kimi_vendor_request_validate_tool_choice() {
        // auto 有效
        assert!(kimi_vendor_request_validate(&json!({"tool_choice": "auto"})).is_none());
        // none 有效
        assert!(kimi_vendor_request_validate(&json!({"tool_choice": "none"})).is_none());
        // required + tools 有效
        assert!(kimi_vendor_request_validate(&json!({
            "tool_choice": "required",
            "tools": [{"type": "function", "function": {"name": "test", "parameters": {}}}]
        }))
        .is_none());
        // required + 无 tools 无效
        let err = kimi_vendor_request_validate(&json!({"tool_choice": "required"}));
        assert!(err.is_some());
        // named function 不支持
        let err = kimi_vendor_request_validate(&json!({
            "tool_choice": {"type": "function", "function": {"name": "test"}}
        }));
        assert!(err.is_some());
    }

    #[test]
    fn test_kimi_vendor_request_validate_dynamic_tools() {
        // system message 带 tools 有效
        assert!(kimi_vendor_request_validate(&json!({
            "messages": [{
                "role": "system",
                "tools": [{"type": "function", "function": {"name": "dyn_tool", "parameters": {}}}]
            }]
        }))
        .is_none());
        // 非 system 带 tools 无效
        let err = kimi_vendor_request_validate(&json!({
            "messages": [{
                "role": "user",
                "tools": [{"type": "function", "function": {"name": "bad", "parameters": {}}}]
            }]
        }));
        assert!(err.is_some());
        // tool role 缺 tool_call_id 无效
        let err = kimi_vendor_request_validate(&json!({
            "messages": [{"role": "tool", "content": "result"}]
        }));
        assert!(err.is_some());
        // 重名 tool 无效
        let err = kimi_vendor_request_validate(&json!({
            "tools": [{"type": "function", "function": {"name": "dup", "parameters": {}}}],
            "messages": [{
                "role": "system",
                "tools": [{"type": "function", "function": {"name": "dup", "parameters": {}}}]
            }]
        }));
        assert!(err.is_some());
    }

    #[test]
    fn test_tool_name_validation() {
        assert!(is_valid_tool_name("valid_name"));
        assert!(is_valid_tool_name("_underscore"));
        assert!(is_valid_tool_name("a"));
        assert!(!is_valid_tool_name("0starts_with_digit"));
        assert!(!is_valid_tool_name("has-dash"));
        assert!(!is_valid_tool_name(""));
        assert!(!is_valid_tool_name(&"x".repeat(65)));
    }
}
