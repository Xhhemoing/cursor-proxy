//! 客户端方言: OpenAI Chat / Anthropic Messages (Claude Code) / OpenAI Responses (Codex).
//! 统一成 Cursor Stream 能吃的 messages + tools.

use serde_json::{json, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone, Default)]
pub struct AssistantOut {
    pub text: String,
    pub tool_calls: Vec<ToolCall>,
    pub thinking: String,
    /// Anthropic 思考签名 (thinking signature), 上游 thinkingPart 可能附带
    pub thinking_signature: Option<String>,
    /// 上游原始的 stop reason (responseInfo.stopReason), 用于判断输出是否被截断
    pub upstream_stop_reason: String,
}

/// 推理签名的代理标识前缀 — 与 Python 版 cursor-openai-proxy 保持一致.
/// 我们永远不会把代理生成的 blob 当作 Anthropic 官方 signature 透传给客户端,
/// 而是用此前缀 + base64 包装, 客户端需要时可解开.
pub const REASONING_PREFIX: &str = "cursor-sand-v1:";
pub const PROXY_SIGNATURE_MARK: &str = "proxygen:";

/// 把 (text, signature) 编码成可嵌入 Responses/Chat 的 encrypted_content 字符串.
/// 与 Python 版 `_encode_reasoning` 等价.
pub fn encode_reasoning(text: &str, signature: Option<&str>) -> String {
    use base64::Engine;
    let sig = signature
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("{}{}", PROXY_SIGNATURE_MARK, uuid::Uuid::new_v4().simple()));
    let blob = json!({"text": text, "signature": sig});
    let raw = serde_json::to_string(&blob).unwrap_or_else(|_| "{}".into());
    let b64 = base64::engine::general_purpose::URL_SAFE.encode(raw.as_bytes());
    format!("{}{}", REASONING_PREFIX, b64)
}

/// 从 encrypted_content 解码 (text, signature). 与 Python 版 `_decode_reasoning` 等价.
pub fn decode_reasoning(encrypted: &str) -> Option<(String, Option<String>)> {
    use base64::Engine;
    let rest = encrypted.strip_prefix(REASONING_PREFIX)?;
    // Python 端用 urlsafe_b64decode(... + \"==\"), 这里补 padding
    let padded = format!("{}{}", rest, "=".repeat((4 - rest.len() % 4) % 4));
    let raw = base64::engine::general_purpose::URL_SAFE
        .decode(padded.as_bytes())
        .ok()?;
    let v: Value = serde_json::from_slice(&raw).ok()?;
    let text = v.get("text").and_then(|x| x.as_str()).unwrap_or("").to_string();
    let sig = v.get("signature").and_then(|x| x.as_str()).map(String::from);
    Some((text, sig))
}

/// 判断请求是否要加密 reasoning (Responses API `include` 字段).
/// 与 Python 版 `_wants_encrypted_reasoning` 等价.
pub fn wants_encrypted_reasoning(request: &Value) -> bool {
    let Some(inc) = request.get("include") else { return false };
    let items: Vec<&str> = match inc {
        Value::String(s) => vec![s.as_str()],
        Value::Array(a) => a.iter().filter_map(|v| v.as_str()).collect(),
        _ => return false,
    };
    items.iter().any(|s| {
        *s == "reasoning.encrypted_content" || *s == "reasoning.encrypted_content.streaming"
    })
}

/// 模型展示名反向映射 — 把 Cursor 内部别名映射回客户端期待的官方 slug.
/// 与 Python 版 `DISPLAY_MODEL_MAP` + `display_model_id` 等价.
/// Claude/Grok 等敏感上游走反带时, 客户端会指纹检测 Anthropic slug.
pub fn display_model_id(model: &str) -> String {
    match model {
        "claude-fable-5" | "claude-fable-5-thinking-max" | "claude-fable" => {
            "claude-sonnet-4-6".into()
        }
        "grok-4.5" | "grok-4.6" | "cursor-grok-4.5" | "cursor-grok-4.6" => {
            "claude-sonnet-4-6".into()
        }
        other => other.to_string(),
    }
}

pub fn hosted_web_search(tools: Option<&Value>) -> bool {
    let Some(arr) = tools.and_then(|v| v.as_array()) else {
        return false;
    };
    arr.iter().any(|t| {
        let ty = t
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        ty == "web_search"
            || ty == "web_search_preview"
            || ty == "web_search_20250305"
            || ty.contains("web_search")
    })
}

fn schema_of_fn(f: &Value) -> Value {
    f.get("parameters")
        .cloned()
        .or_else(|| f.get("input_schema").cloned())
        .or_else(|| f.get("inputSchema").cloned())
        .unwrap_or_else(|| json!({"type": "object", "properties": {}}))
}

/// Cursor Connect 侧工具参数必须包一层 `{jsonSchema: ...}`。
/// 裸 JSON Schema 时上游常只调工具名、arguments 永远是 `{}`，
/// Claude Code / Codex 就会报 Invalid tool parameters。
fn wrap_tool_parameters(schema: Value) -> Value {
    if let Some(obj) = schema.as_object() {
        if obj.len() == 1 && obj.contains_key("jsonSchema") {
            return schema;
        }
    }
    json!({"jsonSchema": schema})
}

/// OpenAI tools / Anthropic tools / Responses tools → Cursor tools 数组.
pub fn cursor_tools_from_client(tools: Option<&Value>) -> Vec<Value> {
    let Some(arr) = tools.and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for t in arr {
        let ty = t.get("type").and_then(|v| v.as_str()).unwrap_or("function");
        if ty == "web_search" || ty.contains("web_search") {
            continue;
        }
        if ty == "function" {
            let f = t.get("function").unwrap_or(t);
            let name = f
                .get("name")
                .or_else(|| t.get("name"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if name.is_empty() {
                continue;
            }
            out.push(json!({
                "name": name,
                "description": f.get("description").or_else(|| t.get("description")).and_then(|v| v.as_str()).unwrap_or(""),
                "parameters": wrap_tool_parameters(schema_of_fn(f)),
            }));
            continue;
        }
        if let Some(name) = t.get("name").and_then(|v| v.as_str()) {
            if !name.is_empty() {
                out.push(json!({
                    "name": name,
                    "description": t.get("description").and_then(|v| v.as_str()).unwrap_or(""),
                    "parameters": wrap_tool_parameters(schema_of_fn(t)),
                }));
            }
        }
    }
    out
}

fn args_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => serde_json::to_string(other).unwrap_or_else(|_| "{}".into()),
    }
}

fn args_to_object(s: &str) -> Value {
    serde_json::from_str(s).unwrap_or_else(|_| json!({}))
}

/// 清洗 Cursor 的 toolCallId — 与 Python 版 `_normalize_call_id` 等价.
/// Cursor 有时发送两行 toolCallId (call-... 加 fc_...), 取最后一行有效值.
fn normalize_call_id(value: &Value) -> String {
    let text = value.as_str().unwrap_or("").trim();
    if text.is_empty() {
        return String::new();
    }
    let owned = text.replace('\r', "\n");
    let parts: Vec<&str> = owned
        .split('\n')
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .collect();
    if parts.is_empty() {
        return String::new();
    }
    for part in parts.iter().rev() {
        if part.starts_with("fc_") || part.starts_with("call_") {
            return part.to_string();
        }
    }
    parts.last().unwrap().to_string()
}

fn content_to_text(content: &Value) -> String {
    match content {
        Value::String(s) => s.clone(),
        Value::Array(arr) => arr
            .iter()
            .filter_map(|p| {
                if let Some(t) = p.get("text").and_then(|v| v.as_str()) {
                    return Some(t.to_string());
                }
                if p.get("type").and_then(|v| v.as_str()) == Some("input_text")
                    || p.get("type").and_then(|v| v.as_str()) == Some("output_text")
                {
                    return p.get("text").and_then(|v| v.as_str()).map(String::from);
                }
                None
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

/// OpenAI Chat 消息 → Cursor `aiserver.v1.InferenceMessage[]`
///
/// 线格式与 Python 参考实现 `responses_input_to_cursor` **逐字段对齐**:
///
/// ```text
/// {"role":"INFERENCE_MESSAGE_ROLE_SYSTEM",    "text": "..."}
/// {"role":"INFERENCE_MESSAGE_ROLE_USER",      "text": "..."}
/// {"role":"INFERENCE_MESSAGE_ROLE_ASSISTANT", "text": "...", "toolCalls":[{"toolCallId","toolName","args"|"rawToolCallArgs"}]}
/// {"role":"INFERENCE_MESSAGE_ROLE_TOOL",      "toolContent":{"parts":[{"toolCallId","toolName","result"}]}}
/// ```
///
/// 历史教训 (2026-09-03): 此前用的是自造的 `parts.parts[].functionCall / functionResult` 形状 +
/// `role:"user", system:true`。Cursor 的 protobuf-JSON 解析对未知字段**静默丢弃**, 所以请求 200 正常,
/// 但模型完全看不到历史工具调用参数和工具返回结果 (input_tokens 只随 assistant 文本增长,
/// 十几 KB 的工具结果贡献 0 token)。Hermes 表现为: 模型每轮"重新开始取证"、
/// 反复调用同一个工具、声称"没有工具输出支撑", 直到迭代上限 → 用户看到"对话中途停止"。
pub fn openai_messages_to_cursor(messages: &[Value]) -> Vec<Value> {
    const ROLE_USER: &str = "INFERENCE_MESSAGE_ROLE_USER";
    const ROLE_ASSISTANT: &str = "INFERENCE_MESSAGE_ROLE_ASSISTANT";
    const ROLE_TOOL: &str = "INFERENCE_MESSAGE_ROLE_TOOL";
    const ROLE_SYSTEM: &str = "INFERENCE_MESSAGE_ROLE_SYSTEM";

    let mut out: Vec<Value> = Vec::new();
    // callId → toolName, 让 TOOL 消息能带上 toolName (Python: pending_calls)
    let mut call_names: std::collections::HashMap<String, String> = Default::default();
    // 连续的 tool 结果合并进同一条 TOOL 消息 (Python: pending_results / flush_results)
    let mut pending_results: Vec<Value> = Vec::new();

    fn flush_results(out: &mut Vec<Value>, pending: &mut Vec<Value>) {
        if !pending.is_empty() {
            out.push(json!({
                "role": ROLE_TOOL,
                "toolContent": {"parts": std::mem::take(pending)},
            }));
        }
    }

    for m in messages {
        let role = m.get("role").and_then(|v| v.as_str()).unwrap_or("user");
        if role == "tool" {
            let call_id = m
                .get("tool_call_id")
                .or_else(|| m.get("call_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let text = content_to_text(m.get("content").unwrap_or(&Value::Null));
            let tool_name = m
                .get("name")
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .or_else(|| call_names.get(&call_id).cloned())
                .unwrap_or_default();
            pending_results.push(json!({
                "toolCallId": call_id,
                "toolName": tool_name,
                "result": tool_result_value(&text),
            }));
            continue;
        }
        flush_results(&mut out, &mut pending_results);

        let text = content_to_text(m.get("content").unwrap_or(&Value::Null));
        match role {
            "assistant" => {
                let mut msg = json!({"role": ROLE_ASSISTANT});
                let mut tool_calls: Vec<Value> = Vec::new();
                if let Some(calls) = m.get("tool_calls").and_then(|v| v.as_array()) {
                    for c in calls {
                        let id = c
                            .get("id")
                            .or_else(|| c.get("call_id"))
                            .and_then(|v| v.as_str())
                            .filter(|s| !s.is_empty())
                            .map(str::to_string)
                            .unwrap_or_else(|| format!("call_{}", uuid::Uuid::new_v4().simple()));
                        let f = c.get("function").unwrap_or(c);
                        let name = f.get("name").and_then(|v| v.as_str()).unwrap_or("");
                        call_names.insert(id.clone(), name.to_string());
                        let mut tc = json!({"toolCallId": id, "toolName": name});
                        match f.get("arguments") {
                            Some(Value::Object(o)) => tc["args"] = Value::Object(o.clone()),
                            Some(Value::String(s)) => match serde_json::from_str::<Value>(s.trim()) {
                                Ok(Value::Object(o)) => tc["args"] = Value::Object(o),
                                _ => tc["rawToolCallArgs"] = json!(s),
                            },
                            Some(other) if !other.is_null() => {
                                tc["rawToolCallArgs"] = json!(other.to_string())
                            }
                            _ => tc["rawToolCallArgs"] = json!("{}"),
                        }
                        tool_calls.push(tc);
                    }
                }
                if !text.is_empty() || tool_calls.is_empty() {
                    msg["text"] = json!(text);
                }
                if !tool_calls.is_empty() {
                    msg["toolCalls"] = json!(tool_calls);
                }
                out.push(msg);
            }
            "system" | "developer" => out.push(json!({"role": ROLE_SYSTEM, "text": text})),
            _ => out.push(json!({"role": ROLE_USER, "text": text})),
        }
    }
    flush_results(&mut out, &mut pending_results);
    out
}

/// 工具结果: JSON 文本解析成结构化值, 否则原样字符串 (Python `_tool_result_value`).
fn tool_result_value(text: &str) -> Value {
    let stripped = text.trim();
    if stripped.starts_with('{') || stripped.starts_with('[') {
        if let Ok(v) = serde_json::from_str::<Value>(stripped) {
            return v;
        }
    }
    json!(text)
}

pub fn tool_choice_hint(tool_choice: Option<&Value>) -> Option<String> {
    let v = tool_choice?;
    // P2: forced tool name (单个具体工具)
    if let Some(name) = forced_tool_name(Some(v)) {
        return Some(format!(
            "You must call the `{}` tool before answering in plain text.",
            name
        ));
    }
    // P2: required / any (必须调用工具)
    if tool_choice_is_required(Some(v)) {
        return Some("You must call a tool before answering in plain text.".into());
    }
    // 兼容旧逻辑: 简单字符串
    match v {
        Value::String(s) if s == "required" || s == "any" => {
            Some("You must call a tool. Do not answer with plain text.".into())
        }
        Value::Object(o) => {
            let name = o
                .get("function")
                .and_then(|f| f.get("name"))
                .or_else(|| o.get("name"))
                .and_then(|v| v.as_str())?;
            Some(format!(
                "You must call the tool named '{}'. Do not answer with plain text.",
                name
            ))
        }
        _ => None,
    }
}

/// P2: tool_choice == "none" / {"type":"none"} / false → 上游请求应剥掉 tools
/// 与 Python `tool_choice_omits_tools` 等价.
pub fn tool_choice_omits_tools(tool_choice: Option<&Value>) -> bool {
    match tool_choice {
        Some(Value::String(s)) if s == "none" => true,
        Some(Value::Bool(false)) => true,
        Some(Value::Object(o)) => o.get("type").and_then(|v| v.as_str()) == Some("none"),
        _ => false,
    }
}

/// Kimi K3 上游不接受 named-function tool_choice 对象。
/// Claude Code / Codex 原生会发 `{\"type\":\"function\",\"function\":{\"name\":\"...\"}}` 或
/// Anthropic `{\"type\":\"tool\",\"name\":\"...\"}`。改写成 `required`，强制名走 hint。
pub fn normalize_tool_choice_for_kimi(tc: &Value) -> Value {
    if forced_tool_name(Some(tc)).is_some() {
        return json!("required");
    }
    if let Some(obj) = tc.as_object() {
        match obj.get("type").and_then(|v| v.as_str()) {
            Some("auto") => return json!("auto"),
            Some("none") => return json!("none"),
            Some("any") | Some("required") => return json!("required"),
            _ => {}
        }
    }
    tc.clone()
}

/// P2: 提取 tool_choice 中强制要求的工具名 ({"type":"function","function":{"name":"..."}})
pub fn forced_tool_name(tool_choice: Option<&Value>) -> Option<String> {
    let v = tool_choice?;
    let obj = v.as_object()?;
    let typ = obj.get("type").and_then(|v| v.as_str())?;
    if !matches!(typ, "function" | "tool" | "custom") {
        return None;
    }
    let fn_obj = obj.get("function").and_then(|v| v.as_object());
    let name = fn_obj
        .and_then(|f| f.get("name"))
        .or_else(|| obj.get("name"))
        .and_then(|v| v.as_str())?;
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

/// P2: tool_choice 是否强制必须调工具 (required/any/forced)
pub fn tool_choice_is_required(tool_choice: Option<&Value>) -> bool {
    match tool_choice {
        Some(Value::String(s)) if s == "required" || s == "any" => true,
        Some(Value::Object(o)) => {
            let typ = o.get("type").and_then(|v| v.as_str());
            if matches!(typ, Some("required") | Some("any")) {
                return true;
            }
            forced_tool_name(tool_choice).is_some()
        }
        _ => false,
    }
}

/// P2: 从 response_format 编译 JSON schema 为 system 提示词.
/// 与 Python `response_format_schema` + `response_format_hint` 等价.
pub fn response_format_schema(body: &Value) -> Option<Value> {
    let rf = body.get("response_format")?.as_object()?;
    let typ = rf.get("type").and_then(|v| v.as_str())?.to_lowercase();
    match typ.as_str() {
        "json_schema" => {
            let js = rf.get("json_schema").and_then(|v| v.as_object());
            let schema = js
                .and_then(|j| j.get("schema").cloned())
                .or_else(|| rf.get("json_schema").cloned())
                .unwrap_or_else(|| json!({"type": "object"}));
            Some(schema)
        }
        "json_object" | "json" => Some(json!({"type": "object"})),
        _ => None,
    }
}

pub fn response_format_hint(body: &Value) -> Option<String> {
    let schema = response_format_schema(body)?;
    let rf = body.get("response_format").and_then(|v| v.as_object());
    let (name, strict) = rf
        .map(|r| {
            let js = r.get("json_schema").and_then(|v| v.as_object());
            let n = js
                .and_then(|j| j.get("name"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let s = js
                .and_then(|j| j.get("strict"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            (n, s)
        })
        .unwrap_or_default();
    let schema_text = serde_json::to_string(&schema).unwrap_or_else(|_| "{}".into());
    let label = if name.is_empty() {
        String::new()
    } else {
        format!(" named `{}`", name)
    };
    let strict_line = if strict {
        " Follow the schema strictly; do not add extra keys."
    } else {
        ""
    };
    Some(format!(
        "Respond with a single valid JSON object{} and nothing else — no prose, \
         no markdown fences, no explanation. The JSON must conform to this schema: \
         {}.{}",
        label, schema_text, strict_line
    ))
}

/// P2: 客户端会话/对话 ID 提取 — 按优先级多源回退.
///
/// 顺序 (与 Python 版 `conversation_id_from` 对齐):
///   1. body.conversationId / body.conversation.id / body.conversation (str)
///   2. body.metadata.conversation_id / metadata.session_id 等
///   3. body.previous_response_id (查 RESPONSE_STORE — Rust 版暂不实现 ResponseStore, 跳过)
///   4. Header: x-conversation-id / x-session-id / x-cursor-conversation-id / openai-conversation-id
///   5. body.user → "user_" + sha1[:16]
///   6. 基于 system + 首条 user 消息内容的 hash
///   7. 全新 UUID
pub fn conversation_id_from(body: &Value, headers: Option<&axum::http::HeaderMap>) -> String {
    // 1. body.conversationId / body.conversation
    let mut value: Option<String> = body
        .get("conversationId")
        .and_then(|v| v.as_str())
        .map(String::from);
    if value.is_none() {
        if let Some(conv) = body.get("conversation") {
            value = match conv {
                Value::String(s) => Some(s.clone()),
                Value::Object(o) => o
                    .get("id")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                _ => None,
            };
        }
    }
    // 2. body.metadata.{conversation_id, conversationId, session_id, sessionId}
    if value.is_none() {
        if let Some(meta) = body.get("metadata").and_then(|v| v.as_object()) {
            for k in ["conversation_id", "conversationId", "session_id", "sessionId"] {
                if let Some(v) = meta.get(k).and_then(|v| v.as_str()) {
                    value = Some(v.to_string());
                    break;
                }
            }
        }
    }
    // 3. headers
    if value.is_none() {
        if let Some(h) = headers {
            for name in [
                "x-conversation-id",
                "x-session-id",
                "x-cursor-conversation-id",
                "openai-conversation-id",
            ] {
                if let Some(v) = h.get(name).and_then(|v| v.to_str().ok()) {
                    let trimmed = v.trim();
                    if !trimmed.is_empty() {
                        value = Some(trimmed.to_string());
                        break;
                    }
                }
            }
        }
    }
    if let Some(v) = value {
        if !v.is_empty() {
            return v;
        }
    }
    // 4. user 字段
    let user = body
        .get("user")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();
    if !user.is_empty() {
        use sha1::Digest;
        let mut h = sha1::Sha1::new();
        h.update(user.as_bytes());
        let hex = format!("{:x}", h.finalize());
        return format!("user_{}", &hex[..16]);
    }
    // 5. 基于 system + 首条 user 内容 hash (Cursor 前缀缓存命中)
    let prefix = conversation_prefix(body);
    if !prefix.is_empty() {
        use sha1::Digest;
        let mut h = sha1::Sha1::new();
        h.update(prefix.as_bytes());
        let hex = format!("{:x}", h.finalize());
        // 与 Python 对齐: 如果 system 部分足够长, 用 sys_ 前缀
        let sys_part = prefix
            .split('\n')
            .find_map(|p| p.strip_prefix("sys:"))
            .unwrap_or("");
        if sys_part.len() >= 80 {
            return format!("sys_{}", &hex[..16]);
        }
        return format!("conv_{}", &hex[..16]);
    }
    uuid::Uuid::new_v4().to_string()
}

/// system + 首条 user 的稳定前缀。同一 Hermes 对话的后续 tool 轮次这份前缀不变，
/// 可当号池粘滞键（客户端经常不传 user / x-session-id）。
pub fn conversation_prefix(body: &Value) -> String {
    let mut chunks: Vec<String> = Vec::new();
    let instructions = body.get("instructions").and_then(|v| v.as_str());
    if let Some(ins) = instructions {
        if !ins.is_empty() {
            let truncated: String = ins.chars().take(400).collect();
            chunks.push(format!("sys:{}", truncated));
        }
    }
    let mut has_sys = !chunks.is_empty();
    let mut has_user = false;
    if let Some(msgs) = body.get("messages").and_then(|v| v.as_array()) {
        for msg in msgs {
            let role = msg
                .get("role")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_lowercase();
            let content_text = content_to_text(msg.get("content").unwrap_or(&Value::Null));
            let truncated: String = content_text.chars().take(400).collect();
            if (role == "system" || role == "developer") && !has_sys {
                if !truncated.is_empty() {
                    chunks.push(format!("sys:{}", truncated));
                    has_sys = true;
                }
                continue;
            }
            if role == "user" && !has_user {
                if !truncated.is_empty() {
                    chunks.push(format!("user:{}", truncated));
                    has_user = true;
                }
                break;
            }
        }
    }
    chunks.into_iter().filter(|s| s.len() > 4).collect::<Vec<_>>().join("\n")
}

/// Anthropic Messages body → OpenAI Chat 形态 (messages + tools + max_tokens).
pub fn anthropic_to_openai_chat(body: &Value) -> Result<Value, String> {
    let model = body
        .get("model")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "model is required".to_string())?;
    let mut messages: Vec<Value> = Vec::new();
    if let Some(sys) = body.get("system") {
        let text = match sys {
            Value::String(s) => s.clone(),
            Value::Array(_) => content_to_text(sys),
            _ => String::new(),
        };
        if !text.is_empty() {
            messages.push(json!({"role": "system", "content": text}));
        }
    }
    let src = body
        .get("messages")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "messages is required".to_string())?;
    for m in src {
        let role = m.get("role").and_then(|v| v.as_str()).unwrap_or("user");
        let content = m.get("content").unwrap_or(&Value::Null);
        match content {
            Value::String(s) => {
                messages.push(json!({"role": role, "content": s}));
            }
            Value::Array(blocks) => {
                let mut text_bits = Vec::new();
                let mut tool_calls = Vec::new();
                let mut tool_results: Vec<Value> = Vec::new();
                for b in blocks {
                    let ty = b.get("type").and_then(|v| v.as_str()).unwrap_or("text");
                    match ty {
                        "text" => {
                            if let Some(t) = b.get("text").and_then(|v| v.as_str()) {
                                text_bits.push(t.to_string());
                            }
                        }
                        "tool_use" => {
                            let id = b.get("id").and_then(|v| v.as_str()).unwrap_or("");
                            let name = b.get("name").and_then(|v| v.as_str()).unwrap_or("");
                            let input = b.get("input").cloned().unwrap_or(json!({}));
                            tool_calls.push(json!({
                                "id": id,
                                "type": "function",
                                "function": {
                                    "name": name,
                                    "arguments": serde_json::to_string(&input).unwrap_or_else(|_| "{}".into()),
                                }
                            }));
                        }
                        "tool_result" => {
                            let id = b.get("tool_use_id").and_then(|v| v.as_str()).unwrap_or("");
                            let c = match b.get("content") {
                                Some(Value::String(s)) => s.clone(),
                                Some(other) => content_to_text(other),
                                None => String::new(),
                            };
                            tool_results.push(json!({
                                "role": "tool",
                                "tool_call_id": id,
                                "content": c,
                            }));
                        }
                        _ => {}
                    }
                }
                if role == "assistant" && !tool_calls.is_empty() {
                    messages.push(json!({
                        "role": "assistant",
                        "content": if text_bits.is_empty() { Value::Null } else { json!(text_bits.join("\n")) },
                        "tool_calls": tool_calls,
                    }));
                } else if !text_bits.is_empty() {
                    messages.push(json!({"role": role, "content": text_bits.join("\n")}));
                }
                messages.extend(tool_results);
            }
            _ => messages.push(json!({"role": role, "content": ""})),
        }
    }
    let mut out = json!({
        "model": model,
        "messages": messages,
        "stream": body.get("stream").and_then(|v| v.as_bool()).unwrap_or(false),
    });
    if let Some(mt) = body.get("max_tokens").and_then(|v| v.as_u64()) {
        out["max_tokens"] = json!(mt);
    }
    if let Some(t) = body.get("temperature").and_then(|v| v.as_f64()) {
        out["temperature"] = json!(t);
    }
    if let Some(tools) = body.get("tools") {
        let mapped: Vec<Value> = tools
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .map(|t| {
                json!({
                    "type": "function",
                    "function": {
                        "name": t.get("name"),
                        "description": t.get("description").cloned().unwrap_or(json!("")),
                        "parameters": t.get("input_schema").cloned().unwrap_or(json!({"type":"object","properties":{}})),
                    }
                })
            })
            .collect();
        out["tools"] = json!(mapped);
    }
    if let Some(tc) = body.get("tool_choice") {
        out["tool_choice"] = normalize_tool_choice_for_kimi(tc);
    }
    Ok(out)
}

fn responses_input_to_messages(input: &Value) -> Vec<Value> {
    match input {
        Value::String(s) => vec![json!({"role": "user", "content": s})],
        Value::Array(items) => {
            let mut messages = Vec::new();
            for it in items {
                let ty = it.get("type").and_then(|v| v.as_str()).unwrap_or("message");
                match ty {
                    "message" => {
                        let role = it.get("role").and_then(|v| v.as_str()).unwrap_or("user");
                        let content = it.get("content").unwrap_or(&Value::Null);
                        messages.push(json!({"role": role, "content": content_to_text(content)}));
                    }
                    "function_call" => {
                        let id = it
                            .get("call_id")
                            .or_else(|| it.get("id"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let name = it.get("name").and_then(|v| v.as_str()).unwrap_or("");
                        let args = it
                            .get("arguments")
                            .map(args_to_string)
                            .unwrap_or_else(|| "{}".into());
                        messages.push(json!({
                            "role": "assistant",
                            "content": null,
                            "tool_calls": [{
                                "id": id,
                                "type": "function",
                                "function": {"name": name, "arguments": args},
                            }]
                        }));
                    }
                    "function_call_output" => {
                        let id = it.get("call_id").and_then(|v| v.as_str()).unwrap_or("");
                        let output = it
                            .get("output")
                            .map(|v| match v {
                                Value::String(s) => s.clone(),
                                other => content_to_text(other),
                            })
                            .unwrap_or_default();
                        messages.push(json!({
                            "role": "tool",
                            "tool_call_id": id,
                            "content": output,
                        }));
                    }
                    _ => {
                        if let Some(role) = it.get("role").and_then(|v| v.as_str()) {
                            messages.push(json!({
                                "role": role,
                                "content": content_to_text(it.get("content").unwrap_or(&Value::Null)),
                            }));
                        }
                    }
                }
            }
            messages
        }
        _ => Vec::new(),
    }
}

/// OpenAI Responses body → OpenAI Chat 形态.
pub fn responses_to_openai_chat(body: &Value) -> Result<Value, String> {
    let model = body
        .get("model")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "model is required".to_string())?;
    let mut messages = Vec::new();
    if let Some(instr) = body.get("instructions").and_then(|v| v.as_str()) {
        if !instr.is_empty() {
            messages.push(json!({"role": "system", "content": instr}));
        }
    }
    if let Some(input) = body.get("input") {
        messages.extend(responses_input_to_messages(input));
    }
    if messages.is_empty() {
        return Err("input is required".into());
    }
    // Responses API: 默认流式 (与 OpenAI 官方行为一致 — 客户端用 SSE 消费)
    let stream = body.get("stream").and_then(|v| v.as_bool()).unwrap_or(true);
    let mut out = json!({
        "model": model,
        "messages": messages,
        "stream": stream,
    });
    // P2: 保留原始 body 的 include 字段 (reasoning.encrypted_content 等), 供翻译层判定
    if let Some(inc) = body.get("include") {
        out["include"] = inc.clone();
    }
    if let Some(mt) = body
        .get("max_output_tokens")
        .or_else(|| body.get("max_tokens"))
        .and_then(|v| v.as_u64())
    {
        out["max_tokens"] = json!(mt);
    }
    if let Some(t) = body.get("temperature").and_then(|v| v.as_f64()) {
        out["temperature"] = json!(t);
    }
    if let Some(user) = body.get("user").and_then(|v| v.as_str()) {
        out["user"] = json!(user);
    }
    if let Some(tools) = body.get("tools") {
        out["tools"] = tools.clone();
    }
    if let Some(tc) = body.get("tool_choice") {
        out["tool_choice"] = normalize_tool_choice_for_kimi(tc);
    }
    if let Some(p) = body.get("parallel_tool_calls") {
        out["parallel_tool_calls"] = p.clone();
    }
    Ok(out)
}

pub fn parse_tool_call_part(part: &Value) -> Option<(String, String, String, bool, bool)> {
    // returns (id, name, args_or_delta, is_delta, is_complete)
    let id = normalize_call_id(
        part.get("toolCallId")
            .or_else(|| part.get("callId"))
            .or_else(|| part.get("id"))
            .unwrap_or(&Value::Null),
    );
    let name = part
        .get("name")
        .or_else(|| part.get("toolName"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let is_complete = part
        .get("isComplete")
        .or_else(|| part.get("is_complete"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if let Some(d) = part
        .get("argumentsDelta")
        .or_else(|| part.get("delta"))
        .or_else(|| part.get("partialArguments"))
        .and_then(|v| v.as_str())
    {
        return Some((id, name, d.to_string(), true, is_complete));
    }
    if let Some(a) = part.get("arguments").or_else(|| part.get("args")) {
        return Some((id, name, args_to_string(a), false, is_complete));
    }
    if name.is_empty() && id.is_empty() {
        return None;
    }
    Some((id, name, String::new(), false, is_complete))
}

/// 增量合并 Cursor toolCallPart args — 与 Python 版 `merge_tool_arg_text` 等价.
///
/// Cursor 可能发送:
///   1. 真字符串增量 (前缀增长)
///   2. 同一对象的 pretty JSON 然后 compact JSON
///   3. 增长的对象快照 (部分 dict 然后完整 Edit/PowerShell 参数)
///   4. 以 [ 或引号开头的片段 (PowerShell `$lines[0..137]`, 路径)
///
/// 返回 (完整 arguments, 安全前缀 delta).
pub fn merge_tool_arg_text(prev: &str, incoming: &str) -> (String, String) {
    let prev = prev;
    let incoming = incoming;
    if incoming.is_empty() {
        return (prev.to_string(), String::new());
    }
    if incoming == prev {
        return (prev.to_string(), String::new());
    }
    if incoming.starts_with(prev) {
        return (incoming.to_string(), incoming[prev.len()..].to_string());
    }
    if prev.starts_with(incoming) {
        return (prev.to_string(), String::new());
    }

    let new_obj = complete_json(incoming);
    let prev_obj = complete_json(prev);

    match (new_obj, prev_obj) {
        (Some(Value::Object(new_m)), Some(Value::Object(prev_m))) => {
            if Value::Object(new_m.clone()) == Value::Object(prev_m.clone()) {
                return (prev.to_string(), String::new());
            }
            // 新对象 payload 严格小于旧对象且所有键已存在 → 视为旧快照, 丢弃
            let new_len = json_payload_len(&Value::Object(new_m.clone()));
            let prev_len = json_payload_len(&Value::Object(prev_m.clone()));
            if new_len < prev_len && new_m.keys().all(|k| prev_m.contains_key(k)) {
                return (prev.to_string(), String::new());
            }
            // 新快照更丰富: 替换, delta 为空
            (incoming.to_string(), String::new())
        }
        (Some(Value::Object(_)), None) => (incoming.to_string(), String::new()),
        _ => {
            // 字符串片段: 直接拼接
            let mut full = String::with_capacity(prev.len() + incoming.len());
            full.push_str(prev);
            full.push_str(incoming);
            let delta = incoming.to_string();
            (full, delta)
        }
    }
}

/// 尝试把字符串解析为完整 JSON 值 (允许多种 dump 格式).
fn complete_json(raw: &str) -> Option<Value> {
    let text = raw.trim();
    if text.is_empty() {
        return None;
    }
    serde_json::from_str(text).ok()
}

/// JSON payload 字符长度 (用于比较两个 dict 的"丰富程度").
fn json_payload_len(v: &Value) -> usize {
    serde_json::to_string(v).map(|s| s.len()).unwrap_or(0)
}

pub fn apply_tool_call_part(out: &mut AssistantOut, part: &Value) -> bool {
    let Some((id, name, payload, is_delta, is_complete)) = parse_tool_call_part(part) else {
        return false;
    };
    if let Some(existing) = out.tool_calls.iter_mut().rev().find(|c| {
        (!id.is_empty() && c.id == id) || (id.is_empty() && !name.is_empty() && c.name == name)
    }) {
        if !name.is_empty() {
            existing.name = name;
        }
        if !id.is_empty() {
            existing.id = id;
        }
        if is_delta {
            // P1: 使用智能合并而非裸拼接, 防止跨帧 JSON 边界产生无效 JSON
            let (merged, _) = merge_tool_arg_text(&existing.arguments, &payload);
            existing.arguments = merged;
        } else if !payload.is_empty() {
            let (merged, _) = merge_tool_arg_text(&existing.arguments, &payload);
            existing.arguments = merged;
        }
        return is_complete;
    }
    let id = if id.is_empty() {
        format!("call_{}", out.tool_calls.len() + 1)
    } else {
        id
    };
    out.tool_calls.push(ToolCall {
        id,
        name,
        arguments: payload,
    });
    is_complete
}

/// 判断 AssistantOut 是否被上游截断 (max_tokens / length limit)
pub fn out_was_truncated(out: &AssistantOut) -> bool {
    let r = &out.upstream_stop_reason;
    r.contains("MAX_TOKENS")
        || r.contains("LENGTH")
        || r.contains("max_tokens")
        || r.contains("length")
}

pub fn anthropic_message(
    id: &str,
    model: &str,
    out: &AssistantOut,
    usage: &crate::translate::Usage,
) -> Value {
    let mut content: Vec<Value> = Vec::new();
    // P0: Anthropic 规范要求 thinking block 在 text 之前, 且必须携带 signature
    if !out.thinking.is_empty() {
        let sig = out
            .thinking_signature
            .clone()
            .unwrap_or_else(|| format!("{}{}", PROXY_SIGNATURE_MARK, uuid::Uuid::new_v4().simple()));
        content.push(json!({
            "type": "thinking",
            "thinking": out.thinking,
            "signature": sig,
        }));
    }
    if !out.text.is_empty() {
        content.push(json!({"type": "text", "text": out.text}));
    }
    for c in &out.tool_calls {
        content.push(json!({
            "type": "tool_use",
            "id": c.id,
            "name": c.name,
            "input": args_to_object(&c.arguments),
        }));
    }
    if content.is_empty() {
        content.push(json!({"type": "text", "text": ""}));
    }
    let stop = if !out.tool_calls.is_empty() {
        "tool_use"
    } else if out_was_truncated(out) {
        "max_tokens"
    } else {
        "end_turn"
    };
    json!({
        "id": id,
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": content,
        "stop_reason": stop,
        "stop_sequence": Value::Null,
        "usage": {
            "input_tokens": usage.input + usage.cache_read,
            "output_tokens": usage.output,
            "cache_read_input_tokens": usage.cache_read,
            "cache_creation_input_tokens": usage.cache_write,
        }
    })
}

/// 构造 Responses API 完整 message, 可选携带 encrypted reasoning.
/// `request` 用于判定是否要 include encrypted_content.
pub fn responses_message_with_request(
    id: &str,
    model: &str,
    out: &AssistantOut,
    usage: &crate::translate::Usage,
    request: Option<&Value>,
) -> Value {
    let mut output: Vec<Value> = Vec::new();
    let wants_encrypted = request.map(wants_encrypted_reasoning).unwrap_or(false);
    // P0: 思考作为独立 reasoning item 放在最前 (Responses 规范)
    if !out.thinking.is_empty() {
        let mut item = json!({
            "id": format!("rs_{}", uuid::Uuid::new_v4().simple()),
            "type": "reasoning",
            "status": "completed",
            "summary": [{"type": "summary_text", "text": out.thinking}],
        });
        if wants_encrypted {
            item["encrypted_content"] =
                json!(encode_reasoning(&out.thinking, out.thinking_signature.as_deref()));
        }
        output.push(item);
    }
    if !out.text.is_empty() || (out.tool_calls.is_empty() && out.thinking.is_empty()) {
        output.push(json!({
            "id": format!("msg_{}", id),
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [{"type": "output_text", "text": out.text}],
        }));
    }
    for c in &out.tool_calls {
        output.push(json!({
            "id": format!("fc_{}", c.id),
            "type": "function_call",
            "status": "completed",
            "call_id": c.id,
            "name": c.name,
            "arguments": c.arguments,
        }));
    }
    json!({
        "id": id,
        "object": "response",
        "created_at": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs(),
        "status": "completed",
        "model": model,
        "output": output,
        "usage": {
            "input_tokens": usage.input + usage.cache_read,
            "output_tokens": usage.output,
            "total_tokens": usage.total(),
            "input_tokens_details": {"cached_tokens": usage.cache_read},
        }
    })
}

pub fn responses_message(
    id: &str,
    model: &str,
    out: &AssistantOut,
    usage: &crate::translate::Usage,
) -> Value {
    responses_message_with_request(id, model, out, usage, None)
}

pub fn openai_message_with_tools(
    id: &str,
    model: &str,
    out: &AssistantOut,
    usage: &crate::translate::Usage,
) -> Value {
    let finish = if !out.tool_calls.is_empty() {
        "tool_calls"
    } else if out_was_truncated(out) {
        "length"
    } else {
        "stop"
    };
    let mut message = json!({
        "role": "assistant",
        "content": if out.text.is_empty() { Value::Null } else { json!(out.text) },
    });
    // P0: 思考内容在 OpenAI 规范中以 reasoning_content 字段呈现 (DeepSeek-R1/Kimi-Thinking 标准)
    if !out.thinking.is_empty() {
        message["reasoning_content"] = json!(out.thinking);
        if let Some(sig) = &out.thinking_signature {
            message["reasoning_signature"] = json!(sig);
        }
    }
    if !out.tool_calls.is_empty() {
        let calls: Vec<Value> = out
            .tool_calls
            .iter()
            .map(|c| {
                json!({
                    "id": c.id,
                    "type": "function",
                    "function": {"name": c.name, "arguments": c.arguments},
                })
            })
            .collect();
        message["tool_calls"] = json!(calls);
    }
    json!({
        "id": id,
        "object": "chat.completion",
        "created": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs(),
        "model": model,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": finish,
        }],
        "usage": usage.to_openai_json(),
    })
}

pub fn sse_event(event: &str, data: &Value) -> String {
    format!(
        "event: {}\ndata: {}\n\n",
        event,
        serde_json::to_string(data).unwrap()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anthropic_tools_roundtrip_to_cursor() {
        let body = json!({
            "model": "kimi-k3",
            "max_tokens": 1024,
            "tools": [{
                "name": "bash",
                "description": "run shell",
                "input_schema": {"type": "object", "properties": {"command": {"type": "string"}}, "required": ["command"]}
            }],
            "messages": [
                {"role": "user", "content": "ls"},
                {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "toolu_1", "name": "bash", "input": {"command": "ls"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "toolu_1", "content": "a.txt"}
                ]}
            ]
        });
        let chat = anthropic_to_openai_chat(&body).unwrap();
        assert_eq!(chat["model"], "kimi-k3");
        let msgs = chat["messages"].as_array().unwrap();
        assert_eq!(msgs[0]["role"], "user");
        assert_eq!(msgs[1]["tool_calls"][0]["function"]["name"], "bash");
        assert_eq!(msgs[2]["role"], "tool");
        assert_eq!(msgs[2]["tool_call_id"], "toolu_1");
        let tools = cursor_tools_from_client(chat.get("tools"));
        assert_eq!(tools[0]["name"], "bash");
        assert_eq!(tools[0]["parameters"]["jsonSchema"]["required"][0], "command");
        let cursor_msgs = openai_messages_to_cursor(msgs);
        assert_eq!(cursor_msgs[1]["role"], "INFERENCE_MESSAGE_ROLE_ASSISTANT");
        assert_eq!(cursor_msgs[1]["toolCalls"][0]["toolCallId"], "toolu_1");
        assert_eq!(cursor_msgs[1]["toolCalls"][0]["toolName"], "bash");
        assert_eq!(cursor_msgs[1]["toolCalls"][0]["args"]["command"], "ls");
        assert_eq!(cursor_msgs[2]["role"], "INFERENCE_MESSAGE_ROLE_TOOL");
        assert_eq!(cursor_msgs[2]["toolContent"]["parts"][0]["toolCallId"], "toolu_1");
        assert_eq!(cursor_msgs[2]["toolContent"]["parts"][0]["toolName"], "bash");
        assert_eq!(cursor_msgs[2]["toolContent"]["parts"][0]["result"], "a.txt");
    }

    #[test]
    fn responses_function_call_loop_to_cursor() {
        let body = json!({
            "model": "kimi-k3",
            "instructions": "be a coder",
            "max_output_tokens": 4096,
            "tools": [{"type": "function", "name": "read_file", "description": "read", "parameters": {"type": "object"}}],
            "input": [
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "open a.rs"}]},
                {"type": "function_call", "call_id": "call_9", "name": "read_file", "arguments": "{\"path\":\"a.rs\"}"},
                {"type": "function_call_output", "call_id": "call_9", "output": "fn main(){}"}
            ]
        });
        let chat = responses_to_openai_chat(&body).unwrap();
        assert_eq!(chat["max_tokens"], 4096);
        assert_eq!(chat["messages"][0]["role"], "system");
        assert_eq!(chat["messages"][2]["tool_calls"][0]["id"], "call_9");
        assert_eq!(chat["messages"][3]["role"], "tool");
        let cursor_msgs = openai_messages_to_cursor(chat["messages"].as_array().unwrap());
        assert_eq!(cursor_msgs[0]["role"], "INFERENCE_MESSAGE_ROLE_SYSTEM");
        let last = cursor_msgs.last().unwrap();
        assert_eq!(last["role"], "INFERENCE_MESSAGE_ROLE_TOOL");
        assert_eq!(last["toolContent"]["parts"][0]["toolCallId"], "call_9");
        assert_eq!(last["toolContent"]["parts"][0]["toolName"], "read_file");
        assert_eq!(last["toolContent"]["parts"][0]["result"], "fn main(){}");
    }

    /// 线格式回归: 与 Python 参考实现 `chat_messages_to_cursor` 的输出逐字段一致.
    /// 2026-09-03 之前用的自造 parts.functionCall/functionResult 形状被 Cursor 静默丢弃,
    /// 模型看不到任何工具历史 → Hermes 反复重跑同一工具直到迭代上限.
    #[test]
    fn chat_tool_round_trip_matches_python_wire_format() {
        let msgs = vec![
            json!({"role": "system", "content": "SYS"}),
            json!({"role": "user", "content": "read it"}),
            json!({"role": "assistant", "content": "ok, reading", "tool_calls": [
                {"id": "read_file_0-abc12", "type": "function",
                 "function": {"name": "read_file", "arguments": "{\"path\":\"/tmp/x\"}"}},
                {"id": "terminal_1-ffff1", "type": "function",
                 "function": {"name": "terminal", "arguments": "not json"}}
            ]}),
            json!({"role": "tool", "tool_call_id": "read_file_0-abc12", "content": "FILE CONTENT HERE"}),
            json!({"role": "tool", "tool_call_id": "terminal_1-ffff1", "content": "{\"status\":\"ok\"}"}),
            json!({"role": "user", "content": "and now?"}),
        ];
        let out = openai_messages_to_cursor(&msgs);
        let expected = json!([
            {"role": "INFERENCE_MESSAGE_ROLE_SYSTEM", "text": "SYS"},
            {"role": "INFERENCE_MESSAGE_ROLE_USER", "text": "read it"},
            {"role": "INFERENCE_MESSAGE_ROLE_ASSISTANT", "text": "ok, reading", "toolCalls": [
                {"toolCallId": "read_file_0-abc12", "toolName": "read_file", "args": {"path": "/tmp/x"}},
                {"toolCallId": "terminal_1-ffff1", "toolName": "terminal", "rawToolCallArgs": "not json"}
            ]},
            // 连续两个 tool 结果合并进一条 TOOL 消息; JSON 文本结果被解析成对象
            {"role": "INFERENCE_MESSAGE_ROLE_TOOL", "toolContent": {"parts": [
                {"toolCallId": "read_file_0-abc12", "toolName": "read_file", "result": "FILE CONTENT HERE"},
                {"toolCallId": "terminal_1-ffff1", "toolName": "terminal", "result": {"status": "ok"}}
            ]}},
            {"role": "INFERENCE_MESSAGE_ROLE_USER", "text": "and now?"},
        ]);
        assert_eq!(out, expected.as_array().unwrap().clone());
        // 旧的错误形状必须彻底消失
        let s = serde_json::to_string(&out).unwrap();
        assert!(!s.contains("functionResult") && !s.contains("functionCall") && !s.contains("\"system\":true"));
    }

    #[test]
    fn assistant_tool_call_only_has_no_empty_text_field() {
        let msgs = vec![json!({"role": "assistant", "content": "", "tool_calls": [
            {"id": "c1", "type": "function", "function": {"name": "f", "arguments": "{}"}}
        ]})];
        let out = openai_messages_to_cursor(&msgs);
        assert!(out[0].get("text").is_none(), "Python 版: 有 toolCalls 且无文本时不带 text 字段");
        assert_eq!(out[0]["toolCalls"][0]["args"], json!({}));
        // 纯文本 assistant 消息保留 text (即使为空串)
        let out2 = openai_messages_to_cursor(&[json!({"role": "assistant", "content": ""})]);
        assert_eq!(out2[0]["text"], "");
    }

    #[test]
    fn tool_call_part_delta_and_anthropic_output() {
        let mut out = AssistantOut::default();
        apply_tool_call_part(
            &mut out,
            &json!({"name": "bash", "toolCallId": "c1", "argumentsDelta": "{\"co"}),
        );
        apply_tool_call_part(
            &mut out,
            &json!({"name": "bash", "toolCallId": "c1", "argumentsDelta": "mmand\":\"ls\"}"}),
        );
        assert_eq!(out.tool_calls.len(), 1);
        assert_eq!(out.tool_calls[0].arguments, "{\"command\":\"ls\"}");
        let usage = crate::translate::Usage {
            input: 10,
            output: 3,
            cache_read: 2,
            cache_write: 0,
        };
        let msg = anthropic_message("msg_x", "kimi-k3", &out, &usage);
        assert_eq!(msg["stop_reason"], "tool_use");
        assert_eq!(msg["content"][0]["name"], "bash");
        assert_eq!(msg["content"][0]["input"]["command"], "ls");
        let resp = responses_message("resp_x", "kimi-k3", &out, &usage);
        assert_eq!(resp["output"][0]["type"], "function_call");
        assert_eq!(resp["output"][0]["call_id"], "c1");
        assert!(hosted_web_search(Some(
            &json!([{"type": "web_search_preview"}])
        )));
        assert!(!hosted_web_search(Some(
            &json!([{"type": "function", "function": {"name": "bash"}}])
        )));
    }

    #[test]
    fn cursor_tools_wrap_json_schema_for_claude_and_codex() {
        let openai = json!([{
            "type": "function",
            "function": {
                "name": "Read",
                "description": "read a file",
                "parameters": {
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"]
                }
            }
        }]);
        let tools = cursor_tools_from_client(Some(&openai));
        assert_eq!(tools[0]["name"], "Read");
        assert_eq!(tools[0]["parameters"]["jsonSchema"]["required"][0], "path");

        let anthropic = json!([{
            "name": "Bash",
            "description": "run",
            "input_schema": {
                "type": "object",
                "properties": {"command": {"type": "string"}},
                "required": ["command"]
            }
        }]);
        let tools = cursor_tools_from_client(Some(&anthropic));
        assert_eq!(tools[0]["parameters"]["jsonSchema"]["required"][0], "command");
    }

    #[test]
    fn named_tool_choice_normalizes_for_kimi() {
        let openai = json!({"type": "function", "function": {"name": "Read"}});
        assert_eq!(normalize_tool_choice_for_kimi(&openai), json!("required"));
        let claude = json!({"type": "tool", "name": "Bash"});
        assert_eq!(normalize_tool_choice_for_kimi(&claude), json!("required"));
        assert_eq!(normalize_tool_choice_for_kimi(&json!("auto")), json!("auto"));
        let hint = tool_choice_hint(Some(&openai)).unwrap();
        assert!(hint.contains("`Read`"));
        let chat = anthropic_to_openai_chat(&json!({
            "model": "kimi-k3",
            "max_tokens": 64,
            "tool_choice": {"type": "tool", "name": "Bash"},
            "tools": [{"name": "Bash", "input_schema": {"type": "object", "properties": {}}}],
            "messages": [{"role": "user", "content": "ls"}]
        }))
        .unwrap();
        assert_eq!(chat["tool_choice"], "required");
    }

    #[test]
    fn conversation_prefix_stable_across_tool_turns() {
        let turn1 = json!({
            "messages": [
                {"role": "system", "content": "You are a precise assistant."},
                {"role": "user", "content": "Read /tmp/secret.txt"}
            ]
        });
        let turn2 = json!({
            "messages": [
                {"role": "system", "content": "You are a precise assistant."},
                {"role": "user", "content": "Read /tmp/secret.txt"},
                {"role": "assistant", "content": "", "tool_calls": [
                    {"id": "t1", "type": "function", "function": {"name": "read_file", "arguments": "{}"}}
                ]},
                {"role": "tool", "tool_call_id": "t1", "content": "ZEBRA"}
            ]
        });
        let p1 = conversation_prefix(&turn1);
        let p2 = conversation_prefix(&turn2);
        assert!(!p1.is_empty());
        assert_eq!(p1, p2);
        assert!(p1.contains("sys:You are a precise assistant."));
        assert!(p1.contains("user:Read /tmp/secret.txt"));
    }
}
