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
        .unwrap_or_else(|| json!({"type": "object", "properties": {}}))
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
                "parameters": schema_of_fn(f),
            }));
            continue;
        }
        if let Some(name) = t.get("name").and_then(|v| v.as_str()) {
            if !name.is_empty() {
                out.push(json!({
                    "name": name,
                    "description": t.get("description").and_then(|v| v.as_str()).unwrap_or(""),
                    "parameters": schema_of_fn(t),
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

/// OpenAI Chat 消息 → Cursor messages[].parts
pub fn openai_messages_to_cursor(messages: &[Value]) -> Vec<Value> {
    let mut out = Vec::new();
    for m in messages {
        let role = m.get("role").and_then(|v| v.as_str()).unwrap_or("user");
        if role == "tool" {
            let call_id = m.get("tool_call_id").and_then(|v| v.as_str()).unwrap_or("");
            let text = content_to_text(m.get("content").unwrap_or(&Value::Null));
            out.push(json!({
                "role": "user",
                "parts": {"parts": [{
                    "functionResult": {
                        "callId": call_id,
                        "result": text,
                    }
                }]},
            }));
            continue;
        }
        let mut parts: Vec<Value> = Vec::new();
        let text = content_to_text(m.get("content").unwrap_or(&Value::Null));
        if !text.is_empty() {
            parts.push(json!({"text": {"text": text}}));
        }
        if let Some(calls) = m.get("tool_calls").and_then(|v| v.as_array()) {
            for c in calls {
                let id = c.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let f = c.get("function").unwrap_or(c);
                let name = f.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let args = f
                    .get("arguments")
                    .map(args_to_string)
                    .unwrap_or_else(|| "{}".into());
                parts.push(json!({
                    "functionCall": {
                        "name": name,
                        "arguments": args_to_object(&args),
                        "callId": id,
                    }
                }));
            }
        }
        if parts.is_empty() {
            parts.push(json!({"text": {"text": ""}}));
        }
        let cursor_role = if role == "system" { "user" } else { role };
        let mut msg = json!({
            "role": cursor_role,
            "parts": {"parts": parts},
        });
        if role == "system" {
            msg["system"] = json!(true);
        }
        out.push(msg);
    }
    out
}

pub fn tool_choice_hint(tool_choice: Option<&Value>) -> Option<String> {
    let v = tool_choice?;
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
        out["tool_choice"] = tc.clone();
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
    let mut out = json!({
        "model": model,
        "messages": messages,
        "stream": body.get("stream").and_then(|v| v.as_bool()).unwrap_or(false),
    });
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
        out["tool_choice"] = tc.clone();
    }
    if let Some(p) = body.get("parallel_tool_calls") {
        out["parallel_tool_calls"] = p.clone();
    }
    Ok(out)
}

pub fn parse_tool_call_part(part: &Value) -> Option<(String, String, String, bool)> {
    // returns (id, name, args_or_delta, is_delta)
    let id = part
        .get("toolCallId")
        .or_else(|| part.get("callId"))
        .or_else(|| part.get("id"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let name = part
        .get("name")
        .or_else(|| part.get("toolName"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if let Some(d) = part
        .get("argumentsDelta")
        .or_else(|| part.get("delta"))
        .or_else(|| part.get("partialArguments"))
        .and_then(|v| v.as_str())
    {
        return Some((id, name, d.to_string(), true));
    }
    if let Some(a) = part.get("arguments").or_else(|| part.get("args")) {
        return Some((id, name, args_to_string(a), false));
    }
    if name.is_empty() && id.is_empty() {
        return None;
    }
    Some((id, name, String::new(), false))
}

pub fn apply_tool_call_part(out: &mut AssistantOut, part: &Value) {
    let Some((id, name, payload, is_delta)) = parse_tool_call_part(part) else {
        return;
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
            existing.arguments.push_str(&payload);
        } else if !payload.is_empty() {
            existing.arguments = payload;
        }
        return;
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
}

pub fn anthropic_message(
    id: &str,
    model: &str,
    out: &AssistantOut,
    usage: &crate::translate::Usage,
) -> Value {
    let mut content: Vec<Value> = Vec::new();
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
    let stop = if out.tool_calls.is_empty() {
        "end_turn"
    } else {
        "tool_use"
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

pub fn responses_message(
    id: &str,
    model: &str,
    out: &AssistantOut,
    usage: &crate::translate::Usage,
) -> Value {
    let mut output: Vec<Value> = Vec::new();
    if !out.text.is_empty() || out.tool_calls.is_empty() {
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

pub fn openai_message_with_tools(
    id: &str,
    model: &str,
    out: &AssistantOut,
    usage: &crate::translate::Usage,
) -> Value {
    let finish = if out.tool_calls.is_empty() {
        "stop"
    } else {
        "tool_calls"
    };
    let mut message = json!({
        "role": "assistant",
        "content": if out.text.is_empty() { Value::Null } else { json!(out.text) },
    });
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
        assert_eq!(tools[0]["parameters"]["required"][0], "command");
        let cursor_msgs = openai_messages_to_cursor(msgs);
        assert_eq!(
            cursor_msgs[1]["parts"]["parts"][0]["functionCall"]["callId"],
            "toolu_1"
        );
        assert_eq!(
            cursor_msgs[2]["parts"]["parts"][0]["functionResult"]["result"],
            "a.txt"
        );
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
        assert_eq!(
            cursor_msgs.last().unwrap()["parts"]["parts"][0]["functionResult"]["callId"],
            "call_9"
        );
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
}
