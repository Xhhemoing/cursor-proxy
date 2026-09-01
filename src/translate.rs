//! OpenAI / Anthropic / Responses 翻译层: Cursor Stream 帧 → 客户端 SSE / JSON.

use futures_util::stream::{Stream, StreamExt};
use serde_json::{json, Value};
use std::pin::Pin;
use uuid::Uuid;

use crate::protocol::{
    anthropic_message, apply_tool_call_part, openai_message_with_tools, responses_message,
    sse_event, AssistantOut,
};
use crate::cursor::MAX_TOKENS_FLOOR;

pub fn openai_error(message: &str, code: &str, status: u16) -> Value {
    json!({
        "error": {
            "message": message,
            "type": if status >= 500 { "server_error" } else { "invalid_request_error" },
            "code": code,
        }
    })
}

pub fn openai_chunk(chunk_id: &str, model: &str, delta: Value, finish: Option<&str>) -> String {
    let payload = json!({
        "id": chunk_id,
        "object": "chat.completion.chunk",
        "created": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        "model": model,
        "choices": [{
            "index": 0,
            "delta": delta,
            "finish_reason": finish,
        }],
    });
    // 使用 to_string 预分配 + 单次写入，避免多次格式化和分配
    let mut buf = String::with_capacity(256);
    buf.push_str("data: ");
    // String 实现了 fmt::Write 但不实现 io::Write，所以直接用 to_string
    // 关键优化是预分配容量 + 避免 format! 的二次分配
    let json_str = serde_json::to_string(&payload).unwrap();
    buf.push_str(&json_str);
    buf.push_str("\n\n");
    buf
}

/// 一次请求的 token 用量 (四类分别计价)
///
/// `input` 为**不含缓存**的输入 tokens: OpenAI 风格 `prompt_tokens_details.cached_tokens` 是
/// prompt 的子集, 解析时已扣除; Anthropic 风格 `cache_read_input_tokens` 本身就独立于 input_tokens.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
}

impl Usage {
    pub fn total(&self) -> u64 {
        self.input + self.output + self.cache_read + self.cache_write
    }
    pub fn to_openai_json(&self) -> Value {
        json!({
            "prompt_tokens": self.input + self.cache_read,
            "completion_tokens": self.output,
            "total_tokens": self.total(),
            "prompt_tokens_details": { "cached_tokens": self.cache_read },
            "cache_creation_input_tokens": self.cache_write,
        })
    }
}

fn pick_u64(u: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter().find_map(|k| u.get(*k).and_then(|v| v.as_u64()))
}

/// 从上游帧提取 usage (兼容 Cursor extendedUsage / OpenAI / Anthropic 命名)
fn extract_usage(obj: &Value) -> Option<Usage> {
    let u = obj.get("extendedUsage").or_else(|| obj.get("usage"))?;
    let mut input = pick_u64(
        u,
        &[
            "promptTokens",
            "inputTokens",
            "prompt_tokens",
            "input_tokens",
        ],
    )
    .unwrap_or(0);
    let output = pick_u64(
        u,
        &[
            "completionTokens",
            "outputTokens",
            "completion_tokens",
            "output_tokens",
        ],
    )
    .unwrap_or(0);
    let mut cache_read = pick_u64(
        u,
        &[
            "cacheReadTokens",
            "cacheReadInputTokens",
            "cache_read_input_tokens",
            "cache_read_tokens",
        ],
    )
    .unwrap_or(0);
    let cache_write = pick_u64(
        u,
        &[
            "cacheWriteTokens",
            "cacheWriteInputTokens",
            "cacheCreationInputTokens",
            "cache_creation_input_tokens",
            "cache_write_tokens",
        ],
    )
    .unwrap_or(0);
    if cache_read == 0 {
        let cached = u
            .get("promptTokensDetails")
            .or_else(|| u.get("prompt_tokens_details"))
            .and_then(|d| pick_u64(d, &["cachedTokens", "cached_tokens"]))
            .or_else(|| pick_u64(u, &["cachedTokens", "cached_tokens"]))
            .unwrap_or(0);
        if cached > 0 {
            cache_read = cached;
            input = input.saturating_sub(cached);
        }
    }
    Some(Usage {
        input,
        output,
        cache_read,
        cache_write,
    })
}

fn tool_call_part<'a>(obj: &'a Value) -> Option<&'a Value> {
    obj.get("toolCallPart")
        .or_else(|| obj.get("functionCallPart"))
        .or_else(|| obj.get("toolCall"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dialect {
    Chat,
    Anthropic,
    Responses,
}

fn openai_tool_delta(index: usize, id: &str, name: &str, args_delta: &str) -> Value {
    json!({
        "tool_calls": [{
            "index": index,
            "id": id,
            "type": "function",
            "function": {"name": name, "arguments": args_delta},
        }]
    })
}

/// 检测上游帧是否为输出 token 限制错误 (isOutputTokenLimitError)
/// Cursor 在输出超限时会在流中发送此错误帧, 需要自动降 budget 重试.
pub fn is_output_token_limit_error(obj: &Value) -> bool {
    // 检查 error 字段
    if let Some(err) = obj.get("error").and_then(|v| v.as_str()) {
        if err.contains("isOutputTokenLimitError") || err.contains("output token limit") {
            return true;
        }
    }
    // 检查 errorCode / errorType
    if let Some(code) = obj.get("errorCode").and_then(|v| v.as_str()) {
        if code.contains("OutputTokenLimit") || code.contains("output_token_limit") {
            return true;
        }
    }
    // 检查 message 字段
    if let Some(msg) = obj.get("message").and_then(|v| v.as_str()) {
        if msg.contains("output token limit") || msg.contains("max output tokens") {
            return true;
        }
    }
    false
}

/// 检测上游帧是否为输出 token 限制错误 (从错误字符串)
pub fn is_output_token_limit_error_str(err: &str) -> bool {
    err.contains("isOutputTokenLimitError")
        || err.contains("output token limit")
        || err.contains("max output tokens")
        || err.contains("OutputTokenLimit")
}

/// 计算重试时的降低后 maxTokens: 当前值减半, 但不低于 floor.
pub fn lower_output_budget(current: u32) -> u32 {
    let halved = current / 2;
    if halved < MAX_TOKENS_FLOOR {
        MAX_TOKENS_FLOOR
    } else {
        halved
    }
}

/// 流式翻译 — 预分配缓冲 + 减少中间分配
pub fn upstream_to_dialect_stream<E>(
    frames: Pin<Box<dyn Stream<Item = Result<Value, E>> + Send>>,
    model: &str,
    dialect: Dialect,
) -> impl Stream<Item = Result<(String, Option<Usage>), String>>
where
    E: std::fmt::Display + Send + 'static,
{
    let id = match dialect {
        Dialect::Chat => format!("chatcmpl-{}", Uuid::new_v4().simple()),
        Dialect::Anthropic => format!("msg_{}", Uuid::new_v4().simple()),
        Dialect::Responses => format!("resp_{}", Uuid::new_v4().simple()),
    };
    let model = model.to_string();
    let mut first = true;
    let mut usage: Option<Usage> = None;
    let mut out = AssistantOut::default();
    let mut started_tools: Vec<String> = Vec::new();
    let mut anthropic_text_open = false;
    let mut anthropic_started = false;
    // P0: 预分配 SSE 缓冲，减少高频小帧的重复分配 (16k 覆盖大多数帧批量)
    let mut sse_buf = String::with_capacity(16384);

    futures_util::stream::unfold(
        (
            frames,
            id,
            model,
            first,
            usage,
            out,
            started_tools,
            dialect,
            anthropic_text_open,
            anthropic_started,
            sse_buf,
        ),
        move |(
            mut frames,
            id,
            model,
            mut first,
            mut usage,
            mut out,
            mut started_tools,
            dialect,
            mut anthropic_text_open,
            mut anthropic_started,
            mut sse_buf,
        )| async move {
            // 每帧复用缓冲
            let sse_buf = &mut sse_buf;
            loop {
                match frames.next().await {
                    Some(Ok(obj)) => {
                        if let Some(u) = extract_usage(&obj) {
                            usage = Some(u);
                        }
                        let mut sse = String::new();
                        if dialect == Dialect::Anthropic && !anthropic_started {
                            anthropic_started = true;
                            sse.push_str(&sse_event(
                                "message_start",
                                &json!({
                                    "type": "message_start",
                                    "message": {
                                        "id": id,
                                        "type": "message",
                                        "role": "assistant",
                                        "model": model,
                                        "content": [],
                                        "stop_reason": Value::Null,
                                        "usage": {"input_tokens": 0, "output_tokens": 0},
                                    }
                                }),
                            ));
                        }
                        if dialect == Dialect::Responses && first {
                            sse.push_str(&sse_event(
                                "response.created",
                                &json!({
                                    "type": "response.created",
                                    "response": {"id": id, "object": "response", "status": "in_progress", "model": model, "output": []}
                                }),
                            ));
                        }

                        if let Some(tp) = obj.get("textPart").and_then(|v| v.as_object()) {
                            if let Some(text) = tp.get("text").and_then(|v| v.as_str()) {
                                if !text.is_empty() {
                                    out.text.push_str(text);
                                    match dialect {
                                        Dialect::Chat => {
                                            let delta = if first {
                                                first = false;
                                                json!({"role": "assistant", "content": text})
                                            } else {
                                                json!({"content": text})
                                            };
                                            sse_buf.push_str(&openai_chunk(&id, &model, delta, None));
                                        }
                                        Dialect::Anthropic => {
                                            if !anthropic_text_open {
                                                anthropic_text_open = true;
                                                sse_buf.push_str(&sse_event(
                                                    "content_block_start",
                                                    &json!({
                                                        "type": "content_block_start",
                                                        "index": 0,
                                                        "content_block": {"type": "text", "text": ""}
                                                    }),
                                                ));
                                            }
                                            sse_buf.push_str(&sse_event(
                                                "content_block_delta",
                                                &json!({
                                                    "type": "content_block_delta",
                                                    "index": 0,
                                                    "delta": {"type": "text_delta", "text": text}
                                                }),
                                            ));
                                        }
                                        Dialect::Responses => {
                                            first = false;
                                            sse_buf.push_str(&sse_event(
                                                "response.output_text.delta",
                                                &json!({"type": "response.output_text.delta", "delta": text}),
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                        if let Some(think) = obj.get("thinkingPart").and_then(|v| v.as_object()) {
                            if let Some(text) = think.get("text").and_then(|v| v.as_str()) {
                                if !text.is_empty() && dialect == Dialect::Chat {
                                    sse_buf.push_str(&openai_chunk(
                                        &id,
                                        &model,
                                        json!({"content": format!("<think>{}</think>", text)}),
                                        None,
                                    ));
                                }
                            }
                        }
                        if let Some(part) = tool_call_part(&obj) {
                            let before = out.tool_calls.len();
                            apply_tool_call_part(&mut out, part);
                            if out.tool_calls.len() > before || !out.tool_calls.is_empty() {
                                let idx = out.tool_calls.len().saturating_sub(1);
                                let c = &out.tool_calls[idx];
                                let args_delta = part
                                    .get("argumentsDelta")
                                    .or_else(|| part.get("delta"))
                                    .and_then(|v| v.as_str())
                                    .map(String::from)
                                    .unwrap_or_else(|| {
                                        if started_tools.iter().any(|x| x == &c.id) {
                                            String::new()
                                        } else {
                                            c.arguments.clone()
                                        }
                                    });
                                match dialect {
                                    Dialect::Chat => {
                                        first = false;
                                        sse_buf.push_str(&openai_chunk(
                                            &id,
                                            &model,
                                            openai_tool_delta(idx, &c.id, &c.name, &args_delta),
                                            None,
                                        ));
                                    }
                                    Dialect::Anthropic => {
                                        if !started_tools.iter().any(|x| x == &c.id) {
                                            let index =
                                                if anthropic_text_open { 1 + idx } else { idx };
                                            sse_buf.push_str(&sse_event(
                                                "content_block_start",
                                                &json!({
                                                    "type": "content_block_start",
                                                    "index": index,
                                                    "content_block": {
                                                        "type": "tool_use",
                                                        "id": c.id,
                                                        "name": c.name,
                                                        "input": {}
                                                    }
                                                }),
                                            ));
                                            started_tools.push(c.id.clone());
                                        }
                                        if !args_delta.is_empty() {
                                            let index =
                                                if anthropic_text_open { 1 + idx } else { idx };
                                            sse_buf.push_str(&sse_event(
                                                "content_block_delta",
                                                &json!({
                                                    "type": "content_block_delta",
                                                    "index": index,
                                                    "delta": {"type": "input_json_delta", "partial_json": args_delta}
                                                }),
                                            ));
                                        }
                                    }
                                    Dialect::Responses => {
                                        first = false;
                                        if !started_tools.iter().any(|x| x == &c.id) {
                                            sse_buf.push_str(&sse_event(
                                                "response.output_item.added",
                                                &json!({
                                                    "type": "response.output_item.added",
                                                    "item": {
                                                        "type": "function_call",
                                                        "id": format!("fc_{}", c.id),
                                                        "call_id": c.id,
                                                        "name": c.name,
                                                        "arguments": ""
                                                    }
                                                }),
                                            ));
                                            started_tools.push(c.id.clone());
                                        }
                                        if !args_delta.is_empty() {
                                            sse_buf.push_str(&sse_event(
                                                "response.function_call_arguments.delta",
                                                &json!({
                                                    "type": "response.function_call_arguments.delta",
                                                    "call_id": c.id,
                                                    "delta": args_delta
                                                }),
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                        if obj.get("responseInfo").is_some() || obj.get("invocationId").is_some() {
                            let u = usage.unwrap_or_default();
                            // 从 responseInfo 提取上游真实的 stop reason
                            // Cursor 帧: responseInfo.stopReason 可能是 "STOP" / "MAX_TOKENS" / "LENGTH" 等
                            let upstream_stop = obj
                                .get("responseInfo")
                                .and_then(|ri| ri.get("stopReason"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            out.upstream_stop_reason = upstream_stop.to_string();
                            let was_truncated = crate::protocol::out_was_truncated(&out);
                            match dialect {
                                Dialect::Chat => {
                                    let finish = if !out.tool_calls.is_empty() {
                                        "tool_calls"
                                    } else if was_truncated {
                                        "length"
                                    } else {
                                        "stop"
                                    };
                                    sse_buf.push_str(&openai_chunk(
                                        &id,
                                        &model,
                                        json!({}),
                                        Some(finish),
                                    ));
                                    sse_buf.push_str("data: [DONE]\n\n");
                                }
                                Dialect::Anthropic => {
                                    if anthropic_text_open {
                                        sse_buf.push_str(&sse_event(
                                            "content_block_stop",
                                            &json!({"type": "content_block_stop", "index": 0}),
                                        ));
                                    }
                                    for (i, _) in out.tool_calls.iter().enumerate() {
                                        let index = if anthropic_text_open { 1 + i } else { i };
                                        sse_buf.push_str(&sse_event(
                                            "content_block_stop",
                                            &json!({"type": "content_block_stop", "index": index}),
                                        ));
                                    }
                                    let stop = if !out.tool_calls.is_empty() {
                                        "tool_use"
                                    } else if was_truncated {
                                        "max_tokens"
                                    } else {
                                        "end_turn"
                                    };
                                    sse_buf.push_str(&sse_event(
                                        "message_delta",
                                        &json!({
                                            "type": "message_delta",
                                            "delta": {"stop_reason": stop, "stop_sequence": Value::Null},
                                            "usage": {"output_tokens": u.output}
                                        }),
                                    ));
                                    sse_buf.push_str(&sse_event(
                                        "message_stop",
                                        &json!({"type": "message_stop"}),
                                    ));
                                }
                                Dialect::Responses => {
                                    let final_obj = responses_message(&id, &model, &out, &u);
                                    sse_buf.push_str(&sse_event(
                                        "response.completed",
                                        &json!({"type": "response.completed", "response": final_obj}),
                                    ));
                                }
                            }
                            return Some((
                                Ok((std::mem::take(sse_buf), usage)),
                                (
                                    frames,
                                    id,
                                    model,
                                    first,
                                    usage,
                                    out,
                                    started_tools,
                                    dialect,
                                    anthropic_text_open,
                                    anthropic_started,
                                    String::with_capacity(16384), // 重置缓冲 (16k 预分配)
                                ),
                            ));
                        }
                        if sse_buf.is_empty() {
                            continue;
                        }
                        return Some((
                            Ok((std::mem::take(sse_buf), usage)),
                            (
                                frames,
                                id,
                                model,
                                first,
                                usage,
                                out,
                                started_tools,
                                dialect,
                                anthropic_text_open,
                                anthropic_started,
                                String::with_capacity(16384), // 重置缓冲 (16k 预分配)
                            ),
                        ));
                    }
                    Some(Err(e)) => {
                        return Some((
                            Err(e.to_string()),
                            (
                                frames,
                                id,
                                model,
                                first,
                                usage,
                                out,
                                started_tools,
                                dialect,
                                anthropic_text_open,
                                anthropic_started,
                                String::with_capacity(16384), // 重置缓冲 (16k 预分配)
                            ),
                        ));
                    }
                    None => return None,
                }
            }
        },
    )
}

pub fn upstream_to_openai_stream<E>(
    frames: Pin<Box<dyn Stream<Item = Result<Value, E>> + Send>>,
    model: &str,
) -> impl Stream<Item = Result<(String, Option<Usage>), String>>
where
    E: std::fmt::Display + Send + 'static,
{
    upstream_to_dialect_stream(frames, model, Dialect::Chat)
}

pub async fn upstream_to_dialect_full<E>(
    mut frames: Pin<Box<dyn Stream<Item = Result<Value, E>> + Send>>,
    model: &str,
    dialect: Dialect,
) -> Result<(Value, Usage), String>
where
    E: std::fmt::Display + Send + 'static,
{
    let id = match dialect {
        Dialect::Chat => format!("chatcmpl-{}", Uuid::new_v4().simple()),
        Dialect::Anthropic => format!("msg_{}", Uuid::new_v4().simple()),
        Dialect::Responses => format!("resp_{}", Uuid::new_v4().simple()),
    };
    let mut out = AssistantOut::default();
    let mut usage = Usage::default();

    while let Some(item) = frames.next().await {
        let obj = item.map_err(|e| e.to_string())?;
        if let Some(tp) = obj.get("textPart").and_then(|v| v.as_object()) {
            if let Some(t) = tp.get("text").and_then(|v| v.as_str()) {
                out.text.push_str(t);
            }
        }
        if let Some(part) = tool_call_part(&obj) {
            apply_tool_call_part(&mut out, part);
        }
        if let Some(u) = extract_usage(&obj) {
            usage = u;
        }
        if obj.get("responseInfo").is_some() || obj.get("invocationId").is_some() {
            // 提取上游 stop reason 用于截断检测
            if let Some(sr) = obj
                .get("responseInfo")
                .and_then(|ri| ri.get("stopReason"))
                .and_then(|v| v.as_str())
            {
                out.upstream_stop_reason = sr.to_string();
            }
            break;
        }
    }

    let value = match dialect {
        Dialect::Chat => openai_message_with_tools(&id, model, &out, &usage),
        Dialect::Anthropic => anthropic_message(&id, model, &out, &usage),
        Dialect::Responses => responses_message(&id, model, &out, &usage),
    };
    Ok((value, usage))
}

pub async fn upstream_to_openai_full<E>(
    frames: Pin<Box<dyn Stream<Item = Result<Value, E>> + Send>>,
    model: &str,
) -> Result<(Value, Usage), String>
where
    E: std::fmt::Display + Send + 'static,
{
    upstream_to_dialect_full(frames, model, Dialect::Chat).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;

    #[tokio::test]
    async fn full_collects_tool_call() {
        let frames = stream::iter(vec![
            Ok::<Value, String>(
                json!({"toolCallPart": {"name": "bash", "toolCallId": "c1", "arguments": {"command": "ls"}}}),
            ),
            Ok(
                json!({"extendedUsage": {"promptTokens": 4, "completionTokens": 2}, "responseInfo": {}}),
            ),
        ]);
        let (v, u) = upstream_to_openai_full(Box::pin(frames), "kimi-k3")
            .await
            .unwrap();
        assert_eq!(v["choices"][0]["finish_reason"], "tool_calls");
        assert_eq!(
            v["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
            "bash"
        );
        assert_eq!(u.input, 4);
        let frames = stream::iter(vec![
            Ok::<Value, String>(
                json!({"toolCallPart": {"name": "bash", "toolCallId": "c1", "arguments": {"command": "ls"}}}),
            ),
            Ok(json!({"responseInfo": {}})),
        ]);
        let (v, _) = upstream_to_dialect_full(Box::pin(frames), "kimi-k3", Dialect::Anthropic)
            .await
            .unwrap();
        assert_eq!(v["stop_reason"], "tool_use");
        assert_eq!(v["content"][0]["type"], "tool_use");
    }
}
