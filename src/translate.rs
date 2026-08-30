//! OpenAI / Anthropic / Responses 翻译层: Cursor Stream 帧 → 客户端 SSE / JSON.

use futures_util::stream::{Stream, StreamExt};
use serde_json::{json, Value};
use std::pin::Pin;
use uuid::Uuid;

use crate::protocol::{
    apply_tool_call_part, sse_event, anthropic_message, openai_message_with_tools, responses_message,
    AssistantOut,
};

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
    format!("data: {}\n\n", serde_json::to_string(&payload).unwrap())
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
    let mut input =
        pick_u64(u, &["promptTokens", "inputTokens", "prompt_tokens", "input_tokens"]).unwrap_or(0);
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

/// 流式翻译
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
        ),
        |(
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
        )| async move {
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
                                            sse.push_str(&openai_chunk(&id, &model, delta, None));
                                        }
                                        Dialect::Anthropic => {
                                            if !anthropic_text_open {
                                                anthropic_text_open = true;
                                                sse.push_str(&sse_event(
                                                    "content_block_start",
                                                    &json!({
                                                        "type": "content_block_start",
                                                        "index": 0,
                                                        "content_block": {"type": "text", "text": ""}
                                                    }),
                                                ));
                                            }
                                            sse.push_str(&sse_event(
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
                                            sse.push_str(&sse_event(
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
                                    sse.push_str(&openai_chunk(
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
                                        sse.push_str(&openai_chunk(
                                            &id,
                                            &model,
                                            openai_tool_delta(idx, &c.id, &c.name, &args_delta),
                                            None,
                                        ));
                                    }
                                    Dialect::Anthropic => {
                                        if !started_tools.iter().any(|x| x == &c.id) {
                                            let index = if anthropic_text_open { 1 + idx } else { idx };
                                            sse.push_str(&sse_event(
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
                                            let index = if anthropic_text_open { 1 + idx } else { idx };
                                            sse.push_str(&sse_event(
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
                                            sse.push_str(&sse_event(
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
                                            sse.push_str(&sse_event(
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
                            match dialect {
                                Dialect::Chat => {
                                    let finish = if out.tool_calls.is_empty() {
                                        "stop"
                                    } else {
                                        "tool_calls"
                                    };
                                    sse.push_str(&openai_chunk(&id, &model, json!({}), Some(finish)));
                                    sse.push_str("data: [DONE]\n\n");
                                }
                                Dialect::Anthropic => {
                                    if anthropic_text_open {
                                        sse.push_str(&sse_event(
                                            "content_block_stop",
                                            &json!({"type": "content_block_stop", "index": 0}),
                                        ));
                                    }
                                    for (i, _) in out.tool_calls.iter().enumerate() {
                                        let index = if anthropic_text_open { 1 + i } else { i };
                                        sse.push_str(&sse_event(
                                            "content_block_stop",
                                            &json!({"type": "content_block_stop", "index": index}),
                                        ));
                                    }
                                    let stop = if out.tool_calls.is_empty() {
                                        "end_turn"
                                    } else {
                                        "tool_use"
                                    };
                                    sse.push_str(&sse_event(
                                        "message_delta",
                                        &json!({
                                            "type": "message_delta",
                                            "delta": {"stop_reason": stop, "stop_sequence": Value::Null},
                                            "usage": {"output_tokens": u.output}
                                        }),
                                    ));
                                    sse.push_str(&sse_event(
                                        "message_stop",
                                        &json!({"type": "message_stop"}),
                                    ));
                                }
                                Dialect::Responses => {
                                    let final_obj = responses_message(&id, &model, &out, &u);
                                    sse.push_str(&sse_event(
                                        "response.completed",
                                        &json!({"type": "response.completed", "response": final_obj}),
                                    ));
                                }
                            }
                            return Some((
                                Ok((sse, usage)),
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
                                ),
                            ));
                        }
                        if sse.is_empty() {
                            continue;
                        }
                        return Some((
                            Ok((sse, usage)),
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
            Ok::<Value, String>(json!({"toolCallPart": {"name": "bash", "toolCallId": "c1", "arguments": {"command": "ls"}}})),
            Ok(json!({"extendedUsage": {"promptTokens": 4, "completionTokens": 2}, "responseInfo": {}})),
        ]);
        let (v, u) = upstream_to_openai_full(Box::pin(frames), "kimi-k3")
            .await
            .unwrap();
        assert_eq!(v["choices"][0]["finish_reason"], "tool_calls");
        assert_eq!(v["choices"][0]["message"]["tool_calls"][0]["function"]["name"], "bash");
        assert_eq!(u.input, 4);
        let frames = stream::iter(vec![
            Ok::<Value, String>(json!({"toolCallPart": {"name": "bash", "toolCallId": "c1", "arguments": {"command": "ls"}}})),
            Ok(json!({"responseInfo": {}})),
        ]);
        let (v, _) = upstream_to_dialect_full(Box::pin(frames), "kimi-k3", Dialect::Anthropic)
            .await
            .unwrap();
        assert_eq!(v["stop_reason"], "tool_use");
        assert_eq!(v["content"][0]["type"], "tool_use");
    }
}
