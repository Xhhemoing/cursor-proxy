//! OpenAI 兼容翻译层: Cursor Stream 帧 → OpenAI ChatCompletion SSE.

use futures_util::stream::{Stream, StreamExt};
use serde_json::{json, Value};
use std::pin::Pin;
use uuid::Uuid;

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

pub fn openai_full_response(chunk_id: &str, model: &str, text: &str, usage: Option<Value>) -> Value {
    json!({
        "id": chunk_id,
        "object": "chat.completion",
        "created": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        "model": model,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": text},
            "finish_reason": "stop",
        }],
        "usage": usage.unwrap_or(json!({
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        })),
    })
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
    let mut input = pick_u64(u, &["promptTokens", "inputTokens", "prompt_tokens", "input_tokens"]).unwrap_or(0);
    let output = pick_u64(u, &["completionTokens", "outputTokens", "completion_tokens", "output_tokens"]).unwrap_or(0);
    // 独立字段 (Anthropic 风格): 不属于 input
    let mut cache_read = pick_u64(u, &["cacheReadTokens", "cacheReadInputTokens", "cache_read_input_tokens", "cache_read_tokens"]).unwrap_or(0);
    let cache_write = pick_u64(u, &["cacheWriteTokens", "cacheCreationInputTokens", "cache_creation_input_tokens", "cache_write_tokens"]).unwrap_or(0);
    // 子集字段 (OpenAI 风格): 从 prompt 中扣除
    if cache_read == 0 {
        let cached = u.get("promptTokensDetails").or_else(|| u.get("prompt_tokens_details"))
            .and_then(|d| pick_u64(d, &["cachedTokens", "cached_tokens"]))
            .or_else(|| pick_u64(u, &["cachedTokens", "cached_tokens"]))
            .unwrap_or(0);
        if cached > 0 {
            cache_read = cached;
            input = input.saturating_sub(cached);
        }
    }
    Some(Usage { input, output, cache_read, cache_write })
}

/// 流式翻译: 上游帧流 → OpenAI SSE 流（泛化错误类型，支持 Cursor/OpenAI 等任意上游）
pub fn upstream_to_openai_stream<E>(
    frames: Pin<Box<dyn Stream<Item = Result<Value, E>> + Send>>,
    model: &str,
) -> impl Stream<Item = Result<(String, Option<Usage>), String>>
where
    E: std::fmt::Display + Send + 'static,
{
    let chunk_id = format!("chatcmpl-{}", Uuid::new_v4().simple());
    let model = model.to_string();
    let mut first = true;
    let mut usage: Option<Usage> = None;

    futures_util::stream::unfold(
        (frames, chunk_id, model, first, usage),
        |(mut frames, chunk_id, model, mut first, mut usage)| async move {
            loop {
                match frames.next().await {
                    Some(Ok(obj)) => {
                        // textPart
                        if let Some(tp) = obj.get("textPart").and_then(|v| v.as_object()) {
                            if let Some(text) = tp.get("text").and_then(|v| v.as_str()) {
                                if !text.is_empty() {
                                    let delta = if first {
                                        first = false;
                                        json!({"role": "assistant", "content": text})
                                    } else {
                                        json!({"content": text})
                                    };
                                    return Some((Ok((openai_chunk(&chunk_id, &model, delta, None), usage)), (frames, chunk_id, model, first, usage)));
                                }
                            }
                        }
                        // thinkingPart
                        if let Some(think) = obj.get("thinkingPart").and_then(|v| v.as_object()) {
                            if let Some(text) = think.get("text").and_then(|v| v.as_str()) {
                                if !text.is_empty() {
                                    let delta = json!({"content": format!("<think>{}</think>", text)});
                                    return Some((Ok((openai_chunk(&chunk_id, &model, delta, None), usage)), (frames, chunk_id, model, first, usage)));
                                }
                            }
                        }
                        // usage
                        if let Some(u) = extract_usage(&obj) {
                            usage = Some(u);
                        }
                        // 结束
                        if obj.get("responseInfo").is_some() || obj.get("invocationId").is_some() {
                            let final_chunk = openai_chunk(&chunk_id, &model, json!({}), Some("stop"));
                            let done = "data: [DONE]\n\n".to_string();
                            return Some((Ok((format!("{}{}", final_chunk, done), usage)), (frames, chunk_id, model, first, usage)));
                        }
                        continue;
                    }
                    Some(Err(e)) => {
                        return Some((Err(e.to_string()), (frames, chunk_id, model, first, usage)));
                    }
                    None => {
                        return None;
                    }
                }
            }
        },
    )
}

/// 非流式翻译: 收集全部 text, 返回完整 ChatCompletion（泛化错误类型）
pub async fn upstream_to_openai_full<E>(
    mut frames: Pin<Box<dyn Stream<Item = Result<Value, E>> + Send>>,
    model: &str,
) -> Result<(Value, Usage), String>
where
    E: std::fmt::Display + Send + 'static,
{
    let chunk_id = format!("chatcmpl-{}", Uuid::new_v4().simple());
    let mut text = String::with_capacity(4096);
    let mut usage = Usage::default();

    while let Some(item) = frames.next().await {
        let obj = item.map_err(|e| e.to_string())?;
        if let Some(tp) = obj.get("textPart").and_then(|v| v.as_object()) {
            if let Some(t) = tp.get("text").and_then(|v| v.as_str()) {
                text.push_str(t);
            }
        }
        if let Some(u) = extract_usage(&obj) {
            usage = u;
        }
        if obj.get("responseInfo").is_some() || obj.get("invocationId").is_some() {
            break;
        }
    }

    Ok((openai_full_response(&chunk_id, model, &text, Some(usage.to_openai_json())), usage))
}
