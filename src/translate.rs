//! OpenAI / Anthropic / Responses 翻译层: Cursor Stream 帧 → 客户端 SSE / JSON.
//!
//! 与 Python 版 cursor-openai-proxy `translate.py` (2840 行) 对齐:
//! - P0: Chat 方言 `reasoning_content` 增量流 (DeepSeek-R1/Kimi-Thinking 标准)
//! - P0: Anthropic 方言独立 `thinking` content block + signature
//! - P0: Responses 方言完整状态机 (reasoning item / output_item / function_call)
//! - P1: 工具参数 JSON 增量合并 (merge_tool_arg_text) — 防跨帧 JSON 边界粘连
//! - P1: Anthropic 出站模型名反向映射 (display_model_id) — 客户端指纹检测
//! - P0: responseInfo.reasoningParts 兜底回填 (Grok/Gemini 思考末帧补发)

use futures_util::stream::{Stream, StreamExt};
use serde_json::{json, Value};
use std::pin::Pin;
use uuid::Uuid;

use crate::protocol::{
    anthropic_message, apply_tool_call_part, display_model_id, encode_reasoning,
    merge_tool_arg_text, openai_message_with_tools, responses_message_with_request, sse_event,
    wants_encrypted_reasoning, AssistantOut,
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

fn tool_call_part(obj: &Value) -> Option<&Value> {
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
    if let Some(err) = obj.get("error").and_then(|v| v.as_str()) {
        if err.contains("isOutputTokenLimitError") || err.contains("output token limit") {
            return true;
        }
    }
    if let Some(code) = obj.get("errorCode").and_then(|v| v.as_str()) {
        if code.contains("OutputTokenLimit") || code.contains("output_token_limit") {
            return true;
        }
    }
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

/// Cursor 把业务错误塞进 connect 帧 `error` 对象；`message` 经常只是 "Error"。
/// 优先取 details[].debug.details.title / detail，避免客户端只看到 empty_content。
pub fn extract_cursor_error_message(obj: &Value) -> Option<String> {
    let err = obj.get("errorMessage").cloned().or_else(|| obj.get("error").cloned())?;
    if let Some(s) = err.as_str() {
        if !s.is_empty() && !s.eq_ignore_ascii_case("error") {
            return Some(s.to_string());
        }
    }
    let details0 = err
        .get("details")
        .and_then(|d| d.as_array())
        .and_then(|a| a.first());
    let title = details0
        .and_then(|d| d.pointer("/debug/details/title"))
        .and_then(|v| v.as_str())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty());
    let detail = details0
        .and_then(|d| d.pointer("/debug/details/detail"))
        .and_then(|v| v.as_str())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty());
    let nested_err = details0
        .and_then(|d| d.pointer("/debug/error"))
        .and_then(|v| v.as_str());
    let code = err.get("code").and_then(|v| v.as_str());
    let msg = err
        .get("message")
        .and_then(|v| v.as_str())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("error"));

    if let (Some(t), Some(d)) = (title, detail) {
        return Some(format!("{t}: {d}"));
    }
    if let Some(t) = title {
        return Some(t.to_string());
    }
    if let Some(d) = detail {
        return Some(d.to_string());
    }
    if let Some(m) = msg {
        return Some(m.to_string());
    }
    if let Some(c) = nested_err.filter(|s| !s.is_empty()) {
        return Some(c.to_string());
    }
    if let Some(c) = code {
        return Some(c.to_string());
    }
    obj.get("errorCode")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

pub fn is_max_mode_restricted(err: &str) -> bool {
    let e = err.to_ascii_lowercase();
    e.contains("max mode is only available")
        || e.contains("only available to paid users")
        || e.contains("membershiptoupgradeto")
}

/// Cursor 对 Kimi K3 的全局限流 / 上游过载。不是账号故障。
/// 记 consecutive_errors + 30s 冷却会在一次 High Load 里把整池打成 erroring，
/// 三次立刻换号重试也会在 ~2s 内打出客户端看到的
/// `HTTP 502: upstream error after 3 retries`。
pub fn is_upstream_capacity_error(err: &str) -> bool {
    let e = err.to_ascii_lowercase();
    if is_max_mode_restricted(&e) {
        return false;
    }
    e.contains("high load")
        || e.contains("high demand")
        || e.contains("try again in a few moments")
        || e.contains("error_rate_limited")
        || e.contains("trouble connecting to the model provider")
        || e.contains("we're having trouble connecting")
        || e.contains("upstream http 502")
        || e.contains("502 bad gateway")
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

// ============================================================================
// 流式翻译状态机 — 把 Cursor Stream 帧转译成 OpenAI/Anthropic/Responses SSE
//
// 设计: 取代旧版 unfold 闭包内嵌 19 个状态变量的写法, 用 struct 集中管理.
// 每种方言持有独立的 block/index 状态, feed() 一次返回 0..N 条 SSE 字符串.
// ============================================================================

/// 流式翻译器状态机.
pub struct StreamTranslator {
    pub dialect: Dialect,
    pub id: String,
    /// 客户端可见的 model 名 (Anthropic 出站会做反向映射)
    pub public_model: String,
    /// Cursor 内部模型名 (用于日志/计费)
    pub upstream_model: String,
    pub out: AssistantOut,
    pub usage: Option<Usage>,
    /// OpenAI 方言: 是否已发首帧 role
    chat_first: bool,
    /// Anthropic 方言: 各 block 开关状态
    anthropic_started: bool,
    anthropic_thinking_open: bool,
    anthropic_text_open: bool,
    anthropic_thinking_idx: Option<usize>,
    anthropic_text_idx: Option<usize>,
    /// Responses 方言: 状态机
    responses_seq: u64,
    responses_created_sent: bool,
    responses_reasoning_id: Option<String>,
    responses_reasoning_open: bool,
    responses_reasoning_idx: Option<usize>,
    responses_message_id: Option<String>,
    responses_message_open: bool,
    responses_message_idx: Option<usize>,
    responses_next_output_idx: usize,
    /// 工具调用追踪 (跨方言共用)
    started_tools: Vec<String>,
    /// 已完成 (isComplete=true) 的工具调用 id 集合, 用于 finalize 时跳过重复发送
    completed_tools: std::collections::HashSet<String>,
    /// 各 tool_call 已发出的 arguments 前缀 (用于 merge_tool_arg_text 的 delta 计算)
    tool_emitted_args: std::collections::HashMap<String, String>,
    /// Responses 方言: 是否携带 encrypted reasoning
    responses_wants_encrypted: bool,
}

impl StreamTranslator {
    pub fn new(dialect: Dialect, upstream_model: &str, request_body: Option<&Value>) -> Self {
        let id = match dialect {
            Dialect::Chat => format!("chatcmpl-{}", Uuid::new_v4().simple()),
            Dialect::Anthropic => format!("msg_{}", Uuid::new_v4().simple()),
            Dialect::Responses => format!("resp_{}", Uuid::new_v4().simple()),
        };
        // Anthropic 出站需要对客户端展示官方 slug, 其他方言保留 Cursor 内部 ID
        let public_model = if dialect == Dialect::Anthropic {
            display_model_id(upstream_model)
        } else {
            upstream_model.to_string()
        };
        let responses_wants_encrypted = request_body
            .map(wants_encrypted_reasoning)
            .unwrap_or(false);
        Self {
            dialect,
            id,
            public_model,
            upstream_model: upstream_model.to_string(),
            out: AssistantOut::default(),
            usage: None,
            chat_first: true,
            anthropic_started: false,
            anthropic_thinking_open: false,
            anthropic_text_open: false,
            anthropic_thinking_idx: None,
            anthropic_text_idx: None,
            responses_seq: 0,
            responses_created_sent: false,
            responses_reasoning_id: None,
            responses_reasoning_open: false,
            responses_reasoning_idx: None,
            responses_message_id: None,
            responses_message_open: false,
            responses_message_idx: None,
            responses_next_output_idx: 0,
            started_tools: Vec::new(),
            completed_tools: std::collections::HashSet::new(),
            tool_emitted_args: std::collections::HashMap::new(),
            responses_wants_encrypted,
        }
    }

    fn next_seq(&mut self) -> u64 {
        self.responses_seq += 1;
        self.responses_seq
    }

    /// 处理单个上游帧, 返回需要发给客户端的 SSE 字符串列表.
    pub fn feed(&mut self, obj: &Value) -> Result<Vec<String>, String> {
        let mut out_events: Vec<String> = Vec::new();

        if let Some(u) = extract_usage(obj) {
            self.usage = Some(u);
        }

        // 调试: 记录上游返回的帧类型（无 textPart 时帮助诊断空内容根因）
        if obj.get("textPart").is_none()
            && obj.get("toolCallPart").is_none()
            && obj.get("responseInfo").is_none()
            && obj.get("invocationId").is_none()
            && obj.get("extendedUsage").is_none()
            && obj.get("usage").is_none()
            && obj.get("thinkingPart").is_none()
        {
            let frame_keys: Vec<&str> = obj
                .as_object()
                .map(|o| o.keys().map(|k| k.as_str()).collect())
                .unwrap_or_default();
            tracing::info!(
                event = "upstream_frame_unknown",
                keys = ?frame_keys,
                "upstream frame without textPart/toolCallPart/responseInfo"
            );
        }

        // 检测上游错误帧: Cursor 返回 error/errorMessage/errorCode 时直接报错
        if let Some(err_msg) = extract_cursor_error_message(obj) {
            tracing::error!(
                event = "upstream_error_frame",
                frame = %obj,
                "upstream returned error frame"
            );
            return Err(format!("upstream error: {err_msg}"));
        }

        // ---- 方言前导事件 ----
        match self.dialect {
            Dialect::Anthropic if !self.anthropic_started => {
                self.anthropic_started = true;
                out_events.push(sse_event(
                    "message_start",
                    &json!({
                        "type": "message_start",
                        "message": {
                            "id": self.id,
                            "type": "message",
                            "role": "assistant",
                            "model": self.public_model,
                            "content": [],
                            "stop_reason": Value::Null,
                            "usage": {"input_tokens": 0, "output_tokens": 0},
                        }
                    }),
                ));
            }
            Dialect::Responses if !self.responses_created_sent => {
                self.responses_created_sent = true;
                let seq = self.next_seq();
                out_events.push(sse_event(
                    "response.created",
                    &json!({
                        "type": "response.created",
                        "sequence_number": seq,
                        "response": {
                            "id": self.id,
                            "object": "response",
                            "status": "in_progress",
                            "model": self.public_model,
                            "output": []
                        }
                    }),
                ));
                let seq = self.next_seq();
                out_events.push(sse_event(
                    "response.in_progress",
                    &json!({
                        "type": "response.in_progress",
                        "sequence_number": seq,
                        "response": {
                            "id": self.id,
                            "object": "response",
                            "status": "in_progress",
                            "model": self.public_model,
                            "output": []
                        }
                    }),
                ));
            }
            _ => {}
        }

        // ---- thinkingPart: 思考增量 (三种方言各自编码) ----
        if let Some(think) = obj.get("thinkingPart").and_then(|v| v.as_object()) {
            if let Some(text) = think.get("text").and_then(|v| v.as_str()) {
                if !text.is_empty() {
                    self.out.thinking.push_str(text);
                    // 提取 signature (上游可能在 thinkingPart 内嵌 signature)
                    if let Some(sig) = think.get("signature").and_then(|v| v.as_str()) {
                        self.out.thinking_signature = Some(sig.to_string());
                    }
                    match self.dialect {
                        Dialect::Chat => {
                            // P0: OpenAI/DeepSeek 规范 — reasoning_content 增量字段
                            self.chat_first = false;
                            out_events.push(openai_chunk(
                                &self.id,
                                &self.public_model,
                                json!({"reasoning_content": text}),
                                None,
                            ));
                        }
                        Dialect::Anthropic => {
                            // P0: Anthropic 规范 — 独立 thinking content block
                            if !self.anthropic_thinking_open {
                                self.anthropic_thinking_open = true;
                                let idx = self.alloc_anthropic_thinking_idx();
                                out_events.push(sse_event(
                                    "content_block_start",
                                    &json!({
                                        "type": "content_block_start",
                                        "index": idx,
                                        "content_block": {"type": "thinking", "thinking": ""}
                                    }),
                                ));
                            }
                            let idx = self.anthropic_thinking_idx.unwrap_or(0);
                            out_events.push(sse_event(
                                "content_block_delta",
                                &json!({
                                    "type": "content_block_delta",
                                    "index": idx,
                                    "delta": {"type": "thinking_delta", "thinking": text}
                                }),
                            ));
                        }
                        Dialect::Responses => {
                            // P0: Responses 规范 — reasoning item + summary_text delta
                            if !self.responses_reasoning_open {
                                self.responses_reasoning_open = true;
                                let rid = format!("rs_{}", Uuid::new_v4().simple());
                                let idx = self.next_output_idx();
                                self.responses_reasoning_id = Some(rid.clone());
                                self.responses_reasoning_idx = Some(idx);
                                let seq = self.next_seq();
                                let mut item = json!({
                                    "id": rid,
                                    "type": "reasoning",
                                    "status": "in_progress",
                                    "summary": []
                                });
                                if self.responses_wants_encrypted {
                                    item["encrypted_content"] = json!("");
                                }
                                out_events.push(sse_event(
                                    "response.output_item.added",
                                    &json!({
                                        "type": "response.output_item.added",
                                        "sequence_number": seq,
                                        "output_index": idx,
                                        "item": item,
                                    }),
                                ));
                            }
                            let seq = self.next_seq();
                            out_events.push(sse_event(
                                "response.reasoning_summary_text.delta",
                                &json!({
                                    "type": "response.reasoning_summary_text.delta",
                                    "sequence_number": seq,
                                    "item_id": self.responses_reasoning_id.clone().unwrap_or_default(),
                                    "output_index": self.responses_reasoning_idx.unwrap_or(0),
                                    "summary_index": 0,
                                    "delta": text,
                                }),
                            ));
                        }
                    }
                }
            }
        }

        // ---- textPart: 正文增量 ----
        if let Some(tp) = obj.get("textPart").and_then(|v| v.as_object()) {
            if let Some(text) = tp.get("text").and_then(|v| v.as_str()) {
                if !text.is_empty() {
                    self.out.text.push_str(text);
                    match self.dialect {
                        Dialect::Chat => {
                            let delta = if self.chat_first {
                                self.chat_first = false;
                                json!({"role": "assistant", "content": text})
                            } else {
                                json!({"content": text})
                            };
                            out_events.push(openai_chunk(
                                &self.id,
                                &self.public_model,
                                delta,
                                None,
                            ));
                        }
                        Dialect::Anthropic => {
                            // 打开 text block 前先关 thinking block
                            if self.anthropic_thinking_open {
                                self.anthropic_thinking_open = false;
                                let idx = self.anthropic_thinking_idx.unwrap_or(0);
                                // Anthropic 规范: thinking block 关闭前要发 signature_delta
                                let sig = self.out.thinking_signature.clone().unwrap_or_else(|| {
                                    format!(
                                        "{}{}",
                                        crate::protocol::PROXY_SIGNATURE_MARK,
                                        Uuid::new_v4().simple()
                                    )
                                });
                                out_events.push(sse_event(
                                    "content_block_delta",
                                    &json!({
                                        "type": "content_block_delta",
                                        "index": idx,
                                        "delta": {"type": "signature_delta", "signature": sig}
                                    }),
                                ));
                                out_events.push(sse_event(
                                    "content_block_stop",
                                    &json!({"type": "content_block_stop", "index": idx}),
                                ));
                            }
                            if !self.anthropic_text_open {
                                self.anthropic_text_open = true;
                                let idx = self.alloc_anthropic_text_idx();
                                out_events.push(sse_event(
                                    "content_block_start",
                                    &json!({
                                        "type": "content_block_start",
                                        "index": idx,
                                        "content_block": {"type": "text", "text": ""}
                                    }),
                                ));
                            }
                            let idx = self.anthropic_text_idx.unwrap_or(0);
                            out_events.push(sse_event(
                                "content_block_delta",
                                &json!({
                                    "type": "content_block_delta",
                                    "index": idx,
                                    "delta": {"type": "text_delta", "text": text}
                                }),
                            ));
                        }
                        Dialect::Responses => {
                            // 打开 message 前先关 reasoning
                            if self.responses_reasoning_open {
                                out_events.extend(self.close_responses_reasoning());
                            }
                            if !self.responses_message_open {
                                self.responses_message_open = true;
                                let mid = format!("msg_{}", Uuid::new_v4().simple());
                                let idx = self.next_output_idx();
                                self.responses_message_id = Some(mid.clone());
                                self.responses_message_idx = Some(idx);
                                let seq = self.next_seq();
                                out_events.push(sse_event(
                                    "response.output_item.added",
                                    &json!({
                                        "type": "response.output_item.added",
                                        "sequence_number": seq,
                                        "output_index": idx,
                                        "item": {
                                            "id": mid,
                                            "type": "message",
                                            "status": "in_progress",
                                            "role": "assistant",
                                            "content": []
                                        }
                                    }),
                                ));
                                let seq = self.next_seq();
                                out_events.push(sse_event(
                                    "response.content_part.added",
                                    &json!({
                                        "type": "response.content_part.added",
                                        "sequence_number": seq,
                                        "item_id": mid,
                                        "output_index": idx,
                                        "content_index": 0,
                                        "part": {"type": "output_text", "text": ""}
                                    }),
                                ));
                            }
                            let seq = self.next_seq();
                            out_events.push(sse_event(
                                "response.output_text.delta",
                                &json!({
                                    "type": "response.output_text.delta",
                                    "sequence_number": seq,
                                    "item_id": self.responses_message_id.clone().unwrap_or_default(),
                                    "output_index": self.responses_message_idx.unwrap_or(0),
                                    "content_index": 0,
                                    "delta": text,
                                }),
                            ));
                        }
                    }
                }
            }
        }

        // ---- toolCallPart: 工具调用增量 (用 merge_tool_arg_text 智能合并) ----
        if let Some(part) = tool_call_part(obj) {
            let before_len = self.out.tool_calls.len();
            // 取出该 part 的 call_id, 用于查询已发出的 args 前缀
            let part_id = crate::protocol::parse_tool_call_part(part)
                .map(|(id, _, _, _, _)| id);
            // 上一帧的完整 args (用于计算这次的真实 delta)
            let prev_args = part_id
                .as_ref()
                .and_then(|pid| {
                    self.out
                        .tool_calls
                        .iter()
                        .find(|c| &c.id == pid)
                        .map(|c| c.arguments.clone())
                })
                .unwrap_or_default();
            let is_complete = apply_tool_call_part(&mut self.out, part);
            if is_complete {
                self.completed_tools.insert(part_id.clone().unwrap_or_default());
            }
            if self.out.tool_calls.len() > before_len || !self.out.tool_calls.is_empty() {
                let idx = self.out.tool_calls.len().saturating_sub(1);
                let c = &self.out.tool_calls[idx];
                let call_id = c.id.clone();
                let call_name = c.name.clone();
                // P1: 智能合并 — 用 merge_tool_arg_text 算出真实 delta, 而非粗暴拼接
                let emitted = self
                    .tool_emitted_args
                    .get(&call_id)
                    .cloned()
                    .unwrap_or_default();
                let full_args = c.arguments.clone();
                let args_delta = if full_args.starts_with(&emitted) {
                    full_args[emitted.len()..].to_string()
                } else if emitted.is_empty() {
                    // 首次发完整 name 帧时带空 args
                    if prev_args.is_empty() {
                        String::new()
                    } else {
                        full_args.clone()
                    }
                } else {
                    // 快照不回退, 但已发出的不是前缀 → 发空 (客户端保留旧值)
                    String::new()
                };
                if !args_delta.is_empty() || emitted.is_empty() {
                    self.tool_emitted_args
                        .insert(call_id.clone(), full_args.clone());
                }
                let is_new_tool = !self.started_tools.iter().any(|x| x == &call_id);
                match self.dialect {
                    Dialect::Chat => {
                        self.chat_first = false;
                        // 首次: 发 id + name; 后续: 发完整 id + type + arguments delta
                        // Codex 客户端要求每个 delta 帧都携带 id 和 type 以正确关联 tool_call
                        let delta = if is_new_tool {
                            self.started_tools.push(call_id.clone());
                            openai_tool_delta(idx, &call_id, &call_name, &args_delta)
                        } else {
                            json!({
                                "tool_calls": [{
                                    "index": idx,
                                    "id": call_id,
                                    "type": "function",
                                    "function": {"arguments": args_delta},
                                }]
                            })
                        };
                        out_events.push(openai_chunk(
                            &self.id,
                            &self.public_model,
                            delta,
                            None,
                        ));
                        // 注意: 这里**不**因 isComplete 发 finish_reason。OpenAI 规范每个 choice 只有一次
                        // 非空 finish_reason (在流末尾)。Codex 的 Chat 路径收到首个 finish_reason 即结束本轮,
                        // 之前逐工具发 "tool_calls" 会让并行第二个工具调用被丢弃 (2026-09-03 修复)。
                        // 与 Python 参考 ChatTranslator.feed()/complete() 行为一致。
                        let _ = is_complete;
                    }
                    Dialect::Anthropic => {
                        if is_new_tool {
                            // 关闭已打开的 thinking/text block
                            if self.anthropic_thinking_open {
                                self.anthropic_thinking_open = false;
                                let tidx = self.anthropic_thinking_idx.unwrap_or(0);
                                let sig = self.out.thinking_signature.clone().unwrap_or_else(|| {
                                    format!(
                                        "{}{}",
                                        crate::protocol::PROXY_SIGNATURE_MARK,
                                        Uuid::new_v4().simple()
                                    )
                                });
                                out_events.push(sse_event(
                                    "content_block_delta",
                                    &json!({
                                        "type": "content_block_delta",
                                        "index": tidx,
                                        "delta": {"type": "signature_delta", "signature": sig}
                                    }),
                                ));
                                out_events.push(sse_event(
                                    "content_block_stop",
                                    &json!({"type": "content_block_stop", "index": tidx}),
                                ));
                            }
                            if self.anthropic_text_open {
                                self.anthropic_text_open = false;
                                let tidx = self.anthropic_text_idx.unwrap_or(0);
                                out_events.push(sse_event(
                                    "content_block_stop",
                                    &json!({"type": "content_block_stop", "index": tidx}),
                                ));
                            }
                            let index = self.alloc_anthropic_tool_idx(idx);
                            out_events.push(sse_event(
                                "content_block_start",
                                &json!({
                                    "type": "content_block_start",
                                    "index": index,
                                    "content_block": {
                                        "type": "tool_use",
                                        "id": call_id,
                                        "name": call_name,
                                        "input": {}
                                    }
                                }),
                            ));
                            self.started_tools.push(call_id.clone());
                        }
                        if !args_delta.is_empty() {
                            let index = self.alloc_anthropic_tool_idx(idx);
                            out_events.push(sse_event(
                                "content_block_delta",
                                &json!({
                                    "type": "content_block_delta",
                                    "index": index,
                                    "delta": {"type": "input_json_delta", "partial_json": args_delta}
                                }),
                            ));
                        }
                        // isComplete: Claude Code 需要 content_block_stop 来关闭 tool_use block
                        if is_complete {
                            let index = self.alloc_anthropic_tool_idx(idx);
                            out_events.push(sse_event(
                                "content_block_stop",
                                &json!({"type": "content_block_stop", "index": index}),
                            ));
                        }
                    }
                    Dialect::Responses => {
                        if is_new_tool {
                            // 关闭已打开的 reasoning/message
                            if self.responses_reasoning_open {
                                out_events.extend(self.close_responses_reasoning());
                            }
                            if self.responses_message_open {
                                out_events.extend(self.close_responses_message());
                            }
                            let fc_id = format!("fc_{}", call_id);
                            let output_idx = self.next_output_idx();
                            let seq = self.next_seq();
                            out_events.push(sse_event(
                                "response.output_item.added",
                                &json!({
                                    "type": "response.output_item.added",
                                    "sequence_number": seq,
                                    "output_index": output_idx,
                                    "item": {
                                        "id": fc_id,
                                        "type": "function_call",
                                        "status": "in_progress",
                                        "call_id": call_id,
                                        "name": call_name,
                                        "arguments": ""
                                    }
                                }),
                            ));
                            self.started_tools.push(call_id.clone());
                        }
                        if !args_delta.is_empty() {
                            let seq = self.next_seq();
                            out_events.push(sse_event(
                                "response.function_call_arguments.delta",
                                &json!({
                                    "type": "response.function_call_arguments.delta",
                                    "sequence_number": seq,
                                    "item_id": format!("fc_{}", call_id),
                                    "output_index": self.responses_next_output_idx.saturating_sub(1),
                                    "call_id": call_id,
                                    "delta": args_delta,
                                }),
                            ));
                        }
                        // isComplete: Responses 需要发送 arguments.done + output_item.done
                        if is_complete {
                            let fc_id = format!("fc_{}", call_id);
                            let output_idx = self.responses_next_output_idx.saturating_sub(1);
                            let seq = self.next_seq();
                            out_events.push(sse_event(
                                "response.function_call_arguments.done",
                                &json!({
                                    "type": "response.function_call_arguments.done",
                                    "sequence_number": seq,
                                    "item_id": fc_id,
                                    "output_index": output_idx,
                                    "call_id": call_id,
                                    "arguments": full_args,
                                }),
                            ));
                            let seq = self.next_seq();
                            out_events.push(sse_event(
                                "response.output_item.done",
                                &json!({
                                    "type": "response.output_item.done",
                                    "sequence_number": seq,
                                    "output_index": output_idx,
                                    "item": {
                                        "id": fc_id,
                                        "type": "function_call",
                                        "status": "completed",
                                        "call_id": call_id,
                                        "name": call_name,
                                        "arguments": full_args,
                                    }
                                }),
                            ));
                        }
                    }
                }
            }
        }

        // ---- responseInfo: 上游收尾 (可能内嵌 reasoningParts 兜底) ----
        if obj.get("responseInfo").is_some() || obj.get("invocationId").is_some() {
            // P0: responseInfo.messages[].reasoningParts 兜底回填 (Grok/Gemini 思考末帧)
            if let Some(ri) = obj.get("responseInfo") {
                if let Some(msgs) = ri.get("messages").and_then(|v| v.as_array()) {
                    for msg in msgs {
                        if let Some(parts) = msg.get("reasoningParts").and_then(|v| v.as_array())
                        {
                            for part in parts {
                                let text = part
                                    .get("text")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");
                                if text.is_empty() {
                                    continue;
                                }
                                // 增量回填: 上游可能把完整 thinking 一次性塞到末帧
                                if self.out.thinking.is_empty() {
                                    self.out.thinking = text.to_string();
                                    // 客户端补偿: 一次性发出 thinking 增量
                                    out_events.extend(self.emit_thinking_backfill(text));
                                } else if text.starts_with(&self.out.thinking)
                                    && text.len() > self.out.thinking.len()
                                {
                                    let extra = &text[self.out.thinking.len()..];
                                    self.out.thinking = text.to_string();
                                    out_events.extend(self.emit_thinking_backfill(extra));
                                }
                                // 提取 signature
                                if let Some(sig) =
                                    part.get("signature").and_then(|v| v.as_str())
                                {
                                    self.out.thinking_signature = Some(sig.to_string());
                                }
                            }
                        }
                    }
                }
                // 提取上游 stop reason
                if let Some(sr) = ri.get("stopReason").and_then(|v| v.as_str()) {
                    self.out.upstream_stop_reason = sr.to_string();
                }
            }
            // 收尾
            out_events.extend(self.finalize_events());
        }

        Ok(out_events)
    }

    fn alloc_anthropic_thinking_idx(&mut self) -> usize {
        if let Some(i) = self.anthropic_thinking_idx {
            return i;
        }
        self.anthropic_thinking_idx = Some(0);
        0
    }

    fn alloc_anthropic_text_idx(&mut self) -> usize {
        if let Some(i) = self.anthropic_text_idx {
            return i;
        }
        let base = if self.anthropic_thinking_idx.is_some() {
            1
        } else {
            0
        };
        self.anthropic_text_idx = Some(base);
        base
    }

    fn alloc_anthropic_tool_idx(&self, tool_ordinal: usize) -> usize {
        let mut base = 0;
        if self.anthropic_thinking_idx.is_some() {
            base += 1;
        }
        if self.anthropic_text_idx.is_some() {
            base += 1;
        }
        base + tool_ordinal
    }

    fn next_output_idx(&mut self) -> usize {
        let idx = self.responses_next_output_idx;
        self.responses_next_output_idx += 1;
        idx
    }

    fn close_responses_reasoning(&mut self) -> Vec<String> {
        let mut evts = Vec::new();
        if !self.responses_reasoning_open {
            return evts;
        }
        self.responses_reasoning_open = false;
        let rid = self
            .responses_reasoning_id
            .clone()
            .unwrap_or_else(|| format!("rs_{}", Uuid::new_v4().simple()));
        let idx = self.responses_reasoning_idx.unwrap_or(0);
        let text = self.out.thinking.clone();
        let summary_part = json!({"type": "summary_text", "text": text});
        // 1. reasoning_summary_text.done
        let seq = self.next_seq();
        evts.push(sse_event(
            "response.reasoning_summary_text.done",
            &json!({
                "type": "response.reasoning_summary_text.done",
                "sequence_number": seq,
                "item_id": rid,
                "output_index": idx,
                "summary_index": 0,
                "text": text,
            }),
        ));
        // 2. reasoning_summary_part.done
        let seq = self.next_seq();
        evts.push(sse_event(
            "response.reasoning_summary_part.done",
            &json!({
                "type": "response.reasoning_summary_part.done",
                "sequence_number": seq,
                "item_id": rid,
                "output_index": idx,
                "summary_index": 0,
                "part": summary_part,
            }),
        ));
        // 3. output_item.done (含 encrypted_content)
        let mut item = json!({
            "id": rid,
            "type": "reasoning",
            "status": "completed",
            "summary": [summary_part],
        });
        if self.responses_wants_encrypted {
            item["encrypted_content"] = json!(encode_reasoning(
                &text,
                self.out.thinking_signature.as_deref()
            ));
        }
        let seq = self.next_seq();
        evts.push(sse_event(
            "response.output_item.done",
            &json!({
                "type": "response.output_item.done",
                "sequence_number": seq,
                "output_index": idx,
                "item": item,
            }),
        ));
        evts
    }

    fn close_responses_message(&mut self) -> Vec<String> {
        let mut evts = Vec::new();
        if !self.responses_message_open {
            return evts;
        }
        self.responses_message_open = false;
        let mid = self
            .responses_message_id
            .clone()
            .unwrap_or_else(|| format!("msg_{}", Uuid::new_v4().simple()));
        let idx = self.responses_message_idx.unwrap_or(0);
        let text = self.out.text.clone();
        let part = json!({"type": "output_text", "text": text});
        // 1. output_text.done
        let seq = self.next_seq();
        evts.push(sse_event(
            "response.output_text.done",
            &json!({
                "type": "response.output_text.done",
                "sequence_number": seq,
                "item_id": mid,
                "output_index": idx,
                "content_index": 0,
                "text": text,
            }),
        ));
        // 2. content_part.done
        let seq = self.next_seq();
        evts.push(sse_event(
            "response.content_part.done",
            &json!({
                "type": "response.content_part.done",
                "sequence_number": seq,
                "item_id": mid,
                "output_index": idx,
                "content_index": 0,
                "part": part,
            }),
        ));
        // 3. output_item.done
        let seq = self.next_seq();
        evts.push(sse_event(
            "response.output_item.done",
            &json!({
                "type": "response.output_item.done",
                "sequence_number": seq,
                "output_index": idx,
                "item": {
                    "id": mid,
                    "type": "message",
                    "status": "completed",
                    "role": "assistant",
                    "content": [part],
                }
            }),
        ));
        evts
    }

    /// P0: 末帧 reasoningParts 回填时, 给三种方言补发对应的 thinking 增量.
    fn emit_thinking_backfill(&mut self, text: &str) -> Vec<String> {
        let mut evts = Vec::new();
        if text.is_empty() {
            return evts;
        }
        match self.dialect {
            Dialect::Chat => {
                evts.push(openai_chunk(
                    &self.id,
                    &self.public_model,
                    json!({"reasoning_content": text}),
                    None,
                ));
            }
            Dialect::Anthropic => {
                // 末帧回填: 此时 thinking block 可能已关闭, 无法再插. 直接走 text block.
                // 注意: 这与 Python 版行为一致 — Python 也是直接发到 chunk.
                // 严格做法需要重开 thinking block, 但部分客户端不接受重开.
                // 权衡: 目前只在 thinking_open 时尚可注入.
                if self.anthropic_thinking_open {
                    let idx = self.anthropic_thinking_idx.unwrap_or(0);
                    evts.push(sse_event(
                        "content_block_delta",
                        &json!({
                            "type": "content_block_delta",
                            "index": idx,
                            "delta": {"type": "thinking_delta", "thinking": text}
                        }),
                    ));
                }
            }
            Dialect::Responses => {
                if self.responses_reasoning_open {
                    let seq = self.next_seq();
                    evts.push(sse_event(
                        "response.reasoning_summary_text.delta",
                        &json!({
                            "type": "response.reasoning_summary_text.delta",
                            "sequence_number": seq,
                            "item_id": self.responses_reasoning_id.clone().unwrap_or_default(),
                            "output_index": self.responses_reasoning_idx.unwrap_or(0),
                            "summary_index": 0,
                            "delta": text,
                        }),
                    ));
                }
            }
        }
        evts
    }

    /// 流末尾: 关闭所有未关闭的 block, 发出 finish/done 帧.
    pub fn finalize_events(&mut self) -> Vec<String> {
        let mut evts = Vec::new();
        let u = self.usage.unwrap_or_default();
        let was_truncated = crate::protocol::out_was_truncated(&self.out);

        match self.dialect {
            Dialect::Chat => {
                let finish = if !self.out.tool_calls.is_empty() {
                    "tool_calls"
                } else if was_truncated {
                    "length"
                } else {
                    "stop"
                };
                // P0: OpenAI 规范允许在最后一个 chunk 携带 usage
                let payload = json!({
                    "id": self.id,
                    "object": "chat.completion.chunk",
                    "created": std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    "model": self.public_model,
                    "choices": [{
                        "index": 0,
                        "delta": {},
                        "finish_reason": finish,
                    }],
                    "usage": u.to_openai_json(),
                });
                evts.push(format!(
                    "data: {}\n\n",
                    serde_json::to_string(&payload).unwrap()
                ));
                evts.push("data: [DONE]\n\n".to_string());
            }
            Dialect::Anthropic => {
                // 关闭 thinking (如果还开着)
                if self.anthropic_thinking_open {
                    self.anthropic_thinking_open = false;
                    let idx = self.anthropic_thinking_idx.unwrap_or(0);
                    let sig = self.out.thinking_signature.clone().unwrap_or_else(|| {
                        format!(
                            "{}{}",
                            crate::protocol::PROXY_SIGNATURE_MARK,
                            Uuid::new_v4().simple()
                        )
                    });
                    evts.push(sse_event(
                        "content_block_delta",
                        &json!({
                            "type": "content_block_delta",
                            "index": idx,
                            "delta": {"type": "signature_delta", "signature": sig}
                        }),
                    ));
                    evts.push(sse_event(
                        "content_block_stop",
                        &json!({"type": "content_block_stop", "index": idx}),
                    ));
                }
                if self.anthropic_text_open {
                    self.anthropic_text_open = false;
                    let idx = self.anthropic_text_idx.unwrap_or(0);
                    evts.push(sse_event(
                        "content_block_stop",
                        &json!({"type": "content_block_stop", "index": idx}),
                    ));
                }
                // 关闭尚未关闭的 tool_use block (isComplete 时已发过 content_block_stop 的跳过,
                // 否则同一 index 会收到两次 stop —— Anthropic 规范每个 block 只能 stop 一次)
                for (i, c) in self.out.tool_calls.iter().enumerate() {
                    if self.completed_tools.contains(&c.id) {
                        continue;
                    }
                    let index = self.alloc_anthropic_tool_idx(i);
                    evts.push(sse_event(
                        "content_block_stop",
                        &json!({"type": "content_block_stop", "index": index}),
                    ));
                }
                let stop = if !self.out.tool_calls.is_empty() {
                    "tool_use"
                } else if was_truncated {
                    "max_tokens"
                } else {
                    "end_turn"
                };
                evts.push(sse_event(
                    "message_delta",
                    &json!({
                        "type": "message_delta",
                        "delta": {"stop_reason": stop, "stop_sequence": Value::Null},
                        "usage": {"output_tokens": u.output}
                    }),
                ));
                evts.push(sse_event(
                    "message_stop",
                    &json!({"type": "message_stop"}),
                ));
            }
            Dialect::Responses => {
                if self.responses_reasoning_open {
                    evts.extend(self.close_responses_reasoning());
                }
                if self.responses_message_open {
                    evts.extend(self.close_responses_message());
                }
                // 关闭未完成的 function_call items (发 arguments.done + output_item.done)
                // 借用规则: 先把 (id, name, args) 拷出来, 避免在调用 next_seq 时与 iter() 冲突
                let tool_calls_snapshot: Vec<(usize, String, String, String)> = self
                    .out
                    .tool_calls
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| !self.completed_tools.contains(&c.id))
                    .map(|(i, c)| (i, c.id.clone(), c.name.clone(), c.arguments.clone()))
                    .collect();
                let tools_len = tool_calls_snapshot.len();
                let base_idx = self.responses_next_output_idx.saturating_sub(tools_len);
                for (i, cid, cname, cargs) in tool_calls_snapshot {
                    let fc_id = format!("fc_{}", cid);
                    let output_idx = base_idx + i;
                    let seq = self.next_seq();
                    evts.push(sse_event(
                        "response.function_call_arguments.done",
                        &json!({
                            "type": "response.function_call_arguments.done",
                            "sequence_number": seq,
                            "item_id": fc_id,
                            "output_index": output_idx,
                            "call_id": cid,
                            "arguments": cargs,
                        }),
                    ));
                    let seq = self.next_seq();
                    evts.push(sse_event(
                        "response.output_item.done",
                        &json!({
                            "type": "response.output_item.done",
                            "sequence_number": seq,
                            "output_index": output_idx,
                            "item": {
                                "id": fc_id,
                                "type": "function_call",
                                "status": "completed",
                                "call_id": cid,
                                "name": cname,
                                "arguments": cargs,
                            }
                        }),
                    ));
                }
                // response.completed
                // 提前构造请求体避免借用临时值
                let include_req = if self.responses_wants_encrypted {
                    Some(json!({"include": ["reasoning.encrypted_content"]}))
                } else {
                    None
                };
                let mut final_obj = responses_message_with_request(
                    &self.id,
                    &self.public_model,
                    &self.out,
                    &u,
                    include_req.as_ref(),
                );
                // response.completed.output[] 的 item id 必须与流中 output_item.added/done 一致
                // (Codex 按 id 关联 reasoning / message item)。
                if let Some(items) = final_obj.get_mut("output").and_then(|v| v.as_array_mut()) {
                    for it in items.iter_mut() {
                        match it.get("type").and_then(|v| v.as_str()) {
                            Some("reasoning") => {
                                if let Some(rid) = &self.responses_reasoning_id {
                                    it["id"] = json!(rid);
                                }
                            }
                            Some("message") => {
                                if let Some(mid) = &self.responses_message_id {
                                    it["id"] = json!(mid);
                                }
                            }
                            _ => {}
                        }
                    }
                }
                let seq = self.next_seq();
                evts.push(sse_event(
                    "response.completed",
                    &json!({
                        "type": "response.completed",
                        "sequence_number": seq,
                        "response": final_obj,
                    }),
                ));
            }
        }
        evts
    }
}

/// 流式翻译 — 把上游帧流转成方言 SSE 帧流.
///
/// 每个 item 是 (sse_chunk, accumulated_usage) —— 调用方拿 sse_chunk 直接写给客户端.
pub fn upstream_to_dialect_stream<E>(
    frames: Pin<Box<dyn Stream<Item = Result<Value, E>> + Send>>,
    model: &str,
    dialect: Dialect,
    request_body: Option<Value>,
) -> impl Stream<Item = Result<(String, Option<Usage>), String>>
where
    E: std::fmt::Display + Send + 'static,
{
    let translator = StreamTranslator::new(dialect, model, request_body.as_ref());
    let model_owned = model.to_string();

    futures_util::stream::unfold(
        (frames, translator, model_owned, false),
        |(mut frames, mut tr, _model, mut finished)| async move {
            if finished {
                return None;
            }
            loop {
                match frames.next().await {
                    Some(Ok(obj)) => {
                        let is_terminal = obj.get("responseInfo").is_some()
                            || obj.get("invocationId").is_some();
                        match tr.feed(&obj) {
                            Ok(events) => {
                                let sse = events.concat();
                                if is_terminal {
                                    finished = true;
                                }
                                if sse.is_empty() && !is_terminal {
                                    continue;
                                }
                                return Some((Ok((sse, tr.usage)), (frames, tr, _model, finished)));
                            }
                            Err(e) => {
                                return Some((Err(e), (frames, tr, _model, true)));
                            }
                        }
                    }
                    Some(Err(e)) => {
                        return Some((Err(e.to_string()), (frames, tr, _model, true)));
                    }
                    None => {
                        // 上游意外结束 (没看到 responseInfo): 也调用 finalize 保证客户端收到收尾
                        if !finished {
                            let events = tr.finalize_events();
                            let sse = events.concat();
                            return Some((Ok((sse, tr.usage)), (frames, tr, _model, true)));
                        }
                        return None;
                    }
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
    upstream_to_dialect_stream(frames, model, Dialect::Chat, None)
}

pub async fn upstream_to_dialect_full<E>(
    mut frames: Pin<Box<dyn Stream<Item = Result<Value, E>> + Send>>,
    model: &str,
    dialect: Dialect,
    request_body: Option<Value>,
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
        if let Some(think) = obj.get("thinkingPart").and_then(|v| v.as_object()) {
            if let Some(t) = think.get("text").and_then(|v| v.as_str()) {
                out.thinking.push_str(t);
            }
            if let Some(sig) = think.get("signature").and_then(|v| v.as_str()) {
                out.thinking_signature = Some(sig.to_string());
            }
        }
        if let Some(part) = tool_call_part(&obj) {
            apply_tool_call_part(&mut out, part);
        }
        if let Some(u) = extract_usage(&obj) {
            usage = u;
        }
        if obj.get("responseInfo").is_some() || obj.get("invocationId").is_some() {
            if let Some(ri) = obj.get("responseInfo") {
                if let Some(sr) = ri.get("stopReason").and_then(|v| v.as_str()) {
                    out.upstream_stop_reason = sr.to_string();
                }
                // 兜底: reasoningParts
                if let Some(msgs) = ri.get("messages").and_then(|v| v.as_array()) {
                    for msg in msgs {
                        if let Some(parts) = msg.get("reasoningParts").and_then(|v| v.as_array())
                        {
                            for part in parts {
                                if let Some(t) = part.get("text").and_then(|v| v.as_str()) {
                                    if out.thinking.is_empty() {
                                        out.thinking = t.to_string();
                                    } else if t.starts_with(&out.thinking)
                                        && t.len() > out.thinking.len()
                                    {
                                        out.thinking = t.to_string();
                                    }
                                }
                                if let Some(sig) =
                                    part.get("signature").and_then(|v| v.as_str())
                                {
                                    out.thinking_signature = Some(sig.to_string());
                                }
                            }
                        }
                    }
                }
            }
            break;
        }
    }

    // 出站时模型名做反向映射 (Anthropic)
    let display_model;
    let model_str = if dialect == Dialect::Anthropic {
        display_model = display_model_id(model);
        display_model.as_str()
    } else {
        model
    };

    let value = match dialect {
        Dialect::Chat => openai_message_with_tools(&id, model_str, &out, &usage),
        Dialect::Anthropic => anthropic_message(&id, model_str, &out, &usage),
        Dialect::Responses => {
            responses_message_with_request(&id, model_str, &out, &usage, request_body.as_ref())
        }
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
    upstream_to_dialect_full(frames, model, Dialect::Chat, None).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;

    #[test]
    fn extract_cursor_error_prefers_title_over_generic_error() {
        let frame = json!({
            "error": {
                "code": "resource_exhausted",
                "message": "Error",
                "details": [{
                    "debug": {
                        "details": {
                            "title": "Max mode is only available to paid users",
                            "detail": "Upgrade to a paid plan or set a Spend Limit"
                        },
                        "error": "ERROR_RATE_LIMITED_CHANGEABLE"
                    }
                }]
            }
        });
        let msg = extract_cursor_error_message(&frame).unwrap();
        assert!(msg.contains("Max mode is only available to paid users"));
        assert!(is_max_mode_restricted(&msg));
        assert!(!is_upstream_capacity_error(&msg));
    }

    #[test]
    fn capacity_error_detects_kimi_high_load_not_max_mode() {
        let load = "upstream rejected: High Load: We're experiencing high demand for Kimi K3 right now. Please upgrade to Pro, switch to Auto, another model, or try again in a few moments.";
        assert!(is_upstream_capacity_error(load));
        assert!(!is_max_mode_restricted(load));
        assert!(is_upstream_capacity_error(
            "Provider Error: We're having trouble connecting to the model provider"
        ));
        assert!(is_upstream_capacity_error("upstream HTTP 502 Bad Gateway"));
        assert!(!is_upstream_capacity_error(
            "Max mode is only available to paid users"
        ));
    }

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
        let (v, _) = upstream_to_dialect_full(Box::pin(frames), "kimi-k3", Dialect::Anthropic, None)
            .await
            .unwrap();
        assert_eq!(v["stop_reason"], "tool_use");
        assert_eq!(v["content"][0]["type"], "tool_use");
    }

    #[tokio::test]
    async fn full_collects_thinking_and_reasoning_parts() {
        // 1. thinkingPart 流式增量
        let frames = stream::iter(vec![
            Ok::<Value, String>(json!({"thinkingPart": {"text": "先分析问题..."}})),
            Ok(json!({"thinkingPart": {"text": "然后给出答案", "signature": "sig-abc"}})),
            Ok(json!({"textPart": {"text": "最终答案"}})),
            Ok(json!({"responseInfo": {"stopReason": "STOP"}})),
        ]);
        let (v, _) = upstream_to_openai_full(Box::pin(frames), "kimi-k3-thinking")
            .await
            .unwrap();
        let msg = &v["choices"][0]["message"];
        assert_eq!(msg["reasoning_content"], "先分析问题...然后给出答案");
        assert_eq!(msg["reasoning_signature"], "sig-abc");
        assert_eq!(msg["content"], "最终答案");
    }

    #[tokio::test]
    async fn full_anthropic_emits_thinking_block() {
        let frames = stream::iter(vec![
            Ok::<Value, String>(json!({"thinkingPart": {"text": "深思..."}})),
            Ok(json!({"textPart": {"text": "回答"}})),
            Ok(json!({"responseInfo": {}})),
        ]);
        let (v, _) =
            upstream_to_dialect_full(Box::pin(frames), "claude-fable-5", Dialect::Anthropic, None)
                .await
                .unwrap();
        // P0: thinking block 必须在 text 之前, 且有 signature
        assert_eq!(v["content"][0]["type"], "thinking");
        assert_eq!(v["content"][0]["thinking"], "深思...");
        assert!(v["content"][0]["signature"].as_str().unwrap().len() > 0);
        assert_eq!(v["content"][1]["type"], "text");
        assert_eq!(v["content"][1]["text"], "回答");
        // P1: display_model_id 反向映射
        assert_eq!(v["model"], "claude-sonnet-4-6");
    }

    #[tokio::test]
    async fn full_responses_emits_reasoning_item_with_encrypted() {
        let frames = stream::iter(vec![
            Ok::<Value, String>(json!({"thinkingPart": {"text": "考虑..."}})),
            Ok(json!({"textPart": {"text": "答案"}})),
            Ok(json!({"responseInfo": {}})),
        ]);
        let req = json!({"include": ["reasoning.encrypted_content"]});
        let (v, _) =
            upstream_to_dialect_full(Box::pin(frames), "gpt-5", Dialect::Responses, Some(req))
                .await
                .unwrap();
        // P0: reasoning item 在 message 之前
        assert_eq!(v["output"][0]["type"], "reasoning");
        // P0: encrypted_content 必须以 cursor-sand-v1: 前缀
        let enc = v["output"][0]["encrypted_content"].as_str().unwrap();
        assert!(enc.starts_with("cursor-sand-v1:"));
        assert_eq!(v["output"][1]["type"], "message");
    }

    #[tokio::test]
    async fn merge_tool_arg_text_handles_json_boundary() {
        // P1: 跨帧 JSON 边界 — 完整 JSON 对象 + 增量 suffix
        let (full, _) = merge_tool_arg_text(r#"{"command":"ls"}"#, "");
        assert_eq!(full, r#"{"command":"ls"}"#);
        // 前缀增长: 直接拼接
        let (full, delta) = merge_tool_arg_text(r#"{"com"#, r#""command":"ls"}"#);
        // incoming 不以 prev 开头, prev 也不以 incoming 开头
        // 都不是完整 JSON → 字符串拼接
        assert!(full.contains("com"));
        assert_eq!(delta, r#""command":"ls"}"#);
        // 新快照覆盖旧快照
        let (full, _) = merge_tool_arg_text(r#"{"a":1}"#, r#"{"a":1,"b":2}"#);
        assert_eq!(full, r#"{"a":1,"b":2}"#);
    }

    // ============ P2 测试 ============

    #[test]
    fn tool_choice_omits_tools_works() {
        use crate::protocol::tool_choice_omits_tools;
        assert!(tool_choice_omits_tools(Some(&json!("none"))));
        assert!(tool_choice_omits_tools(Some(&json!({"type": "none"}))));
        assert!(tool_choice_omits_tools(Some(&json!(false))));
        assert!(!tool_choice_omits_tools(Some(&json!("auto"))));
        assert!(!tool_choice_omits_tools(Some(&json!("required"))));
        assert!(!tool_choice_omits_tools(None));
    }

    #[test]
    fn tool_choice_forced_name_test() {
        use crate::protocol::forced_tool_name;
        let v = json!({"type": "function", "function": {"name": "bash"}});
        assert_eq!(forced_tool_name(Some(&v)).as_deref(), Some("bash"));
        let v = json!({"type": "function", "name": "sh"});
        assert_eq!(forced_tool_name(Some(&v)).as_deref(), Some("sh"));
        assert!(forced_tool_name(Some(&json!("auto"))).is_none());
    }

    #[test]
    fn tool_choice_hint_message() {
        use crate::protocol::tool_choice_hint;
        let h = tool_choice_hint(Some(&json!({"type": "function", "function": {"name": "bash"}})))
            .unwrap();
        assert!(h.contains("bash"), "hint should mention tool name: {}", h);
        let h = tool_choice_hint(Some(&json!("required"))).unwrap();
        assert!(h.contains("must call"), "required hint: {}", h);
        assert!(tool_choice_hint(Some(&json!("none"))).is_none());
        assert!(tool_choice_hint(Some(&json!("auto"))).is_none());
    }

    #[test]
    fn response_format_hint_constructs_prompt() {
        use crate::protocol::response_format_hint;
        let body = json!({
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "User",
                    "strict": true,
                    "schema": {"type": "object", "properties": {"name": {"type": "string"}}}
                }
            }
        });
        let hint = response_format_hint(&body).unwrap();
        assert!(hint.contains("User"));
        assert!(hint.contains("strictly"));
        assert!(hint.contains("\"name\""));

        // json_object 简单形式
        let body = json!({"response_format": {"type": "json_object"}});
        let hint = response_format_hint(&body).unwrap();
        assert!(hint.contains("JSON"));
        // 无 response_format → None
        assert!(response_format_hint(&json!({})).is_none());
    }

    #[test]
    fn conversation_id_from_uses_explicit_then_user_then_prefix() {
        use crate::protocol::conversation_id_from;
        // 1. 显式 conversationId 优先
        let b = json!({"conversationId": "conv-123"});
        assert_eq!(conversation_id_from(&b, None), "conv-123");
        // 2. metadata.session_id
        let b = json!({"metadata": {"session_id": "sess-abc"}});
        assert_eq!(conversation_id_from(&b, None), "sess-abc");
        // 3. user → user_<sha1[:16]>
        let b = json!({"user": "alice"});
        let id = conversation_id_from(&b, None);
        assert!(id.starts_with("user_"), "got: {}", id);
        assert_eq!(id.len(), "user_".len() + 16);
        // 4. system + user → sys_.../conv_...
        let b = json!({
            "messages": [
                {"role": "system", "content": "You are a helpful coding assistant. You write code in Rust. You explain trade-offs."},
                {"role": "user", "content": "hello"}
            ]
        });
        let id = conversation_id_from(&b, None);
        // system 长度 >= 80 → sys_ 前缀
        assert!(id.starts_with("sys_") || id.starts_with("conv_"), "got: {}", id);
        // 5. 全空 → UUID v4
        let b = json!({});
        let id = conversation_id_from(&b, None);
        assert_eq!(id.len(), 36); // UUID 标准长度
    }

    #[test]
    fn conversation_id_sha1_matches_python() {
        use crate::protocol::conversation_id_from;
        // Python: hashlib.sha1(b'alice').hexdigest()[:16] == "522b276a356bdf39"
        let b = json!({"user": "alice"});
        let id = conversation_id_from(&b, None);
        assert_eq!(id, "user_522b276a356bdf39");
    }

    #[test]
    fn encode_decode_reasoning_roundtrip() {
        use crate::protocol::{decode_reasoning, encode_reasoning, REASONING_PREFIX};
        let enc = encode_reasoning("思考过程", Some("sig-xyz"));
        assert!(enc.starts_with(REASONING_PREFIX));
        let (text, sig) = decode_reasoning(&enc).unwrap();
        assert_eq!(text, "思考过程");
        assert_eq!(sig.as_deref(), Some("sig-xyz"));
        // 无签名 → 自动生成 proxygen: 签名
        let enc = encode_reasoning("想", None);
        let (_, sig) = decode_reasoning(&enc).unwrap();
        assert!(sig.unwrap().starts_with("proxygen:"));
    }

    #[test]
    fn display_model_id_maps_claude_fable() {
        use crate::protocol::display_model_id;
        assert_eq!(display_model_id("claude-fable-5"), "claude-sonnet-4-6");
        assert_eq!(display_model_id("grok-4.5"), "claude-sonnet-4-6");
        assert_eq!(display_model_id("cursor-grok-4.5"), "claude-sonnet-4-6");
        assert_eq!(display_model_id("kimi-k3"), "kimi-k3");
    }
}

#[cfg(test)]
mod format_conformance_probe {
    //! 输出格式标准化探针: 三方言对照 OpenAI Chat / Anthropic Messages / OpenAI Responses 规范.
    use super::*;
    use futures_util::stream;

    fn frames() -> Vec<Result<Value, String>> {
        vec![
            Ok(json!({"thinkingPart": {"text": "think..."}})),
            Ok(json!({"textPart": {"text": "Let me run it."}})),
            Ok(json!({"toolCallPart": {"toolCallId": "Bash_0-aaaaa", "toolName": "Bash", "args": {"command": "echo 1"}, "isComplete": true}})),
            Ok(json!({"toolCallPart": {"toolCallId": "Read_1-aaaaa", "toolName": "Read", "args": {"path": "/tmp/x"}, "isComplete": true}})),
            Ok(json!({"extendedUsage": {"promptTokens": 10, "completionTokens": 5}, "responseInfo": {}})),
        ]
    }

    async fn run(dialect: Dialect) -> Vec<(Option<String>, Value)> {
        use futures_util::StreamExt;
        let s = upstream_to_dialect_stream(Box::pin(stream::iter(frames())), "kimi-k3-max", dialect, None);
        let raw: Vec<String> = s.map(|r| r.unwrap().0).collect().await;
        let mut out = Vec::new();
        for chunk in raw {
            let mut ev: Option<String> = None;
            for line in chunk.lines() {
                if let Some(e) = line.strip_prefix("event: ") { ev = Some(e.trim().to_string()); }
                else if let Some(d) = line.strip_prefix("data: ") {
                    let d = d.trim();
                    let v = if d == "[DONE]" { json!("[DONE]") } else { serde_json::from_str(d).unwrap_or(json!(d)) };
                    out.push((ev.clone(), v));
                }
            }
        }
        out
    }

    #[tokio::test]
    async fn probe_all_dialects_print_shapes() {
        for d in [Dialect::Chat, Dialect::Anthropic, Dialect::Responses] {
            println!("\n===== {:?} =====", d);
            for (ev, v) in run(d).await {
                let s = serde_json::to_string(&v).unwrap();
                println!("{:<45} {}", ev.unwrap_or_default(), &s[..s.len().min(260)]);
            }
        }
    }

    /// OpenAI Chat 规范: 每个 choice 的非空 finish_reason 只出现一次 (流末尾), 且在 [DONE] 之前;
    /// 每个 tool_calls delta 都带 id + type; 两个并行工具 index 0/1 都完整到达.
    #[tokio::test]
    async fn chat_stream_has_single_finish_reason_and_both_tools() {
        let evs = run(Dialect::Chat).await;
        let finishes: Vec<&Value> = evs
            .iter()
            .filter_map(|(_, v)| v.get("choices").and_then(|c| c[0].get("finish_reason")))
            .filter(|f| !f.is_null())
            .collect();
        assert_eq!(finishes.len(), 1, "finish_reason 必须且只能出现一次: {finishes:?}");
        assert_eq!(finishes[0], "tool_calls");
        let last_two: Vec<&Value> = evs.iter().rev().take(2).map(|(_, v)| v).collect();
        assert_eq!(last_two[0], &json!("[DONE]"));
        assert!(last_two[1].get("usage").is_some(), "finish 帧携带 usage");
        let mut tool_idx = std::collections::BTreeSet::new();
        for (_, v) in &evs {
            if let Some(tcs) = v.pointer("/choices/0/delta/tool_calls").and_then(|t| t.as_array()) {
                for tc in tcs {
                    assert!(tc.get("id").is_some() && tc["type"] == "function", "delta 需带 id+type: {tc}");
                    tool_idx.insert(tc["index"].as_u64().unwrap());
                }
            }
        }
        assert_eq!(tool_idx.into_iter().collect::<Vec<_>>(), vec![0, 1]);
    }

    /// Anthropic 规范: 每个 content block 的 start/stop 各一次, 顺序 thinking → text → tool_use,
    /// message_delta.stop_reason = tool_use, 最后 message_stop.
    #[tokio::test]
    async fn anthropic_stream_blocks_start_and_stop_exactly_once() {
        let evs = run(Dialect::Anthropic).await;
        let mut starts = std::collections::HashMap::new();
        let mut stops = std::collections::HashMap::new();
        for (ev, v) in &evs {
            match ev.as_deref() {
                Some("content_block_start") => *starts.entry(v["index"].as_u64().unwrap()).or_insert(0) += 1,
                Some("content_block_stop") => *stops.entry(v["index"].as_u64().unwrap()).or_insert(0) += 1,
                _ => {}
            }
        }
        assert_eq!(starts.len(), 4, "thinking+text+2 tools");
        for (idx, n) in &starts {
            assert_eq!(*n, 1, "index {idx} start 次数");
            assert_eq!(stops.get(idx), Some(&1), "index {idx} stop 必须恰好一次, 实际 {:?}", stops.get(idx));
        }
        let names: Vec<&str> = evs.iter().filter_map(|(e, _)| e.as_deref()).collect();
        assert_eq!(names[0], "message_start");
        assert_eq!(names[names.len() - 2], "message_delta");
        assert_eq!(names[names.len() - 1], "message_stop");
        let md = &evs[evs.len() - 2].1;
        assert_eq!(md["delta"]["stop_reason"], "tool_use");
        // tool_use block 形态
        let tu: Vec<&Value> = evs.iter().filter(|(e, v)| e.as_deref() == Some("content_block_start") && v["content_block"]["type"] == "tool_use").map(|(_, v)| v).collect();
        assert_eq!(tu.len(), 2);
        assert_eq!(tu[0]["content_block"]["name"], "Bash");
        assert_eq!(tu[0]["content_block"]["input"], json!({}));
    }

    /// Responses 规范: sequence_number 严格递增; 每个 output item added/done 配对且 id 一致;
    /// response.completed.output 的 item id 与流中一致; function_call 有 arguments.done.
    #[tokio::test]
    async fn responses_stream_items_are_consistent() {
        let evs = run(Dialect::Responses).await;
        let mut last_seq = 0u64;
        let mut added = std::collections::HashMap::new();
        let mut done = std::collections::HashMap::new();
        for (ev, v) in &evs {
            let seq = v["sequence_number"].as_u64().expect("每个事件都有 sequence_number");
            assert!(seq > last_seq, "sequence_number 必须递增");
            last_seq = seq;
            match ev.as_deref() {
                Some("response.output_item.added") => { added.insert(v["item"]["id"].as_str().unwrap().to_string(), v["output_index"].as_u64().unwrap()); }
                Some("response.output_item.done") => { done.insert(v["item"]["id"].as_str().unwrap().to_string(), v["output_index"].as_u64().unwrap()); }
                _ => {}
            }
        }
        assert_eq!(added.len(), 4, "reasoning + message + 2 function_call");
        assert_eq!(added, done, "added/done 的 id 与 output_index 必须一一配对");
        let (ev, completed) = evs.last().unwrap();
        assert_eq!(ev.as_deref(), Some("response.completed"));
        assert_eq!(completed["response"]["status"], "completed");
        let final_ids: std::collections::HashSet<String> = completed["response"]["output"].as_array().unwrap().iter().map(|i| i["id"].as_str().unwrap().to_string()).collect();
        let streamed_ids: std::collections::HashSet<String> = added.keys().cloned().collect();
        assert_eq!(final_ids, streamed_ids, "response.completed.output ids 必须与流中 item id 一致");
        let fc_done = evs.iter().filter(|(e, _)| e.as_deref() == Some("response.function_call_arguments.done")).count();
        assert_eq!(fc_done, 2, "每个 function_call 恰好一个 arguments.done");
        let fc_items: Vec<&Value> = completed["response"]["output"].as_array().unwrap().iter().filter(|i| i["type"] == "function_call").collect();
        assert_eq!(fc_items[0]["call_id"], "Bash_0-aaaaa");
        assert_eq!(fc_items[0]["arguments"], "{\"command\":\"echo 1\"}");
    }
}
