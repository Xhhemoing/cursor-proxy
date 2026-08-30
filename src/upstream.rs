//! 上游抽象: Cursor Connect 协议 + OpenAI 兼容 API.

use serde_json::Value;
use std::pin::Pin;
use futures_util::stream::{Stream, StreamExt};

use crate::cursor::{CursorClient, CursorError, build_cursor_body_with_tools};

/// 上游类型
#[derive(Debug, Clone, PartialEq)]
pub enum UpstreamKind {
    Cursor,
    OpenAiCompatible,
}

/// 上游配置
#[derive(Debug, Clone)]
pub struct UpstreamConfig {
    pub kind: UpstreamKind,
    pub name: String,
    pub base_url: String,
    pub api_key: String,
    pub timeout_s: u64,
}

/// 统一上游客户端
#[derive(Clone)]
pub enum UpstreamClient {
    Cursor(CursorClient),
    OpenAi(OpenAiClient),
}

impl UpstreamClient {
    pub fn new_cursor(backend: &str, timeout_s: u64) -> anyhow::Result<Self> {
        Ok(Self::Cursor(CursorClient::new(backend, timeout_s)?))
    }

    pub fn new_openai(base_url: &str, api_key: &str, timeout_s: u64) -> anyhow::Result<Self> {
        Ok(Self::OpenAi(OpenAiClient::new(base_url, api_key, timeout_s)?))
    }

    /// 流式调用
    pub async fn stream(
        &self,
        model: &str,
        messages: &[Value],
        max_tokens: Option<u32>,
        temperature: Option<f64>,
        cursor_auth: Option<(&str, &str)>,  // (access_token, machine_id) 仅 Cursor 用
        tools: Option<&Value>,
        tool_choice: Option<&Value>,
        parallel_tool_calls: Option<bool>,
        conversation_id: Option<&str>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Value, UpstreamError>> + Send>>, UpstreamError> {
        match self {
            Self::Cursor(client) => {
                let (token, mid) = cursor_auth.ok_or_else(|| UpstreamError::Config("cursor auth required".into()))?;
                let body = build_cursor_body_with_tools(
                    messages,
                    model,
                    max_tokens,
                    temperature,
                    tools,
                    tool_choice,
                    parallel_tool_calls,
                    conversation_id,
                );
                let stream = client.stream(token, mid, &body).await
                    .map_err(|e| UpstreamError::Cursor(e))?;
                Ok(Box::pin(stream.map(|r| r.map_err(UpstreamError::Cursor))))
            }
            Self::OpenAi(client) => {
                let stream = client
                    .stream(model, messages, max_tokens, temperature, tools, tool_choice)
                    .await
                    .map_err(|e| UpstreamError::OpenAi(e))?;
                Ok(Box::pin(stream.map(|r| r.map_err(UpstreamError::OpenAi))))
            }
        }
    }
}

/// OpenAI 兼容 API 客户端
#[derive(Clone)]
pub struct OpenAiClient {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    timeout_s: u64,
}

impl OpenAiClient {
    pub fn new(base_url: &str, api_key: &str, timeout_s: u64) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_s))
            .build()?;
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
            timeout_s,
        })
    }

    pub async fn stream(
        &self,
        model: &str,
        messages: &[Value],
        max_tokens: Option<u32>,
        temperature: Option<f64>,
        tools: Option<&Value>,
        tool_choice: Option<&Value>,
    ) -> Result<impl Stream<Item = Result<Value, OpenAiError>>, OpenAiError> {
        let url = format!("{}/chat/completions", self.base_url);
        let mut body = serde_json::json!({
            "model": model,
            "messages": messages,
            "stream": true,
        });
        if let Some(mt) = max_tokens {
            body["max_tokens"] = serde_json::json!(mt);
        }
        if let Some(t) = temperature {
            body["temperature"] = serde_json::json!(t);
        }
        if let Some(tools) = tools {
            body["tools"] = tools.clone();
        }
        if let Some(tc) = tool_choice {
            body["tool_choice"] = tc.clone();
        }

        let resp = self.client
            .post(&url)
            .header("authorization", format!("Bearer {}", self.api_key))
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| OpenAiError::Network(e.to_string()))?;

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let text = resp.text().await.unwrap_or_default();
            return Err(OpenAiError::Http(status, text));
        }

        let stream = resp.bytes_stream();
        Ok(SseStream::new(stream))
    }
}

/// SSE 流解码器 (OpenAI 格式: data: {...}\n\n)
struct SseStream<S> {
    inner: S,
    buf: String,
}

impl<S, E> SseStream<S>
where
    S: Stream<Item = Result<bytes::Bytes, E>> + Unpin,
    E: std::error::Error,
{
    fn new(inner: S) -> Self {
        Self {
            inner,
            buf: String::new(),
        }
    }
}

impl<S, E> Stream for SseStream<S>
where
    S: Stream<Item = Result<bytes::Bytes, E>> + Unpin,
    E: std::error::Error,
{
    type Item = Result<Value, OpenAiError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            // 尝试从 buf 解析一行
            if let Some(pos) = self.buf.find("\n\n") {
                let line = self.buf[..pos].to_string();
                self.buf = self.buf[pos + 2..].to_string();
                if line.starts_with("data: ") {
                    let data = &line[6..];
                    if data == "[DONE]" {
                        return std::task::Poll::Ready(None);
                    }
                    match serde_json::from_str(data) {
                        Ok(v) => return std::task::Poll::Ready(Some(Ok(v))),
                        Err(e) => return std::task::Poll::Ready(Some(Err(OpenAiError::Decode(e.to_string())))),
                    }
                }
                continue;
            }
            // 读更多
            match Pin::new(&mut self.inner).poll_next(cx) {
                std::task::Poll::Ready(Some(Ok(bytes))) => {
                    self.buf.push_str(&String::from_utf8_lossy(&bytes));
                    continue;
                }
                std::task::Poll::Ready(Some(Err(e))) => {
                    return std::task::Poll::Ready(Some(Err(OpenAiError::Network(e.to_string()))));
                }
                std::task::Poll::Ready(None) => {
                    return std::task::Poll::Ready(None);
                }
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
    }
}

/// 统一上游错误
#[derive(thiserror::Error, Debug)]
pub enum UpstreamError {
    #[error("cursor: {0}")]
    Cursor(#[from] CursorError),
    #[error("openai: {0}")]
    OpenAi(#[from] OpenAiError),
    #[error("config: {0}")]
    Config(String),
}

#[derive(thiserror::Error, Debug)]
pub enum OpenAiError {
    #[error("HTTP {0}: {1}")]
    Http(u16, String),
    #[error("network: {0}")]
    Network(String),
    #[error("decode: {0}")]
    Decode(String),
}
