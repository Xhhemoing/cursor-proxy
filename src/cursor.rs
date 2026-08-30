//! Cursor Connect 协议异步客户端: 帧编解码 + TLS + 流式读取.

use bytes::BytesMut;
use futures_util::stream::Stream;
use hyper::body::{Body as HttpBody, Incoming};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde_json::{json, Value};
use std::pin::Pin;
use std::task::{Context, Poll};
use uuid::Uuid;

pub const STREAM_PATH: &str = "/aiserver.v1.InferenceService/Stream";
pub const CLIENT_TYPE: &str = "sand";
pub const CLIENT_VERSION: &str = "0.18.0";
pub const KIMI_K3_CONTEXT_WINDOW: u32 = 1_048_576;
/// 客户端未传 max_tokens 时, kimi 家族给长输出预算, 避免上游默认 8k 截断.
pub const KIMI_DEFAULT_MAX_TOKENS: u32 = 32_768;

pub fn is_kimi_family(model: &str) -> bool {
    let m = model.to_ascii_lowercase();
    m == "kimi-k3" || m.starts_with("kimi-k3-") || m.starts_with("kimi-k2")
}

/// 客户端省略 max_tokens 时的默认输出预算; 非 kimi 不注入, 避免 Gemini 类模型踩 8k 坑.
pub fn default_max_tokens_for(model: &str) -> Option<u32> {
    if is_kimi_family(model) {
        Some(KIMI_DEFAULT_MAX_TOKENS)
    } else {
        None
    }
}

pub fn context_window_for(model: &str) -> u32 {
    if is_kimi_family(model) {
        KIMI_K3_CONTEXT_WINDOW
    } else {
        200_000
    }
}
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
/// 解码缓冲上限: 允许一帧 + 余量.
const MAX_STREAM_BUF: usize = MAX_FRAME_BYTES + 64 * 1024;

#[derive(thiserror::Error, Debug)]
pub enum CursorError {
    #[error("Cursor Stream HTTP {0}: {1}")]
    Http(u16, String),
    #[error("Cursor Stream network: {0}")]
    Network(String),
    #[error("Cursor Stream decode: {0}")]
    Decode(String),
}

/// Connect 帧: [flags:1][len:4 big-endian][payload]
pub fn connect_envelope(payload: &[u8], flags: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(flags);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// x-cursor-checksum: obfuscated timestamp + machine_id
pub fn checksum(machine_id: &str) -> String {
    use base64::Engine;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let kilo = now_ms / 1_000_000;
    let mut raw = [
        ((kilo >> 40) & 255) as u8,
        ((kilo >> 32) & 255) as u8,
        ((kilo >> 24) & 255) as u8,
        ((kilo >> 16) & 255) as u8,
        ((kilo >> 8) & 255) as u8,
        (kilo & 255) as u8,
    ];
    let mut last = 165u8;
    for (i, cur) in raw.iter_mut().enumerate() {
        let val = ((*cur ^ last) as usize + (i % 256)) as u8;
        *cur = val;
        last = val;
    }
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw) + machine_id
}

/// Cursor 请求头
pub fn cursor_headers(access_token: &str, machine_id: &str) -> Vec<(&'static str, String)> {
    vec![
        ("authorization", format!("Bearer {}", access_token)),
        ("x-cursor-checksum", checksum(machine_id)),
        ("x-cursor-client-type", CLIENT_TYPE.into()),
        ("x-cursor-client-version", CLIENT_VERSION.into()),
        ("x-sand-box-namespace", "prod".into()),
        ("x-ghost-mode", "true".into()),
        ("x-request-id", Uuid::new_v4().to_string()),
        ("user-agent", "cursor-fast-proxy-rs/0.1".into()),
    ]
}

/// 构造 Cursor Stream 请求体
pub fn build_cursor_body(
    messages: &[Value],
    model: &str,
    max_tokens: Option<u32>,
    temperature: Option<f64>,
) -> Value {
    build_cursor_body_with_tools(messages, model, max_tokens, temperature, None, None, None)
}

pub fn build_cursor_body_with_tools(
    messages: &[Value],
    model: &str,
    max_tokens: Option<u32>,
    temperature: Option<f64>,
    tools: Option<&Value>,
    tool_choice: Option<&Value>,
    parallel_tool_calls: Option<bool>,
) -> Value {
    let mut cursor_msgs = crate::protocol::openai_messages_to_cursor(messages);
    if let Some(hint) = crate::protocol::tool_choice_hint(tool_choice) {
        cursor_msgs.insert(
            0,
            json!({
                "role": "user",
                "system": true,
                "parts": {"parts": [{"text": {"text": hint}}]},
            }),
        );
    }

    let mut body = json!({
        "requestedModel": {"modelId": model},
        "conversationId": Uuid::new_v4().to_string(),
        "messages": cursor_msgs,
        "stream": true,
    });
    if let Some(mt) = max_tokens {
        body["maxTokens"] = json!(mt);
    }
    if let Some(t) = temperature {
        body["temperature"] = json!(t);
    }
    let cursor_tools = crate::protocol::cursor_tools_from_client(tools);
    if !cursor_tools.is_empty() {
        body["tools"] = json!(cursor_tools);
        let mut cfg = json!({});
        if let Some(p) = parallel_tool_calls {
            cfg["parallelToolCalls"] = json!(p);
        }
        if !cfg.as_object().map(|o| o.is_empty()).unwrap_or(true) {
            body["modelConfig"] = cfg;
        }
    }
    body
}

/// Cursor 异步客户端
#[derive(Clone)]
pub struct CursorClient {
    client: Client<hyper_rustls::HttpsConnector<HttpConnector>, http_body_util::Full<hyper::body::Bytes>>,
    backend: String,
    timeout_s: u64,
}

impl CursorClient {
    pub fn backend(&self) -> &str {
        &self.backend
    }

    pub fn http(
        &self,
    ) -> &Client<hyper_rustls::HttpsConnector<HttpConnector>, http_body_util::Full<hyper::body::Bytes>>
    {
        &self.client
    }

    pub fn new(backend: &str, timeout_s: u64) -> anyhow::Result<Self> {
        // 连接层: 显式 connect 超时 (默认无限, SYN 卡死会占用槽位) + TCP keepalive/nodelay
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        http.set_connect_timeout(Some(std::time::Duration::from_secs(10)));
        http.set_keepalive(Some(std::time::Duration::from_secs(30)));
        http.set_nodelay(true);
        // ALPN 同时提供 h2 + http/1.1: 只启 h2 会让所有请求挤在一条连接上,
        // 撞服务端 MAX_CONCURRENT_STREAMS 后在客户端内部排队
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_all_versions()
            .wrap_connector(http);
        let client = Client::builder(TokioExecutor::new())
            .pool_max_idle_per_host(256)
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .build(https);
        Ok(Self {
            client,
            backend: backend.to_string(),
            timeout_s,
        })
    }

    /// 流式调用 Cursor Stream, 返回帧流
    pub async fn stream(
        &self,
        access_token: &str,
        machine_id: &str,
        body: &Value,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Value, CursorError>> + Send>>, CursorError> {
        let payload = serde_json::to_vec(body)
            .map_err(|e| CursorError::Decode(e.to_string()))?;
        let envelope = connect_envelope(&payload, 0);

        let mut req = hyper::Request::builder()
            .method("POST")
            .uri(format!("{}{}", self.backend, STREAM_PATH))
            .header("content-type", "application/connect+json")
            .header("connect-protocol-version", "1")
            .header("connect-timeout-ms", (self.timeout_s * 1000).to_string())
            .header("accept", "application/connect+json")
            .header("accept-encoding", "identity");

        for (k, v) in cursor_headers(access_token, machine_id) {
            req = req.header(k, v);
        }

        let req = req
            .body(http_body_util::Full::new(hyper::body::Bytes::from(envelope)))
            .map_err(|e| CursorError::Network(e.to_string()))?;

        // 客户端侧硬超时: 响应头必须在 timeout_s 内到达; 帧级空闲超时由调用方控制
        let timeout = std::time::Duration::from_secs(self.timeout_s.max(1));
        let resp = tokio::time::timeout(timeout, self.client.request(req))
            .await
            .map_err(|_| CursorError::Network(format!("upstream response headers timeout after {}s", self.timeout_s)))?
            .map_err(|e| CursorError::Network(e.to_string()))?;

        let status = resp.status().as_u16();
        if status >= 400 {
            let body_bytes = tokio::time::timeout(
                std::time::Duration::from_secs(15),
                http_body_util::BodyExt::collect(resp.into_body()),
            )
            .await
            .map_err(|_| CursorError::Network("error body read timeout".into()))?
            .map_err(|e| CursorError::Network(e.to_string()))?
            .to_bytes();
            let text = String::from_utf8_lossy(&body_bytes);
            // 按字符截断; 按字节切 &str 会在多字节边界 panic (panic=abort 直接掉进程)
            return Err(CursorError::Http(status, text.chars().take(200).collect()));
        }

        Ok(Box::pin(FrameStream {
            body: resp.into_body(),
            buf: BytesMut::with_capacity(8192),
        }))
    }
}

/// Connect 帧流解码器
struct FrameStream {
    body: Incoming,
    buf: BytesMut,
}

impl Stream for FrameStream {
    type Item = Result<Value, CursorError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // 先尝试从 buf 解析帧
            if self.buf.len() >= 5 {
                let flags = self.buf[0];
                let n = u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]) as usize;
                if n > MAX_FRAME_BYTES {
                    return Poll::Ready(Some(Err(CursorError::Decode(format!(
                        "connect frame {n} bytes exceeds {MAX_FRAME_BYTES}"
                    )))));
                }
                if self.buf.len() >= 5 + n {
                    let payload = self.buf.split_to(5 + n).split_off(5);
                    let payload = if flags & 1 != 0 {
                        match decompress_gzip(&payload) {
                            Ok(p) => p,
                            Err(e) => return Poll::Ready(Some(Err(CursorError::Decode(e)))),
                        }
                    } else {
                        payload.to_vec()
                    };
                    let obj = if payload.is_empty() {
                        None
                    } else {
                        match serde_json::from_slice(&payload) {
                            Ok(v) => Some(v),
                            Err(e) => return Poll::Ready(Some(Err(CursorError::Decode(e.to_string())))),
                        }
                    };
                    if flags & 2 != 0 {
                        return Poll::Ready(None);
                    }
                    if let Some(obj) = obj {
                        return Poll::Ready(Some(Ok(obj)));
                    }
                    continue;
                }
            }
            // buf 不够, 从 body 读更多
            match Pin::new(&mut self.body).poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    if let Ok(data) = frame.into_data() {
                        if self.buf.len().saturating_add(data.len()) > MAX_STREAM_BUF {
                            return Poll::Ready(Some(Err(CursorError::Decode(format!(
                                "stream buffer {}+{} exceeds {MAX_STREAM_BUF}",
                                self.buf.len(),
                                data.len()
                            )))));
                        }
                        self.buf.extend_from_slice(&data);
                    }
                    if self.buf.is_empty() {
                        return Poll::Ready(None);
                    }
                    continue;
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(CursorError::Network(e.to_string()))));
                }
                Poll::Ready(None) => {
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn decompress_gzip(data: &[u8]) -> Result<Vec<u8>, String> {
    use std::io::Read;
    let mut decoder = flate2::read::GzDecoder::new(data);
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|e| e.to_string())?;
    Ok(out)
}

/// 刷新 access_token（OAuth 风格，支持自定义端点）
pub async fn refresh_access_token(
    client: &reqwest::Client,
    refresh_url: Option<&str>,
    backend: &str,
    refresh_token: &str,
) -> Result<(String, Option<u64>), String> {
    let url = refresh_url
        .map(|u| u.to_string())
        .unwrap_or_else(|| format!("{}/oauth/token", backend.trim_end_matches('/')));

    let resp = client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }))
        .send()
        .await
        .map_err(|e| format!("refresh request failed: {}", e))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("refresh failed {}: {}", status, text));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("refresh decode failed: {}", e))?;

    let access_token = body
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "no access_token in refresh response".to_string())?
        .to_string();

    let expires_in = body
        .get("expires_in")
        .and_then(|v| v.as_u64())
        .map(|s| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                + s
        });

    Ok((access_token, expires_in))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kimi_family_gets_1m_and_long_output() {
        assert!(is_kimi_family("kimi-k3"));
        assert!(is_kimi_family("kimi-k3-max"));
        assert!(is_kimi_family("Kimi-K3-high"));
        assert!(!is_kimi_family("claude-sonnet-4-6"));
        assert_eq!(context_window_for("kimi-k3"), 1_048_576);
        assert_eq!(default_max_tokens_for("kimi-k3"), Some(32_768));
        assert_eq!(default_max_tokens_for("claude-sonnet-4-6"), None);
    }

    #[test]
    fn tools_go_into_cursor_body() {
        let msgs = vec![json!({"role": "user", "content": "ls"})];
        let tools = json!([{
            "type": "function",
            "function": {"name": "bash", "description": "sh", "parameters": {"type": "object"}}
        }]);
        let body = build_cursor_body_with_tools(
            &msgs,
            "kimi-k3",
            Some(128),
            None,
            Some(&tools),
            Some(&json!("required")),
            Some(false),
        );
        assert_eq!(body["tools"][0]["name"], "bash");
        assert_eq!(body["modelConfig"]["parallelToolCalls"], false);
        assert!(body["messages"][0]["parts"]["parts"][0]["text"]["text"]
            .as_str()
            .unwrap()
            .contains("must call a tool"));
    }
}
