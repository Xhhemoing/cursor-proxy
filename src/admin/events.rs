//! SSE (Server-Sent Events) 实时推送端点.
//!
//! 替代前端轮询，服务端主动推送变更事件.

use axum::{
    extract::State,
    response::{IntoResponse, Response},
};
use futures_util::stream::{Stream, StreamExt};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

use crate::AppState;

/// SSE 事件类型
#[derive(Debug, Clone, serde::Serialize)]
pub struct AdminEvent {
    /// 事件类型: pool_update / log / config / key_update / proxy_update
    pub kind: String,
    /// 事件数据 (JSON)
    pub data: serde_json::Value,
    /// 服务端时间戳 (unix 秒)
    pub ts: i64,
}

impl AdminEvent {
    pub fn new(kind: &str, data: serde_json::Value) -> Self {
        Self {
            kind: kind.to_string(),
            data,
            ts: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
        }
    }

    /// 编码为 SSE data: 行
    pub fn to_sse(&self) -> String {
        format!(
            "event: {}\ndata: {}\n\n",
            self.kind,
            serde_json::to_string(self).unwrap_or_else(|_| "{}".into())
        )
    }
}

/// 全局事件广播器 (容量 256, 慢消费者丢弃旧消息)
pub struct EventBus {
    tx: broadcast::Sender<AdminEvent>,
}

impl EventBus {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(256);
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<AdminEvent> {
        self.tx.subscribe()
    }

    pub fn publish(&self, event: AdminEvent) {
        // 忽略发送错误 (无订阅者时)
        let _ = self.tx.send(event);
    }

    /// 推送池状态更新 (轻量摘要, 非全量账号表)
    pub fn pool_update(&self, pool: &crate::pool::AccountPool) {
        self.publish(AdminEvent::new("pool_update", pool.summary()));
    }

    /// 推送新日志
    pub fn log(&self, entry: &serde_json::Value) {
        self.publish(AdminEvent::new("log", entry.clone()));
    }

    /// 推送配置变更
    pub fn config_update(&self, config: &crate::config::AppConfig) {
        let view = config.public_view();
        self.publish(AdminEvent::new("config", view));
    }

    /// 推送 key 变更
    pub fn key_update(&self, keys: &[crate::config::ApiKeyRecord]) {
        let view: Vec<serde_json::Value> = keys
            .iter()
            .map(|k| {
                json!({
                    "key": k.key,
                    "name": k.name,
                    "enabled": k.enabled,
                    "token_limit": k.token_limit,
                    "request_limit": k.request_limit,
                })
            })
            .collect();
        self.publish(AdminEvent::new("key_update", json!(view)));
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

/// GET /admin/api/events — SSE 流
pub async fn api_events(State(state): State<Arc<AppState>>) -> Response {
    let rx = state.event_bus.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|result| async move {
        match result {
            Ok(event) => Some(Ok::<_, std::convert::Infallible>(event.to_sse())),
            Err(_) => None, //  Lagged (慢消费者) → 跳过, 继续
        }
    });

    let body = axum::body::Body::from_stream(stream);

    Response::builder()
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .header("connection", "keep-alive")
        .header("x-accel-buffering", "no") // 禁用 nginx 缓冲
        .body(body)
        .unwrap()
        .into_response()
}

/// 启动周期性池状态推送 (每 2s, 替代前端轮询)
pub async fn pool_broadcast_loop(state: Arc<AppState>) {
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(2));
    loop {
        tick.tick().await;
        state.event_bus.pool_update(&state.pool);
    }
}
