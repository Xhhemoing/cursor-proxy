//! 健康诊断 API：GET /admin/api/health/diagnose

use axum::extract::State;
use axum::Json;
use std::sync::Arc;

use crate::health::HealthEngine;
use crate::AppState;

/// GET /admin/api/health/diagnose — 智能健康诊断
pub async fn api_health_diagnose(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let report = HealthEngine::diagnose(
        &state.pool,
        &state.proxies,
        &state.log_buffer,
        &state.config,
    )
    .await;
    Json(serde_json::to_value(report).unwrap_or_else(|_| {
        serde_json::json!({"ok": false, "error": "serialize failed"})
    }))
}
