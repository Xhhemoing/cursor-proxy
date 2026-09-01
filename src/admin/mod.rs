//! 管理面板: 内嵌页面 + 查看/调整 API.
//!
//! 拆分为 accounts / keys / settings 三个子模块.

pub mod accounts;
pub mod billing;
pub mod events;
pub mod health_api;
pub mod keys;
pub mod proxies;
pub mod settings;

use axum::response::{Html, IntoResponse};

/// GET /admin — 管理面板页面
pub async fn admin_page() -> impl IntoResponse {
    Html(include_str!("../../static/admin.html"))
}

// Re-export 便于 main.rs 路由注册
pub use accounts::*;
pub use billing::*;
pub use events::*;
pub use health_api::*;
pub use keys::*;
pub use proxies::*;
pub use settings::*;
