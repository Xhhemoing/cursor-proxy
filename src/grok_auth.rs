//! Grok Build (xAI) 授权与凭证管理.
//!
//! 机制照搬 sub2api `internal/pkg/xai`: sso cookie → OAuth device flow → build token.
//! 与 Cursor 凭证是两套独立体系, 在同一 `Account` 记录里显式绑定 (account.id 为关联键).
//!
//! 流程:
//!   1. sso cookie 种到 accounts.x.ai / auth.x.ai
//!   2. POST auth.x.ai/oauth2/device/code → device_code + user_code
//!   3. 自动 verify + approve (action=allow), 无需人工点授权页
//!   4. 轮询 POST auth.x.ai/oauth2/token (grant_type=device_code) → access/refresh token
//!   5. build token 过期前用 refresh_token 续期

use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const OAUTH_ISSUER: &str = "https://auth.x.ai";
pub const DEVICE_CODE_URL: &str = "https://auth.x.ai/oauth2/device/code";
pub const DEVICE_VERIFY_URL: &str = "https://auth.x.ai/oauth2/device/verify";
pub const DEVICE_APPROVE_URL: &str = "https://auth.x.ai/oauth2/device/approve";
pub const TOKEN_URL: &str = "https://auth.x.ai/oauth2/token";
pub const ACCOUNTS_URL: &str = "https://accounts.x.ai/";
pub const CLIENT_ID: &str = "b1a00492-073a-47ea-816f-4c329264a828";
pub const BUILD_SCOPE: &str =
    "openid profile email offline_access grok-cli:access api:access conversations:read conversations:write";
/// Build 调用端点 (CLI 代理)
pub const CLI_BASE_URL: &str = "https://cli-chat-proxy.grok.com/v1";
pub const CLI_HOST: &str = "cli-chat-proxy.grok.com";

/// CLI 身份头 (cli-chat-proxy 强制校验, 缺了 403)
pub const CLI_TOKEN_AUTH: &str = "xai-grok-cli";
pub const CLI_CLIENT_IDENTIFIER: &str = "grok-shell";
pub const CLI_CLIENT_VERSION: &str = "0.2.93";

const DEFAULT_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
const DEFAULT_TOKEN_TTL_SECS: u64 = 6 * 3600;
const MAX_AUTH_BODY: usize = 2 << 20; // 2 MiB
const MAX_TOKEN_LEN: usize = 16 << 10;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BuildToken {
    pub access_token: String,
    #[serde(default)]
    pub refresh_token: String,
    #[serde(default)]
    pub id_token: String,
    #[serde(default)]
    pub token_type: String,
    /// access_token 过期时间 (unix 秒)
    #[serde(default)]
    pub expires_at: Option<u64>,
    #[serde(default)]
    pub scope: String,
}

impl BuildToken {
    pub fn is_expired(&self, skew_secs: u64) -> bool {
        match self.expires_at {
            Some(ts) => now_unix() + skew_secs >= ts,
            None => false, // 未知按不过期处理
        }
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// 归一化 sso token: 接受 "cookie:sso=xxx", "sso=xxx; sso-rw=xxx", 裸值.
pub fn normalize_sso_token(value: &str) -> String {
    let mut v = value.trim();
    if v.to_lowercase().starts_with("cookie:") {
        v = v[7..].trim();
    }
    // "k=v; k2=v2" 形式优先取 sso / sso-rw
    if v.contains('=') {
        for part in v.split(';') {
            let part = part.trim();
            if let Some((name, tok)) = part.split_once('=') {
                let n = name.trim().to_lowercase();
                if n == "sso" || n == "sso-rw" {
                    return sanitize_token(tok);
                }
            }
        }
        // 有 = 但没匹配到 sso 键, 取第一段
        if let Some((first, _)) = v.split_once(';') {
            return sanitize_token(first);
        }
    }
    sanitize_token(v)
}

fn sanitize_token(value: &str) -> String {
    let cleaned: String = value
        .trim()
        .chars()
        .filter(|c| *c != '\r' && *c != '\n' && *c != '\0')
        .collect();
    if cleaned.len() > MAX_TOKEN_LEN {
        return String::new();
    }
    cleaned
}

/// 从 access_token (JWT) 解出 email / sub 用于绑定展示. 不验签 (仅展示用).
pub fn decode_jwt_claims(token: &str) -> Option<serde_json::Value> {
    let mut parts = token.split('.');
    parts.next()?; // header
    let payload = parts.next()?;
    use base64::Engine;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(format!("{}==", payload))
        })
        .ok()?;
    serde_json::from_slice(&decoded).ok()
}

/// 从 build token 提取展示用 email (id_token 优先, 回退 access_token).
pub fn token_email(tok: &BuildToken) -> Option<String> {
    for t in [&tok.id_token, &tok.access_token] {
        if t.is_empty() {
            continue;
        }
        if let Some(c) = decode_jwt_claims(t) {
            if let Some(e) = c.get("email").and_then(|v| v.as_str()) {
                return Some(e.to_string());
            }
        }
    }
    None
}

/// xAI 授权客户端. 不显式用 cookie jar — device flow 每一步都显式带 sso Cookie 头,
/// 更可控且不依赖 reqwest cookies feature.
pub struct GrokAuth {
    client: reqwest::Client,
    user_agent: String,
    /// 当前会话的 sso token (convert 期间设置, 用于每个请求的 Cookie 头).
    sso: std::sync::Arc<std::sync::RwLock<String>>,
}

impl Default for GrokAuth {
    fn default() -> Self {
        Self::new()
    }
}

impl GrokAuth {
    pub fn new() -> Self {
        // 手动处理重定向: device flow 的 verify/approve 要看 30x 的 Location 判断是否到 consent/done.
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(90))
            .build()
            .expect("grok auth client");
        Self {
            client,
            user_agent: DEFAULT_UA.to_string(),
            sso: std::sync::Arc::new(std::sync::RwLock::new(String::new())),
        }
    }

    /// 完整转换: sso cookie → build access/refresh token.
    pub async fn convert_sso_to_build(&self, sso_token: &str) -> Result<BuildToken, String> {
        let sso = normalize_sso_token(sso_token);
        if sso.is_empty() {
            return Err("xai sso unauthorized (empty)".into());
        }
        if let Ok(mut g) = self.sso.write() {
            *g = sso;
        }

        // 1. 校验 sso 有效性 (GET accounts.x.ai)
        let (status, final_url, _) = self.do_request(reqwest::Method::GET, ACCOUNTS_URL, None).await?;
        if status == 401
            || final_url.contains("sign-in")
            || final_url.contains("sign-up")
        {
            return Err("xai sso unauthorized".into());
        }
        if !(200..400).contains(&status) {
            return Err(format!("validate Grok Web SSO: HTTP {}", status));
        }

        // 2. 启动 device flow
        let (status, _, body) = self
            .do_request(
                reqwest::Method::POST,
                DEVICE_CODE_URL,
                Some(vec![
                    ("client_id", CLIENT_ID),
                    ("scope", BUILD_SCOPE),
                ]),
            )
            .await?;
        if !(200..300).contains(&status) {
            return Err(format!("start xAI device flow: HTTP {}", status));
        }
        let device: serde_json::Value =
            serde_json::from_slice(&body).map_err(|e| format!("parse device flow: {}", e))?;
        let device_code = device
            .get("device_code")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let user_code = device
            .get("user_code")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let verify_uri = device
            .get("verification_uri_complete")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let interval = device.get("interval").and_then(|v| v.as_u64()).unwrap_or(5).max(1);
        let expires_in = device
            .get("expires_in")
            .and_then(|v| v.as_u64())
            .unwrap_or(1800);
        if device_code.is_empty() || user_code.is_empty() || !safe_xai_url(&verify_uri) {
            return Err("xAI device flow response incomplete".into());
        }

        // 3. 打开 verification 页 (建立会话)
        let (status, _, _) = self.do_request(reqwest::Method::GET, &verify_uri, None).await?;
        if !(200..400).contains(&status) {
            return Err(format!("open xAI device verification page: HTTP {}", status));
        }

        // 4. verify user_code → 应到 consent 页
        let (status, final_url, _) = self
            .do_request(
                reqwest::Method::POST,
                DEVICE_VERIFY_URL,
                Some(vec![("user_code", user_code.as_str())]),
            )
            .await?;
        if !(200..400).contains(&status) {
            return Err(format!("verify xAI device code: HTTP {}", status));
        }
        if !final_url.contains("consent") {
            return Err("xAI device verification did not reach consent page".into());
        }

        // 5. approve (action=allow) → 应到 done 页
        let (status, final_url, _) = self
            .do_request(
                reqwest::Method::POST,
                DEVICE_APPROVE_URL,
                Some(vec![
                    ("user_code", user_code.as_str()),
                    ("action", "allow"),
                    ("principal_type", "User"),
                    ("principal_id", ""),
                ]),
            )
            .await?;
        if !(200..400).contains(&status) {
            return Err(format!("approve xAI device code: HTTP {}", status));
        }
        if !final_url.contains("done") {
            return Err("xAI device approval did not reach done page".into());
        }

        // 6. 轮询 token
        self.poll_token(&device_code, interval, expires_in).await
    }

    async fn poll_token(
        &self,
        device_code: &str,
        interval: u64,
        expires_in: u64,
    ) -> Result<BuildToken, String> {
        let deadline = now_unix() + expires_in.min(75);
        loop {
            if now_unix() >= deadline {
                return Err("xAI device token polling timed out".into());
            }
            tokio::time::sleep(Duration::from_secs(interval.max(1))).await;
            let (status, _, body) = self
                .do_request(
                    reqwest::Method::POST,
                    TOKEN_URL,
                    Some(vec![
                        ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                        ("client_id", CLIENT_ID),
                        ("device_code", device_code),
                    ]),
                )
                .await?;
            let payload: serde_json::Value =
                serde_json::from_slice(&body).map_err(|e| format!("parse token response: {}", e))?;
            if (200..300).contains(&status) {
                if let Some(at) = payload.get("access_token").and_then(|v| v.as_str()) {
                    let expires_in = payload
                        .get("expires_in")
                        .and_then(|v| v.as_u64())
                        .filter(|v| *v > 0)
                        .unwrap_or(DEFAULT_TOKEN_TTL_SECS);
                    return Ok(BuildToken {
                        access_token: at.to_string(),
                        refresh_token: payload
                            .get("refresh_token")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        id_token: payload
                            .get("id_token")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        token_type: payload
                            .get("token_type")
                            .and_then(|v| v.as_str())
                            .unwrap_or("Bearer")
                            .to_string(),
                        expires_at: Some(now_unix() + expires_in),
                        scope: payload
                            .get("scope")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                    });
                }
            }
            let err = payload.get("error").and_then(|v| v.as_str()).unwrap_or("");
            match err {
                "authorization_pending" | "slow_down" => continue,
                "access_denied" => return Err("xai device authorization denied".into()),
                "expired_token" => return Err("xai device code expired".into()),
                _ if !err.is_empty() => {
                    let desc = payload
                        .get("error_description")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    return Err(format!("xai token error {}: {}", err, desc));
                }
                _ => continue,
            }
        }
    }

    /// 用 refresh_token 续期 build token.
    pub async fn refresh_build_token(&self, refresh_token: &str) -> Result<BuildToken, String> {
        let (status, _, body) = self
            .do_request(
                reqwest::Method::POST,
                TOKEN_URL,
                Some(vec![
                    ("grant_type", "refresh_token"),
                    ("client_id", CLIENT_ID),
                    ("refresh_token", refresh_token),
                ]),
            )
            .await?;
        let payload: serde_json::Value =
            serde_json::from_slice(&body).map_err(|e| format!("parse refresh response: {}", e))?;
        if !(200..300).contains(&status) {
            let desc = payload
                .get("error_description")
                .or_else(|| payload.get("error"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            return Err(format!("xai refresh failed {}: {}", status, desc));
        }
        let at = payload
            .get("access_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "no access_token in refresh response".to_string())?;
        let expires_in = payload
            .get("expires_in")
            .and_then(|v| v.as_u64())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_TOKEN_TTL_SECS);
        Ok(BuildToken {
            access_token: at.to_string(),
            refresh_token: payload
                .get("refresh_token")
                .and_then(|v| v.as_str())
                .unwrap_or(refresh_token) // 上游有时不轮转 refresh_token
                .to_string(),
            id_token: payload
                .get("id_token")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            token_type: payload
                .get("token_type")
                .and_then(|v| v.as_str())
                .unwrap_or("Bearer")
                .to_string(),
            expires_at: Some(now_unix() + expires_in),
            scope: payload
                .get("scope")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        })
    }

    /// 执行一个 HTTP 请求, 手动跟随重定向, 返回 (status, final_url, body).
    async fn do_request(
        &self,
        method: reqwest::Method,
        url: &str,
        form: Option<Vec<(&str, &str)>>,
    ) -> Result<(u16, String, Vec<u8>), String> {
        let mut current_url = url.to_string();
        let mut current_method = method;
        let mut current_form = form;
        for _ in 0..10 {
            let mut req = self.client.request(current_method.clone(), &current_url);
            req = req
                .header("Accept", "application/json, text/html;q=0.9, */*;q=0.8")
                .header("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
                .header("User-Agent", &self.user_agent);
            // device flow 全程带 sso Cookie (accounts.x.ai / auth.x.ai 都会校验登录态)
            let sso = self.sso.read().map(|g| g.clone()).unwrap_or_default();
            if !sso.is_empty() {
                req = req.header("Cookie", format!("sso={}; sso-rw={}", sso, sso));
            }
            if let Some(f) = &current_form {
                req = req.form(f);
            }
            let resp = req.send().await.map_err(|e| format!("xAI request: {}", e))?;
            let status = resp.status().as_u16();
            if !(300..=399).contains(&status) {
                let bytes = resp.bytes().await.map_err(|e| e.to_string())?;
                let truncated = if bytes.len() > MAX_AUTH_BODY {
                    return Err("xAI OAuth response exceeds 2 MiB".into());
                } else {
                    bytes.to_vec()
                };
                return Ok((status, current_url, truncated));
            }
            // 3xx: 手动跟随
            let location = resp
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .trim()
                .to_string();
            if location.is_empty() {
                return Err("xAI OAuth redirect missing Location".into());
            }
            let base = reqwest::Url::parse(&current_url).map_err(|e| e.to_string())?;
            let next = base.join(&location).map_err(|e| e.to_string())?;
            let next_str = next.to_string();
            if !safe_xai_url(&next_str) {
                return Err("xAI OAuth redirected to untrusted host".into());
            }
            // 303 或 (301/302 且非 GET): 改 GET 并丢表单
            if status == 303 || ((status == 301 || status == 302) && current_method != reqwest::Method::GET) {
                current_method = reqwest::Method::GET;
                current_form = None;
            }
            current_url = next_str;
        }
        Err("xAI OAuth redirected too many times".into())
    }
}

fn safe_xai_url(raw: &str) -> bool {
    let parsed = match reqwest::Url::parse(raw) {
        Ok(u) => u,
        Err(_) => return false,
    };
    if parsed.scheme() != "https" {
        return false;
    }
    let host = parsed.host_str().unwrap_or("").to_lowercase();
    host == "x.ai" || host.ends_with(".x.ai")
}

/// 给 cli-chat-proxy 请求打 CLI 身份头. 直连 api.x.ai 不改.
pub fn cli_headers() -> Vec<(&'static str, String)> {
    vec![
        ("X-XAI-Token-Auth", CLI_TOKEN_AUTH.to_string()),
        ("x-grok-client-version", CLI_CLIENT_VERSION.to_string()),
        ("x-grok-client-identifier", CLI_CLIENT_IDENTIFIER.to_string()),
        ("User-Agent", format!("xai-grok-workspace/{}", CLI_CLIENT_VERSION)),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_sso_variants() {
        assert_eq!(normalize_sso_token("sso=abc123"), "abc123");
        assert_eq!(normalize_sso_token("cookie:sso=abc123"), "abc123");
        assert_eq!(normalize_sso_token("sso-rw=xyz; sso=abc"), "abc");
        assert_eq!(normalize_sso_token("  baretoken  "), "baretoken");
        assert_eq!(normalize_sso_token("other=1; sso-rw=rw2"), "rw2");
    }

    #[test]
    fn sanitize_strips_control_chars() {
        assert_eq!(normalize_sso_token("abc\r\n\0def"), "abcdef");
        let long = "x".repeat(MAX_TOKEN_LEN + 1);
        assert_eq!(normalize_sso_token(&long), "");
    }

    #[test]
    fn safe_url_check() {
        assert!(safe_xai_url("https://auth.x.ai/oauth2/device/verify"));
        assert!(safe_xai_url("https://accounts.x.ai/"));
        assert!(!safe_xai_url("http://auth.x.ai/"));
        assert!(!safe_xai_url("https://evil.com/"));
        assert!(!safe_xai_url("https://x.ai.evil.com/"));
    }

    #[test]
    fn expiry_check() {
        let t = BuildToken {
            access_token: "x".into(),
            expires_at: Some(now_unix() + 3600),
            ..Default::default()
        };
        assert!(!t.is_expired(60));
        let expired = BuildToken {
            access_token: "x".into(),
            expires_at: Some(now_unix() - 10),
            ..Default::default()
        };
        assert!(expired.is_expired(60));
    }
}
