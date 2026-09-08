//! 被上游确定性拒绝 (4xx / ERROR_BAD_REQUEST) 的请求体落盘, 供事后 replay 定位触发形态.
//!
//! 背景 (2026-09-08): card-cd0f30da8 的 claude-opus-4-8 会话在 tool_result 顺序修复 (d6f8751) 之后
//! 仍有 101 次 `providerStatusCode=400`. gateway.log / proxy.log / billing_records 都只存错误尾巴,
//! Anthropic 的原始 error message 被 Cursor 吞成一句 "trouble connecting", 请求体从未落地 —
//! 没有样本就没法说清还有哪种消息形态会被 400. 这里在 `request_rejected_by_model` 分支把翻译前的
//! OpenAI Chat 形态请求体写到 `<数据目录>/rejected/<ts>-<req_id>.json`, 图片 base64 截断到前 64 字节
//! (保留 mime/长度足以判断是不是尺寸问题, 又不让单文件膨胀到几 MB).
//!
//! 开关: `CFP_DUMP_REJECTED=0` 关 (默认开). 上限: `CFP_DUMP_REJECTED_KEEP` 个文件 (默认 200),
//! 超过按文件名 (时间戳前缀) 删最旧. 单文件 > 4 MiB 不写 (只 warn), 避免超长上下文把磁盘吃掉.

use serde_json::{json, Value};
use std::path::{Path, PathBuf};

const DEFAULT_KEEP: usize = 200;
const MAX_FILE_BYTES: usize = 4 * 1024 * 1024;
/// 图片 base64 保留前缀长度: 够看出 magic bytes (PNG `iVBORw0KGgo` / JPEG `/9j/`), 不够还原图.
const IMAGE_KEEP_CHARS: usize = 64;

pub fn enabled() -> bool {
    std::env::var("CFP_DUMP_REJECTED")
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true)
}

fn keep() -> usize {
    std::env::var("CFP_DUMP_REJECTED_KEEP")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(DEFAULT_KEEP)
}

/// 递归截断请求体里的图片 data: URI / 裸 base64, 其余字段原样. 返回副本, 不改原值.
///
/// 识别规则 (与 `protocol::content_parts` 对齐, 再加 Anthropic 原生 `image.source.data`):
/// - `{"type":"image_url","image_url":{"url":"data:…"}}` / `image_url` 直接是字符串
/// - `{"type":"input_image","image_url":"data:…"}`
/// - `{"type":"image","source":{"type":"base64","data":"…"}}`
/// - 任何以 `data:image/` 开头且长度 > 保留长度的字符串 (兜底)
pub fn redact_images(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            let is_anthropic_source = map.get("type").and_then(|t| t.as_str()) == Some("base64")
                && map.get("data").map(|d| d.is_string()).unwrap_or(false);
            for (k, val) in map {
                let redacted = if is_anthropic_source && k == "data" {
                    truncate_b64(val)
                } else if k == "url"
                    && val
                        .as_str()
                        .map(|s| s.starts_with("data:"))
                        .unwrap_or(false)
                {
                    truncate_b64(val)
                } else if k == "image_url" && val.is_string() {
                    truncate_b64(val)
                } else {
                    redact_images(val)
                };
                out.insert(k.clone(), redacted);
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.iter().map(redact_images).collect()),
        Value::String(s) if s.starts_with("data:image/") && s.len() > IMAGE_KEEP_CHARS => {
            truncate_b64(v)
        }
        other => other.clone(),
    }
}

fn truncate_b64(v: &Value) -> Value {
    let Some(s) = v.as_str() else {
        return v.clone();
    };
    if s.len() <= IMAGE_KEEP_CHARS {
        return v.clone();
    }
    // data:<mime>;base64,<payload> → 保留 meta 段 + payload 前缀; 裸 base64 → 保留前缀
    let (meta, payload) = match s
        .strip_prefix("data:")
        .and_then(|rest| rest.split_once(','))
    {
        Some((m, p)) => (format!("data:{m},"), p),
        None => (String::new(), s),
    };
    let head: String = payload.chars().take(IMAGE_KEEP_CHARS).collect();
    Value::String(format!(
        "{meta}{head}…[truncated {} chars, total {} chars]",
        payload.len().saturating_sub(head.len()),
        s.len()
    ))
}

/// 落盘. 同步 IO (单文件 ≤ 4 MiB, 且只在被拒路径触发, 频率低), 失败只 warn 不影响响应.
/// 返回写入路径 (供日志).
pub fn dump(
    dir: &Path,
    req_id: &str,
    model: &str,
    account: &str,
    key_name: &str,
    error: &str,
    body: &Value,
) -> Option<PathBuf> {
    let ts = chrono::Utc::now();
    let envelope = json!({
        "ts": ts.to_rfc3339(),
        "req_id": req_id,
        "model": model,
        "account": account,
        "key_name": key_name,
        "error": error,
        "body": redact_images(body),
    });
    let text = match serde_json::to_string_pretty(&envelope) {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(event = "rejected_dump_failed", req_id = %req_id, error = %e, "serialize failed");
            return None;
        }
    };
    if text.len() > MAX_FILE_BYTES {
        tracing::warn!(
            event = "rejected_dump_skipped",
            req_id = %req_id,
            bytes = text.len(),
            "rejected request body exceeds size cap; not dumped"
        );
        return None;
    }
    if let Err(e) = std::fs::create_dir_all(dir) {
        tracing::warn!(event = "rejected_dump_failed", req_id = %req_id, error = %e, dir = %dir.display(), "mkdir failed");
        return None;
    }
    let fname = format!("{}-{}.json", ts.format("%Y%m%dT%H%M%S%.3fZ"), req_id);
    let path = dir.join(fname);
    if let Err(e) = std::fs::write(&path, text) {
        tracing::warn!(event = "rejected_dump_failed", req_id = %req_id, error = %e, path = %path.display(), "write failed");
        return None;
    }
    prune(dir, keep());
    Some(path)
}

/// 按文件名字典序 (= 时间戳前缀) 删最旧, 只留 `keep` 个 `.json`.
fn prune(dir: &Path, keep: usize) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    let mut names: Vec<PathBuf> = rd
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "json").unwrap_or(false))
        .collect();
    if names.len() <= keep {
        return;
    }
    names.sort();
    let excess = names.len() - keep;
    for p in names.into_iter().take(excess) {
        let _ = std::fs::remove_file(p);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn big_png() -> String {
        format!("data:image/png;base64,{}", "iVBORw0KGgo".repeat(400))
    }

    #[test]
    fn redacts_openai_image_url_object_and_string_forms() {
        let body = json!({
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "看图"},
                {"type": "image_url", "image_url": {"url": big_png()}},
                {"type": "input_image", "image_url": big_png()},
            ]}]
        });
        let r = redact_images(&body);
        let parts = r["messages"][0]["content"].as_array().unwrap();
        assert_eq!(parts[0]["text"], "看图");
        let u = parts[1]["image_url"]["url"].as_str().unwrap();
        assert!(u.starts_with("data:image/png;base64,iVBORw0KGgo"), "{u}");
        assert!(u.contains("[truncated"), "{u}");
        assert!(u.len() < 200, "{}", u.len());
        let u2 = parts[2]["image_url"].as_str().unwrap();
        assert!(u2.contains("[truncated"), "{u2}");
        // 原值不动
        assert!(
            body["messages"][0]["content"][1]["image_url"]["url"]
                .as_str()
                .unwrap()
                .len()
                > 4000
        );
    }

    #[test]
    fn redacts_anthropic_base64_source_and_keeps_short_strings() {
        let body = json!({
            "messages": [{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": "/9j/".repeat(2000)}},
                {"type": "tool_result", "tool_use_id": "t1", "content": "data:image/png;base64,short"}
            ]}]
        });
        let r = redact_images(&body);
        let d = r["messages"][0]["content"][0]["source"]["data"]
            .as_str()
            .unwrap();
        assert!(d.starts_with("/9j//9j/"), "{d}");
        assert!(d.contains("total 8000 chars"), "{d}");
        assert_eq!(
            r["messages"][0]["content"][0]["source"]["media_type"],
            "image/jpeg"
        );
        // 短字符串 (≤ 保留长度) 原样
        assert_eq!(
            r["messages"][0]["content"][1]["content"],
            "data:image/png;base64,short"
        );
    }

    #[test]
    fn dump_writes_envelope_and_prunes_oldest() {
        let dir = std::env::temp_dir().join(format!(
            "cfp-rejected-test-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let body =
            json!({"model": "claude-opus-4-8", "messages": [{"role": "user", "content": "hi"}]});
        let p = dump(
            &dir,
            "req-a",
            "claude-opus-4-8-max",
            "acc-7",
            "card-x",
            "upstream rejected: … providerStatusCode=400]",
            &body,
        )
        .unwrap();
        let text = std::fs::read_to_string(&p).unwrap();
        let v: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(v["req_id"], "req-a");
        assert_eq!(v["account"], "acc-7");
        assert_eq!(v["body"]["messages"][0]["content"], "hi");
        assert!(p
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .ends_with("-req-a.json"));

        // prune: 写 5 个, keep 3 → 剩最新 3 个
        for i in 0..5 {
            std::fs::write(
                dir.join(format!("2000010{i}T000000.000Z-req-{i}.json")),
                "{}",
            )
            .unwrap();
        }
        prune(&dir, 3);
        let mut left: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        left.sort();
        assert_eq!(left.len(), 3);
        // 20000100..20000104 五个 + 本次 dump (2026…) 一个 = 6 个, 留 3 个最新: 20000103, 20000104, 2026…
        assert!(left[0].starts_with("20000103"), "{left:?}");
        assert!(left[1].starts_with("20000104"), "{left:?}");
        assert!(left[2].starts_with("20"), "{left:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
