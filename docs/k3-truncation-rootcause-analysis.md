# Rust 版本截断/停断问题根因分析报告 (2026-09-01)

## 问题现象

用户反馈 Rust 版本 cursor-fast-proxy-rs "经常自己停下来" — 具体表现为：
1. **流式输出中途断流**：客户端收到半截响应，没有 `[DONE]` 或正常收尾
2. **长输出被截断**：生成内容突然中断，没有自动续写
3. **空内容返回**：上游返回 200 但 content 为空

## 已修复的问题

### 1. 输出截断自动重试（51ea318）

**根因**：Cursor 上游在输出超限时发送 `isOutputTokenLimitError`，原实现直接报错，客户端收到不完整响应。

**修复**：
- 检测 `isOutputTokenLimitError` 错误帧
- 自动降 budget 重试（`lower_output_budget`：当前值减半，不低于 1024）
- 同号重试，不消耗 MAX_RETRIES 账号轮换次数

```rust
// main.rs
if translate::is_output_token_limit_error_str(&last_error) {
    let new_budget = translate::lower_output_budget(...);
    if new_budget < current {
        info!(event = "output_budget_retry", ...);
        current_max_tokens = Some(new_budget);
        continue; // 同号重试
    }
}
```

### 2. 流式断流兜底（b2d4577）

**根因**：上游中途断开或网络抖动，客户端收到没收尾的流，卡住显示"继续"。

**修复**：
- 记录是否已发出正常收尾标记（`[DONE]` / `message_stop` / `response.completed`）
- 流结束时未发收尾标记则补发
- SSE 心跳保活（15s 间隔注释帧，防 nginx/CF 掐空闲连接）

```rust
// main.rs
let terminal_marker = match dialect { ... };
let mut sent_terminal = false;
// ...
if !sent_terminal {
    let _ = tx.send(Ok(Bytes::from(dialect_terminal(dialect, &model_clone)))).await;
}
```

### 3. maxTokens floor + 默认 128k（b018bc5 + 2511e81）

**根因**：
- 客户端传 `max_tokens=16` 时上游直接报错 `Provider exceeded max output tokens`
- 默认 32k 对长代码/长文档场景仍频繁截断

**修复**：
- `MAX_TOKENS_FLOOR = 1024`：客户端传 <1024 自动提升
- `DEFAULT_MAX_TOKENS = 131_072`（128k）：默认输出预算

### 4. maxMode 默认开启（2511e81）

**根因**：`maxMode` 未传时默认关闭，Cursor 限制在标准上下文窗口（~128K），1M 上下文不可用。

**修复**：`max_mode.unwrap_or(true)` 默认开启 EM 模式。

### 5. finish_reason 正确报告截断（b018bc5）

**根因**：上游截断时返回 `stopReason: MAX_TOKENS/LENGTH`，但代理层一律返回 `"stop"`，客户端不知道被截断。

**修复**：
- 从 `responseInfo.stopReason` 提取真实停止原因
- `out_was_truncated()` 统一识别四种截断原因
- Chat 方言返回 `"length"`，Anthropic 方言返回 `"max_tokens"`

### 6. 空内容检测（0071ef8）

**根因**：上游偶发返回 200 但 content 为空，客户端收到空响应。

**修复**：非流式响应空内容检测，返回 502 让客户端重试。

### 7. 120s 思考超时控制（0071ef8）

**根因**：算法设计等复杂场景上游思考 243s+，用户等待过久。

**修复**：`default_timeout` 1800s → 120s，超时强制返回。

## 尚未修复的问题

### P1-6: auto-continue 缺失

**现象**：长输出被截断后无法自动续写。

**Python 版有 `_build_continuation_request`**：在 `isOutputTokenLimitError` mid-stream 时，将已生成文本作为 assistant message 追加到 messages，重新打开流。

**Rust 版缺失**：当前截断重试只在**请求开始前**（连接阶段），**流式中途截断**不会自动续写。

**影响**：长代码/长文档生成到一半被截断，用户需要手动说"继续"。

## 问题根因总结

| 问题 | 根因 | 修复状态 |
|------|------|---------|
| 输出截断报错 | `isOutputTokenLimitError` 未处理 | ✅ 已修复（请求前重试） |
| 流式中途断流 | 上游断开/网络抖动 | ✅ 已修复（断流兜底+心跳） |
| 长输出截断 | 默认 32k 太小 | ✅ 已修复（默认 128k） |
| 1M 上下文不可用 | maxMode 未开启 | ✅ 已修复（默认开启） |
| 截断无感知 | finish_reason 一律 "stop" | ✅ 已修复（正确报告 length） |
| 空内容返回 | 上游偶发空响应 | ✅ 已修复（502 让客户端重试） |
| 思考时间过长 | 无超时控制 | ✅ 已修复（120s 超时） |
| **流式中途截断不续写** | **auto-continue 缺失** | ❌ **未修复** |

## 下一步建议

**立即执行**：
- [ ] 实现 P1-6 auto-continue：流式中途截断检测 + 自动续写
  - 检测 `isOutputTokenLimitError` mid-stream
  - 将已生成文本作为 assistant message 追加到 messages
  - 重新打开流继续生成

**验证方法**：
```bash
# 发送需要长输出的请求，观察是否自动续写
curl -X POST http://38.92.24.153:8800/v1/chat/completions \
  -H "Authorization: Bearer sk-test-key" \
  -d '{"model":"kimi-k3","messages":[{"role":"user","content":"Write a 10000-word essay"}],"max_tokens":131072,"stream":true}'
```
