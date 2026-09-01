# Kimi K3 长上下文测试报告 (2026-09-01)

## 测试环境
- 服务: cursor-fast-proxy-rs @ 38.92.24.153:8800
- 模型: kimi-k3 (自动映射到 kimi-k3-max/high/low)
- 测试时间: 2026-09-01 05:40 UTC

## 测试结果汇总

### 基础上下文长度测试

| 测试项 | 状态 | 实际 prompt tokens | 预期 tokens | 耗时 |
|--------|------|-------------------|-------------|------|
| 4K 上下文 | TRUNCATED | 0 | ~1,000 | 0.12s |
| 32K 上下文 | TRUNCATED | 0 | ~8,000 | 0.32s |
| 128K 上下文 | TRUNCATED | 16,088 | ~32,000 | 17.93s |
| 256K 上下文 | TRUNCATED | 32,088 | ~64,000 | 50.98s |
| 512K 上下文 | ERROR | - | ~128,000 | timeout |
| 1M 上下文 | ERROR | - | ~250,000 | timeout |

### NIAH (Needle In A Haystack) 测试

| 测试项 | 状态 | 找到 needle | 回答 |
|--------|------|------------|------|
| NIAH-32K-10% | PASS | ✅ | The secret code is: **K3-1M-CONTEXT-OK** |
| NIAH-32K-50% | PASS | ✅ | The secret code is: **K3-1M-CONTEXT-OK** |
| NIAH-32K-90% | PASS | ✅ | The secret code is: **K3-1M-CONTEXT-OK** |
| NIAH-128K-50% | ERROR | - | timeout |
| NIAH-512K-50% | ERROR | - | timeout |

## 关键发现

### 1. 上下文窗口实际表现 vs 官方声称

| 指标 | 官方声称 | 实际测试 | 差距 |
|------|---------|---------|------|
| 最大上下文 | 1M tokens | ~256K tokens | **-75%** |
| 128K 可用性 | 稳定 | 超时/截断 | 不稳定 |
| NIAH 32K | 应通过 | 通过 | ✅ 达标 |
| NIAH 128K+ | 应通过 | 超时 | ❌ 未达标 |

### 2. 不足点分析

#### P0 — 阻塞性问题
1. **128K+ 上下文超时**: 256K 耗时 50s，512K/1M 直接超时。需要检查：
   - 上游 Cursor API 是否有限流/超时
   - 代理层 buffering 是否导致延迟
   - 是否需要流式传输大上下文

2. **prompt_tokens 计费异常**: 4K/32K 测试 prompt_tokens=0，说明：
   - 上游可能未正确计费
   - 或 usage 提取逻辑有 bug

#### P1 — 功能缺失
3. **maxMode 未生效**: 当前 maxMode 已注入请求体，但 Cursor 上游可能未正确识别
   - 需要验证 requestedModel.maxMode 是否被 Cursor 接受
   - 可能需要额外的 header 或参数

4. **auto-continue 缺失**: 长输出被截断后无法自动续写
   - Python 版有 _build_continuation_request
   - Rust 版需要实现流式输出截断检测 + 自动续写

#### P2 — 协议完整性
5. **prompt caching cache_control 不透传**: Anthropic 风格缓存标记丢失
6. **Responses API 事件链不完整**: 仅基础事件，缺少 reasoning/thinking 完整链
7. **thinking/reasoning 非真正透传**: 仅包标签，无 encrypted_content/summary_text

## 待解决事项

### 立即执行 (本周)
- [ ] 排查 128K+ 上下文超时根因（上游限制 vs 代理层问题）
- [ ] 修复 prompt_tokens 计费为 0 的问题
- [ ] 验证 maxMode 是否被 Cursor 上游正确识别

### 短期 (下周)
- [ ] 实现 auto-continue（输出截断自动续写）
- [ ] 添加 RULER / LongBench 标准测试套件
- [ ] 对比官方 Kimi K3 API 直接调用 vs 通过 Cursor 代理的差异

### 中期 (本月)
- [ ] 实现 prompt caching cache_control 透传
- [ ] 完善 Responses API 事件链
- [ ] 真正的 thinking/reasoning 透传

## 测试工具

已创建 `test_k3_context.py` 支持：
- 基础上下文长度测试（4K/32K/128K/256K/512K/1M）
- NIAH 测试（10%/50%/90% 位置）
- 自动结果汇总和 JSON 导出

运行方式：
```bash
python3 test_k3_context.py
```

## 参考标准

- **RULER**: Microsoft 长上下文评估 benchmark (4K-128K)
- **NIAH**: Needle In A Haystack，测试信息检索能力
- **LongBench**: 多任务长上下文理解评估
- **官方声称**: Moonshot AI 声称 Kimi K3 支持 1M token 上下文

## 结论

当前 cursor-fast-proxy-rs 在 **32K 以内上下文表现良好**（NIAH 全通过），但 **128K+ 存在严重超时问题**，无法达到官方声称的 1M 上下文水准。主要瓶颈疑似在上游 Cursor API 限制或网络延迟，需要进一步排查。
