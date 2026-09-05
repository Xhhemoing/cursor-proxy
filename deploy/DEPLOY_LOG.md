deployed md5 4034ffa3102697a291405e19e849d798 @ 2026-09-03T09:38:38Z PID 2178943
sticky+quota fix: agent hops same account acc1, cache_read 17073 on hop2, quota_blocked=0

## 2026-09-05 12:10 UTC — 本机 8800 零售版 P0 止血 (0bda416 + f23171c)
- 备份: ~/.local/opt/cursor-fast-proxy-rs/cursor-fast-proxy-rs.bak-p0pre-20260905-121054, cards.json.bak-p0pre-20260905-121054
- 换后 /proc/PID/exe md5 46633b46c2a59a4d47ce7ddae4bdee5e == target/release (无 deleted)
- 首启自动建 cards.db (card_face_used); cards.json 当时 0 张卡, 无迁移
- 冒烟 (quota-50 卡 card-5cfa4af9…): kimi-k3-high 中文非流式 200 → $0.010275 落 cards.db;
  流式 4s 客户端 kill → 仍结算 $0.004485 (≈282 tok 本地估算), in_flight 归 0
- 隔离 8899 + TCP 黑洞上游 E2E 11/11: scripts/e2e-cards-p0.sh (结果 docs/e2e-cards-p0-20260905.log)

## 2026-09-05 12:55 UTC — 本机 8800 零售版 A 批 (f324247)
- 备份: cursor-fast-proxy-rs.bak-models-20260905-125318, models.json.bak-20260905-125318
- 换后 /proc/PID/exe md5 5e2c5b0a567a120217fb7f03f1dd61fb == target/release
- 冒烟: /v1/models 23 个 (动态); quota-50 预览 23 可调; kimi-k3-high 中文 200 (usage 151+477)
- 隔离 E2E: scripts/e2e-models-a.sh 10/10 + scripts/e2e-cards-p0.sh 回归 11/11
- 注意: 隔离实例 dummy 号连续错误 ≥5 会 auto_disable → 请求 503 而非 502;
  恢复: POST /admin/api/accounts/:id/enabled {"enabled":true} + cooldown/clear

## 2026-09-05 13:40 UTC — 本机 8800 可见性修复 (40ac772)
- 备份: cursor-fast-proxy-rs.bak-vis-20260905-133230
- 换后 /proc/PID/exe md5 862dd10399a32cd1a00b149992706c0f == target/release
- 实测: /v1/models 213 个 = 上游 212 + kimi-k3 (网关别名, 显式保留); 幽灵 0
- 注意: 可见性名单在内存, 重启后需重新点「获取可用模型」; 未拉时 /v1/models 只有注册表条目+default+kimi-k3
- 隔离 E2E: scripts/e2e-models-visibility.sh 7/7
