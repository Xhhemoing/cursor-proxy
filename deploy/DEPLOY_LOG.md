deployed md5 4034ffa3102697a291405e19e849d798 @ 2026-09-03T09:38:38Z PID 2178943
sticky+quota fix: agent hops same account acc1, cache_read 17073 on hop2, quota_blocked=0

## 2026-09-05 12:10 UTC — 本机 8800 零售版 P0 止血 (0bda416 + f23171c)
- 备份: ~/.local/opt/cursor-fast-proxy-rs/cursor-fast-proxy-rs.bak-p0pre-20260905-121054, cards.json.bak-p0pre-20260905-121054
- 换后 /proc/PID/exe md5 46633b46c2a59a4d47ce7ddae4bdee5e == target/release (无 deleted)
- 首启自动建 cards.db (card_face_used); cards.json 当时 0 张卡, 无迁移
- 冒烟 (quota-50 卡 card-5cfa4af9…): kimi-k3-high 中文非流式 200 → $0.010275 落 cards.db;
  流式 4s 客户端 kill → 仍结算 $0.004485 (≈282 tok 本地估算), in_flight 归 0
- 隔离 8899 + TCP 黑洞上游 E2E 11/11: scripts/e2e-cards-p0.sh (结果 docs/e2e-cards-p0-20260905.log)
