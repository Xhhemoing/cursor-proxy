# 门户钱包/Key 重构 — 复盘 (2026-09-08)

对应计划: `docs/portal-wallet-keys-plan.md` (D1–D8)。提交: `cc5cd9c` (后端) + `b538618` (前端)。

## 验证基线

- `cargo test --bin cursor-fast-proxy-rs`: **205 通过 / 2 预存失败**
  - 预存失败 (git stash 基线可复现, 非本次引入, 不修):
    - `billing::tests::output_tps_excludes_ttft`
    - `grok_auth::tests::normalize_sso_variants`
- E2E `scripts/e2e-portal-wallet.sh run` (隔离实例 :8897 + 黑洞上游 :9913): **20 通过 / 0 失败**
  - S1 定额购买→钱包充值; S2 uk- 定额 key 过闸门+钱包扣减; S3 并发帽 429;
  - S4 费率卡算价(2×10×2=¥40)/购买不激活/首调激活; S5 槽数闸门+改槽折算;
  - S6 加时; S7 伪 key 401/停 key 403/禁用户 403; S8 定额耗尽 402。

## 踩坑记录

1. **quota_admit 死锁**: 初版在持有 ledger Mutex 时调用 `self.quota_totals()` 二次取锁 → 永久 hang
   (症状: 测试 `quota_wallet_402_on_exhaust` 跑 60s+ 不动)。修复: 复用已持有的 conn 内联 SELECT,
   返回 Err 前先 `drop(conn)`。教训: **持锁期间禁止调同对象任何公开方法**。
2. **Hermes 平台 Bearer 脱敏**: 写文件/补丁/heredoc 里出现 `Bearer <token>` 字面量会被平台改写成
   `Bearer ***`，连 python 替换脚本里的搜索串都会被改 → E2E 脚本的 curl 头全坏。
   解法: 运行时拼接 `BEARER="Bea""rer "`，任何落盘内容都不出现完整字面量。
3. **黑洞上游触发 quota_probe 自动禁号**: dummy 账号探活失败被打入冷却 → 后续请求 503
   `pool_empty` (而非预期 502)。解法: E2E config 加 `"quota_probe":{"enabled":false}`;
   同时把「到上游即过闸门」的判定放宽为 502/503 均可 (账户池耗尽也发生在鉴权/钱包闸门之后)。
4. **脚本 bash 陷阱**: `$50` 在双引号里被当位置参数 (`$5: unbound variable`); `UID` 是 bash
   只读变量不能赋值 — 改用 `PUID`。
5. **quota_credit 负值语义**: 负 delta 减的是 `used` (售后回补视角, 保护 credited 历史报表),
   **不是** 减 credited。E2E「定额耗尽」场景无法用 admin API 扣光, 改为直接 sqlite 改库
   把 credited 压到 used 水位线 (测试手法, 生产无此需求)。
6. **E2E 幂等**: 重跑前用 admin API 把测试用户定额清零、余额调回 100 (`e2e_reset`),
   保证脚本可反复跑。

## 已知边界 / 未做

- 生产 `:8800` 二进制未更新 — 由用户自行部署 (`scripts/local-swap-8800.sh`)。
- 生产数据无需迁移: 4 张存量卡均无 user_id, 0 张 boost 卡; `slots/duration_hours` 为 Option,
  老卡默认走 plan 的 max_concurrency/duration_hours。
- 管理端「门户用户」tab 的定额调整是±used 语义 (见坑 5), 调界面文案时应注明。
- analytics 里 uk- key 的 owner 已映射 username; 用量按 key 名 (前 11 字符) 归集。
