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

## 2026-09-05 14:20 UTC — 本机 8800 智能思考强度路由 (a463094)
- 备份: cursor-fast-proxy-rs.bak-smart-20260905-141*
- 换后 /proc/PID/exe md5 f6139c8443897247eaa718cba4f7981a == target/release
- 实测: /v1/models 213 → 37 家族基名; 路由日志 proxy.log 实锤
  claude-opus-5+low→-low, +high→-high, gpt-5.6-sol→-low, 变体全名透传, 全 200
- 注意: 智能路由依赖内存里的上游名单, 重启后需重新点「获取可用模型」;
  名单为空时行为同旧版 (原样透传, 不路由)

## 2026-09-05 15:05 UTC — 本机 8800 删除 tier 机制 (9cbb3e2)
- 备份: cursor-fast-proxy-rs.bak-untier-20260905-145*; cards.json/models.json .bak-untier-*
- 换后 /proc/PID/exe md5 53c1636229288068523164f87847413a == target/release
- 实测: /v1/models 37 家族基名; 5 个存量套餐 (quota-*/day50) 无 tier 字段、
  预览 37 模型 0 警告; claude-opus-5+reasoning_effort=low → claude-opus-5-low 200
- 旧 cards.json/models.json 里的 "tier" 键 serde 忽略, 无迁移; 面板需硬刷新
- 预置套餐改为 day-1/day-2/day-4 (纯并发档, 无层级)

## 2026-09-05 16:52 UTC — 本机 8800 消耗分析 + 删计费账单 + 上游名单落盘
- 备份: cursor-fast-proxy-rs.bak-analytics-20260905-165225; cards.json/config.json/billing.db .bak-analytics-20260905-165225
- 换后 /proc/PID/exe md5 b3da388dff40fb641af126127c778dfd == target/release (无 deleted)
- 删: /admin/api/billing/* 6 端点 + 面板「计费账单」页 + 价格规则/销售分成/reject_unpriced/币种 + key 的 sales_id
  (config.json 旧字段 serde 忽略, 无迁移; billing.db 保留 sales_id/commission 列恒 NULL/0)
- 账本 cost_nano 改为官方面值 (cards::model_price 同一张表), 中断流有 token 也计面值
- 新: /admin/api/analytics/{consumption,presence,sessions} + 面板「消耗分析」页 (在线状态/按模型/组/套餐/卡/时间轴)
- 新: upstream-models.json 落盘, 重启自动读回 → 不再需要重启后手点「获取可用模型」
- 隔离 E2E (真实 billing.db 副本, 1029 行): scripts/e2e-analytics.sh 19/19; cargo test 151/151
- 未做: 换后实时冒烟 (curl 读 admin_token 的命令被审批层拦, 交用户在面板核对)

## 2026-09-06 01:52 UTC — 本机 8800 并发槽消耗 + 修 promptTokens 含缓存双计
- 备份: *.bak-lanes-20260906-015245 (bin/cards.json/config.json/billing.db); 脚本 scripts/local-swap-8800.sh
- 换后 /proc/PID/exe md5 8202033a74c1a31723997b68dbea0a7e == target/release
- 算法: 同 key 请求按区间划分进并发槽 (assign_lanes), 每槽各自切时段 → lane_hours / peak_concurrency;
  主定价指标改 usd_per_lane_hour (面值 ÷ 槽·时); 套餐行加 满载上限 = $/槽·时 × 并发 × 时长
- Bug: Cursor extendedUsage.promptTokens 含 cacheRead+cacheWrite, 曾按 input 全价再算一遍 →
  sol $61.7→$9.0 (7×), fable-high $237→$56 (4×). translate::extract_usage 扣缓存; 账本加列
  input_incl_cache (老行 1 / 新行 0), 分析端归一化. 老行 cost_nano 与卡 face_used 历史值仍偏高 (未回写)
- 实测 (今日 1454 行): $/槽·时 fable-high 22.0 / fable-max 18.7 / sol 28.3 / grok-xhigh 5.5 / kimi-max 0.3;
  峰值并发 3, 均并发 1.2–1.5; day50 满载上限 $1313/卡
- 上游名单落盘实测: 点一次「获取可用模型」(212) → restart → /v1/models 仍 37 家族 (之前重启后只剩 1)
- 隔离 E2E scripts/e2e-analytics.sh 20/20 (加假号+黑洞 9911 才能验新行 input_incl_cache=0); cargo test 155/155

## 2026-09-06 03:00 UTC — 本机 8800 速度/延迟指标 (TTFT + tok/s)
- 备份 *.bak-speed-20260906-030031; md5 d17782f4dee3a1beafe7c9b0e2fa769a == target/release
- 账本加列 ttft_ms (流式: 首个内容帧到达; 老行 NULL); BillingRecord.output_tps = out ÷ (latency − ttft)
- analytics 每维度 speed{ttft_p50/p90, tps_p50/p10/p90, latency_p50/p90, 样本数}; presence today_speed; sessions 每段 ttft/tps
- 面板: 消耗分析各表 + 在线状态 + 时段明细 加「首字 p50/p90」「tok/s p50/p10」(<15 tok/s 标黄); 请求日志延迟列附首字/tok/s
- 隔离 E2E 22/22; cargo test 157/157. 换后真实流量 ttft 落库待流量到达后核对 (/tmp/chk_ttft.sh)
- 03:10 追加 ac28517: ttft 含 reasoning 帧; 换后 md5 95198551d32435a1807c16cc265b1497. 实测 fable-thinking-high 首字 7–15s (思考期无输出), 之后正文 90–180 tok/s 突发; sol-max 首字 5s / 78 tok/s

## 2026-09-06 UTC — 本机 8800 限速记录 + 速度画像/what-if + 修 inputTokens 含缓存
- 备份 *.bak-pace-rec-*; md5 见 swap 输出
- translate::extract_usage: Cursor 实际字段是 inputTokens (非 promptTokens), 同样含缓存 → extendedUsage 容器一律扣 cr+cw (input≥cr+cw 保护). 05 03:00–06 12:11 间写入的 2454 行 incl=0 但未扣 → 已 UPDATE 回 incl=1, 分析端归一化正确
- 账本加列 pace_tps / pace_wait_ms (每请求生效限速与累计 sleep); BillingRecord.upstream_tps 剔 sleep
- analytics: speed{upstream_tps_p50, paced_requests, pace_wait_hours, pace_lane_share}; simulate_pace what-if
- /admin/api/analytics/speed?paces=25,15,12,8 → 按模型 原生速度 + 各档限速后 $/槽·时 与省幅; 面板「消耗分析 → 速度/限速」tab
- 隔离 E2E 24/24; cargo test 163/163

## 2026-09-06 UTC — 本机 8800 面板可调风控 (RiskPolicy)
- 备份 *.bak-risk-*; md5 见 swap 输出
- 新 cards::RiskPolicy (cards.json.risk_policy 落盘, 热更新): 总开关 / 评分权重 ScoreWeights (格/秒接/跨度/日消耗 四类阈值+分值) /
  压制阈值覆盖 + 软化比例 / 全局 pace 三档覆盖 / 按模型前缀规则 (免限 或 单独 pace, 最长前缀优先, 兼容 cursor-) /
  长输出放开 (单请求 ≥N tok 后放开或降档) / 日面值硬帽 (超帽只放白名单前缀, 其余 429)
- 优先级: 模型规则 exempt > 模型规则 pace > 全局覆盖 > 套餐 pace_*; enabled=false → 全部 Normal 不限速
- API: GET/POST /admin/api/cards/risk-policy, POST .../preview (用今日各卡真实信号按候选参数重算档位, 不落盘)
- 面板: 套餐卡 → 「风控」tab (总控 / 权重 / 模型规则表 / 预演表 / 恢复缺省)
- 缺省策略 = 现行为 (权重同旧硬编码, pace 沿用套餐, relief 3000 tok 放开, 硬帽关) → 换后行为不变
- 隔离 E2E 30/30; cargo test 166/166

## 2026-09-06 13:17 UTC — 本机 8800 风控策略上线 + 便宜模型自动免限
- 12:56 用 scripts/apply-risk-recommended.sh 应用: 全局 pace 40/25/12, grok 规则免限, relief 3000 tok, 硬帽 $250 (白名单 kimi-k3/grok); 权重缺省
- 实测: grok 行 pace_tps=0 ✓; gemini-3.8-flash 行 pace_tps=40, pace_wait 25–35s (原生 3800 tok/s 被压到 37) → 便宜模型限了只伤体验
- 加 RiskPolicy.pace_min_output_price_per_m (缺省 0 关): 输出价 < $N/M 自动免限, 显式模型规则仍优先. 面板加字段. 设为 $10 → gemini/grok 免限, kimi($15)/sol/opus/fable 仍限
- 备份 *.bak-cheap-exempt-20260906-131736; md5 56af3f3b377f6894db611703573fc836; 166 tests
- 13:32 追加: 账本列 out_visible_est (流式可见输出估算); simulate_pace 用可见输出 (思考模型 output 含不流式的思考 token, 实测 fable-max 40 tok/s 限速下客户端仍 84–89 tok/s = 可见部分只占一半); speed tab 加「可见比」列. md5 4b93fb1c90f22f0711e68792423d35cd; 167 tests

## 2026-09-06 16:30 UTC — 本机 8800 修利润报表口径
- 利润报表 (/admin/api/cards/profit, 面板「套餐卡→利润」+ 概览「套餐卡利润」) 逐条重算面值时没按 input_incl_cache 扣缓存
  → day50 成本显示 ¥302 (消耗分析同数据 ¥72), 毛利 -1%. 现与 analytics::normalize_input 同口径
- md5 da45efffdce38f93f4147d4dbbeaf5ad; 167 tests

## 2026-09-07 02:30 UTC — 本机 8800 价格表二次校准 + pacer 补 tool_calls.arguments
- scripts/reconcile-official.py: 拉 6 号 GetAggregatedUsageEvents(startDate=周期起点) 逐模型对账 (官方 cents vs token×表价).
  按号最小二乘反推: claude-fable-5-1-* cache_read $0.25 (原 $1, 高估 fable-5-1 面值 30%); gpt-5.6-sol-max cr 0.75/cw 2.5 (原低估 20%);
  gpt-5.5 $5/$30/$0.5 (原走 gpt-5 低估一半). 其余 (fable-5, opus-5, kimi-k3, grok-4.6, gemini-3.8) ratio 1.00.
  修后: 6 号合计 8800+8791 两账本 = 官方 84% (差额 = 客户端直连 grok-bot-*/未经代理的 fable-5-thinking/opus-fast).
  md5 75b14d1360292d0e660827b9ad073be4 (prices2)
- TokenPacer::estimate_tokens 补 `"arguments":"` (OpenAI 工具调用参数增量). 之前 agent 写文件的输出全绕过限速:
  实测 fable-max 可见比 27%, grok-4.6 7% → pace 40 形同虚设. md5 4c5464174e4562ff8baff0567f050c5f (pace-args); 167 tests
- 待观察: 换后 out_visible_est/output_tokens 应升到 ≥60% (fable) / ≥90% (grok); pace_wait_ms 显著增大

## 2026-09-07 03:30 UTC — 本机 8800 去硬帽 + 价值比评分 + 申诉 + 压制粘滞
- 硬帽停用 (RiskPolicy.hard_cap_* 保留字段不生效, 面板移除). 替代: ScoreWeights.value_ratio_lo/hi (缺省 0.6→+10, 0.8→+20):
  价值比 = 今日成本¥ ÷ (paid_rmb ÷ 套餐天数). 超过付费只加分不拒绝.
- 申诉: 客户 POST /v1/appeal {"message"} / GET /v1/card/status (Bearer 卡 key, 不暴露权重); 管理 GET /admin/api/cards/appeals,
  POST /admin/api/cards/:key/appeal {approve,note,trust_hours}, POST /admin/api/cards/:key/trust {hours}. 批准 → 信任期评分不生效 + 清今日信号;
  拒绝 → 6h 内不可再提. 面板 套餐卡→申诉 tab (待审角标), 卡列表「信任24h/撤信任」.
- 压制粘滞 degraded_hold_secs (缺省 1800): 进压制后 30min 内不回落. 流式响应头 x-card-throttle / x-card-pace-tps.
- 策略落地 scripts/apply-risk-final.sh: fable/opus/sol 35/25/12, grok 70/40/20, gemini 80/60/30, kimi 免限; relief 3000; 无硬帽.
- md5 ccac12fc88a6dcc5ebb44e52727aa9a6; 170 tests; E2E 43/43
