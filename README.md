# cursor-proxy

Rust 实现的 Cursor API 网关：多账号号池、管理面板、请求日志与用量统计。

## 快速开始

```bash
cp config.example.json config.json
cp accounts.example.json accounts.json
# 编辑 config.json / accounts.json，填入真实 token，不要提交这两个文件
cargo build --release
./target/release/cursor-fast-proxy-rs
```

默认监听 `0.0.0.0:8800`。管理面板：`/admin`（需要 `admin_token`）。

默认模型 `kimi-k3`（Cursor 实测 ~1,048,576 上下文）。客户端不传 `max_tokens` 时注入 32,768 长输出预算，上游超时默认 1800s。计费币种统一 `RMB`（`CNY`/`人民币` 写入时归一）。

原生协议：

- Claude Code：`POST /v1/messages`（`x-api-key` 或 Bearer；`tools` / `tool_use` / `tool_result`）
- Codex：`POST /v1/responses`（`input` 里的 `function_call` / `function_call_output`）
- OpenAI Chat：`POST /v1/chat/completions` 同样转发 `tools` / `tool_calls`

不支持托管 `web_search`（Cursor 无对应能力，直接 400）。

蒸馏缓存：同一 `user` 或 `X-Session-Id` 会粘到同一号，并把 Cursor `conversationId` 做成稳定 UUID v5。共享前缀的多轮才能命中 `cache_read`。每号默认并发 **5**（128G 上可热改，别开到 20，容易 429）。

## 128G 机器稳定运行

网关本身常驻只有十几 MB，真正会拖垮 128G 主机的是无上限日志、SQLite WAL、畸形上游帧、以及 systemd 无内存/重启上限。默认已做：

1. Tokio 固定 16 worker（不跟 32 核一对一膨胀）
2. `proxy.log` 超过 1GiB 轮转，保留 4 代（`CFP_LOG_MAX_BYTES` / `CFP_LOG_KEEP`）
3. 计费库每 60s `wal_checkpoint(TRUNCATE)`，打开时 `wal_autocheckpoint=1000`
4. Connect 帧 / 解码缓冲上限 16MiB，畸形长度不再把进程打爆
5. 部署单元 `deploy/cursor-proxy.service`：`MemoryMax=16G`、`MemoryHigh=4G`、`Restart=always`、`StartLimitBurst=5`、`LimitNOFILE=65535`

拷到 128G 机：

```bash
install -m 644 deploy/cursor-proxy.service ~/.config/systemd/user/cursor-proxy.service
systemctl --user daemon-reload
systemctl --user restart cursor-proxy
```

## 不要提交的文件

`config.json`、`accounts.json`、`usage.json`、`*.log` 含密钥或运行数据，已在 `.gitignore` 中。

## 计费

- 每个 API key 可配 `tags`（标签）和 `sales_id`（归属销售）；`billing.prices` 为模型价格表（每 1M tokens，`model` 支持精确名 / `prefix*` / `*` 兜底，匹配优先级 精确 > 最长前缀 > `*`）；`billing.sales[].commission_bps` 为分成万分比。
- 账本写入 `billing.db`（SQLite WAL）。金额以整数纳（1e-9）存储，每条记录快照当时单价与分成比例，`req_id` 唯一不重复计费。改价只影响之后的请求。
- 管理面板「计费账单」：按天 / 小时 / key / 销售 / 模型 / 标签汇总，明细分页，标签 / 模型通配 / 销售 / 状态 / 自由搜索筛选，CSV 导出，价格与销售在线编辑热生效。
- 接口：`GET /admin/api/billing/{summary,records,export,tags,stats}`，`GET|POST /admin/api/billing/pricing`。时间参数 `from` / `to` 接受 `YYYY-MM-DD`、`YYYY-MM-DD HH`、`YYYY-MM-DDTHH:MM` 或 unix 秒，按 `billing.tz_offset_minutes` 解释；`to` 为闭区间（`to=2026-08-30` 含当天）。
- `billing.reject_unpriced=true` 时未匹配价格的模型直接 402，避免漏计费。

## 套餐卡（无限畅饮 / 定额卡，与 token 计费并行）

卡是独立的 `card-` 前缀 key，命中后走卡闸门（fail-closed），不碰主 api key 体系。两种套餐类型：

- **无限畅饮** `kind=unlimited`：时长内不限次数，控成本的三件事 = 并发上限 + 输出匀速 + 行为评分。
- **定额卡** `kind=quota`：面值 `face_usd`（官方口径 $），用完 402，余额持久化在 `cards.json`，7 天有效，不限速。

### 成本模型（Grok Heavy，2026-09-05 官方仪表盘实测）

- ¥130/号；高级额度 **$1000/周重置**（不是按月）；经 Cursor 走的 grok 也吃这个池，没有独立 grok 额度可卖。
- 号只用 2 周 → ¥130 / $2000 = **¥0.065 / 面值 $**。可在 `POST /admin/api/cards/cost-model` 调整（号价 / 周额度 / 可用周数），利润报表随之重算。
- 额度按**官方口径**价格表折算（`src/cards.rs::PREMIUM_PRICES`，与 `GetAggregatedUsageEvents` 对账误差 ≤5%）：`-fast` 变体 ×2；kimi-k3 是 $3/$15，不是旧 pricing.json 的 $20/$100。

### 限速：匀速流出（pacing），不注入延迟、不截 max_tokens

截输出会让 agent 客户端重试，越限越贵（实测 fable $/h +30% / +91%）。现在只有两个动作：

| 档位 | 触发 | 并发 | 输出匀速 (默认) |
|---|---|---|---|
| normal | — | `max_concurrency` | `pace_normal_tps` 25 tok/s |
| soften | load ≥ `soften_ratio` 或 行为评分 ≥ 阈值×0.6 | 减半 | `pace_soften_tps` 18 |
| degraded | load > 1.0 或 行为评分 ≥ `abuse_score_threshold` | `degraded_concurrency` | `pace_degraded_tps` 12 |

`load = max(日额度比 daily_quota_usd, 次数比 fair_use_rpd)`。人类阅读 ~6 tok/s，12 tok/s 仍无感；上游 34–48 tok/s 的突发被 `TokenPacer` 令牌桶（1s burst，首帧不等）匀成目标速率，完整内容一字不少。

### 行为评分（脚本识别，0–100）

用 8.6 天 / 61k 条真实流量校准：个人用户全天 0–25 分，二道贩子脚本 55–85 分。信号：活跃 5 分钟格数（≥200/288 +30）、秒接率（上条结束→下条到达 <3s 占比 ≥40% +30）、20h+ 无 30min 停顿（+25）、日消耗 ≥$250（+15）。按自然日滚动，全 atomic 无锁。`GET /admin/api/cards/:key` 返回 `abuse.score/reasons`。

### 模型分层（按 $/h 烧速，不按单次价）

`tier`: `economy`（grok / opus-5 / gpt-5.6-sol / kimi-k3-high ≤ $12/h）· `standard`（+ fable-5 / kimi-k3-max / opus-5-fast ≤ $24/h）· `flagship`（+ fable-5-1-thinking）。卡 tier ≥ 模型 tier 才放行，越级 403。未知模型按 flagship。

### 预置套餐（`POST /admin/api/cards/plans/seed`）

| id | 售价 | 类型 | 层级 | 并发 |
|---|---|---|---|---|
| day-eco-1 / -2 | ¥19.9 / ¥35.9 | 畅饮 24h | economy | 1 / 2 |
| day-std-1 / -2 | ¥34.9 / ¥62.9 | 畅饮 24h | standard | 1 / 2 |
| day-pro-1 / -2 | ¥49.9 / ¥89.9 | 畅饮 24h | flagship | 1 / 2 |
| quota-50 / 200 / 500 / 1000 | ¥15 / ¥49 / ¥99 / ¥169 | 定额 7 天 | 不限 | 4 |

### 接口

- 套餐：`GET|POST /admin/api/cards/plans`（POST 全字段可选，缺省沿用旧值 → 可只改一个 `price`）、`DELETE .../plans/:id`、`POST .../plans/seed[?overwrite=1]`、`GET .../presets`
- 成本：`GET|POST /admin/api/cards/cost-model`、`GET /admin/api/cards/pricing-table`（官方价 + 层级对照）
- 卡：`GET /admin/api/cards`（实时档位 / 匀速 / 评分 / 余额 / 今日成本¥）、`POST .../issue {plan_id, owner, count, paid_rmb?}`、`GET|DELETE .../:key`、`POST .../:key/extend|revoke`
- 报表：`GET /admin/api/cards/report`（按卡 token 汇总）、**`GET /admin/api/cards/profit?group=plan|card|day&from=&to=`**（收入 = 卡实收 `paid_rmb` 按开卡日；成本 = 逐条按官方价重算面值 × ¥/$；输出毛利、每卡平均成本、按模型拆分）
- 请求路径错误码：401 伪卡 / 402 过期或定额用尽 / 403 吊销·越级模型·前缀不符 / 429 超并发或 RPM

定价推演全文：`docs/day-card-pricing-20260905.md`。

## IP 代理池

`config.json` 的 `proxy` 段：节点列表 + 自动分配规则。热更新，不必重启。

- 手动：账号字段 `proxy_id` 钉死出口，优先于规则。
- 自动：`hash`（同号粘性）/ `round_robin` / `least_accounts` / `exclusive`（一号一 IP，`max_accounts` 默认 1）。
- 规则按 `priority` 从高到低匹配账号前缀或标签。
- 管理页「IP 代理池」：节点健康、出口 IP、延迟、绑定数；「探测全部」走 CONNECT + ipify；「按规则重绑」只动未手动指定的号。
- 接口：`GET|POST /admin/api/proxies`，`POST /admin/api/proxies/probe`，`POST /admin/api/proxies/rebalance`。
- 超大号池：按区域/标签分组看节点，不要一次渲染全部账号；探测是后台 50 并发额度探测 + 代理探测，失败节点自动踢出自动分配。

