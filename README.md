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
- 管理面板「计费账单」：按天 / 小时 / key / 销售 / 模型 / 标签 / 账号汇总，明细分页，标签 / 模型通配 / 销售 / 状态 / 自由搜索筛选，CSV 导出，价格与销售在线编辑热生效。
- 接口：`GET /admin/api/billing/{summary,records,export,tags,stats}`，`GET|POST /admin/api/billing/pricing`。时间参数 `from` / `to` 接受 `YYYY-MM-DD`、`YYYY-MM-DD HH`、`YYYY-MM-DDTHH:MM` 或 unix 秒，按 `billing.tz_offset_minutes` 解释；`to` 为闭区间（`to=2026-08-30` 含当天）。
- `billing.reject_unpriced=true` 时未匹配价格的模型直接 402，避免漏计费。

## IP 代理池

`config.json` 的 `proxy` 段：节点列表 + 自动分配规则。热更新，不必重启。

- 手动：账号字段 `proxy_id` 钉死出口，优先于规则。
- 自动：`hash`（同号粘性）/ `round_robin` / `least_accounts` / `exclusive`（一号一 IP，`max_accounts` 默认 1）。
- 规则按 `priority` 从高到低匹配账号前缀或标签。
- 管理页「IP 代理池」：节点健康、出口 IP、延迟、绑定数；「探测全部」走 CONNECT + ipify；「按规则重绑」只动未手动指定的号。
- 接口：`GET|POST /admin/api/proxies`，`POST /admin/api/proxies/probe`，`POST /admin/api/proxies/rebalance`。
- 超大号池：按区域/标签分组看节点，不要一次渲染全部账号；探测是后台 50 并发额度探测 + 代理探测，失败节点自动踢出自动分配。

