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

## 128G 机器稳定运行

网关本身常驻只有十几 MB，真正会拖垮 128G 主机的是无上限日志、SQLite WAL、畸形上游帧、以及 systemd 无内存/重启上限。默认已做：

1. Tokio 固定 16 worker（不跟 32 核一对一膨胀）
2. `proxy.log` 超过 512MiB 轮转，保留 3 代（`CFP_LOG_MAX_BYTES` / `CFP_LOG_KEEP`）
3. 计费库每 60s `wal_checkpoint(TRUNCATE)`，打开时 `wal_autocheckpoint=1000`
4. Connect 帧 / 解码缓冲上限 16MiB，畸形长度不再把进程打爆
5. 部署单元 `deploy/cursor-proxy.service`：`MemoryMax=8G`、`Restart=always`、`StartLimitBurst=5`、`LimitNOFILE=65535`

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
