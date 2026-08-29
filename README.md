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

## 不要提交的文件

`config.json`、`accounts.json`、`usage.json`、`*.log` 含密钥或运行数据，已在 `.gitignore` 中。
