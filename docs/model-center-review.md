# 模型中心改造 — 待复盘清单 (2026-09-08)

施工原则: 每部分写完即跑测试, 失败/疑点不就地停留, 记到本文档, 最后统一复盘。

## 待复盘项

### 1. [预存·非本次] billing::tests::output_tps_excludes_ttft 失败
- 位置: src/billing.rs:893 `Option::unwrap()` on None
- 证据: 该测试在我改动前就失败 (billing.rs 未被我触碰, git diff HEAD 为空)
- 细节: `BillingRecord::build(...).with_ttft(Some(5000))` (ttft==latency) 期望 output_tps=40 但得 None
- 归属: 653180a (请求日志提质) 引入的 tps 钳制逻辑, 测试与实现已漂移
- 建议: 单独修, 与本任务无关

### 2. [预存·非本次] grok_auth::tests::normalize_sso_variants 失败
- 位置: src/grok_auth.rs:501, left="xyz" right="abc"
- 证据: grok_auth.rs 是并行智能体的未跟踪新文件, 我从未改动
- 归属: 并行智能体
- 建议: 通知该智能体或用户

### 3. [设计妥协] gpt-5.5-extra 过 404 闸门 (known=true)
- 现象: route-preview?model=gpt-5.5-extra → gate: pass
- 原因: 内置价目表有 `gpt-5.5` 行, builtin_model_known 前缀匹配命中 → 闸门放行
- 缓解: 它**不在**客户菜单 (visible_models 只认上游), 幽灵不会主动暴露; 真被请求时会原样透传给上游报 404/502, 按内置价计费 (与旧行为一致)
- 待办: 是否要把「内置价目前缀命中」从 404 闸门里剔掉, 只留「上游 ∪ 注册表 known ∪ 别名」? 这会让所有未拉上游时的内置模型冷启动 404 —— 需要用户拍板

### 4. [已验证] 家族合并 grok-4.6
- cursor-grok-4.6 (336 req) + grok-4.6 (955 req) 已合并为 grok-4.6 一族 1291 req $56.15
- 但 grok / grok-2 / grok-beta / grok-vision 各 1 次请求的化石家族仍列着 (upstream_missing=true 红标)
- 待办: 这些化石家族是否要在面板里默认折叠/隐藏? 目前只是红标

### 5. [已验证] 客户菜单 52 项
- /v1/models 实返 52 (含 grok-4.6 ✓, kimi-k3 ✓, claude-opus-5-fast ✓, 无 gpt-5.5-extra ✓)
- 旧版 108 项里的 23 个基名 + 84 真名 → 现在 52 项 (基名+fast 折叠 + 别名)
- 待办: 用户确认 52 项是否符合预期 (可用菜单形态 all 展开)

### 6. [未验证] admin.html 浏览器实测
- 已做: node --check 语法 OK; getElementById 引用全部存在; 服务端 serve 的 JS 语法 OK
- 未做: 浏览器真实渲染 (Camofox 不在); 家族 tab 内联下拉、路由测试 tab 的交互未点过
- 待办: 用户部署后开 /admin#model-center 实测, 或我这边起 headless 浏览器

### 7. [待办] e2e-models-visibility.sh 可能红
- 该脚本断言「kimi-k3-low 未拉上游时不可见」—— 我的 visible_models 仍遵守上游门, 应该兼容
- 但脚本还断言 opus-5/fable-5 别名不可见 —— 兼容
- 未跑 (需要隔离实例+特定数据), 建议部署前在 dev 实例跑一遍 scripts/e2e-models-visibility.sh

### 8. [性能] /admin/api/models/families 全量扫描 30d 账本
- 实测 dev 实例 (24749 行/30d) 响应 < 2s, 可接受
- 但无缓存: 每次切 tab 都重扫; 若账本涨到 10 万行级可能变慢
- 待办: 需要时加 60s 缓存 (仿 analytics_price_map 的 price_cache 模式)

## 已验证通过 (无需复盘)
- cargo check 0 error
- cargo test models:: 14/14, cursor:: 17/17, admin:: 3/3
- 全量 192 passed / 2 failed (均为预存, 见 #1 #2)
- dev 实例 :8891 实测: families 51 族 / route-preview 全路径 (基名/fast折叠/grok前缀/家族规则) / family-rule CRUD / /v1/models 52 项
- JS 语法 + id 引用完整性
