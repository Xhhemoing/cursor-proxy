# 零售版门户重构计划书 — 钱包定额 + 用户 Key + 畅饮卡按时长×并发计价

日期：2026-09-08 ｜ 状态：**待审批** ｜ 前置：`docs/portal-design.md`（P0–P2 已上线）

---

## 1. 最终目标

用户在 `/portal` 自助完成闭环：

1. **两类套餐**
   - **定额卡**：购买后直接把面值（官方口径 $）充进用户钱包的「定额池」，多次购买累加。
   - **限时畅饮卡**：购买后不激活（首次调用或手动激活才开始倒计时）；单卡价格 = 费率 × 时长 × 并发槽。
2. **用户能力**
   - 钱包页：余额（¥）、定额池（$ 已充/已用/剩余）、拥有套餐一览、流水。
   - 调用 Key 管理：创建/命名/停用/删除自己的 key；**同一个 key 同一时刻只能选「定额」或「一张畅饮卡」**，可随时切换绑定。
   - 畅饮卡操作：激活、加时（按时长补款）、增减并发槽（增槽按剩余时间折算扣款）、把卡绑到任意自己的 key。

## 2. 现状基线与差距

| 已有（生产在跑） | 差距 |
|---|---|
| 登录/余额/流水/邀请/用户组/组白名单（P0–P2） | 无「定额池」概念，余额只有 ¥ |
| CardPlan 双类型（Unlimited/Quota）、叠加、子套餐、first_call | Quota 购买是「发卡」，不是「充钱包」；畅饮只有固定价，无费率模式 |
| 卡即 key（`card-` 直接当 Bearer） | 用户没有自己的 key；无法「一key 选一源」、无法自由换绑 |
| boost 临时加并发（按剩余折算） | 以「子卡」实现，不能减槽；生产 0 张 boost 卡，可重构 |
| B1 在途预扣、B4 SQLite 账本、B7/B8 限速/车道 | 热路径模式可直接复刻到定额池 |
| 管理端：套餐表单（含门户扩展字段）、门户用户页、卡列表 | 套餐表单缺费率字段；用户页无定额展示/手动调定额 |

**生产数据快照（重构自由度来源）**：8 套餐（4 quota + 4 unlimited，其中 1 个是测试套餐 `111`）、4 张卡全部无 user_id、0 张子卡/boost 卡、2 个测试用户（test1 ¥45.51 / test ¥60）。→ **无需数据迁移，语义可改。**

## 3. 核心设计决策（含推理，非选择题）

**D1 定额 = 用户级 $ 钱包，不是卡。**
「购买后直接增加可用额度」的字面语义就是池子。钱包池让「key 选定额」天然成立（不用选某张卡），多次购买自动累加。实现复刻 B4：`cards.db` 加 `user_quota` 表（credited_micro / used_micro 单行 upsert），热路径不写 JSON。

**D2 畅饮卡 = 卡对象 + 费率套餐。**
CardPlan 增加「费率模式」：`price_per_lane_hour > 0` 时，购买页出现时长/并发滑块，价格 = 费率 × 小时 × 槽；`max_concurrency` 复用为槽数上限。旧固定价模式（`price` + `duration_hours`）保留，一键购买不变。

**D3 购买不激活 = first_call 语义落地到发卡。**
门户购买的畅饮卡一律 `expires_at = 0`（未激活）；首次调用时 `admit()` 自动激活（`expires_at = now + 时长`），门户另有「立即激活」按钮。`first_call` 字段 P1 已存在，只需让 `issue_card` 尊重它。admin 手工开的卡维持购买即计时。

**D4 用户 Key 独立模型，`uk-` 前缀，门户可见完整 key。**
`PortalKey { key, user_id, name, mode: quota|card, card_key?, max_concurrency, enabled }` 存 `portal.json`。自己的 key 自己看全文（与 admin keys 的 reveal 一致，风险可接受，体验远好）。推理路径解析顺序：`card-` 前缀（旧卡直通，保留）→ config.api_keys → 门户 key → 401。

**D5 增/减并发 = 改卡的 `slots`，废掉 boost 子卡。**
Card 加 `slots: Option<u32>`（None = 用套餐值）。增槽：费率 × Δ槽 × 剩余小时 扣余额；减槽：立即生效不退款（v1 决策，防薅；退款通道 = admin 手动加余额）。`/portal/api/boost` 删除（生产 0 张子卡，无迁移负担），前端同步改。

**D6 定额池 v1 不设有效期。**
池化后「168h 有效」无从挂载（每笔 credit 独立过期实现复杂、收益低）。旧 quota 套餐的 duration_hours 在门户隐藏。后续要加可再做 credit 级过期。**这是与旧语义唯一的行为偏差，特此标出。**

**D7 定额 key 的并发帽用户自调、免费。**
定额按量计费，并发帽只是防爆保护，不影响收入 → 用户在 key 上自由设 1–8（硬顶 8，常量），复用 AppState.key_semaphores。

**D8 一张畅饮卡可绑多个 key。**
车道（in_flight）本来就是按卡计的，多 key 共享一卡 = 共享槽数，语义自然、零额外代码。

## 4. 数据模型变更

### 4.1 `cards.db`（SQLite，open_ledger 迁移）

```sql
CREATE TABLE IF NOT EXISTS user_quota (
    user_id TEXT PRIMARY KEY,
    credited_micro INTEGER NOT NULL DEFAULT 0,   -- 累计充值 (官方$ ×1e6)
    used_micro INTEGER NOT NULL DEFAULT 0,       -- 累计消耗
    updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS user_quota_log (      -- 充值/调整流水 (消耗不逐条记)
    id INTEGER PRIMARY KEY,
    user_id TEXT NOT NULL,
    delta_usd REAL NOT NULL,                     -- 正=充入
    kind TEXT NOT NULL,                          -- purchase | admin_adjust
    ref_id TEXT,                                 -- 套餐 id
    ts_ms INTEGER NOT NULL,
    operator TEXT NOT NULL
);
```

### 4.2 `portal.json` — PortalData 加字段

```rust
pub struct PortalKey {
    pub key: String,                    // "uk-" + 24 hex, 唯一
    pub user_id: String,
    pub name: String,
    pub mode: KeyMode,                  // quota | card
    pub card_key: Option<String>,       // mode=card 时绑定的畅饮卡
    pub max_concurrency: u32,           // quota 模式并发帽 (1..=8), card 模式忽略
    pub enabled: bool,
    pub created_at: u64,
}
// PortalData { users, groups, keys: Vec<PortalKey>, next_user_seq }
```

### 4.3 CardPlan 加字段（serde default，旧配置无损）

```rust
pub price_per_lane_hour: f64,   // >0 = 费率模式 (¥/槽·时); 0 = 固定价模式
pub min_hours: u64,             // 费率模式最少购买小时, 默认 1
// 复用: max_concurrency = 费率模式槽数硬顶; face_usd/price 不变
```

### 4.4 Card 加字段

```rust
pub slots: Option<u32>,          // None = 用 plan.max_concurrency
pub duration_hours: Option<u64>, // 费率模式购买的时长; 激活时 expires_at = now + 该值
// expires_at = 0 表示未激活; is_expired 改为 expires_at > 0 && now >= expires_at
```

## 5. 端点变更

### 用户端（`/portal/api/*`，Bearer session）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/balance` | **扩展**：返回 `balance_rmb` + `quota: {credited_usd, used_usd, remaining_usd}` + 合并流水（balance_log ∪ user_quota_log） |
| GET | `/plans` | **扩展**：plan JSON 带 `price_per_lane_hour / min_hours / max_concurrency / billing_mode` |
| POST | `/purchase` | **扩展**：`{plan_id, hours?, slots?}`。kind=quota → 充钱包；费率模式 → 校验 hours/slots、算价、发卡（未激活）；固定价 → 现有流程（含叠加模态） |
| GET | `/keys` | **新增**：我的 key 列表（含绑定卡状态/定额剩余快照） |
| POST | `/keys` | **新增**：创建 `{name, mode, card_key?, max_concurrency?}` → 返回完整 key |
| POST | `/keys/:key` | **新增**：改 `{name?, mode?, card_key?, max_concurrency?, enabled?}` —— 换绑/换模式走这里 |
| DELETE | `/keys/:key` | **新增**：删除（不可逆，前端二次确认） |
| GET | `/cards` | **扩展**：带 `slots / activated / duration_hours / 已绑定的 key 名` |
| POST | `/cards/activate` | **新增**：`{card_key}` 立即激活 |
| POST | `/cards/extend` | **新增**：`{card_key, hours}` 加时；价格 = 费率 × 当前槽 × Δh；已过期卡从 now 重基 |
| POST | `/cards/slots` | **新增**：`{card_key, slots}` 增减槽（D5） |
| ~~POST `/boost`~~ | | **删除**（由 slots 取代） |

### 管理端（`/admin/api/*`）

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/portal/users/:id/quota` | **新增**：`{delta_usd, reason}` 手动调定额（售后/补偿通道，与「余额仅 admin 手动加」同哲学） |
| POST | `/cards/plans` | **扩展**：透传 `price_per_lane_hour / min_hours`（upsert 共享 CardPlan，改动小） |
| GET | `/cards` | list_status JSON 加 `user_id / slots / activated` |

### 推理路径（`src/main.rs`）

- `inference_handler_inner`：api_keys 未命中后再查门户 key：
  - `mode=quota` → 用户 enabled 检查 → `card_store.quota_admit(user_id, est_in_micro)`（钱包版 B1 预扣，不足 402）→ key 信号量（`max_concurrency`）→ 放行；5 个 settle 点挂平行钩子：`quota_settle(user_id, …)`（hold 转 used，复用 settle 内同一官方价目函数）。
  - `mode=card` → 用户 enabled 检查 → 直接 `card_store.acquire(bound_card_key, …)`，后续与 `card-` 路径完全同码。
- `models_handler`：门户 key 分支——card 模式按套餐 model_groups/前缀过滤；quota 模式不限（v1）。
- 账本 `key_name` 记 `uk-` token 全文（与卡一致），消耗分析按 key 即出；analytics owner 映射加 portal key → username。

## 6. 门户前端（`static/portal.html`）

1. **首页**：统计行加「定额剩余 $」与「我的 Key 数」；有效套餐卡片区显示未激活卡（灰标「未激活」）。
2. **购买套餐**：分区「定额卡」（卡片：面值/售价/说明，一键充钱包）与「畅饮卡」（费率套餐卡片 → 弹窗：时长滑块 × 槽滑块，实时价 = 费率×h×槽；固定价套餐维持现状+叠加模态）。
3. **新页「我的 Key」**：创建（起名+选模式）、列表（key 全文+一键复制/模式徽标/并发帽编辑/绑定卡下拉/停用/删除）。
4. **我的套餐**：列 = 套餐/状态(未激活·生效中·已过期)/剩余时间/槽数/已绑 key；操作 = 激活 / 加时 / 增减槽 / 绑到 key。
5. **余额页**：¥ 余额 + 定额池（已充/已用/剩余）+ 合并流水。

## 7. 管理端（`static/admin.html`）

- 套餐表单：费率模式字段行（`price_per_lane_hour` / `min_hours`；kind=unlimited 时显示），quota 隐藏时长。
- 门户用户页：加「定额 $」列 + 「调定额」按钮。
- 卡列表：加 用户/槽数/激活状态 三列（td 数 14→17 同步改）。

## 8. 测试与验收

1. **单测**（`cargo test --bin cursor-fast-proxy-rs`，现行 192 过 2 个预存失败只记不修）：
   - 钱包：充值累加/settle 扣减/402 边界/hold 释放（panic 路径 Drop）。
   - key CRUD、换绑排他（同 key 单绑定）、用户禁用 → key 全拒。
   - 费率算价、slots 增减折算、extend 重基、激活语义（expires_at=0 → 首调激活）。
   - 旧卡直通回归（`card-` bearer 不受影响）。
2. **E2E**（新 `scripts/e2e-portal-wallet.sh`，8899 黑洞模式，502=过闸）：
   建用户→加余额→买定额→建 quota key→调用→钱包扣减→买费率畅饮→建卡 key→绑卡→首调激活→并发闸门→增槽扣款→换绑→余额不足 402。
3. **交付规则**：代码 + 单测，**不编译 release 不替换 :8800**（你另行部署）；复盘记 `docs/portal-wallet-review.md`。

## 9. 阶段拆分（每阶段独立可验收）

| 阶段 | 内容 | 估时 |
|---|---|---|
| S1 | 数据模型 + 钱包账本 + key CRUD + cards.rs 闸门/算价 | 1 天 |
| S2 | 推理路径接入 + models 过滤 + settle 钩子 + analytics owner | 半天 |
| S3 | 门户前端三页改版（钱包/购买/Key/套餐操作） | 1 天 |
| S4 | 管理端（套餐费率字段/用户定额/卡列表列） | 半天 |
| S5 | E2E 脚本 + 复盘文档 | 半天 |

## 10. 风险

| 风险 | 缓解 |
|---|---|
| 热路径每请求查门户 key | DashMap 直查，O(1)，与 api_keys 同级 |
| quota hold 泄漏 | RAII guard（Drop 释放），复刻 CardPermit 模式 |
| f64 ¥ 与 micro-$ 换算漂移 | 充值/扣款全部 micro 整数；展示层 round 2dp |
| 减槽不退款引争议 | 前端明示「减槽不退款」；申诉出口 = admin 手动加余额 |
| 费率定价待校准 | 表单只暴露费率字段；具体数值参考成本模型（¥/槽·时）你定，建议起步 ¥20/槽·天 ≈ ¥0.83/槽·时 |

---

**待审批。批准后按 S1→S5 实施。**
