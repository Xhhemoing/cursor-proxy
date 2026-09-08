# 用户门户系统设计 (2026-09-07)

## 需求确认

| 项 | 确认方案 |
|---|---|
| 门户路径 | `/portal` 独立页面，与 `/admin` 并行 |
| 注册 | 无自助注册，admin 在后台手动创建用户 |
| 登录 | 用户名或编号 + 密码 → session token（24h） |
| 用户界面 | 余额管理 / aff 邀请 / 购买套餐 / 查询 key / 套餐状态 |
| 后台 | 用户管理 / 用户组 / 套餐按组开放 |
| 余额 | admin 手动添加（无支付网关），用户查看余额和流水 |
| 无限套餐 | 也限时，计价单位（时间×槽）自由分配 |
| 多买叠加 | time_multiply（时间叠加）或 slot_multiply（槽位叠加） |

## 数据模型

### 用户表 `users.json`

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortalUser {
    pub id: String,                    // 用户编号 (如 "u1001")
    pub username: String,              // 登录名 (唯一)
    pub password_hash: String,         // bcrypt/argon2
    pub balance_rmb: f64,              // 余额 (元)
    pub aff_code: String,              // 我的邀请码 (唯一)
    pub invited_by: Option<String>,    // 谁邀请的我 (aff_code)
    pub group_id: Option<String>,      // 所属用户组
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default)]
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserGroup {
    pub id: String,
    pub name: String,
    pub note: String,
    /// 该组可见的套餐 id 白名单 (空 = 全部可见)
    #[serde(default)]
    pub plan_whitelist: Vec<String>,
}
```

### 余额流水 `balance_log` (cards.db 新表)

```sql
CREATE TABLE balance_log (
    id INTEGER PRIMARY KEY,
    user_id TEXT NOT NULL,
    delta_rmb REAL NOT NULL,          -- 正=充值, 负=消费
    balance_after REAL NOT NULL,
    reason TEXT NOT NULL,             -- "admin_add" / "purchase_plan" / "aff_reward"
    ref_id TEXT,                      -- 关联卡 key 或邀请记录
    ts_ms INTEGER NOT NULL,
    operator TEXT NOT NULL            -- "admin" 或用户名
);
```

### 邀请记录 `aff_rewards` (cards.db 新表)

```sql
CREATE TABLE aff_rewards (
    id INTEGER PRIMARY KEY,
    inviter_id TEXT NOT NULL,         -- 邀请人 user_id
    invitee_id TEXT NOT NULL,         -- 被邀请人 user_id
    reward_rmb REAL NOT NULL,         -- 返佣金额
    status TEXT NOT NULL,             -- "pending" / "paid"
    ts_ms INTEGER NOT NULL
);
```

### CardPlan 扩展字段

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CardPlan {
    // ... 现有字段 ...
    
    /// 额度 ($). 0 = 无限 (但仍限时)
    #[serde(default)]
    pub quota_usd: f64,
    
    /// 过期时间 (小时). 0 或 None = 不过期
    #[serde(default)]
    pub expire_hours: Option<u64>,
    
    /// 计时起点: purchase_time (购买即计时) | first_call (首次调用计时)
    #[serde(default)]
    pub billing_mode: BillingMode,
    
    /// 额外时间额度限制: 每 N 小时限 M $
    #[serde(default)]
    pub extra_limits: Vec<TimeQuotaLimit>,
    
    /// 子套餐 id 列表 (开通主套餐后才可选加购)
    #[serde(default)]
    pub sub_plan_ids: Vec<String>,
    
    /// 多买同套餐时的叠加方式
    #[serde(default)]
    pub stack_mode: StackMode,
    
    /// 仅对这些用户组开放 (空 = 全部)
    #[serde(default)]
    pub allowed_group_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BillingMode {
    #[default]
    PurchaseTime,
    FirstCall,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StackMode {
    /// 时间叠加: 2 张 7 天卡 = 14 天 1 槽
    #[default]
    TimeMultiply,
    /// 槽位叠加: 2 张 7 天卡 = 7 天 2 槽
    SlotMultiply,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeQuotaLimit {
    pub hours: u64,      // 时间窗口
    pub quota_usd: f64,  // 限额
}
```

### Card 扩展字段

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Card {
    // ... 现有字段 ...
    
    /// 所属用户 (portal user id)
    #[serde(default)]
    pub user_id: Option<String>,
    
    /// 首次调用时间 (billing_mode=first_call 时用)
    #[serde(default)]
    pub first_call_at: Option<u64>,
    
    /// 主套餐卡 key (子套餐卡指向主卡)
    #[serde(default)]
    pub parent_card_key: Option<String>,
}
```

## API 端点

### 门户用户端 `/portal/api/*`

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/portal/api/login` | 登录 `{username_or_id, password}` → `{token, user}` |
| POST | `/portal/api/logout` | 登出 |
| GET | `/portal/api/me` | 当前用户信息 |
| GET | `/portal/api/balance` | 余额 + 最近 20 条流水 |
| GET | `/portal/api/plans` | 可购买套餐列表 (按组过滤) |
| POST | `/portal/api/purchase` | 购买套餐 `{plan_id, stack_mode?}` → `{card_key}` |
| GET | `/portal/api/cards` | 我的卡列表 (状态/剩余额度/到期时间) |
| GET | `/portal/api/cards/:key` | 单卡详情 + 消耗统计 |
| GET | `/portal/api/aff` | 我的邀请码 + 邀请记录 + 累计返佣 |
| POST | `/portal/api/aff/apply` | 注册时填邀请码 (仅创建时可用) |

### 后台管理端 `/admin/api/portal/*`

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/admin/api/portal/users` | 用户列表 |
| POST | `/admin/api/portal/users` | 创建用户 `{username, password, group_id?, initial_balance?}` |
| PUT | `/admin/api/portal/users/:id` | 编辑用户 (禁用/改组/改备注) |
| POST | `/admin/api/portal/users/:id/balance` | 手动加余额 `{delta_rmb, reason}` |
| GET | `/admin/api/portal/groups` | 用户组列表 |
| POST | `/admin/api/portal/groups` | 创建/编辑用户组 |
| GET | `/admin/api/portal/aff/pending` | 待审批返佣列表 |
| POST | `/admin/api/portal/aff/:id/pay` | 审批返佣到账 |

## 页面结构

### 门户 `/portal` (新 HTML)

```
/login        → 登录页 (用户名/编号 + 密码)
/dashboard    → 首页 (余额卡片 + 我的套餐 + 快速购买)
/balance      → 余额管理 (当前余额 + 流水列表)
/purchase     → 购买套餐 (卡片式列表, 按组过滤, 显示叠加选项)
/cards        → 我的套餐 (状态/剩余/到期/续费)
/aff          → 邀请 (我的邀请码 + 邀请记录 + 返佣)
```

### 后台 `/admin` 新增 tab

```
门户管理
  ├─ 用户 (列表/创建/禁用/加余额)
  ├─ 用户组 (列表/创建/编辑套餐白名单)
  ├─ 邀请返佣 (待审批列表)
  └─ 门户设置 (返佣比例/默认组)
```

### 套餐配置界面重构

现在 CardPlan 有 20+ 字段平铺，改为分组：

```
基础信息
  ├─ id / 名称 / 售价 / 类型 (Daily/Weekly/Monthly/Quota)
  └─ 备注

限额
  ├─ 额度 $ (0=无限)
  ├─ 过期时间 (小时, 0=不过期)
  ├─ 并发上限 / RPM / 每日请求软上限
  └─ 额外限制 (每N小时限M$) [可添加多条]

模型访问
  ├─ 模型组 (复选)
  ├─ 模型前缀 (文本)
  └─ 仅对用户组开放 (复选)

高级
  ├─ 计时起点 (购买即计时 / 首次调用)
  ├─ 叠加方式 (时间叠加 / 槽位叠加)
  ├─ 子套餐 (多选)
  ├─ 风控 (限速三档 / 行为评分阈值)
  └─ 启用开关
```

## 技术选型

| 项 | 方案 | 理由 |
|---|---|---|
| 密码哈希 | `argon2` crate | 比 bcrypt 更现代, Rust 原生 |
| Session | `DashMap<token, (user_id, expire_at)>` 内存 + 24h TTL | 简单, 重启清空可接受 |
| 前端 | 单 HTML 文件 (同 admin.html 模式), `include_str!` | 与现有架构一致 |
| 路由 | hash-based (`/portal#/dashboard`) | 无需服务端路由 |
| 数据库 | 复用 `cards.db` (rusqlite) | 已有连接管理 |

## 集成点

### CardStore 扩展

```rust
impl CardStore {
    // 用户管理
    pub fn create_user(&self, u: PortalUser) -> Result<()>;
    pub fn get_user(&self, id_or_name: &str) -> Option<PortalUser>;
    pub fn list_users(&self) -> Vec<PortalUser>;
    pub fn update_user(&self, id: &str, f: impl Fn(&mut PortalUser)) -> Result<()>;
    
    // 余额
    pub fn add_balance(&self, user_id: &str, delta: f64, reason: &str, operator: &str) -> Result<f64>;
    pub fn balance_log(&self, user_id: &str, limit: usize) -> Vec<BalanceLogEntry>;
    
    // 用户组
    pub fn list_groups(&self) -> Vec<UserGroup>;
    pub fn upsert_group(&self, g: UserGroup) -> Result<()>;
    
    // 购买
    pub fn purchase_plan(&self, user_id: &str, plan_id: &str, stack: Option<StackMode>) -> Result<Card, PurchaseError>;
    
    // 邀请
    pub fn apply_aff_code(&self, user_id: &str, code: &str) -> Result<()>;
    pub fn aff_stats(&self, user_id: &str) -> AffStats;
}
```

### 计费集成

- 购买套餐: `purchase_plan` 检查余额 → 扣费 → 发卡 → 写流水
- 消耗记账: 现有 billing.db 不动, 用户余额只受「购买」影响
- 返佣: 被邀请人首次购买后, 邀请人得返佣 (pending → admin 审批 → paid 到余额)

## 分阶段实施

### P0 核心流程 (1-2 天)

1. 用户表 + 登录 + session
2. 后台用户管理 (创建/禁用/加余额)
3. 门户登录页 + 余额查看
4. 套餐购买 (从余额扣费, 发卡)

### P1 完整功能 (2-3 天)

1. 用户组 + 套餐按组开放
2. aff 邀请码 + 返佣
3. 套餐配置界面重构 (分组展示)
4. CardPlan 扩展字段 (quota_usd/expire_hours/billing_mode/stack_mode)
5. 子套餐 + 叠加购买

### P2 优化 (1 天)

1. 余额流水导出
2. 邀请排行榜
3. 套餐购买历史
4. 门户主题/品牌定制

## 风险与缓解

| 风险 | 缓解 |
|---|---|
| 密码明文泄漏 | argon2 哈希, 日志不记密码 |
| session 劫持 | token 随机 32 字节, 24h 过期, HTTPS only |
| 余额负数 | 购买时检查余额, 不足拒绝 |
| 并发购买 | CardStore 用 RwLock, 余额操作原子 |
| 邀请刷量 | 同 IP/设备限制, admin 审批返佣 |

---

**文件**: `docs/portal-design.md`  
**字数**: ~3500 字  
**状态**: 待审批
