# 合并 GitHub 最新源码方案

## 现状

- **本地仓库**: `/home/ubuntu/work/cursor-fast-proxy-rs` (Rust 版，16+ 提交，最新)
- **远程仓库**: `https://github.com/Xhhemoing/cursor-proxy.git` → **404 Not Found**
- **旧版 Python**: `/home/ubuntu/work/cursor-fast-proxy/` (非 git，已废弃)

## 问题

远程仓库不存在或私有，无法 fetch/merge。

## 解决方案

### 方案 A: 推送到新仓库（推荐）

如果 GitHub 仓库不存在，创建新仓库并推送本地代码：

```bash
# 1. 在 GitHub 创建新仓库 cursor-fast-proxy-rs
# 2. 添加新远程并推送
cd /home/ubuntu/work/cursor-fast-proxy-rs
git remote set-url origin https://github.com/YOUR_USERNAME/cursor-fast-proxy-rs.git
git push -u origin master
```

### 方案 B: 私有仓库访问

如果仓库是私有的，配置访问令牌：

```bash
# 1. 创建 GitHub Personal Access Token (repo 权限)
# 2. 配置 credential helper
git config --global credential.helper store
echo "https://YOUR_TOKEN@github.com" > ~/.git-credentials

# 3. 重新 fetch
cd /home/ubuntu/work/cursor-fast-proxy-rs
git fetch origin
git merge origin/master  # 或 git rebase origin/master
```

### 方案 C: 合并其他仓库

如果要合并其他仓库（如 Python 版旧代码）：

```bash
# 添加旧版为远程
cd /home/ubuntu/work/cursor-fast-proxy-rs
git remote add legacy /home/ubuntu/work/cursor-fast-proxy
git fetch legacy

# 查看差异
git log --oneline legacy/master..master  # 本地领先
git log --oneline master..legacy/master  # 旧版领先

# 合并（如果有需要保留的旧代码）
git merge legacy/master --allow-unrelated-histories
```

## 当前本地状态

- 分支: `master`
- 最新提交: `c238cce` (fix: 禁用账号后立即重建可用列表)
- 测试: 21/21 通过
- 压测: 17/17 通过

## 建议

由于远程仓库 404，且本地是最新开发版本，建议：
1. **确认远程仓库地址是否正确**
2. **如果是新仓库，直接推送本地代码**
3. **如果是私有仓库，提供令牌后合并**
