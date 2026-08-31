#!/usr/bin/env bash
# Update cursor-proxy on kpzhu-haochi (sm95x6-03, 38.92.24.153) by building ON the server.
#
# Flow: rsync source -> build as user1 with cargo -> backup -> stop -> install binary/static/unit -> start -> health check
#
# Usage (from repo root):
#   ./deploy/kpzhu-haochi-update.sh
# Optional env:
#   HOST=kpzhu-haochi            ssh alias (root login, key auth, see ~/.ssh/config)
#   APP_USER=user1               user that owns and runs the service
#   SKIP_BUILD=1                 reuse existing build in the build dir
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOST="${HOST:-kpzhu-haochi}"
APP_USER="${APP_USER:-user1}"
APP_HOME="/home/$APP_USER"
APP_DIR="$APP_HOME/code/cursor-proxy"
BUILD_DIR="$APP_HOME/code/cursor-proxy-build"
UNIT_DST="$APP_HOME/.config/systemd/user/cursor-proxy.service"
STAMP="$(date -u +%Y%m%d_%H%M%S)"

as_user() {  # run a bash script on the server as $APP_USER
  ssh -o ConnectTimeout=15 "$HOST" "sudo -u $APP_USER bash -lc $(printf '%q' "$1")"
}

echo "1/4 rsync source -> $HOST:$BUILD_DIR"
rsync -az --delete -e ssh \
  --exclude target --exclude .git \
  --include='/Cargo.toml' --include='/Cargo.lock' --include='/build.rs' \
  --include='/src/***' --include='/static/***' --include='/deploy/***' \
  --exclude='*' \
  "$ROOT/" "$HOST:$BUILD_DIR/"
ssh "$HOST" "chown -R $APP_USER:$APP_USER $BUILD_DIR"

if [[ -z "${SKIP_BUILD:-}" ]]; then
  echo "2/4 cargo build --release on server (as $APP_USER)"
  as_user "cd $BUILD_DIR && ~/.cargo/bin/cargo build --release 2>&1 | grep -vE '^\s*(warning|-->|\||=|[0-9]+ \|)' | tail -20"
else
  echo "2/4 build skipped (SKIP_BUILD=1)"
fi
as_user "test -x $BUILD_DIR/target/release/cursor-fast-proxy-rs" || { echo "no release binary" >&2; exit 1; }

echo "3/4 backup + stop + install + start (backup suffix .bak.$STAMP)"
as_user "
set -e
export XDG_RUNTIME_DIR=/run/user/\$(id -u)
cd $APP_DIR
install -d bin
[ -f bin/cursor-fast-proxy-rs ] && cp -a bin/cursor-fast-proxy-rs bin/cursor-fast-proxy-rs.bak.$STAMP
[ -f $UNIT_DST ] && cp -a $UNIT_DST $UNIT_DST.bak.$STAMP
[ -d static ] && cp -a static static.bak.$STAMP
systemctl --user stop cursor-proxy || true
# 兜底: 干掉 systemd 管不到、仍占用 8800 的孤儿进程 (KillMode=mixed 或手动运行残留).
# 注意: 绝不能用 pkill -f 'bin/cursor-fast-proxy-rs' — 本脚本 shell 的命令行也含该串, 会自杀。
# 只按端口锁定 PID 精确 kill。
port_pid() { ss -ltnpH 2>/dev/null | grep ':8800 ' | grep -oE 'pid=[0-9]+' | head -1 | cut -d= -f2; }
for _ in 1 2 3 4 5; do P=\$(port_pid); [ -n \"\$P\" ] || break; kill \"\$P\" 2>/dev/null || true; sleep 1; done
P=\$(port_pid); [ -n \"\$P\" ] && kill -9 \"\$P\" 2>/dev/null || true
systemctl --user reset-failed cursor-proxy 2>/dev/null || true
install -m 755 $BUILD_DIR/target/release/cursor-fast-proxy-rs bin/cursor-fast-proxy-rs
rsync -a --delete $BUILD_DIR/static/ static/
rsync -a --delete $BUILD_DIR/src/ src/
cp $BUILD_DIR/Cargo.toml $BUILD_DIR/Cargo.lock .
install -Dm 644 $BUILD_DIR/deploy/cursor-proxy.service $UNIT_DST
systemctl --user daemon-reload
systemctl --user start cursor-proxy
sleep 3
# 验证信息化, 不因 set -e 中断 (否则失败会跳过后续输出)
systemctl --user is-active cursor-proxy || true
systemctl --user show cursor-proxy -p MainPID -p MemoryMax
curl -sS -m 5 http://127.0.0.1:8800/health || true; echo
"

echo "4/4 done."
echo "rollback: ssh $HOST \"sudo -u $APP_USER bash -lc 'export XDG_RUNTIME_DIR=/run/user/\\\$(id -u); cd $APP_DIR && cp -a bin/cursor-fast-proxy-rs.bak.$STAMP bin/cursor-fast-proxy-rs && systemctl --user restart cursor-proxy'\""
