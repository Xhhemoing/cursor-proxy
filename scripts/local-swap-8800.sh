#!/usr/bin/env bash
# 本机 8800 零售版换二进制 (备份 → stop → cp → start → md5 核对). 用法: scripts/local-swap-8800.sh <tag>
set -euo pipefail
TAG=${1:?tag required, e.g. lanes}
export XDG_RUNTIME_DIR=/run/user/1000
B=$HOME/.local/opt/cursor-fast-proxy-rs
D=$HOME/.local/share/cursor-fast-proxy-rs
TS=$(date +%Y%m%d-%H%M%S)
SRC=target/release/cursor-fast-proxy-rs
[ -x "$SRC" ] || { echo "no $SRC"; exit 1; }
cp "$B/cursor-fast-proxy-rs" "$B/cursor-fast-proxy-rs.bak-$TAG-$TS"
[ -f "$D/cards.json" ] && cp "$D/cards.json" "$D/cards.json.bak-$TAG-$TS" || echo "no cards.json (cards live in cards.db), skip"
cp "$D/config.json" "$D/config.json.bak-$TAG-$TS"
sqlite3 "$D/billing.db" ".backup $D/billing.db.bak-$TAG-$TS"
sqlite3 "$D/cards.db" ".backup $D/cards.db.bak-$TAG-$TS"
echo "backups: *.bak-$TAG-$TS"
systemctl --user stop cursor-fast-proxy-rs-8800
cp "$SRC" "$B/cursor-fast-proxy-rs"
systemctl --user start cursor-fast-proxy-rs-8800
sleep 2
PID=$(systemctl --user show -p MainPID --value cursor-fast-proxy-rs-8800)
A=$(md5sum "$SRC" | cut -d' ' -f1)
R=$(md5sum "/proc/$PID/exe" | cut -d' ' -f1)
echo "pid=$PID md5 target=$A live=$R"
[ "$A" = "$R" ] && echo "SWAP OK" || { echo "MD5 MISMATCH"; exit 2; }
ls -l "/proc/$PID/exe" | grep -q deleted && { echo "exe deleted?!"; exit 3; } || true
