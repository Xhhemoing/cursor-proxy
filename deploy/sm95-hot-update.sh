#!/usr/bin/env bash
# Hot-update SM95 cursor-proxy from a local release binary.
# Usage (from repo root):
#   SM95_PASS='...' ./deploy/sm95-hot-update.sh
# Optional:
#   SM95_HOST=user1@38.92.24.153
#   BIN=./target/release/cursor-fast-proxy-rs
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/target/release/cursor-fast-proxy-rs}"
UNIT="$ROOT/deploy/cursor-proxy.service"
HOST="${SM95_HOST:-user1@38.92.24.153}"
PASS="${SM95_PASS:-}"
STAMP="$(date -u +%Y%m%d_%H%M%S)"
REMOTE_BIN="/home/user1/code/cursor-proxy/bin/cursor-fast-proxy-rs"
REMOTE_UNIT="/home/user1/.config/systemd/user/cursor-proxy.service"

if [[ ! -x "$BIN" ]]; then
  echo "missing binary: $BIN (run cargo build --release)" >&2
  exit 1
fi
if [[ ! -f "$UNIT" ]]; then
  echo "missing unit: $UNIT" >&2
  exit 1
fi
if [[ -z "$PASS" ]]; then
  echo "set SM95_PASS" >&2
  exit 1
fi

ssh_run() {
  python3 - "$HOST" "$PASS" "$1" <<'PY'
import pexpect, shlex, sys
host, password, cmd = sys.argv[1], sys.argv[2], sys.argv[3]
full = f"ssh -o StrictHostKeyChecking=no -o ConnectTimeout=12 {host} {shlex.quote(cmd)}"
child = pexpect.spawn(full, timeout=90, encoding="utf-8", codec_errors="replace")
i = child.expect(["[Pp]assword:", "Permission denied", pexpect.EOF, pexpect.TIMEOUT], timeout=20)
if i == 0:
    child.sendline(password)
    child.expect(pexpect.EOF, timeout=90)
    print(child.before or "")
    sys.exit(child.exitstatus or 0)
print(child.before or "", file=sys.stderr)
sys.exit(2)
PY
}

scp_put() {
  python3 - "$HOST" "$PASS" "$1" "$2" <<'PY'
import pexpect, sys
host, password, src, dst = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
full = f"scp -o StrictHostKeyChecking=no -o ConnectTimeout=12 {src} {host}:{dst}"
child = pexpect.spawn(full, timeout=120, encoding="utf-8", codec_errors="replace")
i = child.expect(["[Pp]assword:", "Permission denied", pexpect.EOF, pexpect.TIMEOUT], timeout=20)
if i == 0:
    child.sendline(password)
    child.expect(pexpect.EOF, timeout=120)
    print(child.before or "")
    sys.exit(child.exitstatus or 0)
print(child.before or "", file=sys.stderr)
sys.exit(2)
PY
}

echo "1/5 scp binary + unit -> /tmp"
scp_put "$BIN" "/tmp/cursor-fast-proxy-rs.$STAMP"
scp_put "$UNIT" "/tmp/cursor-proxy.service.$STAMP"

echo "2/5 backup + stop + install + start"
ssh_run "set -e
export XDG_RUNTIME_DIR=/run/user/\$(id -u)
install -d /home/user1/code/cursor-proxy/bin
if [ -f $REMOTE_BIN ]; then cp -a $REMOTE_BIN $REMOTE_BIN.bak.$STAMP; fi
if [ -f $REMOTE_UNIT ]; then cp -a $REMOTE_UNIT $REMOTE_UNIT.bak.$STAMP; fi
systemctl --user stop cursor-proxy || true
install -m 755 /tmp/cursor-fast-proxy-rs.$STAMP $REMOTE_BIN
install -m 644 /tmp/cursor-proxy.service.$STAMP $REMOTE_UNIT
systemctl --user daemon-reload
systemctl --user start cursor-proxy
sleep 2
systemctl --user is-active cursor-proxy
systemctl --user show cursor-proxy -p MemoryMax -p MemoryHigh -p MainPID -p LimitNOFILE
curl -sS -m 5 http://127.0.0.1:8800/health
echo
"

echo "3/5 done. rollback: copy $REMOTE_BIN.bak.$STAMP back, restart"
