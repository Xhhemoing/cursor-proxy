#!/usr/bin/env bash
# 零售版 (cards v3) live smoke on the LOCAL 8800 instance — run right after a binary swap.
# Checks: seed idempotent, fake card 401, tier gate 403, paced tok/s (official completion_tokens),
# quota card deduction, profit report; then revokes+deletes its smoke-* cards.
# Usage: bash local-smoke.sh [base_url]   (default http://127.0.0.1:8800)
set -euo pipefail
U=${1:-http://127.0.0.1:8800}
D=${CFP_DATA:-$HOME/.local/share/cursor-fast-proxy-rs}
T=$(python3 -c "import json;print(json.load(open('$D/config.json'))['admin_token'])")
H="Authorization: Bearer $T"
C=$U/admin/api/cards
PACED_PLAN=${PACED_PLAN:-day-std-1}     # since 2026-09-05 presets ship pace_*_tps=0 (user rule: limits default OFF);
                                        # "paced" line then just reports raw upstream tok/s. To test pacing, first:
                                        # curl -X POST $C/plans -d '{"id":"day-std-1","pace_normal_tps":25}' and restore to 0 after.
QUOTA_PLAN=${QUOTA_PLAN:-quota-50}
MODEL=${MODEL:-kimi-k3-high}            # cheap, streams reliably; grok-4.6 misbehaves with max_tokens

issue() { curl -s -X POST -H "$H" -H 'content-type: application/json' "$C/issue" \
  -d "{\"plan_id\":\"$1\",\"owner\":\"smoke-$1\",\"count\":1}" \
  | python3 -c "import json,sys;print(json.load(sys.stdin)['issued'][0]['card_key'])"; }
code() { curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $1" -X POST "$U/v1/chat/completions" \
  -H 'content-type: application/json' -d "{\"model\":\"$2\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}"; }
measure() { # $1 card $2 label → prints tok/s from official usage frame
  curl -s -N -H "Authorization: Bearer $1" -X POST "$U/v1/chat/completions" -H 'content-type: application/json' \
    -d "{\"model\":\"$MODEL\",\"stream\":true,\"messages\":[{\"role\":\"user\",\"content\":\"Write 200 words about the solar system in English.\"}]}" \
    -w "%{time_starttransfer} %{time_total}\n" -o /tmp/smoke.sse > /tmp/smoke.t
  python3 - "$2" <<'EOF'
import re,sys
s=open('/tmp/smoke.sse').read(); ttfb,tot=map(float,open('/tmp/smoke.t').read().split())
m=re.search(r'"completion_tokens":(\d+)',s)
if not m: print(f"{sys.argv[1]:14} NO usage frame — check pool/503"); sys.exit(0)
ct=int(m.group(1)); w=tot-ttfb
print(f"{sys.argv[1]:14} completion_tokens={ct} window={w:.1f}s → {ct/w:.1f} tok/s")
EOF
}

echo "== seed (idempotent; written should be 0 on a seeded instance)"
curl -s -X POST -H "$H" "$C/plans/seed" | python3 -c "import json,sys;print('written',json.load(sys.stdin)['written'])"
echo "== cost-model"; curl -s -H "$H" "$C/cost-model"; echo
echo "== fake card → expect 401: $(code card-deadbeef claude-opus-5)"

K=$(issue "$PACED_PLAN"); Q=$(issue "$QUOTA_PLAN")
trap 'for k in $K $Q; do curl -s -X POST -H "$H" "$C/$k/revoke" >/dev/null; curl -s -X DELETE -H "$H" "$C/$k" >/dev/null; done; echo "== smoke cards removed (ledger rows remain)"' EXIT

echo "== tier gate: $PACED_PLAN → claude-fable-5-1-thinking-high, expect 403: $(code "$K" claude-fable-5-1-thinking-high)"
echo "== pacing (if plan pace_normal_tps>0: expect that ±15%; if 0: raw upstream speed, both lines similar)"
measure "$K" "paced"
measure "$Q" "quota-unpaced"
echo "== quota card balance"
curl -s -H "$H" "$C/$Q" | python3 -c "import json,sys;d=json.load(sys.stdin);print('face_used_usd',d.get('face_used_usd'),'face_left_usd',d.get('face_left_usd'))"
echo "== profit (group=plan)"
curl -s -H "$H" "$C/profit?group=plan" | python3 -c "import json,sys;print(json.load(sys.stdin)['totals'])"
