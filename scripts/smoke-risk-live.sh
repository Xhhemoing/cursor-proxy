#!/usr/bin/env bash
# 换后只读核对: 风控策略读回 + 预演 (不改任何东西)
D=$HOME/.local/share/cursor-fast-proxy-rs
B=http://127.0.0.1:8800
TOK=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['admin_token'])" "$D/config.json")
H="Authorization: Bearer $TOK"
curl -s "$B/admin/api/cards/risk-policy" -H "$H" | python3 -c 'import sys,json;p=json.load(sys.stdin)["policy"];print("policy: enabled",p["enabled"],"th_override",p["abuse_threshold_override"],"pace",p["pace_normal_tps"],p["pace_soften_tps"],p["pace_degraded_tps"],"relief",p["relief_after_tokens"],p["relief_tps"],"cap",p["hard_cap_usd"],"rules",len(p["model_rules"]))'
curl -s "$B/admin/api/cards/risk-policy" -H "$H" | python3 -c 'import sys,json;print(json.dumps(json.load(sys.stdin)["policy"]))' > /tmp/rp_cur.json
curl -s -X POST "$B/admin/api/cards/risk-policy/preview" -H "$H" -H 'Content-Type: application/json' -d @/tmp/rp_cur.json | python3 -c '
import sys,json;d=json.load(sys.stdin);print("preview (现策略):",d["summary"])
for r in d["rows"][:6]: print("  ",r["card_key"][5:13],r["plan_id"],"$%.1f"%r["day_quota_usd"],r["day_used"],"req | now",r["current"]["throttle"],r["current"]["score"],"| reasons",r["preview"]["reasons"],"| slots/fast/span",r["signals"]["active_slots"],round(r["signals"]["fast_follow_ratio"],2),round(r["signals"]["span_hours"],1))'
