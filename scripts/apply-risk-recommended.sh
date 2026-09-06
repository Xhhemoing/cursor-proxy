#!/usr/bin/env bash
# 应用建议风控: 全局 pace 40/25/12, grok 免限, 硬帽 $250 (白名单 kimi-k3/grok), relief 3000 tok 放开; 权重保持缺省
set -u
D=$HOME/.local/share/cursor-fast-proxy-rs
B=http://127.0.0.1:8800
TOK=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['admin_token'])" "$D/config.json")
H="Authorization: Bearer $TOK"
cp "$D/cards.json" "$D/cards.json.bak-riskapply-$(date +%Y%m%d-%H%M%S)"
curl -s "$B/admin/api/cards/risk-policy" -H "$H" | python3 -c '
import sys,json
p=json.load(sys.stdin)["policy"]
p["enabled"]=True
p["pace_normal_tps"]=40; p["pace_soften_tps"]=25; p["pace_degraded_tps"]=12
p["relief_after_tokens"]=3000; p["relief_tps"]=0
p["hard_cap_usd"]=250.0; p["hard_cap_allow_prefixes"]=["kimi-k3","grok"]
p["model_rules"]=[{"prefix":"grok","exempt":True,"pace_normal_tps":None,"pace_soften_tps":None,"pace_degraded_tps":None,"note":"原生 374 tok/s 且输出长 (p90 10.8k), 限了只会超时"}]
print(json.dumps(p))' > /tmp/rp_new.json
curl -s -X POST "$B/admin/api/cards/risk-policy" -H "$H" -H 'Content-Type: application/json' -d @/tmp/rp_new.json | python3 -c 'import sys,json;d=json.load(sys.stdin);p=d["policy"];print("saved:",d["ok"],"pace",p["pace_normal_tps"],p["pace_soften_tps"],p["pace_degraded_tps"],"cap",p["hard_cap_usd"],p["hard_cap_allow_prefixes"],"rules",[r["prefix"] for r in p["model_rules"]])'
python3 -c "import json,sys;p=json.load(open(sys.argv[1]))['risk_policy'];print('cards.json:',p['pace_normal_tps'],p['hard_cap_usd'])" "$D/cards.json"
curl -s "$B/admin/api/cards" -H "$H" | python3 -c 'import sys,json;rows=json.load(sys.stdin)["cards"];print("cards pace now:",[(r["card_key"][5:11],r["throttle"],r["pace_tps"]) for r in rows if r.get("enabled")])'
