#!/usr/bin/env bash
# 最终风控策略 (2026-09-07, 用户定): 高价模型 35 tok/s, 便宜模型 70–80, kimi 免限; 无面值硬帽;
# 价值比 (成本/日均实收) 60%→+10 / 80%→+20 进评分; 压制粘滞 30min; 申诉信任期 24h.
set -e
D=$HOME/.local/share/cursor-fast-proxy-rs
B=http://127.0.0.1:8800
TOK=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['admin_token'])" "$D/config.json")
H="Authorization: Bearer $TOK"
cp "$D/cards.json" "$D/cards.json.bak-riskfinal-$(date +%Y%m%d-%H%M%S)"
curl -s "$B/admin/api/cards/risk-policy" -H "$H" > /tmp/rp_cur.json
python3 - <<'EOF'
import json
d=json.load(open('/tmp/rp_cur.json')); p=d['policy']; w=p['weights']
p['enabled']=True
p['pace_normal_tps']=35; p['pace_soften_tps']=25; p['pace_degraded_tps']=12
p['relief_after_tokens']=3000; p['relief_tps']=0
p['hard_cap_usd']=0; p['hard_cap_allow_prefixes']=[]
p['pace_min_output_price_per_m']=0   # 用显式规则代替
p['degraded_hold_secs']=1800; p['appeal_trust_hours']=24
p['soften_ratio_of_threshold']=0.6
p['model_rules']=[
 {"prefix":"claude-fable","exempt":False,"pace_normal_tps":35,"pace_soften_tps":25,"pace_degraded_tps":12,"note":"高价; 思考不流式, 35 ≈ 客户端可见"},
 {"prefix":"claude-opus","exempt":False,"pace_normal_tps":35,"pace_soften_tps":25,"pace_degraded_tps":12,"note":"高价"},
 {"prefix":"gpt-5.6-sol","exempt":False,"pace_normal_tps":35,"pace_soften_tps":25,"pace_degraded_tps":12,"note":"高价; 原生 118, 限速最有效"},
 {"prefix":"grok","exempt":False,"pace_normal_tps":70,"pace_soften_tps":40,"pace_degraded_tps":20,"note":"便宜; 原生 350"},
 {"prefix":"gemini","exempt":False,"pace_normal_tps":80,"pace_soften_tps":60,"pace_degraded_tps":30,"note":"便宜; 原生 470-3800"},
 {"prefix":"kimi","exempt":True,"note":"≈$0 面值, 不限"},
]
# 价值比 (用户定 60/80), 其余权重不动; 绝对日消耗关
w['value_ratio_lo']=0.6; w['value_ratio_lo_pts']=10; w['value_ratio_hi']=0.8; w['value_ratio_hi_pts']=20
w['quota_usd']=0; w['quota_pts']=0
json.dump(p,open('/tmp/rp_new.json','w'),ensure_ascii=False,indent=1)
EOF
curl -s -X POST "$B/admin/api/cards/risk-policy" -H "$H" -H 'Content-Type: application/json' -d @/tmp/rp_new.json | python3 -c '
import sys,json;d=json.load(sys.stdin);p=d["policy"]
print("saved:",d["ok"],"pace",p["pace_normal_tps"],p["pace_soften_tps"],p["pace_degraded_tps"],"hold",p["degraded_hold_secs"],"trust",p["appeal_trust_hours"],"cap",p["hard_cap_usd"])
for r in p["model_rules"]: print("  rule",r["prefix"],"exempt" if r["exempt"] else (r["pace_normal_tps"],r["pace_soften_tps"],r["pace_degraded_tps"]))
w=p["weights"]; print("  value_ratio",w["value_ratio_lo"],w["value_ratio_lo_pts"],w["value_ratio_hi"],w["value_ratio_hi_pts"])'
echo "cards:"; curl -s "$B/admin/api/cards" -H "$H" | python3 -c '
import sys,json
for c in json.load(sys.stdin)["cards"]: print("  %s %s %s pace=%s score=%s vr=%.0f%% trusted=%s appeal=%s" % (c["card_key"][5:13],c["plan_id"],c["throttle"],c["pace_tps"],c["abuse"]["score"],c["abuse"]["value_ratio"]*100,c["trusted"],c["appeal"]["status"] or "-"))'
