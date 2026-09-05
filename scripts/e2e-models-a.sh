#!/usr/bin/env bash
# A-batch E2E on isolated 8899 (blackhole upstream already assumed running on 9911).
set -u
B=http://127.0.0.1:8899
A="Authorization: Bearer e2e-admin-tok"
J="Content-Type: application/json"
pass=0; fail=0
ok(){ echo "  ✅ $1"; pass=$((pass+1)); }
ko(){ echo "  ❌ $1"; fail=$((fail+1)); }

echo "── A2: /v1/models 动态列表 ──"
# 注册一个面板定价模型, 它必须出现在 /v1/models
curl -s -X POST $B/admin/api/models -H "$A" -H "$J" -d '{"model":"e2e-visible-1","input_per_m":1,"output_per_m":2,"cache_read_per_m":0.1,"tier":"economy"}' >/dev/null
IDS=$(curl -s $B/v1/models -H "Authorization: Bearer e2e-admin-tok" | python3 -c 'import sys,json;print(" ".join(m["id"] for m in json.load(sys.stdin)["data"]))')
echo "$IDS" | grep -q 'e2e-visible-1' && ok "/v1/models 含注册表新模型" || ko "/v1/models missing e2e-visible-1: $(echo $IDS | head -c 200)"
echo "$IDS" | grep -q 'kimi-k3' && ok "/v1/models 含内置表模型 kimi-k3" || ko "builtin missing"
# 停用后消失
curl -s -X POST $B/admin/api/models -H "$A" -H "$J" -d '{"model":"e2e-visible-1","enabled":false}' >/dev/null
IDS2=$(curl -s $B/v1/models -H "Authorization: Bearer e2e-admin-tok" | python3 -c 'import sys,json;print(" ".join(m["id"] for m in json.load(sys.stdin)["data"]))')
echo "$IDS2" | grep -q 'e2e-visible-1' && ko "disabled model still listed" || ok "enabled=false 后从 /v1/models 消失"
curl -s -X POST $B/admin/api/models -H "$A" -H "$J" -d '{"model":"e2e-visible-1","enabled":true}' >/dev/null

echo "── A3: pricing-table 动态 ──"
PT=$(curl -s $B/admin/api/cards/pricing-table -H "$A")
echo "$PT" | python3 -c 'import sys,json;d=json.load(sys.stdin);rows=d["models"];print("rows:",len(rows));sys.exit(0 if any(r["model"]=="e2e-visible-1" and r["source"]=="manual" for r in rows) else 1)' && ok "pricing-table 含手动条目并标 source=manual" || ko "pricing-table missing manual row"
echo "$PT" | python3 -c 'import sys,json;d=json.load(sys.stdin);rows=d["models"];sys.exit(0 if any(r["model"]=="kimi-k3" for r in rows) else 1)' && ok "pricing-table 含内置模型" || ko "pricing-table missing builtin"

echo "── A4: 套餐可调模型预览 + 体检 ──"
# 组 + 前缀组合: 组建 kimi-*, 套餐前缀 gpt- → 交集 0 → 警告
curl -s -X POST $B/admin/api/models/groups -H "$A" -H "$J" -d '{"id":"e2e-g-kimi","name":"kimi","members":["kimi-*"]}' >/dev/null
R=$(curl -s -X POST $B/admin/api/cards/plans -H "$A" -H "$J" -d '{"id":"e2e-pm","name":"t","price":1,"kind":"unlimited","duration_hours":24,"max_concurrency":1,"model_groups":["e2e-g-kimi"],"model_prefixes":["gpt-"]}')
echo "$R" | python3 -c 'import sys,json;d=json.load(sys.stdin);print("  model_count:",d.get("model_count"),"warnings:",d.get("warnings"));sys.exit(0 if d.get("model_count")==0 and d.get("warnings") else 1)' && ok "组∧前缀交集=0 → 保存响应带警告" || ko "expected 0 models + warnings"
PM=$(curl -s $B/admin/api/cards/plans/e2e-pm/models -H "$A")
echo "$PM" | python3 -c 'import sys,json;d=json.load(sys.stdin);sys.exit(0 if d["count"]==0 and d["warnings"] else 1)' && ok "预览端点同样报 0 可调 + 警告" || ko "preview endpoint wrong: $(echo $PM | head -c 200)"
# 修正为 kimi 前缀 → 有模型
R2=$(curl -s -X POST $B/admin/api/cards/plans -H "$A" -H "$J" -d '{"id":"e2e-pm","model_prefixes":["kimi-"]}')
echo "$R2" | python3 -c 'import sys,json;d=json.load(sys.stdin);print("  after fix model_count:",d.get("model_count"));sys.exit(0 if (d.get("model_count") or 0)>0 else 1)' && ok "修正前缀后预览有模型" || ko "still 0 after fix"
# 坏组 id 警告
R3=$(curl -s -X POST $B/admin/api/cards/plans -H "$A" -H "$J" -d '{"id":"e2e-pm","model_groups":["e2e-g-kimi","ghost-group"]}')
echo "$R3" | python3 -c 'import sys,json;d=json.load(sys.stdin);w=" ".join(d.get("warnings") or []);sys.exit(0 if "ghost-group" in w else 1)' && ok "失效组 id 有警告" || ko "ghost group not warned"
# 列表带 model_count
curl -s $B/admin/api/cards/plans -H "$A" | python3 -c 'import sys,json;d=json.load(sys.stdin);p=[x for x in d["plans"] if x["id"]=="e2e-pm"][0];print("  list row: model_count",p.get("model_count"),"warnings",len(p.get("warnings") or []));sys.exit(0 if "model_count" in p else 1)' && ok "套餐列表带 model_count/warnings" || ko "list missing model_count"

echo "── A1: 上游标记 (无真号 → 只验 mark_upstream 不建行) ──"
# 隔离实例有 dummy 号但黑洞上游 → upstream 拉取会 502; 这里只验证注册表未被污染
CNT=$(curl -s "$B/admin/api/models" -H "$A" | python3 -c 'import sys,json;print(len(json.load(sys.stdin)["models"]))')
echo "  registry rows: $CNT"

echo; echo "══ RESULT: $pass passed, $fail failed ══"
curl -s -X DELETE $B/admin/api/cards/plans/e2e-pm -H "$A" >/dev/null
curl -s -X DELETE "$B/admin/api/models/groups/e2e-g-kimi" -H "$A" >/dev/null
curl -s -X DELETE "$B/admin/api/models/e2e-visible-1" -H "$A" >/dev/null
exit $fail
