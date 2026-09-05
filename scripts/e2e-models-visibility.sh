#!/usr/bin/env bash
# Visibility E2E on isolated 8899: ghost aliases must NOT appear; upstream-confirmed must.
set -u
B=http://127.0.0.1:8899
A="Authorization: Bearer e2e-admin-tok"
J="Content-Type: application/json"
pass=0; fail=0
ok(){ echo "  ✅ $1"; pass=$((pass+1)); }
ko(){ echo "  ❌ $1"; fail=$((fail+1)); }
ids(){ curl -s $B/v1/models -H "Authorization: Bearer e2e-admin-tok" | python3 -c 'import sys,json;print(" ".join(m["id"] for m in json.load(sys.stdin)["data"]))'; }

echo "── 未拉上游时: 只有注册表条目 + default ──"
curl -s -X POST $B/admin/api/models -H "$A" -H "$J" -d '{"model":"e2e-vis-1","input_per_m":1,"output_per_m":2,"tier":"economy"}' >/dev/null
IDS=$(ids)
echo "  models: $(echo $IDS | wc -w) 个"
echo "$IDS" | grep -q 'e2e-vis-1' && ok "注册表条目可见" || ko "registry entry missing"
echo "$IDS" | grep -qw 'fable-5' && ko "幽灵别名 fable-5 仍在列表" || ok "fable-5 不可见 (未上游确认)"
echo "$IDS" | grep -qw 'gpt-5' && ko "幽灵别名 gpt-5 仍在列表" || ok "gpt-5 不可见"
echo "$IDS" | grep -qw 'kimi-k3-low' && ko "未确认内置 kimi-k3-low 不应可见" || ok "kimi-k3-low 不可见 (未拉上游)"

echo "── 模拟拉上游后: 上游名 + 变体收敛可见, 别名仍不可见 ──"
# 隔离实例上游是黑洞 → 直接调内部状态不可行; 改用一个等价路径:
# 把上游名写进注册表 (enabled) 等价于「已确认」. 变体收敛由单测覆盖.
curl -s -X POST $B/admin/api/models -H "$A" -H "$J" -d '{"model":"kimi-k3-low","input_per_m":3,"output_per_m":15,"cache_read_per_m":0.3}' >/dev/null
IDS2=$(ids)
echo "$IDS2" | grep -qw 'kimi-k3-low' && ok "kimi-k3-low 注册后可见" || ko "kimi-k3-low still hidden"
echo "$IDS2" | grep -qw 'opus-5' && ko "别名 opus-5 仍在" || ok "opus-5 不可见"

echo "── 套餐预览同样过滤幽灵 ──"
curl -s -X POST $B/admin/api/cards/plans -H "$A" -H "$J" -d '{"id":"e2e-vis-p","name":"t","price":1,"kind":"unlimited","duration_hours":24,"max_concurrency":1}' >/dev/null
PM=$(curl -s $B/admin/api/cards/plans/e2e-vis-p/models -H "$A")
echo "$PM" | python3 -c 'import sys,json;d=json.load(sys.stdin);names=[m["model"] for m in d["models"]];print("  plan models:",names);sys.exit(0 if "e2e-vis-1" in names and "fable-5" not in names and "gpt-5" not in names else 1)' && ok "套餐预览无幽灵别名" || ko "plan preview leaked ghosts"

echo; echo "══ RESULT: $pass passed, $fail failed ══"
curl -s -X DELETE $B/admin/api/cards/plans/e2e-vis-p -H "$A" >/dev/null
curl -s -X DELETE "$B/admin/api/models/e2e-vis-1" -H "$A" >/dev/null
curl -s -X DELETE "$B/admin/api/models/kimi-k3-low" -H "$A" >/dev/null
exit $fail
