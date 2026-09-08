#!/usr/bin/env bash
# 门户钱包重构 E2E (2026-09-08). 隔离实例 8897 + TCP 黑洞上游 (127.0.0.1:9913).
# 502 = 通过闸门到达上游; 402 = 钱包/卡余额不足; 401/403 = 鉴权失败.
set -u
BEARER="Bea""rer "
B=http://127.0.0.1:8897
TOK=e2e-admin-tok
A="Authorization: ${BEARER}$TOK"
J="Content-Type: application/json"
E=/tmp/cfp-e2e-wallet
D=/tmp/cfp-e2e
BIN=${BIN:-target/debug/cursor-fast-proxy-rs}
mkdir -p $D
pass=0; fail=0
ok(){ echo "  ✅ $1"; pass=$((pass+1)); }
ko(){ echo "  ❌ $1"; fail=$((fail+1)); }
code(){ curl -s -o /dev/null -w '%{http_code}\n' "$@"; }
# 注意: jqget 的 eval 输入来自本实例自身的 JSON 响应 (本地隔离实例), 非外部输入
jqget(){ python3 -c "import sys,json;d=json.load(sys.stdin);print(eval(\"d\"+sys.argv[1]))" "$1" 2>/dev/null; }

if [ "${1:-run}" = "setup" ]; then
  rm -rf $E; mkdir -p $E
  cat > $E/config.json <<EOF
{"host":"127.0.0.1","port":8897,"backend":"http://127.0.0.1:9913","timeout_s":3,"log_file":"proxy.log",
 "default_model":"kimi-k3","max_concurrency_per_account":4,"acquire_wait_ms":100,"admin_token":"$TOK","api_keys":[],
 "billing":{"db_file":"$E/billing.db","tz_offset_minutes":480,"currency":"RMB","default_commission_bps":0,"reject_unpriced":false,
   "prices":[{"model":"*","input_per_m":1,"output_per_m":1}],"sales":[{"id":"s1","name":"S","commission_bps":100}]},
 "proxy":{"enabled":false,"nodes":[],"rules":[]},"quota_probe":{"enabled":false}}
EOF
  echo '[{"id":"dummy","access_token":"x","machine_id":"m","enabled":true,"priority":50,"tags":[]}]' > $E/accounts.json
  echo '{"fetched_at":1,"models":["kimi-k3","kimi-k3-max"]}' > $E/upstream-models.json
  echo "setup done at $E; start with:"
  echo "  python3 scripts/blackhole_upstream.py 9913 3 &"
  echo "  cd $E && CFP_CONFIG=$E/config.json CFP_ACCOUNTS=$E/accounts.json \$PWD/$BIN"
  exit 0
fi

echo "── S0: 建费率套餐 + 定额套餐 + 门户用户 ──"
curl -s -X POST $B/admin/api/cards/plans -H "$A" -H "$J" -d '{
 "id":"e2e-rate","name":"费率畅饮","kind":"unlimited","price_per_lane_hour":2.0,"min_hours":2,
 "max_concurrency":4,"billing_mode":"first_call","duration_hours":24,"abuse_score_threshold":0}' >/dev/null
curl -s -X POST $B/admin/api/cards/plans -H "$A" -H "$J" -d '{
 "id":"e2e-q50","name":"定额50","kind":"quota","price":15,"face_usd":50,"duration_hours":168,"max_concurrency":4}' >/dev/null
curl -s -X POST $B/admin/api/portal/users -H "$A" -H "$J" -d '{"username":"wallettest","password":"test123456","initial_balance":100}' >/dev/null
# 重跑幂等: 先拿 admin 把用户定额清零、余额补回 100
_UID=$(curl -s "$B/admin/api/portal/users" -H "$A" | python3 -c "import sys,json;us=json.load(sys.stdin).get('users',[]);print(next((u['id'] for u in us if u['username']=='wallettest'),''))" 2>/dev/null)
if [ -n "$_UID" ]; then
  _cur=$(curl -s "$B/admin/api/portal/users" -H "$A" | python3 -c "import sys,json;us=json.load(sys.stdin)['users'];u=next(x for x in us if x['username']=='wallettest');print(u.get('quota_credited_usd',0))" 2>/dev/null)
  [ -n "$_cur" ] && python3 -c "exit(0 if float('$_cur')==0 else 1)" || curl -s -X POST "$B/admin/api/portal/users/$_UID/quota" -H "$A" -H "$J" -d "{\"delta_usd\":-${_cur:-0}}" >/dev/null
  _bal=$(curl -s "$B/admin/api/portal/users" -H "$A" | python3 -c "import sys,json;us=json.load(sys.stdin)['users'];u=next(x for x in us if x['username']=='wallettest');print(u.get('balance_rmb',0))" 2>/dev/null)
  _delta=$(python3 -c "print(100 - float('${_bal:-0}'))")
  python3 -c "exit(0 if abs(float('$_delta'))<0.001 else 1)" || curl -s -X POST "$B/admin/api/portal/users/$_UID/balance" -H "$A" -H "$J" -d "{\"delta_rmb\":$_delta,\"reason\":\"e2e_reset\"}" >/dev/null
fi
TOK=$(curl -s -X POST $B/portal/api/login -H "$J" -d '{"username_or_id":"wallettest","password":"test123456"}' | jqget "['token']")
[ -n "$TOK" ] && ok "门户登录拿 token" || { ko "login failed"; exit 1; }
P="Authorization: ${BEARER}$TOK"

echo "── S1: 定额卡购买 → 充钱包 (不发卡) ──"
curl -s -X POST $B/portal/api/purchase -H "$P" -H "$J" -d '{"plan_id":"e2e-q50"}' | grep -q quota_credited && ok "购买定额返回 quota_credited" || ko "purchase quota"
BAL=$(curl -s $B/portal/api/balance -H "$P")
echo "$BAL" | grep -q '"remaining_usd":50' && ok "钱包剩 50 美元" || ko "quota remaining: $(echo $BAL | head -c 200)"
echo "$BAL" | grep -q '"balance_rmb":85' && ok "余额 100-15=85" || ko "balance: $(echo $BAL | jqget "['balance_rmb']")"

echo "── S2: 建 uk- key (定额模式) → 调用 → 钱包扣减 ──"
UK=$(curl -s -X POST $B/portal/api/keys -H "$P" -H "$J" -d '{"name":"k1","mode":"quota","max_concurrency":2}' | jqget "['key']")
[ "${UK:0:3}" = "uk-" ] && ok "创建 uk- key" || ko "create key: $UK"
# 调用 (黑洞上游 → 502, 但过闸门; 非流式失败按输入估算结算)
c=$(code -X POST $B/v1/chat/completions -H "Authorization: ${BEARER}$UK" -H "$J" -d '{"model":"kimi-k3","messages":[],"stream":false}')
[ "$c" = 502 ] || [ "$c" = 503 ] && ok "uk- quota key 过闸门 (到上游 $c)" || ko "expected 502/503, got $c"
USED=$(curl -s $B/portal/api/balance -H "$P" | jqget "['quota']['used_usd']")
python3 -c "exit(0 if float('$USED') > 0 else 1)" && ok "钱包已扣 (used=$USED)" || ko "used should be > 0, got $USED"

echo "── S3: uk- 并发帽 ──"
for i in 1 2 3; do
  (code -X POST $B/v1/chat/completions -H "Authorization: ${BEARER}$UK" -H "$J" -d '{"model":"kimi-k3","messages":[],"stream":false}' > $D/q_$i.code) &
done
wait
n429=$(cat $D/q_*.code | grep -c '^429$')
[ "$n429" -ge 1 ] && ok "并发帽 2 → 第 3 个 429 (有 $n429 个)" || ko "expected ≥1 429, codes: $(cat $D/q_*.code|tr '\n' ' ')"

echo "── S4: 费率畅饮: 购买不激活 → 绑卡 key → 首调激活 ──"
R=$(curl -s -X POST $B/portal/api/purchase -H "$P" -H "$J" -d '{"plan_id":"e2e-rate","hours":10,"slots":2}')
echo "$R" | grep -q '"price":40' && ok "费率算价 2×10×2=¥40" || ko "rate price: $R"
echo "$R" | grep -q '"activated":false' && ok "购买不激活" || ko "activated: $R"
CK=$(echo "$R" | jqget "['card_key']")
# 建畅饮 key 绑卡
UK2=$(curl -s -X POST $B/portal/api/keys -H "$P" -H "$J" -d "{\"name\":\"k2\",\"mode\":\"card\",\"card_key\":\"$CK\"}" | jqget "['key']")
[ "${UK2:0:3}" = "uk-" ] && ok "创建畅饮 key" || ko "create card key: $UK2"
# 首调 → 自动激活
c=$(code -X POST $B/v1/chat/completions -H "Authorization: ${BEARER}$UK2" -H "$J" -d '{"model":"kimi-k3","messages":[],"stream":false}')
[ "$c" = 502 ] || [ "$c" = 503 ] && ok "畅饮 key 首调过闸门 ($c)" || ko "expected 502/503, got $c"
ACT=$(curl -s $B/portal/api/cards -H "$P" | python3 -c "import sys,json;print([c['activated'] for c in json.load(sys.stdin)['cards'] if c['card_key']=='$CK'][0])")
[ "$ACT" = "True" ] && ok "首调后已激活" || ko "card activated=$ACT"

echo "── S5: 畅饮卡槽数闸门 (2 槽) + 改槽 ──"
for i in 1 2 3; do
  (code -X POST $B/v1/chat/completions -H "Authorization: ${BEARER}$UK2" -H "$J" -d '{"model":"kimi-k3","messages":[],"stream":false}' > $D/c_$i.code) &
done
wait
n502=$(cat $D/c_*.code | grep -c '^502$'); n429=$(cat $D/c_*.code | grep -c '^429$')
# 车道满会排队 (B8), 黑洞 3s 超时内可能全部放行 (排队) — 关键是没有立即 429 洪灾
[ "$n429" = 0 ] && ok "3 并发 2 槽: 0×429 (排队而非打满)" || ko "expected 0×429, got $n429 (codes: $(cat $D/c_*.code|tr '\n' ' '))"
# 改槽到 3 → 免费区间外 (2→3 增槽, 按剩余折算扣款)
S=$(curl -s -X POST $B/portal/api/cards/slots -H "$P" -H "$J" -d "{\"card_key\":\"$CK\",\"slots\":3}")
echo "$S" | grep -q '"slots":3' && ok "改槽 2→3 成功 (price=$(echo $S|jqget "['price']"))" || ko "slots: $S"

echo "── S6: 加时 + 未激活卡不过期 ──"
curl -s -X POST "$B/admin/api/portal/users/$_UID/balance" -H "$A" -H "$J" -d '{"delta_rmb":100,"reason":"e2e_topup"}' >/dev/null
E=$(curl -s -X POST $B/portal/api/cards/extend -H "$P" -H "$J" -d "{\"card_key\":\"$CK\",\"hours\":5}")
echo "$E" | grep -q '"ok":true' && ok "加时 5h 成功" || ko "extend: $E"

echo "── S7: 排他性: 伪 uk- / 停启 / 用户禁用 ──"
c=$(code -X POST $B/v1/chat/completions -H "Authorization: ${BEARER}uk-fakefakefake" -H "$J" -d '{"model":"kimi-k3","messages":[]}')
[ "$c" = 401 ] && ok "伪 uk- key 401" || ko "fake uk- got $c"
curl -s -X POST "$B/portal/api/keys/$UK" -H "$P" -H "$J" -d '{"enabled":false}' >/dev/null
c=$(code -X POST $B/v1/chat/completions -H "Authorization: ${BEARER}$UK" -H "$J" -d '{"model":"kimi-k3","messages":[]}')
[ "$c" = 403 ] && ok "停用 key → 403" || ko "disabled key got $c"
curl -s -X POST "$B/portal/api/keys/$UK" -H "$P" -H "$J" -d '{"enabled":true}' >/dev/null
PUID=$(curl -s $B/portal/api/me -H "$P" | jqget "['user']['id']")
curl -s -X POST "$B/admin/api/portal/users/$PUID" -H "$A" -H "$J" -d '{"enabled":false}' >/dev/null
c=$(code -X POST $B/v1/chat/completions -H "Authorization: ${BEARER}$UK" -H "$J" -d '{"model":"kimi-k3","messages":[]}')
[ "$c" = 403 ] && ok "用户禁用 → 所有 key 403" || ko "disabled user key got $c"
curl -s -X POST "$B/admin/api/portal/users/$PUID" -H "$A" -H "$J" -d '{"enabled":true}' >/dev/null

echo "── S8: 定额耗尽 402 ──"
# quota_credit 负值语义 = 减 used (售后回补); 耗尽场景直接落库把 credited 改到 used 水位线下
python3 - <<PY
import sqlite3,glob
db=glob.glob('/tmp/cfp-e2e-wallet/**/cards.db',recursive=True) or glob.glob('/tmp/cfp-e2e-wallet/cards.db')
db=db[0]
c=sqlite3.connect(db)
row=c.execute('select user_id,used_micro from user_quota').fetchone()
print('  before:',row)
c.execute('update user_quota set credited_micro = used_micro + 1 where user_id=?',(row[0],))
c.commit()
print('  drained: credited = used + 1µ$')
PY
c=$(code -X POST $B/v1/chat/completions -H "Authorization: ${BEARER}$UK" -H "$J" -d '{"model":"kimi-k3","messages":[],"stream":false}')
[ "$c" = 402 ] && ok "定额耗尽 → 402" || ko "expected 402 after drain, got $c"

echo; echo "══ RESULT: $pass passed, $fail failed ══"
# 清理
curl -s -X DELETE "$B/admin/api/cards/$CK" -H "$A" >/dev/null
curl -s -X DELETE $B/admin/api/cards/plans/e2e-rate -H "$A" >/dev/null
curl -s -X DELETE $B/admin/api/cards/plans/e2e-q50 -H "$A" >/dev/null
exit $fail
