#!/usr/bin/env bash
# P0 E2E on isolated instance 8899. Upstream = TCP blackhole (127.0.0.1:9911, holds 3s) with
# timeout_s=3 → every admitted request occupies its card lane ~9s (3 retries) and ends 502.
# Gate rejections are 401/402/403/429; so 502 == "passed the card gate, reached upstream".
set -u
B=http://127.0.0.1:8899
A="Authorization: Bearer e2e-admin-tok"
J="Content-Type: application/json"
pass=0; fail=0
ok(){ echo "  ✅ $1"; pass=$((pass+1)); }
ko(){ echo "  ❌ $1"; fail=$((fail+1)); }
code(){ curl -s -o /dev/null -w '%{http_code}\n' "$@"; }
issue(){ curl -s -X POST $B/admin/api/cards/issue -H "$A" -H "$J" -d "{\"plan_id\":\"$1\",\"owner\":\"e2e\"}" \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["issued"][0]["card_key"])'; }
field(){ curl -s $B/admin/api/cards/$1 -H "$A" | python3 -c "import sys,json;print(json.load(sys.stdin).get('$2'))"; }

echo "── S1: B1 定额卡在途预扣 (3 并发 × $0.215 估算 vs 面值 $0.30) ──"
curl -s -X POST $B/admin/api/cards/plans -H "$A" -H "$J" -d '{
 "id":"e2e-q","name":"e2e quota","price":1,"kind":"quota","face_usd":0.30,"duration_hours":24,
 "max_concurrency":4,"rpm_limit":1000,"fair_use_rpd":0,"abuse_score_threshold":0}' >/dev/null
CARD=$(issue e2e-q); echo "  card=$CARD"
for i in 1 2 3; do
  (code -X POST $B/v1/chat/completions -H "Authorization: Bearer $CARD" -H "$J" \
     -d '{"model":"claude-opus-5","messages":[],"stream":false}' > /tmp/cfp-e2e/s1_$i.code) &
  sleep 0.2
done
sleep 0.5
inf_mid=$(field $CARD in_flight)
wait
c1=$(tr -d "\n" < /tmp/cfp-e2e/s1_1.code); c2=$(tr -d "\n" < /tmp/cfp-e2e/s1_2.code); c3=$(tr -d "\n" < /tmp/cfp-e2e/s1_3.code)
echo "  codes: $c1 $c2 $c3   in_flight(mid)=$inf_mid"
[ "$c1" = 502 ] && [ "$c2" = 502 ] && [ "$c3" = 402 ] && ok "1st/2nd 放行到上游(502), 3rd 被在途预扣挡下(402) — 旧版会 3 个全放" || ko "expected 502 502 402, got $c1 $c2 $c3"
[ "$inf_mid" = 2 ] && ok "在途时 in_flight=2 (402 那个未占车道)" || ko "in_flight mid should be 2, got $inf_mid"
fu=$(field $CARD face_used_usd); inf=$(field $CARD in_flight)
[ "$fu" = "0.0" ] && ok "上游失败且 0 token → 不扣款 face_used=$fu (hold 已归还)" || ko "face_used should be 0.0, got $fu"
[ "$inf" = 0 ] && ok "结束后 in_flight=$inf" || ko "in_flight should be 0, got $inf"
# 402 文案
(curl -s -o /dev/null -X POST $B/v1/chat/completions -H "Authorization: Bearer $CARD" -H "$J" -d '{"model":"claude-opus-5","messages":[],"stream":false}') &
(curl -s -o /dev/null -X POST $B/v1/chat/completions -H "Authorization: Bearer $CARD" -H "$J" -d '{"model":"claude-opus-5","messages":[],"stream":false}') &
sleep 0.6
MSG=$(curl -s -X POST $B/v1/chat/completions -H "Authorization: Bearer $CARD" -H "$J" -d '{"model":"claude-opus-5","messages":[],"stream":false}')
echo "  3rd body: $(echo "$MSG" | head -c 200)"
echo "$MSG" | grep -q 'in flight' && ok "402 文案带 in-flight 金额" || ko "402 message lacks 'in flight'"
wait

echo "── S2: B8 车道排队替代 429 (1 并发卡, 3 个同时到) ──"
rm -f /tmp/cfp-e2e/s2_*.code
curl -s -X POST $B/admin/api/cards/plans -H "$A" -H "$J" -d '{
 "id":"e2e-u1","name":"e2e unl 1conc","price":1,"kind":"unlimited","duration_hours":24,
 "max_concurrency":1,"rpm_limit":1000,"fair_use_rpd":0,"abuse_score_threshold":0}' >/dev/null
CU=$(issue e2e-u1)
t0=$(date +%s%N)
for i in 1 2 3; do
  (code -X POST $B/v1/chat/completions -H "Authorization: Bearer $CU" -H "$J" \
     -d '{"model":"kimi-k3","messages":[],"stream":false}' > /tmp/cfp-e2e/s2_$i.code) &
done
sleep 1; inf_q=$(field $CU in_flight)
wait
t1=$(date +%s%N); ms=$(( (t1-t0)/1000000 ))
codes=$(cat /tmp/cfp-e2e/s2_*.code | tr '\n' ' ')
echo "  codes: $codes  wall=${ms}ms  in_flight(during)=$inf_q"
n429=$(cat /tmp/cfp-e2e/s2_*.code | grep -c '^429$'); n502=$(cat /tmp/cfp-e2e/s2_*.code | grep -c '^502$')
[ "$n429" = 0 ] && [ "$n502" = 3 ] && ok "0×429, 3 个全部排队后放行 — 旧版会有 2 个立刻 429" || ko "expected 0×429 3×502, got: $codes"
[ "$inf_q" = 1 ] && ok "排队期间 in_flight 恒为 1 (车道未被突破)" || ko "in_flight during queue should be 1, got $inf_q"
# 每个请求占车道 ~9s; 串行 ⇒ 墙钟 ≥ 2×9=18s; 若被并行放行只需 ~9s
[ "$ms" -ge 17000 ] && ok "墙钟 ${ms}ms ≥ 17s ⇒ 逐个放行, 非并行" || ko "wall ${ms}ms too fast — not queued?"

echo "── S3: 伪卡 fail-closed / 模型注册表 ──"
[ "$(code -X POST $B/v1/chat/completions -H 'Authorization: Bearer card-deadbeef' -H "$J" -d '{"model":"kimi-k3","messages":[]}')" = 401 ] && ok "伪卡 401" || ko "fake card not 401"
curl -s -X POST $B/admin/api/models -H "$A" -H "$J" -d '{"model":"zeta-e2e","input_per_m":4,"output_per_m":20,"cache_read_per_m":0.4,"tier":"standard"}' >/dev/null
curl -s "$B/admin/api/models" -H "$A" | grep -q 'zeta-e2e' && ok "注册表写入 zeta-e2e (-fast ×2 倍率由单测 registry_prefix_hit_keeps_fast_multiplier 覆盖)" || ko "registry upsert failed"
curl -s -X DELETE "$B/admin/api/models/zeta-e2e" -H "$A" >/dev/null

echo "── S4: B4 定额结算写 cards.db 而非重写 cards.json ──"
sqlite3 /tmp/cfp-e2e/cards.db '.schema card_face_used' | grep -q face_used_micro && ok "cards.db / card_face_used 表已建" || ko "schema missing"

echo; echo "══ RESULT: $pass passed, $fail failed ══"
curl -s -X DELETE $B/admin/api/cards/$CARD -H "$A" >/dev/null; curl -s -X DELETE $B/admin/api/cards/$CU -H "$A" >/dev/null
curl -s -X DELETE $B/admin/api/cards/plans/e2e-q -H "$A" >/dev/null; curl -s -X DELETE $B/admin/api/cards/plans/e2e-u1 -H "$A" >/dev/null
exit $fail
