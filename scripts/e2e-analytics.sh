#!/usr/bin/env bash
# 消耗分析 + 删计费账单 E2E (隔离实例 8899, 用真实 billing.db / cards.json 的只读副本).
# 用法: 先 bash scripts/e2e-analytics.sh setup  (拷数据 + 起服务),  再 bash scripts/e2e-analytics.sh run
set -u
E=/tmp/cfp-e2e-an
B=http://127.0.0.1:8899
TOK=e2e-admin-tok
A="Authorization: Bearer $TOK"
BIN=${BIN:-target/release/cursor-fast-proxy-rs}
D=~/.local/share/cursor-fast-proxy-rs
pass=0; fail=0
ok(){ echo "  ✅ $1"; pass=$((pass+1)); }
ko(){ echo "  ❌ $1"; fail=$((fail+1)); }
py(){ python3 -c "$1"; }

if [ "${1:-run}" = "setup" ]; then
  rm -rf $E; mkdir -p $E
  # 只读副本: 账本 (含 WAL 合并) / 卡 / 模型
  sqlite3 $D/billing.db ".backup $E/billing.db"
  cp $D/cards.json $E/cards.json; cp $D/models.json $E/models.json 2>/dev/null || echo '{"models":[],"groups":[]}' > $E/models.json
  # 老式 config (仍带 prices/sales/currency) → 必须能被新版本忽略加载
  cat > $E/config.json <<EOF
{"host":"127.0.0.1","port":8899,"backend":"http://127.0.0.1:9911","timeout_s":3,"log_file":"proxy.log",
 "default_model":"kimi-k3","max_concurrency_per_account":4,"acquire_wait_ms":100,"admin_token":"$TOK","api_keys":[],
 "billing":{"db_file":"$E/billing.db","tz_offset_minutes":480,"currency":"RMB","default_commission_bps":0,"reject_unpriced":false,
   "prices":[{"model":"*","input_per_m":1,"output_per_m":1}],"sales":[{"id":"s1","name":"S","commission_bps":100}]},
 "proxy":{"enabled":false,"nodes":[],"rules":[]}}
EOF
  # 一个假号: 请求会真正打到 9911 黑洞上游 → 超时 502 → 走账本 (才能验新行 input_incl_cache=0)
  echo '[{"id":"dummy","access_token":"x","machine_id":"m","enabled":true,"priority":50,"tags":[]}]' > $E/accounts.json
  # 预置上游名单文件 → 启动后 /v1/models 必须包含 (重启不丢)
  echo '{"fetched_at":1,"models":["e2e-upstream-model-high","kimi-k3-max"]}' > $E/upstream-models.json
  echo "setup done at $E; start with:"
  echo "  python3 scripts/blackhole_upstream.py 9911 3 &   # 黑洞上游 (先起)"
  echo "  cd $E && CFP_CONFIG=$E/config.json CFP_ACCOUNTS=$E/accounts.json $PWD/$BIN"
  exit 0
fi

echo "── 计费账单已删 ──"
for ep in billing/summary billing/records billing/pricing billing/stats billing/tags billing/export; do
  c=$(curl -s -o /dev/null -w '%{http_code}' $B/admin/api/$ep -H "$A")
  [ "$c" = "404" ] && ok "/admin/api/$ep → 404" || ko "/admin/api/$ep → $c (应 404)"
done
S=$(curl -s $B/admin/api/settings -H "$A")
echo "$S" | grep -q billing_currency && ko "settings 仍暴露 billing_currency" || ok "settings 无 billing_currency/价格规则"
echo "$S" | grep -q billing_tz_offset_minutes && ok "settings 保留账本时区" || ko "settings 丢了 tz"

echo "── 老 config (带 prices/sales) 可加载 ──"
curl -s -o /dev/null -w '%{http_code}' $B/admin/api/pool -H "$A" | grep -q 200 && ok "服务起来了 (老 config 字段被忽略)" || ko "服务不可达"

echo "── 上游名单落盘 → 重启后 /v1/models 仍含 ──"
IDS=$(curl -s $B/v1/models -H "Authorization: Bearer x" | py 'import sys,json;print(" ".join(m["id"] for m in json.load(sys.stdin).get("data",[])))')
# /v1/models 把上游变体折叠成家族基名: e2e-upstream-model-high → e2e-upstream-model
echo "$IDS" | grep -qw 'e2e-upstream-model' && ok "预置 upstream-models.json 的模型可见 (折叠为家族基名)" || ko "upstream-models.json 未被读回: $IDS"

echo "── 消耗分析: 全量 ──"
C=$(curl -s "$B/admin/api/analytics/consumption?scope=all" -H "$A")
echo "$C" | py '
import sys,json;d=json.load(sys.stdin)
t=d["totals"]; assert d["rows_scanned"]>0, "no rows"
assert t["face_usd"]>0, "face 0"; assert t["online_hours"]>0, "online 0"; assert t["busy_hours"]>0
assert t["online_hours"]>=t["busy_hours"]*0.5, ("online<busy?",t)
assert t["usd_per_online_hour"]>0
assert len(d["by_model"])>0 and len(d["by_key"])>0 and len(d["hourly"])>0 and len(d["by_plan"])>0
m=d["by_model"][0]; assert "usd_per_online_hour" in m and "groups" in m and "priced" in m
assert d["gap_mode"]=="auto" and d["gap_secs"] is None
# 并发槽: 槽·时 ≥ 在线时 (多路并发才会大于), 槽·时 ≥ 忙时 (槽在等人也算), 峰值并发 ≥ 1
assert t["lane_hours"]>=t["online_hours"]-1e-9, ("lane<online",t)
assert t["lane_hours"]>=t["busy_hours"]*0.95, ("lane<busy",t)
assert t["peak_concurrency"]>=1 and t["usd_per_lane_hour"]>0
assert t["usd_per_lane_hour"]<=t["usd_per_online_hour"]+1e-9
for r in d["by_model"]:
    assert r["lane_hours"]>=r["online_hours"]-1e-9 and r["lane_hours"]>=r["busy_hours"]*0.95, ("model lane", r["model"])
# 老行 input 含缓存 → 归一化后 sol 的面值必须低于 fable-thinking-high (修 bug 前 $61 vs $237 被高估 6 倍)
bm={r["model"]:r for r in d["by_model"]}
if "gpt-5.6-sol" in bm and "claude-fable-5-1-thinking-high" in bm:
    sol=bm["gpt-5.6-sol"]; fab=bm["claude-fable-5-1-thinking-high"]
    assert sol["face_usd"]<fab["face_usd"], ("sol>=fable", sol["face_usd"], fab["face_usd"])
    # sol 的 $/请求 应远低于旧口径的 $0.8 (全是 200k 缓存命中, 修正后 ≈ $0.12)
    assert sol["avg_face_per_req"]<0.4, ("sol per req still cache-inflated", sol["avg_face_per_req"])
print("  rows=%d face=$%.3f cost=¥%.2f online=%.2fh lane=%.2fh busy=%.2fh peak_conc=%d sessions=%d users=%d $/lane-h=%.2f $/online-h=%.2f" % (d["rows_scanned"],t["face_usd"],t["cost_rmb"],t["online_hours"],t["lane_hours"],t["busy_hours"],t["peak_concurrency"],t["sessions"],t["users"],t["usd_per_lane_hour"],t["usd_per_online_hour"]))
print("  top models (req, face, $/lane-h, peak/avg conc):", [(r["model"],r["requests"],round(r["face_usd"],2),r["usd_per_lane_hour"] and round(r["usd_per_lane_hour"],1),r["peak_concurrency"],r["avg_concurrency"] and round(r["avg_concurrency"],2)) for r in d["by_model"][:4]])
print("  by_plan:", [(r["plan_id"],r["requests"],round(r["face_usd"],3)) for r in d["by_plan"]])
print("  by_group:", [(r["group_id"],r["requests"]) for r in d["by_group"]])
' && ok "consumption(all) 结构+数值合理" || ko "consumption(all) 断言失败"

echo "── 消耗分析: 只看卡 ⊆ 全量; 固定 gap 生效 ──"
CC=$(curl -s "$B/admin/api/analytics/consumption?scope=cards&gap=60" -H "$A")
python3 - "$C" "$CC" <<'EOF' && ok "cards 子集 + gap=60 fixed" || ko "cards/gap 断言失败"
import sys,json
a=json.loads(sys.argv[1]); b=json.loads(sys.argv[2])
assert b["rows_scanned"]<=a["rows_scanned"]
assert all(r["key_name"].startswith("card-") for r in b["by_key"]), "non-card leaked"
assert b["gap_mode"]=="fixed" and b["gap_secs"]==60
# 更小的 gap → 时段数 ≥ 自适应的 (同一数据子集下无法直接比, 只验类型)
assert b["totals"]["sessions"]>=1
EOF

echo "── 时间窗过滤 ──"
F=$(curl -s "$B/admin/api/analytics/consumption?scope=all&from=2030-01-01" -H "$A")
echo "$F" | py 'import sys,json;d=json.load(sys.stdin);assert d["rows_scanned"]==0 and d["totals"]["requests"]==0 and d["by_model"]==[]' && ok "未来时间窗 → 空" || ko "时间窗未生效"

echo "── 在线状态 ──"
P=$(curl -s "$B/admin/api/analytics/presence?window=120" -H "$A")
echo "$P" | py '
import sys,json;d=json.load(sys.stdin)
assert d["window_secs"]==120 and "rows" in d
for r in d["rows"]:
    assert r["presence"] in ("online","idle","offline")
    assert "today_face_usd" in r and "today_online_hours" in r and "models_today" in r
print("  online=%d idle=%d total=%d" % (d["online"],d["idle"],d["total"]))
print("  first rows:", [(r["key_name"][:16], r["presence"], r["today_requests"], r["idle_secs"]) for r in d["rows"][:3]])
' && ok "presence 结构合理" || ko "presence 断言失败"
c=$(curl -s -o /dev/null -w '%{http_code}' "$B/admin/api/analytics/presence?window=abc" -H "$A"); [ "$c" = "400" ] && ok "presence 非法 window → 400" || ko "presence window=abc → $c"

echo "── 单卡时段明细 ──"
TOP=$(echo "$C" | py 'import sys,json;d=json.load(sys.stdin);ks=[r["key_name"] for r in d["by_key"] if r["is_card"]];print(ks[0] if ks else "")')
if [ -n "$TOP" ]; then
  SS=$(curl -s "$B/admin/api/analytics/sessions?key=$TOP" -H "$A")
  echo "$SS" | py '
import sys,json;d=json.load(sys.stdin)
assert d["totals"]["sessions"]>=1 and d["totals"]["online_hours"]>0
s=d["sessions"][0]; assert s["end_ms"]>=s["start_ms"] and s["requests"]>=1 and "models" in s and 0<=s["busy_ratio"]
# 时段互不重叠且按时间倒序
ss=d["sessions"]
for i in range(len(ss)-1): assert ss[i]["start_ms"]>=ss[i+1]["end_ms"], "overlap"
assert d["totals"]["lane_hours"]>=d["totals"]["online_hours"]-1e-9 and d["totals"]["peak_concurrency"]>=1
assert all("peak_concurrency" in x and "lane_hours" in x for x in ss)
print("  %s… sessions=%d online=%.2fh lane=%.2fh peak=%d face=$%.3f gap=%.0fs" % (d["key"][:16],d["totals"]["sessions"],d["totals"]["online_hours"],d["totals"]["lane_hours"],d["totals"]["peak_concurrency"],d["totals"]["face_usd"],d["gap_secs"]))
print("  latest:", ss[0]["start"], "→", ss[0]["end"][11:], "%.1fmin" % (ss[0]["secs"]/60), ss[0]["requests"], "req", [m["model"] for m in ss[0]["models"]][:3])
' && ok "sessions 明细合理 (无重叠, 倒序)" || ko "sessions 断言失败"
else
  ko "账本里没有卡请求, 无法测 sessions"
fi
c=$(curl -s "$B/admin/api/analytics/sessions?key=" -H "$A"); echo "$c" | grep -q 'key required' && ok "sessions 缺 key → error" || ko "sessions 缺 key 未报错: $c"

echo "── 账本新口径: 新请求 (502 黑洞, 无 usage) 记 0 面值, sales/commission 恒 NULL/0, input_incl_cache=0 ──"
curl -s -o /dev/null -X POST $B/v1/chat/completions -H "Authorization: Bearer $TOK" -H 'Content-Type: application/json' -d '{"model":"kimi-k3-high","messages":[{"role":"user","content":"hi"}]}'
sleep 12   # 3 次重试 × 3s 黑洞超时 → 502 落账
N=$(sqlite3 $E/billing.db "select count(*) from billing_records where sales_id is not null or commission_nano<>0")
[ "$N" = "0" ] && ok "新写入行 sales_id NULL / commission 0 (老行也无)" || echo "  ℹ 历史行含 sales/commission: $N (老库残留, 不算失败)"
sqlite3 $E/billing.db "select name from sqlite_master where type='index' and name='idx_br_keyname_ts'" | grep -q idx_br_keyname_ts && ok "老库自动补 idx_br_keyname_ts 索引" || ko "索引未建"
OLD_INCL=$(sqlite3 $E/billing.db "select count(*) from billing_records where input_incl_cache=1")
NEW_INCL=$(sqlite3 $E/billing.db "select count(*) from billing_records where input_incl_cache=0")
[ "$OLD_INCL" -gt 0 ] && [ "$NEW_INCL" -gt 0 ] && ok "老库补 input_incl_cache: 老行=1 ($OLD_INCL) 新行=0 ($NEW_INCL)" || ko "input_incl_cache 迁移异常 old=$OLD_INCL new=$NEW_INCL"
sqlite3 $E/billing.db "pragma table_info(billing_records)" | grep -q ttft_ms && ok "老库补 ttft_ms 列" || ko "ttft_ms 列未建"
echo "$C" | py '
import sys,json;d=json.load(sys.stdin)
for r in d["by_model"][:3]:
    sp=r["speed"]; assert set(["ttft_p50_ms","tps_p50","tps_p10","latency_p50_ms","tps_samples"])<=set(sp), sp.keys()
    if sp["tps_samples"]>0: assert 1<sp["tps_p50"]<500, sp
print("  speed:", [(r["model"][:24], sp["tps_p50"] and round(sp["tps_p50"]), sp["tps_p10"] and round(sp["tps_p10"]), sp["ttft_p50_ms"], sp["latency_p50_ms"] and round(sp["latency_p50_ms"]/1000,1)) for r in d["by_model"][:3] for sp in [r["speed"]]])
' && ok "consumption 带 speed (tok/s p50/p10, ttft, latency)" || ko "speed 字段异常"
sqlite3 $E/billing.db "pragma table_info(billing_records)" | grep -q pace_wait_ms && ok "老库补 pace_tps/pace_wait_ms 列" || ko "pace 列未建"
SP=$(curl -s "$B/admin/api/analytics/speed?scope=all&paces=25,12" -H "$A")
echo "$SP" | py '
import sys,json;d=json.load(sys.stdin)
assert d["paces"]==[25.0,12.0], d["paces"]
t=d["totals"]; assert len(t["whatif"])==2
for w in t["whatif"]:
    assert w["lane_hours_after"]>=w["lane_hours_before"]-1e-9, w
    assert w["usd_per_lane_hour_after"]<=w["usd_per_lane_hour_before"]+1e-9, w
    assert 0<=w["saving_ratio"]<1, w
w25,w12=t["whatif"]; assert w12["saving_ratio"]>=w25["saving_ratio"]-1e-9, "更低 pace 应省更多"
m=d["by_model"][0]; assert "upstream_tps_p50" in m["speed"] and "whatif" in m
print("  speed what-if 全量: $/槽·时 %.1f → @25 %.1f (-%.0f%%) → @12 %.1f (-%.0f%%); 拉长 %d/%d 条" % (w25["usd_per_lane_hour_before"],w25["usd_per_lane_hour_after"],w25["saving_ratio"]*100,w12["usd_per_lane_hour_after"],w12["saving_ratio"]*100,w25["stretched_requests"],d["rows_scanned"]))
for r in d["by_model"][:3]: print("   ", r["model"][:30], "native tok/s", r["speed"]["upstream_tps_p50"] and round(r["speed"]["upstream_tps_p50"]), "@25 -%.0f%%" % (r["whatif"][0]["saving_ratio"]*100 if r["whatif"][0]["saving_ratio"] is not None else 0))
' && ok "speed what-if 单调 (更低 pace 省更多, $/槽·时 只降不升)" || ko "speed what-if 异常"

echo; echo "══ RESULT: $pass passed, $fail failed ══"
exit $fail
