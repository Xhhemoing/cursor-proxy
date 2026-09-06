#!/usr/bin/env bash
# 换后只读冒烟: 不发推理请求, 只读 admin 接口
set -u
D=$HOME/.local/share/cursor-fast-proxy-rs
B=http://127.0.0.1:8800
TOK=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['admin_token'])" "$D/config.json")
H="Authorization: Bearer $TOK"
echo "billing/summary -> $(curl -s -o /dev/null -w '%{http_code}' $B/admin/api/billing/summary -H "$H") (expect 404)"
echo "v1/models -> $(curl -s $B/v1/models -H "$H" | python3 -c 'import sys,json;print(len(json.load(sys.stdin)["data"]),"families")')"
python3 -c "import json,sys;d=json.load(open(sys.argv[1]));print('upstream-models.json:',len(d.get('models',[])),'models, fetched_at',d.get('fetched_at'))" "$D/upstream-models.json" 2>/dev/null || echo "upstream-models.json: absent (点一次「获取可用模型」后才会落盘)"
echo "input_incl_cache live:"; sqlite3 "$D/billing.db" "select input_incl_cache, count(*) from billing_records group by 1"
curl -s "$B/admin/api/analytics/consumption?scope=all" -H "$H" | python3 -c '
import sys,json;d=json.load(sys.stdin);t=d["totals"]
print("today(all): rows=%d face=$%.2f cost=¥%.2f online=%.2fh lane=%.2fh peak=%d $/lane-h=%.1f $/online-h=%.1f users=%d" % (d["rows_scanned"],t["face_usd"],t["cost_rmb"],t["online_hours"],t["lane_hours"],t["peak_concurrency"],t["usd_per_lane_hour"] or 0,t["usd_per_online_hour"] or 0,t["users"]))
for r in d["by_model"][:6]: print("  %-34s req=%4d face=$%7.2f $/lane-h=%6s peak=%d avg=%s" % (r["model"],r["requests"],r["face_usd"],r["usd_per_lane_hour"] and round(r["usd_per_lane_hour"],1),r["peak_concurrency"],r["avg_concurrency"] and round(r["avg_concurrency"],2)))
for r in d["by_plan"]: print("  plan %-12s cards=%d face=$%.2f worst=%s" % (r["plan_id"],r["users"],r["face_usd"],r.get("worst_case_face_usd") and round(r["worst_case_face_usd"])))'
curl -s "$B/admin/api/analytics/presence" -H "$H" | python3 -c '
import sys,json;d=json.load(sys.stdin);print("presence: online=%d idle=%d total=%d" % (d["online"],d["idle"],d["total"]))
for r in d["rows"][:5]: print("  ",r["key_name"][:18],r["presence"],r["plan_id"],"req",r["today_requests"],"$%.2f"%r["today_face_usd"],"online %.2fh lane %.2fh peak %d"%(r["today_online_hours"],r["today_lane_hours"],r["today_peak_concurrency"]))'
