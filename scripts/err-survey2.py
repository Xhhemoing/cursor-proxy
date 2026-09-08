#!/usr/bin/env python3
"""Deep dive: reasons for rejected rows, none-status rows, time pattern for 502/503."""
import json, os, sys, time
from collections import Counter, defaultdict
from datetime import datetime, timezone

D = os.path.expanduser("~/.local/share/cursor-fast-proxy-rs")
hours = float(sys.argv[1]) if len(sys.argv) > 1 else 24
since_ms = int((time.time() - hours * 3600) * 1000)

def ts_of(r):
    v = r.get("ts")
    if isinstance(v, str):
        try:
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp() * 1000
        except Exception:
            return None
    return None

rows = []
with open(os.path.join(D, "proxy.log"), errors="replace") as f:
    for line in f:
        try:
            rows.append(json.loads(line))
        except Exception:
            pass
recent = [r for r in rows if (ts_of(r) or 0) >= since_ms]

none = [r for r in recent if r.get("status") is None]
print(f"status=None rows: {len(none)}")
kc = Counter()
for r in none:
    kc[tuple(sorted(r.keys()))] += 1
for k, v in kc.most_common(5):
    print("  ", v, k)
for r in none[:3]:
    print("  sample:", json.dumps(r, ensure_ascii=False)[:400])

print("\n=== rejected rows: status / reason ===")
rc = Counter()
for r in recent:
    if r.get("rejected"):
        rc[(r.get("status"), str(r.get("reason"))[:140])] += 1
for k, v in rc.most_common(60):
    print(f"  {v:5d}  {k}")

print("\n=== 429 by key_prefix / model / reason ===")
c = Counter()
for r in recent:
    if r.get("status") == 429:
        c[(r.get("key_prefix"), r.get("model"), str(r.get("reason"))[:80])] += 1
for k, v in c.most_common(20):
    print(f"  {v:5d}  {k}")

print("\n=== 403 by key_prefix / model / reason ===")
c = Counter()
for r in recent:
    if r.get("status") == 403:
        c[(r.get("key_prefix"), r.get("client_ip"), str(r.get("reason"))[:80])] += 1
for k, v in c.most_common(20):
    print(f"  {v:5d}  {k}")

print("\n=== 402 by key_prefix / reason ===")
c = Counter()
for r in recent:
    if r.get("status") == 402:
        c[(r.get("key_prefix"), r.get("model"), str(r.get("reason"))[:120])] += 1
for k, v in c.most_common(20):
    print(f"  {v:5d}  {k}")

print("\n=== 401 by key_prefix / client_ip / reason ===")
c = Counter()
for r in recent:
    if r.get("status") == 401:
        c[(r.get("key_prefix"), r.get("client_ip"), str(r.get("reason"))[:120])] += 1
for k, v in c.most_common(20):
    print(f"  {v:5d}  {k}")

print("\n=== 400 rows ===")
for r in recent:
    if r.get("status") == 400:
        print("  ", json.dumps(r, ensure_ascii=False)[:500])

print("\n=== 502/503 hourly (UTC) by model ===")
h = defaultdict(Counter)
for r in recent:
    if r.get("status") in (502, 503):
        t = datetime.fromtimestamp(ts_of(r) / 1000, tz=timezone.utc).strftime("%m-%d %H")
        h[t][(r.get("status"), r.get("model"))] += 1
for t in sorted(h):
    print(f"  {t}  {dict(h[t].most_common(4))}")

print("\n=== 502/503 by account+model ===")
c = Counter()
for r in recent:
    if r.get("status") in (502, 503):
        c[(r.get("status"), r.get("account"), r.get("model"))] += 1
for k, v in c.most_common(25):
    print(f"  {v:5d}  {k}")

print("\n=== 502/503 latency buckets ===")
for s in (502, 503):
    lat = sorted(r.get("latency_ms") or 0 for r in recent if r.get("status") == s)
    if lat:
        n = len(lat)
        print(f"  {s}: n={n} p10={lat[n//10]} p50={lat[n//2]} p90={lat[9*n//10]} max={lat[-1]}")

print("\n=== 502/503 by client_ip + key_prefix ===")
c = Counter()
for r in recent:
    if r.get("status") in (502, 503):
        c[(r.get("status"), r.get("client_ip"), r.get("key_prefix"))] += 1
for k, v in c.most_common(15):
    print(f"  {v:5d}  {k}")

print("\n=== stream flag on 502/503 ===")
c = Counter((r.get("status"), r.get("stream")) for r in recent if r.get("status") in (502, 503))
print("  ", c)
