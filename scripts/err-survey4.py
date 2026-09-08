#!/usr/bin/env python3
"""Sample the proxy.log rows without status/kind; check display-name models; card-cd0f30da first seen; models 200 in 16:40-16:54."""
import json, os
from collections import Counter

D = os.path.expanduser("~/.local/share/cursor-fast-proxy-rs")
rows = []
with open(os.path.join(D, "proxy.log"), errors="replace") as f:
    for line in f:
        try:
            rows.append(json.loads(line))
        except Exception:
            pass

nostatus = [r for r in rows if "status" not in r]
print("rows without status:", len(nostatus))
kc = Counter(tuple(sorted(r.keys())) for r in nostatus)
for k, v in kc.most_common(5):
    print("  ", v, k)
for r in nostatus[:2]:
    print("  sample:", json.dumps(r, ensure_ascii=False)[:500])
ev = Counter(r.get("event") or r.get("fields", {}).get("event") for r in nostatus)
print("  events:", ev.most_common(20))

print("\n=== display-name models (with space / capital) ===")
c = Counter()
for r in rows:
    m = str(r.get("model", ""))
    if " " in m or (m[:1].isupper()):
        c[(m, r.get("status"))] += 1
for k, v in sorted(c.items()):
    print(f"  {v:4d} {k}")

print("\n=== card-cd0f30da first/last rows ===")
cd = [r for r in rows if r.get("key_prefix") == "card-cd0f30da" and r.get("ts")]
cd.sort(key=lambda r: r["ts"])
print("  first:", cd[0]["ts"], cd[0].get("status"), cd[0].get("model"))
print("  last :", cd[-1]["ts"], cd[-1].get("status"), cd[-1].get("model"))
print("  rows 15:09..15:12:", [(r["ts"][11:19], r.get("status"), r.get("model")) for r in cd if "2026-09-07T15:09" <= r["ts"] <= "2026-09-07T15:12"])

print("\n=== 16:40..16:55 card-cd0f30da model×status ===")
c = Counter()
for r in rows:
    ts = r.get("ts", "")
    if "2026-09-07T16:40" <= ts <= "2026-09-07T16:55" and r.get("key_prefix") == "card-cd0f30da":
        c[(r.get("model"), r.get("status"))] += 1
for k, v in sorted(c.items(), key=lambda x: -x[1])[:20]:
    print(f"  {v:4d} {k}")

print("\n=== acc-6 / acc-8 rows: models, latency ===")
for a in ("acc-6", "acc-8"):
    rs = [r for r in rows if r.get("account") == a]
    print(a, len(rs), Counter((r.get("model"), r.get("status")) for r in rs).most_common(6))
    lat = sorted(r.get("latency_ms", 0) for r in rs)
    if lat:
        print("   latency p10/p50/p90:", lat[len(lat)//10], lat[len(lat)//2], lat[9*len(lat)//10])

print("\n=== 502 rows 16:40..16:55 by account with latency ===")
for r in rows:
    ts = r.get("ts", "")
    if "2026-09-07T16:40" <= ts <= "2026-09-07T16:55" and r.get("status") == 502:
        print("  ", ts[11:19], r.get("account"), r.get("model"), r.get("latency_ms"), r.get("key_prefix"))
