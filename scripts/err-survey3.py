#!/usr/bin/env python3
"""Zoom: 09-07 16h cluster, RPM/0 rows, 401 on live card, 502 fable 10-13h, per-minute timeline."""
import json, os, sys
from collections import Counter, defaultdict
from datetime import datetime, timezone

D = os.path.expanduser("~/.local/share/cursor-fast-proxy-rs")
rows = []
with open(os.path.join(D, "proxy.log"), errors="replace") as f:
    for line in f:
        try:
            rows.append(json.loads(line))
        except Exception:
            pass

def t(r):
    return r.get("ts", "")[:19]

print("=== 09-07T16 cluster (status>=500) per minute, account, model, latency ===")
c = Counter()
for r in rows:
    if r.get("ts", "").startswith("2026-09-07T16") and (r.get("status") or 0) >= 500:
        c[(r["ts"][11:16], r.get("status"), r.get("account"), r.get("model"), r.get("key_prefix"))] += 1
for k, v in sorted(c.items()):
    print(f"  {v:3d}  {k}")

print("\n=== all rows 09-07T16:35..17:00 status counts per minute ===")
c = Counter()
for r in rows:
    ts = r.get("ts", "")
    if "2026-09-07T16:35" <= ts <= "2026-09-07T17:59":
        c[(ts[11:16], r.get("status"))] += 1
for k, v in sorted(c.items()):
    print(f"  {v:3d}  {k}")

print("\n=== RPM x/0 rows ===")
for r in rows:
    if r.get("status") == 429 and "/0)" in str(r.get("reason")):
        print("  ", t(r), r.get("key_prefix"), r.get("model"), r.get("client_ip"), r.get("reason"))
        break
n = sum(1 for r in rows if r.get("status") == 429 and "/0)" in str(r.get("reason")))
print("  total:", n)
ts_rpm0 = sorted(t(r) for r in rows if r.get("status") == 429 and "/0)" in str(r.get("reason")))
print("  range:", ts_rpm0[0] if ts_rpm0 else None, "..", ts_rpm0[-1] if ts_rpm0 else None)

print("\n=== 401 rows on card-cd0f30da / card-5ff935f1 ===")
for r in rows:
    if r.get("status") == 401 and r.get("key_prefix") in ("card-cd0f30da", "card-5ff935f1"):
        print("  ", t(r), r.get("key_prefix"), r.get("model"), r.get("client_ip"))

print("\n=== 400 rows ===")
for r in rows:
    if r.get("status") == 400:
        print("  ", t(r), r.get("key_prefix"), r.get("model"), r.get("client_ip"), r.get("reason"))

print("\n=== card-61d15daa timeline (status per 10min) ===")
c = Counter()
for r in rows:
    if r.get("key_prefix") == "card-61d15daa":
        c[(r["ts"][:15], r.get("status"))] += 1
for k, v in sorted(c.items()):
    print(f"  {v:3d}  {k}")

print("\n=== fable 502 rows 09-07T10..13 details (first 12) ===")
n = 0
for r in rows:
    ts = r.get("ts", "")
    if "2026-09-07T10" <= ts <= "2026-09-07T13:59" and r.get("status") == 502:
        print("  ", t(r), r.get("account"), r.get("key_prefix"), r.get("latency_ms"), r.get("input_tokens"), r.get("output_tokens"), r.get("stream"))
        n += 1
        if n >= 12:
            break

print("\n=== grok-4.6 502 cluster 09-06T21..23: latency + interleaved 200s? ===")
c = Counter()
for r in rows:
    ts = r.get("ts", "")
    if "2026-09-06T21" <= ts <= "2026-09-06T23:59" and r.get("model") == "grok-4.6":
        c[(ts[11:13], r.get("status"), r.get("key_prefix"))] += 1
for k, v in sorted(c.items()):
    print(f"  {v:3d}  {k}")
# any grok-4.6 200 in that window?
lat = sorted(r.get("latency_ms", 0) for r in rows if "2026-09-06T21" <= r.get("ts", "") <= "2026-09-06T23:59" and r.get("model") == "grok-4.6" and r.get("status") == 502)
if lat:
    print("  502 latency p10/p50/p90:", lat[len(lat)//10], lat[len(lat)//2], lat[9*len(lat)//10])

print("\n=== fable 503 cluster 09-06T17..18: per key/ip/account + interleaved 200 ===")
c = Counter()
for r in rows:
    ts = r.get("ts", "")
    if "2026-09-06T17" <= ts <= "2026-09-06T18:59" and r.get("model") == "claude-fable-5-1-thinking-max":
        c[(ts[11:13], r.get("status"), r.get("account"))] += 1
for k, v in sorted(c.items()):
    print(f"  {v:3d}  {k}")
