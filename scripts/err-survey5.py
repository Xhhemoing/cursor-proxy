#!/usr/bin/env python3
"""Extract tracing events embedded in proxy.log (fields/level/target rows)."""
import json, os
from collections import Counter

D = os.path.expanduser("~/.local/share/cursor-fast-proxy-rs")
ev = []
with open(os.path.join(D, "proxy.log"), errors="replace") as f:
    for line in f:
        try:
            r = json.loads(line)
        except Exception:
            continue
        if "fields" in r and "level" in r:
            ev.append(r)
print("tracing rows:", len(ev), "range:", ev[0]["timestamp"][:19] if ev else None, "..", ev[-1]["timestamp"][:19] if ev else None)

print("\n=== WARN/ERROR events ===")
c = Counter()
for r in ev:
    if r["level"] in ("WARN", "ERROR"):
        f = r["fields"]
        c[(r["level"], f.get("event"), str(f.get("error") or f.get("message"))[:170])] += 1
for k, v in c.most_common(40):
    print(f"  {v:4d} {k}")

print("\n=== capacity_backoff / upstream_error full samples ===")
n = 0
for r in ev:
    f = r["fields"]
    if f.get("event") in ("capacity_backoff", "upstream_error", "auto_disable", "retry"):
        print("  ", r["timestamp"][11:19], r["level"], f.get("event"), "acct=", f.get("account"), "att=", f.get("attempt"), "|", str(f.get("error") or f.get("message"))[:260])
        n += 1
        if n > 60:
            break

print("\n=== startup rows ===")
for r in ev:
    f = r["fields"]
    if f.get("event") in ("startup", "listening", "config_watch", "models_init", "card_load", "quota_cache_restore"):
        print("  ", r["timestamp"][11:19], f)

print("\n=== card_settle_estimated ===")
for r in ev:
    f = r["fields"]
    if f.get("event") == "card_settle_estimated":
        print("  ", r["timestamp"][11:19], {k: v for k, v in f.items() if k != "message"})
