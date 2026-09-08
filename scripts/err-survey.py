#!/usr/bin/env python3
"""Survey error classes in proxy.log + billing.db for the retail edition.
Read-only. Usage: python3 scripts/err-survey.py [hours=24]
"""
import json, os, sqlite3, sys, time
from collections import Counter, defaultdict

D = os.path.expanduser("~/.local/share/cursor-fast-proxy-rs")
hours = float(sys.argv[1]) if len(sys.argv) > 1 else 24
since_ms = int((time.time() - hours * 3600) * 1000)

# ---------- proxy.log ----------
rows = []
bad = 0
with open(os.path.join(D, "proxy.log"), errors="replace") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            bad += 1
print(f"proxy.log rows={len(rows)} unparsable={bad}")
if rows:
    keys = Counter()
    for r in rows[-200:]:
        keys.update(r.keys())
    print("keys(last200):", sorted(keys))
    # find ts key
    sample = rows[-1]
    print("sample last row:", json.dumps(sample, ensure_ascii=False)[:600])

from datetime import datetime, timezone
def ts_of(r):
    for k in ("ts_ms", "ts", "time", "timestamp"):
        if k in r:
            v = r[k]
            if isinstance(v, (int, float)):
                return v if v > 1e12 else v * 1000
            if isinstance(v, str):
                try:
                    dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.timestamp() * 1000
                except Exception:
                    return None
    return None

recent = [r for r in rows if (ts_of(r) or 0) >= since_ms]
print(f"\nproxy.log rows in last {hours}h: {len(recent)}")
st = Counter()
for r in recent:
    st[(r.get("status"), r.get("kind"), r.get("rejected"))] += 1
print("status/kind/rejected:")
for k, v in st.most_common(30):
    print(f"  {v:6d}  {k}")

# error strings
errs = Counter()
err_by_model = defaultdict(Counter)
for r in recent:
    s = r.get("status")
    if isinstance(s, int) and s >= 400:
        e = r.get("error") or r.get("error_msg") or r.get("message") or ""
        e = str(e)[:160]
        errs[(s, e)] += 1
        err_by_model[r.get("model")][s] += 1
print("\nTop error (status, text):")
for k, v in errs.most_common(40):
    print(f"  {v:6d}  {k}")
print("\nerrors by model:")
for m, c in sorted(err_by_model.items(), key=lambda x: -sum(x[1].values())):
    print(f"  {sum(c.values()):6d}  {m}  {dict(c)}")

# ---------- billing.db ----------
db = os.path.join(D, "billing.db")
con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
cols = [r[1] for r in con.execute("PRAGMA table_info(billing_records)")]
print("\nbilling_records cols:", cols)
q = f"SELECT status, COUNT(*) FROM billing_records WHERE ts_ms>{since_ms} GROUP BY 1 ORDER BY 2 DESC"
print("\nledger status counts:")
for r in con.execute(q):
    print("  ", r)
q = f"SELECT model, status, COUNT(*), ROUND(AVG(latency_ms)) FROM billing_records WHERE ts_ms>{since_ms} AND status>=400 GROUP BY 1,2 ORDER BY 3 DESC LIMIT 40"
print("\nledger failures by model/status:")
for r in con.execute(q):
    print("  ", r)
if "error_msg" in cols:
    q = f"SELECT status, substr(error_msg,1,140), COUNT(*) FROM billing_records WHERE ts_ms>{since_ms} AND status>=400 GROUP BY 1,2 ORDER BY 3 DESC LIMIT 40"
    print("\nledger error_msg:")
    for r in con.execute(q):
        print("  ", r)
q = f"SELECT account, status, COUNT(*) FROM billing_records WHERE ts_ms>{since_ms} AND status>=400 GROUP BY 1,2 ORDER BY 3 DESC LIMIT 30"
print("\nledger failures by account/status:")
for r in con.execute(q):
    print("  ", r)
