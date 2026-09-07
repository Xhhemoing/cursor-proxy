"""[核账脚本] 拉 accounts.json 里全部启用号的官方仪表盘: GetSandUsageStatus + GetAggregatedUsageEvents(startDate=周期起点)
输出 /tmp/cursor_official_usage_2.json, 并与 Rust PREMIUM_PRICES 逐模型对账 (官方 cents vs 我们按 token×表价算)。
用法: cd /home/ubuntu/work/cursor-openai-proxy-0.1.21 && .venv/bin/python <repo>/scripts/reconcile-official.py
      (复用 Python 版的 Cursor TLS/CONNECT 通道; ratio 偏离 0.95–1.05 的行标 <<<, 说明表价要改)
下方 PRICES 必须与 src/cards.rs PREMIUM_PRICES 手动同步.
"""
import json, os, sys, time
sys.path.insert(0, '/home/ubuntu/work/cursor-openai-proxy-0.1.21')
from cursor_openai_proxy import connect
from cursor_openai_proxy.connect import dashboard, _as_unix

accs = json.load(open(os.path.expanduser('~/.local/share/cursor-fast-proxy-rs/accounts.json')))
accs = accs if isinstance(accs, list) else accs.get('accounts', accs)

# Rust 内置表 (src/cards.rs PREMIUM_PRICES) — 与代码同步手抄
PRICES = [
 ("gpt-5.4-pro",(30,180,3,0)),("gpt-5.6-cyber",(12.5,75,1.25,15.62)),("claude-fable-5-1",(10,50,0.25,12.5)),("claude-fable-5",(10,50,1,12.5)),
 ("claude-opus-5",(5,25,0.5,6.25)),("claude-opus-4",(5,25,0.5,6.25)),("gpt-5.6-sol-max",(4,20,0.75,2.5)),("gpt-5.6-sol",(4,20,0.4,5)),
 ("gpt-5.6",(4,20,0.4,5)),("gpt-5.6-terra",(2,12,0.2,2.5)),("gpt-5.6-luna",(0.2,1.2,0.02,0.25)),
 ("claude-sonnet-4",(3,15,0.3,3.75)),("claude-sonnet-5",(2,10,0.2,2.5)),("kimi-k3",(3,15,0.3,0)),
 ("kimi-k2",(0.95,4,0.19,0)),("gpt-5.5",(5,30,0.5,0)),("gpt-5.4",(2.5,15,0.25,0)),("gpt-5",(2.5,15,0.25,0)),
 ("claude-4.5-haiku",(1,5,0.1,1.25)),("gemini-3",(0.75,3.75,0.07,0)),("glm-5",(1.4,4.4,0.28,0)),
 ("grok-4.5",(2,6,0.3,0)),("grok-4.6",(2,6,0.5,0)),("cursor-grok-4.6",(2,6,0.5,0)),
]
FALLBACK=(5,25,0.5,6.25)
def price(m):
    base=m[:-5] if m.endswith('-fast') else m
    best=None
    for n,p in PRICES:
        if base==n: best=p;break
        if base.startswith(n) and (best is None or len(n)>best[1]): best=(p,len(n))
    p = best if best is None or isinstance(best[0],(int,float)) else best[0]
    p = p or FALLBACK
    return tuple(x*2 for x in p) if m.endswith('-fast') else p
def face(m,i,o,cr,cw):
    p=price(m); return (i*p[0]+o*p[1]+cr*p[2]+cw*p[3])/1e6

out={}
for a in accs:
    if not a.get('enabled',True): continue
    auth={'access_token':a['access_token'],'machine_id':a['machine_id']}
    try:
        sand=dashboard('GetSandUsageStatus',timeout=20,auth=auth)
        ps=_as_unix(sand.get('currentPeriodStart'))
        body=json.dumps({'startDate': str(int(ps*1000))} if ps else {}).encode()
        agg=dashboard('GetAggregatedUsageEvents',body=body,timeout=25,auth=auth)
    except Exception as e:
        print(a['id'],'ERR',str(e)[:120]); continue
    rows=[]
    for it in agg.get('aggregations') or []:
        rows.append(dict(model=it.get('modelIntent') or it.get('model') or '', cents=float(it.get('totalCents') or 0),
            i=float(it.get('inputTokens') or 0), o=float(it.get('outputTokens') or 0),
            cr=float(it.get('cacheReadTokens') or 0), cw=float(it.get('cacheWriteTokens') or 0), tier=it.get('tier')))
    out[a['id']]=dict(sand={k:sand.get(k) for k in ('currentPeriodStart','nextResetTimestampUtc','usagePercent','hasAvailableUsage')},
        total_cents=agg.get('totalCostCents'), rows=rows)
    tot=sum(r['cents'] for r in rows)/100
    print(f"{a['id']:8s} pct={sand.get('usagePercent'):6.2f} period={sand.get('currentPeriodStart')} reset={sand.get('nextResetTimestampUtc')} models={len(rows)} $={tot:.1f}  pct*10={float(sand.get('usagePercent') or 0)*10:.1f}")
json.dump(out,open('/tmp/cursor_official_usage_2.json','w'),indent=1)

print("\n== 逐模型对账: 官方 cents vs Rust 表价 (合并全部号) ==")
from collections import defaultdict
agg=defaultdict(lambda:[0,0,0,0,0])
for v in out.values():
    for r in v['rows']:
        a=agg[r['model']]; a[0]+=r['cents']; a[1]+=r['i']; a[2]+=r['o']; a[3]+=r['cr']; a[4]+=r['cw']
print(f"{'model':40s} {'官方$':>8s} {'表价$':>8s} ratio   in/out/cr/cw (k)")
for m,(c,i,o,cr,cw) in sorted(agg.items(), key=lambda x:-x[1][0]):
    off=c/100; ours=face(m,i,o,cr,cw)
    flag='' if off==0 or 0.95<=ours/off<=1.05 else ' <<<'
    print(f"{m:40s} {off:8.2f} {ours:8.2f} {ours/off if off else 0:5.2f}   {i/1e3:.0f}/{o/1e3:.0f}/{cr/1e3:.0f}/{cw/1e3:.0f}{flag}")
