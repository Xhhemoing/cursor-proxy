#!/usr/bin/env python3
"""套餐卡定价/盈亏分析工具 — Grok Heavy 版。

真实成本结构 (用户提供):
  ¥130/号/月, 含 $2000 Grok模型额度 + $1000 高级模型额度
  高级额度 8 并发/号, Grok(低级)额度并发未知
  你付固定月租, 额度是 Cursor 给的; 烧穿额度不额外花钱, 但该号本月对应能力报废。

正确安全线 (不是物理并发, 是额度速率):
  单卡每日烧的高级额度 ≤ 号日预算($1000/30=$33.3) × 让渡比例

用法:
  python3 scripts/card_pricing.py --price 50 --conc 2 --hours 24
  python3 scripts/card_pricing.py --model claude-opus-5 --reqs 150
  python3 scripts/card_pricing.py --simulate 10 --tier standard
"""
import argparse
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# ── Grok Heavy 真实参数 ──
ACCOUNT_RMB_MO = 130.0
GROK_QUOTA_USD = 2000.0       # Grok 模型额度/号/月
PREMIUM_QUOTA_USD = 1000.0    # 高级模型额度/号/月
PREMIUM_CONC_PER_ACCT = 8     # 高级额度并发/号
DAYS_MO = 30

# ── 高级模型真实价格 (Python 版 billing/pricing.json, $/1M tokens) ──
# (input, output, cache_read)
PREMIUM = {
    "gpt-5.4-pro":     (30.00, 180.00, 3.00),
    "gpt-5.6-cyber":   (12.50, 75.00, 1.25),
    "claude-fable-5":  (10.00, 50.00, 1.00),
    "fable-5":         (10.00, 50.00, 1.00),
    "claude-opus-5":   (5.00, 25.00, 0.50),
    "claude-opus-4-5": (5.00, 25.00, 0.50),
    "gpt-5.6":         (4.00, 20.00, 0.40),
    "gpt-5.6-sol":     (4.00, 20.00, 0.40),
    "claude-4.5-sonnet": (3.00, 15.00, 0.30),
    "kimi-k3":         (3.00, 15.00, 0.30),
    "gpt-5.4":         (2.50, 15.00, 0.25),
    "gpt-5":           (2.50, 15.00, 0.25),
    "claude-sonnet-5": (2.00, 10.00, 0.20),
    "sonnet-5":        (2.00, 10.00, 0.20),
    "gpt-5.6-terra":   (2.00, 12.00, 0.20),
    "claude-4.5-haiku": (1.00, 5.00, 0.10),
    "grok-4.5":        (2.00, 6.00, 0.30),   # Grok 额度, 不算高级
    "grok-4.6":        (2.00, 6.00, 0.50),
}
# 模型档位 (哪些算"高级", 吃 $1000 额度)
TIER_MODELS = {
    "economy":  ["claude-sonnet-5", "sonnet-5", "claude-4.5-haiku", "haiku-4-5",
                 "kimi-k3", "glm-5.2", "gemini-3.7", "gpt-5.4-mini", "gpt-5.4-nano"],
    "standard": ["claude-opus-5", "claude-opus-4-5", "opus-5", "gpt-5.6", "gpt-5.4", "gpt-5"],
    "flagship": ["claude-fable-5", "fable-5", "gpt-5.4-pro", "gpt-5.6-cyber"],
}


def pool_size():
    p = ROOT / "accounts.json"
    if not p.exists():
        return 9
    accs = json.loads(p.read_text())
    return len(accs) if isinstance(accs, list) else len(accs.keys())


def real_avg_latency():
    """k3-realworld 实测高级模型均延迟."""
    return 95.8


def req_cost(model, inp, out, cr):
    i, o, c = PREMIUM.get(model, (5.0, 25.0, 0.5))
    return (inp * i + out * o + cr * c) / 1e6


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--price", type=float, default=50.0, help="卡售价 (元)")
    ap.add_argument("--conc", type=int, default=2, help="卡并发上限")
    ap.add_argument("--hours", type=float, default=24.0, help="卡有效期 (小时)")
    ap.add_argument("--usd-cny", type=float, default=7.2)
    ap.add_argument("--model", default="claude-opus-5", choices=list(PREMIUM))
    ap.add_argument("--reqs", type=int, default=150, help="正常用户每日请求数")
    ap.add_argument("--tier", default="standard", choices=list(TIER_MODELS))
    ap.add_argument("--simulate", type=int, default=0, help="模拟 N 张满负荷卡")
    ap.add_argument("--in", dest="inp", type=int, default=20000, help="单请求输入 tokens")
    ap.add_argument("--out", dest="out", type=int, default=4000, help="单请求输出 tokens")
    ap.add_argument("--cr", dest="cr", type=int, default=30000, help="单请求缓存读 tokens")
    args = ap.parse_args()

    n_acc = pool_size()
    lat = real_avg_latency()
    acct_daily_premium = PREMIUM_QUOTA_USD / DAYS_MO   # $33.3/日

    print("=" * 70)
    print(f"Grok Heavy 套餐卡分析  |  {n_acc}号 × ¥{ACCOUNT_RMB_MO}/月  高级额度$1000/号")
    print("=" * 70)
    print(f"\n[成本结构]")
    print(f"  号池月成本        ¥{ACCOUNT_RMB_MO * n_acc:,.0f} ({n_acc}号)")
    print(f"  单号高级日预算    ${acct_daily_premium:.1f}/日 (30天均匀用完$1000)")
    print(f"  单号高级并发      {PREMIUM_CONC_PER_ACCT} 车道")
    print(f"  号池高级总并发    {n_acc * PREMIUM_CONC_PER_ACCT} 车道")

    cost_per_req = req_cost(args.model, args.inp, args.out, args.cr)
    print(f"\n[单请求成本 @ {args.model}]  (in={args.inp} out={args.out} cr={args.cr})")
    print(f"  ${cost_per_req:.4f}/请求")

    # 极端: 满负荷 24h
    max_reqs = args.conc * args.hours * 3600 / lat
    extreme_burn = max_reqs * cost_per_req
    days_kill = PREMIUM_QUOTA_USD / extreme_burn if extreme_burn else 9e9
    print(f"\n[极端: {args.conc}并发×{args.hours}h 满负荷]")
    print(f"  {max_reqs:,.0f} 请求 → 烧 ${extreme_burn:,.0f} 高级额度")
    print(f"  → {days_kill:.2f} 天烧穿一个号的 $1000 (号报废, 损失¥{ACCOUNT_RMB_MO:.0f})")

    # 正常用户
    normal_burn = args.reqs * cost_per_req
    print(f"\n[正常用户 {args.reqs}次/日 @ {args.model}]")
    print(f"  烧 ${normal_burn:.1f}/日 ≈ ¥{normal_burn * args.usd_cny:.1f}/日")

    # 盈亏判定: 卡费 vs 用户真实烧的额度(换算回号成本)
    # 号成本分摊: 单卡占 conc/8 车道, 占号日预算 conc/8 × $33.3
    lane_share = args.conc / PREMIUM_CONC_PER_ACCT
    card_quota_budget = acct_daily_premium * lane_share   # 这卡"该有"的日额度预算
    card_fee_usd = args.price / args.usd_cny
    print(f"\n[盈亏 @ ¥{args.price}/{args.hours}h {args.conc}并发]")
    print(f"  卡费              ${card_fee_usd:.2f} (¥{args.price})")
    print(f"  卡占号额度预算    ${card_quota_budget:.2f}/日 ({lane_share*100:.0f}% 车道)")
    print(f"  正常用户实烧      ${normal_burn:.2f}/日")
    if normal_burn <= card_quota_budget:
        print(f"  ✓ 正常用户烧不完该卡额度预算, 安全")
    else:
        print(f"  ✗ 正常用户会超预算 {normal_burn/card_quota_budget:.1f}x → 需日额度帽 ${card_quota_budget:.2f}")

    # 盈亏平衡卡费: 卡费 ≥ 用户实烧额度对应的号成本
    # 号成本 = ¥130/月. 用户日烧 $X 占月额度比例 X/1000, 对应号成本 ¥130×(X/1000)×30? 不对.
    # 正确: 用户日烧 $X, 30天烧 $30X. 若 30X > $1000 则号提前报废.
    # 号的"高级能力"价值 = ¥130 (你付的). 用户烧 $X/日, 号能撑 1000/X 天.
    # 只要 卡费×天数 ≥ ¥130 × (卡占用的号份额), 就不亏.
    print(f"\n[盈亏平衡]")
    # 简化: 单卡日烧 $X, 等价于每天消耗号价值的 X/1000 × ¥130 ... 这是"额度面值"口径
    daily_value_consumed = normal_burn / PREMIUM_QUOTA_USD * ACCOUNT_RMB_MO
    print(f"  正常用户日烧 ${normal_burn:.2f} = 号价值的 ¥{daily_value_consumed:.2f}/日 (面值口径)")
    print(f"  卡费 ¥{args.price}/日 vs 消耗 ¥{daily_value_consumed:.2f}/日", end="  ")
    if args.price >= daily_value_consumed:
        print(f"✓ 毛利 ¥{args.price - daily_value_consumed:.2f}/日 ({(args.price-daily_value_consumed)/args.price*100:.0f}%)")
    else:
        print(f"✗ 亏 ¥{daily_value_consumed - args.price:.2f}/日")
    be = daily_value_consumed
    print(f"  盈亏平衡卡费      ¥{be:.2f}/日 (按 {args.model} 正常用量)")

    if args.simulate > 0:
        print(f"\n[压力模拟]  {args.simulate} 张满负荷 {args.conc}并发卡")
        used_lanes = args.simulate * args.conc
        total_lanes = n_acc * PREMIUM_CONC_PER_ACCT
        total_burn = args.simulate * extreme_burn
        print(f"  占用高级车道 {used_lanes}/{total_lanes} ({used_lanes/total_lanes*100:.0f}%)")
        print(f"  {args.simulate}卡满负荷日烧 ${total_burn:,.0f} 高级额度")
        print(f"  全号池月高级额度 ${PREMIUM_QUOTA_USD*n_acc:,.0f} → {PREMIUM_QUOTA_USD*n_acc/total_burn:.2f} 天烧穿全部")
        daily_rev = args.simulate * args.price
        daily_cost = args.simulate * daily_value_consumed
        print(f"  日收入 ¥{daily_rev:.0f} vs 日消耗 ¥{daily_cost:.0f} → 净 ¥{daily_rev-daily_cost:.0f}/日")
    print()


if __name__ == "__main__":
    main()
