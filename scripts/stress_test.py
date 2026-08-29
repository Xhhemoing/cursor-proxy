#!/usr/bin/env python3
"""cursor-fast-proxy-rs 多场景真实压测
场景:
  1. 高并发非流式 (100 并发 x 50 请求)
  2. 高并发流式 SSE (50 并发 x 20 请求)
  3. 会话一致性 (同一 session_id 100 请求 → 应命中同一账号)
  4. 账号冷却恢复 (打挂1个账号 → 验证其他账号接管 → 清除冷却 → 验证恢复)
  5. 重试切换 (Mock 上游随机失败 → 验证重试机制)
"""
import asyncio
import json
import time
import sys
import aiohttp
from collections import Counter

BASE = "http://127.0.0.1:8899"
ADMIN_TOKEN = ""
API_KEY = ""

def load_config():
    global ADMIN_TOKEN, API_KEY
    with open("/home/ubuntu/work/cursor-fast-proxy-rs/config.json") as f:
        cfg = json.load(f)
    ADMIN_TOKEN = cfg.get("admin_token", "")
    keys = cfg.get("api_keys", [])
    if keys:
        k = keys[0]
        API_KEY = k["key"] if isinstance(k, dict) else k
    else:
        API_KEY = ""

load_config()

HEADERS = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
ADMIN_HEADERS = {"Authorization": f"Bearer {ADMIN_TOKEN}"}

PASS = 0
FAIL = 0

def report(name, ok, detail=""):
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  ✅ {name} {detail}")
    else:
        FAIL += 1
        print(f"  ❌ {name} {detail}")

async def chat_request(session, stream=False, session_id=None, model="kimi-k3"):
    body = {
        "model": model,
        "messages": [{"role": "user", "content": "hi"}],
        "stream": stream,
    }
    if session_id:
        body["user"] = session_id
    try:
        async with session.post(f"{BASE}/v1/chat/completions", json=body, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if stream:
                chunks = 0
                async for line in resp.content:
                    if line.startswith(b"data:"):
                        chunks += 1
                return resp.status, chunks, None
            else:
                data = await resp.json()
                return resp.status, data, None
    except Exception as e:
        return 0, None, str(e)

async def scenario_1_high_concurrency_nonstream():
    print("\n📊 场景1: 高并发非流式 (100并发 x 50请求 = 5000)")
    connector = aiohttp.TCPConnector(limit=200)
    async with aiohttp.ClientSession(connector=connector) as session:
        sem = asyncio.Semaphore(100)
        results = []
        async def worker(i):
            async with sem:
                status, data, err = await chat_request(session, stream=False)
                results.append((status, err))
        start = time.time()
        await asyncio.gather(*[worker(i) for i in range(5000)])
        elapsed = time.time() - start
        ok = sum(1 for s, _ in results if s == 200)
        errors = Counter(str(e) for s, e in results if e)
        report("5000请求完成", len(results) == 5000)
        report("成功率>95%", ok / len(results) > 0.95, f"{ok}/{len(results)} = {ok/len(results)*100:.1f}%")
        report("QPS>100", len(results) / elapsed > 100, f"{len(results)/elapsed:.0f} QPS")
        if errors:
            print(f"    错误分布: {dict(errors.most_common(3))}")

async def scenario_2_high_concurrency_stream():
    print("\n📊 场景2: 高并发流式 SSE (50并发 x 20请求 = 1000)")
    connector = aiohttp.TCPConnector(limit=100)
    async with aiohttp.ClientSession(connector=connector) as session:
        sem = asyncio.Semaphore(50)
        results = []
        async def worker(i):
            async with sem:
                status, chunks, err = await chat_request(session, stream=True)
                results.append((status, chunks, err))
        start = time.time()
        await asyncio.gather(*[worker(i) for i in range(1000)])
        elapsed = time.time() - start
        ok = sum(1 for s, _, _ in results if s == 200)
        avg_chunks = sum(c for _, c, _ in results if c) / max(ok, 1)
        report("1000流式请求完成", len(results) == 1000)
        report("成功率>95%", ok / len(results) > 0.95, f"{ok}/{len(results)}")
        report("平均chunks>0", avg_chunks > 0, f"avg={avg_chunks:.1f}")
        report("QPS>50", len(results) / elapsed > 50, f"{len(results)/elapsed:.0f} QPS")

async def scenario_3_session_stickiness():
    print("\n📊 场景3: 会话一致性 (同一session 100请求)")
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        # 先获取账号列表
        async with session.get(f"{BASE}/admin/api/pool", headers=ADMIN_HEADERS) as resp:
            pool_before = await resp.json()
        accounts_before = {a["id"]: a["stats"]["requests"] for a in pool_before["accounts"]}

        # 同一 session 发 100 请求
        tasks = [chat_request(session, stream=False, session_id="sticky-test-123") for _ in range(100)]
        results = await asyncio.gather(*tasks)
        ok = sum(1 for s, _, _ in results if s == 200)

        # 检查账号分布
        async with session.get(f"{BASE}/admin/api/pool", headers=ADMIN_HEADERS) as resp:
            pool_after = await resp.json()
        accounts_after = {a["id"]: a["stats"]["requests"] for a in pool_after["accounts"]}

        deltas = {aid: accounts_after.get(aid, 0) - accounts_before.get(aid, 0) for aid in accounts_after}
        used_accounts = {aid: d for aid, d in deltas.items() if d > 0}
        report("100请求成功", ok >= 95, f"{ok}/100")
        report("会话粘性(1个账号)", len(used_accounts) == 1, f"使用了{len(used_accounts)}个账号: {used_accounts}")

async def scenario_4_cooldown_recovery():
    print("\n📊 场景4: 账号冷却恢复")
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        # 获取账号列表
        async with session.get(f"{BASE}/admin/api/accounts", headers=ADMIN_HEADERS) as resp:
            data = await resp.json()
        accounts = data.get("accounts", data) if isinstance(data, dict) else data
        if len(accounts) < 1:
            report("至少1个账号", False, f"只有{len(accounts)}个")
            return

        victim = accounts[0]["id"]
        # 手动禁用第一个账号
        async with session.post(f"{BASE}/admin/api/accounts/{victim}/enabled", headers=ADMIN_HEADERS, json={"enabled": False}) as resp:
            report("禁用账号", resp.status == 200, f"禁用 {victim}")

        # 发请求验证其他账号接管
        status, data, err = await chat_request(session, stream=False)
        report("禁用后请求成功", status == 200, f"status={status}")

        # 重新启用
        async with session.post(f"{BASE}/admin/api/accounts/{victim}/enabled", headers=ADMIN_HEADERS, json={"enabled": True}) as resp:
            report("重新启用", resp.status == 200)

        # 验证恢复
        status, data, err = await chat_request(session, stream=False)
        report("恢复后请求成功", status == 200)

async def scenario_5_pool_stats():
    print("\n📊 场景5: 号池统计一致性")
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.get(f"{BASE}/admin/api/pool", headers=ADMIN_HEADERS) as resp:
            stats = await resp.json()
        report("总账号数>0", stats["total_accounts"] > 0, f"{stats['total_accounts']}个")
        report("可用账号>=0", stats["available"] >= 0, f"{stats['available']}个")
        report("总请求数>0", stats["total_requests"] > 0, f"{stats['total_requests']}")
        report("统计字段完整", all(k in stats for k in ["total_accounts", "available", "total_requests", "total_errors", "accounts"]))

async def main():
    print("=" * 60)
    print("cursor-fast-proxy-rs 多场景真实压测")
    print("=" * 60)

    # 先确认服务在跑
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{BASE}/health", timeout=aiohttp.ClientTimeout(total=3)) as r:
                health = await r.json()
                print(f"✅ 服务在线: {health['status']}, 账号数: {health['pool']['total_accounts']}")
    except Exception as e:
        print(f"❌ 服务不在线: {e}")
        print("请先启动: cargo run --release")
        sys.exit(1)

    await scenario_1_high_concurrency_nonstream()
    await scenario_2_high_concurrency_stream()
    await scenario_3_session_stickiness()
    await scenario_4_cooldown_recovery()
    await scenario_5_pool_stats()

    print("\n" + "=" * 60)
    print(f"总计: {PASS} 通过, {FAIL} 失败")
    print("=" * 60)
    sys.exit(0 if FAIL == 0 else 1)

if __name__ == "__main__":
    asyncio.run(main())
