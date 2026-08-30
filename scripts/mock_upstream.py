#!/usr/bin/env python3
"""Mock OpenAI 上游 — 用于 cursor-fast-proxy-rs 全场景压测
特性:
  - 标准 OpenAI SSE / 非流式响应
  - 可控延迟 (MOCK_LATENCY_MS)
  - 故障注入 (MOCK_FAIL_RATE, 0.0-1.0 随机 500)
  - 按 authorization token 区分账号 → 支持定向故障 (MOCK_FAIL_TOKENS)
  - 记录每个 token 的请求数 (供 /_stats 查询负载均衡)
"""
import asyncio
import json
import os
import random
import time
from aiohttp import web

LATENCY_MS = int(os.environ.get("MOCK_LATENCY_MS", "20"))
FAIL_RATE = float(os.environ.get("MOCK_FAIL_RATE", "0.0"))
# 逗号分隔的 token 列表，这些 token 永远 500（用于故障注入）
FAIL_TOKENS = set(filter(None, os.environ.get("MOCK_FAIL_TOKENS", "").split(",")))

# token -> request_count 统计
STATS = {}
lock = asyncio.Lock()

def token_of(request):
    auth = request.headers.get("authorization", "")
    return auth.replace("Bearer ", "").strip() or "(none)"

async def chat(request):
    tok = token_of(request)
    async with lock:
        STATS[tok] = STATS.get(tok, 0) + 1

    # 定向故障
    if tok in FAIL_TOKENS:
        return web.Response(status=500, text=json.dumps({"error": {"message": "injected failure", "type": "server_error"}}), content_type="application/json")
    # 随机故障
    if FAIL_RATE > 0 and random.random() < FAIL_RATE:
        return web.Response(status=500, text=json.dumps({"error": {"message": "random failure", "type": "server_error"}}), content_type="application/json")

    try:
        body = await request.json()
    except Exception:
        body = {}
    model = body.get("model", "mock-model")
    stream = body.get("stream", False)

    # 模拟上游延迟
    if LATENCY_MS > 0:
        await asyncio.sleep(LATENCY_MS / 1000.0)

    text = f"Mock reply from {tok[:12]}"
    prompt_tokens = 10
    completion_tokens = 5

    if stream:
        resp = web.StreamResponse(status=200, headers={
            "content-type": "text/event-stream",
            "cache-control": "no-cache",
        })
        await resp.prepare(request)
        # 发 3 个 chunk
        for i, piece in enumerate(["Mock ", "reply ", "done"]):
            chunk = {
                "id": "chatcmpl-mock",
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": model,
                "choices": [{"index": 0, "delta": ({"role": "assistant", "content": piece} if i == 0 else {"content": piece}), "finish_reason": None}],
            }
            await resp.write(f"data: {json.dumps(chunk)}\n\n".encode())
        # 结束 chunk
        end = {
            "id": "chatcmpl-mock",
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": model,
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": prompt_tokens, "completion_tokens": completion_tokens, "total_tokens": prompt_tokens + completion_tokens},
        }
        await resp.write(f"data: {json.dumps(end)}\n\n".encode())
        await resp.write(b"data: [DONE]\n\n")
        await resp.write_eof()
        return resp
    else:
        return web.json_response({
            "id": "chatcmpl-mock",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [{"index": 0, "message": {"role": "assistant", "content": text}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": prompt_tokens, "completion_tokens": completion_tokens, "total_tokens": prompt_tokens + completion_tokens},
        })

async def stats(request):
    async with lock:
        return web.json_response({"counts": dict(STATS), "total": sum(STATS.values())})

async def reset(request):
    async with lock:
        STATS.clear()
    return web.json_response({"status": "reset"})

async def health(request):
    return web.json_response({"status": "ok"})

app = web.Application()
app.router.add_post("/v1/chat/completions", chat)
app.router.add_get("/_stats", stats)
app.router.add_post("/_reset", reset)
app.router.add_get("/health", health)

if __name__ == "__main__":
    port = int(os.environ.get("MOCK_PORT", "9900"))
    print(f"Mock OpenAI upstream on :{port} latency={LATENCY_MS}ms fail_rate={FAIL_RATE} fail_tokens={len(FAIL_TOKENS)}")
    web.run_app(app, host="127.0.0.1", port=port, print=None)
