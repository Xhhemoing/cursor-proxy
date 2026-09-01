#!/usr/bin/env python3
"""
Kimi K3 流式输出性能测试
测试 SSE 流式传输的帧率和延迟
"""
import requests
import time
import json

API = "http://38.92.24.153:8800/v1/chat/completions"
KEY = "sk-test-key"

def test_stream_performance(name: str, prompt: str, max_tokens: int = 1024):
    """测试流式输出性能"""
    start = time.time()
    first_byte_time = None
    chunk_count = 0
    total_content_len = 0
    
    try:
        resp = requests.post(
            API,
            headers={"Authorization": f"Bearer {KEY}", "Content-Type": "application/json"},
            json={
                "model": "kimi-k3",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
                "stream": True,
            },
            stream=True,
            timeout=120,
        )
        
        for line in resp.iter_lines():
            if first_byte_time is None:
                first_byte_time = time.time() - start
            if line:
                chunk_count += 1
                # 解析 SSE data 行
                if line.startswith(b"data: "):
                    try:
                        data = json.loads(line[6:])
                        if data.get("choices") and data["choices"][0].get("delta", {}).get("content"):
                            total_content_len += len(data["choices"][0]["delta"]["content"])
                    except:
                        pass
        
        elapsed = time.time() - start
        ttft = first_byte_time or elapsed
        
        return {
            "name": name,
            "status": "PASS",
            "chunks": chunk_count,
            "content_len": total_content_len,
            "ttft": f"{ttft:.3f}s",
            "total_time": f"{elapsed:.2f}s",
            "chunks_per_sec": f"{chunk_count/elapsed:.1f}" if elapsed > 0 else "N/A",
        }
    except Exception as e:
        return {"name": name, "status": "ERROR", "error": str(e)[:100]}

def main():
    print("=" * 60)
    print("Kimi K3 流式输出性能测试")
    print("=" * 60)
    
    results = []
    
    # 1. 基础流式测试
    print("\n--- 基础流式测试 ---")
    tests = [
        ("短回答 (100 tokens)", "Say hello", 100),
        ("中回答 (1K tokens)", "Write a short story about a robot learning to paint", 1024),
        ("长回答 (4K tokens)", "Write a detailed essay about the history of artificial intelligence", 4096),
    ]
    
    for name, prompt, mt in tests:
        print(f"  测试 {name}...", end=" ", flush=True)
        r = test_stream_performance(name, prompt, mt)
        results.append(r)
        print(r["status"])
        if r["status"] == "PASS":
            print(f"    chunks={r['chunks']}, content_len={r['content_len']}, TTFT={r['ttft']}, total={r['total_time']}, rate={r['chunks_per_sec']}/s")
    
    # 2. 并发流式测试
    print("\n--- 并发流式测试 (3并发) ---")
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(test_stream_performance, f"并发-{i}", f"Count from 1 to {50 + i*10}", 256)
            for i in range(3)
        ]
        for f in concurrent.futures.as_completed(futures):
            r = f.result()
            results.append(r)
            print(f"  {r['name']}: {r['status']} TTFT={r.get('ttft', 'N/A')}")
    
    # 3. 汇总
    print("\n" + "=" * 60)
    print("测试结果汇总")
    print("=" * 60)
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] in ("FAIL", "ERROR"))
    print(f"通过: {passed} | 失败: {failed}")
    
    for r in results:
        if r["status"] == "PASS":
            print(f"✅ {r['name']}: TTFT={r['ttft']}, chunks={r['chunks']}, rate={r['chunks_per_sec']}/s")
        else:
            print(f"❌ {r['name']}: {r.get('error', 'unknown')}")
    
    with open("/tmp/k3_stream_perf_results.json", "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n详细结果已保存到 /tmp/k3_stream_perf_results.json")

if __name__ == "__main__":
    main()
