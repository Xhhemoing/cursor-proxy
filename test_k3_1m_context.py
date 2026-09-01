#!/usr/bin/env python3
"""
Kimi K3 1M 上下文 + 大输出测试
验证 maxMode 默认开启后的实际表现
"""
import requests
import time
import json

API = "http://38.92.24.153:8800/v1/chat/completions"
KEY = "sk-test-key"

def test_large_context(name: str, content_len: int, max_tokens: int = 512):
    """测试大上下文输入"""
    content = "The quick brown fox jumps over the lazy dog. " * (content_len // 45)
    start = time.time()
    try:
        resp = requests.post(
            API,
            headers={"Authorization": f"Bearer {KEY}", "Content-Type": "application/json"},
            json={
                "model": "kimi-k3",
                "messages": [{"role": "user", "content": content + "\n\nSummarize the above in one sentence."}],
                "max_tokens": max_tokens,
                "stream": False,
            },
            timeout=300,
        )
        elapsed = time.time() - start
        data = resp.json()
        
        if resp.status_code != 200:
            return {"name": name, "status": "FAIL", "error": f"HTTP {resp.status_code}: {data}"}
        
        usage = data.get("usage", {})
        answer = data["choices"][0]["message"]["content"] or ""
        
        return {
            "name": name,
            "status": "PASS",
            "prompt_tokens": usage.get("prompt_tokens", 0),
            "completion_tokens": usage.get("completion_tokens", 0),
            "answer_len": len(answer),
            "elapsed": f"{elapsed:.2f}s",
        }
    except Exception as e:
        return {"name": name, "status": "ERROR", "error": str(e)}

def test_large_output(name: str, max_tokens: int):
    """测试大输出"""
    start = time.time()
    try:
        resp = requests.post(
            API,
            headers={"Authorization": f"Bearer {KEY}", "Content-Type": "application/json"},
            json={
                "model": "kimi-k3",
                "messages": [{"role": "user", "content": "Write a detailed 2000-word essay about the history of computing."}],
                "max_tokens": max_tokens,
                "stream": False,
            },
            timeout=300,
        )
        elapsed = time.time() - start
        data = resp.json()
        
        if resp.status_code != 200:
            return {"name": name, "status": "FAIL", "error": f"HTTP {resp.status_code}: {data}"}
        
        usage = data.get("usage", {})
        answer = data["choices"][0]["message"]["content"] or ""
        
        return {
            "name": name,
            "status": "PASS",
            "prompt_tokens": usage.get("prompt_tokens", 0),
            "completion_tokens": usage.get("completion_tokens", 0),
            "answer_len": len(answer),
            "elapsed": f"{elapsed:.2f}s",
        }
    except Exception as e:
        return {"name": name, "status": "ERROR", "error": str(e)}

def main():
    print("=" * 60)
    print("Kimi K3 1M 上下文 + 大输出测试 (maxMode 默认开启)")
    print("=" * 60)
    
    results = []
    
    # 1. 大上下文测试
    print("\n--- 大上下文输入测试 ---")
    for name, size in [("32K", 32_000), ("128K", 128_000), ("256K", 256_000), ("512K", 512_000), ("1M", 1_000_000)]:
        print(f"  测试 {name} 上下文...", end=" ", flush=True)
        r = test_large_context(f"{name} 上下文", size)
        results.append(r)
        print(r["status"])
        if r["status"] == "PASS":
            print(f"    prompt={r.get('prompt_tokens')}, completion={r.get('completion_tokens')}, answer_len={r.get('answer_len')}, time={r.get('elapsed')}")
        elif r["status"] == "ERROR":
            print(f"    错误: {r.get('error', 'unknown')[:100]}")
    
    # 2. 大输出测试
    print("\n--- 大输出测试 ---")
    for name, mt in [("4K 输出", 4096), ("8K 输出", 8192), ("16K 输出", 16384), ("32K 输出", 32768)]:
        print(f"  测试 {name}...", end=" ", flush=True)
        r = test_large_output(name, mt)
        results.append(r)
        print(r["status"])
        if r["status"] == "PASS":
            print(f"    completion={r.get('completion_tokens')}, answer_len={r.get('answer_len')}, time={r.get('elapsed')}")
    
    # 3. 汇总
    print("\n" + "=" * 60)
    print("测试结果汇总")
    print("=" * 60)
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] in ("FAIL", "ERROR"))
    print(f"通过: {passed} | 失败: {failed}")
    
    for r in results:
        icon = "✅" if r["status"] == "PASS" else "❌"
        print(f"{icon} {r['name']}: {r['status']}")
    
    with open("/tmp/k3_1m_test_results.json", "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n详细结果已保存到 /tmp/k3_1m_test_results.json")

if __name__ == "__main__":
    main()
