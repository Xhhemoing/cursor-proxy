#!/usr/bin/env python3
"""
Kimi K3 长上下文测试脚本
测试目标: 验证 1M token 上下文窗口的实际表现
对标官方: Moonshot AI 声称 Kimi K3 支持 1M 上下文
"""
import requests
import time
import json

API = "http://38.92.24.153:8800/v1/chat/completions"
KEY = "sk-test-key"

def test_context(name: str, content_len: int, expected_tokens: int):
    """发送指定长度的上下文，测试模型是否能正确接收和处理"""
    content = "x" * content_len
    start = time.time()
    try:
        resp = requests.post(
            API,
            headers={"Authorization": f"Bearer {KEY}", "Content-Type": "application/json"},
            json={
                "model": "kimi-k3",
                "messages": [{"role": "user", "content": content}],
                "max_tokens": 32,
            },
            timeout=120,
        )
        elapsed = time.time() - start
        data = resp.json()
        
        if resp.status_code != 200:
            return {
                "name": name,
                "status": "FAIL",
                "error": f"HTTP {resp.status_code}: {data}",
                "elapsed": elapsed,
            }
        
        usage = data.get("usage", {})
        prompt_tokens = usage.get("prompt_tokens", 0)
        completion_tokens = usage.get("completion_tokens", 0)
        
        # 检查是否截断
        truncated = prompt_tokens < expected_tokens * 0.9
        
        return {
            "name": name,
            "status": "TRUNCATED" if truncated else "PASS",
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "expected_tokens": expected_tokens,
            "elapsed": f"{elapsed:.2f}s",
        }
    except Exception as e:
        return {"name": name, "status": "ERROR", "error": str(e)}

def test_needle_in_haystack(name: str, context_len: int, needle_pos: float):
    """
    NIAH (Needle In A Haystack) 测试
    在指定位置插入特定信息，测试模型是否能找到
    """
    # 生成填充内容
    filler = "The quick brown fox jumps over the lazy dog. " * (context_len // 45)
    
    # 在指定位置插入 needle
    needle = "The secret code is: K3-1M-CONTEXT-OK"
    pos = int(len(filler) * needle_pos)
    content = filler[:pos] + needle + filler[pos:]
    
    start = time.time()
    try:
        resp = requests.post(
            API,
            headers={"Authorization": f"Bearer {KEY}", "Content-Type": "application/json"},
            json={
                "model": "kimi-k3",
                "messages": [{"role": "user", "content": f"{content}\n\nWhat is the secret code?"}],
                "max_tokens": 64,
            },
            timeout=120,
        )
        elapsed = time.time() - start
        data = resp.json()
        
        if resp.status_code != 200:
            return {"name": name, "status": "FAIL", "error": f"HTTP {resp.status_code}"}
        
        answer = data["choices"][0]["message"]["content"]
        found = "K3-1M-CONTEXT-OK" in answer
        
        return {
            "name": name,
            "status": "PASS" if found else "MISS",
            "found": found,
            "answer": answer[:100],
            "elapsed": f"{elapsed:.2f}s",
        }
    except Exception as e:
        return {"name": name, "status": "ERROR", "error": str(e)}

def main():
    print("=" * 60)
    print("Kimi K3 长上下文测试")
    print("=" * 60)
    
    # 1. 基础上下文长度测试
    print("\n--- 基础上下文长度测试 ---")
    tests = [
        ("4K 上下文", 4_000, 1_000),
        ("32K 上下文", 32_000, 8_000),
        ("128K 上下文", 128_000, 32_000),
        ("256K 上下文", 256_000, 64_000),
        ("512K 上下文", 512_000, 128_000),
        ("1M 上下文", 1_000_000, 250_000),
    ]
    
    results = []
    for name, content_len, expected in tests:
        print(f"  测试 {name}...", end=" ", flush=True)
        r = test_context(name, content_len, expected)
        results.append(r)
        print(r["status"])
        if r["status"] == "ERROR":
            print(f"    错误: {r.get('error', 'unknown')}")
    
    # 2. NIAH 测试
    print("\n--- NIAH (Needle In A Haystack) 测试 ---")
    niah_tests = [
        ("NIAH-32K-10%", 32_000, 0.1),
        ("NIAH-32K-50%", 32_000, 0.5),
        ("NIAH-32K-90%", 32_000, 0.9),
        ("NIAH-128K-50%", 128_000, 0.5),
        ("NIAH-512K-50%", 512_000, 0.5),
    ]
    
    for name, ctx_len, pos in niah_tests:
        print(f"  测试 {name}...", end=" ", flush=True)
        r = test_needle_in_haystack(name, ctx_len, pos)
        results.append(r)
        print(r["status"])
        if r.get("answer"):
            print(f"    回答: {r['answer'][:80]}")
    
    # 3. 汇总
    print("\n" + "=" * 60)
    print("测试结果汇总")
    print("=" * 60)
    passed = sum(1 for r in results if r["status"] == "PASS")
    truncated = sum(1 for r in results if r["status"] == "TRUNCATED")
    failed = sum(1 for r in results if r["status"] in ("FAIL", "ERROR", "MISS"))
    print(f"通过: {passed} | 截断: {truncated} | 失败: {failed}")
    
    for r in results:
        status_icon = {"PASS": "✅", "TRUNCATED": "⚠️", "FAIL": "❌", "ERROR": "💥", "MISS": "❓"}.get(r["status"], "?")
        detail = ""
        if "prompt_tokens" in r:
            detail = f"prompt={r['prompt_tokens']}, completion={r['completion_tokens']}"
        if "found" in r:
            detail = f"found={r['found']}"
        print(f"{status_icon} {r['name']}: {r['status']} {detail}")
    
    # 保存详细结果
    with open("/tmp/k3_context_test_results.json", "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n详细结果已保存到 /tmp/k3_context_test_results.json")

if __name__ == "__main__":
    main()
