#!/usr/bin/env python3
"""
Kimi K3 蒸馏 + 真实代码编写场景测试
测试模型在知识蒸馏和实际编程任务中的表现
"""
import requests
import time
import json
import re

API = "http://38.92.24.153:8800/v1/chat/completions"
KEY = "sk-test-key"

def chat(prompt: str, max_tokens: int = 4096, model: str = "kimi-k3"):
    """发送聊天请求"""
    start = time.time()
    try:
        resp = requests.post(
            API,
            headers={"Authorization": f"Bearer {KEY}", "Content-Type": "application/json"},
            json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
                "stream": False,
            },
            timeout=300,
        )
        elapsed = time.time() - start
        data = resp.json()
        
        if resp.status_code != 200:
            return {"status": "FAIL", "error": f"HTTP {resp.status_code}: {data}"}
        
        content = data["choices"][0]["message"]["content"] or ""
        usage = data.get("usage", {})
        
        return {
            "status": "PASS",
            "content": content,
            "content_len": len(content),
            "prompt_tokens": usage.get("prompt_tokens", 0),
            "completion_tokens": usage.get("completion_tokens", 0),
            "elapsed": f"{elapsed:.2f}s",
            "elapsed_ms": int(elapsed * 1000),
        }
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}

# ============ 蒸馏测试场景 ============

def test_knowledge_distillation():
    """知识蒸馏：从大模型提取知识到小模型"""
    print("\n--- 知识蒸馏测试 ---")
    
    # 场景1: 代码解释蒸馏
    print("  场景1: 复杂代码解释...", end=" ", flush=True)
    code = '''
    #[derive(Debug, Clone)]
    pub struct AccountPool {
        slots: Arc<DashMap<String, Arc<Slot>>>,
        rr_index: Arc<AtomicUsize>,
        available_slots: Arc<arc_swap::ArcSwap<Vec<AvailableSlot>>>,
    }
    
    impl AccountPool {
        pub async fn acquire_by_session(&self, session_id: Option<&str>) 
            -> Result<(Account, OwnedSemaphorePermit), AcquireError> 
        {
            let deadline = Instant::now() + self.acquire_wait;
            let mut backoff = Duration::from_millis(10);
            loop {
                match self.try_acquire_by_session(session_id) {
                    AcquireTry::Got(pair) => return Ok(pair),
                    AcquireTry::Empty => return Err(AcquireError::Empty),
                    AcquireTry::Busy => {
                        let now = Instant::now();
                        if now >= deadline { return Err(AcquireError::Busy); }
                        let sleep = backoff.min(deadline - now);
                        tokio::time::sleep(sleep).await;
                        backoff = (backoff * 2).min(Duration::from_millis(100));
                    }
                }
            }
        }
    }
    '''
    r = chat(f"请用简单的语言解释这段 Rust 代码的核心逻辑，并指出其中使用的并发控制技术：\n\n{code}")
    print(r["status"])
    if r["status"] == "PASS":
        has_explanation = len(r["content"]) > 100
        has_keywords = any(k in r["content"].lower() for k in ["semaphore", "atomic", "lock", "concurrent", "dashmap"])
        print(f"    长度={r['content_len']}, 含关键词={has_keywords}, 耗时={r['elapsed']}")
        return {"scenario": "代码解释蒸馏", "status": "PASS" if has_explanation and has_keywords else "WEAK", "detail": r}
    return {"scenario": "代码解释蒸馏", "status": r["status"], "detail": r}

def test_document_generation():
    """文档生成蒸馏：从代码生成文档"""
    print("  场景2: API 文档生成...", end=" ", flush=True)
    code = '''
    pub async fn stream(&self, access_token: &str, machine_id: &str, body: &Value) 
        -> Result<Pin<Box<dyn Stream<Item = Result<Value, CursorError>> + Send>>, CursorError>
    '''
    r = chat(f"为以下 Rust 函数生成完整的 API 文档，包括参数说明、返回值、错误处理和示例：\n\n{code}")
    print(r["status"])
    if r["status"] == "PASS":
        has_params = "access_token" in r["content"] and "machine_id" in r["content"]
        has_return = "Stream" in r["content"] or "Result" in r["content"]
        print(f"    长度={r['content_len']}, 含参数说明={has_params}, 含返回值={has_return}, 耗时={r['elapsed']}")
        return {"scenario": "API文档生成", "status": "PASS" if has_params and has_return else "WEAK", "detail": r}
    return {"scenario": "API文档生成", "status": r["status"], "detail": r}

def test_code_translation():
    """代码翻译蒸馏：Rust → Python"""
    print("  场景3: Rust→Python 代码翻译...", end=" ", flush=True)
    rust_code = '''
    pub fn checksum(machine_id: &str) -> String {
        use base64::Engine;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let kilo = now_ms / 1_000_000;
        let mut raw = [
            ((kilo >> 40) & 255) as u8,
            ((kilo >> 32) & 255) as u8,
            ((kilo >> 24) & 255) as u8,
            ((kilo >> 16) & 255) as u8,
            ((kilo >> 8) & 255) as u8,
            (kilo & 255) as u8,
        ];
        let mut last = 165u8;
        for (i, cur) in raw.iter_mut().enumerate() {
            let val = ((*cur ^ last) as usize + (i % 256)) as u8;
            *cur = val;
            last = val;
        }
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw) + machine_id
    }
    '''
    r = chat(f"将以下 Rust 函数翻译成等效的 Python 代码，保持相同的逻辑和注释：\n\n{rust_code}")
    print(r["status"])
    if r["status"] == "PASS":
        has_python = "def " in r["content"] or "import " in r["content"]
        has_logic = "base64" in r["content"].lower() or "encode" in r["content"].lower()
        print(f"    长度={r['content_len']}, 含Python代码={has_python}, 含逻辑={has_logic}, 耗时={r['elapsed']}")
        return {"scenario": "代码翻译蒸馏", "status": "PASS" if has_python and has_logic else "WEAK", "detail": r}
    return {"scenario": "代码翻译蒸馏", "status": r["status"], "detail": r}

# ============ 真实代码编写场景 ============

def test_function_implementation():
    """函数实现：编写实用函数"""
    print("\n--- 真实代码编写测试 ---")
    
    print("  场景4: 实现 LRU 缓存...", end=" ", flush=True)
    r = chat("用 Rust 实现一个线程安全的 LRU 缓存，要求：\n1. 使用 DashMap 或类似并发 map\n2. 支持 TTL 过期\n3. 支持最大容量限制\n4. 提供 get/put/remove 方法\n5. 包含单元测试")
    print(r["status"])
    if r["status"] == "PASS":
        has_struct = "struct" in r["content"]
        has_impl = "impl" in r["content"]
        has_test = "#[test]" in r["content"] or "#[cfg(test)]" in r["content"]
        has_dashmap = "DashMap" in r["content"] or "dashmap" in r["content"]
        score = sum([has_struct, has_impl, has_test, has_dashmap])
        print(f"    长度={r['content_len']}, struct={has_struct}, impl={has_impl}, test={has_test}, dashmap={has_dashmap}, 耗时={r['elapsed']}")
        return {"scenario": "LRU缓存实现", "status": "PASS" if score >= 3 else "WEAK", "score": f"{score}/4", "detail": r}
    return {"scenario": "LRU缓存实现", "status": r["status"], "detail": r}

def test_bug_fixing():
    """Bug 修复：识别并修复代码问题"""
    print("  场景5: Bug 识别与修复...", end=" ", flush=True)
    buggy_code = '''
    fn process_items(items: Vec<String>) -> Vec<String> {
        let mut results = Vec::new();
        for item in items.iter() {
            let processed = expensive_operation(item);
            results.push(processed);
        }
        results
    }
    
    fn expensive_operation(s: &String) -> String {
        s.clone().to_uppercase()
    }
    '''
    r = chat(f"以下 Rust 代码有性能问题，请指出并修复：\n\n{buggy_code}")
    print(r["status"])
    if r["status"] == "PASS":
        has_identify = any(k in r["content"].lower() for k in ["clone", "reference", "borrow", "move", "performance"])
        has_fix = "fn " in r["content"] and ("&str" in r["content"] or "String" in r["content"])
        print(f"    长度={r['content_len']}, 识别问题={has_identify}, 提供修复={has_fix}, 耗时={r['elapsed']}")
        return {"scenario": "Bug修复", "status": "PASS" if has_identify and has_fix else "WEAK", "detail": r}
    return {"scenario": "Bug修复", "status": r["status"], "detail": r}

def test_code_review():
    """代码审查：审查代码质量"""
    print("  场景6: 代码审查...", end=" ", flush=True)
    code_to_review = '''
    pub async fn handle_request(req: Request) -> Response {
        let data = req.body().await.unwrap();
        let parsed: Value = serde_json::from_slice(&data).unwrap();
        let result = process(parsed).await;
        Response::new(result.to_string())
    }
    '''
    r = chat(f"请审查以下 Rust 代码，指出所有潜在问题（panic、错误处理、性能等），并提供改进版本：\n\n{code_to_review}")
    print(r["status"])
    if r["status"] == "PASS":
        has_issues = any(k in r["content"].lower() for k in ["unwrap", "panic", "error", "expect", "handle"])
        has_improvement = "fn " in r["content"] or "Result" in r["content"]
        print(f"    长度={r['content_len']}, 识别问题={has_issues}, 提供改进={has_improvement}, 耗时={r['elapsed']}")
        return {"scenario": "代码审查", "status": "PASS" if has_issues and has_improvement else "WEAK", "detail": r}
    return {"scenario": "代码审查", "status": r["status"], "detail": r}

def test_async_pattern():
    """异步模式：编写正确的异步代码"""
    print("  场景7: 异步并发模式...", end=" ", flush=True)
    r = chat("用 Rust 实现一个并发爬虫，要求：\n1. 使用 tokio 异步运行时\n2. 支持并发限制（最多10个并发请求）\n3. 支持超时控制\n4. 支持重试机制（最多3次）\n5. 结果收集到 Vec")
    print(r["status"])
    if r["status"] == "PASS":
        has_tokio = "tokio" in r["content"]
        has_semaphore = "Semaphore" in r["content"] or "semaphore" in r["content"]
        has_timeout = "timeout" in r["content"].lower()
        has_retry = "retry" in r["content"].lower() or "attempt" in r["content"].lower()
        score = sum([has_tokio, has_semaphore, has_timeout, has_retry])
        print(f"    长度={r['content_len']}, tokio={has_tokio}, semaphore={has_semaphore}, timeout={has_timeout}, retry={has_retry}, 耗时={r['elapsed']}")
        return {"scenario": "异步并发模式", "status": "PASS" if score >= 3 else "WEAK", "score": f"{score}/4", "detail": r}
    return {"scenario": "异步并发模式", "status": r["status"], "detail": r}

def test_sql_optimization():
    """SQL 优化：数据库查询优化"""
    print("  场景8: SQL 查询优化...", end=" ", flush=True)
    sql = '''
    SELECT * FROM orders 
    WHERE user_id IN (SELECT id FROM users WHERE created_at > '2024-01-01')
    AND status = 'completed'
    ORDER BY created_at DESC
    LIMIT 100;
    '''
    r = chat(f"优化以下 SQL 查询，假设表有百万级数据。提供优化后的 SQL 和索引建议：\n\n{sql}")
    print(r["status"])
    if r["status"] == "PASS":
        has_index = "INDEX" in r["content"].upper() or "index" in r["content"]
        has_optimization = any(k in r["content"].upper() for k in ["JOIN", "EXISTS", "BETWEEN", "COVERING"])
        print(f"    长度={r['content_len']}, 含索引建议={has_index}, 含优化={has_optimization}, 耗时={r['elapsed']}")
        return {"scenario": "SQL优化", "status": "PASS" if has_index and has_optimization else "WEAK", "detail": r}
    return {"scenario": "SQL优化", "status": r["status"], "detail": r}

def test_algorithm_design():
    """算法设计：设计高效算法"""
    print("  场景9: 算法设计...", end=" ", flush=True)
    r = chat("设计一个算法，在 Rust 中实现：\n1. 从 1000 万个整数中找出出现次数最多的前 100 个数\n2. 要求时间复杂度 O(n)\n3. 空间复杂度尽可能低\n4. 提供完整实现和复杂度分析")
    print(r["status"])
    if r["status"] == "PASS":
        has_hash = "HashMap" in r["content"] or "hash" in r["content"].lower()
        has_heap = "heap" in r["content"].lower() or "BinaryHeap" in r["content"]
        has_complexity = "O(" in r["content"] or "complexity" in r["content"].lower()
        score = sum([has_hash, has_heap, has_complexity])
        print(f"    长度={r['content_len']}, hash={has_hash}, heap={has_heap}, complexity={has_complexity}, 耗时={r['elapsed']}")
        return {"scenario": "算法设计", "status": "PASS" if score >= 2 else "WEAK", "score": f"{score}/3", "detail": r}
    return {"scenario": "算法设计", "status": r["status"], "detail": r}

def test_system_design():
    """系统设计：高并发系统设计"""
    print("  场景10: 高并发系统设计...", end=" ", flush=True)
    r = chat("设计一个高并发 API 网关，要求：\n1. 支持 10万 QPS\n2. 支持 rate limiting\n3. 支持 circuit breaker\n4. 支持请求/响应缓存\n5. 用 Rust 实现核心组件\n6. 画出架构图（用 ASCII）")
    print(r["status"])
    if r["status"] == "PASS":
        has_rate_limit = "rate" in r["content"].lower() and "limit" in r["content"].lower()
        has_circuit = "circuit" in r["content"].lower() or "breaker" in r["content"].lower()
        has_cache = "cache" in r["content"].lower()
        has_architecture = "→" in r["content"] or "->" in r["content"] or "│" in r["content"] or "├" in r["content"]
        score = sum([has_rate_limit, has_circuit, has_cache, has_architecture])
        print(f"    长度={r['content_len']}, rate_limit={has_rate_limit}, circuit={has_circuit}, cache={has_cache}, arch={has_architecture}, 耗时={r['elapsed']}")
        return {"scenario": "系统设计", "status": "PASS" if score >= 3 else "WEAK", "score": f"{score}/4", "detail": r}
    return {"scenario": "系统设计", "status": r["status"], "detail": r}

def main():
    print("=" * 60)
    print("Kimi K3 蒸馏 + 真实代码编写场景测试")
    print("=" * 60)
    
    results = []
    
    # 蒸馏测试
    results.append(test_knowledge_distillation())
    results.append(test_document_generation())
    results.append(test_code_translation())
    
    # 真实代码编写测试
    results.append(test_function_implementation())
    results.append(test_bug_fixing())
    results.append(test_code_review())
    results.append(test_async_pattern())
    results.append(test_sql_optimization())
    results.append(test_algorithm_design())
    results.append(test_system_design())
    
    # 汇总
    print("\n" + "=" * 60)
    print("测试结果汇总")
    print("=" * 60)
    
    passed = sum(1 for r in results if r["status"] == "PASS")
    weak = sum(1 for r in results if r["status"] == "WEAK")
    failed = sum(1 for r in results if r["status"] in ("FAIL", "ERROR"))
    
    print(f"通过: {passed} | 一般: {weak} | 失败: {failed}")
    
    for r in results:
        icon = {"PASS": "✅", "WEAK": "⚠️", "FAIL": "❌", "ERROR": "💥"}.get(r["status"], "?")
        score = f" ({r.get('score', '')})" if "score" in r else ""
        print(f"{icon} {r['scenario']}: {r['status']}{score}")
        if r["status"] == "PASS" and "detail" in r:
            d = r["detail"]
            print(f"    tokens: {d.get('prompt_tokens', 0)}→{d.get('completion_tokens', 0)}, 耗时: {d.get('elapsed', 'N/A')}")
    
    # 保存详细结果
    with open("/tmp/k3_realworld_test_results.json", "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n详细结果已保存到 /tmp/k3_realworld_test_results.json")

if __name__ == "__main__":
    main()
