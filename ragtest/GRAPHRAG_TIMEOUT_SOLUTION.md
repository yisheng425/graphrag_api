# GraphRAG 超时问题解决方案

## 问题分析

### 根本原因

您的 GraphRAG API 使用了 **异步任务架构** (Celery):

1. `/query` 端点提交查询后立即返回 `task_id` (快速响应)
2. 查询任务在后台异步执行 (需要 **60-120 秒**)
3. 需要通过 `/tasks/{task_id}` 端点轮询结果

### 原问题

原代码可能使用了同步方式调用 `/query` 端点并等待响应，导致:
- 连接超时（80秒）
- API 超时（30秒）
- 实际任务需要 108+ 秒才能完成

## 解决方案

### 方案 1: 使用提供的 GraphRAG API 客户端（推荐）

使用 [`graphrag_api_client.py`](graphrag_api_client.py) 正确处理异步任务：

```python
from graphrag_api_client import GraphRAGClient, GraphRAGConfig

# 1. 创建客户端（使用默认配置或自定义配置）
client = GraphRAGClient()

# 或自定义配置
config = GraphRAGConfig(
    api_base_url="http://10.176.25.114:8001",
    timeout=180,        # 总超时时间
    poll_interval=2,    # 每2秒轮询一次
    max_poll_attempts=60  # 最多轮询60次 (120秒)
)
client = GraphRAGClient(config)

# 2. 执行查询（自动处理异步任务）
result = client.query(
    query="投诉问题",
    root="/home/xhm/graphrag/ragtest",
    method="local"  # local, global, drift, basic
)

# 3. 处理结果
if result:
    response_text = result.get('response', '')
    context = result.get('context', {})
    
    # 获取实体和关系
    entities = context.get('entities', [])
    relationships = context.get('relationships', [])
    
    print(f"找到 {len(entities)} 个实体")
    print(f"找到 {len(relationships)} 个关系")
else:
    print("查询失败，使用后备方案")
```

### 方案 2: 在 Kafka 消费者中集成

如果您有 Kafka 消费者代码（从日志来看您应该有），需要替换原来的同步调用：

```python
# ❌ 旧代码 (错误的同步调用)
import requests

response = requests.post(
    "http://10.176.25.114:8001/query",
    json={"query": text, "root": "/home/xhm/graphrag/ragtest", "method": "local"},
    timeout=30  # 这会超时！
)
result = response.json()  # 这里拿不到真实结果

# ✅ 新代码 (正确的异步处理)
from graphrag_api_client import GraphRAGClient

client = GraphRAGClient()

try:
    result = client.query(
        query=text,
        root="/home/xhm/graphrag/ragtest",
        method="local"
    )
    
    if result:
        # 成功获取结果
        entities = result.get('context', {}).get('entities', [])
        relationships = result.get('context', {}).get('relationships', [])
        
        # 处理实体和关系...
        logger.info(f"提取了 {len(entities)} 个实体, {len(relationships)} 条关系")
    else:
        # 失败，使用后备方案
        logger.warning("GraphRAG 抽取失败，使用后备方案")
        fallback_result = extract_with_fallback(text)
        
except Exception as e:
    logger.error(f"GraphRAG 异常: {e}")
    # 使用后备方案
```

### 方案 3: 手动实现异步调用逻辑

如果不想使用客户端类，可以手动实现：

```python
import requests
import time

def query_graphrag_async(query_text, max_wait_seconds=120):
    """异步查询 GraphRAG"""
    
    # 1. 提交任务
    response = requests.post(
        "http://10.176.25.114:8001/query",
        json={
            "query": query_text,
            "root": "/home/xhm/graphrag/ragtest",
            "method": "local"
        },
        timeout=30
    )
    
    if response.status_code != 200:
        return None
    
    task_id = response.json()['task_id']
    print(f"任务已提交: {task_id}")
    
    # 2. 轮询任务状态
    start_time = time.time()
    while time.time() - start_time < max_wait_seconds:
        status_response = requests.get(
            f"http://10.176.25.114:8001/tasks/{task_id}",
            timeout=10
        )
        
        if status_response.status_code != 200:
            time.sleep(2)
            continue
        
        status_data = status_response.json()
        state = status_data.get('state')
        
        if state == 'SUCCESS':
            print(f"任务完成，耗时 {time.time() - start_time:.1f}秒")
            return status_data.get('result')
        elif state == 'FAILURE':
            print(f"任务失败: {status_data.get('error')}")
            return None
        
        # 继续等待
        time.sleep(2)
    
    print(f"任务超时")
    return None

# 使用
result = query_graphrag_async("投诉问题")
```

## API 文档

### 端点列表

GraphRAG API 提供以下端点：

| 端点 | 方法 | 说明 |
|------|------|------|
| `/index` | POST | 提交索引构建任务 |
| `/update` | POST | 提交索引更新任务 |
| `/query` | POST | 提交查询任务 |
| `/tasks/{task_id}` | GET | 获取任务状态和结果 |
| `/docs` | GET | Swagger UI 文档 |
| `/openapi.json` | GET | OpenAPI 规范 |

### 查询请求格式

```json
{
  "query": "投诉问题",                           // 必填: 查询文本
  "root": "/home/xhm/graphrag/ragtest",        // 必填: 项目根目录
  "method": "local",                            // 可选: local/global/drift/basic
  "community_level": 2,                         // 可选: 社区层级
  "dynamic_community_selection": false,         // 可选: 动态社区选择
  "response_type": "Multiple Paragraphs",       // 可选: 响应格式
  "streaming": false,                           // 可选: 是否流式返回
  "verbose": false                              // 可选: 详细日志
}
```

### 查询响应格式

**1. 提交响应** (立即返回):
```json
{
  "task_id": "b43536f7-0b48-4155-aa1a-c5c012f3c6c3"
}
```

**2. 任务状态** (轮询 `/tasks/{task_id}`):
```json
{
  "task_id": "b43536f7-0b48-4155-aa1a-c5c012f3c6c3",
  "status": "SUCCESS",
  "state": "SUCCESS",
  "result": {
    "type": "query",
    "method": "local",
    "query": "投诉问题",
    "response": "### 投诉问题概述\n\n在当前的数据中...",
    "context": {
      "reports": [...],
      "entities": [...],
      "relationships": [...],
      "claims": [...],
      "sources": [...]
    },
    "response_type": "Multiple Paragraphs",
    "community_level": 2
  },
  "error": null
}
```

**任务状态值**:
- `PENDING`: 等待执行
- `STARTED`: 正在执行
- `SUCCESS`: 执行成功
- `FAILURE`: 执行失败
- `RETRY`: 正在重试

## 性能优化建议

### 1. 调整超时配置

根据测试结果，查询一般需要 **60-120 秒**：

```python
config = GraphRAGConfig(
    timeout=180,         # 总超时时间 180 秒
    poll_interval=3,     # 降低轮询频率到 3 秒
    max_poll_attempts=60 # 60 * 3 = 180 秒
)
```

### 2. 优化 GraphRAG 配置

修改 [`settings.yaml`](settings.yaml) 以提升速度：

```yaml
# 减少并发请求，避免 API 限流
models:
  default_chat_model:
    concurrent_requests: 3  # 从 10 降到 3
    
  default_embedding_model:
    concurrent_requests: 10  # 从 25 降到 10

# 减小文本块大小
chunks:
  size: 800    # 从 1200 降到 800
  overlap: 50  # 从 100 降到 50

# 减少额外的提取轮次
extract_graph:
  max_gleanings: 0  # 从 1 降到 0
```

### 3. 批量处理

如果有多个查询，考虑批量提交：

```python
client = GraphRAGClient()

# 批量提交
task_ids = []
for query_text in queries:
    task_id = client.submit_query(query_text)
    if task_id:
        task_ids.append((query_text, task_id))
    time.sleep(0.5)  # 避免过快提交

# 批量获取结果
results = []
for query_text, task_id in task_ids:
    result = client.wait_for_result(task_id)
    results.append((query_text, result))
```

### 4. 添加缓存

对于相同或相似的查询，可以缓存结果：

```python
import hashlib
import json

cache = {}

def query_with_cache(client, query_text):
    # 生成缓存键
    cache_key = hashlib.md5(query_text.encode()).hexdigest()
    
    # 检查缓存
    if cache_key in cache:
        print("使用缓存结果")
        return cache[cache_key]
    
    # 执行查询
    result = client.query(query_text)
    
    # 保存缓存
    if result:
        cache[cache_key] = result
    
    return result
```

## 监控和日志

### 添加详细日志

```python
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('graphrag_queries.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('graphrag')

# 使用日志
client = GraphRAGClient()
client.logger = logger

result = client.query("投诉问题")
```

### 收集统计信息

```python
import time
from collections import defaultdict

stats = {
    'total_queries': 0,
    'successful_queries': 0,
    'failed_queries': 0,
    'total_time': 0,
    'avg_time': 0
}

def query_with_stats(client, query_text):
    stats['total_queries'] += 1
    
    start_time = time.time()
    result = client.query(query_text)
    elapsed = time.time() - start_time
    
    stats['total_time'] += elapsed
    
    if result:
        stats['successful_queries'] += 1
    else:
        stats['failed_queries'] += 1
    
    stats['avg_time'] = stats['total_time'] / stats['total_queries']
    
    logger.info(f"统计: 成功={stats['successful_queries']}, "
                f"失败={stats['failed_queries']}, "
                f"平均耗时={stats['avg_time']:.2f}秒")
    
    return result
```

## 测试和验证

### 快速测试

```bash
# 1. 测试 API 端点
python test_graphrag_endpoint.py

# 2. 测试客户端
python graphrag_api_client.py

# 3. 查看 API 文档
curl http://10.176.25.114:8001/docs
# 或在浏览器中打开: http://10.176.25.114:8001/docs
```

### 压力测试

```python
# test_graphrag_load.py
from graphrag_api_client import GraphRAGClient
import time
import concurrent.futures

def test_concurrent_queries(num_queries=5):
    """测试并发查询"""
    client = GraphRAGClient()
    
    queries = [f"测试查询 {i}" for i in range(num_queries)]
    
    start_time = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(client.query, q) for q in queries]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]
    
    elapsed = time.time() - start_time
    
    successful = sum(1 for r in results if r is not None)
    print(f"完成 {num_queries} 个查询，耗时 {elapsed:.2f}秒")
    print(f"成功: {successful}/{num_queries}")
    print(f"平均: {elapsed/num_queries:.2f}秒/查询")

if __name__ == "__main__":
    test_concurrent_queries()
```

## 故障排除

### 常见问题

**1. 任务一直返回 404**
- 检查端点: 应该是 `/tasks/{task_id}` 不是 `/status/{task_id}`
- 确认 task_id 格式正确

**2. 任务一直处于 PENDING 状态**
- 检查 Celery worker 是否运行
- 查看服务器日志
- 增加轮询时间

**3. 任务状态为 FAILURE**
- 查看 `error` 字段获取详细错误信息
- 检查 GraphRAG 配置和索引是否正确
- 验证 API 密钥是否有效

**4. 查询速度太慢**
- 参考"性能优化建议"部分
- 考虑使用更快的 LLM 模型
- 减小文本块大小和实体类型数量

### 调试命令

```bash
# 检查 API 服务状态
curl -v http://10.176.25.114:8001/

# 查看 API 文档
curl -s http://10.176.25.114:8001/openapi.json | python -m json.tool

# 手动提交查询
curl -X POST http://10.176.25.114:8001/query \
  -H "Content-Type: application/json" \
  -d '{"query": "测试", "root": "/home/xhm/graphrag/ragtest", "method": "local"}'

# 检查任务状态
curl http://10.176.25.114:8001/tasks/{task_id}
```

## 总结

### 关键要点

1. ✅ GraphRAG API 使用 **异步任务模式**，需要轮询 `/tasks/{task_id}` 获取结果
2. ✅ 查询通常需要 **60-120 秒**，不能使用短超时
3. ✅ 使用提供的 [`graphrag_api_client.py`](graphrag_api_client.py) 客户端可以自动处理异步任务
4. ✅ 建议总超时时间设置为 **180 秒**，轮询间隔 **2-3 秒**
5. ✅ 添加错误处理和后备方案，避免单点故障

### 下一步行动

1. **立即执行**:
   - 在您的 Kafka 消费者代码中集成 `GraphRAGClient`
   - 调整超时配置到 180 秒
   - 添加错误处理和后备方案

2. **短期优化**:
   - 优化 [`settings.yaml`](settings.yaml) 配置
   - 添加查询缓存
   - 实现结果监控和统计

3. **长期改进**:
   - 考虑使用消息队列解耦查询和处理
   - 实现查询结果持久化
   - 添加告警和自动恢复机制

## 相关文件

- [`graphrag_api_client.py`](graphrag_api_client.py) - GraphRAG API 客户端（推荐使用）
- [`test_graphrag_endpoint.py`](test_graphrag_endpoint.py) - API 端点测试工具
- [`settings.yaml`](settings.yaml) - GraphRAG 配置文件
- [`GRAPHRAG_TIMEOUT_TROUBLESHOOTING.md`](GRAPHRAG_TIMEOUT_TROUBLESHOOTING.md) - 详细故障排查指南

---

**最后更新**: 2025-12-24  
**测试状态**: ✅ 已验证工作正常  
**平均查询时间**: 108 秒
