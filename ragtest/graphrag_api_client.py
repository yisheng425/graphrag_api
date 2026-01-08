#!/usr/bin/env python3
"""
GraphRAG API 客户端
用于与 GraphRAG API 服务进行异步查询交互
"""

import requests
import time
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class GraphRAGConfig:
    """GraphRAG API 配置"""
    api_base_url: str = "http://10.176.25.114:8001"
    timeout: int = 180  # 增加到180秒
    poll_interval: int = 2  # 轮询间隔（秒）
    max_poll_attempts: int = 60  # 最大轮询次数 (60 * 2 = 120秒)


class GraphRAGClient:
    """GraphRAG API 异步客户端"""
    
    def __init__(self, config: Optional[GraphRAGConfig] = None):
        """
        初始化 GraphRAG 客户端
        
        Args:
            config: API 配置，如果为 None 则使用默认配置
        """
        self.config = config or GraphRAGConfig()
        self.logger = logging.getLogger(__name__)
        
    def submit_query(self, 
                     query: str, 
                     root: str = "/home/xhm/graphrag/ragtest",
                     method: str = "local",
                     **kwargs) -> Optional[str]:
        """
        提交 GraphRAG 查询任务
        
        Args:
            query: 查询文本
            root: GraphRAG 项目根目录
            method: 查询方法 (local, global, drift, basic)
            **kwargs: 其他查询参数
            
        Returns:
            task_id: 任务ID，如果提交失败则返回 None
        """
        url = f"{self.config.api_base_url}/query"
        
        data = {
            "query": query,
            "root": root,
            "method": method,
            **kwargs
        }
        
        self.logger.info(f"[GraphRAG] 提交查询: {query[:50]}...")
        self.logger.debug(f"[GraphRAG] 请求数据: {data}")
        
        try:
            response = requests.post(
                url,
                json=data,
                timeout=30,  # 提交任务只需要30秒
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                result = response.json()
                task_id = result.get('task_id')
                self.logger.info(f"[GraphRAG] 任务已提交: {task_id}")
                return task_id
            else:
                self.logger.error(f"[GraphRAG] 提交失败: HTTP {response.status_code}")
                self.logger.error(f"[GraphRAG] 响应: {response.text}")
                return None
                
        except requests.Timeout:
            self.logger.error(f"[GraphRAG] 提交超时")
            return None
        except Exception as e:
            self.logger.error(f"[GraphRAG] 提交异常: {e}")
            return None
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        获取任务状态
        
        Args:
            task_id: 任务ID
            
        Returns:
            任务状态信息，包含 status, state, result 等字段
        """
        url = f"{self.config.api_base_url}/tasks/{task_id}"
        
        try:
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.warning(f"[GraphRAG] 状态查询失败: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            self.logger.warning(f"[GraphRAG] 状态查询异常: {e}")
            return None
    
    def wait_for_result(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        等待任务完成并返回结果
        
        Args:
            task_id: 任务ID
            
        Returns:
            查询结果，如果失败返回 None
        """
        start_time = time.time()
        attempts = 0
        
        self.logger.info(f"[GraphRAG] 等待任务完成: {task_id}")
        
        while attempts < self.config.max_poll_attempts:
            status_info = self.get_task_status(task_id)
            
            if not status_info:
                self.logger.warning(f"[GraphRAG] 无法获取任务状态 (尝试 {attempts + 1}/{self.config.max_poll_attempts})")
                time.sleep(self.config.poll_interval)
                attempts += 1
                continue
            
            state = status_info.get('state', 'UNKNOWN')
            status = status_info.get('status', 'unknown')
            
            self.logger.debug(f"[GraphRAG] 任务状态: {state}, 已等待 {time.time() - start_time:.1f}秒")
            
            # 任务成功完成
            if state == 'SUCCESS':
                elapsed = time.time() - start_time
                self.logger.info(f"[GraphRAG] 任务完成，耗时 {elapsed:.2f}秒")
                return status_info.get('result')
            
            # 任务失败
            elif state == 'FAILURE':
                error = status_info.get('error', 'Unknown error')
                self.logger.error(f"[GraphRAG] 任务失败: {error}")
                return None
            
            # 任务仍在进行中 (PENDING, STARTED, RETRY, etc.)
            else:
                time.sleep(self.config.poll_interval)
                attempts += 1
        
        # 超时
        elapsed = time.time() - start_time
        self.logger.error(f"[GraphRAG] 任务超时，已等待 {elapsed:.2f}秒")
        return None
    
    def query(self, 
              query: str, 
              root: str = "/home/xhm/graphrag/ragtest",
              method: str = "local",
              **kwargs) -> Optional[Dict[str, Any]]:
        """
        同步查询（提交并等待结果）
        
        Args:
            query: 查询文本
            root: GraphRAG 项目根目录
            method: 查询方法
            **kwargs: 其他查询参数
            
        Returns:
            查询结果，如果失败返回 None
        """
        # 提交任务
        task_id = self.submit_query(query, root, method, **kwargs)
        if not task_id:
            return None
        
        # 等待结果
        return self.wait_for_result(task_id)
    
    def extract_entities_and_relationships(self, 
                                           text: str,
                                           root: str = "/home/xhm/graphrag/ragtest") -> Dict[str, Any]:
        """
        从文本中提取实体和关系
        
        Args:
            text: 待提取的文本
            root: GraphRAG 项目根目录
            
        Returns:
            包含实体和关系的字典
        """
        result = self.query(query=text, root=root, method="local")
        
        if result:
            # 解析结果
            entities = []
            relationships = []
            
            # 根据实际返回结构提取实体和关系
            # 这里需要根据您的实际需求调整
            context = result.get('context', {})
            if isinstance(context, dict):
                entities = context.get('entities', [])
                relationships = context.get('relationships', [])
            
            return {
                'entities': entities,
                'relationships': relationships,
                'response': result.get('response', '')
            }
        else:
            # 失败时返回空结果
            return {
                'entities': [],
                'relationships': [],
                'response': None
            }


def test_client():
    """测试 GraphRAG 客户端"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 创建客户端
    client = GraphRAGClient()
    
    # 测试查询
    test_query = "投诉问题"
    
    print("="*60)
    print(f"测试查询: {test_query}")
    print("="*60)
    
    result = client.query(test_query)
    
    if result:
        print("\n✅ 查询成功!")
        print(f"Method: {result.get('method')}")
        print(f"Response Type: {result.get('response_type')}")
        print(f"\nResponse (前500字符):")
        print("-" * 60)
        response_text = str(result.get('response', ''))
        print(response_text[:500])
        
        # 显示上下文信息
        context = result.get('context')
        if context:
            print(f"\nContext 信息:")
            print(f"- Type: {type(context)}")
            if isinstance(context, dict):
                for key in list(context.keys())[:5]:
                    print(f"- {key}: {type(context[key])}")
    else:
        print("\n❌ 查询失败")


if __name__ == "__main__":
    test_client()
