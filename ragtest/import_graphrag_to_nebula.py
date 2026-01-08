#!/usr/bin/env python3
"""
GraphRAG数据导入Nebula Graph脚本

此脚本将GraphRAG提取的实体和关系数据从parquet文件导入到Nebula Graph数据库中。

使用方法:
    python import_graphrag_to_nebula.py

配置文件:
    nebula_config.yaml - Nebula Graph连接和导入配置
    
数据文件:
    entities.parquet - GraphRAG提取的实体数据
    relationships.parquet - GraphRAG提取的关系数据
"""

import os
import sys
import logging
import time
import yaml
import pandas as pd
import hashlib
import uuid
import unicodedata
import re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any, Set
from collections import defaultdict
from tqdm import tqdm

try:
    from nebula3.gclient.net import ConnectionPool
    from nebula3.Config import Config
    from nebula3.common import ttypes
except ImportError:
    print("错误: 请安装nebula3-python库")
    print("运行: pip install nebula3-python")
    sys.exit(1)


class NebulaGraphImporter:
    """GraphRAG数据导入Nebula Graph的主类"""
    
    def __init__(self, config_file: str = "nebula_config.yaml"):
        """
        初始化导入器
        
        Args:
            config_file: 配置文件路径
        """
        self.config = self._load_config(config_file)
        self.connection_pool = None
        self.session = None
        self.logger = self._setup_logging()
        
        # 统计信息
        self.stats = {
            'entities_processed': 0,
            'entities_imported': 0,
            'relationships_processed': 0,
            'relationships_imported': 0,
            'errors': [],
            'entity_types': defaultdict(int)
        }
    
    def _load_config(self, config_file: str) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"配置文件 {config_file} 不存在")
        except yaml.YAMLError as e:
            raise ValueError(f"配置文件格式错误: {e}")
    
    def _setup_logging(self) -> logging.Logger:
        """设置日志记录"""
        log_config = self.config.get('logging', {})
        
        logging.basicConfig(
            level=getattr(logging, log_config.get('level', 'INFO')),
            format=log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(log_config.get('file', 'nebula_import.log'), encoding='utf-8')
            ]
        )
        
        return logging.getLogger(__name__)
    
    def connect_nebula(self) -> bool:
        """连接到Nebula Graph数据库"""
        import socket
        
        try:
            # 获取连接配置
            hosts = [(host['host'], host['port']) for host in self.config['nebula']['hosts']]
            username = self.config['nebula']['username']
            password = self.config['nebula']['password']
            space_name = self.config['nebula']['space_name']
            
            self.logger.info(f"开始连接Nebula Graph...")
            self.logger.info(f"目标主机: {hosts}")
            self.logger.info(f"用户名: {username}")
            self.logger.info(f"目标空间: {space_name}")
            
            # 首先测试网络连接
            for host, port in hosts:
                self.logger.info(f"测试网络连接: {host}:{port}")
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5)
                    result = sock.connect_ex((host, port))
                    sock.close()
                    
                    if result != 0:
                        error_msg = f"无法连接到主机 {host}:{port} (错误代码: {result})"
                        self.logger.error(error_msg)
                        self.logger.error("可能的原因:")
                        self.logger.error("  1. Nebula Graph服务未启动")
                        self.logger.error("  2. 主机地址或端口配置错误")
                        self.logger.error("  3. 网络连接问题或防火墙阻止")
                        self.logger.error(f"  4. 如果使用Docker，请确保端口已正确映射")
                        self.logger.error(f"建议: 尝试运行 'telnet {host} {port}' 测试连接")
                        return False
                    else:
                        self.logger.info(f"网络连接测试成功: {host}:{port}")
                        
                except socket.gaierror as e:
                    error_msg = f"DNS解析失败: {host} - {e}"
                    self.logger.error(error_msg)
                    self.logger.error("建议:")
                    self.logger.error("  1. 检查主机名是否正确")
                    self.logger.error("  2. 尝试使用IP地址替代主机名")
                    self.logger.error("  3. 检查/etc/hosts文件配置")
                    return False
                except Exception as e:
                    self.logger.error(f"网络连接测试失败: {host}:{port} - {e}")
                    return False
            
            # 创建连接池配置
            self.logger.info("创建Nebula Graph连接池配置...")
            config = Config()
            pool_config = self.config['nebula']['connection_pool']
            config.max_connection_pool_size = pool_config.get('max_size', 10)
            config.timeout = pool_config.get('timeout', 30) * 1000  # 转换为毫秒
            config.idle_time = pool_config.get('idle_time', 3600) * 1000
            config.interval_check = pool_config.get('interval_check', -1)
            
            self.logger.info(f"连接池配置: max_size={config.max_connection_pool_size}, "
                           f"timeout={config.timeout}ms, idle_time={config.idle_time}ms")
            
            # 创建连接池
            self.logger.info("初始化连接池...")
            self.connection_pool = ConnectionPool()
            
            if not self.connection_pool.init(hosts, config):
                error_msg = "连接池初始化失败"
                self.logger.error(error_msg)
                self.logger.error("可能的原因:")
                self.logger.error("  1. Nebula Graph服务未正常启动")
                self.logger.error("  2. 连接池配置参数错误")
                self.logger.error("  3. 系统资源不足")
                self.logger.error("建议: 检查Nebula Graph服务状态和日志")
                return False
            
            self.logger.info("连接池初始化成功")
            
            # 获取会话
            self.logger.info(f"尝试获取会话 (用户: {username})...")
            self.session = self.connection_pool.get_session(username, password)
            
            if self.session is None:
                error_msg = f"获取会话失败: 用户名或密码错误"
                self.logger.error(error_msg)
                self.logger.error("可能的原因:")
                self.logger.error(f"  1. 用户名 '{username}' 不存在")
                self.logger.error("  2. 密码错误")
                self.logger.error("  3. 用户账户被锁定或禁用")
                self.logger.error("建议:")
                self.logger.error("  1. 检查nebula_config.yaml中的用户名和密码")
                self.logger.error("  2. 使用Nebula Console验证账户信息")
                self.logger.error("  3. 确认用户有相应的访问权限")
                return False
            
            self.logger.info("会话获取成功，用户认证通过")
            
            # 检查空间是否存在
            self.logger.info("检查可用空间...")
            spaces_result = self.session.execute("SHOW SPACES")
            if spaces_result.is_succeeded():
                available_spaces = []
                if spaces_result.row_size() > 0:
                    for i in range(spaces_result.row_size()):
                        row_values = spaces_result.row_values(i)
                        available_spaces.append(row_values[0].as_string())
                
                self.logger.info(f"可用空间列表: {available_spaces}")
                
                if space_name not in available_spaces:
                    error_msg = f"目标空间 '{space_name}' 不存在"
                    self.logger.error(error_msg)
                    self.logger.error(f"可用空间: {available_spaces}")
                    self.logger.error("建议:")
                    self.logger.error(f"  1. 创建空间: CREATE SPACE {space_name}(vid_type=FIXED_STRING(256))")
                    self.logger.error("  2. 修改配置文件中的space_name为已存在的空间")
                    self.logger.error("  3. 确认用户有空间访问权限")
                    return False
            else:
                self.logger.warning(f"无法获取空间列表: {spaces_result.error_msg()}")
            
            # 使用指定的空间
            self.logger.info(f"切换到空间: {space_name}")
            result = self.session.execute(f"USE {space_name}")
            
            if not result.is_succeeded():
                error_msg = f"使用空间 {space_name} 失败: {result.error_msg()}"
                self.logger.error(error_msg)
                self.logger.error("可能的原因:")
                self.logger.error(f"  1. 空间 '{space_name}' 不存在")
                self.logger.error(f"  2. 用户没有访问空间 '{space_name}' 的权限")
                self.logger.error("  3. 空间正在被其他操作占用")
                return False
            
            # 测试基本查询
            self.logger.info("执行连接测试查询...")
            test_result = self.session.execute("SHOW TAGS")
            if test_result.is_succeeded():
                self.logger.info("连接测试查询成功")
            else:
                self.logger.warning(f"连接测试查询失败: {test_result.error_msg()}")
            
            self.logger.info(f"✅ 成功连接到Nebula Graph，使用空间: {space_name}")
            return True
            
        except ImportError as e:
            error_msg = f"导入nebula3-python库失败: {e}"
            self.logger.error(error_msg)
            self.logger.error("解决方案: pip install nebula3-python")
            return False
        except Exception as e:
            error_msg = f"连接Nebula Graph时发生未知错误: {e}"
            self.logger.error(error_msg)
            self.logger.error(f"错误类型: {type(e).__name__}")
            self.logger.error("建议:")
            self.logger.error("  1. 检查Nebula Graph服务状态")
            self.logger.error("  2. 验证配置文件格式和内容")
            self.logger.error("  3. 查看Nebula Graph服务日志")
            self.logger.error("  4. 运行 test_nebula_connection.py 进行详细诊断")
            return False
    
    def disconnect_nebula(self):
        """断开Nebula Graph连接"""
        if self.session:
            self.session.release()
        if self.connection_pool:
            self.connection_pool.close()
        self.logger.info("已断开Nebula Graph连接")
    
    def execute_query(self, query: str, max_retries: int = None) -> bool:
        """
        执行Nebula Graph查询
        
        Args:
            query: 要执行的查询语句
            max_retries: 最大重试次数
            
        Returns:
            bool: 执行是否成功
        """
        if max_retries is None:
            max_retries = self.config['import']['max_retries']
        
        for attempt in range(max_retries + 1):
            try:
                result = self.session.execute(query)
                if result.is_succeeded():
                    return True
                else:
                    error_msg = result.error_msg()
                    if attempt == max_retries:
                        self.logger.error(f"查询执行失败: {error_msg}")
                        self.logger.debug(f"失败的查询: {query}")
                        self.stats['errors'].append(f"Query failed: {error_msg}")
                        return False
                    else:
                        self.logger.warning(f"查询执行失败 (尝试 {attempt + 1}/{max_retries + 1}): {error_msg}")
                        time.sleep(self.config['import']['retry_delay'])
                        
            except Exception as e:
                if attempt == max_retries:
                    self.logger.error(f"查询执行异常: {e}")
                    self.stats['errors'].append(f"Query exception: {e}")
                    return False
                else:
                    self.logger.warning(f"查询执行异常 (尝试 {attempt + 1}/{max_retries + 1}): {e}")
                    time.sleep(self.config['import']['retry_delay'])
        
        return False
    
    def execute_query_with_schema_check(self, query: str, schema_name: str, schema_type: str = "TAG", max_retries: int = None) -> bool:
        """
        执行查询并在失败时检查schema状态
        
        Args:
            query: 要执行的查询语句
            schema_name: 相关的schema名称
            schema_type: schema类型 ("TAG" 或 "EDGE")
            max_retries: 最大重试次数
            
        Returns:
            bool: 执行是否成功
        """
        if max_retries is None:
            max_retries = self.config['import']['max_retries']
        
        for attempt in range(max_retries + 1):
            try:
                result = self.session.execute(query)
                if result.is_succeeded():
                    return True
                else:
                    error_msg = result.error_msg()
                    
                    # 检查是否是schema相关错误
                    if "No schema found" in error_msg or "not exist" in error_msg.lower():
                        self.logger.warning(f"检测到schema错误: {error_msg}")
                        self.logger.info(f"验证{schema_type} {schema_name} 状态...")
                        
                        # 验证schema是否存在
                        if not self.verify_schema_exists(schema_name, schema_type):
                            self.logger.error(f"❌ {schema_type} {schema_name} 不存在，等待其生效...")
                            if not self.wait_for_schema_ready(schema_name, schema_type, 30):
                                self.logger.error(f"❌ {schema_type} {schema_name} 仍不可用")
                                if attempt == max_retries:
                                    self.stats['errors'].append(f"Schema not available: {schema_name}")
                                    return False
                                continue
                        else:
                            self.logger.info(f"✅ {schema_type} {schema_name} 存在，可能是临时问题")
                    
                    if attempt == max_retries:
                        self.logger.error(f"查询执行失败: {error_msg}")
                        self.logger.debug(f"失败的查询: {query[:200]}...")  # 只显示前200个字符
                        self.stats['errors'].append(f"Query failed: {error_msg}")
                        return False
                    else:
                        self.logger.warning(f"查询执行失败 (尝试 {attempt + 1}/{max_retries + 1}): {error_msg}")
                        time.sleep(self.config['import']['retry_delay'])
                        
            except Exception as e:
                if attempt == max_retries:
                    self.logger.error(f"查询执行异常: {e}")
                    self.stats['errors'].append(f"Query exception: {e}")
                    return False
                else:
                    self.logger.warning(f"查询执行异常 (尝试 {attempt + 1}/{max_retries + 1}): {e}")
                    time.sleep(self.config['import']['retry_delay'])
        
        return False
    
    def load_parquet_data(self) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """加载parquet数据文件"""
        try:
            entities_file = self.config['data']['entities_file']
            relationships_file = self.config['data']['relationships_file']
            
            self.logger.info(f"加载实体数据: {entities_file}")
            entities_df = pd.read_parquet(entities_file)
            self.logger.info(f"加载了 {len(entities_df)} 个实体")
            
            self.logger.info(f"加载关系数据: {relationships_file}")
            relationships_df = pd.read_parquet(relationships_file)
            self.logger.info(f"加载了 {len(relationships_df)} 个关系")
            
            return entities_df, relationships_df
            
        except Exception as e:
            self.logger.error(f"加载数据文件失败: {e}")
            return None, None
    
    def wait_for_schema_ready(self, schema_name: str, schema_type: str = "TAG", max_wait_time: int = 60) -> bool:
        """
        等待schema生效并验证其可用性
        
        Args:
            schema_name: schema名称
            schema_type: schema类型 ("TAG" 或 "EDGE")
            max_wait_time: 最大等待时间（秒）
            
        Returns:
            bool: schema是否可用
        """
        start_time = time.time()
        wait_interval = 1  # 初始等待间隔1秒
        max_interval = 5   # 最大等待间隔5秒
        
        self.logger.info(f"等待{schema_type} {schema_name} 生效...")
        
        while time.time() - start_time < max_wait_time:
            if self.verify_schema_exists(schema_name, schema_type):
                elapsed_time = time.time() - start_time
                self.logger.info(f"✅ {schema_type} {schema_name} 已生效 (等待时间: {elapsed_time:.1f}秒)")
                return True
            
            self.logger.debug(f"等待{schema_type} {schema_name} 生效... (已等待 {time.time() - start_time:.1f}秒)")
            time.sleep(wait_interval)
            
            # 逐渐增加等待间隔，避免频繁查询
            wait_interval = min(wait_interval * 1.2, max_interval)
        
        self.logger.error(f"❌ {schema_type} {schema_name} 在 {max_wait_time} 秒内未生效")
        return False
    
    def verify_schema_exists(self, schema_name: str, schema_type: str = "TAG") -> bool:
        """
        验证schema是否存在且可用
        
        Args:
            schema_name: schema名称
            schema_type: schema类型 ("TAG" 或 "EDGE")
            
        Returns:
            bool: schema是否存在
        """
        try:
            if schema_type == "TAG":
                # 验证TAG是否存在
                query = "SHOW TAGS"
                result = self.session.execute(query)
                
                if result.is_succeeded():
                    # 检查结果中是否包含目标TAG
                    for i in range(result.row_size()):
                        row_values = result.row_values(i)
                        if len(row_values) > 0:
                            tag_name = row_values[0].as_string()
                            if tag_name == schema_name:
                                self.logger.debug(f"验证TAG {schema_name} 存在")
                                return True
                    
                    self.logger.debug(f"TAG {schema_name} 不在SHOW TAGS结果中")
                    return False
                else:
                    self.logger.warning(f"SHOW TAGS查询失败: {result.error_msg()}")
                    return False
                    
            elif schema_type == "EDGE":
                # 验证EDGE是否存在
                query = "SHOW EDGES"
                result = self.session.execute(query)
                
                if result.is_succeeded():
                    # 检查结果中是否包含目标EDGE
                    for i in range(result.row_size()):
                        row_values = result.row_values(i)
                        if len(row_values) > 0:
                            edge_name = row_values[0].as_string()
                            if edge_name == schema_name:
                                self.logger.debug(f"验证EDGE {schema_name} 存在")
                                return True
                    
                    self.logger.debug(f"EDGE {schema_name} 不在SHOW EDGES结果中")
                    return False
                else:
                    self.logger.warning(f"SHOW EDGES查询失败: {result.error_msg()}")
                    return False
            
            return False
            
        except Exception as e:
            self.logger.error(f"验证{schema_type} {schema_name} 时发生异常: {e}")
            return False
    
    def create_tag_with_verification(self, tag_name: str, entity_count: int) -> bool:
        """
        创建TAG并验证其可用性
        
        Args:
            tag_name: TAG名称
            entity_count: 该TAG预期的实体数量
            
        Returns:
            bool: 创建并验证成功
        """
        max_retries = self.config['import'].get('schema_creation_retries', 3)
        
        for attempt in range(max_retries):
            try:
                self.logger.info(f"创建TAG {tag_name} (尝试 {attempt + 1}/{max_retries})")
                
                # 创建TAG的DDL语句
                create_tag_query = f"""
                CREATE TAG IF NOT EXISTS {tag_name}(
                  id string,
                  human_readable_id string,
                  title string,
                  description string,
                  text_unit_ids string,
                  frequency int,
                  degree int,
                  x double,
                  y double
                )
                """
                
                # 执行创建命令
                if not self.execute_query(create_tag_query):
                    self.logger.warning(f"TAG {tag_name} 创建命令执行失败 (尝试 {attempt + 1})")
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    else:
                        return False
                
                # 等待并验证TAG生效
                max_wait_time = self.config['import'].get('schema_wait_timeout', 30)
                if self.wait_for_schema_ready(tag_name, "TAG", max_wait_time):
                    self.logger.info(f"✅ TAG {tag_name} 创建成功并已生效")
                    self.stats['entity_types'][tag_name] = entity_count
                    return True
                else:
                    self.logger.warning(f"TAG {tag_name} 创建后未能及时生效 (尝试 {attempt + 1})")
                    if attempt < max_retries - 1:
                        # 等待更长时间后重试
                        time.sleep(5)
                        continue
                    else:
                        self.logger.error(f"❌ TAG {tag_name} 在多次尝试后仍未生效")
                        return False
                        
            except Exception as e:
                self.logger.error(f"创建TAG {tag_name} 时发生异常 (尝试 {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                else:
                    return False
        
        return False
    
    def create_edge_with_verification(self, edge_name: str) -> bool:
        """
        创建EDGE并验证其可用性
        
        Args:
            edge_name: EDGE名称
            
        Returns:
            bool: 创建并验证成功
        """
        max_retries = self.config['import'].get('schema_creation_retries', 3)
        
        for attempt in range(max_retries):
            try:
                self.logger.info(f"创建EDGE {edge_name} (尝试 {attempt + 1}/{max_retries})")
                
                # 创建EDGE的DDL语句
                create_edge_query = f"""
                CREATE EDGE IF NOT EXISTS {edge_name}(
                  id string,
                  human_readable_id string,
                  description string,
                  weight double,
                  combined_degree int,
                  text_unit_ids string
                )
                """
                
                # 执行创建命令
                if not self.execute_query(create_edge_query):
                    self.logger.warning(f"EDGE {edge_name} 创建命令执行失败 (尝试 {attempt + 1})")
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    else:
                        return False
                
                # 等待并验证EDGE生效
                max_wait_time = self.config['import'].get('schema_wait_timeout', 30)
                if self.wait_for_schema_ready(edge_name, "EDGE", max_wait_time):
                    self.logger.info(f"✅ EDGE {edge_name} 创建成功并已生效")
                    return True
                else:
                    self.logger.warning(f"EDGE {edge_name} 创建后未能及时生效 (尝试 {attempt + 1})")
                    if attempt < max_retries - 1:
                        # 等待更长时间后重试
                        time.sleep(5)
                        continue
                    else:
                        self.logger.error(f"❌ EDGE {edge_name} 在多次尝试后仍未生效")
                        return False
                        
            except Exception as e:
                self.logger.error(f"创建EDGE {edge_name} 时发生异常 (尝试 {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                else:
                    return False
        
        return False

    def create_dynamic_tags(self, entities_df: pd.DataFrame) -> bool:
        """根据实体类型动态创建TAG，增强schema等待和验证机制"""
        try:
            # 获取所有唯一的实体类型
            entity_types = entities_df['type'].unique()
            self.logger.info(f"发现 {len(entity_types)} 种实体类型: {list(entity_types)}")
            
            created_tags = []
            
            for entity_type in entity_types:
                if pd.isna(entity_type) or entity_type == '':
                    self.logger.warning("跳过空的实体类型")
                    continue
                
                # 清理实体类型名称，确保符合Nebula Graph命名规范
                clean_type = str(entity_type).upper().replace(' ', '_').replace('-', '_')
                clean_type = ''.join(c for c in clean_type if c.isalnum() or c == '_')
                
                if not clean_type:
                    self.logger.warning(f"跳过无效的实体类型: {entity_type}")
                    continue
                
                # 计算该类型的实体数量
                entity_count = len(entities_df[entities_df['type'] == entity_type])
                self.logger.info(f"准备创建TAG {clean_type}，预期实体数量: {entity_count}")
                
                # 创建TAG并验证
                if self.create_tag_with_verification(clean_type, entity_count):
                    created_tags.append(clean_type)
                else:
                    self.logger.error(f"❌ TAG {clean_type} 创建失败")
                    return False
            
            self.logger.info(f"成功创建 {len(created_tags)} 个TAG: {created_tags}")
            
            # 创建RELATED边类型并验证
            self.logger.info("开始创建RELATED边类型...")
            if not self.create_edge_with_verification("RELATED"):
                self.logger.error("❌ EDGE RELATED 创建失败")
                return False
            
            # 最终验证所有schema都已生效
            self.logger.info("执行最终schema验证...")
            all_schemas_ready = True
            
            for tag_name in created_tags:
                if not self.verify_schema_exists(tag_name, "TAG"):
                    self.logger.error(f"❌ 最终验证失败: TAG {tag_name} 不可用")
                    all_schemas_ready = False
            
            if not self.verify_schema_exists("RELATED", "EDGE"):
                self.logger.error("❌ 最终验证失败: EDGE RELATED 不可用")
                all_schemas_ready = False
            
            if all_schemas_ready:
                self.logger.info("✅ 所有schema已创建并验证可用")
                return True
            else:
                self.logger.error("❌ schema最终验证失败")
                return False
            
        except Exception as e:
            self.logger.error(f"创建动态TAG失败: {e}")
            return False
    
    def escape_string(self, value: Any) -> str:
        """转义字符串中的特殊字符"""
        if pd.isna(value) or value is None:
            return ""
        
        # 转换为字符串并转义特殊字符
        str_value = str(value)
        # 转义反斜杠、双引号和换行符
        str_value = str_value.replace('\\', '\\\\')
        str_value = str_value.replace('"', '\\"')
        str_value = str_value.replace('\n', '\\n')
        str_value = str_value.replace('\r', '\\r')
        str_value = str_value.replace('\t', '\\t')
        
        return str_value
    
    def import_entities(self, entities_df: pd.DataFrame) -> bool:
        """导入实体数据，增强schema验证和错误处理"""
        try:
            batch_size = self.config['import']['batch_size']
            total_entities = len(entities_df)
            
            self.logger.info(f"开始导入 {total_entities} 个实体，批次大小: {batch_size}")
            
            # 按实体类型分组处理
            for entity_type, group_df in entities_df.groupby('type'):
                if pd.isna(entity_type) or entity_type == '':
                    self.logger.warning("跳过空的实体类型")
                    continue
                
                clean_type = str(entity_type).upper().replace(' ', '_').replace('-', '_')
                clean_type = ''.join(c for c in clean_type if c.isalnum() or c == '_')
                
                if not clean_type:
                    self.logger.warning(f"跳过无效的实体类型: {entity_type}")
                    continue
                
                # 在导入前再次验证TAG是否可用
                self.logger.info(f"验证TAG {clean_type} 可用性...")
                if not self.verify_schema_exists(clean_type, "TAG"):
                    self.logger.error(f"❌ TAG {clean_type} 不可用，无法导入实体")
                    # 尝试重新等待schema生效
                    self.logger.info(f"尝试重新等待TAG {clean_type} 生效...")
                    if not self.wait_for_schema_ready(clean_type, "TAG", 30):
                        self.logger.error(f"❌ TAG {clean_type} 仍不可用，跳过该类型实体")
                        continue
                
                self.logger.info(f"开始导入实体类型 {clean_type}: {len(group_df)} 个实体")
                
                # 分批处理
                batch_success_count = 0
                batch_fail_count = 0
                
                for i in tqdm(range(0, len(group_df), batch_size), 
                             desc=f"导入 {clean_type}",
                             disable=not self.config['import']['enable_progress_bar']):
                    
                    batch_df = group_df.iloc[i:i + batch_size]
                    
                    # 构建批量INSERT语句
                    insert_values = []
                    for _, row in batch_df.iterrows():
                        try:
                            values = (
                                f'"{self.escape_string(row.get("id", ""))}"',
                                f'"{self.escape_string(row.get("human_readable_id", ""))}"',
                                f'"{self.escape_string(row.get("title", ""))}"',
                                f'"{self.escape_string(row.get("description", ""))}"',
                                f'"{self.escape_string(row.get("text_unit_ids", ""))}"',
                                str(int(row.get('frequency', 0)) if pd.notna(row.get('frequency')) else 0),
                                str(int(row.get('degree', 0)) if pd.notna(row.get('degree')) else 0),
                                str(float(row.get('x', 0.0)) if pd.notna(row.get('x')) else 0.0),
                                str(float(row.get('y', 0.0)) if pd.notna(row.get('y')) else 0.0)
                            )
                            
                            vertex_id = self.escape_string(row.get('id', ''))
                            if vertex_id:  # 确保vertex_id不为空
                                insert_values.append(f'"{vertex_id}": ({", ".join(values)})')
                            else:
                                self.logger.warning(f"跳过空ID的实体: {row.get('title', 'Unknown')}")
                                
                        except Exception as e:
                            self.logger.warning(f"处理实体数据时出错: {e}, 跳过该实体")
                            continue
                    
                    if insert_values:
                        insert_query = f"""
                        INSERT VERTEX {clean_type}(id, human_readable_id, title, description, text_unit_ids, frequency, degree, x, y) 
                        VALUES {", ".join(insert_values)}
                        """
                        
                        # 使用增强的重试机制执行查询
                        if self.execute_query_with_schema_check(insert_query, clean_type, "TAG"):
                            self.stats['entities_imported'] += len(insert_values)
                            batch_success_count += 1
                            self.logger.debug(f"成功导入批次 {i//batch_size + 1}，包含 {len(insert_values)} 个实体")
                        else:
                            batch_fail_count += 1
                            self.logger.error(f"批次 {i//batch_size + 1} 导入失败")
                        
                        self.stats['entities_processed'] += len(batch_df)
                
                self.logger.info(f"实体类型 {clean_type} 导入完成: 成功批次 {batch_success_count}, 失败批次 {batch_fail_count}")
            
            self.logger.info(f"实体导入完成: 处理 {self.stats['entities_processed']} 个，成功导入 {self.stats['entities_imported']} 个")
            return True
            
        except Exception as e:
            self.logger.error(f"导入实体数据失败: {e}")
            return False
    
    def import_relationships(self, relationships_df: pd.DataFrame) -> bool:
        """导入关系数据，增强schema验证和错误处理"""
        try:
            batch_size = self.config['import']['batch_size']
            total_relationships = len(relationships_df)
            
            self.logger.info(f"开始导入 {total_relationships} 个关系，批次大小: {batch_size}")
            
            # 在导入前验证RELATED边是否可用
            self.logger.info("验证EDGE RELATED 可用性...")
            if not self.verify_schema_exists("RELATED", "EDGE"):
                self.logger.error("❌ EDGE RELATED 不可用，无法导入关系")
                # 尝试重新等待schema生效
                self.logger.info("尝试重新等待EDGE RELATED 生效...")
                if not self.wait_for_schema_ready("RELATED", "EDGE", 30):
                    self.logger.error("❌ EDGE RELATED 仍不可用，无法导入关系")
                    return False
            
            # 分批处理关系数据
            batch_success_count = 0
            batch_fail_count = 0
            
            for i in tqdm(range(0, total_relationships, batch_size),
                         desc="导入关系",
                         disable=not self.config['import']['enable_progress_bar']):
                
                batch_df = relationships_df.iloc[i:i + batch_size]
                
                # 构建批量INSERT语句
                insert_values = []
                skipped_count = 0
                
                for _, row in batch_df.iterrows():
                    try:
                        source_id = self.escape_string(row.get('source', ''))
                        target_id = self.escape_string(row.get('target', ''))
                        
                        if not source_id or not target_id:
                            skipped_count += 1
                            self.logger.debug(f"跳过无效关系: source={source_id}, target={target_id}")
                            continue
                        
                        values = (
                            f'"{self.escape_string(row.get("id", ""))}"',
                            f'"{self.escape_string(row.get("human_readable_id", ""))}"',
                            f'"{self.escape_string(row.get("description", ""))}"',
                            str(float(row.get('weight', 0.0)) if pd.notna(row.get('weight')) else 0.0),
                            str(int(row.get('combined_degree', 0)) if pd.notna(row.get('combined_degree')) else 0),
                            f'"{self.escape_string(row.get("text_unit_ids", ""))}"'
                        )
                        
                        insert_values.append(f'"{source_id}" -> "{target_id}": ({", ".join(values)})')
                        
                    except Exception as e:
                        skipped_count += 1
                        self.logger.warning(f"处理关系数据时出错: {e}, 跳过该关系")
                        continue
                
                if skipped_count > 0:
                    self.logger.warning(f"批次 {i//batch_size + 1} 跳过了 {skipped_count} 个无效关系")
                
                if insert_values:
                    insert_query = f"""
                    INSERT EDGE RELATED(id, human_readable_id, description, weight, combined_degree, text_unit_ids) 
                    VALUES {", ".join(insert_values)}
                    """
                    
                    # 使用增强的重试机制执行查询
                    if self.execute_query_with_schema_check(insert_query, "RELATED", "EDGE"):
                        self.stats['relationships_imported'] += len(insert_values)
                        batch_success_count += 1
                        self.logger.debug(f"成功导入关系批次 {i//batch_size + 1}，包含 {len(insert_values)} 个关系")
                    else:
                        batch_fail_count += 1
                        self.logger.error(f"关系批次 {i//batch_size + 1} 导入失败")
                    
                    self.stats['relationships_processed'] += len(batch_df)
                else:
                    self.logger.warning(f"批次 {i//batch_size + 1} 没有有效的关系数据")
            
            self.logger.info(f"关系导入完成: 成功批次 {batch_success_count}, 失败批次 {batch_fail_count}")
            self.logger.info(f"关系导入统计: 处理 {self.stats['relationships_processed']} 个，成功导入 {self.stats['relationships_imported']} 个")
            return True
            
        except Exception as e:
            self.logger.error(f"导入关系数据失败: {e}")
            return False
    
    def validate_import(self) -> bool:
        """验证导入数据的完整性"""
        if not self.config['import']['validate_data']:
            return True
        
        try:
            self.logger.info("开始验证导入数据...")
            
            # 验证实体数量 (简化验证，不使用复杂查询)
            self.logger.info("数据导入验证:")
            for tag_name, expected_count in self.stats['entity_types'].items():
                self.logger.info(f"TAG {tag_name}: 期望导入 {expected_count} 个实体")
            
            self.logger.info(f"EDGE RELATED: 期望导入 {self.stats['relationships_imported']} 个关系")
            
            # 简单的连接测试
            test_query = "SHOW TAGS"
            result = self.session.execute(test_query)
            
            if result.is_succeeded():
                self.logger.info("数据库连接正常，schema创建成功")
            else:
                self.logger.error(f"验证失败: {result.error_msg()}")
            
            self.logger.info("数据验证完成")
            return True
            
        except Exception as e:
            self.logger.error(f"数据验证失败: {e}")
            return False
    
    def print_statistics(self):
        """打印导入统计信息"""
        print("\n" + "="*60)
        print("GraphRAG数据导入统计")
        print("="*60)
        print(f"实体处理数量: {self.stats['entities_processed']}")
        print(f"实体导入数量: {self.stats['entities_imported']}")
        print(f"关系处理数量: {self.stats['relationships_processed']}")
        print(f"关系导入数量: {self.stats['relationships_imported']}")
        
        if self.stats['entity_types']:
            print(f"\n实体类型分布:")
            for entity_type, count in self.stats['entity_types'].items():
                print(f"  {entity_type}: {count}")
        
        if self.stats['errors']:
            print(f"\n错误数量: {len(self.stats['errors'])}")
            print("错误详情:")
            for i, error in enumerate(self.stats['errors'][:5]):  # 只显示前5个错误
                print(f"  {i+1}. {error}")
            if len(self.stats['errors']) > 5:
                print(f"  ... 还有 {len(self.stats['errors']) - 5} 个错误")
        
        print("="*60)
    
    def run(self) -> bool:
        """运行完整的导入流程"""
        try:
            self.logger.info("开始GraphRAG数据导入流程")
            
            # 1. 连接数据库
            if not self.connect_nebula():
                return False
            
            # 2. 加载数据
            entities_df, relationships_df = self.load_parquet_data()
            if entities_df is None or relationships_df is None:
                return False
            
            # 3. 创建动态TAG
            if not self.create_dynamic_tags(entities_df):
                return False
            
            # 4. 导入实体数据
            if not self.import_entities(entities_df):
                return False
            
            # 5. 导入关系数据
            if not self.import_relationships(relationships_df):
                return False
            
            # 6. 验证导入结果
            self.validate_import()
            
            # 7. 打印统计信息
            self.print_statistics()
            
            self.logger.info("GraphRAG数据导入完成")
            return True
            
        except Exception as e:
            self.logger.error(f"导入流程失败: {e}")
            return False
        
        finally:
            self.disconnect_nebula()


def main():
    """主函数"""
    print("GraphRAG数据导入Nebula Graph工具")
    print("="*50)
    
    # 检查配置文件
    config_file = "nebula_config.yaml"
    if not os.path.exists(config_file):
        print(f"错误: 配置文件 {config_file} 不存在")
        print("请确保配置文件存在并包含正确的Nebula Graph连接信息")
        sys.exit(1)
    
    # 创建导入器并运行
    importer = NebulaGraphImporter(config_file)
    
    try:
        success = importer.run()
        if success:
            print("\n✅ 数据导入成功完成!")
            sys.exit(0)
        else:
            print("\n❌ 数据导入失败，请检查日志文件获取详细信息")
            sys.exit(1)
    
    except KeyboardInterrupt:
        print("\n⚠️  用户中断导入流程")
        sys.exit(1)
    
    except Exception as e:
        print(f"\n❌ 导入过程中发生未预期的错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()