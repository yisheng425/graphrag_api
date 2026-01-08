#!/usr/bin/env python3
"""
GraphRAG数据导入Nebula Graph脚本 (MD5版本)

此脚本将GraphRAG提取的实体和关系数据从CSV文件导入到Nebula Graph数据库中。
使用title的MD5编码作为实体ID，跳过没有类型的实体。
简化属性存储：实体只存储title、type、description，关系只存储source、target、description。

使用方法:
    python import_graphrag_to_nebula_md5.py

配置文件:
    nebula_config.yaml - Nebula Graph连接和导入配置
    
数据文件:
    entities.csv - GraphRAG提取的实体数据
    relationships.csv - GraphRAG提取的关系数据
"""

import os
import sys
import logging
import time
import yaml
import pandas as pd
import hashlib
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


class NebulaGraphImporterMD5:
    """GraphRAG数据导入Nebula Graph的主类 (MD5版本)"""
    
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
            'entity_types': defaultdict(int),
            'skipped_entities': 0
        }
        
        # 实体ID映射 (title -> md5)
        self.entity_id_map = {}
    
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
                logging.FileHandler(log_config.get('file', 'nebula_import_md5.log'), encoding='utf-8')
            ]
        )
        
        return logging.getLogger(__name__)
    
    def connect_nebula(self) -> bool:
        """连接到Nebula Graph数据库"""
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
            
            # 创建连接池配置
            self.logger.info("创建Nebula Graph连接池配置...")
            config = Config()
            pool_config = self.config['nebula']['connection_pool']
            config.max_connection_pool_size = pool_config.get('max_size', 10)
            config.timeout = pool_config.get('timeout', 30) * 1000  # 转换为毫秒
            config.idle_time = pool_config.get('idle_time', 3600) * 1000
            config.interval_check = pool_config.get('interval_check', -1)
            
            # 创建连接池
            self.logger.info("初始化连接池...")
            self.connection_pool = ConnectionPool()
            
            if not self.connection_pool.init(hosts, config):
                self.logger.error("连接池初始化失败")
                return False
            
            self.logger.info("连接池初始化成功")
            
            # 获取会话
            self.logger.info(f"尝试获取会话 (用户: {username})...")
            self.session = self.connection_pool.get_session(username, password)
            
            if self.session is None:
                self.logger.error(f"获取会话失败: 用户名或密码错误")
                return False
            
            self.logger.info("会话获取成功，用户认证通过")
            
            # 使用指定的空间
            self.logger.info(f"切换到空间: {space_name}")
            result = self.session.execute(f"USE {space_name}")
            
            if not result.is_succeeded():
                self.logger.error(f"使用空间 {space_name} 失败: {result.error_msg()}")
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
            
        except Exception as e:
            self.logger.error(f"连接Nebula Graph时发生错误: {e}")
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
    
    def generate_md5(self, text: str) -> str:
        """
        生成文本的MD5哈希值
        
        Args:
            text: 要哈希的文本
            
        Returns:
            str: MD5哈希值
        """
        if not text:
            return ""
        
        # 确保文本是字符串类型
        text_str = str(text)
        
        # 计算MD5哈希值
        md5_hash = hashlib.md5(text_str.encode('utf-8')).hexdigest()
        
        return md5_hash
    
    def load_csv_data(self) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """加载CSV数据文件"""
        try:
            # 使用固定路径
            entities_file = "/Users/bytedance/Desktop/github/graphrag/ragtest/output/entities.csv"
            relationships_file = "/Users/bytedance/Desktop/github/graphrag/ragtest/output/relationships.csv"
            
            self.logger.info(f"加载实体数据: {entities_file}")
            if not os.path.exists(entities_file):
                self.logger.error(f"实体数据文件不存在: {entities_file}")
                return None, None
                
            entities_df = pd.read_csv(entities_file)
            self.logger.info(f"加载了 {len(entities_df)} 个实体")
            
            self.logger.info(f"加载关系数据: {relationships_file}")
            if not os.path.exists(relationships_file):
                self.logger.error(f"关系数据文件不存在: {relationships_file}")
                return None, None
                
            relationships_df = pd.read_csv(relationships_file)
            self.logger.info(f"加载了 {len(relationships_df)} 个关系")
            
            return entities_df, relationships_df
            
        except Exception as e:
            self.logger.error(f"加载数据文件失败: {e}")
            return None, None
    
    def check_schema_exists(self) -> Tuple[bool, bool]:
        """检查TAG和EDGE是否存在"""
        try:
            # 检查TAG是否存在
            tag_exists = False
            edge_exists = False
            
            # 查询所有TAG
            show_tags_result = self.session.execute("SHOW TAGS")
            if show_tags_result.is_succeeded():
                tags = []
                for row in show_tags_result.rows():
                    # 直接获取第一个值
                    try:
                        # 尝试多种方式获取值
                        if hasattr(row, 'values') and callable(getattr(row, 'values')):
                            tag_name = row.values()[0].as_string()
                        elif hasattr(row, 'values') and not callable(getattr(row, 'values')):
                            tag_name = row.values[0].as_string()
                        elif hasattr(row, 'get_value'):
                            tag_name = row.get_value(0).as_string()
                        else:
                            # 最后尝试直接访问
                            tag_name = str(row[0])
                        tags.append(tag_name)
                    except Exception as e:
                        self.logger.warning(f"获取TAG名称时出错: {e}")
                        continue
                
                self.logger.info(f"现有TAG: {tags}")
                tag_exists = any(tag.lower() == "entity" for tag in tags)
            
            # 查询所有EDGE
            show_edges_result = self.session.execute("SHOW EDGES")
            if show_edges_result.is_succeeded():
                edges = []
                for row in show_edges_result.rows():
                    # 直接获取第一个值
                    try:
                        # 尝试多种方式获取值
                        if hasattr(row, 'values') and callable(getattr(row, 'values')):
                            edge_name = row.values()[0].as_string()
                        elif hasattr(row, 'values') and not callable(getattr(row, 'values')):
                            edge_name = row.values[0].as_string()
                        elif hasattr(row, 'get_value'):
                            edge_name = row.get_value(0).as_string()
                        else:
                            # 最后尝试直接访问
                            edge_name = str(row[0])
                        edges.append(edge_name)
                    except Exception as e:
                        self.logger.warning(f"获取EDGE名称时出错: {e}")
                        continue
                
                self.logger.info(f"现有EDGE: {edges}")
                edge_exists = any(edge.lower() == "related" for edge in edges)
            
            return tag_exists, edge_exists
            
        except Exception as e:
            self.logger.error(f"检查schema失败: {e}")
            return False, False
    
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
                  title string,
                  type string,
                  description string
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
                
                self.logger.info(f"✅ TAG {tag_name} 创建成功")
                self.stats['entity_types'][tag_name] = entity_count
                return True
                    
            except Exception as e:
                self.logger.error(f"创建TAG {tag_name} 时发生异常 (尝试 {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                else:
                    return False
        
        return False
    
    def create_dynamic_tags(self, entities_df: pd.DataFrame) -> bool:
        """根据实体类型动态创建TAG"""
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
            return True
            
        except Exception as e:
            self.logger.error(f"创建动态TAG失败: {e}")
            return False
    
    def create_schema(self) -> bool:
        """创建Nebula Graph的schema"""
        try:
            self.logger.info("开始创建schema...")
            
            # 先检查schema是否已存在
            tag_exists, edge_exists = self.check_schema_exists()
            
            # 创建通用关系EDGE - 添加source和target原文字属性
            if not edge_exists:
                create_relation_edge_query = """
                CREATE EDGE IF NOT EXISTS related(
                  source string,
                  target string,
                  description string
                )
                """
                
                if not self.execute_query(create_relation_edge_query):
                    self.logger.error("创建related EDGE失败")
                    return False
                
                self.logger.info("related EDGE创建成功")
            else:
                self.logger.info("related EDGE已存在，跳过创建")
            
            # 等待schema生效
            self.logger.info("等待schema生效...")
            time.sleep(5)  # 等待时间
            
            self.logger.info("Schema创建并生效成功")
            return True
            
        except Exception as e:
            self.logger.error(f"创建schema失败: {e}")
            return False
    
    def escape_string(self, value: Any) -> str:
        """转义字符串中的特殊字符并处理编码问题"""
        if pd.isna(value) or value is None:
            return ""
        
        try:
            # 确保是字符串类型
            if not isinstance(value, str):
                str_value = str(value)
            else:
                str_value = value
                
            # 处理可能的编码问题
            # 如果是bytes类型，尝试解码
            if isinstance(str_value, bytes):
                try:
                    str_value = str_value.decode('utf-8')
                except UnicodeDecodeError:
                    try:
                        # 尝试其他编码
                        str_value = str_value.decode('latin-1')
                    except:
                        # 如果都失败，使用忽略错误的方式解码
                        str_value = str_value.decode('utf-8', errors='ignore')
            
            # 转义特殊字符
            str_value = str_value.replace('\\', '\\\\')
            str_value = str_value.replace('"', '\\"')
            str_value = str_value.replace('\n', '\\n')
            str_value = str_value.replace('\r', '\\r')
            str_value = str_value.replace('\t', '\\t')
            
            return str_value
        except Exception as e:
            self.logger.warning(f"转义字符串时出错: {e}, 返回空字符串")
            return ""
    
    def import_entities(self, entities_df: pd.DataFrame) -> bool:
        """
        导入实体数据，使用title的MD5作为ID，按实体类型分组处理
        
        Args:
            entities_df: 实体数据DataFrame
            
        Returns:
            bool: 导入是否成功
        """
        try:
            batch_size = self.config['import']['batch_size']
            total_entities = len(entities_df)
            
            self.logger.info(f"开始导入 {total_entities} 个实体，批次大小: {batch_size}")
            
            # 预处理实体数据，生成MD5 ID并跳过没有类型的实体
            valid_entities = []
            
            for _, row in entities_df.iterrows():
                self.stats['entities_processed'] += 1
                
                # 跳过没有类型的实体
                if pd.isna(row.get('type')) or row.get('type') == '':
                    self.stats['skipped_entities'] += 1
                    continue
                
                title = row.get('title', '')
                if pd.isna(title) or title == '':
                    self.stats['skipped_entities'] += 1
                    continue
                
                # 生成MD5 ID
                md5_id = self.generate_md5(title)
                
                # 保存ID映射
                self.entity_id_map[title] = md5_id
                
                # 添加到有效实体列表
                valid_entities.append({
                    'md5_id': md5_id,
                    'title': title,
                    'type': row.get('type', ''),
                    'description': row.get('description', '')
                })
                
                # 更新统计信息
                entity_type = row.get('type', '')
                if not pd.isna(entity_type) and entity_type != '':
                    self.stats['entity_types'][entity_type] += 1
            
            self.logger.info(f"有效实体数量: {len(valid_entities)}")
            self.logger.info(f"跳过的实体数量: {self.stats['skipped_entities']}")
            
            # 按实体类型分组处理
            entity_by_type = {}
            for entity in valid_entities:
                entity_type = entity['type']
                if entity_type not in entity_by_type:
                    entity_by_type[entity_type] = []
                entity_by_type[entity_type].append(entity)
            
            # 分批导入有效实体，按类型处理
            total_batch_success = 0
            total_batch_fail = 0
            
            for entity_type, entities in entity_by_type.items():
                # 清理实体类型名称，确保符合Nebula Graph命名规范
                clean_type = str(entity_type).upper().replace(' ', '_').replace('-', '_')
                clean_type = ''.join(c for c in clean_type if c.isalnum() or c == '_')
                
                if not clean_type:
                    self.logger.warning(f"跳过无效的实体类型: {entity_type}")
                    continue
                
                self.logger.info(f"开始导入实体类型 {clean_type}: {len(entities)} 个实体")
                
                batch_success_count = 0
                batch_fail_count = 0
                
                for i in tqdm(range(0, len(entities), batch_size),
                             desc=f"导入 {clean_type}",
                             disable=not self.config['import']['enable_progress_bar']):
                    
                    batch = entities[i:i + batch_size]
                    
                    # 构建批量INSERT语句
                    insert_values = []
                    
                    for entity in batch:
                        try:
                            values = (
                                f'"{self.escape_string(entity["title"])}"',
                                f'"{self.escape_string(entity["type"])}"',
                                f'"{self.escape_string(entity["description"])}"'
                            )
                            
                            insert_values.append(f'"{entity["md5_id"]}": ({", ".join(values)})')
                            
                        except Exception as e:
                            self.logger.warning(f"处理实体数据时出错: {e}, 跳过该实体")
                            continue
                    
                    if insert_values:
                        insert_query = f"""
                        INSERT VERTEX {clean_type}(title, type, description)
                        VALUES {", ".join(insert_values)}
                        """
                        
                        if self.execute_query(insert_query):
                            self.stats['entities_imported'] += len(insert_values)
                            batch_success_count += 1
                            self.logger.debug(f"成功导入批次 {i//batch_size + 1}，包含 {len(insert_values)} 个实体")
                        else:
                            batch_fail_count += 1
                            self.logger.error(f"批次 {i//batch_size + 1} 导入失败")
                
                total_batch_success += batch_success_count
                total_batch_fail += batch_fail_count
                self.logger.info(f"实体类型 {clean_type} 导入完成: 成功批次 {batch_success_count}, 失败批次 {batch_fail_count}")
            
            self.logger.info(f"实体导入完成: 成功批次 {total_batch_success}, 失败批次 {total_batch_fail}")
            self.logger.info(f"实体导入统计: 处理 {self.stats['entities_processed']} 个，成功导入 {self.stats['entities_imported']} 个，跳过 {self.stats['skipped_entities']} 个")
            return True
            
        except Exception as e:
            self.logger.error(f"导入实体数据失败: {e}")
            return False
    
    def import_relationships(self, relationships_df: pd.DataFrame) -> bool:
        """
        导入关系数据，使用实体title的MD5作为VID
        
        Args:
            relationships_df: 关系数据DataFrame
            
        Returns:
            bool: 导入是否成功
        """
        try:
            batch_size = self.config['import']['batch_size']
            total_relationships = len(relationships_df)
            
            self.logger.info(f"开始导入 {total_relationships} 个关系，批次大小: {batch_size}")
            
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
                        self.stats['relationships_processed'] += 1
                        
                        source_title = row.get('source', '')
                        target_title = row.get('target', '')
                        
                        # 跳过无效的关系
                        if pd.isna(source_title) or source_title == '' or pd.isna(target_title) or target_title == '':
                            skipped_count += 1
                            continue
                        
                        # 获取源和目标实体的MD5 ID
                        source_id = self.entity_id_map.get(source_title)
                        target_id = self.entity_id_map.get(target_title)
                        
                        # 如果源或目标实体不存在，跳过该关系
                        if not source_id or not target_id:
                            skipped_count += 1
                            continue
                        
                        # 安全处理source, target和description
                        source_title = row.get("source", "")
                        target_title = row.get("target", "")
                        description = row.get("description", "")
                        
                        try:
                            # 确保所有字段是有效的UTF-8字符串
                            # 处理source
                            if isinstance(source_title, bytes):
                                source_title = source_title.decode('utf-8', errors='ignore')
                            elif isinstance(source_title, str):
                                source_title.encode('utf-8').decode('utf-8')
                            else:
                                source_title = str(source_title)
                                
                            # 处理target
                            if isinstance(target_title, bytes):
                                target_title = target_title.decode('utf-8', errors='ignore')
                            elif isinstance(target_title, str):
                                target_title.encode('utf-8').decode('utf-8')
                            else:
                                target_title = str(target_title)
                                
                            # 处理description
                            if isinstance(description, bytes):
                                description = description.decode('utf-8', errors='ignore')
                            elif isinstance(description, str):
                                description.encode('utf-8').decode('utf-8')
                            else:
                                description = str(description)
                                
                            # 转义处理
                            escaped_source = self.escape_string(source_title)
                            escaped_target = self.escape_string(target_title)
                            escaped_description = self.escape_string(description)
                            
                            values = (
                                f'"{escaped_source}"',
                                f'"{escaped_target}"',
                                f'"{escaped_description}"'
                            )
                        except Exception as e:
                            self.logger.warning(f"处理属性时出错: {e}, 使用空字符串")
                            values = ('""', '""', '""')
                        
                        insert_values.append(f'"{source_id}" -> "{target_id}": ({", ".join(values)})')
                        
                    except Exception as e:
                        skipped_count += 1
                        self.logger.warning(f"处理关系数据时出错: {e}, 跳过该关系")
                        continue
                
                if skipped_count > 0:
                    self.logger.warning(f"批次 {i//batch_size + 1} 跳过了 {skipped_count} 个无效关系")
                
                if insert_values:
                    # 构建查询并记录
                    insert_query = f"""
                    INSERT EDGE related(source, target, description)
                    VALUES {", ".join(insert_values)}
                    """
                    
                    # 记录部分查询用于调试
                    debug_query = insert_query
                    if len(debug_query) > 200:
                        debug_query = debug_query[:200] + "... [截断]"
                    self.logger.debug(f"执行查询: {debug_query}")
                    
                    if self.execute_query(insert_query):
                        self.stats['relationships_imported'] += len(insert_values)
                        batch_success_count += 1
                        self.logger.debug(f"成功导入关系批次 {i//batch_size + 1}，包含 {len(insert_values)} 个关系")
                    else:
                        batch_fail_count += 1
                        self.logger.error(f"关系批次 {i//batch_size + 1} 导入失败")
                else:
                    self.logger.warning(f"批次 {i//batch_size + 1} 没有有效的关系数据")
            
            self.logger.info(f"关系导入完成: 成功批次 {batch_success_count}, 失败批次 {batch_fail_count}")
            self.logger.info(f"关系导入统计: 处理 {self.stats['relationships_processed']} 个，成功导入 {self.stats['relationships_imported']} 个")
            return True
            
        except Exception as e:
            self.logger.error(f"导入关系数据失败: {e}")
            return False
    
    def print_statistics(self):
        """打印导入统计信息"""
        print("\n" + "="*60)
        print("GraphRAG数据导入统计 (MD5版本)")
        print("="*60)
        print(f"实体处理数量: {self.stats['entities_processed']}")
        print(f"实体导入数量: {self.stats['entities_imported']}")
        print(f"实体跳过数量: {self.stats['skipped_entities']}")
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
            self.logger.info("开始GraphRAG数据导入流程 (MD5版本)")
            
            # 1. 连接数据库
            if not self.connect_nebula():
                return False
            
            # 2. 加载数据
            entities_df, relationships_df = self.load_csv_data()
            if entities_df is None or relationships_df is None:
                return False
            
            # 3. 创建动态TAG
            if not self.create_dynamic_tags(entities_df):
                return False
            
            # 4. 创建关系EDGE
            if not self.create_schema():
                return False
            
            # 5. 导入实体数据
            if not self.import_entities(entities_df):
                return False
            
            # 5. 导入关系数据
            if not self.import_relationships(relationships_df):
                return False
            
            # 6. 打印统计信息
            self.print_statistics()
            
            self.logger.info("GraphRAG数据导入完成 (MD5版本)")
            return True
            
        except Exception as e:
            self.logger.error(f"导入流程失败: {e}")
            return False
        
        finally:
            self.disconnect_nebula()


def main():
    """主函数"""
    print("GraphRAG数据导入Nebula Graph工具 (MD5版本)")
    print("="*50)
    
    # 使用固定路径
    config_file = "/Users/bytedance/Desktop/github/graphrag/ragtest/nebula_config.yaml"
    if not os.path.exists(config_file):
        print(f"错误: 配置文件 {config_file} 不存在")
        print("请确保配置文件存在并包含正确的Nebula Graph连接信息")
        sys.exit(1)
    
    # 创建导入器并运行
    importer = NebulaGraphImporterMD5(config_file)
    
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