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
from typing import Dict, List, Tuple, Optional, Any
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
        try:
            # 创建连接池配置
            config = Config()
            pool_config = self.config['nebula']['connection_pool']
            config.max_connection_pool_size = pool_config.get('max_size', 10)
            config.timeout = pool_config.get('timeout', 30) * 1000  # 转换为毫秒
            config.idle_time = pool_config.get('idle_time', 3600) * 1000
            config.interval_check = pool_config.get('interval_check', -1)
            
            # 创建连接池
            self.connection_pool = ConnectionPool()
            hosts = [(host['host'], host['port']) for host in self.config['nebula']['hosts']]
            
            if not self.connection_pool.init(hosts, config):
                raise Exception("连接池初始化失败")
            
            # 获取会话
            self.session = self.connection_pool.get_session(
                self.config['nebula']['username'],
                self.config['nebula']['password']
            )
            
            if self.session is None:
                raise Exception("获取会话失败")
            
            # 使用指定的空间
            space_name = self.config['nebula']['space_name']
            result = self.session.execute(f"USE {space_name}")
            
            if not result.is_succeeded():
                raise Exception(f"使用空间 {space_name} 失败: {result.error_msg()}")
            
            self.logger.info(f"成功连接到Nebula Graph，使用空间: {space_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"连接Nebula Graph失败: {e}")
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
    
    def create_dynamic_tags(self, entities_df: pd.DataFrame) -> bool:
        """根据实体类型动态创建TAG"""
        try:
            # 获取所有唯一的实体类型
            entity_types = entities_df['type'].unique()
            self.logger.info(f"发现实体类型: {list(entity_types)}")
            
            for entity_type in entity_types:
                if pd.isna(entity_type) or entity_type == '':
                    continue
                
                # 清理实体类型名称，确保符合Nebula Graph命名规范
                clean_type = str(entity_type).upper().replace(' ', '_').replace('-', '_')
                clean_type = ''.join(c for c in clean_type if c.isalnum() or c == '_')
                
                if not clean_type:
                    continue
                
                # 创建TAG的DDL语句
                create_tag_query = f"""
                CREATE TAG IF NOT EXISTS {clean_type}(
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
                
                if self.execute_query(create_tag_query):
                    self.logger.info(f"成功创建TAG: {clean_type}")
                    self.stats['entity_types'][clean_type] = len(entities_df[entities_df['type'] == entity_type])
                else:
                    self.logger.error(f"创建TAG失败: {clean_type}")
                    return False
            
            return True
            
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
        """导入实体数据"""
        try:
            batch_size = self.config['import']['batch_size']
            total_entities = len(entities_df)
            
            self.logger.info(f"开始导入 {total_entities} 个实体，批次大小: {batch_size}")
            
            # 按实体类型分组处理
            for entity_type, group_df in entities_df.groupby('type'):
                if pd.isna(entity_type) or entity_type == '':
                    continue
                
                clean_type = str(entity_type).upper().replace(' ', '_').replace('-', '_')
                clean_type = ''.join(c for c in clean_type if c.isalnum() or c == '_')
                
                if not clean_type:
                    continue
                
                self.logger.info(f"导入实体类型 {clean_type}: {len(group_df)} 个实体")
                
                # 分批处理
                for i in tqdm(range(0, len(group_df), batch_size), 
                             desc=f"导入 {clean_type}",
                             disable=not self.config['import']['enable_progress_bar']):
                    
                    batch_df = group_df.iloc[i:i + batch_size]
                    
                    # 构建批量INSERT语句
                    insert_values = []
                    for _, row in batch_df.iterrows():
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
                        insert_values.append(f'"{vertex_id}": ({", ".join(values)})')
                    
                    if insert_values:
                        insert_query = f"""
                        INSERT VERTEX {clean_type}(id, human_readable_id, title, description, text_unit_ids, frequency, degree, x, y) 
                        VALUES {", ".join(insert_values)}
                        """
                        
                        if self.execute_query(insert_query):
                            self.stats['entities_imported'] += len(batch_df)
                        
                        self.stats['entities_processed'] += len(batch_df)
            
            self.logger.info(f"实体导入完成: 处理 {self.stats['entities_processed']} 个，成功导入 {self.stats['entities_imported']} 个")
            return True
            
        except Exception as e:
            self.logger.error(f"导入实体数据失败: {e}")
            return False
    
    def import_relationships(self, relationships_df: pd.DataFrame) -> bool:
        """导入关系数据"""
        try:
            batch_size = self.config['import']['batch_size']
            total_relationships = len(relationships_df)
            
            self.logger.info(f"开始导入 {total_relationships} 个关系，批次大小: {batch_size}")
            
            # 分批处理关系数据
            for i in tqdm(range(0, total_relationships, batch_size),
                         desc="导入关系",
                         disable=not self.config['import']['enable_progress_bar']):
                
                batch_df = relationships_df.iloc[i:i + batch_size]
                
                # 构建批量INSERT语句
                insert_values = []
                for _, row in batch_df.iterrows():
                    source_id = self.escape_string(row.get('source', ''))
                    target_id = self.escape_string(row.get('target', ''))
                    
                    if not source_id or not target_id:
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
                
                if insert_values:
                    insert_query = f"""
                    INSERT EDGE RELATED(id, human_readable_id, description, weight, combined_degree, text_unit_ids) 
                    VALUES {", ".join(insert_values)}
                    """
                    
                    if self.execute_query(insert_query):
                        self.stats['relationships_imported'] += len(insert_values)
                    
                    self.stats['relationships_processed'] += len(batch_df)
            
            self.logger.info(f"关系导入完成: 处理 {self.stats['relationships_processed']} 个，成功导入 {self.stats['relationships_imported']} 个")
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
            
            # 验证实体数量
            for tag_name, expected_count in self.stats['entity_types'].items():
                count_query = f"MATCH (v:{tag_name}) RETURN count(v) AS count"
                result = self.session.execute(count_query)
                
                if result.is_succeeded():
                    actual_count = result.row_values(0)[0].as_int()
                    self.logger.info(f"TAG {tag_name}: 期望 {expected_count}，实际 {actual_count}")
                    
                    if actual_count != expected_count:
                        self.logger.warning(f"TAG {tag_name} 数量不匹配")
                else:
                    self.logger.error(f"验证TAG {tag_name} 失败: {result.error_msg()}")
            
            # 验证关系数量
            count_query = "MATCH ()-[e:RELATED]->() RETURN count(e) AS count"
            result = self.session.execute(count_query)
            
            if result.is_succeeded():
                actual_count = result.row_values(0)[0].as_int()
                expected_count = self.stats['relationships_imported']
                self.logger.info(f"关系 RELATED: 期望 {expected_count}，实际 {actual_count}")
                
                if actual_count != expected_count:
                    self.logger.warning("关系数量不匹配")
            else:
                self.logger.error(f"验证关系失败: {result.error_msg()}")
            
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