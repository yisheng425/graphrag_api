#!/usr/bin/env python3
"""
Nebula Graph配置文件
"""

# Nebula Graph连接配置
NEBULA_CONFIG = {
    'hosts': [('127.0.0.1', 9669)],  # Nebula Graph服务器地址和端口
    'username': 'root',               # 用户名
    'password': 'nebula',             # 密码
    'space_name': 'schema_incident'          # 图空间名称（需要预先创建）
}

# 数据文件路径配置
DATA_CONFIG = {
    'entities_file': './entities.parquet',        # 实体数据文件路径
    'relationships_file': './relationships.parquet'  # 关系数据文件路径
}

# 导入配置
IMPORT_CONFIG = {
    'batch_size': 100,                # 批量导入大小
    'enable_index': True,             # 是否创建索引
    'clear_existing_data': False      # 是否清理已有数据
}