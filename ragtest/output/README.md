# GraphRAG 数据导入 Nebula Graph 脚本

这个脚本用于将 GraphRAG 生成的实体和关系数据从 Parquet 文件导入到 Nebula Graph 图数据库中。

## 功能特性

- 自动创建 Nebula Graph schema（标签和边类型）
- 批量导入实体和关系数据
- 支持数据类型转换和特殊字符处理
- 提供详细的日志记录
- 导入完成后显示统计信息

## 前置要求

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 启动 Nebula Graph

确保 Nebula Graph 服务正在运行，默认配置：
- Graph 服务：127.0.0.1:9669
- 用户名：root
- 密码：nebula

### 3. 创建图空间

在 Nebula Graph 中创建一个图空间用于存储数据：

```ngql
# 连接到 Nebula Graph
nebula> CREATE SPACE IF NOT EXISTS graphrag(partition_num=10, replica_factor=1);
nebula> USE graphrag;
```

## 数据结构

### 实体表结构 (entities.parquet)
- `id`: 实体唯一标识符
- `human_readable_id`: 人类可读的ID
- `title`: 实体标题
- `type`: 实体类型
- `description`: 实体描述
- `frequency`: 频率
- `degree`: 度数
- `x`, `y`: 坐标位置
- `text_unit_ids`: 关联的文本单元ID

### 关系表结构 (relationships.parquet)
- `id`: 关系唯一标识符
- `source`: 源实体
- `target`: 目标实体
- `description`: 关系描述
- `weight`: 关系权重
- `combined_degree`: 组合度数
- `text_unit_ids`: 关联的文本单元ID

## 配置文件

修改 `nebula_config.py` 中的配置：

```python
NEBULA_CONFIG = {
    'hosts': [('127.0.0.1', 9669)],  # Nebula Graph地址
    'username': 'root',               # 用户名
    'password': 'nebula',             # 密码
    'space_name': 'graphrag'          # 图空间名称
}
```

## 使用方法

### 基本使用

```bash
python import_to_nebula.py
```

### 查看导入结果

导入完成后，可以在 Nebula Graph 中查询数据：

```ngql
# 查看实体总数
MATCH (v:entity) RETURN COUNT(v);

# 查看关系总数
MATCH ()-[e:relationship]->() RETURN COUNT(e);

# 查看实体类型分布
MATCH (v:entity) RETURN v.type, COUNT(v) GROUP BY v.type;

# 查看具体实体
MATCH (v:entity) RETURN v.title, v.type, v.description LIMIT 10;

# 查看关系
MATCH (s:entity)-[r:relationship]->(t:entity)
RETURN s.title, r.description, t.title LIMIT 10;
```

## 生成的 Schema

### 标签 (Tags)
- `entity`: 实体标签，包含所有实体属性

### 边类型 (Edge Types)
- `relationship`: 关系边，包含关系属性

### 索引
- `entity_title_index`: 基于实体标题的索引
- `entity_type_index`: 基于实体类型的索引

## 故障排除

### 常见问题

1. **连接失败**
   - 检查 Nebula Graph 服务是否运行
   - 验证主机名和端口配置
   - 确认用户名和密码正确

2. **图空间不存在**
   - 使用 `CREATE SPACE` 命令创建图空间
   - 确保有足够的权限

3. **导入失败**
   - 检查 Parquet 文件是否存在且可读
   - 验证数据格式是否正确
   - 查看详细错误日志

4. **内存不足**
   - 减少批量导入大小（修改 `batch_size`）
   - 增加 Nebula Graph 内存配置

### 日志级别

脚本使用 Python logging 模块，默认级别为 INFO。可以修改日志级别获取更详细信息：

```python
logging.basicConfig(level=logging.DEBUG)
```

## 性能优化

1. **批量大小调整**
   - 根据系统性能调整 `batch_size` 参数
   - 较大的批量可能导致内存问题
   - 较小的批量会增加网络开销

2. **并发导入**
   - 可以考虑并行导入实体和关系
   - 注意避免依赖冲突

3. **索引策略**
   - 在大量数据导入前禁用索引
   - 导入完成后重建索引

## 扩展功能

脚本可以根据需要进行扩展：

1. **增量导入**：支持增量更新已有数据
2. **数据验证**：添加数据完整性检查
3. **备份恢复**：支持数据备份和恢复
4. **监控告警**：添加导入过程监控

## 注意事项

- 确保 Parquet 文件格式正确
- 大数据量导入可能需要较长时间
- 建议在测试环境先验证脚本
- 定期备份重要数据