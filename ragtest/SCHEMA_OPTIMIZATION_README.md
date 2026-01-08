# GraphRAG Nebula导入脚本Schema优化说明

## 优化概述

针对用户遇到的"No schema found for COMPANY"错误，我们对`import_graphrag_to_nebula.py`脚本进行了全面优化，解决了Nebula Graph schema创建和生效的时序问题。

## 主要优化内容

### 1. 增强的Schema等待机制

**原有问题**: 简单的`time.sleep(2)`无法保证schema真正生效
**优化方案**: 实现智能等待机制

```python
def wait_for_schema_ready(self, schema_name: str, schema_type: str = "TAG", max_wait_time: int = 60) -> bool:
    """
    等待schema生效并验证其可用性
    - 动态调整等待间隔（1秒到5秒）
    - 持续验证schema状态
    - 最大等待时间可配置
    """
```

**特点**:
- 动态等待间隔：从1秒开始，逐渐增加到5秒
- 实时验证：每次等待后都验证schema是否真正可用
- 可配置超时：默认60秒，可通过配置文件调整

### 2. Schema验证函数

**新增功能**: 验证TAG和EDGE是否真正存在且可用

```python
def verify_schema_exists(self, schema_name: str, schema_type: str = "TAG") -> bool:
    """
    验证schema是否存在且可用
    - 执行SHOW TAGS/SHOW EDGES查询
    - 解析结果确认schema存在
    - 提供详细的调试日志
    """
```

**验证方式**:
- TAG验证：执行`SHOW TAGS`查询，检查结果中是否包含目标TAG
- EDGE验证：执行`SHOW EDGES`查询，检查结果中是否包含目标EDGE
- 错误处理：捕获查询异常，提供详细错误信息

### 3. 带验证的Schema创建

**原有问题**: 创建schema后未验证是否真正生效
**优化方案**: 创建后立即验证，失败则重试

```python
def create_tag_with_verification(self, tag_name: str, entity_count: int) -> bool:
    """
    创建TAG并验证其可用性
    - 执行CREATE TAG命令
    - 等待并验证TAG生效
    - 失败时自动重试
    """
```

**重试机制**:
- 最大重试次数：默认3次，可配置
- 重试间隔：失败后等待2-5秒再重试
- 详细日志：记录每次尝试的结果

### 4. 增强的错误处理和日志

**日志改进**:
- 添加详细的schema创建状态日志
- 区分不同类型的错误（网络、权限、schema等）
- 提供具体的错误解决建议

**错误分类处理**:
- Schema错误：检测"No schema found"等错误，自动重新验证schema
- 网络错误：重试机制处理临时连接问题
- 数据错误：跳过有问题的数据，继续处理其他数据

### 5. 带Schema检查的查询执行

**新增功能**: 查询失败时自动检查schema状态

```python
def execute_query_with_schema_check(self, query: str, schema_name: str, schema_type: str = "TAG") -> bool:
    """
    执行查询并在失败时检查schema状态
    - 检测schema相关错误
    - 自动重新验证schema
    - 智能重试机制
    """
```

## 配置文件更新

在`nebula_config.yaml`中添加了新的配置项：

```yaml
import:
  # 原有配置...
  
  # Schema创建和验证配置
  schema_creation_retries: 3        # schema创建最大重试次数
  schema_wait_timeout: 30           # 等待schema生效的最大时间（秒）
  schema_verification_interval: 1   # schema验证查询间隔（秒）
```

## 使用方法

### 1. 正常导入流程

```bash
# 确保配置文件正确
python import_graphrag_to_nebula.py
```

### 2. 测试Schema验证功能

```bash
# 运行schema验证测试
python test_schema_verification.py
```

### 3. 调试模式

修改配置文件中的日志级别：

```yaml
logging:
  level: "DEBUG"  # 显示详细的schema验证日志
```

## 优化效果

### 解决的问题

1. **"No schema found"错误**: 通过schema验证和等待机制彻底解决
2. **时序问题**: 确保schema创建后真正可用再进行数据导入
3. **错误诊断**: 提供详细的日志帮助定位问题
4. **稳定性**: 增强的重试机制提高导入成功率

### 性能影响

- **轻微延迟**: schema验证增加了少量时间开销（通常1-5秒）
- **更高成功率**: 避免因schema未生效导致的导入失败
- **更好的用户体验**: 详细的进度和状态信息

## 故障排除

### 常见问题

1. **Schema创建超时**
   - 检查Nebula Graph服务状态
   - 增加`schema_wait_timeout`配置值
   - 查看Nebula Graph日志

2. **验证失败但schema存在**
   - 检查用户权限
   - 验证空间配置
   - 重启Nebula Graph服务

3. **频繁重试**
   - 检查网络连接稳定性
   - 调整重试次数和间隔
   - 查看详细错误日志

### 调试步骤

1. 启用DEBUG日志级别
2. 运行测试脚本验证基本功能
3. 检查Nebula Graph服务状态
4. 验证配置文件正确性
5. 查看详细错误日志

## 总结

通过这些优化，脚本现在能够：

- ✅ 可靠地创建和验证schema
- ✅ 智能等待schema生效
- ✅ 自动处理时序问题
- ✅ 提供详细的诊断信息
- ✅ 在出现问题时自动重试

这些改进确保了GraphRAG数据能够稳定、可靠地导入到Nebula Graph中，避免了"No schema found"等常见错误。