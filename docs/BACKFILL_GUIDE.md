# 数据回填指南

## 概述

数据回填服务用于从币安API获取历史K线数据，填补数据库中的缺失数据。新版本支持从当前时间往前推5天的完整数据回填。

## 配置

### 回填配置 (config/config.yaml)

```yaml
# 数据回填配置
backfill:
  enabled: true
  days_to_backfill: 5         # 回填天数：从当前时间往前推5天
  batch_size: 1000            # 每次API请求的最大记录数
  request_interval: "100ms"   # 请求间隔，避免触发币安限制
  symbol_interval: "1s"       # 不同交易对之间的间隔
  max_concurrent_symbols: 1   # 同时处理的交易对数量（设为1避免限制）
  retry_attempts: 3           # 重试次数
  retry_delay: "5s"           # 重试延迟
```

## API端点

### 1. 获取回填状态
```bash
GET /api/v1/backfill/status
```

### 2. 获取回填进度
```bash
GET /api/v1/backfill/progress
```

### 3. 范围回填单个交易对
```bash
POST /api/v1/backfill/symbol/{symbol}
```

参数：
- `start_time` (可选): 开始时间 (ISO 8601格式)
- `end_time` (可选): 结束时间 (ISO 8601格式)

### 4. 完整回填单个交易对 (5天)
```bash
POST /api/v1/backfill/symbol/{symbol}/complete
```

### 5. 范围回填所有交易对
```bash
POST /api/v1/backfill/all
```

参数：
- `start_time` (可选): 开始时间 (ISO 8601格式)
- `end_time` (可选): 结束时间 (ISO 8601格式)

### 6. 完整回填所有交易对 (5天)
```bash
POST /api/v1/backfill/all/complete
```

## 使用示例

### 测试单个交易对的完整回填

```bash
# 测试AAVEUSDT的5天完整回填
./test_single_backfill.sh AAVEUSDT

# 或者直接调用API
curl -X POST http://localhost:8080/api/v1/backfill/symbol/AAVEUSDT/complete
```

### 测试所有交易对的完整回填

```bash
# 运行完整测试脚本
./test_backfill_complete.sh

# 或者直接调用API
curl -X POST http://localhost:8080/api/v1/backfill/all/complete
```

### 检查回填进度

```bash
# 获取回填状态
curl http://localhost:8080/api/v1/backfill/status | jq '.'

# 获取回填进度
curl http://localhost:8080/api/v1/backfill/progress | jq '.'
```

## 币安API限制处理

为了避免触发币安API限制，系统实现了以下策略：

1. **请求间隔**: 每次API请求之间等待100ms
2. **交易对间隔**: 不同交易对之间等待1秒
3. **并发限制**: 同时只处理1个交易对
4. **批量大小**: 每次请求最多1000条记录
5. **重试机制**: 失败时自动重试3次

## 数据完整性

### 5天完整回填

- 从当前时间往前推5天
- 每个交易对获取完整的1分钟K线数据
- 自动处理时间范围和数据分页
- 确保数据的连续性和完整性

### 数据验证

回填完成后，系统会自动验证：
- 数据连续性
- 时间戳准确性
- 数据完整性

## 监控和日志

### 日志级别

- `INFO`: 回填开始、完成、进度更新
- `DEBUG`: 详细的API请求和响应
- `WARNING`: 重试、部分失败
- `ERROR`: 完全失败、API错误

### 进度监控

```bash
# 实时监控回填进度
watch -n 2 'curl -s http://localhost:8080/api/v1/backfill/progress | jq "."'
```

## 故障排除

### 常见问题

1. **API限制错误**
   - 增加 `request_interval` 和 `symbol_interval`
   - 减少 `batch_size`

2. **网络连接问题**
   - 检查代理设置
   - 验证网络连接

3. **数据库连接问题**
   - 检查ClickHouse服务状态
   - 验证数据库配置

### 调试命令

```bash
# 检查服务状态
curl http://localhost:8080/health

# 检查数据库连接
curl http://localhost:8080/api/v1/stats

# 查看日志
tail -f logs/collector.log
```

## 性能优化

### 配置建议

- **高并发环境**: 增加 `request_interval` 到 "200ms"
- **低延迟网络**: 减少 `symbol_interval` 到 "500ms"
- **大量数据**: 增加 `batch_size` 到 1000

### 监控指标

- 回填速度 (记录/秒)
- 成功率
- API错误率
- 数据库写入性能

## 注意事项

1. **币安API限制**: 严格遵守币安的API使用限制
2. **数据量**: 5天完整回填会产生大量数据，确保有足够存储空间
3. **时间**: 完整回填所有403个交易对可能需要数小时
4. **网络**: 确保网络连接稳定，建议使用代理
5. **存储**: 确保ClickHouse有足够的存储空间和性能 