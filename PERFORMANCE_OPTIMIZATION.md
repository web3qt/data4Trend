# Data4Trend 性能优化指南

## 问题分析

根据日志分析，你的数据写入速度慢的主要原因是：

1. **WebSocket连接大量断开** - 导致数据接收中断
2. **批量写入配置不够优化** - 100条记录一批，10秒超时太长
3. **错误率高达20%** - 系统运行6分钟，处理2015条消息，错误403条
4. **网络连接不稳定** - 可能由于代理或网络问题

## 优化措施

### 1. 批量写入优化

**修改前：**
```yaml
batch_writer:
  batch_size: 100
  batch_timeout: "10s"
  retry_interval: "5s"
```

**修改后：**
```yaml
batch_writer:
  batch_size: 50           # 减少批量大小，更快触发写入
  batch_timeout: "2s"      # 大幅减少超时时间
  retry_interval: "1s"     # 减少重试间隔
```

### 2. Kafka生产者优化

**修改前：**
```yaml
producer:
  batch_size: 100
  batch_timeout: "1s"
  channel_buffer_size: 2048
  flush_bytes: 16384
  send_timeout: "5s"
```

**修改后：**
```yaml
producer:
  batch_size: 50           # 减少批量大小，更快触发
  batch_timeout: "500ms"   # 大幅减少超时时间
  channel_buffer_size: 4096    # 增加通道缓冲区大小
  flush_bytes: 8192            # 减少批量刷新大小
  send_timeout: "2s"           # 减少发送超时时间
```

### 3. 代码优化

- 添加了详细的性能监控日志
- 优化了错误处理和重试机制
- 增加了写入时间统计
- 改进了批量插入的性能监控

## 使用方法

### 1. 启动优化版本

```bash
./start_optimized.sh
```

这个脚本会：
- 检查依赖服务状态
- 使用优化配置启动收集器
- 启动性能监控
- 显示实时日志

### 2. 监控性能

```bash
./monitor_performance.sh
```

实时监控：
- 总记录数
- 写入速率
- 唯一交易对数量
- 最新记录时间

### 3. 诊断WebSocket问题

```bash
./diagnose_websocket.sh
```

检查：
- 网络连接状态
- 代理配置
- 系统资源使用
- 端口占用情况

## 预期改进

优化后你应该看到：

1. **更快的写入速度** - 每2秒一批，而不是10秒
2. **更小的批量大小** - 50条记录一批，更快触发写入
3. **更低的错误率** - 减少重试间隔，更快恢复
4. **更好的监控** - 实时性能指标

## 故障排除

### 如果WebSocket连接仍然断开：

1. 检查网络连接：
   ```bash
   ./diagnose_websocket.sh
   ```

2. 减少交易对数量：
   ```yaml
   # 在config.yaml中临时减少symbols
   symbols:
     - "BTCUSDT"
     - "ETHUSDT"
     - "BNBUSDT"
   ```

3. 检查代理设置：
   ```yaml
   proxy:
     enabled: false  # 临时禁用代理测试
   ```

### 如果写入仍然很慢：

1. 检查ClickHouse性能：
   ```sql
   SELECT count() FROM data4trend.klines_1m;
   ```

2. 检查系统资源：
   ```bash
   top -l 1
   ```

3. 查看详细日志：
   ```bash
   tail -f logs/collector.log
   ```

## 性能基准

优化后的预期性能：

- **写入速率**: 400+ 记录/分钟
- **批量大小**: 50 记录/批
- **批量间隔**: 2 秒
- **错误率**: < 5%
- **活跃交易对**: 400+ (启用auto_fetch_symbols)

## 监控指标

关键监控指标：

1. **写入速率** - 每分钟写入的记录数
2. **批量频率** - 每批写入的时间间隔
3. **错误率** - 失败的消息比例
4. **连接状态** - WebSocket连接数量
5. **系统资源** - CPU、内存、网络使用情况

通过这些优化，你的数据写入速度应该显著提升。