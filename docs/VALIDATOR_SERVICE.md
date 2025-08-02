# 数据校验与回补服务 (Validator & Backfiller)

## 概述

数据校验与回补服务是一个独立的后台服务，专门用于检测和修复K线数据中的断层问题。该服务定期扫描ClickHouse数据库，识别数据缺口，并通过调用币安REST API自动回补缺失的历史数据。

## 核心功能

### 1. 数据连续性检测
- **定期扫描**: 按配置的时间间隔（默认5分钟）自动检查数据完整性
- **间隙识别**: 检测相邻K线数据之间的时间戳间隔，识别缺失的数据点
- **多交易对支持**: 同时检查数据库中所有交易对的数据完整性
- **历史数据验证**: 可配置检查历史数据的时间范围（默认7天）

### 2. 自动数据回补
- **REST API调用**: 使用币安官方REST API获取缺失的历史K线数据
- **精确时间范围**: 根据检测到的数据缺口，精确指定startTime和endTime
- **批量处理**: 支持批量获取和插入数据，提高效率
- **并发处理**: 支持多线程并发处理多个交易对的回补任务

### 3. 数据去重处理
- **幂等性保证**: 确保重复运行不会产生重复数据
- **智能合并**: 利用ClickHouse的ReplacingMergeTree引擎特性处理重复数据
- **边界处理**: 智能处理数据缺口边界的重复数据点

## 技术架构

### 服务组件

```
┌─────────────────────────────────────────────────────────────┐
│                    Validator Service                        │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │   Gap Detector  │  │  Backfill Engine │  │ Stats Monitor│ │
│  │                 │  │                 │  │              │ │
│  │ • 时间序列分析   │  │ • REST API调用   │  │ • 性能统计   │ │
│  │ • 缺口识别      │  │ • 批量数据处理   │  │ • 健康检查   │ │
│  │ • 多线程扫描    │  │ • 错误重试      │  │ • 监控指标   │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     External Services                      │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐                    ┌─────────────────┐ │
│  │   ClickHouse    │                    │   Binance API   │ │
│  │                 │                    │                 │ │
│  │ • 数据存储      │                    │ • 历史K线数据   │ │
│  │ • 缺口查询      │                    │ • REST接口      │ │
│  │ • 批量插入      │                    │ • 限流控制      │ │
│  └─────────────────┘                    └─────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### 数据流程

1. **定期触发**: 根据配置的检查间隔启动验证流程
2. **获取交易对**: 从数据库查询所有存在的交易对列表
3. **并发检测**: 为每个交易对启动独立的缺口检测任务
4. **缺口分析**: 分析时间序列数据，识别缺失的时间段
5. **过滤处理**: 根据配置过滤掉过大或过小的缺口
6. **API调用**: 调用币安REST API获取缺失数据
7. **数据插入**: 批量插入回补的数据到ClickHouse
8. **统计更新**: 更新服务运行统计信息

## 配置说明

### 配置文件示例

```yaml
validator:
  enabled: true                # 是否启用验证服务
  check_interval: "5m"         # 检查间隔
  max_gap_duration: "24h"      # 最大允许修复的数据间隙时长
  history_days: 7              # 检查历史数据的天数
  batch_size: 100              # 批量处理大小
  concurrent_workers: 3        # 并发工作线程数
```

### 配置参数详解

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `enabled` | bool | true | 是否启用验证服务 |
| `check_interval` | string | "5m" | 定期检查的时间间隔 |
| `max_gap_duration` | string | "24h" | 允许自动修复的最大缺口时长 |
| `history_days` | int | 7 | 检查历史数据的天数 |
| `batch_size` | int | 100 | 批量处理的数据条数 |
| `concurrent_workers` | int | 3 | 并发处理的工作线程数 |

## 使用方法

### 1. 独立运行

```bash
# 编译backfill-validator服务
go build -o bin/backfill-validator cmd/backfill-validator/main.go

# 运行服务（持续模式）
./bin/backfill-validator -config config/config.yaml

# 仅执行验证
./bin/backfill-validator -config config/config.yaml -validate-only

# 回填特定交易对
./bin/backfill-validator -config config/config.yaml -symbol BTCUSDT -days 5

# 回填所有交易对
./bin/backfill-validator -config config/config.yaml -days 5
```

### 2. 集成到主服务

```go
package main

import (
    "data4trend/pkg/backfill"
    "data4trend/pkg/config"
    "data4trend/pkg/storage"
)

func main() {
    // 加载配置
    cfg, _ := config.LoadConfig("config.yaml")
    
    // 初始化存储
    storage, _ := storage.NewClickHouseStorage(cfg, logger)
    
    // 初始化合并的BackfillValidator服务
    backfillValidator := backfill.NewBackfillValidatorService(cfg, storage, logger)
    
    // 启动服务
    backfillValidator.Start()
    defer backfillValidator.Stop()
    
    // 获取统计信息
    stats := backfillValidator.GetStats()
    fmt.Printf("Total checks: %d\n", stats.TotalChecks)
}
```

### 3. API接口

验证服务提供以下方法：

```go
// 启动服务
validator.Start()

// 停止服务
validator.Stop()

// 检查服务状态
isRunning := validator.IsRunning()

// 获取统计信息
stats := validator.GetStats()

// 强制执行一次验证
result := validator.ForceValidation()

// 验证指定时间范围
result := validator.ValidateDataRange(startTime, endTime)

// 验证指定交易对
result := validator.ValidateSymbol("BTCUSDT", startTime, endTime)
```

## 监控与统计

### 统计指标

服务提供详细的运行统计信息：

```json
{
  "last_check_time": "2023-10-26T15:30:00Z",
  "total_checks": 1250,
  "gaps_detected": 45,
  "gaps_fixed": 42,
  "backfill_errors": 3,
  "data_coverage_pct": 99.85,
  "symbols_checked": 150,
  "total_missing_minutes": 180,
  "continuous_days": 7,
  "oldest_data_time": "2023-10-19T00:00:00Z",
  "newest_data_time": "2023-10-26T15:29:00Z",
  "last_backfill_duration": "2.5s"
}
```

### 日志监控

服务提供结构化日志输出：

```
2023-10-26 15:30:00 INFO Starting validator service
2023-10-26 15:30:05 INFO Found 150 symbols for validation
2023-10-26 15:30:10 INFO Detected gap: BTCUSDT 2023-10-26 10:05:00 - 10:20:00 (15 minutes)
2023-10-26 15:30:12 INFO Successfully backfilled gap: BTCUSDT 15 records
2023-10-26 15:30:15 INFO Validation completed: 150 symbols, 3 gaps fixed
```

## 错误处理

### 常见错误及解决方案

1. **API限流错误**
   - 错误: `rate limit exceeded`
   - 解决: 增加重试间隔，减少并发数

2. **数据库连接错误**
   - 错误: `connection refused`
   - 解决: 检查ClickHouse服务状态和网络连接

3. **数据格式错误**
   - 错误: `invalid timestamp format`
   - 解决: 检查API返回数据格式，更新解析逻辑

4. **内存不足**
   - 错误: `out of memory`
   - 解决: 减少batch_size，增加系统内存

### 重试机制

- **API调用重试**: 自动重试失败的API调用，支持指数退避
- **数据库重试**: 数据库操作失败时自动重试
- **错误记录**: 详细记录所有错误信息用于问题排查

## 性能优化

### 配置优化建议

1. **检查间隔**: 根据数据重要性调整检查频率
   - 高频交易: 1-5分钟
   - 普通监控: 10-30分钟

2. **批量大小**: 根据网络和内存情况调整
   - 网络良好: 500-1000
   - 网络一般: 100-500
   - 网络较差: 50-100

3. **并发数**: 根据系统资源调整
   - 高性能服务器: 5-10
   - 普通服务器: 2-5
   - 低配置服务器: 1-2

### 资源使用

- **CPU**: 主要用于数据处理和API调用
- **内存**: 缓存批量数据和统计信息
- **网络**: API调用和数据库连接
- **磁盘**: 日志文件和临时数据

## 最佳实践

1. **定期监控**: 设置监控告警，及时发现问题
2. **日志轮转**: 配置日志轮转避免磁盘空间不足
3. **备份策略**: 定期备份配置文件和重要数据
4. **版本管理**: 使用版本控制管理配置变更
5. **测试验证**: 在生产环境部署前充分测试

## 故障排除

### 诊断命令

```bash
# 检查服务状态
./bin/validator -stats

# 运行一次性检查
./bin/validator -once -log-level debug

# 检查配置文件
./bin/validator -config config/config.yaml -validate-config

# 测试数据库连接
./bin/validator -test-db
```

### 常见问题

1. **服务无法启动**
   - 检查配置文件格式
   - 验证数据库连接
   - 查看端口占用情况

2. **数据回补失败**
   - 检查API密钥配置
   - 验证网络连接
   - 查看API限流状态

3. **性能问题**
   - 调整并发数和批量大小
   - 检查系统资源使用
   - 优化数据库查询

## 更新日志

### v1.0.0 (2023-10-26)
- 初始版本发布
- 支持基本的数据缺口检测和回补
- 提供完整的配置和监控功能
- 支持多交易对并发处理
- 集成币安REST API
- 提供详细的统计信息和日志