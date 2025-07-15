# 断点续传功能实现文档

## 概述

本文档详细说明了Data4Trend系统中断点续传功能的实现原理、技术细节和使用方法。断点续传功能确保数据收集器在程序重启后能够从上次中断的位置继续收集数据，避免重复收集和数据丢失。

## 功能特性

### 核心特性
- **自动状态保存**：每收集完一批数据后自动保存当前进度
- **智能恢复**：程序重启时自动检测并加载上次保存的状态
- **多维度支持**：支持多个交易对和多个时间周期的独立状态管理
- **异常处理**：状态文件损坏或丢失时自动使用配置的默认起始时间
- **实时更新**：收集过程中实时更新状态，确保进度不丢失

### 技术优势
- **零配置**：无需额外配置，自动启用断点续传功能
- **高可靠性**：多层异常处理确保系统稳定运行
- **低开销**：状态保存操作对性能影响极小
- **易维护**：状态文件采用YAML格式，便于查看和调试

## 技术实现

### 架构设计

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  BinanceCollector │    │  SymbolCollector  │    │  CollectorState │
│                 │    │                  │    │                 │
│ StartWithSymbols│───▶│ NewSymbolCollector│───▶│ LoadCollectorState│
│                 │    │                  │    │                 │
│ AddSymbol       │    │ CollectData      │    │ SaveCollectorState│
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### 核心组件

#### 1. 状态管理 (config/collector_state.go)

**数据结构**：
```go
type CollectorStateData struct {
    UpdatedAt time.Time         `yaml:"updated_at"`
    States    []SymbolStateData `yaml:"states"`
}

type SymbolStateData struct {
    SymbolState   SymbolInfo         `yaml:"symbol_state"`
    IntervalState []IntervalStateData `yaml:"interval_state"`
}

type IntervalStateData struct {
    Interval string    `yaml:"interval"`
    LastTime time.Time `yaml:"last_time"`
}
```

**核心方法**：
- `LoadCollectorState()`: 从YAML文件加载状态
- `SaveCollectorState()`: 将状态保存到YAML文件

#### 2. 符号收集器 (pkg/datacollector/symbol_collector.go)

**修改的函数签名**：
```go
// 原始签名
func NewSymbolCollector(symbol string, config *config.SymbolConfig) *SymbolCollector

// 新签名（支持断点续传）
func NewSymbolCollector(symbol string, config *config.SymbolConfig, savedStates map[string]time.Time) *SymbolCollector
```

**状态优先级**：
1. 传入的savedStates（断点续传状态）
2. 配置文件中的StartTime
3. 默认时间（当前时间-24小时）

#### 3. Binance收集器 (pkg/datacollector/binance_collector.go)

**StartWithSymbols函数修改**：
```go
func (b *BinanceCollector) StartWithSymbols(symbols []string) error {
    // 加载收集器状态
    savedStates, err := b.config.LoadCollectorState()
    if err != nil {
        logging.Logger.WithError(err).Warn("Failed to load collector state, starting fresh")
        savedStates = make(map[string]map[string]time.Time)
    }

    for _, symbol := range symbols {
        // 获取该交易对的保存状态
        symbolStates := savedStates[symbol]
        if symbolStates == nil {
            symbolStates = make(map[string]time.Time)
        }

        // 创建SymbolCollector时传入保存的状态
        collector := NewSymbolCollector(symbol, b.config, symbolStates)
        // ...
    }
}
```

## 状态文件格式

### 文件位置
`config/collector_state.yaml`

### 文件格式
```yaml
updated_at: "2024-01-15T10:30:00Z"
states:
  - symbol_state:
      symbol: "BTCUSDT"
    interval_state:
      - interval: "1m"
        last_time: "2024-01-15T10:29:00Z"
      - interval: "5m"
        last_time: "2024-01-15T10:25:00Z"
      - interval: "15m"
        last_time: "2024-01-15T10:15:00Z"
      - interval: "1h"
        last_time: "2024-01-15T10:00:00Z"
      - interval: "4h"
        last_time: "2024-01-15T08:00:00Z"
      - interval: "1d"
        last_time: "2024-01-15T00:00:00Z"
  - symbol_state:
      symbol: "ETHUSDT"
    interval_state:
      - interval: "1m"
        last_time: "2024-01-15T10:29:00Z"
      # ... 其他时间周期
```

## 使用方法

### 基本使用

1. **正常启动**：
```bash
./bin/data-collector-materialized -config=config/symbols.yaml
```

2. **程序会自动**：
   - 检查是否存在状态文件
   - 如果存在，加载上次的收集进度
   - 如果不存在，使用配置的起始时间

3. **停止程序**：
   - 使用Ctrl+C优雅停止
   - 状态会自动保存

4. **重新启动**：
   - 程序自动从上次中断位置继续

### 状态管理

#### 查看当前状态
```bash
cat config/collector_state.yaml
```

#### 重置状态（从头开始收集）
```bash
rm config/collector_state.yaml
```

#### 手动调整特定交易对的起始时间
```bash
# 编辑状态文件
vim config/collector_state.yaml

# 修改对应交易对的last_time字段
```

## 测试验证

### 测试用例

项目包含完整的测试套件 (`test/resume_test.go`)：

1. **TestResumeFromSavedState**: 测试从保存状态恢复的基本功能
2. **TestSymbolCollectorWithSavedState**: 测试SymbolCollector使用保存状态的逻辑
3. **TestEmptyStateFile**: 测试空状态文件的处理
4. **TestInvalidStateFile**: 测试无效状态文件的异常处理

### 运行测试
```bash
cd test
go test -v resume_test.go
```

### 演示程序

项目提供了演示程序 (`examples/resume_demo.go`) 展示断点续传功能：

```bash
go run examples/resume_demo.go
```

演示程序会：
1. 模拟保存状态
2. 加载状态
3. 更新状态
4. 展示SymbolCollector如何使用保存的状态

## 异常处理

### 状态文件损坏
- **现象**：YAML解析失败
- **处理**：记录警告日志，使用配置的默认起始时间
- **恢复**：删除损坏的状态文件，程序会重新创建

### 状态文件不存在
- **现象**：首次运行或状态文件被删除
- **处理**：使用配置文件中的起始时间
- **行为**：程序正常运行，会自动创建新的状态文件

### 权限问题
- **现象**：无法读写状态文件
- **处理**：记录错误日志，程序继续运行但不保存状态
- **解决**：检查config目录的读写权限

## 性能影响

### 内存使用
- **状态数据**：每个交易对约100-200字节
- **总体影响**：对于200个交易对，额外内存使用约20-40KB

### 磁盘I/O
- **读取频率**：程序启动时读取一次
- **写入频率**：每收集完一批数据写入一次
- **文件大小**：通常小于50KB

### CPU开销
- **YAML序列化/反序列化**：毫秒级操作
- **总体影响**：可忽略不计

## 最佳实践

### 部署建议
1. **备份状态文件**：定期备份`config/collector_state.yaml`
2. **监控日志**：关注状态加载/保存相关的日志信息
3. **权限设置**：确保程序对config目录有读写权限

### 故障排除
1. **检查日志**：查看是否有状态相关的错误或警告
2. **验证文件**：确认状态文件格式正确
3. **重置测试**：删除状态文件测试是否能正常重新开始

### 开发扩展
1. **添加新字段**：在状态结构体中添加新字段时注意向后兼容
2. **修改格式**：如需修改状态文件格式，提供迁移机制
3. **性能优化**：对于大量交易对，考虑使用更高效的序列化格式

## 总结

断点续传功能为Data4Trend系统提供了重要的可靠性保障，确保数据收集的连续性和完整性。该功能的实现具有以下特点：

- **透明性**：对用户完全透明，无需额外配置
- **可靠性**：多层异常处理确保系统稳定
- **高效性**：最小的性能开销
- **可维护性**：清晰的代码结构和完整的测试覆盖

通过这个功能，Data4Trend系统能够在各种异常情况下保持数据收集的连续性，为量化交易应用提供更可靠的数据基础。