# Data4Trend - 币安WebSocket数据收集器

基于Go语言开发的高性能币安交易所实时数据收集器，支持1分钟K线数据收集，存储到ClickHouse数据库，并提供REST API接口。

## ✨ 项目特性

- 🚀 **高性能**: Go语言开发，支持高并发WebSocket连接
- 📊 **实时数据**: 实时收集币安交易所K线数据
- 🗄️ **ClickHouse存储**: 高效的列式数据库存储
- 🌐 **REST API**: 提供完整的数据查询API
- 🔄 **自动重连**: 智能重连机制，确保数据连续性
- 🛡️ **代理支持**: 支持HTTP代理，避免网络限制
- 📈 **监控系统**: 内置监控和健康检查
- 🔧 **易于部署**: 单一二进制文件，配置简单

## 🚀 快速开始

### 启动程序
```bash
# 使用启动脚本（推荐）
./start.sh

# 或手动启动
export HTTP_PROXY=http://127.0.0.1:7890
export HTTPS_PROXY=http://127.0.0.1:7890
./data4trend-collector --config=config/config.yaml --log-level=info
```

### 数据库管理
```bash
# 清理数据库，重新开始
./clean_database.sh
```

### 编译程序
```bash
# 编译二进制文件
go build -o bin/data4trend-collector cmd/collector/main.go
```

## 🌐 API接口

程序启动后，API服务器运行在 `http://localhost:8080`

### 健康检查
```bash
curl http://localhost:8080/health
```

### 获取统计信息
```bash
# 数据库统计
curl http://localhost:8080/api/v1/stats

# WebSocket连接统计
curl http://localhost:8080/api/v1/websocket/stats
```

### 获取K线数据
```bash
# 获取BTCUSDT最新5条数据
curl "http://localhost:8080/api/v1/klines/BTCUSDT?limit=5"

# 获取指定时间范围的数据
curl "http://localhost:8080/api/v1/klines/ETHUSDT?limit=10&start_time=1640995200000&end_time=1640998800000"
```

### 数据回填接口
```bash
# 检查数据缺口状态
curl http://localhost:8080/api/v1/backfill/status

# 回填特定交易对的数据（默认最近24小时）
curl -X POST http://localhost:8080/api/v1/backfill/symbol/BTCUSDT

# 回填特定时间范围的数据
curl -X POST 'http://localhost:8080/api/v1/backfill/symbol/BTCUSDT?start_time=2025-07-30T04:00:00Z&end_time=2025-07-30T04:10:00Z'

# 回填所有交易对的数据缺口
curl -X POST http://localhost:8080/api/v1/backfill/all
```

## 📊 数据库连接

### ClickHouse连接信息
- **主机**: localhost
- **HTTP端口**: 8123
- **用户名**: default
- **密码**: 123456
- **数据库**: data4trend

### 直接查询数据库
```bash
# 测试连接
curl -u default:123456 "http://localhost:8123" --data-binary "SELECT 1"

# 查看表
curl -u default:123456 "http://localhost:8123" --data-binary "SHOW TABLES FROM data4trend"

# 查看数据量
curl -u default:123456 "http://localhost:8123" --data-binary "SELECT count(*) FROM data4trend.klines_1m"
```

## 📁 项目结构

```
data4Trend/
├── cmd/collector/          # 主程序入口
├── pkg/                    # 核心包
│   ├── api/               # REST API服务器
│   ├── config/            # 配置管理
│   ├── monitoring/        # 监控系统
│   ├── storage/           # ClickHouse存储
│   └── websocket/         # WebSocket客户端
├── internal/              # 内部包
│   ├── types/             # 数据类型定义
│   └── utils/             # 工具函数
├── config/                # 配置文件
│   ├── config_go.yaml    # 完整配置
│   └── config_go_simple.yaml # 简化配置
├── scripts/               # 数据库脚本
└── docs/                  # 文档
```

## ⚙️ 配置说明

### 主要配置项

- **database**: ClickHouse数据库连接配置
- **websocket**: WebSocket连接配置
- **api**: REST API服务器配置
- **proxy**: HTTP代理配置
- **symbols**: 监控的交易对列表
- **interval**: 数据收集间隔

### 配置文件

- `config.yaml`: 主配置文件，支持动态获取币安所有USDT交易对

### 动态交易对获取

程序启动时会自动从币安API获取所有可用的USDT交易对，无需手动配置交易对列表：

- **自动获取**: 从币安API实时获取所有USDT交易对（通常400+个）
- **智能过滤**: 自动排除杠杆代币（UP/DOWN/BEAR/BULL）
- **状态检查**: 只监控状态为TRADING且支持现货交易的交易对
- **配置灵活**: 可通过配置文件控制是否启用自动获取

```yaml
websocket:
  auto_fetch_symbols: true    # 启用自动获取
  symbol_filter:
    quote_asset: USDT         # 只获取USDT交易对
    exclude_patterns:         # 排除包含这些模式的交易对
      - UP
      - DOWN
      - BEAR
      - BULL
```

## 📈 常用查询

```sql
-- 查看数据量
SELECT count() as total_records FROM data4trend.klines_1m;

-- 查看最新数据
SELECT * FROM data4trend.klines_1m ORDER BY open_time DESC LIMIT 10;

-- 查看特定交易对数据
SELECT * FROM data4trend.klines_1m WHERE symbol = 'BTCUSDT' ORDER BY open_time DESC LIMIT 10;

-- 查看数据统计
SELECT 
    symbol,
    count() as records,
    min(open_time) as first_time,
    max(open_time) as last_time
FROM data4trend.klines_1m 
GROUP BY symbol;
```

## 🔄 数据回填机制

### 工作原理

数据回填机制是为了解决程序重启或网络中断导致的数据缺失问题。系统通过以下步骤实现智能数据回填：

#### 1. 数据缺口检测
- **时间序列生成**: 使用ClickHouse的`numbers()`函数生成连续的1分钟时间序列
- **实际数据对比**: 查询数据库中已存在的数据记录
- **缺口识别**: 通过LEFT JOIN找出时间序列中缺失的数据点

```sql
WITH 
    time_series AS (
        SELECT toDateTime(number * 60 + toUnixTimestamp(toDateTime('start_time'))) as expected_time
        FROM numbers(dateDiff('minute', toDateTime('start_time'), toDateTime('end_time')) + 1)
    ),
    actual_data AS (
        SELECT DISTINCT toDateTime(toInt64(open_time) / 1000) as actual_time
        FROM data4trend.klines_1m 
        WHERE symbol = 'BTCUSDT' AND ...
    )
SELECT expected_time
FROM time_series
LEFT JOIN actual_data ON time_series.expected_time = actual_data.actual_time
WHERE actual_data.actual_time IS NULL
```

#### 2. 历史数据获取
- **币安API调用**: 使用币安REST API获取历史K线数据
- **代理支持**: 自动使用配置的HTTP代理
- **速率限制**: 内置100ms请求间隔，避免触发API限制
- **数据转换**: 将币安API返回的数据转换为内部格式

#### 3. 数据插入
- **批量插入**: 使用ClickHouse的批量插入功能提高效率
- **重复检测**: 数据库层面的去重机制
- **事务安全**: 确保数据一致性

#### 4. 回填策略
- **按需回填**: 支持特定交易对和时间范围的回填
- **全量回填**: 检测所有交易对的数据缺口并自动回填
- **智能分组**: 将连续的缺失时间点合并为时间段，减少API调用次数

### 使用场景

1. **程序重启后**: 自动检测停机期间的数据缺失并回填
2. **网络中断**: 恢复连接后补充中断期间的数据
3. **历史数据补充**: 获取项目启动前的历史数据
4. **数据质量保证**: 定期检查和修复数据完整性

### 数据保留策略

- **自动清理**: ClickHouse TTL机制自动删除7天前的数据
- **存储优化**: 列式存储和压缩，节省存储空间
- **性能保证**: 定期清理确保查询性能

### 监控和状态

通过API接口可以实时监控回填状态：
- 检测到的数据缺口数量
- 各交易对的缺失情况
- 回填操作的成功率和耗时

## 🔧 代理设置

如果需要使用代理访问币安API，请设置环境变量：

```bash
# 设置HTTP代理
export HTTP_PROXY=http://127.0.0.1:7890
export HTTPS_PROXY=http://127.0.0.1:7890

# 然后启动程序
./start_go_simple.sh
```

## 🎯 运行状态检查

### 程序正常运行的标志
- ✅ WebSocket连接成功日志
- ✅ API服务器启动在8080端口
- ✅ ClickHouse数据库连接成功
- ✅ 数据开始写入数据库

### 健康检查命令
```bash
# 检查API服务器
curl http://localhost:8080/health

# 检查数据库连接
curl -u default:123456 "http://localhost:8123" --data-binary "SELECT 1"

# 检查数据写入
curl http://localhost:8080/api/v1/stats
```

## 🛠️ 故障排除

### 常见问题

1. **WebSocket连接失败**
   - 检查网络连接
   - 确认代理设置正确
   - 查看是否被币安限制

2. **数据库连接失败**
   - 确认ClickHouse服务运行
   - 检查连接配置
   - 验证用户名密码

3. **API服务器无响应**
   - 检查8080端口是否被占用
   - 查看程序启动日志

### 日志查看
```bash
# 查看程序运行日志
./bin/data4trend-collector --config=config/config_go_simple.yaml --log-level=debug
```

## 📋 技术栈

- **语言**: Go 1.21+
- **数据库**: ClickHouse
- **WebSocket**: gorilla/websocket
- **HTTP路由**: gin-gonic/gin
- **配置**: YAML
- **日志**: logrus

## 📄 许可证

本项目采用 MIT 许可证。

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

---

**Data4Trend** - 专业的加密货币数据收集解决方案