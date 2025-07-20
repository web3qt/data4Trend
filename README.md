# 币安WebSocket数据收集器

## 📊 项目简介

这是一个高性能的币安WebSocket数据收集器，专门用于实时收集**所有币安代币的1分钟K线数据**，并自动管理7天数据保留期。

## ✨ 主要特性

- 🚀 **WebSocket实时数据流** - 使用币安WebSocket API实时接收数据
- 💰 **全币种覆盖** - 自动获取并订阅所有币安现货交易对
- ⏰ **1分钟K线数据** - 专注收集1分钟时间间隔的K线数据
- 🗄️ **自动数据管理** - 自动清理7天前的历史数据
- 📈 **高性能存储** - 使用ClickHouse数据库进行高效存储
- 🔄 **自动重连** - WebSocket连接断开时自动重连
- 📊 **实时监控** - 提供数据收集状态和质量监控

## 🏗️ 系统架构

```
币安WebSocket API → WebSocket收集器 → ClickHouse数据库
                                   ↓
                              数据清理服务 (7天TTL)
                                   ↓
                              监控和状态API
```

## 📋 系统要求

- Go 1.21+
- ClickHouse 23.0+
- 网络连接（访问币安API）

## ⚙️ 安装和配置

### 1. 安装依赖

```bash
go mod download
```

### 2. 配置ClickHouse

```bash
# 启动ClickHouse服务
docker-compose up -d

# 初始化数据库
clickhouse-client --query="$(cat scripts/init_database.sql)"
```

### 3. 配置文件

编辑 `config/websocket_1m.yaml`:

```yaml
# 数据库配置
clickhouse:
  host: "localhost"
  port: 9000
  database: "data4trend"
  user: "default"
  password: "123456"

# Binance API配置
binance:
  api_key: "your_api_key"      # 可选，用于获取交易对列表
  secret_key: "your_secret"    # 可选

# 性能配置
performance:
  workers: 10
  data_channel_buffer: 50000
  websocket_batch_size: 50
```

### 4. 运行程序

```bash
# 使用默认配置启动
go run cmd/websocket-collector/main.go

# 使用自定义配置启动
go run cmd/websocket-collector/main.go -config /path/to/config.yaml

# 初始化数据库表
go run cmd/websocket-collector/main.go -init-db
```

## 📊 数据表结构

### 主数据表 (klines_1m)

| 字段 | 类型 | 说明 |
|------|------|------|
| symbol | String | 交易对符号 (如 BTCUSDT) |
| open_time | DateTime64(3) | 开盘时间 |
| close_time | DateTime64(3) | 收盘时间 |
| open | Decimal(20,8) | 开盘价 |
| high | Decimal(20,8) | 最高价 |
| low | Decimal(20,8) | 最低价 |
| close | Decimal(20,8) | 收盘价 |
| volume | Decimal(20,8) | 成交量 |
| quote_asset_volume | Decimal(20,8) | 成交额 |
| number_of_trades | UInt64 | 成交笔数 |

**数据保留政策**: 自动删除7天前的数据 (TTL: 7天)

## 🔧 API接口

### 系统状态查询

```bash
# 查询实时统计
curl http://localhost:8080/api/stats

# 查询WebSocket连接状态
curl http://localhost:8080/api/websocket/status

# 查询数据质量指标
curl http://localhost:8080/api/data/quality?symbol=BTCUSDT
```

### 数据查询

```bash
# 查询特定交易对的最新数据
curl "http://localhost:8080/api/klines?symbol=BTCUSDT&limit=100"

# 查询指定时间范围的数据
curl "http://localhost:8080/api/klines?symbol=BTCUSDT&start_time=2024-01-01T00:00:00Z&end_time=2024-01-02T00:00:00Z"
```

## 🏃 使用示例

### 基本启动

```bash
# 1. 启动ClickHouse数据库
docker-compose up -d

# 2. 初始化数据库表
go run cmd/websocket-collector/main.go -init-db

# 3. 启动数据收集器
go run cmd/websocket-collector/main.go
```

### 查询数据

```sql
-- 查询BTCUSDT最新1小时数据
SELECT * FROM klines_1m 
WHERE symbol = 'BTCUSDT' 
  AND open_time >= now() - INTERVAL 1 HOUR 
ORDER BY open_time DESC;

-- 查询所有交易对的最新价格
SELECT symbol, close as latest_price, open_time 
FROM klines_1m 
WHERE (symbol, open_time) IN (
  SELECT symbol, max(open_time) 
  FROM klines_1m 
  GROUP BY symbol
);
```

## 📈 监控和运维

### 系统监控

程序提供实时监控指标:
- 活跃连接数
- 数据收集速率
- 内存使用情况
- 错误统计

### 日志管理

日志级别配置:
```yaml
log:
  level: "info"           # debug, info, warn, error
  json_format: true
  file_path: "logs/collector.log"
```

### 性能调优

关键配置项:
- `websocket_batch_size`: WebSocket连接批次大小 (推荐: 50)
- `data_channel_buffer`: 数据通道缓冲区 (推荐: 50000)
- `workers`: 工作协程数 (推荐: 10)

## 🔍 故障排除

### 常见问题

1. **WebSocket连接失败**
   - 检查网络连接
   - 验证代理设置
   - 查看错误日志

2. **数据缺失**
   - 检查WebSocket连接状态
   - 查看数据质量监控表
   - 验证交易对是否有效

3. **性能问题**
   - 监控内存使用
   - 调整缓冲区大小
   - 检查ClickHouse性能

### 日志分析

```bash
# 查看实时日志
tail -f logs/collector.log

# 过滤错误日志
grep "ERROR" logs/collector.log

# 统计连接状态
grep "WebSocket" logs/collector.log | grep "connected"
```

## 🤝 贡献指南

1. Fork项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启Pull Request

## 📝 许可证

此项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 🆘 支持

如有问题，请通过以下方式联系:
- 创建 GitHub Issue
- 发送邮件到项目维护者

---

**注意**: 这是一个数据收集工具，仅用于获取公开的市场数据。请遵守币安API使用条款和相关法律法规。