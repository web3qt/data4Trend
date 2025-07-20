# 币安WebSocket数据收集器

这是一个高性能的币安WebSocket数据收集器，用Rust编写，专门收集所有币安代币的1分钟K线数据并存储到ClickHouse数据库中。

## 功能特性

### 🚀 核心功能

- **实时数据收集**: 通过WebSocket连接收集币安所有交易对的1分钟K线数据
- **高性能存储**: 使用ClickHouse数据库进行高效数据存储和查询
- **RESTful API**: 提供完整的HTTP API接口用于数据查询
- **智能监控**: 内置系统监控、健康检查和性能统计
- **自动重连**: 智能的WebSocket重连机制，确保数据连续性

### 📊 数据管理

- **批量处理**: 高效的批量数据插入机制
- **数据清理**: 自动清理过期数据（默认保留7天）
- **符号过滤**: 支持交易对过滤和筛选
- **错误处理**: 完善的错误处理和重试机制

### 🔧 技术特性

- **异步架构**: 基于Tokio的高性能异步运行时
- **内存安全**: Rust语言的内存安全保证
- **配置驱动**: 灵活的YAML配置文件
- **生产就绪**: 包含日志、监控、健康检查等生产特性

## 快速开始

### 前置要求

1. **Rust环境** (1.70+)

   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

2. **ClickHouse数据库**

   ```bash
   # 使用Docker运行ClickHouse
   docker run -d --name clickhouse-server \
     -p 8123:8123 -p 9000:9000 \
     --ulimit nofile=262144:262144 \
     clickhouse/clickhouse-server
   ```

### 安装和配置

1. **克隆项目**

   ```bash
   git clone <repository-url>
   cd data4Trend
   ```

2. **配置数据库**

   ```bash
   # 编辑配置文件
   cp config/config.yaml config/config.yaml.local
   vim config/config.yaml.local
   ```

3. **初始化数据库**

   ```bash
   cd binance_ws_collector
   cargo run -- --config ../config/config.yaml.local --init-db
   ```

4. **启动服务**

   ```bash
   cargo run -- --config ../config/config.yaml.local
   ```

## 配置说明

### 主要配置项

```yaml
# ClickHouse数据库配置
clickhouse:
  host: "localhost"
  port: 9000
  http_port: 8123
  database: "data4trend"
  username: "default"
  password: "your_password"
  data_retention_days: 7

# 币安API配置
binance:
  ws_url: "wss://stream.binance.com:9443/ws"
  reconnect_interval: 5
  max_reconnect_attempts: 10

# 性能配置
performance:
  data_channel_buffer: 50000
  batch_size: 1000
  batch_timeout: 5
  max_concurrent_connections: 50

# 服务器配置
server:
  host: "0.0.0.0"
  port: 8080

# 监控配置
monitoring:
  enabled: true
  metrics_interval: 60
  alert_thresholds:
    error_rate_percent: 10.0
    no_data_timeout_seconds: 300
```

## API接口

### 健康检查

```bash
GET /health
```

### 获取K线数据

```bash
GET /api/klines/:symbol?limit=100&start_time=1640995200&end_time=1641081600
```

### 获取系统统计

```bash
GET /api/stats
```

### 获取支持的交易对

```bash
GET /api/symbols
```

## 监控和运维

### 日志管理

- 日志文件位置: `logs/collector.log`
- 支持JSON格式日志
- 自动日志轮转和压缩

### 性能监控

- 实时消息处理统计
- 错误率监控
- 连接状态监控
- 内存和CPU使用率监控

### 健康检查

- 数据库连接状态
- WebSocket连接状态
- API服务状态
- 数据接收状态

## 架构设计

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   WebSocket     │    │   Data Channel   │    │   ClickHouse    │
│   Collector     │───▶│   (Async MPSC)   │───▶│   Storage       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                                               │
         ▼                                               ▼
┌─────────────────┐                            ┌─────────────────┐
│   Monitoring    │                            │   HTTP API      │
│   Manager       │                            │   Server        │
└─────────────────┘                            └─────────────────┘
```

### 核心组件

1. **WebSocket收集器** (`src/collector/websocket.rs`)
   - 管理多个WebSocket连接
   - 自动重连和错误处理
   - 符号管理和过滤

2. **数据存储** (`src/storage/clickhouse.rs`)
   - 批量数据插入
   - 数据清理和维护
   - 查询接口

3. **API服务器** (`src/api/server.rs`)
   - RESTful API接口
   - 请求处理和响应
   - 错误处理

4. **监控系统** (`src/monitoring/mod.rs`)
   - 系统统计
   - 健康检查
   - 性能监控

## 性能优化

### 内存优化

- 使用异步通道减少内存拷贝
- 批量处理减少系统调用
- 智能缓冲区管理

### 网络优化

- 连接池管理
- 自动重连机制
- 压缩传输支持

### 数据库优化

- 批量插入操作
- 索引优化
- 分区表设计

## 故障排除

### 常见问题

1. **连接失败**
   - 检查网络连接
   - 验证ClickHouse服务状态
   - 检查配置文件

2. **数据丢失**
   - 检查WebSocket连接状态
   - 查看错误日志
   - 验证数据库写入权限

3. **性能问题**
   - 调整批量大小
   - 增加缓冲区大小
   - 优化数据库配置

### 调试模式

```bash
# 启用调试日志
RUST_LOG=debug cargo run -- --config config/config.yaml
```

## 贡献指南

1. Fork项目
2. 创建特性分支
3. 提交更改
4. 推送到分支
5. 创建Pull Request
