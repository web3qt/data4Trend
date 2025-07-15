# Data4Trend 数据馈送与趋势分析系统

## 项目概述

Data4Trend 是一个高性能加密货币市场数据收集和趋势分析系统，专为量化交易应用设计。系统自动获取市值前200的加密货币，从Binance交易所实时获取K线数据，经过处理后存储到ClickHouse数据库，并提供API接口供其他应用程序访问。同时，系统包含趋势扫描器组件，可基于移动平均线(MA)策略对收集的数据进行趋势分析。

**🚀 新架构升级**: 项目已重构为基于ClickHouse最佳实践的**物化视图架构**，采用单一事实表 + 物化视图自动聚合的设计模式，提供更高的存储效率和查询性能。详见 [物化视图架构指南](MATERIALIZED_VIEWS_GUIDE.md)。

**📈 历史数据支持**: 系统已配置为从**2019年1月1日**开始收集历史数据，为长期趋势分析和回测提供充足的数据基础。所有配置已验证并测试通过。数据库采用Docker Compose部署，确保数据能正常写入和查询。

## 功能特性

### 🏗️ 架构特性
- **物化视图架构**：基于ClickHouse最佳实践，单一事实表 + 物化视图自动聚合
- **存储优化**：只存储1分钟原始数据，其他时间粒度自动聚合生成
- **查询性能**：预聚合数据提供毫秒级查询响应
- **数据一致性**：单一数据源确保所有时间粒度的数据一致性
- **实时更新**：物化视图自动、实时地更新聚合数据

### 📊 数据收集
- **市值前200排名**：自动获取并跟踪市值排名前200的加密货币
- **环境变量配置**：通过环境变量配置Binance API密钥，增强安全性
- **自定义开始时间**：可通过环境变量指定数据收集的开始时间
- **多交易对支持**：同时收集多个加密货币交易对的K线数据
- **多时间周期**：支持1分钟、5分钟、15分钟、1小时、4小时、1天等时间周期
- **历史数据回填**：支持从指定时间点开始回填历史数据
- **断点续传**：自动保存收集进度，程序重启后从上次中断位置继续收集数据
- **数据清洗**：对原始数据进行验证和清洗，确保数据质量

### 🔌 接口服务
- **实时API**：提供RESTful API和WebSocket接口，支持实时数据查询和推送
- **数据完整性检查**：提供工具检测和修复数据缺口
- **数据删除功能**：支持删除指定时间范围内的数据
- **趋势分析引擎**：基于MA线策略对币种进行趋势扫描分析
- **结果导出**：支持将趋势分析结果导出为CSV文件
- **容器化部署**：支持Docker和docker-compose快速部署

## 系统架构

```
+----------------+       +---------------+       +---------------+
| Binance API    |<----->| DataCollector |<----->| ClickHouse    |
| (加密货币K线)   |       | (Go routine)  |       | (数据存储)     |
+----------------+       +---------------+       +---------------+
                                ｜
                                ｜ 数据管道
                                ▼
                        +---------------+       +---------------+       +----------------+
                        | DataProcessor |------>| API Server    |       | TrendScanner   |
                        | (数据清洗)     |       | (数据访问)     |       | (趋势分析)     |
                        +---------------+       +---------------+       +----------------+
                                                      ｜                        |
                                                      ｜ WebSocket/REST         | CSV导出
                                                      ▼                        ▼
                                                +---------------+       +----------------+
                                                | 客户端应用    |       | 趋势分析结果   |
                                                | (数据消费)     |       | (CSV文件)     |
                                                +---------------+       +----------------+
```

### 核心组件

- **DataCollector**：负责从Binance API获取K线数据，自动获取市值前200的加密货币
- **DataProcessor**：对收集到的数据进行清洗和验证，确保数据质量
- **ClickHouse存储**：将处理后的数据存储到ClickHouse数据库，高性能时序数据库
- **API服务器**：提供RESTful API和WebSocket接口，支持数据查询和实时推送
- **TrendScanner**：独立运行的趋势分析引擎，基于MA线策略对币种进行趋势扫描

## 环境要求

- Go 1.18+
- ClickHouse 22.0+
- Binance API访问权限（可选，公共API不需要密钥）
- Docker & Docker Compose（可选，用于容器化部署）

## 快速开始（物化视图架构）

### 🚀 一键启动

```bash
# 克隆代码库
git clone https://github.com/web3qt/data4Trend.git
cd data4Trend

# 安装依赖
go mod download

# 构建程序
go build -o bin/data-collector-materialized ./cmd/data-collector-materialized
go build -o bin/trendscanner ./cmd/trendscanner

# 测试物化视图架构
./scripts/test-materialized.sh

# 启动数据收集器（物化视图架构）
INIT_DB=true ./scripts/start-materialized.sh
```

### 1. 启动ClickHouse数据库

使用Docker Compose启动ClickHouse服务：

```bash
# 启动ClickHouse容器
docker compose up -d clickhouse

# 检查容器状态
docker ps

# 验证数据库连接
docker exec data4trend-clickhouse-1 clickhouse-client --database data4trend --query "SHOW TABLES"
```

### 2. 验证数据写入和查询

```bash
# 插入测试数据
docker exec data4trend-clickhouse-1 clickhouse-client --database data4trend --query "INSERT INTO kline_raw (id, symbol, open_time, close_time, open_price, high_price, low_price, close_price, volume) VALUES (1, 'BTCUSDT', '2019-01-01 00:00:00', '2019-01-01 00:01:00', 3800.0, 3850.0, 3790.0, 3820.0, 100.5)"

# 查询数据验证
docker exec data4trend-clickhouse-1 clickhouse-client --database data4trend --query "SELECT * FROM kline_raw ORDER BY open_time"

# 聚合查询测试
docker exec data4trend-clickhouse-1 clickhouse-client --database data4trend --query "SELECT symbol, COUNT(*) as count, AVG(close_price) as avg_price FROM kline_raw GROUP BY symbol"
```

### 📋 详细步骤

1. **初始化数据库**
```bash
# 初始化ClickHouse表结构
./bin/data-collector-materialized -init-db
```

2. **启动数据收集**
```bash
# 启动数据收集器
./bin/data-collector-materialized -config=config/symbols.yaml
```

3. **启动趋势分析**
```bash
# 启动趋势扫描器
./bin/trendscanner -config=config/trend_scanner.yaml
```

## 安装与部署

### 源码编译

```bash
# 克隆代码库
git clone https://github.com/web3qt/data4Trend.git
cd data4Trend

# 安装依赖
go mod download

# 编译数据采集器（物化视图架构）
go build -o bin/data-collector-materialized ./cmd/data-collector-materialized

# 编译趋势扫描器
go build -o bin/trendscanner ./cmd/trendscanner

# 编译传统数据采集器（兼容性）
go build -o bin/dataFeeder cmd/main.go
```

### 数据库管理

系统使用ClickHouse作为数据存储，建议使用Docker Compose快速启动：

```bash
# 启动ClickHouse数据库
docker-compose up -d clickhouse

# 查看ClickHouse日志
docker-compose logs -f clickhouse

# 连接到ClickHouse CLI
docker exec -it data4trend_clickhouse_1 clickhouse-client
```

### 环境变量配置

系统通过环境变量读取敏感配置信息。您可以设置以下环境变量：

```bash
# 设置Binance API密钥（可选）
export BINANCE_API_KEY="your_api_key"
export BINANCE_SECRET_KEY="your_secret_key"

# 设置数据收集开始时间（RFC3339格式，可选，默认为30天前）
export COLLECTION_START_TIME="2022-01-01T00:00:00Z"

# ClickHouse数据库配置（可选，如果与默认值不同）
export CLICKHOUSE_HOST="localhost"
export CLICKHOUSE_PORT="9000"
export CLICKHOUSE_HTTP_PORT="8123"
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD=""
export CLICKHOUSE_DATABASE="data4trend"
```

### 运行数据采集服务

#### 🆕 物化视图架构（推荐）

```bash
# 设置环境变量
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=9000
export CLICKHOUSE_HTTP_PORT=8123
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=123456
export CLICKHOUSE_DATABASE=data4trend

# 初始化数据库表结构
./bin/data-collector-materialized -init-db

# 启动数据收集器
./bin/data-collector-materialized -config config/symbols.yaml

# 使用启动脚本（自动初始化）
INIT_DB=true ./scripts/start-materialized.sh

# 测试架构功能
./scripts/test-materialized.sh
```

#### 传统架构（兼容性）

```bash
# 直接运行
./bin/dataFeeder

# 使用配置文件运行
./bin/dataFeeder -config config/symbols.yaml

# 指定API服务器端口
./bin/dataFeeder -port 8080

# 或者直接运行
./bin/dataFeeder
```

### 运行趋势扫描器

趋势扫描器是一个独立的组件，可以与数据采集服务同时运行。**已更新为兼容物化视图架构**：

```bash
# 使用默认配置文件运行（兼容物化视图架构）
./bin/trendscanner

# 使用自定义配置文件
./bin/trendscanner -config config/trend_scanner.yaml

```

### 数据库状态检查与管理

系统提供了工具脚本用于检查和管理数据库状态：

```bash
# 检查数据库表和记录
go run check_db.go

# 检查特定交易对（如BTC）的数据
go run check_btc.go
```

### Docker部署

#### 单独构建镜像

```bash
docker build \
  --build-arg GOLANG_IMAGE=docker.m.daocloud.io/library/golang:1.20-alpine3.17 \
  --build-arg ALPINE_IMAGE=docker.m.daocloud.io/library/alpine:3.17 \
  -t data-feeder .
```

#### 使用环境变量配置

```bash
docker run -d \
  -e BINANCE_API_KEY="your_api_key" \
  -e BINANCE_SECRET_KEY="your_secret_key" \
  -e COLLECTION_START_TIME="2022-01-01T00:00:00Z" \
  -e CLICKHOUSE_HOST=clickhouse \
  -e CLICKHOUSE_PORT=9000 \
  -e CLICKHOUSE_HTTP_PORT=8123 \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=123456 \
  -e CLICKHOUSE_DATABASE=data4trend \
  -p 8080:8080 \
  --name datafeeder \
  data-feeder
```

#### 使用docker-compose

```bash
# 启动所有服务
docker-compose up -d

# 查看日志
docker-compose logs -f
```

## 配置说明

系统配置文件位于`config/`目录下。**物化视图架构使用相同的配置文件，无需额外配置**：

### 主配置文件 (config.yaml)

```yaml
binance:
  api_key: ""  # 从环境变量BINANCE_API_KEY读取
  secret_key: ""  # 从环境变量BINANCE_SECRET_KEY读取

clickhouse:
  host: "localhost"
  port: 9000
  http_port: 8123
  user: "default"
  password: ""
  database: "data4trend"

server:
  port: 8080
  env: development

http:
  timeout: 30
  proxy: "http://127.0.0.1:7890"  # 可选HTTP代理
  
# 币种配置文件路径
symbols_config_path: "config/symbols.yaml"

log:
  level: "debug"
  json_format: false
  output_path: "logs/dataFeeder.log"
```

### 交易对配置 (symbols.yaml)

```yaml
# 币种配置
groups:
  # 空组，不再使用主交易对组

# 全局设置
settings:
  max_symbols_per_batch: 30  # 每批处理的币种数，增加到30个
  discovery_enabled: true  # 启用自动发现新币种
  discovery_interval: 6h   # 缩短自动发现新币种的间隔
  excluded_symbols:  # 排除的币种
    - USDCUSDT
    - BUSDUSDT
    - TUSDUSDT
```

### 趋势扫描器配置 (trend_scanner.yaml)

```yaml
# 趋势扫描器配置

# 数据库配置
database:
  host: localhost
  port: 9000
  http_port: 8123
  user: default
  password: ""
  name: data4trend

# MA线配置
ma:
  period: 81         # MA周期，如MA81
  interval: "15m"    # K线时间间隔

# 扫描配置
scan:
  workers: 4                 # 工作协程数
  interval: "1h"             # 扫描间隔时间
  csv_output: "trend_results" # CSV输出目录

# 趋势条件配置
trend:
  check_points:
    - 10m    # 10分钟前 (1个15分钟K线)
    - 30m    # 30分钟前 (2个15分钟K线)
    - 1h     # 1小时前 (4个15分钟K线)
    - 4h     # 4小时前 (16个15分钟K线)
    - 1d     # 1天前 (96个15分钟K线)
  require_strict_up: false   # 是否要求严格上升（true）或者允许平稳（false）
  consecutive_klines: 20     # 要求连续多少根K线运行在MA线之上
```

## 数据收集时间控制

系统支持通过环境变量`COLLECTION_START_TIME`设置数据收集的开始时间。该时间应以RFC3339格式提供（例如："2022-01-01T00:00:00Z"）。

- 如果未设置此环境变量，系统默认从当前时间的30天前开始收集数据
- 设置较早的开始时间将导致系统回填更多的历史数据，这可能需要更长的处理时间
- 对于新添加的币种，系统也会自动从指定的开始时间收集数据

示例：
```bash
# 从2022年初开始收集数据
export COLLECTION_START_TIME="2022-01-01T00:00:00Z"
./dataFeeder
```

## 断点续传功能

系统支持断点续传功能，能够自动保存数据收集进度，在程序重启后从上次中断的位置继续收集数据，避免重复收集和数据丢失。

### 功能特性

- **自动状态保存**：系统每收集完一批数据后自动保存当前进度
- **智能恢复**：程序重启时自动检测并加载上次保存的状态
- **多维度支持**：支持多个交易对和多个时间周期的独立状态管理
- **异常处理**：状态文件损坏或丢失时自动使用配置的默认起始时间
- **实时更新**：收集过程中实时更新状态，确保进度不丢失

### 状态文件

断点续传状态保存在 `config/collector_state.yaml` 文件中，包含以下信息：

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
```

### 使用示例

```bash
# 首次启动数据收集器
./bin/data-collector-materialized -config=config/symbols.yaml

# 程序运行一段时间后手动停止（Ctrl+C）
# 状态会自动保存到 config/collector_state.yaml

# 重新启动程序，将自动从上次中断位置继续
./bin/data-collector-materialized -config=config/symbols.yaml
```

### 状态管理

- **查看状态**：可以直接查看 `config/collector_state.yaml` 文件了解当前收集进度
- **重置状态**：删除状态文件可以从配置的起始时间重新开始收集
- **手动修改**：可以手动编辑状态文件来调整特定交易对的收集起始时间

### 注意事项

- 状态文件会在程序正常运行时自动创建和更新
- 如果状态文件不存在或损坏，程序会使用配置文件中的起始时间
- 动态添加的新交易对不会使用断点续传，而是从配置的起始时间开始收集
- 建议定期备份状态文件以防意外丢失

## 数据存储结构

### 🆕 物化视图架构（推荐）

新架构采用单一事实表 + 物化视图的设计模式：

**原始数据表 (`kline_raw`)**：
- `symbol`: 交易对符号（如 'BTCUSDT'）
- `open_time`: 开盘时间（1分钟粒度）
- `close_time`: 收盘时间
- `open_price`: 开盘价格
- `high_price`: 最高价格
- `low_price`: 最低价格
- `close_price`: 收盘价格
- `volume`: 交易量
- `quote_volume`: 计价货币交易量
- `trades_count`: 交易笔数
- `taker_buy_volume`: 主动买入量
- `taker_buy_quote_volume`: 主动买入计价货币量

**聚合表（自动生成）**：
- `kline_5m`: 5分钟K线数据
- `kline_15m`: 15分钟K线数据
- `kline_1h`: 1小时K线数据
- `kline_4h`: 4小时K线数据
- `kline_1d`: 1天K线数据

**统一查询视图 (`v_kline_unified`)**：
- 提供跨时间粒度的统一查询接口
- 自动路由到对应的聚合表

### 传统架构（兼容性）

传统架构为每个交易对创建单独的数据表，表名为交易对名称的小写形式（例如BTCUSDT对应表名为`btc`）。每个表包含以下字段：

- `id`: 自动递增的主键
- `interval_type`: 时间周期（15m、1h、1d）
- `open_time`: 开盘时间
- `close_time`: 收盘时间
- `open_price`: 开盘价格
- `high_price`: 最高价格
- `low_price`: 最低价格
- `close_price`: 收盘价格
- `volume`: 交易量

## API接口

系统提供以下API接口。**物化视图架构完全兼容现有API，无需修改客户端代码**：

### REST API

#### 获取K线数据

```
GET /api/v1/klines?symbol=BTCUSDT&interval=15m&limit=100&start_time=1672527600000
```

参数说明：

- `symbol`: 交易对名称（必填）
- `interval`: 时间周期，如15m, 1h, 1d（必填）
- `limit`: 返回的数据点数量，默认500，最大1000
- `start_time`: 开始时间戳（毫秒）
- `end_time`: 结束时间戳（毫秒）

响应示例：

```json
[
  {
    "timestamp": 1672527600000,
    "open": 16850.3,
    "high": 16892.1,
    "low": 16820.5,
    "close": 16875.4,
    "volume": 25.384
  },
  ...
]
```

#### 获取多交易对K线数据

```
GET /api/v1/multi_klines?symbols=BTCUSDT,ETHUSDT&interval=1h&limit=10
```

#### 获取支持的交易对列表

```
GET /api/v1/symbols
```

#### 检查数据缺口

```
GET /api/v1/check_gaps?symbol=BTCUSDT&interval=1d&start_time=1672527600000&end_time=1672614000000
```

#### 修复数据缺口

```
POST /api/v1/fix_gaps
Content-Type: application/json

{
  "symbol": "BTCUSDT",
  "interval": "1d",
  "start_time": "2023-01-01T00:00:00Z",
  "end_time": "2023-01-10T00:00:00Z"
}
```

#### 删除指定时间范围内的数据

```
DELETE /api/v1/klines?symbol=BTCUSDT&interval=1h&start_time=2023-01-01T00:00:00Z&end_time=2023-01-31T23:59:59Z&confirm=true
```

### WebSocket API

WebSocket接口提供实时K线数据推送：

```
GET /api/v1/ws
```

连接后发送订阅消息：

```json
{
  "action": "subscribe",
  "symbol": "BTCUSDT",
  "interval": "15m"
}
```

## 趋势扫描分析

趋势扫描器是系统的核心分析组件，用于识别市场趋势。它基于移动平均线(MA)策略分析K线数据：

### 扫描策略

1. 计算指定周期（默认81）的移动平均线
2. 检查K线是否在MA线之上运行
3. 识别连续多根K线（默认20根）均在MA线之上的币种
4. 在多个时间点（10分钟前、30分钟前、1小时前、4小时前、1天前）检查趋势
5. 将满足条件的币种导出到CSV文件

### 结果文件

扫描结果会保存在`trend_results`目录下，文件名格式为：`trend_results_YYYYMMDD_HHMMSS.csv`

结果文件包含以下字段：
- 币种名称
- MA值
- 当前价格
- 1分钟、5分钟、15分钟、1小时、4小时、1天等时间段的趋势状态
- 趋势开始时间
- 连续在MA线上方的K线数量

## 故障排除

### 无法连接到数据库

- 检查ClickHouse数据库连接配置是否正确
- 确认ClickHouse服务是否运行中
- 检查数据库用户权限和网络连接
- 使用`docker-compose up -d clickhouse`启动ClickHouse服务

### 无法获取币种数据

- 检查网络连接，特别是对Binance API的访问
- 如果使用HTTP代理，确认代理服务正常
- 可能是API请求限制，等待一段时间后重试

### 数据表为空或数据不完整

使用API接口检查数据收集状态：

```bash
# 检查支持的交易对列表
curl "http://localhost:8080/api/v1/symbols"

# 查询特定交易对的数据
curl "http://localhost:8080/api/v1/klines?symbol=BTCUSDT&interval=1h&limit=10"
```

如果看到某个币种的数据不完整，可以尝试：

```bash
# 检查数据缺口
curl "http://localhost:8080/api/v1/check_gaps?symbol=BTCUSDT&interval=1h"

# 删除并重新收集数据
curl -X DELETE "http://localhost:8080/api/v1/klines?symbol=BTCUSDT&interval=1h&start_time=2023-01-01T00:00:00Z&end_time=2023-01-31T23:59:59Z&confirm=true"
```

### 趋势扫描器不产生结果

- 检查ClickHouse数据库中是否有足够的历史数据（至少需要MA周期+连续K线数量的数据点）
- 确认`trend_results`目录是否存在并可写
- 查看日志文件检查错误信息

## 📚 相关文档

- **[物化视图架构指南](MATERIALIZED_VIEWS_GUIDE.md)** - 详细的架构说明和最佳实践
- **[迁移指南](MIGRATION_GUIDE.md)** - 从传统架构迁移到物化视图架构的步骤
- **[监控使用指南](MONITOR_USAGE.md)** - 系统监控和性能优化

## 🔧 开发扩展

开发者可以根据需要扩展系统功能：

### 物化视图架构扩展
- 在 <mcfile name="materialized_clickhouse_store.go" path="pkg/datastore/materialized_clickhouse_store.go"></mcfile> 中扩展存储功能
- 修改 <mcfile name="clickhouse-init-materialized-views.sql" path="scripts/clickhouse-init-materialized-views.sql"></mcfile> 添加新的聚合表
- 在 <mcfile name="main.go" path="cmd/data-collector-materialized/main.go"></mcfile> 中调整数据收集逻辑

### 通用扩展
- 在`pkg/datacollector`中修改以支持其他交易所
- 在`pkg/dataprocessor`中添加更多数据处理逻辑
- 在`pkg/apiserver`中扩展API功能
- 在`pkg/trendscanner`中添加新的趋势分析算法

## 🚀 架构优势总结

**物化视图架构相比传统架构的优势**：

| 特性 | 传统架构 | 物化视图架构 |
|------|----------|-------------|
| 存储效率 | 重复存储多个时间粒度 | 只存储1分钟原始数据 |
| 查询性能 | 需要实时聚合计算 | 预聚合，毫秒级响应 |
| 数据一致性 | 多表可能不一致 | 单一数据源保证一致性 |
| 维护复杂度 | 需要管理多个表 | 自动化聚合，维护简单 |
| 扩展性 | 添加新粒度需要修改代码 | 只需添加物化视图 |

## 许可证

本项目采用MIT许可证 - 详情见LICENSE文件