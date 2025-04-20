# Data4Trend 数据馈送系统

## 项目概述

Data4Trend 数据馈送系统是一个高性能的加密货币市场数据收集和处理系统，专为量化交易应用设计。系统自动获取市值前200的加密货币，从Binance交易所实时获取K线数据，经过处理后存储到MySQL数据库，并提供API接口供其他应用程序访问。

## 功能特性

- **市值前200排名**：自动获取并跟踪市值排名前200的加密货币
- **环境变量配置**：通过环境变量配置Binance API密钥，增强安全性
- **自定义开始时间**：可通过环境变量指定数据收集的开始时间
- **多交易对支持**：同时收集多个加密货币交易对的K线数据
- **三种时间周期**：支持15分钟、4小时、1天三种关键时间周期
- **历史数据回填**：支持从指定时间点开始回填历史数据
- **数据清洗**：对原始数据进行验证和清洗，确保数据质量
- **实时API**：提供RESTful API和WebSocket接口，支持实时数据查询和推送
- **数据完整性检查**：提供工具检测和修复数据缺口
- **数据删除功能**：支持删除指定时间范围内的数据
- **容器化部署**：支持Docker和docker-compose快速部署

## 系统架构

```
+----------------+       +---------------+       +---------------+
| Binance API    |<----->| DataCollector |<----->| MySQL         |
| (加密货币K线)   |       | (Go routine)  |       | (数据存储)     |
+----------------+       +---------------+       +---------------+
                                ｜
                                ｜ 数据管道
                                ▼
                        +---------------+       +---------------+
                        | DataProcessor |------>| API Server    |
                        | (数据清洗)     |       | (数据访问)     |
                        +---------------+       +---------------+
                                                      ｜
                                                      ｜ WebSocket/REST
                                                      ▼
                                                +---------------+
                                                | 客户端应用    |
                                                | (数据消费)     |
                                                +---------------+
```

### 核心组件

- **DataCollector**：负责从Binance API获取K线数据，自动获取市值前200的加密货币
- **DataProcessor**：对收集到的数据进行清洗和验证，确保数据质量
- **MySQL存储**：将处理后的数据存储到MySQL数据库，按交易对分表存储
- **API服务器**：提供RESTful API和WebSocket接口，支持数据查询和实时推送

## 环境要求

- Go 1.18+
- MySQL 5.7+
- Binance API访问权限（可选，公共API不需要密钥）
- Docker & Docker Compose（可选，用于容器化部署）

## 安装与部署

### 源码编译

```bash
# 克隆代码库
git clone https://github.com/web3qt/data4Trend.git
cd data4Trend

# 安装依赖
go mod download

# 编译
go build -o dataFeeder cmd/main.go
```

### 配置MySQL

```bash
# 重置数据库（清空所有数据）
./reset_local_db.sh

# 或者仅初始化数据库（如果不存在）
./setup_mysql.sh
```

### 环境变量配置

系统通过环境变量读取敏感配置信息。您可以设置以下环境变量：

```bash
# 设置Binance API密钥（可选）
export BINANCE_API_KEY="your_api_key"
export BINANCE_SECRET_KEY="your_secret_key"

# 设置数据收集开始时间（RFC3339格式，可选，默认为30天前）
export COLLECTION_START_TIME="2022-01-01T00:00:00Z"

# 数据库配置（可选，如果与默认值不同）
export MYSQL_HOST="localhost"
export MYSQL_PORT="3306"
export MYSQL_USER="root"
export MYSQL_PASSWORD="123456"
export MYSQL_DATABASE="data4trend"
```

### 运行服务

```bash
# 直接运行
./dataFeeder

# 或者使用提供的脚本
./run.sh
```

### 数据库状态检查

系统提供了工具脚本用于检查数据库状态，确认数据是否正常收集：

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
  -e MYSQL_HOST=mysql \
  -e MYSQL_PORT=3306 \
  -e MYSQL_USER=root \
  -e MYSQL_PASSWORD=123456 \
  -e MYSQL_DATABASE=data4trend \
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

系统配置文件位于`config/`目录下：

### 主配置文件 (config.yaml)

```yaml
binance:
  api_key: ""  # 从环境变量BINANCE_API_KEY读取
  secret_key: ""  # 从环境变量BINANCE_SECRET_KEY读取

mysql:
  host: "localhost"
  port: 3306
  user: "root"
  password: "123456"
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

## 数据存储结构

系统为每个交易对创建单独的数据表，表名为交易对名称的小写形式（例如BTCUSDT对应表名为`btc`）。每个表包含以下字段：

- `id`: 自动递增的主键
- `interval_type`: 时间周期（15m、4h、1d）
- `open_time`: 开盘时间
- `close_time`: 收盘时间
- `open_price`: 开盘价格
- `high_price`: 最高价格
- `low_price`: 最低价格
- `close_price`: 收盘价格
- `volume`: 交易量

## API接口

系统提供以下API接口：

### REST API

#### 获取K线数据

```
GET /api/v1/klines?symbol=BTCUSDT&interval=15m&limit=100&start_time=1672527600000
```

参数说明：

- `symbol`: 交易对名称（必填）
- `interval`: 时间周期，如15m, 4h, 1d（必填）
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
GET /api/v1/multi_klines?symbols=BTCUSDT,ETHUSDT&interval=4h&limit=10
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
DELETE /api/v1/klines?symbol=BTCUSDT&interval=4h&start_time=2023-01-01T00:00:00Z&end_time=2023-01-31T23:59:59Z&confirm=true
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

## 故障排除

### 无法连接到数据库

- 检查数据库连接配置是否正确
- 确认MySQL服务是否运行中
- 检查数据库用户权限

### 无法获取币种数据

- 检查网络连接，特别是对Binance API的访问
- 如果使用HTTP代理，确认代理服务正常
- 可能是API请求限制，等待一段时间后重试

### 数据表为空或数据不完整

使用检查脚本确认数据收集状态：

```bash
go run check_db.go
```

如果看到某个币种的数据不完整，可以尝试：

```bash
# 检查特定币种（如BTC）
go run check_btc.go

# 删除并重新收集数据
curl -X DELETE "http://localhost:8080/api/v1/klines?symbol=BTCUSDT&interval=4h&start_time=2023-01-01T00:00:00Z&end_time=2023-01-31T23:59:59Z&confirm=true"
```


## 启动扫描系统
```shell
./trendScanner -config config/trend_scanner.yaml
```

## 开发扩展

开发者可以根据需要扩展系统功能：

- 在`pkg/datacollector`中修改以支持其他交易所
- 在`pkg/dataprocessor`中添加更多数据处理逻辑
- 在`pkg/apiserver`中扩展API功能

## 许可证

本项目采用MIT许可证 - 详情见LICENSE文件