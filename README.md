# Web3QT 数据馈送系统

## 项目概述

Web3QT 数据馈送系统是一个高性能的加密货币市场数据收集和处理系统，专为量化交易应用设计。系统自动获取市值前200的加密货币，从Binance交易所实时获取K线数据，经过处理后存储到MySQL数据库，并提供API接口供其他应用程序访问。

## 功能特性

- **市值前200排名**：自动获取并跟踪市值排名前200的加密货币
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
- **MySQL存储**：将处理后的数据存储到MySQL数据库，按交易对和时间周期组织
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
# 运行MySQL初始化脚本
./setup_mysql.sh
```

### 运行服务

```bash
# 设置环境变量（可选）
export BINANCE_API_KEY="your_api_key"
export BINANCE_SECRET_KEY="your_secret_key"

# 运行服务
./dataFeeder
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
  api_key: ""  # 不需要API密钥就可以获取公共K线数据
  secret_key: ""

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
  level: "info"
  json_format: false
  output_path: "logs/dataFeeder.log"
```

### 交易对配置 (symbols.yaml)

```yaml
# 币种配置
groups:
  # 主交易对组
  main:
    symbols: []  # 将由程序自动获取前200个市值最大的币种
    intervals: ["15m", "4h", "1d"]  # 仅支持这三个时间周期
    start_times:
      minute: "2023-01-01T00:00:00Z"
      hour: "2023-01-01T00:00:00Z"
      day: "2023-01-01T00:00:00Z"
    enabled: true
    poll_intervals:
      "15m": 15m
      "4h": 4h
      "1d": 24h

# 全局设置
settings:
  max_symbols_per_batch: 20 # 每批处理的最大币种数
  discovery_enabled: false # 是否启用自动发现新币种
  discovery_interval: 24h # 自动发现新币种的间隔
  excluded_symbols: # 排除的币种
    - USDCUSDT
    - BUSDUSDT
    - TUSDUSDT
```

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

#### 删除指定时间范围内的数据

```
DELETE /api/v1/klines?symbol=BTCUSDT&interval=4h&start_time=2023-01-01T00:00:00Z&end_time=2023-01-31T23:59:59Z&confirm=true
```

参数说明：

- `symbol`: 交易对名称（必填）
- `interval`: 时间周期，仅支持 15m, 4h, 1d（必填）
- `start_time`: 开始时间，ISO8601格式（必填）
- `end_time`: 结束时间，ISO8601格式（必填）
- `confirm`: 必须设为true才会执行删除操作（必填）

### WebSocket API

连接WebSocket：

```
ws://localhost:8080/api/v1/ws
```

订阅K线数据：

```json
{
  "action": "subscribe",
  "symbol": "BTCUSDT",
  "interval": "15m"
}
```

## 数据流程

1. DataCollector从Binance API获取市值排名前200的加密货币列表
2. DataCollector从Binance API获取K线数据
3. 数据通过管道传输到DataProcessor进行清洗
4. 清洗后的数据存储到MySQL数据库
5. API服务器从数据库读取数据并提供给客户端
6. 新数据通过WebSocket实时推送给订阅的客户端

## 工具和脚本

系统提供了多个工具和脚本用于数据管理和问题诊断：

- `tools/checktables/main.go`: 检查数据库表结构
- `tools/data_continuity/main.go`: 检查数据连续性
- `tools/fix_data_gaps/main.go`: 修复数据缺口
- `tools/test_binance.go`: 测试Binance API连接
- `tools/test_connection.go`: 测试数据库连接

## 测试

### 运行测试

1. **安装测试依赖**：
   ```bash
   go get -t ./...
   ```

2. **运行所有测试**：
   ```bash
   go test -v ./...
   ```

3. **运行特定包的测试**：
   ```bash
   go test -v ./pkg/datacollector
   ```

4. **生成测试覆盖率报告**：
   ```bash
   go test -coverprofile=coverage.out ./...
   go tool cover -html=coverage.out
   ```

5. **测试API**
  ```
  go run tools/api_test/main.go http://localhost:8080
  ```
### 测试类型

1. **单元测试**：
   - 位于各个包的`_test.go`文件中
   - 测试单个函数或方法的功能
   - 使用mock对象隔离依赖

2. **API测试**：
   - 位于`tests/api_test.go`
   - 测试所有API端点的功能和响应
   - 使用`httptest`包模拟HTTP请求

3. **集成测试**：
   - 位于`tests/integration_test.go`
   - 测试多个组件的协同工作
   - 使用测试数据库和mock外部服务


### 持续集成

测试已集成到CI/CD流程中，每次提交都会自动运行所有测试。如果测试失败，构建将中止。


## 贡献指南

欢迎提交问题报告和功能请求。如果您想贡献代码，请遵循以下步骤：

1. Fork项目
2. 创建您的特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交您的更改 (`git commit -m 'Add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 打开Pull Request

## 许可证

本项目采用MIT许可证 - 详情请参阅LICENSE文件