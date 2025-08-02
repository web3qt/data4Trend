# Data4Trend 使用说明

## 架构概述

本项目采用分离式架构：
- **ClickHouse**: 独立的共享服务，多个项目可以共用
- **Kafka**: 项目专用服务，用于数据流处理
- **数据收集服务**: WebSocket实时数据收集
- **数据回填服务**: 历史数据验证和回填

## 快速开始

### 1. 启动ClickHouse（首次使用）

```bash
# 启动共享ClickHouse服务
./manage_clickhouse.sh start

# 检查状态
./manage_clickhouse.sh status
```

### 2. 启动所有服务

```bash
# 启动数据收集和回填服务（包含Kafka）
./start_services.sh
```

### 3. 单独启动数据收集服务

```bash
# 确保ClickHouse已启动
./manage_clickhouse.sh start

# 启动数据收集服务
./start_collector.sh
```

### 4. 单独启动数据回填服务

```bash
# 确保ClickHouse已启动
./manage_clickhouse.sh start

# 启动数据回填服务
./start_backfill.sh -validate-only  # 仅验证
./start_backfill.sh -symbol BTCUSDT -days 1  # 回填特定交易对
```

### 5. 停止服务

```bash
# 停止项目服务（Kafka + 应用服务）
./stop_services.sh

# 停止ClickHouse（谨慎使用，可能影响其他项目）
./manage_clickhouse.sh stop
```

## 服务管理

### ClickHouse管理

```bash
# 启动ClickHouse
./manage_clickhouse.sh start

# 停止ClickHouse
./manage_clickhouse.sh stop

# 重启ClickHouse
./manage_clickhouse.sh restart

# 检查状态
./manage_clickhouse.sh status
```

### 数据回填服务

```bash
# 仅执行数据验证
./start_backfill.sh -validate-only

# 回填特定交易对
./start_backfill.sh -symbol BTCUSDT -days 5

# 回填所有交易对
./start_backfill.sh -days 3

# 使用自定义配置文件
./start_backfill.sh -config config/config-high-performance.yaml -symbol ETHUSDT -days 1
```

## 健康检查

```bash
# 检查数据收集服务
curl http://localhost:8080/health

# 检查ClickHouse连接
curl http://localhost:8123/ping

# 检查Kafka连接
docker exec data4trend-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

## 日志查看

```bash
# 查看数据收集服务日志
tail -f logs/collector.log

# 查看数据回填服务日志
tail -f logs/backfill.log

# 查看Kafka日志
docker logs data4trend-kafka

# 查看ClickHouse日志
docker logs shared-clickhouse
```

## 配置说明

### ClickHouse连接信息
- **地址**: localhost:8123 (HTTP), localhost:9000 (Native)
- **数据库**: data4trend
- **用户名**: default
- **密码**: 123456

### Kafka连接信息
- **地址**: localhost:9092
- **主题**: binance_klines

## 故障排除

### ClickHouse连接失败
```bash
# 检查ClickHouse是否运行
./manage_clickhouse.sh status

# 重启ClickHouse
./manage_clickhouse.sh restart
```

### Kafka连接失败
```bash
# 检查Kafka容器状态
docker ps | grep kafka

# 重启Kafka
docker compose -f docker-compose.yml restart kafka
```

### 服务启动失败
```bash
# 检查日志
tail -f logs/collector.log
tail -f logs/backfill.log

# 重新编译
/usr/local/go/bin/go build -o bin/data4trend-collector cmd/collector/main.go
/usr/local/go/bin/go build -o bin/backfill-validator cmd/backfill-validator/main.go
```

## 注意事项

1. **ClickHouse共享**: ClickHouse作为独立服务，多个项目可以共用，停止前请确认没有其他项目在使用
2. **端口占用**: 确保8123、9000、9092、8080端口未被占用
3. **网络连接**: 确保能够访问Binance API和代理设置正确
4. **磁盘空间**: 确保有足够的磁盘空间存储数据

## 文件结构

```
data4Trend/
├── manage_clickhouse.sh          # ClickHouse管理脚本
├── start_services.sh             # 启动所有服务
├── start_collector.sh            # 启动数据收集服务
├── start_backfill.sh             # 启动数据回填服务
├── stop_services.sh              # 停止项目服务
├── docker-compose-clickhouse.yml # ClickHouse Docker配置
├── docker-compose.yml            # Kafka Docker配置
├── config/                       # 配置文件
├── logs/                         # 日志文件
└── bin/                          # 编译后的可执行文件
``` 