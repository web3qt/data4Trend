# KRaft Kafka架构说明文档

## 架构概述

本项目已完成从直接写入ClickHouse到基于KRaft模式Kafka的流式处理架构的重构。新架构使用Kafka KRaft模式（无ZooKeeper依赖），提供了更好的可扩展性、容错性和数据处理能力，同时简化了部署和维护。

## 架构组件

### 1. 数据流架构

```
Binance WebSocket API
        ↓
   WebSocket客户端 (gorilla/websocket)
        ↓
   Kafka生产者 (Sarama)
        ↓
     Kafka主题 (binance_klines)
        ↓
   Kafka消费者 (Sarama)
        ↓
   批量写入器 (每分钟批量)
        ↓
   ClickHouse数据库
```

### 2. 核心组件

#### WebSocket客户端 (`pkg/websocket/client.go`)

- 使用 `gorilla/websocket` 连接Binance实时K线API
- 接收实时K线数据并发送到Kafka
- 支持自动重连和错误处理

#### Kafka生产者 (`pkg/kafka/producer.go`)

- 使用 `IBM/sarama` 客户端
- 支持批量发送和压缩
- 异步处理成功和错误消息

#### Kafka消费者 (`pkg/kafka/consumer.go`)

- 消费者组模式，支持水平扩展
- 自动提交偏移量
- 支持重平衡和故障恢复

#### 批量写入器 (`pkg/batchwriter/writer.go`)

- 缓冲K线数据并批量写入ClickHouse
- 支持基于大小和时间的批量触发
- 重试机制和错误处理

## 配置说明

### Kafka配置

```yaml
kafka:
  brokers:
    - "localhost:9092"
  topic: "binance_klines"
  producer:
    batch_size: 100
    batch_timeout: "1s"
    compression: "snappy"
    max_message_bytes: 1000000
  consumer:
    group_id: "data4trend_consumer"
    auto_offset_reset: "latest"
    session_timeout: "30s"
    heartbeat_interval: "3s"
```

### 批量写入配置

```yaml
batch_writer:
  batch_size: 1000
  batch_timeout: "60s"
  max_retries: 3
  retry_interval: "5s"
```

## 部署和运行

### 1. 启动系统 (KRaft模式)

```bash
# 启动KRaft模式Kafka和应用程序
./start_with_kafka.sh
```

**KRaft模式优势:**
- 无需ZooKeeper，简化部署
- 启动速度更快 (约20秒 vs 30秒)
- 更好的元数据一致性
- 减少运维复杂度

### 2. 停止系统

```bash
# 停止所有服务
./stop_kafka.sh
```

### 3. 手动操作

```bash
# 仅启动KRaft模式Kafka服务
docker compose -f docker-compose-kafka.yml up -d

# 仅启动应用程序
./bin/data4trend-collector --config=config/config.yaml --log-level=info

# 停止Kafka服务
docker compose -f docker-compose-kafka.yml down
```

## 监控和管理

### 1. 服务端点

- **API服务器**: <http://localhost:8080>
- **Kafka UI**: <http://localhost:8090>
- **健康检查**: <http://localhost:8080/health>

### 2. Kafka主题管理

```bash
# 查看主题列表
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# 查看主题详情
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic binance_klines

# 查看消费者组状态
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group data4trend_consumer
```

### 3. 性能监控

```bash
# 查看生产者统计
curl http://localhost:8080/api/v1/kafka/producer/stats

# 查看消费者统计
curl http://localhost:8080/api/v1/kafka/consumer/stats

# 查看批量写入统计
curl http://localhost:8080/api/v1/batch/stats
```

## 架构优势

### 1. KRaft模式优势

- **无ZooKeeper依赖**: 简化架构，减少组件数量
- **更快启动**: 元数据管理更高效，启动时间减少33%
- **更好一致性**: 使用Raft协议保证元数据一致性
- **简化运维**: 减少故障点，降低运维复杂度
- **更好扩展**: 控制器和代理角色分离，支持独立扩展

### 2. 可扩展性

- Kafka支持水平扩展
- 消费者组支持多实例并行处理
- 批量写入提高数据库写入效率

### 3. 容错性

- Kafka提供数据持久化和副本机制
- 消费者支持自动重平衡
- 批量写入器支持重试机制
- KRaft模式提供更强的元数据一致性保证

### 4. 性能优化

- 异步处理减少延迟
- 批量操作提高吞吐量
- 压缩减少网络传输
- KRaft模式减少元数据操作延迟

### 5. 运维友好

- 组件解耦，便于独立扩展
- 丰富的监控指标
- 支持热重启和滚动更新
- 无需管理ZooKeeper集群

## 故障排除

### 1. Kafka连接问题

```bash
# 检查Kafka服务状态
docker-compose -f docker-compose-kafka.yml ps

# 查看Kafka日志
docker logs kafka
```

### 2. 消费延迟问题

```bash
# 检查消费者组延迟
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group data4trend_consumer
```

### 3. 批量写入问题

```bash
# 查看批量写入统计
curl http://localhost:8080/api/v1/batch/stats

# 检查ClickHouse连接
curl http://localhost:8080/health
```

## 性能调优

### 1. Kafka调优

- 调整分区数量以支持更多并发消费者
- 优化批量大小和超时时间
- 选择合适的压缩算法

### 2. 批量写入调优

- 根据数据量调整批量大小
- 优化批量超时时间
- 调整重试策略

### 3. 系统资源调优

- 增加JVM堆内存（Kafka）
- 调整Go程序的GOMAXPROCS
- 优化ClickHouse配置

## 版本兼容性

- **Kafka**: 2.8+
- **Go**: 1.19+
- **ClickHouse**: 21.8+
- **Docker**: 20.10+
- **Docker Compose**: 2.0+
