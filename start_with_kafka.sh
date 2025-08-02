#!/bin/bash

# 启动KRaft模式Kafka和数据收集器

echo "=== 启动KRaft Kafka架构的数据收集系统 ==="
echo

# 检查Docker是否运行
if ! docker info > /dev/null 2>&1; then
    echo "错误: Docker未运行，请先启动Docker"
    exit 1
fi

# 启动Kafka服务 (KRaft模式)
echo "1. 启动Kafka服务 (KRaft模式)..."
docker compose -f docker-compose-kafka.yml up -d

if [ $? -ne 0 ]; then
    echo "错误: 启动Kafka服务失败"
    exit 1
fi

echo "Kafka服务启动成功"
echo

# 等待Kafka服务完全启动 (KRaft模式启动更快)
echo "2. 等待Kafka服务完全启动..."
sleep 20

# 检查Kafka是否可用
echo "3. 检查Kafka连接..."
for i in {1..8}; do
    if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
        echo "Kafka连接成功"
        break
    fi
    echo "等待Kafka启动... ($i/8)"
    sleep 3
done

# 创建Kafka主题
echo "4. 创建Kafka主题..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic binance_klines --partitions 3 --replication-factor 1 --if-not-exists

if [ $? -eq 0 ]; then
    echo "Kafka主题创建成功"
else
    echo "警告: Kafka主题创建失败，可能已存在"
fi
echo

# 编译应用程序
echo "5. 编译应用程序..."
echo "  - 编译数据收集器..."
go build -o bin/data4trend-collector cmd/collector/main.go

if [ $? -ne 0 ]; then
    echo "错误: 数据收集器编译失败"
    exit 1
fi

echo "  - 编译数据校验与回补服务..."
go build -o bin/validator cmd/validator/main.go

if [ $? -ne 0 ]; then
    echo "错误: 数据校验服务编译失败"
    exit 1
fi

echo "编译成功"
echo

# 启动应用程序
echo "6. 启动服务..."
echo "KRaft架构说明:"
echo "  - Kafka运行在KRaft模式 (无ZooKeeper依赖)"
echo "  - WebSocket客户端 -> Kafka生产者 -> Kafka主题"
echo "  - Kafka消费者 -> 批量写入器 -> ClickHouse"
echo "  - 数据校验与回补服务自动检测和修复数据缺口"
echo "  - API服务器: http://localhost:8080"
echo "  - Kafka UI: http://localhost:8090"
echo

# 启动数据校验与回补服务（后台运行）
echo "启动数据校验与回补服务..."
./bin/validator --config=config/config.yaml --log-level=info &
VALIDATOR_PID=$!
echo "数据校验服务已启动 (PID: $VALIDATOR_PID)"

# 等待一下确保validator服务启动
sleep 2

echo "启动数据收集器..."
echo "按Ctrl+C停止所有服务"
echo

# 设置信号处理，确保停止时同时停止两个服务
trap 'echo "\n正在停止服务..."; kill $VALIDATOR_PID 2>/dev/null; exit' INT TERM

# 启动数据收集器（前台运行）
./bin/data4trend-collector --config=config/config.yaml --log-level=info