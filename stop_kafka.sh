#!/bin/bash

# 停止KRaft模式Kafka架构的数据收集系统

echo "=== 停止KRaft Kafka架构的数据收集系统 ==="
echo

# 停止应用程序
echo "1. 停止应用程序..."
echo "  - 停止数据收集器..."
pkill -f "data4trend-collector" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "    数据收集器已停止"
else
    echo "    数据收集器未运行或已停止"
fi

echo "  - 停止数据校验与回补服务..."
pkill -f "validator" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "    数据校验服务已停止"
else
    echo "    数据校验服务未运行或已停止"
fi
echo

# 停止Kafka服务 (KRaft模式)
echo "2. 停止Kafka服务 (KRaft模式)..."
docker compose -f docker-compose-kafka.yml down

if [ $? -eq 0 ]; then
    echo "Kafka服务已停止"
else
    echo "警告: 停止Kafka服务时出现问题"
fi
echo

echo "=== KRaft Kafka系统已停止 ==="
echo
echo "如需清理Kafka数据，请运行:"
echo "docker compose -f docker-compose-kafka.yml down -v"
echo
echo "注意: KRaft模式无需ZooKeeper，数据清理更简单"