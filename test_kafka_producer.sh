#!/bin/bash

# 测试Kafka生产者修复效果

echo "=== 测试Kafka生产者修复效果 ==="
echo

echo "1. 检查配置文件更新..."
grep -A 10 "producer:" config/config.yaml
echo

echo "2. 编译应用程序..."
go build -o bin/data4trend-collector cmd/collector/main.go
if [ $? -eq 0 ]; then
    echo "✅ 编译成功"
else
    echo "❌ 编译失败"
    exit 1
fi
echo

echo "3. 检查Kafka服务状态..."
docker ps | grep kafka
if [ $? -eq 0 ]; then
    echo "✅ Kafka服务运行中"
else
    echo "⚠️  Kafka服务未运行，请先启动Kafka"
    echo "运行: docker compose -f docker-compose-kafka.yml up -d"
fi
echo

echo "=== 修复说明 ==="
echo "问题: kafka producer input channel is full"
echo "原因: Kafka生产者输入通道缓冲区太小，无法处理高频消息"
echo "解决方案:"
echo "  1. 增加通道缓冲区大小 (256 -> 2048)"
echo "  2. 优化批量刷新设置 (16KB)"
echo "  3. 添加发送超时机制 (5秒)"
echo "  4. 改进错误处理和重试逻辑"
echo
echo "配置参数:"
echo "  - channel_buffer_size: 2048"
echo "  - flush_bytes: 16384"
echo "  - send_timeout: 5s"
echo
echo "现在可以重新启动服务测试修复效果"