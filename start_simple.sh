#!/bin/bash

echo "🚀 简化启动 Data4Trend (带代理)"
echo "================================"

# 直接设置代理配置
export HTTP_PROXY="socks5://127.0.0.1:7890"
export HTTPS_PROXY="socks5://127.0.0.1:7890"
export ALL_PROXY="socks5://127.0.0.1:7890"

echo "🔧 设置的环境变量:"
echo "   HTTP_PROXY=$HTTP_PROXY"
echo "   HTTPS_PROXY=$HTTPS_PROXY"
echo "   ALL_PROXY=$ALL_PROXY"

# 测试代理连接
echo ""
echo "🌐 测试代理连接..."
if curl --proxy socks5://127.0.0.1:7890 --connect-timeout 10 -s "https://api.binance.com/api/v3/exchangeInfo" > /dev/null; then
    echo "✅ 代理连接成功"
else
    echo "❌ 代理连接失败"
    exit 1
fi

# 停止现有服务
echo ""
echo "⏹️  停止现有服务..."
./stop_kafka.sh 2>/dev/null || true
pkill -f "data4trend-collector" 2>/dev/null || true
sleep 3

# 重新编译
echo ""
echo "🔨 重新编译程序..."
go build -o bin/data4trend-collector cmd/collector/main.go

if [ $? -ne 0 ]; then
    echo "❌ 编译失败"
    exit 1
fi

# 启动Kafka
echo ""
echo "🚀 启动Kafka服务..."
docker compose -f docker-compose-kafka.yml up -d

# 等待Kafka启动
echo "⏳ 等待Kafka启动..."
sleep 10

# 启动应用程序
echo ""
echo "🚀 启动数据收集器..."
./bin/data4trend-collector --config=config/config.yaml --log-level=info &

# 等待应用程序启动
echo "⏳ 等待应用程序启动..."
sleep 15

# 检查服务状态
echo ""
echo "🔍 检查服务状态..."

# 检查API服务器
if curl -s http://localhost:8080/health > /dev/null; then
    echo "✅ API服务器启动成功"
else
    echo "❌ API服务器启动失败"
fi

# 检查数据库连接
if curl -s -u default:123456 "http://localhost:8123" --data-binary "SELECT 1" > /dev/null; then
    echo "✅ ClickHouse连接成功"
else
    echo "❌ ClickHouse连接失败"
fi

echo ""
echo "🎯 系统已启动，现在可以:"
echo "1. 监控性能: ./monitor_performance.sh"
echo "2. 检查状态: ./check_optimization.sh"
echo "3. 查看日志: tail -f logs/app.log"
echo ""
echo "💡 预期效果:"
echo "   - 监控400+个交易对"
echo "   - 每10秒写入100-400条数据"
echo "   - 数据量增加40倍以上" 