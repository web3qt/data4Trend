#!/bin/bash

echo "🚀 使用代理启动 Data4Trend"
echo "=========================="

# 获取代理配置
PROXY_HOST=$(grep -A3 "proxy:" config/config.yaml | grep "host:" | awk '{print $2}' | tr -d '"')
PROXY_PORT=$(grep -A3 "proxy:" config/config.yaml | grep "port:" | awk '{print $2}' | tr -d '"')
PROXY_TYPE=$(grep -A3 "proxy:" config/config.yaml | grep "type:" | awk '{print $2}' | tr -d '"')

echo "📋 代理配置:"
echo "   类型: $PROXY_TYPE"
echo "   主机: $PROXY_HOST"
echo "   端口: $PROXY_PORT"

# 设置环境变量
export HTTP_PROXY="$PROXY_TYPE://$PROXY_HOST:$PROXY_PORT"
export HTTPS_PROXY="$PROXY_TYPE://$PROXY_HOST:$PROXY_PORT"
export ALL_PROXY="$PROXY_TYPE://$PROXY_HOST:$PROXY_PORT"

echo ""
echo "🔧 设置的环境变量:"
echo "   HTTP_PROXY=$HTTP_PROXY"
echo "   HTTPS_PROXY=$HTTPS_PROXY"
echo "   ALL_PROXY=$ALL_PROXY"

# 测试代理连接
echo ""
echo "🌐 测试代理连接..."
if curl --proxy "$PROXY_TYPE://$PROXY_HOST:$PROXY_PORT" --connect-timeout 10 -s "https://api.binance.com/api/v3/exchangeInfo" > /dev/null; then
    echo "✅ 代理连接成功"
else
    echo "❌ 代理连接失败，请检查代理服务"
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
echo "🚀 启动数据收集器 (使用代理)..."
./bin/data4trend-collector --config=config/config.yaml --log-level=info &

# 等待应用程序启动
echo "⏳ 等待应用程序启动..."
sleep 10

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
echo "3. 查看日志: docker logs -f data4trend-collector"
echo ""
echo "💡 预期效果:"
echo "   - 监控400+个交易对"
echo "   - 每10秒写入100-400条数据"
echo "   - 数据量增加40倍以上" 