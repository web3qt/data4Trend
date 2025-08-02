#!/bin/bash

echo "🚀 Starting Data4Trend with Optimized Configuration"
echo "=================================================="

# 检查依赖服务
echo "1. Checking dependencies..."

# 检查ClickHouse
if ! curl -s "http://localhost:8123/ping" > /dev/null; then
    echo "❌ ClickHouse is not running. Please start it first."
    echo "   Run: docker-compose up -d clickhouse"
    exit 1
fi
echo "✅ ClickHouse is running"

# 检查Kafka
if ! nc -z localhost 9092 2>/dev/null; then
    echo "❌ Kafka is not running. Please start it first."
    echo "   Run: docker-compose up -d kafka"
    exit 1
fi
echo "✅ Kafka is running"

# 清理旧日志
echo ""
echo "2. Cleaning up old logs..."
rm -f logs/collector.log
mkdir -p logs

# 设置环境变量
export GOMAXPROCS=4  # 限制CPU使用
export GODEBUG=gctrace=1  # 启用GC跟踪

# 启动收集器
echo ""
echo "3. Starting collector with optimized settings..."
echo "   - Batch size: 50 records"
echo "   - Batch timeout: 2 seconds"
echo "   - Retry interval: 1 second"
echo ""

# 在后台启动收集器
./bin/data4trend-collector -config config/config.yaml -log-level info > logs/collector.log 2>&1 &
COLLECTOR_PID=$!

echo "✅ Collector started with PID: $COLLECTOR_PID"

# 等待服务启动
echo ""
echo "4. Waiting for services to start..."
sleep 10

# 检查服务状态
echo ""
echo "5. Checking service status..."

# 检查API服务器
if curl -s http://localhost:8080/health > /dev/null; then
    echo "✅ API server is responding"
else
    echo "❌ API server is not responding"
fi

# 启动性能监控
echo ""
echo "6. Starting performance monitoring..."
echo "   Press Ctrl+C to stop monitoring and shutdown"
echo ""

# 在后台启动性能监控
./monitor_performance.sh &
MONITOR_PID=$!

# 等待用户中断
trap 'cleanup' INT TERM

cleanup() {
    echo ""
    echo "🛑 Shutting down services..."
    
    # 停止监控
    kill $MONITOR_PID 2>/dev/null
    
    # 停止收集器
    kill $COLLECTOR_PID 2>/dev/null
    
    # 等待进程结束
    wait $COLLECTOR_PID 2>/dev/null
    
    echo "✅ Services stopped"
    exit 0
}

# 显示实时日志
echo ""
echo "7. Real-time logs (Ctrl+C to stop):"
echo "====================================="
tail -f logs/collector.log 