#!/bin/bash

# Go版本的Data4Trend启动脚本
# 简化版本，适用于已经配置好环境的情况

set -e

echo "🚀 启动Go版本的Data4Trend数据收集器..."

# 检查Go程序是否已编译
if [ ! -f "bin/data4trend-collector" ]; then
    echo "📦 编译Go程序..."
    mkdir -p bin
    go build -o bin/data4trend-collector cmd/collector/main.go
    echo "✅ 编译完成"
fi

# 设置环境变量
export HTTP_PROXY=http://127.0.0.1:7890
export HTTPS_PROXY=http://127.0.0.1:7890

echo "🔧 代理设置: $HTTP_PROXY"
echo "📊 配置文件: config/config_go_simple.yaml"
echo "🌐 API服务器: http://localhost:8080"
echo "📈 健康检查: http://localhost:8080/health"
echo "📊 统计信息: http://localhost:8080/api/v1/stats"
echo "🔌 WebSocket统计: http://localhost:8080/api/v1/websocket/stats"
echo ""
echo "按 Ctrl+C 停止程序"
echo "==========================================="

# 启动程序
./bin/data4trend-collector --config=config/config_go_simple.yaml --log-level=info