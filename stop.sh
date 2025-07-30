#!/bin/bash

# Data4Trend 停止脚本
# 停止Go程序和ClickHouse Docker容器

set -e

echo "🛑 停止Data4Trend数据收集器..."

# 停止Go程序进程
echo "📊 查找并停止Go程序进程..."
PROCESS_NAME="data4trend-collector"
PIDS=$(pgrep -f "$PROCESS_NAME" || true)

if [ -n "$PIDS" ]; then
    echo "🔍 找到进程: $PIDS"
    for PID in $PIDS; do
        echo "⏹️  停止进程 $PID..."
        kill -TERM $PID 2>/dev/null || true
        
        # 等待进程优雅退出
        for i in {1..10}; do
            if ! kill -0 $PID 2>/dev/null; then
                echo "✅ 进程 $PID 已停止"
                break
            fi
            echo "⏳ 等待进程 $PID 退出... ($i/10)"
            sleep 1
        done
        
        # 如果进程仍在运行，强制杀死
        if kill -0 $PID 2>/dev/null; then
            echo "💀 强制停止进程 $PID"
            kill -KILL $PID 2>/dev/null || true
        fi
    done
else
    echo "ℹ️  未找到运行中的Go程序进程"
fi

# 停止ClickHouse Docker容器
echo "🐳 停止ClickHouse Docker容器..."

# 检查docker-compose管理的ClickHouse容器
if docker-compose ps clickhouse 2>/dev/null | grep -q "Up"; then
    echo "⏹️  停止docker-compose管理的ClickHouse容器..."
    docker-compose stop clickhouse
    echo "✅ docker-compose ClickHouse容器已停止"
else
    echo "ℹ️  docker-compose ClickHouse容器未运行"
fi

# 检查独立的ClickHouse容器
CLICKHOUSE_CONTAINERS=$(docker ps --filter "name=clickhouse" --format "{{.Names}}" 2>/dev/null || true)
if [ -n "$CLICKHOUSE_CONTAINERS" ]; then
    echo "🔍 找到独立的ClickHouse容器: $CLICKHOUSE_CONTAINERS"
    for container in $CLICKHOUSE_CONTAINERS; do
        echo "⏹️  停止容器 $container..."
        docker stop "$container" >/dev/null 2>&1 || true
        echo "✅ 容器 $container 已停止"
    done
else
    echo "ℹ️  未找到独立的ClickHouse容器"
fi

# 可选：停止所有docker-compose服务
read -p "🤔 是否停止所有Docker服务? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "⏹️  停止所有Docker服务..."
    docker-compose down
    echo "✅ 所有Docker服务已停止"
fi

echo "🎯 停止完成！"
echo ""
echo "📊 检查状态:"
echo "  - Go程序进程: $(pgrep -f "$PROCESS_NAME" | wc -l | tr -d ' ') 个运行中"
echo "  - ClickHouse容器: $(docker-compose ps clickhouse 2>/dev/null | grep -c "Up" || echo "0") 个运行中"
echo "  - API服务器: $(curl -s http://localhost:8080/health >/dev/null 2>&1 && echo "运行中" || echo "已停止")"
echo ""
echo "🔄 重新启动请运行: ./start_go_simple.sh"