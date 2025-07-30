#!/bin/bash

# 数据库清理脚本
# 用于清理所有数据，重新启动项目

set -e

echo "=== 数据库清理脚本 ==="
echo "⚠️  警告：此操作将删除所有数据，无法恢复！"
echo

# 确认操作
read -p "确定要清理所有数据吗？(输入 'YES' 确认): " confirm
if [ "$confirm" != "YES" ]; then
    echo "❌ 操作已取消"
    exit 1
fi

echo "开始清理数据库..."
echo

# 检查ClickHouse容器是否运行
echo "1. 检查ClickHouse容器状态..."
if ! docker ps | grep -q clickhouse; then
    echo "⚠️  ClickHouse容器未运行，尝试启动..."
    if docker ps -a | grep -q clickhouse; then
        docker start clickhouse
        echo "✅ ClickHouse容器已启动"
        sleep 5
    else
        echo "❌ 未找到ClickHouse容器，请先启动容器"
        exit 1
    fi
else
    echo "✅ ClickHouse容器正在运行"
fi
echo

# 停止数据收集器（如果正在运行）
echo "2. 停止数据收集器..."
if pgrep -f "data4trend-collector" > /dev/null; then
    echo "发现运行中的数据收集器，正在停止..."
    pkill -f "data4trend-collector" || true
    sleep 2
    echo "✅ 数据收集器已停止"
else
    echo "✅ 数据收集器未运行"
fi
echo

# 清理数据库
echo "3. 清理数据库表..."

# 删除主数据表
echo "删除 klines_1m 表..."
docker exec clickhouse clickhouse-client --query="DROP TABLE IF EXISTS data4trend.klines_1m" 2>/dev/null || true

# 删除系统统计表
echo "删除 system_stats 表..."
docker exec clickhouse clickhouse-client --query="DROP TABLE IF EXISTS data4trend.system_stats" 2>/dev/null || true

# 删除WebSocket状态表
echo "删除 websocket_status 表..."
docker exec clickhouse clickhouse-client --query="DROP TABLE IF EXISTS data4trend.websocket_status" 2>/dev/null || true

# 删除数据质量指标表
echo "删除 data_quality_metrics 表..."
docker exec clickhouse clickhouse-client --query="DROP TABLE IF EXISTS data4trend.data_quality_metrics" 2>/dev/null || true

# 删除数据库（可选）
echo "删除 data4trend 数据库..."
docker exec clickhouse clickhouse-client --query="DROP DATABASE IF EXISTS data4trend" 2>/dev/null || true

echo "✅ 数据库表清理完成"
echo

# 重新初始化数据库
echo "4. 重新初始化数据库..."
if [ -f "scripts/init_database.sql" ]; then
    echo "使用初始化脚本重建数据库..."
    docker exec -i clickhouse clickhouse-client < scripts/init_database.sql
    echo "✅ 数据库重新初始化完成"
else
    echo "⚠️  未找到初始化脚本，请手动初始化数据库"
fi
echo

# 清理日志文件（可选）
echo "5. 清理日志文件..."
if [ -f "collector.log" ]; then
    rm -f collector.log
    echo "✅ 删除 collector.log"
fi

if [ -d "logs" ]; then
    rm -rf logs/*
    echo "✅ 清理 logs 目录"
fi
echo

# 验证清理结果
echo "6. 验证清理结果..."
sleep 2

# 检查数据库是否为空
record_count=$(docker exec clickhouse clickhouse-client --query="SELECT count() FROM data4trend.klines_1m" 2>/dev/null || echo "0")
echo "当前数据记录数: $record_count"

if [ "$record_count" = "0" ]; then
    echo "✅ 数据库清理验证成功"
else
    echo "⚠️  数据库可能未完全清理"
fi
echo

echo "=== 清理完成 ==="
echo "✅ 数据库已清理完成，可以重新启动项目"
echo
echo "📋 后续步骤:"
echo "   1. 启动数据收集器: ./start_go_simple.sh"
echo "   2. 检查服务状态: curl http://localhost:8080/health"
echo "   3. 查看数据统计: curl http://localhost:8080/api/v1/stats"
echo
echo "🎉 项目已准备好重新开始收集数据！"