#!/bin/bash

# 一键重置数据库并启动按时间级别分表的数据采集服务
# 解决脏数据问题和类型转换问题

echo "🚀 数据4趋势 - 一键重置与启动"
echo "==============================================="
echo "  修复问题："
echo "  ✓ Float64/Decimal64 类型转换问题"
echo "  ✓ 脏数据清理"
echo "  ✓ 按时间级别分表架构"
echo "==============================================="

# 设置环境变量使用按时间级别分表
export USE_INTERVAL_TABLES=true

# 检查ClickHouse是否运行
echo "📦 检查ClickHouse状态..."
if ! docker ps | grep -q clickhouse; then
    echo "ClickHouse 容器未运行，正在启动..."
    docker-compose up -d clickhouse
    echo "等待 ClickHouse 启动完成..."
    sleep 15
else
    echo "✓ ClickHouse 已运行"
fi

# 强制重置数据库
echo ""
echo "🔄 重置数据库（清理脏数据）..."
docker exec data4trend-clickhouse-1 clickhouse-client --host 127.0.0.1 --port 9000 --multiquery < scripts/reset_database.sql
if [ $? -eq 0 ]; then
    echo "✓ 数据库重置成功"
else
    echo "✗ 数据库重置失败"
    exit 1
fi

# 创建新的表结构（使用 Float64 类型）
echo ""
echo "🏗️  创建按时间级别分表结构（Float64 类型）..."
docker exec data4trend-clickhouse-1 clickhouse-client --host 127.0.0.1 --port 9000 --multiquery < scripts/clickhouse-init-interval-tables.sql
if [ $? -eq 0 ]; then
    echo "✓ 表结构创建成功"
else
    echo "✗ 表结构创建失败"
    exit 1
fi

# 验证表结构
echo ""
echo "🔍 验证表结构..."
TABLES_COUNT=$(docker exec data4trend-clickhouse-1 clickhouse-client --host 127.0.0.1 --port 9000 --query "SELECT COUNT(*) FROM system.tables WHERE database = 'data4trend' AND name LIKE 'kline_%'" 2>/dev/null || echo "0")
echo "创建的表数量：$TABLES_COUNT"

if [ "$TABLES_COUNT" -lt 6 ]; then
    echo "⚠️  表创建可能有问题，但继续启动..."
else
    echo "✓ 表结构验证成功"
fi

# 编译项目
echo ""
echo "🔨 编译项目..."
if [ ! -f "main" ] || [ "cmd/main.go" -nt "main" ]; then
    go build -o main cmd/main.go
    if [ $? -ne 0 ]; then
        echo "✗ 编译失败"
        exit 1
    fi
    echo "✓ 编译成功"
else
    echo "✓ 项目已是最新版本"
fi

# 显示配置信息
echo ""
echo "📋 当前配置："
echo "- 存储模式：按时间级别分表"
echo "- 数据类型：Float64（修复转换问题）"
echo "- 环境变量：USE_INTERVAL_TABLES=true"
echo "- 支持时间级别：1m, 5m, 15m, 1h, 4h, 1d"
echo ""

# 启动服务
echo "🚀 启动数据采集服务..."
echo "端口：8080"
echo "使用 Ctrl+C 停止服务"
echo "日志级别：INFO"
echo ""

./main -config config/symbols.yaml -port 8080

echo ""
echo "服务已停止" 