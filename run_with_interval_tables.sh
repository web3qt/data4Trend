#!/bin/bash

# 运行使用按时间级别分表的数据采集服务
# 作者：web3qt/data4Trend
# 版本：v1.0

echo "==============================================="
echo "  数据采集服务 - 按时间级别分表模式"
echo "==============================================="

# 检查是否已创建新表结构
echo "检查数据库表结构..."

# 设置环境变量使用按时间级别分表
export USE_INTERVAL_TABLES=true

# 检查ClickHouse是否运行
if ! docker ps | grep -q clickhouse; then
    echo "ClickHouse 容器未运行，正在启动..."
    docker-compose up -d clickhouse
    echo "等待 ClickHouse 启动完成..."
    sleep 10
fi

# 询问是否重置数据库
echo "检测到可能存在的脏数据问题..."
echo "是否要完全重置数据库（删除所有现有数据）？[y/N]"
read -r reset_response
if [[ "$reset_response" =~ ^[Yy]$ ]]; then
    echo "🔄 重置数据库中..."
    docker exec data4trend-clickhouse-1 clickhouse-client --host 127.0.0.1 --port 9000 --multiquery < scripts/reset_database.sql
    if [ $? -eq 0 ]; then
        echo "✓ 数据库重置成功"
    else
        echo "✗ 数据库重置失败"
        exit 1
    fi
fi

# 创建按时间级别分表结构
echo "创建按时间级别分表结构..."
docker exec data4trend-clickhouse-1 clickhouse-client --host 127.0.0.1 --port 9000 --multiquery < scripts/clickhouse-init-interval-tables.sql
if [ $? -eq 0 ]; then
    echo "✓ 表结构创建成功"
else
    echo "✗ 表结构创建失败，请检查ClickHouse连接"
    exit 1
fi

# 检查是否需要数据迁移
echo "检查是否需要进行数据迁移..."
KLINE_COUNT=$(docker exec data4trend-clickhouse-1 clickhouse-client --host 127.0.0.1 --port 9000 --query "SELECT COUNT(*) FROM data4trend.kline" 2>/dev/null || echo "0")
KLINE_1H_COUNT=$(docker exec data4trend-clickhouse-1 clickhouse-client --host 127.0.0.1 --port 9000 --query "SELECT COUNT(*) FROM data4trend.kline_1h" 2>/dev/null || echo "0")

if [ "$KLINE_COUNT" -gt 0 ] && [ "$KLINE_1H_COUNT" -eq 0 ]; then
    echo "检测到现有数据需要迁移到新表结构..."
    echo "是否立即进行数据迁移？[y/N]"
    read -r response
    if [[ "$response" =~ ^[Yy]$ ]]; then
        echo "开始数据迁移..."
        docker exec data4trend-clickhouse-1 clickhouse-client --host 127.0.0.1 --port 9000 --multiquery < scripts/migrate_to_interval_tables.sql
        if [ $? -eq 0 ]; then
            echo "✓ 数据迁移完成"
        else
            echo "✗ 数据迁移失败"
            exit 1
        fi
    else
        echo "跳过数据迁移，直接启动服务"
    fi
else
    echo "✓ 表结构已准备就绪"
fi

# 显示当前配置
echo ""
echo "当前配置："
echo "- 存储模式：按时间级别分表"
echo "- 环境变量：USE_INTERVAL_TABLES=true"
echo "- 数据库：$(docker exec data4trend-clickhouse-1 clickhouse-client --host 127.0.0.1 --port 9000 --query "SELECT currentDatabase()" 2>/dev/null || echo "未连接")"

# 启动服务
echo ""
echo "启动数据采集服务..."

# 编译并运行
if [ ! -f "main" ] || [ "cmd/main.go" -nt "main" ]; then
    echo "编译项目..."
    go build -o main cmd/main.go
    if [ $? -ne 0 ]; then
        echo "✗ 编译失败"
        exit 1
    fi
fi

# 运行程序
echo "启动服务，端口：8080"
echo "使用 Ctrl+C 停止服务"
echo ""

./main -config config/symbols.yaml -port 8080

echo ""
echo "服务已停止" 