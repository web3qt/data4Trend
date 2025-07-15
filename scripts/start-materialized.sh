#!/bin/bash

# Data4Trend 物化视图架构启动脚本
# 该脚本会初始化数据库表结构并启动数据收集器

set -e

echo "=== Data4Trend 物化视图架构启动脚本 ==="
echo "时间: $(date)"
echo

# 检查必要的文件
echo "检查必要文件..."
if [ ! -f "scripts/clickhouse-init-materialized-views.sql" ]; then
    echo "错误: 找不到 scripts/clickhouse-init-materialized-views.sql"
    exit 1
fi

if [ ! -f "config/symbols.yaml" ]; then
    echo "警告: 找不到 config/symbols.yaml，将使用默认配置"
fi

echo "文件检查完成"
echo

# 设置默认参数
DB_HOST=${DB_HOST:-"localhost"}
DB_PORT=${DB_PORT:-9000}
DB_USER=${DB_USER:-"default"}
DB_PASS=${DB_PASS:-""}
DB_NAME=${DB_NAME:-"data4trend"}
LOG_LEVEL=${LOG_LEVEL:-"info"}
CONFIG_PATH=${CONFIG_PATH:-"config/symbols.yaml"}

echo "配置参数:"
echo "  数据库主机: $DB_HOST"
echo "  数据库端口: $DB_PORT"
echo "  数据库用户: $DB_USER"
echo "  数据库名称: $DB_NAME"
echo "  日志级别: $LOG_LEVEL"
echo "  配置文件: $CONFIG_PATH"
echo

# 构建程序（如果需要）
if [ "$BUILD" = "true" ] || [ ! -f "bin/data-collector-materialized" ]; then
    echo "构建数据收集器..."
    mkdir -p bin
    go build -o bin/data-collector-materialized ./cmd/data-collector-materialized
    if [ $? -ne 0 ]; then
        echo "错误: 构建失败"
        exit 1
    fi
    echo "构建完成"
    echo
fi

# 检查ClickHouse连接
echo "检查ClickHouse连接..."
if command -v clickhouse-client >/dev/null 2>&1; then
    if clickhouse-client --host="$DB_HOST" --port="$DB_PORT" --user="$DB_USER" --password="$DB_PASS" --query="SELECT 1" >/dev/null 2>&1; then
        echo "ClickHouse连接正常"
    else
        echo "警告: 无法连接到ClickHouse，程序可能会失败"
    fi
else
    echo "警告: 未找到clickhouse-client，跳过连接检查"
fi
echo

# 初始化数据库（如果指定）
if [ "$INIT_DB" = "true" ]; then
    echo "初始化数据库表结构..."
    ./bin/data-collector-materialized \
        -config="$CONFIG_PATH" \
        -db-host="$DB_HOST" \
        -db-port="$DB_PORT" \
        -db-user="$DB_USER" \
        -db-pass="$DB_PASS" \
        -db-name="$DB_NAME" \
        -log-level="$LOG_LEVEL" \
        -init-db
    
    if [ $? -eq 0 ]; then
        echo "数据库初始化完成"
    else
        echo "错误: 数据库初始化失败"
        exit 1
    fi
    echo
fi

# 启动数据收集器
echo "启动数据收集器（物化视图架构）..."
echo "按 Ctrl+C 停止程序"
echo

exec ./bin/data-collector-materialized \
    -config="$CONFIG_PATH" \
    -db-host="$DB_HOST" \
    -db-port="$DB_PORT" \
    -db-user="$DB_USER" \
    -db-pass="$DB_PASS" \
    -db-name="$DB_NAME" \
    -log-level="$LOG_LEVEL"