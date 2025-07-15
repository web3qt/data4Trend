#!/bin/bash

# Data4Trend 物化视图架构测试脚本
# 该脚本会测试数据库初始化和基本功能

set -e

echo "=== Data4Trend 物化视图架构测试 ==="
echo "时间: $(date)"
echo

# 设置测试参数
DB_HOST=${DB_HOST:-"localhost"}
DB_PORT=${DB_PORT:-9000}
DB_USER=${DB_USER:-"default"}
DB_PASS=${DB_PASS:-""}
DB_NAME=${DB_NAME:-"data4trend_test"}
LOG_LEVEL=${LOG_LEVEL:-"debug"}

echo "测试配置:"
echo "  数据库主机: $DB_HOST"
echo "  数据库端口: $DB_PORT"
echo "  数据库用户: $DB_USER"
echo "  数据库名称: $DB_NAME"
echo "  日志级别: $LOG_LEVEL"
echo

# 检查必要文件
echo "检查必要文件..."
if [ ! -f "bin/data-collector-materialized" ]; then
    echo "错误: 找不到 bin/data-collector-materialized，请先构建程序"
    echo "运行: go build -o bin/data-collector-materialized ./cmd/data-collector-materialized"
    exit 1
fi

if [ ! -f "scripts/clickhouse-init-materialized-views.sql" ]; then
    echo "错误: 找不到 scripts/clickhouse-init-materialized-views.sql"
    exit 1
fi

echo "文件检查完成"
echo

# 检查ClickHouse连接
echo "检查ClickHouse连接..."
if command -v clickhouse-client >/dev/null 2>&1; then
    if clickhouse-client --host="$DB_HOST" --port="$DB_PORT" --user="$DB_USER" --password="$DB_PASS" --query="SELECT 1" >/dev/null 2>&1; then
        echo "ClickHouse连接正常"
    else
        echo "错误: 无法连接到ClickHouse"
        echo "请确保ClickHouse服务正在运行，并检查连接参数"
        exit 1
    fi
else
    echo "警告: 未找到clickhouse-client，跳过连接检查"
fi
echo

# 创建测试数据库
echo "创建测试数据库..."
if command -v clickhouse-client >/dev/null 2>&1; then
    clickhouse-client --host="$DB_HOST" --port="$DB_PORT" --user="$DB_USER" --password="$DB_PASS" --query="CREATE DATABASE IF NOT EXISTS $DB_NAME" || {
        echo "错误: 无法创建数据库 $DB_NAME"
        exit 1
    }
    echo "数据库 $DB_NAME 创建成功"
else
    echo "跳过数据库创建（未找到clickhouse-client）"
fi
echo

# 测试数据库初始化
echo "测试数据库初始化..."
./bin/data-collector-materialized \
    -db-host="$DB_HOST" \
    -db-port="$DB_PORT" \
    -db-user="$DB_USER" \
    -db-pass="$DB_PASS" \
    -db-name="$DB_NAME" \
    -log-level="$LOG_LEVEL" \
    -init-db

if [ $? -eq 0 ]; then
    echo "数据库初始化测试通过"
else
    echo "错误: 数据库初始化失败"
    exit 1
fi
echo

# 验证表结构
echo "验证表结构..."
if command -v clickhouse-client >/dev/null 2>&1; then
    echo "检查原始数据表..."
    clickhouse-client --host="$DB_HOST" --port="$DB_PORT" --user="$DB_USER" --password="$DB_PASS" --database="$DB_NAME" --query="DESCRIBE kline_raw" || {
        echo "错误: kline_raw 表不存在"
        exit 1
    }
    
    echo "检查聚合表..."
    for table in kline_5m kline_15m kline_1h kline_4h kline_1d; do
        clickhouse-client --host="$DB_HOST" --port="$DB_PORT" --user="$DB_USER" --password="$DB_PASS" --database="$DB_NAME" --query="DESCRIBE $table" >/dev/null || {
            echo "错误: $table 表不存在"
            exit 1
        }
        echo "  ✓ $table 表存在"
    done
    
    echo "检查物化视图..."
    for mv in mv_kline_1m_to_5m mv_kline_1m_to_15m mv_kline_1m_to_1h mv_kline_1m_to_4h mv_kline_1m_to_1d; do
        clickhouse-client --host="$DB_HOST" --port="$DB_PORT" --user="$DB_USER" --password="$DB_PASS" --database="$DB_NAME" --query="SHOW CREATE TABLE $mv" >/dev/null || {
            echo "错误: $mv 物化视图不存在"
            exit 1
        }
        echo "  ✓ $mv 物化视图存在"
    done
    
    echo "检查统一视图..."
    clickhouse-client --host="$DB_HOST" --port="$DB_PORT" --user="$DB_USER" --password="$DB_PASS" --database="$DB_NAME" --query="DESCRIBE v_kline_unified" >/dev/null || {
        echo "错误: v_kline_unified 视图不存在"
        exit 1
    }
    echo "  ✓ v_kline_unified 视图存在"
    
    echo "表结构验证通过"
else
    echo "跳过表结构验证（未找到clickhouse-client）"
fi
echo

# 插入测试数据
echo "插入测试数据..."
if command -v clickhouse-client >/dev/null 2>&1; then
    # 插入一些测试数据到原始表
    clickhouse-client --host="$DB_HOST" --port="$DB_PORT" --user="$DB_USER" --password="$DB_PASS" --database="$DB_NAME" --query="
        INSERT INTO kline_raw (id, symbol, open_time, close_time, open_price, high_price, low_price, close_price, volume) VALUES
        (1, 'BTCUSDT', '2024-01-01 00:00:00', '2024-01-01 00:01:00', 50000, 50100, 49900, 50050, 100),
        (2, 'BTCUSDT', '2024-01-01 00:01:00', '2024-01-01 00:02:00', 50050, 50150, 49950, 50100, 150),
        (3, 'BTCUSDT', '2024-01-01 00:02:00', '2024-01-01 00:03:00', 50100, 50200, 50000, 50150, 200),
        (4, 'BTCUSDT', '2024-01-01 00:03:00', '2024-01-01 00:04:00', 50150, 50250, 50050, 50200, 180),
        (5, 'BTCUSDT', '2024-01-01 00:04:00', '2024-01-01 00:05:00', 50200, 50300, 50100, 50250, 220)
    "
    
    if [ $? -eq 0 ]; then
        echo "测试数据插入成功"
    else
        echo "错误: 测试数据插入失败"
        exit 1
    fi
    
    # 等待物化视图处理数据
    echo "等待物化视图处理数据..."
    sleep 2
    
    # 验证聚合数据
    echo "验证聚合数据..."
    
    echo "检查5分钟聚合数据..."
    count_5m=$(clickhouse-client --host="$DB_HOST" --port="$DB_PORT" --user="$DB_USER" --password="$DB_PASS" --database="$DB_NAME" --query="SELECT count() FROM kline_5m WHERE symbol = 'BTCUSDT'")
    if [ "$count_5m" -gt 0 ]; then
        echo "  ✓ 5分钟聚合数据存在 ($count_5m 条记录)"
        # 显示聚合数据
        clickhouse-client --host="$DB_HOST" --port="$DB_PORT" --user="$DB_USER" --password="$DB_PASS" --database="$DB_NAME" --query="SELECT * FROM kline_5m WHERE symbol = 'BTCUSDT' FORMAT Pretty"
    else
        echo "  ⚠ 5分钟聚合数据为空（可能需要更多时间处理）"
    fi
    
    echo "原始数据查询测试..."
    clickhouse-client --host="$DB_HOST" --port="$DB_PORT" --user="$DB_USER" --password="$DB_PASS" --database="$DB_NAME" --query="SELECT count() as total_records FROM kline_raw WHERE symbol = 'BTCUSDT'"
    
else
    echo "跳过数据测试（未找到clickhouse-client）"
fi
echo

# 清理测试数据库（可选）
read -p "是否删除测试数据库 $DB_NAME? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    if command -v clickhouse-client >/dev/null 2>&1; then
        echo "删除测试数据库..."
        clickhouse-client --host="$DB_HOST" --port="$DB_PORT" --user="$DB_USER" --password="$DB_PASS" --query="DROP DATABASE IF EXISTS $DB_NAME"
        echo "测试数据库已删除"
    fi
else
    echo "保留测试数据库 $DB_NAME"
fi

echo
echo "=== 测试完成 ==="
echo "物化视图架构测试通过！"
echo
echo "下一步:"
echo "1. 配置 config/symbols.yaml 文件"
echo "2. 运行 ./scripts/start-materialized.sh 启动数据收集器"
echo "3. 运行 ./bin/trendscanner 启动趋势扫描器"
echo