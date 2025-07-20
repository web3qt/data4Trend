#!/bin/bash

# 快速1分钟数据收集重置脚本
echo "🚀 开始快速1分钟数据收集重置..."

# 设置环境变量 - 这是关键
export USE_INTERVAL_TABLES=true
export CONFIG_FILE=config/fast_1m_collection.yaml

echo "✅ 环境变量设置:"
echo "  USE_INTERVAL_TABLES=${USE_INTERVAL_TABLES}"
echo "  CONFIG_FILE=${CONFIG_FILE}"

# 检查配置文件是否存在
if [ ! -f "${CONFIG_FILE}" ]; then
    echo "❌ 配置文件不存在: ${CONFIG_FILE}"
    exit 1
fi

echo "📋 正在使用配置文件: ${CONFIG_FILE}"

# 停止现有进程
echo "🛑 停止现有的数据收集进程..."
pkill -f "data4trend"
pkill -f "main"
sleep 2

# 重置数据库 - 删除旧数据，创建新表结构
echo "🗄️  重置数据库..."
docker exec -i clickhouse-server clickhouse-client -d data4trend << 'EOF'
-- 删除旧的kline表（如果存在）
DROP TABLE IF EXISTS kline;

-- 删除物化视图（如果存在）
DROP VIEW IF EXISTS kline_5m;
DROP VIEW IF EXISTS kline_15m;
DROP VIEW IF EXISTS kline_1h;
DROP VIEW IF EXISTS kline_4h;
DROP VIEW IF EXISTS kline_1d;

-- 删除kline_raw表（如果存在）
DROP TABLE IF EXISTS kline_raw;

-- 创建kline_raw表用于存储1分钟数据
CREATE TABLE IF NOT EXISTS kline_raw (
    symbol String,
    open_time DateTime64(3),
    close_time DateTime64(3), 
    open Decimal(20, 8),
    high Decimal(20, 8),
    low Decimal(20, 8),
    close Decimal(20, 8),
    volume Decimal(20, 8),
    trade_count UInt64,
    taker_buy_base_volume Decimal(20, 8),
    taker_buy_quote_volume Decimal(20, 8)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time);

-- 创建物化视图进行自动聚合
-- 5分钟视图
CREATE MATERIALIZED VIEW kline_5m
ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
AS SELECT
    symbol,
    toStartOfInterval(open_time, INTERVAL 5 MINUTE) as open_time,
    toStartOfInterval(open_time, INTERVAL 5 MINUTE) + INTERVAL 5 MINUTE - INTERVAL 1 SECOND as close_time,
    argMin(open, open_time) as open,
    max(high) as high, 
    min(low) as low,
    argMax(close, open_time) as close,
    sum(volume) as volume,
    sum(trade_count) as trade_count,
    sum(taker_buy_base_volume) as taker_buy_base_volume,
    sum(taker_buy_quote_volume) as taker_buy_quote_volume
FROM kline_raw
GROUP BY symbol, toStartOfInterval(open_time, INTERVAL 5 MINUTE);

-- 15分钟视图  
CREATE MATERIALIZED VIEW kline_15m
ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
AS SELECT
    symbol,
    toStartOfInterval(open_time, INTERVAL 15 MINUTE) as open_time,
    toStartOfInterval(open_time, INTERVAL 15 MINUTE) + INTERVAL 15 MINUTE - INTERVAL 1 SECOND as close_time,
    argMin(open, open_time) as open,
    max(high) as high,
    min(low) as low, 
    argMax(close, open_time) as close,
    sum(volume) as volume,
    sum(trade_count) as trade_count,
    sum(taker_buy_base_volume) as taker_buy_base_volume,
    sum(taker_buy_quote_volume) as taker_buy_quote_volume
FROM kline_raw
GROUP BY symbol, toStartOfInterval(open_time, INTERVAL 15 MINUTE);

-- 1小时视图
CREATE MATERIALIZED VIEW kline_1h  
ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
AS SELECT
    symbol,
    toStartOfInterval(open_time, INTERVAL 1 HOUR) as open_time,
    toStartOfInterval(open_time, INTERVAL 1 HOUR) + INTERVAL 1 HOUR - INTERVAL 1 SECOND as close_time,
    argMin(open, open_time) as open,
    max(high) as high,
    min(low) as low,
    argMax(close, open_time) as close, 
    sum(volume) as volume,
    sum(trade_count) as trade_count,
    sum(taker_buy_base_volume) as taker_buy_base_volume,
    sum(taker_buy_quote_volume) as taker_buy_quote_volume
FROM kline_raw
GROUP BY symbol, toStartOfInterval(open_time, INTERVAL 1 HOUR);

-- 4小时视图
CREATE MATERIALIZED VIEW kline_4h
ENGINE = MergeTree() 
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
AS SELECT
    symbol,
    toStartOfInterval(open_time, INTERVAL 4 HOUR) as open_time,
    toStartOfInterval(open_time, INTERVAL 4 HOUR) + INTERVAL 4 HOUR - INTERVAL 1 SECOND as close_time,
    argMin(open, open_time) as open,
    max(high) as high,
    min(low) as low,
    argMax(close, open_time) as close,
    sum(volume) as volume,
    sum(trade_count) as trade_count,
    sum(taker_buy_base_volume) as taker_buy_base_volume,
    sum(taker_buy_quote_volume) as taker_buy_quote_volume
FROM kline_raw  
GROUP BY symbol, toStartOfInterval(open_time, INTERVAL 4 HOUR);

-- 1天视图
CREATE MATERIALIZED VIEW kline_1d
ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
AS SELECT
    symbol,
    toStartOfInterval(open_time, INTERVAL 1 DAY) as open_time,
    toStartOfInterval(open_time, INTERVAL 1 DAY) + INTERVAL 1 DAY - INTERVAL 1 SECOND as close_time,
    argMin(open, open_time) as open,
    max(high) as high,
    min(low) as low,
    argMax(close, open_time) as close,
    sum(volume) as volume,
    sum(trade_count) as trade_count,
    sum(taker_buy_base_volume) as taker_buy_base_volume,
    sum(taker_buy_quote_volume) as taker_buy_quote_volume
FROM kline_raw
GROUP BY symbol, toStartOfInterval(open_time, INTERVAL 1 DAY);
EOF

if [ $? -eq 0 ]; then
    echo "✅ 数据库重置成功"
else
    echo "❌ 数据库重置失败"
    exit 1
fi

# 验证表结构
echo "🔍 验证表结构..."
docker exec clickhouse-server clickhouse-client -d data4trend -q "SHOW TABLES"

# 清理日志
echo "🧹 清理日志..."
mkdir -p logs
rm -f logs/fast_1m_collection.log

# 编译程序
echo "🔨 编译程序..."
go build -o main cmd/main.go
if [ $? -eq 0 ]; then
    echo "✅ 编译成功"
else
    echo "❌ 编译失败"
    exit 1
fi

echo "🎯 环境准备完成！现在可以启动快速1分钟数据收集："
echo "   USE_INTERVAL_TABLES=true CONFIG_FILE=config/fast_1m_collection.yaml ./main"
echo ""
echo "⚠️  注意："
echo "   - 只收集5个主要加密货币的1分钟数据"
echo "   - 其他时间间隔通过物化视图自动生成"
echo "   - 数据存储在kline_raw表中"
echo "   - 建议先等待API限制解除(IP banned until 1752816773733)" 