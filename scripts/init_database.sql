-- 简化的ClickHouse数据库初始化脚本
-- 专门用于WebSocket 1分钟K线数据收集

-- 创建数据库
CREATE DATABASE IF NOT EXISTS data4trend;

-- 使用数据库
USE data4trend;

-- 创建1分钟K线数据表
CREATE TABLE IF NOT EXISTS klines_1m (
    symbol String,
    open_time Int64,
    close_time Int64,
    open String,
    high String,
    low String,
    close String,
    volume String,
    created_at DateTime DEFAULT now(),
    version UInt32 DEFAULT 1
) ENGINE = ReplacingMergeTree(version)
ORDER BY (symbol, open_time)
PARTITION BY toYYYYMM(toDateTime(open_time / 1000));

-- ClickHouse MergeTree表的ORDER BY已经提供了索引，无需手动创建

-- 创建系统监控表
CREATE TABLE IF NOT EXISTS system_stats (
    timestamp DateTime DEFAULT now(),
    active_symbols UInt32,
    total_records UInt64,
    websocket_connections UInt32,
    data_collection_rate Float64,  -- 每秒收集的数据条数
    memory_usage_mb Float64,
    disk_usage_mb Float64,
    error_count UInt32,
    last_error String DEFAULT ''
) ENGINE = MergeTree()
ORDER BY timestamp
TTL timestamp + INTERVAL 30 DAY;  -- 保留30天的监控数据

-- 创建WebSocket连接状态表
CREATE TABLE IF NOT EXISTS websocket_status (
    symbol String,
    connection_status Enum8('connected'=1, 'disconnected'=2, 'error'=3),
    last_data_time DateTime,
    reconnect_count UInt32 DEFAULT 0,
    error_message String DEFAULT '',
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY symbol;

-- 创建数据质量监控表
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    symbol String,
    date Date,
    expected_records UInt32,  -- 预期记录数（每天1440条1分钟数据）
    actual_records UInt32,    -- 实际记录数
    missing_records UInt32,   -- 缺失记录数
    duplicate_records UInt32, -- 重复记录数
    data_completeness_rate Float64, -- 数据完整性比率
    last_updated DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(last_updated)
ORDER BY (symbol, date)
PARTITION BY toYYYYMM(date)
TTL date + INTERVAL 30 DAY;

-- 插入初始系统统计记录
INSERT INTO system_stats (active_symbols, total_records, websocket_connections, data_collection_rate) 
VALUES (0, 0, 0, 0.0);

-- 显示表创建结果
SHOW TABLES;

-- 显示klines_1m表结构
DESCRIBE TABLE klines_1m; 