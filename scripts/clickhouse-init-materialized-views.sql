-- ClickHouse initialization script with Materialized Views (Best Practice)
-- 基于ClickHouse最佳实践：单一事实表 + 物化视图自动聚合

-- Create database
CREATE DATABASE IF NOT EXISTS data4trend;

-- Use the database
USE data4trend;

-- ========================================
-- 1. 单一事实表：存储最细粒度数据（1分钟）
-- ========================================
CREATE TABLE IF NOT EXISTS kline_raw
(
    id UInt64,
    symbol LowCardinality(String),
    open_time DateTime64(3),
    close_time DateTime64(3),
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    volume Float64,
    created_at DateTime64(3) DEFAULT now64(),
    updated_at DateTime64(3) DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
SETTINGS 
    index_granularity = 8192,
    allow_nullable_key = 0;

-- 为原始表添加索引
ALTER TABLE kline_raw ADD INDEX IF NOT EXISTS bloom_symbol symbol TYPE bloom_filter(0.01) GRANULARITY 1;

-- ========================================
-- 2. 目标表：存储聚合后的数据
-- ========================================

-- 5分钟聚合表
CREATE TABLE IF NOT EXISTS kline_5m
(
    symbol LowCardinality(String),
    open_time DateTime64(3),
    close_time DateTime64(3),
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    volume Float64,
    created_at DateTime64(3) DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;

-- 15分钟聚合表
CREATE TABLE IF NOT EXISTS kline_15m
(
    symbol LowCardinality(String),
    open_time DateTime64(3),
    close_time DateTime64(3),
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    volume Float64,
    created_at DateTime64(3) DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;

-- 1小时聚合表
CREATE TABLE IF NOT EXISTS kline_1h
(
    symbol LowCardinality(String),
    open_time DateTime64(3),
    close_time DateTime64(3),
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    volume Float64,
    created_at DateTime64(3) DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;

-- 4小时聚合表
CREATE TABLE IF NOT EXISTS kline_4h
(
    symbol LowCardinality(String),
    open_time DateTime64(3),
    close_time DateTime64(3),
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    volume Float64,
    created_at DateTime64(3) DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;

-- 1天聚合表
CREATE TABLE IF NOT EXISTS kline_1d
(
    symbol LowCardinality(String),
    open_time DateTime64(3),
    close_time DateTime64(3),
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    volume Float64,
    created_at DateTime64(3) DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;

-- ========================================
-- 3. 物化视图：自动聚合数据
-- ========================================

-- 1分钟 -> 5分钟聚合物化视图
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kline_1m_to_5m
TO kline_5m
AS SELECT
    symbol,
    toStartOfInterval(open_time, INTERVAL 5 minute) as open_time,
    toStartOfInterval(open_time, INTERVAL 5 minute) + INTERVAL 5 minute - INTERVAL 1 second as close_time,
    argMin(open_price, open_time) as open_price,
    max(high_price) as high_price,
    min(low_price) as low_price,
    argMax(close_price, open_time) as close_price,
    sum(volume) as volume,
    now64() as created_at
FROM kline_raw
GROUP BY symbol, toStartOfInterval(open_time, INTERVAL 5 minute);

-- 1分钟 -> 15分钟聚合物化视图
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kline_1m_to_15m
TO kline_15m
AS SELECT
    symbol,
    toStartOfInterval(open_time, INTERVAL 15 minute) as open_time,
    toStartOfInterval(open_time, INTERVAL 15 minute) + INTERVAL 15 minute - INTERVAL 1 second as close_time,
    argMin(open_price, open_time) as open_price,
    max(high_price) as high_price,
    min(low_price) as low_price,
    argMax(close_price, open_time) as close_price,
    sum(volume) as volume,
    now64() as created_at
FROM kline_raw
GROUP BY symbol, toStartOfInterval(open_time, INTERVAL 15 minute);

-- 1分钟 -> 1小时聚合物化视图
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kline_1m_to_1h
TO kline_1h
AS SELECT
    symbol,
    toStartOfInterval(open_time, INTERVAL 1 hour) as open_time,
    toStartOfInterval(open_time, INTERVAL 1 hour) + INTERVAL 1 hour - INTERVAL 1 second as close_time,
    argMin(open_price, open_time) as open_price,
    max(high_price) as high_price,
    min(low_price) as low_price,
    argMax(close_price, open_time) as close_price,
    sum(volume) as volume,
    now64() as created_at
FROM kline_raw
GROUP BY symbol, toStartOfInterval(open_time, INTERVAL 1 hour);

-- 1分钟 -> 4小时聚合物化视图
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kline_1m_to_4h
TO kline_4h
AS SELECT
    symbol,
    toStartOfInterval(open_time, INTERVAL 4 hour) as open_time,
    toStartOfInterval(open_time, INTERVAL 4 hour) + INTERVAL 4 hour - INTERVAL 1 second as close_time,
    argMin(open_price, open_time) as open_price,
    max(high_price) as high_price,
    min(low_price) as low_price,
    argMax(close_price, open_time) as close_price,
    sum(volume) as volume,
    now64() as created_at
FROM kline_raw
GROUP BY symbol, toStartOfInterval(open_time, INTERVAL 4 hour);

-- 1分钟 -> 1天聚合物化视图
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kline_1m_to_1d
TO kline_1d
AS SELECT
    symbol,
    toStartOfInterval(open_time, INTERVAL 1 day) as open_time,
    toStartOfInterval(open_time, INTERVAL 1 day) + INTERVAL 1 day - INTERVAL 1 second as close_time,
    argMin(open_price, open_time) as open_price,
    max(high_price) as high_price,
    min(low_price) as low_price,
    argMax(close_price, open_time) as close_price,
    sum(volume) as volume,
    now64() as created_at
FROM kline_raw
GROUP BY symbol, toStartOfInterval(open_time, INTERVAL 1 day);

-- ========================================
-- 4. 兼容性视图：保持向后兼容
-- ========================================

-- 统一查询视图（兼容旧的查询方式）
CREATE VIEW IF NOT EXISTS v_kline_unified AS
SELECT 
    0 as id,  -- 兼容旧的id字段
    symbol,
    '1m' as interval_type,
    open_time,
    close_time,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    created_at,
    created_at as updated_at
FROM kline_raw
UNION ALL
SELECT 
    0 as id,
    symbol,
    '5m' as interval_type,
    open_time,
    close_time,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    created_at,
    created_at as updated_at
FROM kline_5m
UNION ALL
SELECT 
    0 as id,
    symbol,
    '15m' as interval_type,
    open_time,
    close_time,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    created_at,
    created_at as updated_at
FROM kline_15m
UNION ALL
SELECT 
    0 as id,
    symbol,
    '1h' as interval_type,
    open_time,
    close_time,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    created_at,
    created_at as updated_at
FROM kline_1h
UNION ALL
SELECT 
    0 as id,
    symbol,
    '4h' as interval_type,
    open_time,
    close_time,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    created_at,
    created_at as updated_at
FROM kline_4h
UNION ALL
SELECT 
    0 as id,
    symbol,
    '1d' as interval_type,
    open_time,
    close_time,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    created_at,
    created_at as updated_at
FROM kline_1d;

-- ========================================
-- 5. 监控和统计视图
-- ========================================

-- 表统计视图
CREATE VIEW IF NOT EXISTS v_table_stats AS
SELECT 
    table AS table_name,
    sum(rows) AS total_rows,
    sum(bytes_on_disk) AS size_bytes,
    formatReadableSize(sum(bytes_on_disk)) AS size_readable,
    max(modification_time) AS last_modified
FROM system.parts 
WHERE database = 'data4trend' 
  AND table IN ('kline_raw', 'kline_5m', 'kline_15m', 'kline_1h', 'kline_4h', 'kline_1d')
GROUP BY table
ORDER BY total_rows DESC;

-- 物化视图状态监控
CREATE VIEW IF NOT EXISTS v_materialized_views_status AS
SELECT 
    name,
    engine,
    create_table_query
FROM system.tables 
WHERE database = 'data4trend' 
  AND engine = 'MaterializedView'
ORDER BY name;

-- ========================================
-- 6. 测试和验证
-- ========================================

-- 连接测试表
CREATE TABLE IF NOT EXISTS connection_test
(
    id UInt32,
    test_time DateTime64(3) DEFAULT now64()
)
ENGINE = MergeTree()
ORDER BY id;

-- 插入测试数据
INSERT INTO connection_test (id) VALUES (1);

-- ========================================
-- 7. 使用说明（注释）
-- ========================================

/*
使用说明：

1. 数据写入：
   - 只向 kline_raw 表写入1分钟粒度的数据
   - 物化视图会自动聚合生成其他时间粒度的数据

2. 数据查询：
   - 1分钟数据：SELECT * FROM kline_raw WHERE symbol = 'BTCUSDT'
   - 5分钟数据：SELECT * FROM kline_5m WHERE symbol = 'BTCUSDT'
   - 其他时间粒度类似
   - 兼容查询：SELECT * FROM v_kline_unified WHERE symbol = 'BTCUSDT' AND interval_type = '1h'

3. 监控：
   - 查看表统计：SELECT * FROM v_table_stats
   - 查看物化视图状态：SELECT * FROM v_materialized_views_status

4. 优势：
   - 存储效率高：避免重复存储
   - 查询性能好：预聚合数据，无需实时计算
   - 数据一致性：自动同步，无需手动维护
   - 扩展性强：新增时间粒度只需添加物化视图
*/