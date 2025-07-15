-- ClickHouse initialization script for interval-based tables
-- Create database
CREATE DATABASE IF NOT EXISTS data4trend;

-- Use the database
USE data4trend;

-- Create interval-based tables with optimized structure for time-series data
-- Each table stores data for a specific time interval

-- 1 minute K-line table
CREATE TABLE IF NOT EXISTS kline_1m
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

-- 5 minute K-line table
CREATE TABLE IF NOT EXISTS kline_5m
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

-- 15 minute K-line table
CREATE TABLE IF NOT EXISTS kline_15m
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

-- 1 hour K-line table
CREATE TABLE IF NOT EXISTS kline_1h
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

-- 4 hour K-line table
CREATE TABLE IF NOT EXISTS kline_4h
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

-- 1 day K-line table
CREATE TABLE IF NOT EXISTS kline_1d
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

-- Create bloom filter indexes for better query performance
ALTER TABLE kline_1m ADD INDEX bloom_symbol symbol TYPE bloom_filter(0.01) GRANULARITY 1;
ALTER TABLE kline_5m ADD INDEX bloom_symbol symbol TYPE bloom_filter(0.01) GRANULARITY 1;
ALTER TABLE kline_15m ADD INDEX bloom_symbol symbol TYPE bloom_filter(0.01) GRANULARITY 1;
ALTER TABLE kline_1h ADD INDEX bloom_symbol symbol TYPE bloom_filter(0.01) GRANULARITY 1;
ALTER TABLE kline_4h ADD INDEX bloom_symbol symbol TYPE bloom_filter(0.01) GRANULARITY 1;
ALTER TABLE kline_1d ADD INDEX bloom_symbol symbol TYPE bloom_filter(0.01) GRANULARITY 1;

-- Create materialized views for data aggregation (optional)
-- This can help with cross-interval analysis

-- Aggregate 1m to 5m data
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kline_1m_to_5m
TO kline_5m
AS SELECT
    id,
    symbol,
    toStartOfInterval(open_time, INTERVAL 5 minute) as open_time,
    toStartOfInterval(open_time, INTERVAL 5 minute) + INTERVAL 5 minute - INTERVAL 1 second as close_time,
    any(open_price) as open_price,
    max(high_price) as high_price,
    min(low_price) as low_price,
    anyLast(close_price) as close_price,
    sum(volume) as volume,
    now64() as created_at,
    now64() as updated_at
FROM kline_1m
GROUP BY symbol, toStartOfInterval(open_time, INTERVAL 5 minute), id;

-- Keep original kline table for backward compatibility (optional)
-- You can drop this after migration is complete
CREATE TABLE IF NOT EXISTS kline_legacy
(
    id UInt64,
    symbol String,
    interval_type String,
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
ORDER BY (symbol, interval_type, open_time)
SETTINGS index_granularity = 8192;

-- Create a connection test table
CREATE TABLE IF NOT EXISTS connection_test
(
    id UInt32,
    test_time DateTime64(3) DEFAULT now64()
)
ENGINE = MergeTree()
ORDER BY id;

-- Insert test data
INSERT INTO connection_test (id) VALUES (1);

-- Create monitoring views for performance tracking
CREATE VIEW IF NOT EXISTS v_table_stats AS
SELECT 
    table AS table_name,
    sum(rows) AS total_rows,
    sum(bytes_on_disk) AS size_bytes,
    formatReadableSize(sum(bytes_on_disk)) AS size_readable,
    max(modification_time) AS last_modified
FROM system.parts 
WHERE database = 'data4trend' AND table LIKE 'kline_%' AND table != 'kline_legacy'
GROUP BY table
ORDER BY total_rows DESC;

-- Create a unified view for querying across all intervals (for backward compatibility)
CREATE VIEW IF NOT EXISTS v_kline_unified AS
SELECT 
    id,
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
    updated_at
FROM kline_1m
UNION ALL
SELECT 
    id,
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
    updated_at
FROM kline_5m
UNION ALL
SELECT 
    id,
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
    updated_at
FROM kline_15m
UNION ALL
SELECT 
    id,
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
    updated_at
FROM kline_1h
UNION ALL
SELECT 
    id,
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
    updated_at
FROM kline_4h
UNION ALL
SELECT 
    id,
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
    updated_at
FROM kline_1d; 