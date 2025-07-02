-- ClickHouse initialization script
-- Create database
CREATE DATABASE IF NOT EXISTS data4trend;

-- Use the database
USE data4trend;

-- Create the main kline table with partitioning for better performance
CREATE TABLE IF NOT EXISTS kline
(
    id UInt64,
    symbol String,
    interval_type String,
    open_time DateTime64(3),
    close_time DateTime64(3),
    open_price Decimal64(8),
    high_price Decimal64(8),
    low_price Decimal64(8),
    close_price Decimal64(8),
    volume Decimal64(8),
    created_at DateTime64(3) DEFAULT now64(),
    updated_at DateTime64(3) DEFAULT now64()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, interval_type, open_time)
SETTINGS index_granularity = 8192;

-- Create an index for faster queries
CREATE INDEX IF NOT EXISTS idx_symbol_interval ON kline (symbol, interval_type);

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