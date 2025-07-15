-- Fixed table creation script - recreate all tables with Float64 types
USE data4trend;

-- 1 minute K-line table
CREATE TABLE kline_1m
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
CREATE TABLE kline_5m
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
CREATE TABLE kline_15m
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
CREATE TABLE kline_4h
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
CREATE TABLE kline_1d
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

-- Create a connection test table
CREATE TABLE connection_test
(
    id UInt32,
    test_time DateTime64(3) DEFAULT now64()
)
ENGINE = MergeTree()
ORDER BY id;

-- Insert test data
INSERT INTO connection_test (id) VALUES (1);

SELECT 'All tables created successfully with Float64 types!' as status; 