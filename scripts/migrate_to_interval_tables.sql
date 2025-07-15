-- Data migration script: From unified kline table to interval-based tables
-- This script migrates data from the original 'kline' table to interval-specific tables

USE data4trend;

-- Step 1: Create backup of original table
CREATE TABLE IF NOT EXISTS kline_backup AS kline;

-- Step 2: Check data distribution in original table
SELECT 
    interval_type,
    COUNT(*) as record_count,
    MIN(open_time) as earliest_data,
    MAX(open_time) as latest_data,
    COUNT(DISTINCT symbol) as symbol_count
FROM kline 
GROUP BY interval_type 
ORDER BY record_count DESC;

-- Step 3: Migrate data to interval-specific tables

-- Migrate 1m data
INSERT INTO kline_1m (id, symbol, open_time, close_time, open_price, high_price, low_price, close_price, volume, created_at, updated_at)
SELECT 
    id,
    symbol,
    open_time,
    close_time,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    created_at,
    updated_at
FROM kline 
WHERE interval_type = '1m';

-- Migrate 5m data
INSERT INTO kline_5m (id, symbol, open_time, close_time, open_price, high_price, low_price, close_price, volume, created_at, updated_at)
SELECT 
    id,
    symbol,
    open_time,
    close_time,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    created_at,
    updated_at
FROM kline 
WHERE interval_type = '5m';

-- Migrate 15m data
INSERT INTO kline_15m (id, symbol, open_time, close_time, open_price, high_price, low_price, close_price, volume, created_at, updated_at)
SELECT 
    id,
    symbol,
    open_time,
    close_time,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    created_at,
    updated_at
FROM kline 
WHERE interval_type = '15m';

-- Migrate 1h data
INSERT INTO kline_1h (id, symbol, open_time, close_time, open_price, high_price, low_price, close_price, volume, created_at, updated_at)
SELECT 
    id,
    symbol,
    open_time,
    close_time,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    created_at,
    updated_at
FROM kline 
WHERE interval_type = '1h';

-- Migrate 4h data
INSERT INTO kline_4h (id, symbol, open_time, close_time, open_price, high_price, low_price, close_price, volume, created_at, updated_at)
SELECT 
    id,
    symbol,
    open_time,
    close_time,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    created_at,
    updated_at
FROM kline 
WHERE interval_type = '4h';

-- Migrate 1d data
INSERT INTO kline_1d (id, symbol, open_time, close_time, open_price, high_price, low_price, close_price, volume, created_at, updated_at)
SELECT 
    id,
    symbol,
    open_time,
    close_time,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    created_at,
    updated_at
FROM kline 
WHERE interval_type = '1d';

-- Step 4: Verify migration
SELECT 'Migration completed. Check v_table_stats for verification.' as message; 