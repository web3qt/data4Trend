-- 迁移到ReplacingMergeTree引擎的脚本
-- 用于解决重复数据问题

-- 1. 创建新表（使用ReplacingMergeTree引擎）
CREATE TABLE IF NOT EXISTS data4trend.klines_1m_new (
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

-- 2. 迁移数据（去重）
INSERT INTO data4trend.klines_1m_new (symbol, open_time, close_time, open, high, low, close, volume, created_at, version)
SELECT 
    symbol,
    open_time,
    close_time,
    open,
    high,
    low,
    close,
    volume,
    created_at,
    ROW_NUMBER() OVER (PARTITION BY symbol, open_time ORDER BY created_at DESC) as version
FROM data4trend.klines_1m
WHERE version = 1;

-- 3. 删除旧表
DROP TABLE IF EXISTS data4trend.klines_1m;

-- 4. 重命名新表
RENAME TABLE data4trend.klines_1m_new TO data4trend.klines_1m;

-- 5. 验证迁移结果
SELECT 
    'Migration completed' as status,
    COUNT(*) as total_records,
    COUNT(DISTINCT symbol) as unique_symbols
FROM data4trend.klines_1m;

-- 6. 检查是否还有重复数据
SELECT 
    symbol,
    open_time,
    COUNT(*) as duplicate_count
FROM data4trend.klines_1m
GROUP BY symbol, open_time
HAVING duplicate_count > 1
LIMIT 10; 