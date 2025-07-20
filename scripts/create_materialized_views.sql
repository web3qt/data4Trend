-- 基于1m数据创建物化视图生成其他时间间隔
-- 这样只需要收集1m数据，其他时间间隔自动计算生成

-- 删除可能存在的视图/表
DROP TABLE IF EXISTS kline_5m;
DROP TABLE IF EXISTS kline_15m;
DROP TABLE IF EXISTS kline_1h;
DROP TABLE IF EXISTS kline_4h;
DROP TABLE IF EXISTS kline_1d;

-- 创建 kline_raw 表
CREATE TABLE IF NOT EXISTS kline_raw (
    id UInt64,
    symbol LowCardinality(String),
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
ORDER BY (symbol, open_time)
SETTINGS index_granularity = 8192;

-- 5分钟K线视图（从1m数据聚合）
CREATE MATERIALIZED VIEW kline_5m
ENGINE = ReplacingMergeTree()
ORDER BY (symbol, interval_start)
AS SELECT
    symbol,
    toStartOfInterval(open_time, INTERVAL 5 MINUTE) as interval_start,
    toStartOfInterval(open_time, INTERVAL 5 MINUTE) as open_time,
    toStartOfInterval(open_time, INTERVAL 5 MINUTE) + INTERVAL 5 MINUTE - INTERVAL 1 SECOND as close_time,
    any(open_price) as open_price,
    max(high_price) as high_price,
    min(low_price) as low_price,
    anyLast(close_price) as close_price,
    sum(volume) as volume
FROM kline_raw
GROUP BY symbol, toStartOfInterval(open_time, INTERVAL 5 MINUTE);

-- 15分钟K线视图
CREATE MATERIALIZED VIEW kline_15m
ENGINE = ReplacingMergeTree()
ORDER BY (symbol, interval_start)
AS SELECT
    symbol,
    toStartOfInterval(open_time, INTERVAL 15 MINUTE) as interval_start,
    toStartOfInterval(open_time, INTERVAL 15 MINUTE) as open_time,
    toStartOfInterval(open_time, INTERVAL 15 MINUTE) + INTERVAL 15 MINUTE - INTERVAL 1 SECOND as close_time,
    any(open_price) as open_price,
    max(high_price) as high_price,
    min(low_price) as low_price,
    anyLast(close_price) as close_price,
    sum(volume) as volume
FROM kline_raw
GROUP BY symbol, toStartOfInterval(open_time, INTERVAL 15 MINUTE);

-- 1小时K线视图
CREATE MATERIALIZED VIEW kline_1h
ENGINE = ReplacingMergeTree()
ORDER BY (symbol, interval_start)
AS SELECT
    symbol,
    toStartOfInterval(open_time, INTERVAL 1 HOUR) as interval_start,
    toStartOfInterval(open_time, INTERVAL 1 HOUR) as open_time,
    toStartOfInterval(open_time, INTERVAL 1 HOUR) + INTERVAL 1 HOUR - INTERVAL 1 SECOND as close_time,
    any(open_price) as open_price,
    max(high_price) as high_price,
    min(low_price) as low_price,
    anyLast(close_price) as close_price,
    sum(volume) as volume
FROM kline_raw
GROUP BY symbol, toStartOfInterval(open_time, INTERVAL 1 HOUR);

-- 4小时K线视图
CREATE MATERIALIZED VIEW kline_4h
ENGINE = ReplacingMergeTree()
ORDER BY (symbol, interval_start)
AS SELECT
    symbol,
    toStartOfInterval(open_time, INTERVAL 4 HOUR) as interval_start,
    toStartOfInterval(open_time, INTERVAL 4 HOUR) as open_time,
    toStartOfInterval(open_time, INTERVAL 4 HOUR) + INTERVAL 4 HOUR - INTERVAL 1 SECOND as close_time,
    any(open_price) as open_price,
    max(high_price) as high_price,
    min(low_price) as low_price,
    anyLast(close_price) as close_price,
    sum(volume) as volume
FROM kline_raw
GROUP BY symbol, toStartOfInterval(open_time, INTERVAL 4 HOUR);

-- 1天K线视图
CREATE MATERIALIZED VIEW kline_1d
ENGINE = ReplacingMergeTree()
ORDER BY (symbol, interval_start)
AS SELECT
    symbol,
    toStartOfInterval(open_time, INTERVAL 1 DAY) as interval_start,
    toStartOfInterval(open_time, INTERVAL 1 DAY) as open_time,
    toStartOfInterval(open_time, INTERVAL 1 DAY) + INTERVAL 1 DAY - INTERVAL 1 SECOND as close_time,
    any(open_price) as open_price,
    max(high_price) as high_price,
    min(low_price) as low_price,
    anyLast(close_price) as close_price,
    sum(volume) as volume
FROM kline_raw
GROUP BY symbol, toStartOfInterval(open_time, INTERVAL 1 DAY);

-- 创建统一查询视图
CREATE VIEW v_kline_unified AS
SELECT symbol, '1m' as interval_type, open_time, close_time, open_price, high_price, low_price, close_price, volume FROM kline_1m
UNION ALL
SELECT symbol, '5m' as interval_type, open_time, close_time, open_price, high_price, low_price, close_price, volume FROM v_kline_5m
UNION ALL
SELECT symbol, '15m' as interval_type, open_time, close_time, open_price, high_price, low_price, close_price, volume FROM v_kline_15m
UNION ALL
SELECT symbol, '1h' as interval_type, open_time, close_time, open_price, high_price, low_price, close_price, volume FROM v_kline_1h
UNION ALL
SELECT symbol, '4h' as interval_type, open_time, close_time, open_price, high_price, low_price, close_price, volume FROM v_kline_4h
UNION ALL
SELECT symbol, '1d' as interval_type, open_time, close_time, open_price, high_price, low_price, close_price, volume FROM v_kline_1d; 