-- 验证按时间级别分表结构的脚本

USE data4trend;

-- 1. 检查所有表是否存在
SELECT 
    'Table Check' as test_type,
    name as table_name,
    engine,
    'EXISTS' as status
FROM system.tables 
WHERE database = 'data4trend' 
AND name LIKE 'kline_%'
ORDER BY name;

-- 2. 检查表结构和数据类型
SELECT 
    'Column Types' as test_type,
    table as table_name,
    name as column_name,
    type as data_type,
    CASE 
        WHEN type = 'Float64' AND name IN ('open_price', 'high_price', 'low_price', 'close_price', 'volume') THEN 'CORRECT'
        WHEN type = 'LowCardinality(String)' AND name = 'symbol' THEN 'CORRECT'
        WHEN type = 'DateTime64(3)' AND name IN ('open_time', 'close_time', 'created_at', 'updated_at') THEN 'CORRECT'
        WHEN type = 'UInt64' AND name = 'id' THEN 'CORRECT'
        ELSE 'CHECK'
    END as type_status
FROM system.columns 
WHERE database = 'data4trend' 
AND table LIKE 'kline_%'
AND table NOT LIKE 'kline_legacy'
ORDER BY table, name;

-- 3. 检查分区和索引配置
SELECT 
    'Partition Check' as test_type,
    table as table_name,
    partition_key,
    sorting_key,
    'OK' as status
FROM system.tables 
WHERE database = 'data4trend' 
AND name LIKE 'kline_%'
AND name NOT LIKE 'kline_legacy'
ORDER BY name;

-- 4. 检查表大小和性能
SELECT 
    'Table Stats' as test_type,
    table as table_name,
    sum(rows) as total_rows,
    formatReadableSize(sum(bytes_on_disk)) as size_on_disk,
    count(*) as parts_count
FROM system.parts 
WHERE database = 'data4trend' 
AND table LIKE 'kline_%'
AND table != 'kline_legacy'
GROUP BY table
ORDER BY table;

-- 5. 验证视图是否创建成功
SELECT 
    'Views Check' as test_type,
    name as view_name,
    'EXISTS' as status
FROM system.tables 
WHERE database = 'data4trend' 
AND engine = 'View'
ORDER BY name;

-- 6. 测试写入和查询（如果有数据）
SELECT 
    'Data Sample' as test_type,
    'kline_1h' as table_name,
    COUNT(*) as record_count,
    'SAMPLE' as status
FROM kline_1h 
LIMIT 1;

SELECT 
    'Connection Test' as test_type,
    'SUCCESS' as table_name,
    now() as current_time,
    'OK' as status;

-- 7. 显示修复状态总结
SELECT 
    '=== 修复状态总结 ===' as summary,
    CASE 
        WHEN (SELECT COUNT(*) FROM system.tables WHERE database = 'data4trend' AND name LIKE 'kline_%' AND name != 'kline_legacy') >= 6 
        THEN '✅ 所有表已创建'
        ELSE '❌ 表创建不完整'
    END as table_status,
    CASE 
        WHEN (SELECT COUNT(*) FROM system.columns WHERE database = 'data4trend' AND table LIKE 'kline_%' AND name = 'open_price' AND type = 'Float64') >= 6
        THEN '✅ 数据类型已修复'
        ELSE '❌ 数据类型需要修复'
    END as type_status; 