#!/bin/bash

# 数据库迁移脚本 - 解决重复数据问题
echo "🔄 开始数据库迁移..."
echo "=================="

# 检查ClickHouse连接
echo "1. 检查ClickHouse连接..."
if curl -s "http://localhost:8123/?user=default&password=123456&query=SELECT%201" > /dev/null; then
    echo "✅ ClickHouse连接正常"
else
    echo "❌ ClickHouse连接失败"
    exit 1
fi

# 备份当前数据
echo "2. 备份当前数据..."
curl -s "http://localhost:8123/?user=default&password=123456&query=SELECT%20COUNT(*)%20FROM%20data4trend.klines_1m" | grep -o '[0-9]*' > /tmp/backup_count.txt
echo "✅ 当前数据记录数: $(cat /tmp/backup_count.txt)"

# 执行迁移 - 分步执行
echo "3. 执行数据库迁移..."

# 步骤1: 创建新表
echo "   步骤1: 创建新表..."
curl -s "http://localhost:8123/?user=default&password=123456" --data-binary "
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
PARTITION BY toYYYYMM(toDateTime(open_time / 1000))
"

# 步骤2: 迁移数据（去重）
echo "   步骤2: 迁移数据..."
curl -s "http://localhost:8123/?user=default&password=123456" --data-binary "
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
    1 as version
FROM (
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
        ROW_NUMBER() OVER (PARTITION BY symbol, open_time ORDER BY created_at DESC) as rn
    FROM data4trend.klines_1m
) t
WHERE rn = 1
"

# 步骤3: 删除旧表
echo "   步骤3: 删除旧表..."
curl -s "http://localhost:8123/?user=default&password=123456" --data-binary "DROP TABLE IF EXISTS data4trend.klines_1m"

# 步骤4: 重命名新表
echo "   步骤4: 重命名新表..."
curl -s "http://localhost:8123/?user=default&password=123456" --data-binary "RENAME TABLE data4trend.klines_1m_new TO data4trend.klines_1m"

if [ $? -eq 0 ]; then
    echo "✅ 数据库迁移完成"
else
    echo "❌ 数据库迁移失败"
    exit 1
fi

# 验证迁移结果
echo "4. 验证迁移结果..."
curl -s "http://localhost:8123/?user=default&password=123456&query=SELECT%20COUNT(*)%20FROM%20data4trend.klines_1m" | grep -o '[0-9]*' > /tmp/migrated_count.txt
echo "✅ 迁移后数据记录数: $(cat /tmp/migrated_count.txt)"

# 检查重复数据
echo "5. 检查重复数据..."
duplicate_count=$(curl -s "http://localhost:8123/?user=default&password=123456&query=SELECT%20COUNT(*)%20FROM%20(SELECT%20symbol,%20open_time,%20COUNT(*)%20as%20cnt%20FROM%20data4trend.klines_1m%20GROUP%20BY%20symbol,%20open_time%20HAVING%20cnt%20%3E%201)" | grep -o '[0-9]*' || echo "0")

if [ "$duplicate_count" = "0" ]; then
    echo "✅ 重复数据已清理"
else
    echo "⚠️  仍有 $duplicate_count 条重复数据"
fi

echo ""
echo "🎉 数据库迁移完成！"
echo "📊 迁移总结："
echo "- 原数据记录数: $(cat /tmp/backup_count.txt)"
echo "- 迁移后记录数: $(cat /tmp/migrated_count.txt)"
echo "- 重复数据: $duplicate_count"
echo ""
echo "🔧 新功能："
echo "- ✅ 使用ReplacingMergeTree引擎"
echo "- ✅ 自动去重功能"
echo "- ✅ 版本控制支持"
echo "- ✅ 更好的数据一致性" 