#!/bin/bash

# 性能监控脚本
echo "=== Data4Trend Performance Monitor ==="
echo "Monitoring data collection performance..."
echo ""

# 检查ClickHouse连接
echo "1. Checking ClickHouse connection..."
if curl -s -u default:123456 "http://localhost:8123/ping" > /dev/null; then
    echo "✅ ClickHouse is running"
else
    echo "❌ ClickHouse is not accessible"
    exit 1
fi

# 检查Kafka连接
echo ""
echo "2. Checking Kafka connection..."
if nc -z localhost 9092 2>/dev/null; then
    echo "✅ Kafka is running"
else
    echo "❌ Kafka is not accessible"
fi

# 监控数据写入速度
echo ""
echo "3. Monitoring data write performance..."
echo "Press Ctrl+C to stop monitoring"
echo ""

# 每秒检查一次数据写入情况
while true; do
    # 获取当前时间
    current_time=$(date '+%Y-%m-%d %H:%M:%S')
    
    # 查询ClickHouse中的记录数（使用认证）
    total_records=$(curl -s -u default:123456 "http://localhost:8123/?query=SELECT%20count()%20FROM%20data4trend.klines_1m" 2>/dev/null | tail -n 1)
    
    # 查询最近1分钟的记录数（使用认证）
    recent_records=$(curl -s -u default:123456 "http://localhost:8123/?query=SELECT%20count()%20FROM%20data4trend.klines_1m%20WHERE%20created_at%20%3E%20now()%20-%20INTERVAL%201%20MINUTE" 2>/dev/null | tail -n 1)
    
    # 查询唯一交易对数量（使用认证）
    unique_symbols=$(curl -s -u default:123456 "http://localhost:8123/?query=SELECT%20count(DISTINCT%20symbol)%20FROM%20data4trend.klines_1m" 2>/dev/null | tail -n 1)
    
    # 查询最新记录时间（使用认证）
    latest_time=$(curl -s -u default:123456 "http://localhost:8123/?query=SELECT%20max(created_at)%20FROM%20data4trend.klines_1m" 2>/dev/null | tail -n 1)
    
    # 计算写入速率（每分钟）
    if [ "$recent_records" -gt 0 ] 2>/dev/null; then
        write_rate="$recent_records records/min"
    else
        write_rate="0 records/min"
    fi
    
    # 显示状态
    echo "[$current_time]"
    echo "  📊 Total records: $total_records"
    echo "  ⚡ Write rate: $write_rate"
    echo "  🏷️  Unique symbols: $unique_symbols"
    echo "  🕐 Latest record: $latest_time"
    echo "  ──────────────────────────────────────"
    
    sleep 5
done