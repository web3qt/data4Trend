#!/bin/bash

echo "📊 实时数据流监控"
echo "=================="

# 初始化计数器
last_count=0
start_time=$(date +%s)

echo "⏳ 开始监控数据流..."
echo "按 Ctrl+C 停止监控"
echo ""

while true; do
    # 获取当前记录数
    current_count=$(curl -s -u default:123456 "http://localhost:8123" --data-binary "SELECT count() FROM data4trend.klines_1m" 2>/dev/null || echo "0")
    
    # 获取当前时间
    current_time=$(date +%s)
    elapsed=$((current_time - start_time))
    
    # 计算新增记录数
    new_records=$((current_count - last_count))
    
    # 计算每秒写入速率
    if [ $elapsed -gt 0 ]; then
        total_rate=$((current_count / elapsed))
    else
        total_rate=0
    fi
    
    # 获取当前活跃交易对数量
    symbol_count=$(curl -s -u default:123456 "http://localhost:8123" --data-binary "SELECT count(DISTINCT symbol) FROM data4trend.klines_1m WHERE created_at >= now() - INTERVAL 10 MINUTE" 2>/dev/null || echo "0")
    
    # 获取最新数据时间
    latest_time=$(curl -s -u default:123456 "http://localhost:8123" --data-binary "SELECT max(created_at) FROM data4trend.klines_1m" 2>/dev/null || echo "无数据")
    
    # 清屏并显示统计信息
    clear
    echo "📊 Data4Trend 实时监控"
    echo "========================"
    echo "⏰ 监控时间: $(date)"
    echo "🕐 运行时长: ${elapsed}秒"
    echo ""
    echo "📈 数据统计:"
    echo "   总记录数: $current_count"
    echo "   新增记录: $new_records (过去5秒)"
    echo "   平均速率: $total_rate 记录/秒"
    echo "   活跃币种: $symbol_count 个"
    echo "   最新数据: $latest_time"
    echo ""
    
    # 显示最近的数据样本
    echo "📋 最新数据样本:"
    curl -s -u default:123456 "http://localhost:8123" --data-binary "
    SELECT 
        symbol,
        toDateTime(open_time/1000) as time,
        close as price,
        created_at
    FROM data4trend.klines_1m 
    ORDER BY created_at DESC 
    LIMIT 5
    " 2>/dev/null | column -t || echo "暂无数据"
    
    echo ""
    echo "💡 优化目标:"
    echo "   - 活跃币种: 400+ 个"
    echo "   - 数据写入: 每10秒100-400条"
    echo "   - 每分钟: 400+ 条新数据"
    
    last_count=$current_count
    sleep 5
done 