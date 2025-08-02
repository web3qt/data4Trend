#!/bin/bash

# 优化效果检查脚本
echo "🔍 检查优化配置效果"
echo "==================="

# 检查配置文件
echo "📋 配置检查:"

if grep -q "auto_fetch_symbols: true" config/config.yaml; then
    echo "✅ auto_fetch_symbols: 已启用 (将监控400+个交易对)"
else
    echo "❌ auto_fetch_symbols: 未启用 (仅监控10个交易对)"
fi

batch_size=$(grep -A3 "batch_writer:" config/config.yaml | grep "batch_size:" | awk '{print $2}')
batch_timeout=$(grep -A3 "batch_writer:" config/config.yaml | grep "batch_timeout:" | awk '{print $2}' | tr -d '"')

echo "✅ batch_size: $batch_size (推荐: 100-500)"
echo "✅ batch_timeout: $batch_timeout (推荐: 5s-15s)"

echo ""
echo "🔌 服务检查:"

# 检查Kafka状态
if docker ps | grep -q kafka; then
    echo "✅ Kafka: 运行中"
else
    echo "❌ Kafka: 未运行 (运行: ./start_with_kafka.sh)"
fi

# 检查应用程序
if pgrep -f "data4trend-collector" > /dev/null; then
    echo "✅ 数据收集器: 运行中"
else
    echo "❌ 数据收集器: 未运行"
fi

echo ""
echo "📊 实时数据检查:"

# 检查最近5分钟的数据
recent_symbols=$(curl -s -u default:123456 "http://localhost:8123" --data-binary "
SELECT count(DISTINCT symbol) 
FROM data4trend.klines_1m 
WHERE created_at >= now() - INTERVAL 5 MINUTE
" 2>/dev/null | head -1)

recent_records=$(curl -s -u default:123456 "http://localhost:8123" --data-binary "
SELECT count() 
FROM data4trend.klines_1m 
WHERE created_at >= now() - INTERVAL 5 MINUTE
" 2>/dev/null | head -1)

echo "📈 最近5分钟:"
echo "   活跃交易对: $recent_symbols 个"
echo "   新增记录: $recent_records 条"

if [ "$recent_symbols" -gt 50 ]; then
    echo "✅ 交易对数量正常 (优化生效)"
else
    echo "⚠️  交易对数量偏少，可能需要重启应用程序"
fi

if [ "$recent_records" -gt 100 ]; then
    echo "✅ 数据流量正常"
else
    echo "⚠️  数据流量偏少"
fi

echo ""
echo "🎯 下一步建议:"
echo "1. 重启应用程序使配置生效: ./start_with_kafka.sh"
echo "2. 运行性能监控: ./monitor_performance.sh"
echo "3. 查看实时日志了解详细状态"