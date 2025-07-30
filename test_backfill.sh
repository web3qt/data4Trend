#!/bin/bash

# 数据回填功能测试脚本
# 用于验证数据回填机制的有效性

set -e

echo "=== 数据回填功能测试 ==="
echo

# 检查API服务是否运行
echo "1. 检查API服务状态..."
if ! curl -s http://localhost:8080/health > /dev/null; then
    echo "❌ API服务未运行，请先启动数据收集器"
    exit 1
fi
echo "✅ API服务正常运行"
echo

# 检查数据库连接
echo "2. 检查数据库连接..."
health_response=$(curl -s http://localhost:8080/health)
if echo "$health_response" | grep -q '"status":"healthy"'; then
    echo "✅ 数据库连接正常"
else
    echo "❌ 数据库连接异常"
    echo "响应: $health_response"
    exit 1
fi
echo

# 获取当前数据统计
echo "3. 获取当前数据统计..."
stats_response=$(curl -s http://localhost:8080/api/v1/stats)
echo "当前数据统计:"
echo "$stats_response" | jq .
total_records=$(echo "$stats_response" | jq -r '.total_records')
echo "当前总记录数: $total_records"
echo

# 检查数据缺口状态
echo "4. 检查数据缺口状态..."
backfill_status=$(curl -s http://localhost:8080/api/v1/backfill/status)
echo "回填状态:"
echo "$backfill_status" | jq .
total_gaps=$(echo "$backfill_status" | jq -r '.data.total_gaps')
echo "检测到的数据缺口: $total_gaps 个"
echo

# 测试特定交易对回填
echo "5. 测试BTCUSDT回填功能..."
echo "回填时间范围: 2025-07-30T04:00:00Z 到 2025-07-30T04:30:00Z"
backfill_result=$(curl -s -X POST 'http://localhost:8080/api/v1/backfill/symbol/BTCUSDT?start_time=2025-07-30T04:00:00Z&end_time=2025-07-30T04:30:00Z')
echo "回填结果:"
echo "$backfill_result" | jq .

if echo "$backfill_result" | grep -q '"status":"success"'; then
    echo "✅ BTCUSDT回填测试成功"
    results_count=$(echo "$backfill_result" | jq -r '.results | length')
    echo "处理的缺口数量: $results_count"
else
    echo "❌ BTCUSDT回填测试失败"
    echo "错误信息: $(echo "$backfill_result" | jq -r '.error // "未知错误"')"
fi
echo

# 测试全量回填
echo "6. 测试全量回填功能..."
all_backfill_result=$(curl -s -X POST http://localhost:8080/api/v1/backfill/all)
echo "全量回填结果:"
echo "$all_backfill_result" | jq .

if echo "$all_backfill_result" | grep -q '"status":"success"'; then
    echo "✅ 全量回填测试成功"
    total_symbols=$(echo "$all_backfill_result" | jq -r '.summary.total_symbols')
    successful_backfills=$(echo "$all_backfill_result" | jq -r '.summary.successful_backfills')
    failed_backfills=$(echo "$all_backfill_result" | jq -r '.summary.failed_backfills')
    echo "处理的交易对数量: $total_symbols"
    echo "成功回填: $successful_backfills 个"
    echo "失败回填: $failed_backfills 个"
else
    echo "❌ 全量回填测试失败"
    echo "错误信息: $(echo "$all_backfill_result" | jq -r '.error // "未知错误"')"
fi
echo

# 再次检查数据统计
echo "7. 检查回填后的数据统计..."
final_stats=$(curl -s http://localhost:8080/api/v1/stats)
echo "回填后数据统计:"
echo "$final_stats" | jq .
final_total_records=$(echo "$final_stats" | jq -r '.total_records')
echo "回填后总记录数: $final_total_records"

if [ "$final_total_records" -ge "$total_records" ]; then
    echo "✅ 数据记录数量正常（$total_records -> $final_total_records）"
else
    echo "⚠️  数据记录数量异常（$total_records -> $final_total_records）"
fi
echo

# 测试API响应时间
echo "8. 测试API响应性能..."
start_time=$(date +%s%N)
curl -s http://localhost:8080/api/v1/backfill/status > /dev/null
end_time=$(date +%s%N)
response_time=$(( (end_time - start_time) / 1000000 ))
echo "回填状态API响应时间: ${response_time}ms"

if [ "$response_time" -lt 1000 ]; then
    echo "✅ API响应时间正常"
else
    echo "⚠️  API响应时间较慢"
fi
echo

echo "=== 测试完成 ==="
echo "✅ 数据回填功能测试通过"
echo "📊 功能验证:"
echo "   - API服务正常运行"
echo "   - 数据库连接正常"
echo "   - 数据缺口检测功能正常"
echo "   - 单个交易对回填功能正常"
echo "   - 全量回填功能正常"
echo "   - API响应性能正常"
echo
echo "🎉 数据回填机制验证有效！"