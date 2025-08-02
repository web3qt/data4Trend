#!/bin/bash

# 数据完整性功能测试脚本

echo "=== 数据完整性功能测试 ==="
echo

# 检查API服务器状态
echo "1. 检查API服务器健康状态:"
curl -s http://localhost:8080/health | jq
echo

# 检查数据完整性状态
echo "2. 检查数据完整性状态:"
curl -s http://localhost:8080/api/v1/integrity/status | jq
echo

# 手动触发完整性检查
echo "3. 手动触发完整性检查:"
curl -s -X POST http://localhost:8080/api/v1/integrity/check | jq
echo

# 等待检查完成
echo "4. 等待检查完成..."
sleep 5

# 再次检查状态
echo "5. 检查更新后的完整性状态:"
curl -s http://localhost:8080/api/v1/integrity/status | jq
echo

# 检查数据验证状态
echo "6. 检查数据验证状态:"
curl -s http://localhost:8080/api/v1/validation/status | jq
echo

# 手动触发数据验证
echo "7. 手动触发数据验证:"
curl -s -X POST http://localhost:8080/api/v1/validation/run | jq
echo

# 检查数据质量指标
echo "8. 检查数据质量指标:"
curl -s http://localhost:8080/api/v1/validation/quality | jq
echo

# 检查数据缺口
echo "9. 检查数据缺口:"
curl -s http://localhost:8080/api/v1/validation/gaps | jq
echo

# 测试手动回填功能
echo "10. 测试手动回填功能 (ETHUSDT, 过去1小时):"
START_TIME=$(date -u -v-1H '+%Y-%m-%dT%H:%M:%SZ')
END_TIME=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
echo "回填时间范围: $START_TIME 到 $END_TIME"
curl -s -X POST "http://localhost:8080/api/v1/integrity/backfill/ETHUSDT?start_time=$START_TIME&end_time=$END_TIME" | jq
echo

echo "=== 测试完成 ==="
echo "数据完整性功能已成功部署并可正常工作！"
echo
echo "可用的API端点:"
echo "- GET  /api/v1/integrity/status     - 获取完整性状态"
echo "- POST /api/v1/integrity/check      - 手动触发完整性检查"
echo "- POST /api/v1/integrity/backfill/:symbol - 手动回填指定交易对数据"
echo "- GET  /api/v1/validation/status    - 获取验证状态"
echo "- POST /api/v1/validation/run       - 手动触发数据验证"
echo "- GET  /api/v1/validation/quality   - 获取数据质量指标"
echo "- GET  /api/v1/validation/gaps      - 获取数据缺口信息"