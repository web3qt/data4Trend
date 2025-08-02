#!/bin/bash

echo "🚀 重启系统以应用性能优化"
echo "========================="

# 停止现有服务
echo "⏹️  停止现有服务..."
./stop_kafka.sh 2>/dev/null || true
pkill -f "data4trend-collector" 2>/dev/null || true
sleep 3

# 清理旧数据（可选）
echo ""
read -p "🗑️  是否清理数据库重新开始？ (y/N): " clean_db
if [[ $clean_db =~ ^[Yy]$ ]]; then
    echo "清理数据库..."
    ./clean_database.sh
fi

# 选择配置文件
echo ""
echo "📋 选择配置文件:"
echo "1. 标准优化配置 (config.yaml - 已优化)"
echo "2. 高性能配置 (config-high-performance.yaml)"
read -p "请选择 (1/2): " config_choice

case $config_choice in
    2)
        CONFIG_FILE="config/config-high-performance.yaml"
        echo "使用高性能配置"
        ;;
    *)
        CONFIG_FILE="config/config.yaml"
        echo "使用标准优化配置"
        ;;
esac

# 重新编译（确保使用最新代码）
echo ""
echo "🔨 重新编译程序..."
go build -o bin/data4trend-collector cmd/collector/main.go

if [ $? -ne 0 ]; then
    echo "❌ 编译失败"
    exit 1
fi

# 启动系统
echo ""
echo "🚀 启动优化后的系统..."
./start_with_kafka.sh

# 等待服务启动
echo ""
echo "⏳ 等待服务完全启动..."
sleep 15

# 检查服务状态
echo ""
echo "🔍 检查服务状态..."

# 检查API服务器
if curl -s http://localhost:8080/health > /dev/null; then
    echo "✅ API服务器启动成功"
else
    echo "❌ API服务器启动失败"
fi

# 检查数据库连接
if curl -s -u default:123456 "http://localhost:8123" --data-binary "SELECT 1" > /dev/null; then
    echo "✅ ClickHouse连接成功"
else
    echo "❌ ClickHouse连接失败"
fi

# 显示优化后的配置
echo ""
echo "📊 当前配置:"
echo "   配置文件: $CONFIG_FILE"
echo "   自动获取币种: $(grep 'auto_fetch_symbols:' $CONFIG_FILE | awk '{print $2}')"
echo "   批量大小: $(grep 'batch_size:' $CONFIG_FILE | tail -1 | awk '{print $2}')"
echo "   批量超时: $(grep 'batch_timeout:' $CONFIG_FILE | tail -1 | awk '{print $2}')"

echo ""
echo "🎯 下一步:"
echo "1. 运行监控脚本: ./monitor_performance.sh"
echo "2. 检查优化效果: ./check_optimization.sh"
echo "3. 查看实时日志: docker logs -f data4trend-collector"
echo ""
echo "💡 预期改进:"
echo "   - 监控币种: 从10个增加到400+个"
echo "   - 写入频率: 从60秒改为5-10秒"
echo "   - 数据量: 增加40倍以上"