#!/bin/bash

echo "=== 币安连接诊断报告 ==="
echo "时间: $(date)"
echo

# 1. 检查网络连通性
echo "1. 网络连通性测试:"
ping -c 3 8.8.8.8 > /dev/null 2>&1 && echo "✅ 外网连接正常" || echo "❌ 网络连接异常"

# 2. 检查币安API连通性
echo "2. 币安API连通性测试:"
if curl -s --max-time 10 "https://api.binance.com/api/v3/time" > /dev/null; then
    echo "✅ 币安API可以直接访问"
else
    echo "❌ 币安API无法直接访问"
fi

# 3. 检查代理设置
echo "3. 代理设置检查:"
if [ -f "config/symbols.yaml" ]; then
    PROXY=$(grep "proxy:" config/symbols.yaml | sed 's/.*proxy: *"\([^"]*\)".*/\1/')
    if [ -n "$PROXY" ] && [ "$PROXY" != "#" ]; then
        echo "📋 检测到代理配置: $PROXY"
        if curl -s --max-time 10 -x "$PROXY" "https://api.binance.com/api/v3/time" > /dev/null; then
            echo "✅ 代理工作正常"
        else
            echo "❌ 代理无法连接"
        fi
    else
        echo "📋 未配置代理"
    fi
fi

# 4. 检查ClickHouse连接
echo "4. ClickHouse连接测试:"
if docker ps | grep -q clickhouse; then
    echo "✅ ClickHouse容器正在运行"
    if curl -s --max-time 5 "http://localhost:8123/ping" > /dev/null; then
        echo "✅ ClickHouse端口8123可访问"
    else
        echo "❌ ClickHouse端口8123无法访问"
    fi
else
    echo "⚠️  ClickHouse容器未运行"
fi

# 5. 提供解决方案
echo
echo "=== 解决方案 ==="
echo "问题: 获取币安数据卡住"
echo

echo "方案1 - 禁用代理（推荐）:"
echo "  1. 编辑 config/symbols.yaml"
echo "  2. 将 proxy: 行注释掉"
echo "  3. 重新运行程序"
echo

echo "方案2 - 配置正确代理:"
echo "  1. 确保代理软件已启动"
echo "  2. 检查代理端口是否正确"
echo "  3. 测试代理是否能访问币安"
echo

echo "方案3 - 使用备用币安域名:"
echo "  1. 修改 config/symbols.yaml"
echo "  2. 将 base_url 改为: https://api1.binance.com"
echo

echo "=== 快速修复命令 ==="
echo "# 禁用代理:"
echo "sed -i.bak 's/proxy:.*/# proxy: ""/' config/symbols.yaml"
echo "# 重新启动:"
echo "./bin/data-collector-materialized -config=config/symbols.yaml"