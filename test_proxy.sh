#!/bin/bash

echo "🔍 测试代理连接"
echo "================"

# 检查代理配置
echo "📋 当前代理配置:"
if grep -q "enabled: true" config/config.yaml; then
    echo "✅ 代理已启用"
    PROXY_HOST=$(grep -A3 "proxy:" config/config.yaml | grep "host:" | awk '{print $2}' | tr -d '"')
    PROXY_PORT=$(grep -A3 "proxy:" config/config.yaml | grep "port:" | awk '{print $2}' | tr -d '"')
    PROXY_TYPE=$(grep -A3 "proxy:" config/config.yaml | grep "type:" | awk '{print $2}' | tr -d '"')
    echo "   类型: $PROXY_TYPE"
    echo "   主机: $PROXY_HOST"
    echo "   端口: $PROXY_PORT"
else
    echo "❌ 代理未启用"
    exit 1
fi

# 测试代理连接
echo ""
echo "🌐 测试代理连接..."

# 使用curl测试代理
if curl --proxy socks5://$PROXY_HOST:$PROXY_PORT --connect-timeout 10 -s "https://api.binance.com/api/v3/exchangeInfo" > /dev/null; then
    echo "✅ 代理连接成功 - 可以访问币安API"
else
    echo "❌ 代理连接失败 - 无法访问币安API"
    echo ""
    echo "🔧 故障排除建议:"
    echo "1. 检查代理服务是否运行: lsof -i :$PROXY_PORT"
    echo "2. 测试代理服务: curl --proxy socks5://$PROXY_HOST:$PROXY_PORT http://httpbin.org/ip"
    echo "3. 检查防火墙设置"
    exit 1
fi

# 测试WebSocket连接
echo ""
echo "🔌 测试WebSocket连接..."
if curl --proxy socks5://$PROXY_HOST:$PROXY_PORT --connect-timeout 10 -s "wss://stream.binance.com:9443" > /dev/null 2>&1; then
    echo "✅ WebSocket代理连接成功"
else
    echo "⚠️  WebSocket连接测试失败 (这是正常的，因为curl不支持WebSocket)"
fi

# 测试环境变量
echo ""
echo "🔧 检查环境变量..."
if [ -n "$HTTP_PROXY" ]; then
    echo "✅ HTTP_PROXY: $HTTP_PROXY"
else
    echo "⚠️  HTTP_PROXY 未设置"
fi

if [ -n "$HTTPS_PROXY" ]; then
    echo "✅ HTTPS_PROXY: $HTTPS_PROXY"
else
    echo "⚠️  HTTPS_PROXY 未设置"
fi

# 建议设置环境变量
echo ""
echo "💡 建议设置环境变量以确保所有组件都使用代理:"
echo "export HTTP_PROXY=socks5://$PROXY_HOST:$PROXY_PORT"
echo "export HTTPS_PROXY=socks5://$PROXY_HOST:$PROXY_PORT"
echo "export ALL_PROXY=socks5://$PROXY_HOST:$PROXY_PORT"

echo ""
echo "🎯 如果代理测试成功，现在可以重启应用程序:"
echo "./restart_optimized.sh" 