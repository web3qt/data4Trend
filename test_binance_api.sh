#!/bin/bash

echo "🔍 测试币安API连接"
echo "=================="

# 设置代理
export HTTP_PROXY=socks5://127.0.0.1:7890
export HTTPS_PROXY=socks5://127.0.0.1:7890
export ALL_PROXY=socks5://127.0.0.1:7890

echo "🔧 代理设置:"
echo "   HTTP_PROXY=$HTTP_PROXY"
echo "   HTTPS_PROXY=$HTTPS_PROXY"
echo "   ALL_PROXY=$ALL_PROXY"

echo ""
echo "🌐 测试币安API连接..."

# 测试获取交易所信息
echo "📊 获取交易所信息..."
RESPONSE=$(curl --proxy socks5://127.0.0.1:7890 --connect-timeout 10 -s "https://api.binance.com/api/v3/exchangeInfo")

if [ $? -eq 0 ]; then
    echo "✅ 币安API连接成功"
    
    # 解析USDT交易对数量
    USDT_SYMBOLS=$(echo "$RESPONSE" | jq -r '.symbols[] | select(.quoteAsset == "USDT" and .status == "TRADING" and .isSpotTradingAllowed == true) | .symbol' | wc -l)
    echo "📈 找到 $USDT_SYMBOLS 个活跃的USDT交易对"
    
    # 显示前10个交易对
    echo "📋 前10个USDT交易对:"
    echo "$RESPONSE" | jq -r '.symbols[] | select(.quoteAsset == "USDT" and .status == "TRADING" and .isSpotTradingAllowed == true) | .symbol' | head -10
    
else
    echo "❌ 币安API连接失败"
    exit 1
fi

echo ""
echo "🎯 如果API测试成功，应用程序应该能够:"
echo "   - 获取400+个USDT交易对"
echo "   - 建立WebSocket连接"
echo "   - 开始收集数据" 