#!/bin/bash

echo "=== WebSocket Connection Diagnostic ==="
echo ""

# 检查网络连接
echo "1. Network Connectivity Test:"
echo "   Testing connection to Binance WebSocket..."

# 测试到Binance WebSocket的连接
if curl -s --connect-timeout 10 "https://stream.binance.com:9443" > /dev/null; then
    echo "   ✅ Direct connection to Binance WebSocket successful"
else
    echo "   ❌ Direct connection to Binance WebSocket failed"
fi

# 检查代理设置
echo ""
echo "2. Proxy Configuration:"
if [ -n "$http_proxy" ] || [ -n "$https_proxy" ]; then
    echo "   HTTP_PROXY: $http_proxy"
    echo "   HTTPS_PROXY: $https_proxy"
else
    echo "   No proxy environment variables set"
fi

# 检查SOCKS5代理
echo ""
echo "3. SOCKS5 Proxy Test:"
if nc -z 127.0.0.1 7890 2>/dev/null; then
    echo "   ✅ SOCKS5 proxy on 127.0.0.1:7890 is accessible"
    
    # 测试通过代理连接
    if curl -s --connect-timeout 10 --socks5 127.0.0.1:7890 "https://stream.binance.com:9443" > /dev/null; then
        echo "   ✅ Connection through SOCKS5 proxy successful"
    else
        echo "   ❌ Connection through SOCKS5 proxy failed"
    fi
else
    echo "   ❌ SOCKS5 proxy on 127.0.0.1:7890 is not accessible"
fi

# 检查系统资源
echo ""
echo "4. System Resources:"
echo "   CPU Usage: $(top -l 1 | grep "CPU usage" | awk '{print $3}')"
echo "   Memory Usage: $(top -l 1 | grep "PhysMem" | awk '{print $2}')"
echo "   Network Connections: $(netstat -an | grep ESTABLISHED | wc -l | tr -d ' ')"

# 检查端口占用
echo ""
echo "5. Port Usage:"
echo "   Port 8080 (API): $(lsof -i :8080 2>/dev/null | wc -l | tr -d ' ') connections"
echo "   Port 8123 (ClickHouse): $(lsof -i :8123 2>/dev/null | wc -l | tr -d ' ') connections"
echo "   Port 9092 (Kafka): $(lsof -i :9092 2>/dev/null | wc -l | tr -d ' ') connections"

# 检查日志文件
echo ""
echo "6. Recent Log Analysis:"
if [ -f "logs/collector.log" ]; then
    echo "   Recent WebSocket errors:"
    tail -n 50 logs/collector.log | grep -i "websocket\|connection\|closed" | tail -n 5
else
    echo "   No log file found"
fi

echo ""
echo "=== Diagnostic Complete ==="
echo ""
echo "Recommendations:"
echo "1. If WebSocket connections are failing, check your proxy settings"
echo "2. Ensure your network can reach Binance WebSocket endpoints"
echo "3. Consider reducing the number of symbols if connection limits are hit"
echo "4. Monitor system resources to ensure adequate capacity" 