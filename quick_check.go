package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/adshao/go-binance/v2"
)

func main() {
	fmt.Println("快速测试Binance API连接...")
	
	// 创建带代理的HTTP客户端
	proxyURL, _ := url.Parse("http://127.0.0.1:7890")
	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
		MaxIdleConns:        10,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     30 * time.Second,
	}
	
	httpClient := &http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
	}
	
	// 创建Binance客户端
	client := binance.NewClient("", "")
	client.HTTPClient = httpClient
	
	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	
	fmt.Println("正在测试连接...")
	start := time.Now()
	
	serverTime, err := client.NewServerTimeService().Do(ctx)
	if err != nil {
		fmt.Printf("❌ 连接失败: %v (耗时: %v)\n", err, time.Since(start))
		return
	}
	
	fmt.Printf("✅ 连接成功! 服务器时间: %v (耗时: %v)\n", 
		time.Unix(serverTime/1000, 0).Format("2006-01-02 15:04:05"), 
		time.Since(start))
	
	// 测试获取K线数据
	fmt.Println("正在测试K线数据获取...")
	start = time.Now()
	
	klines, err := client.NewKlinesService().
		Symbol("BTCUSDT").
		Interval("1h").
		Limit(2).
		Do(ctx)
	
	if err != nil {
		fmt.Printf("❌ K线数据获取失败: %v (耗时: %v)\n", err, time.Since(start))
	} else {
		fmt.Printf("✅ K线数据获取成功! 获取到 %d 条数据 (耗时: %v)\n", 
			len(klines), time.Since(start))
	}
}
