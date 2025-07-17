package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/adshao/go-binance/v2"
	"github.com/web3qt/data4Trend/config"
	"github.com/web3qt/data4Trend/pkg/logging"
)

func main() {
	// 初始化日志
	logging.InitLogger(&config.LogConfig{
		Level:      "debug",
		JSONFormat: false,
		OutputPath: "",
	})

	// 加载配置
	cfg, err := config.LoadConfig("config/symbols.yaml")
	if err != nil {
		fmt.Printf("加载配置失败: %v\n", err)
		return
	}

	fmt.Println("=== 币安API连接测试 ===")

	// 测试1：直接HTTP连接
	fmt.Println("1. 测试HTTP连接...")
	client := &http.Client{Timeout: 10 * time.Second}
	
	if cfg.HTTP.Proxy != "" {
		proxyURL, _ := url.Parse(cfg.HTTP.Proxy)
		client.Transport = &http.Transport{Proxy: http.ProxyURL(proxyURL)}
		fmt.Printf("使用代理: %s\n", cfg.HTTP.Proxy)
	} else {
		fmt.Println("不使用代理")
	}

	// 测试币安API连接
	binanceClient := binance.NewClient(cfg.Binance.APIKey, cfg.Binance.SecretKey)
	if cfg.HTTP.Proxy != "" {
		proxyURL, _ := url.Parse(cfg.HTTP.Proxy)
		binanceClient.HTTPClient.Transport = &http.Transport{Proxy: http.ProxyURL(proxyURL)}
	}

	// 测试服务器时间
	fmt.Println("2. 获取服务器时间...")
	serverTime, err := binanceClient.NewServerTimeService().Do(context.Background())
	if err != nil {
		fmt.Printf("获取服务器时间失败: %v\n", err)
	} else {
		fmt.Printf("服务器时间: %v\n", time.Unix(serverTime/1000, 0))
	}

	// 测试交易所信息
	fmt.Println("3. 获取交易所信息...")
	exchangeInfo, err := binanceClient.NewExchangeInfoService().Do(context.Background())
	if err != nil {
		fmt.Printf("获取交易所信息失败: %v\n", err)
		return
	}

	fmt.Printf("交易对数量: %d\n", len(exchangeInfo.Symbols))
	fmt.Printf("交易对示例: BTCUSDT, ETHUSDT 等")

	// 测试获取BTCUSDT的K线数据
	fmt.Println("4. 测试获取BTCUSDT的K线数据...")
	klines, err := binanceClient.NewKlinesService().Symbol("BTCUSDT").
		Interval("1m").Limit(5).Do(context.Background())
	if err != nil {
		fmt.Printf("获取K线数据失败: %v\n", err)
		return
	}

	fmt.Printf("成功获取 %d 条K线数据\n", len(klines))
	for i, k := range klines {
		fmt.Printf("K线 %d: OpenTime=%v, Open=%s, Close=%s\n", 
			i+1, 
			time.Unix(k.OpenTime/1000, 0),
			k.Open, 
			k.Close)
	}

	fmt.Println("=== 测试完成 ===")
}