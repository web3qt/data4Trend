package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/adshao/go-binance/v2"
)

func main() {
	fmt.Println("=== Binance API 连接诊断 ===")

	// 1. 检查环境变量
	fmt.Println("\n1. 检查环境变量:")
	httpProxy := os.Getenv("HTTP_PROXY")
	httpsProxy := os.Getenv("HTTPS_PROXY")
	noProxy := os.Getenv("NO_PROXY")

	fmt.Printf("HTTP_PROXY: %s\n", httpProxy)
	fmt.Printf("HTTPS_PROXY: %s\n", httpsProxy)
	fmt.Printf("NO_PROXY: %s\n", noProxy)

	// 2. 测试不同的超时配置
	timeouts := []time.Duration{30 * time.Second, 60 * time.Second, 120 * time.Second}

	for _, timeout := range timeouts {
		fmt.Printf("\n2. 测试超时配置: %v\n", timeout)
		testBinanceWithTimeout(timeout, "")
	}

	// 3. 如果有代理配置，测试代理连接
	if httpProxy != "" {
		fmt.Printf("\n3. 测试代理连接: %s\n", httpProxy)
		testBinanceWithTimeout(60*time.Second, httpProxy)
	}

	// 4. 测试常见的代理配置
	commonProxies := []string{
		"http://127.0.0.1:7890", // 常见的本地代理端口
		"http://127.0.0.1:1087", // ClashX 默认端口
		"http://127.0.0.1:8080", // 另一个常见端口
	}

	fmt.Println("\n4. 测试常见代理配置:")
	for _, proxy := range commonProxies {
		fmt.Printf("测试代理: %s\n", proxy)
		testBinanceWithTimeout(30*time.Second, proxy)
	}
}

func testBinanceWithTimeout(timeout time.Duration, proxyURL string) {
	// 创建自定义HTTP客户端
	transport := &http.Transport{
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   30 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	// 如果指定了代理，配置代理
	if proxyURL != "" {
		if parsedURL, err := url.Parse(proxyURL); err == nil {
			transport.Proxy = http.ProxyURL(parsedURL)
		} else {
			fmt.Printf("  ❌ 代理URL解析失败: %v\n", err)
			return
		}
	}

	client := &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}

		// 创建Binance客户端
	var binanceClient *binance.Client
	if proxyURL != "" {
		// 使用代理时，创建普通客户端然后设置HTTP客户端
		binanceClient = binance.NewClient("", "")
		binanceClient.HTTPClient = client
		fmt.Printf("  🔀 使用自定义HTTP客户端配置代理\n")
	} else {
		// 直连时，创建普通客户端并设置HTTP客户端
		binanceClient = binance.NewClient("", "")
		binanceClient.HTTPClient = client
		fmt.Printf("  🔗 使用直连配置\n")
	}

	// 创建带超时的context
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// 测试简单的ping
	start := time.Now()
	err := binanceClient.NewPingService().Do(ctx)
	duration := time.Since(start)

	if err != nil {
		fmt.Printf("  ❌ 连接失败 (耗时: %v): %v\n", duration, err)
	} else {
		fmt.Printf("  ✅ 连接成功 (耗时: %v)\n", duration)
	}

	// 测试获取K线数据
	start = time.Now()
	_, err = binanceClient.NewKlinesService().
		Symbol("BTCUSDT").
		Interval("1m").
		Limit(1).
		Do(ctx)
	duration = time.Since(start)

	if err != nil {
		fmt.Printf("  ❌ K线数据获取失败 (耗时: %v): %v\n", duration, err)
	} else {
		fmt.Printf("  ✅ K线数据获取成功 (耗时: %v)\n", duration)
	}
}
