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
	fmt.Println("=== Binance 连接诊断工具 ===")
	fmt.Println()

	// 加载配置
	cfg, err := config.LoadConfig("config/symbols.yaml")
	if err != nil {
		fmt.Printf("❌ 加载配置失败: %v\n", err)
		return
	}

	// 初始化日志
	logging.InitLogger(&config.LogConfig{
		Level:      "info",
		JSONFormat: false,
		OutputPath: "",
	})

	// 1. 测试基本网络连接
	fmt.Println("1. 🌐 测试基本网络连接...")
	testBasicConnectivity()

	// 2. 测试直连 Binance API
	fmt.Println("2. 🔗 测试直连 Binance API...")
	testDirectBinanceConnection()

	// 3. 测试代理连接（如果配置了代理）
	if cfg.HTTP.Proxy != "" {
		fmt.Printf("3. 🔀 测试代理连接 (%s)...\n", cfg.HTTP.Proxy)
		testProxyConnection(cfg.HTTP.Proxy)
	} else {
		fmt.Println("3. ⚠️  未配置代理，跳过代理测试")
	}

	// 4. 测试 Binance Go 客户端
	fmt.Println("4. 📊 测试 Binance Go 客户端...")
	testBinanceClient(cfg)

	// 5. 提供解决方案
	fmt.Println()
	provideSolutions(cfg)
}

func testBasicConnectivity() {
	client := &http.Client{Timeout: 10 * time.Second}

	resp, err := client.Get("https://www.google.com")
	if err != nil {
		fmt.Printf("   ❌ 无法连接到外网: %v\n", err)
		return
	}
	resp.Body.Close()
	fmt.Printf("   ✅ 基本网络连接正常 (状态码: %d)\n", resp.StatusCode)
}

func testDirectBinanceConnection() {
	client := &http.Client{Timeout: 10 * time.Second}

	resp, err := client.Get("https://api.binance.com/api/v3/time")
	if err != nil {
		fmt.Printf("   ❌ 无法直接连接到 Binance API: %v\n", err)
		return
	}
	resp.Body.Close()
	fmt.Printf("   ✅ 可以直接连接到 Binance API (状态码: %d)\n", resp.StatusCode)
}

func testProxyConnection(proxyURL string) {
	proxy, err := url.Parse(proxyURL)
	if err != nil {
		fmt.Printf("   ❌ 代理URL格式错误: %v\n", err)
		return
	}

	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxy),
		},
	}

	resp, err := client.Get("https://api.binance.com/api/v3/time")
	if err != nil {
		fmt.Printf("   ❌ 通过代理无法连接到 Binance API: %v\n", err)
		return
	}
	resp.Body.Close()
	fmt.Printf("   ✅ 通过代理可以连接到 Binance API (状态码: %d)\n", resp.StatusCode)
}

func testBinanceClient(cfg *config.Config) {
	var client *binance.Client

	// 根据配置创建客户端
	if cfg.HTTP.Proxy != "" {
		client = binance.NewProxiedClient("", "", cfg.HTTP.Proxy)
		fmt.Printf("   使用代理客户端: %s\n", cfg.HTTP.Proxy)
	} else {
		client = binance.NewClient("", "")
		client.HTTPClient = cfg.NewHTTPClient()
		fmt.Println("   使用直连客户端")
	}

	// 测试获取服务器时间
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	serverTime, err := client.NewServerTimeService().Do(ctx)
	if err != nil {
		fmt.Printf("   ❌ Binance 客户端调用失败: %v\n", err)
		return
	}

	fmt.Printf("   ✅ Binance 客户端工作正常 (服务器时间: %v)\n",
		time.Unix(serverTime/1000, 0).Format("2006-01-02 15:04:05"))

	// 测试获取少量 K线数据
	klines, err := client.NewKlinesService().
		Symbol("BTCUSDT").
		Interval("1h").
		Limit(2).
		Do(ctx)

	if err != nil {
		fmt.Printf("   ❌ 获取K线数据失败: %v\n", err)
		return
	}

	fmt.Printf("   ✅ 成功获取 %d 条 BTCUSDT K线数据\n", len(klines))
}

func provideSolutions(cfg *config.Config) {
	fmt.Println("=== 🔧 解决方案建议 ===")
	fmt.Println()

	if cfg.HTTP.Proxy != "" {
		fmt.Println("当前使用代理模式，可以尝试：")
		fmt.Println("1. 📝 临时禁用代理测试:")
		fmt.Println("   - 编辑 config/symbols.yaml")
		fmt.Println("   - 将 proxy 行注释掉或设为空字符串")
		fmt.Println("   - 重新运行程序")
		fmt.Println()
		fmt.Println("2. 🔧 检查代理设置:")
		fmt.Println("   - 确保代理软件正在运行")
		fmt.Println("   - 确认代理端口号正确")
		fmt.Println("   - 测试代理是否能访问其他网站")
		fmt.Println()
	} else {
		fmt.Println("当前使用直连模式，可以尝试：")
		fmt.Println("1. 🔀 配置代理:")
		fmt.Println("   - 编辑 config/symbols.yaml")
		fmt.Println("   - 设置 proxy: \"http://127.0.0.1:7890\"")
		fmt.Println("   - 确保有对应的代理软件运行")
		fmt.Println()
	}

	fmt.Println("3. ⏱️  增加超时时间:")
	fmt.Println("   - 编辑 config/symbols.yaml")
	fmt.Println("   - 将 timeout 从 120 增加到 300")
	fmt.Println()

	fmt.Println("4. 🔄 重试机制:")
	fmt.Println("   - 程序已内置重试机制")
	fmt.Println("   - 如果偶尔失败是正常的")
	fmt.Println("   - 可以重新启动程序")
	fmt.Println()

	fmt.Println("5. 🌐 使用备用域名:")
	fmt.Println("   - 如果主域名被封，可以尝试其他 Binance 域名")
	fmt.Println("   - api1.binance.com 或 api2.binance.com")
	fmt.Println()

	fmt.Println("运行诊断完成！根据上述测试结果选择合适的解决方案。")
}
