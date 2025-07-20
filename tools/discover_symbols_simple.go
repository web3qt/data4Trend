package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	binance "github.com/adshao/go-binance/v2"
)

func main() {
	// 创建Binance客户端
	client := binance.NewClient("", "")
	
	// 获取交易所信息
	exchangeInfo, err := client.NewExchangeInfoService().Do(context.Background())
	if err != nil {
		log.Fatal("获取交易所信息失败:", err)
	}

	var symbols []struct {
		Symbol      string
		ListingTime time.Time
		StartTime   time.Time
	}

	minStart := time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)
	
	for _, symbol := range exchangeInfo.Symbols {
		if symbol.QuoteAsset == "USDT" && 
		   symbol.Status == "TRADING" && 
		   symbol.IsSpotTradingAllowed {
			
			listingTime := time.Unix(symbol.ListingTime/1000, 0)
			startTime := listingTime
			if startTime.Before(minStart) {
				startTime = minStart
			}

			symbols = append(symbols, struct {
				Symbol      string
				ListingTime time.Time
				StartTime   time.Time
			}{
				Symbol:      symbol.Symbol,
				ListingTime: listingTime,
				StartTime:   startTime,
			})
		}
	}

	// 按上市时间排序
	sort.Slice(symbols, func(i, j int) bool {
		return symbols[i].ListingTime.Before(symbols[j].ListingTime)
	})

	// 生成YAML配置文件
	yamlContent := `# 自动生成的全币种1分钟K线收集配置
# 包含币安所有USDT交易对，从上市时间开始收集

# 数据库配置
clickhouse:
  host: "localhost"
  port: 9000
  http_port: 8123
  database: "data4trend"
  user: "default"
  password: "123456"
  max_open_conns: 100
  max_idle_conns: 50
  conn_max_lifetime: "1h"

# Binance API配置
binance:
  api_key: ""
  secret_key: ""
  base_url: "https://api.binance.com"

# HTTP客户端配置
http:
  timeout: 60
  proxy: ""

# 性能配置
performance:
  workers: 100
  data_channel_buffer: 20000
  task_queue_size: 5000

# 日志配置
log:
  level: "info"
  format: "json"
  output_path: "logs/all_1m_collection.log"
  max_size: 1000
  max_backups: 20
  max_age: 30
  compress: true

# 符号配置
symbols:
`

	for _, s := range symbols {
		yamlContent += fmt.Sprintf("  - symbol: \"%s\"\n    enabled: true\n    start_time: \"%s\"\n    intervals:\n      - \"1m\"\n", 
			s.Symbol, s.StartTime.Format("2006-01-02T15:04:05Z"))
	}

	yamlContent += `
# 全局设置
settings:
  max_symbols_per_batch: 100
  discovery_enabled: true
  discovery_interval: "30m"
  excluded_symbols:
    - USDCUSDT
    - BUSDUSDT
    - TUSDUSDT
    - USDPUSDT
  smart_collection:
    enabled: true
    use_listing_time: true
    fallback_start: "2019-01-01T00:00:00Z"
    auto_adjust: true
`

	// 保存配置文件
	file, err := os.Create("config/all_symbols_1m_complete.yaml")
	if err != nil {
		log.Fatal("创建配置文件失败:", err)
	}
	defer file.Close()

	_, err = file.WriteString(yamlContent)
	if err != nil {
		log.Fatal("写入配置文件失败:", err)
	}

	// 打印统计信息
	fmt.Printf("=== 币种发现完成 ===\n")
	fmt.Printf("发现USDT交易对总数: %d\n", len(symbols))
	
	// 按年份统计
	yearStats := make(map[int]int)
	for _, s := range symbols {
		year := s.ListingTime.Year()
		yearStats[year]++
	}

	fmt.Printf("\n按年份分布:\n")
	for year := 2017; year <= time.Now().Year(); year++ {
		if count, exists := yearStats[year]; exists {
			fmt.Printf("  %d年: %d个币种\n", year, count)
		}
	}

	fmt.Printf("\n配置文件已生成: config/all_symbols_1m_complete.yaml\n")

	// 显示前20个币种
	fmt.Printf("\n前20个币种:\n")
	for i := 0; i < 20 && i < len(symbols); i++ {
		fmt.Printf("  %2d. %-10s - 上市时间: %s\n", 
			i+1, symbols[i].Symbol, symbols[i].ListingTime.Format("2006-01-02"))
	}

	// 显示最后20个币种
	if len(symbols) > 20 {
		fmt.Printf("\n最后20个币种:\n")
		start := len(symbols) - 20
		if start < 0 {
			start = 0
		}
		for i := start; i < len(symbols); i++ {
			fmt.Printf("  %2d. %-10s - 上市时间: %s\n", 
				i+1, symbols[i].Symbol, symbols[i].ListingTime.Format("2006-01-02"))
		}
	}
}