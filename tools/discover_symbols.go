package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	binance "github.com/adshao/go-binance/v2"
)

// SymbolDiscovery 币种发现器
type SymbolDiscovery struct {
	client *binance.Client
}

// NewSymbolDiscovery 创建新的币种发现器
func NewSymbolDiscovery() *SymbolDiscovery {
	return &SymbolDiscovery{
		client: binance.NewClient("", ""),
	}
}

// DiscoverAllSymbols 发现所有可用的USDT交易对
func (sd *SymbolDiscovery) DiscoverAllSymbols() ([]SymbolInfo, error) {
	exchangeInfo, err := sd.client.NewExchangeInfoService().Do(context.Background())
	if err != nil {
		return nil, fmt.Errorf("获取交易所信息失败: %w", err)
	}

	var symbols []SymbolInfo
	now := time.Now()

	for _, symbol := range exchangeInfo.Symbols {
		// 筛选条件：
		// 1. 以USDT结尾的交易对
		// 2. 状态为TRADING
		// 3. 支持现货交易
		if symbol.QuoteAsset == "USDT" && 
		   symbol.Status == "TRADING" && 
		   symbol.IsSpotTradingAllowed {
			
			// 获取上市时间
			listingTime := time.Unix(symbol.ListingTime/1000, 0)
			
			// 计算开始收集时间（从上市时间开始，但不早于2019年）
			startTime := listingTime
			minStart := time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)
			if startTime.Before(minStart) {
				startTime = minStart
			}

			symbolInfo := SymbolInfo{
				Symbol:      symbol.Symbol,
				BaseAsset:   symbol.BaseAsset,
				QuoteAsset:  symbol.QuoteAsset,
				ListingTime: listingTime,
				StartTime:   startTime,
				Enabled:     true,
				Intervals:   []string{"1m"},
			}

			symbols = append(symbols, symbolInfo)
		}
	}

	// 按市值排序（这里使用交易量作为近似）
	// 实际应用中可以使用CoinMarketCap或CoinGecko API获取真实市值数据
	
	return symbols, nil
}

// SymbolInfo 币种信息
type SymbolInfo struct {
	Symbol      string    `json:"symbol"`
	BaseAsset   string    `json:"base_asset"`
	QuoteAsset  string    `json:"quote_asset"`
	ListingTime time.Time `json:"listing_time"`
	StartTime   time.Time `json:"start_time"`
	Enabled     bool      `json:"enabled"`
	Intervals   []string  `json:"intervals"`
}

// GenerateConfig 生成配置文件
func (sd *SymbolDiscovery) GenerateConfig() error {
	symbols, err := sd.DiscoverAllSymbols()
	if err != nil {
		return err
	}

	// 创建增强版配置
	config := EnhancedConfig{
		ClickHouse: ClickHouseConfig{
			Host:             "localhost",
			Port:             9000,
			HTTPPort:         8123,
			Database:         "data4trend",
			User:             "default",
			Password:         "123456",
			MaxOpenConns:     100,
			MaxIdleConns:     50,
			ConnMaxLifetime:  "1h",
		},
		Binance: BinanceConfig{
			APIKey:    "",
			SecretKey: "",
			BaseURL:   "https://api.binance.com",
		},
		Performance: PerformanceConfig{
			Workers:           100,
			DataChannelBuffer: 20000,
			TaskQueueSize:     5000,
		},
		Log: LogConfig{
			Level:      "info",
			Format:     "json",
			OutputPath: "logs/all_1m_collection.log",
			MaxSize:    1000,
			MaxBackups: 20,
			MaxAge:     30,
			Compress:   true,
		},
		Symbols: symbols,
		Settings: SettingsConfig{
			MaxSymbolsPerBatch: 100,
			DiscoveryEnabled:   true,
			DiscoveryInterval:  "30m",
			ExcludedSymbols: []string{
				"USDCUSDT",
				"BUSDUSDT",
				"TUSDUSDT",
			},
			SmartCollection: SmartCollectionConfig{
				Enabled:       true,
				UseListingTime: true,
				FallbackStart: "2019-01-01T00:00:00Z",
				AutoAdjust:    true,
			},
		},
	}

	// 保存配置文件
	configFile, err := os.Create("config/generated_all_symbols_1m.yaml")
	if err != nil {
		return fmt.Errorf("创建配置文件失败: %w", err)
	}
	defer configFile.Close()

	encoder := json.NewEncoder(configFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(config); err != nil {
		return fmt.Errorf("写入配置文件失败: %w", err)
	}

	// 同时生成YAML格式
	yamlConfig := sd.generateYAMLConfig(symbols)
	yamlFile, err := os.Create("config/all_symbols_1m_auto.yaml")
	if err != nil {
		return fmt.Errorf("创建YAML配置文件失败: %w", err)
	}
	defer yamlFile.Close()

	if _, err := yamlFile.WriteString(yamlConfig); err != nil {
		return fmt.Errorf("写入YAML配置文件失败: %w", err)
	}

	fmt.Printf("发现 %d 个USDT交易对\n", len(symbols))
	fmt.Printf("配置文件已生成:\n")
	fmt.Printf("- config/generated_all_symbols_1m.yaml (JSON格式)\n")
	fmt.Printf("- config/all_symbols_1m_auto.yaml (YAML格式)\n")

	return nil
}

func (sd *SymbolDiscovery) generateYAMLConfig(symbols []SymbolInfo) string {
	// 这里简化生成YAML，实际使用gopkg.in/yaml.v2包
	yaml := `# 自动生成的全币种1分钟K线收集配置
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
"

	for _, symbol := range symbols {
		yaml += fmt.Sprintf("  - symbol: \"%s\"\n    enabled: true\n    start_time: \"%s\"\n    intervals:\n      - \"1m\"\n", 
			symbol.Symbol, symbol.StartTime.Format("2006-01-02T15:04:05Z"))
	}

	yaml += `
# 全局设置
settings:
  max_symbols_per_batch: 100
  discovery_enabled: true
  discovery_interval: "30m"
  excluded_symbols:
    - USDCUSDT
    - BUSDUSDT
    - TUSDUSDT
  smart_collection:
    enabled: true
    use_listing_time: true
    fallback_start: "2019-01-01T00:00:00Z"
    auto_adjust: true
`
	return yaml
}

func main() {
	discovery := NewSymbolDiscovery()
	
	// 生成配置文件
	if err := discovery.GenerateConfig(); err != nil {
		log.Fatal("生成配置文件失败:", err)
	}
	
	// 也可以直接运行发现
	symbols, err := discovery.DiscoverAllSymbols()
	if err != nil {
		log.Fatal("发现币种失败:", err)
	}
	
	fmt.Printf("\n=== 发现的USDT交易对统计 ===\n")
	fmt.Printf("总数: %d\n", len(symbols))
	
	// 按年份统计
	yearStats := make(map[int]int)
	for _, symbol := range symbols {
		year := symbol.ListingTime.Year()
		yearStats[year]++
	}
	
	fmt.Printf("\n按年份分布:\n")
	for year := 2017; year <= time.Now().Year(); year++ {
		if count, exists := yearStats[year]; exists {
			fmt.Printf("  %d年: %d个币种\n", year, count)
		}
	}
	
	// 显示前10个
	fmt.Printf("\n前10个币种:\n")
	for i := 0; i < 10 && i < len(symbols); i++ {
		fmt.Printf("  %s - 上市时间: %s\n", symbols[i].Symbol, symbols[i].ListingTime.Format("2006-01-02"))
	}
}