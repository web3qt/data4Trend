package config

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
	"github.com/adshao/go-binance/v2"
)

// SymbolPriority 定义币种优先级
type SymbolPriority string

const (
	PriorityHigh   SymbolPriority = "high"
	PriorityMedium SymbolPriority = "medium"
	PriorityLow    SymbolPriority = "low"
)

// SymbolGroup 定义币种分组配置
type SymbolGroup struct {
	Symbols       []string          `yaml:"symbols"`
	Intervals     []string          `yaml:"intervals"`
	StartTimes    map[string]string `yaml:"start_times"`
	Enabled       bool              `yaml:"enabled"`
	PollIntervals map[string]string `yaml:"poll_intervals"`
}

// SymbolSpecificConfig 定义单个币种的特殊配置
type SymbolSpecificConfig struct {
	Priority   SymbolPriority    `yaml:"priority"`
	StartTimes map[string]string `yaml:"start_times"`
	Intervals  []string          `yaml:"intervals"`
	Enabled    *bool             `yaml:"enabled"`
}

// SymbolsConfig 定义整个币种配置
type SymbolsConfig struct {
	Groups   map[string]SymbolGroup          `yaml:"groups"`
	Symbols  map[string]SymbolSpecificConfig `yaml:"symbols"`
	Settings struct {
		MaxSymbolsPerBatch int      `yaml:"max_symbols_per_batch"`
		DiscoveryEnabled   bool     `yaml:"discovery_enabled"`
		DiscoveryInterval  string   `yaml:"discovery_interval"`
		ExcludedSymbols    []string `yaml:"excluded_symbols"`
		GlobalStartTime    string   `yaml:"global_start_time"`
	} `yaml:"settings"`
}

// SymbolManager 币种配置管理器
type SymbolManager struct {
	config           *SymbolsConfig
	configPath       string
	mu               sync.RWMutex
	discoveryTicker  *time.Ticker
	discoveryEnabled bool
	binanceConfig    *BinanceConfig
}

// NewSymbolManager 创建新的币种管理器
func NewSymbolManager(configPath string, binanceConfig *BinanceConfig) (*SymbolManager, error) {
	if configPath == "" {
		configPath = "config/symbols.yaml"
	}

	manager := &SymbolManager{
		configPath:    configPath,
		binanceConfig: binanceConfig,
	}

	if err := manager.LoadConfig(); err != nil {
		return nil, err
	}

	return manager, nil
}

// LoadConfig 加载币种配置
func (m *SymbolManager) LoadConfig() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := os.ReadFile(m.configPath)
	if err != nil {
		return fmt.Errorf("读取币种配置文件失败: %w", err)
	}

	var config SymbolsConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("解析币种配置YAML失败: %w", err)
	}

	m.config = &config

	// 设置自动发现
	if m.config.Settings.DiscoveryEnabled {
		m.setupDiscovery()
	}

	log.Printf("币种配置加载成功，共 %d 个分组，%d 个特殊配置",
		len(m.config.Groups), len(m.config.Symbols))

	return nil
}

// SaveConfig 保存币种配置
func (m *SymbolManager) SaveConfig() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, err := yaml.Marshal(m.config)
	if err != nil {
		return fmt.Errorf("序列化币种配置失败: %w", err)
	}

	if err := os.WriteFile(m.configPath, data, 0644); err != nil {
		return fmt.Errorf("写入币种配置文件失败: %w", err)
	}

	log.Printf("币种配置保存成功")
	return nil
}

// GetAllSymbols 获取所有启用的币种
func (m *SymbolManager) GetAllSymbols() []SymbolConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()

	symbols := make([]SymbolConfig, 0)
	symbolMap := make(map[string]bool)

	// 先处理分组配置
	for groupName, group := range m.config.Groups {
		if !group.Enabled {
			continue
		}

		for _, symbol := range group.Symbols {
			// 检查是否在排除列表中
			if contains(m.config.Settings.ExcludedSymbols, symbol) {
				continue
			}

			// 检查是否已添加
			if _, exists := symbolMap[symbol]; exists {
				continue
			}

			// 获取该币种的配置
			symbolConfig := m.getSymbolConfig(symbol, groupName, group)
			symbols = append(symbols, symbolConfig)
			symbolMap[symbol] = true
		}
	}

	return symbols
}

// GetSymbolsInGroup 获取指定分组中的币种配置
func (m *SymbolManager) GetSymbolsInGroup(groupName string) ([]SymbolConfig, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	group, exists := m.config.Groups[groupName]
	if !exists {
		return nil, fmt.Errorf("分组 %s 不存在", groupName)
	}

	if !group.Enabled {
		return nil, fmt.Errorf("分组 %s 未启用", groupName)
	}

	symbols := make([]SymbolConfig, 0, len(group.Symbols))
	for _, symbol := range group.Symbols {
		if contains(m.config.Settings.ExcludedSymbols, symbol) {
			continue
		}

		symbolConfig := m.getSymbolConfig(symbol, groupName, group)
		symbols = append(symbols, symbolConfig)
	}

	return symbols, nil
}

// getSymbolConfig 获取币种配置，应用组配置和特殊配置
func (m *SymbolManager) getSymbolConfig(symbol, groupName string, group SymbolGroup) SymbolConfig {
	// 基础配置（从组中获取）
	config := SymbolConfig{
		Symbol:    symbol,
		Enabled:   group.Enabled,
		Intervals: make([]string, len(group.Intervals)),
	}

	// 复制间隔数组，避免修改原始值
	copy(config.Intervals, group.Intervals)

	// 解析开始时间
	if minute, ok := group.StartTimes["minute"]; ok {
		config.MinuteStart = minute
	}
	if hour, ok := group.StartTimes["hour"]; ok {
		config.HourlyStart = hour
	}
	if day, ok := group.StartTimes["day"]; ok {
		config.DailyStart = day
	}

	// 应用特殊配置（如果存在）
	if specificConfig, exists := m.config.Symbols[symbol]; exists {
		// 应用开始时间
		if specificStartTimes := specificConfig.StartTimes; specificStartTimes != nil {
			if minute, ok := specificStartTimes["minute"]; ok {
				config.MinuteStart = minute
			}
			if hour, ok := specificStartTimes["hour"]; ok {
				config.HourlyStart = hour
			}
			if day, ok := specificStartTimes["day"]; ok {
				config.DailyStart = day
			}
		}

		// 应用间隔
		if specificConfig.Intervals != nil && len(specificConfig.Intervals) > 0 {
			config.Intervals = specificConfig.Intervals
		}

		// 应用启用状态
		if specificConfig.Enabled != nil {
			config.Enabled = *specificConfig.Enabled
		}
	}

	return config
}

// AddSymbol 添加新币种到分组
func (m *SymbolManager) AddSymbol(symbol string, groupName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查分组是否存在
	group, exists := m.config.Groups[groupName]
	if !exists {
		return fmt.Errorf("分组 %s 不存在", groupName)
	}

	// 检查币种是否已存在于该分组
	for _, s := range group.Symbols {
		if s == symbol {
			return fmt.Errorf("币种 %s 已存在于分组 %s", symbol, groupName)
		}
	}

	// 添加到分组
	group.Symbols = append(group.Symbols, symbol)
	m.config.Groups[groupName] = group

	log.Printf("添加币种 %s 到分组 %s", symbol, groupName)
	return m.SaveConfig()
}

// RemoveSymbol 从分组中移除币种
func (m *SymbolManager) RemoveSymbol(symbol string, groupName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查分组是否存在
	group, exists := m.config.Groups[groupName]
	if !exists {
		return fmt.Errorf("分组 %s 不存在", groupName)
	}

	// 找到并移除币种
	found := false
	newSymbols := make([]string, 0, len(group.Symbols))
	for _, s := range group.Symbols {
		if s == symbol {
			found = true
			continue
		}
		newSymbols = append(newSymbols, s)
	}

	if !found {
		return fmt.Errorf("币种 %s 不存在于分组 %s", symbol, groupName)
	}

	// 更新分组
	group.Symbols = newSymbols
	m.config.Groups[groupName] = group

	log.Printf("从分组 %s 移除币种 %s", groupName, symbol)
	return m.SaveConfig()
}

// setupDiscovery 设置自动发现新币种
func (m *SymbolManager) setupDiscovery() {
	// 转换发现间隔
	discoveryInterval, err := time.ParseDuration(m.config.Settings.DiscoveryInterval)
	if err != nil {
		log.Printf("解析发现间隔失败: %v，使用默认值24小时", err)
		discoveryInterval = 24 * time.Hour
	}

	// 停止现有的定时器
	if m.discoveryTicker != nil {
		m.discoveryTicker.Stop()
	}

	// 创建新的定时器
	m.discoveryTicker = time.NewTicker(discoveryInterval)
	m.discoveryEnabled = true

	// 启动发现任务
	go m.runDiscovery()
}

// runDiscovery 运行自动发现任务
func (m *SymbolManager) runDiscovery() {
	// 立即执行一次发现
	m.discoverNewSymbols()

	// 定期执行发现
	for range m.discoveryTicker.C {
		if !m.discoveryEnabled {
			return
		}
		m.discoverNewSymbols()
	}
}

// discoverNewSymbols 发现新币种
func (m *SymbolManager) discoverNewSymbols() {
	log.Printf("开始自动发现新币种")
	
	// 检查是否有"main"分组，如果没有则创建
	m.mu.Lock()
	
	// 使用当前时间作为默认时间
	defaultTime := time.Now().Format(time.RFC3339)
	
	mainGroup, exists := m.config.Groups["main"]
	if !exists {
		log.Printf("未找到'main'分组，将创建新的分组")
		// 创建Groups映射如果不存在
		if m.config.Groups == nil {
			m.config.Groups = make(map[string]SymbolGroup)
		}
		
		// 创建新的main分组
		mainGroup = SymbolGroup{
			Symbols:   []string{},
			Intervals: []string{"15m", "4h", "1d"},
			StartTimes: map[string]string{
				"minute": defaultTime,
				"hour":   defaultTime,
				"day":    defaultTime,
			},
			Enabled: true,
			PollIntervals: map[string]string{
				"15m": "15m",
				"4h":  "4h",
				"1d":  "24h",
			},
		}
		m.config.Groups["main"] = mainGroup
	} else {
		// 如果分组已存在但没有设置开始时间，则设置
		if mainGroup.StartTimes == nil {
			mainGroup.StartTimes = make(map[string]string)
		}
		// 只有在未设置时才更新开始时间
		if _, ok := mainGroup.StartTimes["minute"]; !ok {
			mainGroup.StartTimes["minute"] = defaultTime
		}
		if _, ok := mainGroup.StartTimes["hour"]; !ok {
			mainGroup.StartTimes["hour"] = defaultTime
		}
		if _, ok := mainGroup.StartTimes["day"]; !ok {
			mainGroup.StartTimes["day"] = defaultTime
		}
		m.config.Groups["main"] = mainGroup
	}
	m.mu.Unlock()
	
	// 检查binanceConfig是否为空
	if m.binanceConfig == nil {
		log.Printf("警告: binanceConfig未设置，将使用默认币种列表")
		m.binanceConfig = &BinanceConfig{
			APIKey:    "",
			SecretKey: "",
		}
	}
	
	// 创建一个临时的BinanceClient
	apiKey := m.binanceConfig.APIKey
	secretKey := m.binanceConfig.SecretKey
	
	var result []string
	
	if apiKey == "" || secretKey == "" {
		log.Printf("API密钥未设置，将使用默认币种列表进行更新")
		// 使用常见交易对作为默认列表
		result = []string{
			"BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT",
			"DOGEUSDT", "SOLUSDT", "DOTUSDT", "MATICUSDT", "LTCUSDT",
			"AVAXUSDT", "LINKUSDT", "ATOMUSDT", "UNIUSDT", "ETCUSDT",
			"TRXUSDT", "XLMUSDT", "VETUSDT", "ICPUSDT", "FILUSDT",
			"THETAUSDT", "XMRUSDT", "FTMUSDT", "ALGOUSDT", "HBARUSDT",
		}
	} else {
		// 有API密钥，使用Binance API获取
		client := binance.NewClient(apiKey, secretKey)
		
		// 获取前200个市值最大的币种
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		
		// 获取24小时价格变动信息，包含市值信息
		tickers, err := client.NewListPriceChangeStatsService().Do(ctx)
		if err != nil {
			log.Printf("获取24小时价格变动信息失败: %v", err)
			// 使用备用列表
			result = []string{
				"BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT",
				"DOGEUSDT", "SOLUSDT", "DOTUSDT", "MATICUSDT", "LTCUSDT",
				"AVAXUSDT", "LINKUSDT", "ATOMUSDT", "UNIUSDT", "ETCUSDT",
				"TRXUSDT", "XLMUSDT", "VETUSDT", "ICPUSDT", "FILUSDT",
				"THETAUSDT", "XMRUSDT", "FTMUSDT", "ALGOUSDT", "HBARUSDT",
			}
			log.Printf("使用备用币种列表进行更新，包含%d个币种", len(result))
			return
		}
		
		log.Printf("成功获取到 %d 个交易对信息", len(tickers))
		
		// 过滤USDT交易对并排序
		usdtPairs := make([]*binance.PriceChangeStats, 0)
		for _, ticker := range tickers {
			if strings.HasSuffix(ticker.Symbol, "USDT") {
				usdtPairs = append(usdtPairs, ticker)
			}
		}
		
		log.Printf("过滤出 %d 个USDT交易对", len(usdtPairs))
		
		// 按交易量排序（使用交易量作为市值的代理指标）
		sort.Slice(usdtPairs, func(i, j int) bool {
			// 按交易量（QuoteVolume）降序排序
			vi, _ := strconv.ParseFloat(usdtPairs[i].QuoteVolume, 64)
			vj, _ := strconv.ParseFloat(usdtPairs[j].QuoteVolume, 64)
			return vi > vj
		})
		
		// 取前200个交易对
		count := 200
		if count > len(usdtPairs) {
			count = len(usdtPairs)
		}
		
		result = make([]string, count)
		for i := 0; i < count; i++ {
			result[i] = usdtPairs[i].Symbol
		}
	}
	
	// 检查是否需要更新
	m.mu.RLock()
	mainGroup = m.config.Groups["main"]
	existingSymbols := make(map[string]bool)
	for _, s := range mainGroup.Symbols {
		existingSymbols[s] = true
	}
	m.mu.RUnlock()
	
	// 检查现有币种数量
	log.Printf("现有币种数量: %d", len(mainGroup.Symbols))
	
	// 如果现有币种少于10个，直接使用全部result
	if len(mainGroup.Symbols) < 10 {
		log.Printf("币种数量过少，将直接使用全部默认币种列表")
		m.mu.Lock()
		defer m.mu.Unlock()
		
		// 确保至少有BTC和ETH，然后添加result中的所有币种
		updatedSymbols := make([]string, 0, len(result))
		
		// 先放入BTC和ETH
		updatedSymbols = append(updatedSymbols, "BTCUSDT", "ETHUSDT")
		
		// 添加result中的其他币种（排除已添加的和排除列表中的）
		symbolMap := map[string]bool{
			"BTCUSDT": true,
			"ETHUSDT": true,
		}
		
		for _, s := range result {
			if !symbolMap[s] && !contains(m.config.Settings.ExcludedSymbols, s) {
				updatedSymbols = append(updatedSymbols, s)
				symbolMap[s] = true
			}
		}
		
		// 更新分组
		mainGroup.Symbols = updatedSymbols
		m.config.Groups["main"] = mainGroup
		
		// 保存配置
		if err := m.SaveConfig(); err != nil {
			log.Printf("保存更新的币种配置失败: %v", err)
			return
		}
		
		log.Printf("自动发现完成，总共添加 %d 个币种", len(updatedSymbols))
		return
	}
	
	// 正常流程：只添加新的币种
	newSymbols := make([]string, 0)
	for _, s := range result {
		if !existingSymbols[s] && !contains(m.config.Settings.ExcludedSymbols, s) {
			newSymbols = append(newSymbols, s)
		}
	}
	
	if len(newSymbols) == 0 {
		log.Printf("没有发现新的币种，无需更新")
		return
	}
	
	// 更新符号列表
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// 获取当前的main分组
	mainGroup = m.config.Groups["main"]
	
	// 合并新旧符号列表（确保不会重复添加）
	updatedSymbols := mainGroup.Symbols
	for _, s := range newSymbols {
		if !contains(updatedSymbols, s) {
			updatedSymbols = append(updatedSymbols, s)
		}
	}
	
	// 确保至少有BTC和ETH
	if !contains(updatedSymbols, "BTCUSDT") {
		updatedSymbols = append(updatedSymbols, "BTCUSDT")
	}
	if !contains(updatedSymbols, "ETHUSDT") {
		updatedSymbols = append(updatedSymbols, "ETHUSDT")
	}
	
	// 更新分组
	mainGroup.Symbols = updatedSymbols
	m.config.Groups["main"] = mainGroup
	
	// 保存配置
	if err := m.SaveConfig(); err != nil {
		log.Printf("保存更新的币种配置失败: %v", err)
		return
	}
	
	log.Printf("自动发现完成，更新了 %d 个新币种，总共 %d 个币种", len(newSymbols), len(updatedSymbols))
}

// contains 检查字符串是否在切片中
func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

// GetGroup 获取指定名称的币种组
func (m *SymbolManager) GetGroup(groupName string) *SymbolGroup {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.config == nil {
		return nil
	}

	group, exists := m.config.Groups[groupName]
	if !exists {
		return nil
	}

	// 返回组的副本，以避免并发修改
	groupCopy := group
	return &groupCopy
}
