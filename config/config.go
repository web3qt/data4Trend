package config

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"gopkg.in/yaml.v2"
)

// Config 主配置结构
type Config struct {
	Symbols    []SymbolConfig    `yaml:"symbols"`
	Binance    BinanceConfig     `yaml:"binance"`
	HTTP       HTTPConfig        `yaml:"http"`
	Performance PerformanceConfig `yaml:"performance"`
	ClickHouse ClickHouseConfig  `yaml:"clickhouse"`
	Log        LogConfig         `yaml:"log"`
}

// SymbolConfig 交易对配置
type SymbolConfig struct {
	Symbol    string `yaml:"symbol"`
	Enabled   bool   `yaml:"enabled"`
	StartTime string `yaml:"start_time,omitempty"`
	Intervals []string `yaml:"intervals"`
}

// BinanceConfig Binance API配置
type BinanceConfig struct {
	APIKey    string `yaml:"api_key"`
	SecretKey string `yaml:"secret_key"`
	BaseURL   string `yaml:"base_url"`
}

// HTTPConfig HTTP配置
type HTTPConfig struct {
	Proxy   string `yaml:"proxy"`
	Timeout int    `yaml:"timeout"`
}

// PerformanceConfig 性能配置
type PerformanceConfig struct {
	Workers           int `yaml:"workers"`
	DataChannelBuffer int `yaml:"data_channel_buffer"`
	TaskQueueSize     int `yaml:"task_queue_size"`
}

// ClickHouseConfig ClickHouse配置
type ClickHouseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	HTTPPort int    `yaml:"http_port"`
	Database string `yaml:"database"`
	Username string `yaml:"username"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

// LogConfig 日志配置
type LogConfig struct {
	Level      string `yaml:"level"`
	JSONFormat bool   `yaml:"json_format"`
	FilePath   string `yaml:"file_path"`
	OutputPath string `yaml:"output_path"`
	MaxSize    int    `yaml:"max_size"`
	MaxBackups int    `yaml:"max_backups"`
	MaxAge     int    `yaml:"max_age"`
	Compress   bool   `yaml:"compress"`
}

// LoadConfig 加载配置文件
func LoadConfig(configPath ...string) (*Config, error) {
	// 如果没有提供配置文件路径，使用默认路径
	path := "config/symbols.yaml"
	if len(configPath) > 0 && configPath[0] != "" {
		path = configPath[0]
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}

	// 设置默认值
	if config.ClickHouse.Host == "" {
		config.ClickHouse.Host = "localhost"
	}
	if config.ClickHouse.Port == 0 {
		config.ClickHouse.Port = 9000
	}
	if config.ClickHouse.Database == "" {
		config.ClickHouse.Database = "crypto_data"
	}

	return &config, nil
}

// SaveConfig 保存配置文件
func SaveConfig(configPath string, config *Config) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("序列化配置失败: %w", err)
	}

	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("写入配置文件失败: %w", err)
	}

	return nil
}

// GetDefaultStartTime 获取默认开始时间
func GetDefaultStartTime() time.Time {
	return time.Now().AddDate(0, 0, -1) // 默认从1天前开始
}

// LoadCollectorState 加载收集器状态（为了兼容性，调用collector_state.go中的函数）
func (c *Config) LoadCollectorState() (map[string]map[string]time.Time, error) {
	return LoadCollectorState()
}

// SaveCollectorState 保存收集器状态（为了兼容性，调用collector_state.go中的函数）
func (c *Config) SaveCollectorState(states map[string]map[string]time.Time) error {
	return SaveCollectorState(states)
}

// UpdateCollectorState 更新收集器状态
func (c *Config) UpdateCollectorState(symbol, interval string, lastTime time.Time) error {
	return UpdateCollectorState(symbol, interval, lastTime)
}

// NewHTTPClient 创建HTTP客户端
func (c *Config) NewHTTPClient() *http.Client {
	client := &http.Client{
		Timeout: time.Duration(c.HTTP.Timeout) * time.Second,
	}
	if c.HTTP.Timeout <= 0 {
		client.Timeout = 30 * time.Second // 默认30秒超时
	}
	return client
}

// Group 组配置
type Group struct {
	StartTime string   `yaml:"start_time"`
	Intervals []string `yaml:"intervals"`
	Symbols   []SymbolConfig
}

// SymbolManager 符号管理器接口
type SymbolManager interface {
	GetAllSymbols() []SymbolConfig
	GetGroup(groupName string) *Group
}

// SimpleSymbolManager 简单符号管理器实现
type SimpleSymbolManager struct {
	symbols []SymbolConfig
}

// GetAllSymbols 获取所有符号
func (s *SimpleSymbolManager) GetAllSymbols() []SymbolConfig {
	return s.symbols
}

// GetSymbolManager 获取符号管理器
func (c *Config) GetSymbolManager() (SymbolManager, error) {
	return &SimpleSymbolManager{symbols: c.Symbols}, nil
}

// NewSymbolManager 创建新的符号管理器
func NewSymbolManager(configPath string, binanceConfig *BinanceConfig) (SymbolManager, error) {
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return nil, err
	}
	return &SimpleSymbolManager{symbols: cfg.Symbols}, nil
}

// GetGroup 获取指定组的配置（简化实现）
func (s *SimpleSymbolManager) GetGroup(groupName string) *Group {
	// 简化实现，返回默认组配置
	intervals := []string{"1m", "5m", "15m", "1h", "4h", "1d"}
	if len(s.symbols) > 0 && len(s.symbols[0].Intervals) > 0 {
		intervals = s.symbols[0].Intervals
	}
	return &Group{
		StartTime: "2024-01-01 00:00:00",
		Intervals: intervals,
		Symbols:   s.symbols,
	}
}