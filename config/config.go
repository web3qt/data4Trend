package config

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"gopkg.in/yaml.v2"
)

// Config 主配置结构
type Config struct {
	Symbols        []SymbolConfig       `yaml:"symbols"`
	Binance        BinanceConfig        `yaml:"binance"`
	HTTP           HTTPConfig           `yaml:"http"`
	Performance    PerformanceConfig    `yaml:"performance"`
	ClickHouse     ClickHouseConfig     `yaml:"clickhouse"`
	Log            LogConfig            `yaml:"log"`
	Server         ServerConfig         `yaml:"server"`
	Monitoring     MonitoringConfig     `yaml:"monitoring"`
	DataManagement DataManagementConfig `yaml:"data_management"`
	SymbolFilter   SymbolFilterConfig   `yaml:"symbol_filter"`
	WebSocket      WebSocketConfig      `yaml:"websocket"`
}

// SymbolConfig 交易对配置
type SymbolConfig struct {
	Symbol    string   `yaml:"symbol"`
	Enabled   bool     `yaml:"enabled"`
	StartTime string   `yaml:"start_time,omitempty"`
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

// ServerConfig API服务器配置
type ServerConfig struct {
	Port               int  `yaml:"port"`
	EnableCORS         bool `yaml:"enable_cors"`
	EnableWebSocketAPI bool `yaml:"enable_websocket_api"`
	EnableRestAPI      bool `yaml:"enable_rest_api"`
}

// MonitoringConfig 监控配置
type MonitoringConfig struct {
	EnableStats                 bool `yaml:"enable_stats"`
	StatsIntervalMinutes        int  `yaml:"stats_interval_minutes"`
	EnableHealthCheck           bool `yaml:"enable_health_check"`
	HealthCheckIntervalMinutes  int  `yaml:"health_check_interval_minutes"`
}

// DataManagementConfig 数据管理配置
type DataManagementConfig struct {
	RetentionDays          int `yaml:"retention_days"`
	CleanupIntervalHours   int `yaml:"cleanup_interval_hours"`
	MaxSymbols             int `yaml:"max_symbols"`
}

// SymbolFilterConfig 币种过滤配置
type SymbolFilterConfig struct {
	BaseCurrency         string   `yaml:"base_currency"`
	ExcludedSymbols      []string `yaml:"excluded_symbols"`
	MinVolumeFilter      bool     `yaml:"min_volume_filter"`
	MinVolumeThreshold   float64  `yaml:"min_volume_threshold"`
}

// WebSocketConfig WebSocket连接配置
type WebSocketConfig struct {
	ReconnectDelaySeconds    int `yaml:"reconnect_delay_seconds"`
	MaxReconnectAttempts     int `yaml:"max_reconnect_attempts"`
	PingIntervalSeconds      int `yaml:"ping_interval_seconds"`
	ConnectionTimeoutSeconds int `yaml:"connection_timeout_seconds"`
	ReadTimeoutSeconds       int `yaml:"read_timeout_seconds"`
	WriteTimeoutSeconds      int `yaml:"write_timeout_seconds"`
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

	// 设置服务器配置默认值
	if config.Server.Port == 0 {
		config.Server.Port = 8080
	}

	// 设置监控配置默认值
	if config.Monitoring.StatsIntervalMinutes == 0 {
		config.Monitoring.StatsIntervalMinutes = 5
	}
	if config.Monitoring.HealthCheckIntervalMinutes == 0 {
		config.Monitoring.HealthCheckIntervalMinutes = 1
	}

	// 设置数据管理配置默认值
	if config.DataManagement.RetentionDays == 0 {
		config.DataManagement.RetentionDays = 7
	}
	if config.DataManagement.CleanupIntervalHours == 0 {
		config.DataManagement.CleanupIntervalHours = 1
	}

	// 设置性能配置默认值
	if config.Performance.Workers == 0 {
		config.Performance.Workers = 10
	}
	if config.Performance.DataChannelBuffer == 0 {
		config.Performance.DataChannelBuffer = 50000
	}

	// 设置WebSocket配置默认值
	if config.WebSocket.ReconnectDelaySeconds == 0 {
		config.WebSocket.ReconnectDelaySeconds = 5
	}
	if config.WebSocket.MaxReconnectAttempts == 0 {
		config.WebSocket.MaxReconnectAttempts = 5
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

// Note: 断点续传功能已移除，WebSocket模式不需要状态管理

// NewHTTPClient 创建HTTP客户端
func (c *Config) NewHTTPClient() *http.Client {
	timeout := time.Duration(c.HTTP.Timeout) * time.Second
	if c.HTTP.Timeout <= 0 {
		timeout = 120 * time.Second // 默认120秒超时
	}

	// 创建自定义Transport
	transport := &http.Transport{
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   30 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	// 如果配置了代理，设置代理
	if c.HTTP.Proxy != "" {
		proxyURL, err := url.Parse(c.HTTP.Proxy)
		if err != nil {
			fmt.Printf("代理URL解析失败: %v，将不使用代理\n", err)
		} else {
			transport.Proxy = http.ProxyURL(proxyURL)
			fmt.Printf("使用代理: %s\n", c.HTTP.Proxy)
		}
	}

	// 创建自定义的HTTP客户端，增加超时和重试配置
	client := &http.Client{
		Timeout:   timeout,
		Transport: transport,
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
