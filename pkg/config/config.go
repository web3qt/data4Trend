package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config 代表应用程序配置
type Config struct {
	WebSocket   WebSocketConfig   `yaml:"websocket"`
	Database    DatabaseConfig    `yaml:"database"`
	API         APIConfig         `yaml:"api"`
	Kafka       KafkaConfig       `yaml:"kafka"`
	BatchWriter BatchWriterConfig `yaml:"batch_writer"`
	Validator   ValidatorConfig   `yaml:"validator"`
	Backfill    BackfillConfig    `yaml:"backfill"`
	Proxy       ProxyConfig       `yaml:"proxy"`
	Symbols     []string          `yaml:"symbols,omitempty"`
	Interval    string            `yaml:"interval"`
}

// WebSocketConfig 代表WebSocket配置
type WebSocketConfig struct {
	BaseURL           string       `yaml:"base_url"`
	ReconnectInterval int          `yaml:"reconnect_interval"`
	PingInterval      int          `yaml:"ping_interval"`
	MaxRetries        int          `yaml:"max_retries"`
	Timeout           int          `yaml:"timeout"`
	AutoFetchSymbols  bool         `yaml:"auto_fetch_symbols"`
	SymbolFilter      SymbolFilter `yaml:"symbol_filter"`
}

// SymbolFilter 代表交易对过滤配置
type SymbolFilter struct {
	QuoteAsset      string   `yaml:"quote_asset"`
	ExcludePatterns []string `yaml:"exclude_patterns"`
}

// DatabaseConfig 代表数据库配置
type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Table    string `yaml:"table"`
}

// APIConfig 代表API服务器配置
type APIConfig struct {
	Port int    `yaml:"port"`
	Host string `yaml:"host"`
}

// ProxyConfig 代表代理配置
type ProxyConfig struct {
	Enabled bool   `yaml:"enabled"`
	Type    string `yaml:"type"` // http, socks5
	Host    string `yaml:"host"`
	Port    int    `yaml:"port"`
}

// KafkaConfig 代表Kafka配置
type KafkaConfig struct {
	Brokers  []string            `yaml:"brokers"`
	Topic    string              `yaml:"topic"`
	Producer KafkaProducerConfig `yaml:"producer"`
	Consumer KafkaConsumerConfig `yaml:"consumer"`
}

// KafkaProducerConfig 代表Kafka生产者配置
type KafkaProducerConfig struct {
	BatchSize         int    `yaml:"batch_size"`
	BatchTimeout      string `yaml:"batch_timeout"`
	Compression       string `yaml:"compression"`
	MaxMessageBytes   int    `yaml:"max_message_bytes"`
	ChannelBufferSize int    `yaml:"channel_buffer_size"` // 通道缓冲区大小
	FlushBytes        int    `yaml:"flush_bytes"`         // 刷新字节数
	SendTimeout       string `yaml:"send_timeout"`        // 发送超时时间
}

// KafkaConsumerConfig 代表Kafka消费者配置
type KafkaConsumerConfig struct {
	GroupID           string `yaml:"group_id"`
	AutoOffsetReset   string `yaml:"auto_offset_reset"`
	SessionTimeout    string `yaml:"session_timeout"`
	HeartbeatInterval string `yaml:"heartbeat_interval"`
}

// BatchWriterConfig 代表批量写入器配置
type BatchWriterConfig struct {
	BatchSize     int    `yaml:"batch_size"`
	BatchTimeout  string `yaml:"batch_timeout"`
	MaxRetries    int    `yaml:"max_retries"`
	RetryInterval string `yaml:"retry_interval"`
}

// ValidatorConfig 代表验证器服务配置
type ValidatorConfig struct {
	Enabled           bool   `yaml:"enabled"`
	CheckInterval     string `yaml:"check_interval"`
	MaxGapDuration    string `yaml:"max_gap_duration"`
	HistoryDays       int    `yaml:"history_days"`
	BatchSize         int    `yaml:"batch_size"`
	ConcurrentWorkers int    `yaml:"concurrent_workers"`
	AutoBackfill      bool   `yaml:"auto_backfill"`
	BackfillThreshold string `yaml:"backfill_threshold"`
	IntegrationMode   string `yaml:"integration_mode"`
}

// BackfillConfig 代表数据回填配置
type BackfillConfig struct {
	Enabled              bool   `yaml:"enabled"`
	DaysToBackfill       int    `yaml:"days_to_backfill"`
	BatchSize            int    `yaml:"batch_size"`
	RequestInterval      string `yaml:"request_interval"`
	SymbolInterval       string `yaml:"symbol_interval"`
	MaxConcurrentSymbols int    `yaml:"max_concurrent_symbols"`
	RetryAttempts        int    `yaml:"retry_attempts"`
	RetryDelay           string `yaml:"retry_delay"`
}

// LoadConfig 从文件和环境变量加载配置
func LoadConfig(configPath string) (*Config, error) {
	config := &Config{
		WebSocket: WebSocketConfig{
			BaseURL:           "wss://stream.binance.com:9443",
			ReconnectInterval: 5,
			PingInterval:      20,
			MaxRetries:        10,
			Timeout:           30,
		},
		Database: DatabaseConfig{
			Host:     "localhost",
			Port:     8123,
			Database: "data4trend",
			Username: "default",
			Password: "123456",
			Table:    "klines_1m",
		},
		API: APIConfig{
			Port: 8080,
			Host: "0.0.0.0",
		},
		Proxy: ProxyConfig{
			Enabled: false,
			Type:    "http",
			Host:    "127.0.0.1",
			Port:    7890,
		},
		Symbols: []string{
			"BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "DOTUSDT",
			"XRPUSDT", "LTCUSDT", "LINKUSDT", "BCHUSDT", "XLMUSDT",
		},
		Interval: "1m",
	}

	// 如果文件存在则从文件加载
	if configPath != "" {
		if _, err := os.Stat(configPath); err == nil {
			data, err := ioutil.ReadFile(configPath)
			if err != nil {
				return nil, fmt.Errorf("failed to read config file: %w", err)
			}

			if err := yaml.Unmarshal(data, config); err != nil {
				return nil, fmt.Errorf("failed to parse config file: %w", err)
			}
		}
	}

	// 用环境变量覆盖配置
	overrideWithEnv(config)

	return config, nil
}

// overrideWithEnv 用环境变量覆盖配置
func overrideWithEnv(config *Config) {
	if host := os.Getenv("CLICKHOUSE_HOST"); host != "" {
		config.Database.Host = host
	}
	if port := os.Getenv("CLICKHOUSE_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			config.Database.Port = p
		}
	}
	if db := os.Getenv("CLICKHOUSE_DB"); db != "" {
		config.Database.Database = db
	}
	if user := os.Getenv("CLICKHOUSE_USER"); user != "" {
		config.Database.Username = user
	}
	if pass := os.Getenv("CLICKHOUSE_PASSWORD"); pass != "" {
		config.Database.Password = pass
	}
	if table := os.Getenv("CLICKHOUSE_TABLE"); table != "" {
		config.Database.Table = table
	}

	// 从环境变量获取代理配置
	if httpProxy := os.Getenv("HTTP_PROXY"); httpProxy != "" {
		config.Proxy.Enabled = true
		parseProxyURL(httpProxy, &config.Proxy)
	} else if httpProxy := os.Getenv("http_proxy"); httpProxy != "" {
		config.Proxy.Enabled = true
		parseProxyURL(httpProxy, &config.Proxy)
	}

	if httpsProxy := os.Getenv("HTTPS_PROXY"); httpsProxy != "" {
		config.Proxy.Enabled = true
		parseProxyURL(httpsProxy, &config.Proxy)
	} else if httpsProxy := os.Getenv("https_proxy"); httpsProxy != "" {
		config.Proxy.Enabled = true
		parseProxyURL(httpsProxy, &config.Proxy)
	}

	if allProxy := os.Getenv("ALL_PROXY"); allProxy != "" {
		config.Proxy.Enabled = true
		parseProxyURL(allProxy, &config.Proxy)
	} else if allProxy := os.Getenv("all_proxy"); allProxy != "" {
		config.Proxy.Enabled = true
		parseProxyURL(allProxy, &config.Proxy)
	}

	// API配置
	if apiPort := os.Getenv("API_PORT"); apiPort != "" {
		if p, err := strconv.Atoi(apiPort); err == nil {
			config.API.Port = p
		}
	}
	if apiHost := os.Getenv("API_HOST"); apiHost != "" {
		config.API.Host = apiHost
	}
}

// parseProxyURL 解析代理URL并更新代理配置
func parseProxyURL(proxyURL string, proxy *ProxyConfig) {
	if strings.HasPrefix(proxyURL, "http://") {
		proxy.Type = "http"
		proxyURL = strings.TrimPrefix(proxyURL, "http://")
	} else if strings.HasPrefix(proxyURL, "https://") {
		proxy.Type = "http"
		proxyURL = strings.TrimPrefix(proxyURL, "https://")
	} else if strings.HasPrefix(proxyURL, "socks5://") {
		proxy.Type = "socks5"
		proxyURL = strings.TrimPrefix(proxyURL, "socks5://")
	}

	parts := strings.Split(proxyURL, ":")
	if len(parts) >= 2 {
		proxy.Host = parts[0]
		if port, err := strconv.Atoi(parts[1]); err == nil {
			proxy.Port = port
		}
	}
}

// GetDSN 返回ClickHouse DSN
func (c *Config) GetDSN() string {
	return fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s",
		c.Database.Username,
		c.Database.Password,
		c.Database.Host,
		c.Database.Port,
		c.Database.Database)
}

// GetProxyURL 返回代理URL
func (c *Config) GetProxyURL() string {
	if !c.Proxy.Enabled {
		return ""
	}
	scheme := "http"
	if c.Proxy.Type == "socks5" {
		scheme = "socks5"
	}
	return fmt.Sprintf("%s://%s:%d", scheme, c.Proxy.Host, c.Proxy.Port)
}
