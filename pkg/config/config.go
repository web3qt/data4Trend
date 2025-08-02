package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config represents the application configuration
type Config struct {
	WebSocket    WebSocketConfig    `yaml:"websocket"`
	Database     DatabaseConfig     `yaml:"database"`
	API          APIConfig          `yaml:"api"`
	Kafka        KafkaConfig        `yaml:"kafka"`
	BatchWriter  BatchWriterConfig  `yaml:"batch_writer"`
	Validator    ValidatorConfig    `yaml:"validator"`
	Proxy        ProxyConfig        `yaml:"proxy"`
	Symbols      []string           `yaml:"symbols,omitempty"`
	Interval     string             `yaml:"interval"`
}

// WebSocketConfig represents WebSocket configuration
type WebSocketConfig struct {
	BaseURL           string       `yaml:"base_url"`
	ReconnectInterval int          `yaml:"reconnect_interval"`
	PingInterval      int          `yaml:"ping_interval"`
	MaxRetries        int          `yaml:"max_retries"`
	Timeout           int          `yaml:"timeout"`
	AutoFetchSymbols  bool         `yaml:"auto_fetch_symbols"`
	SymbolFilter      SymbolFilter `yaml:"symbol_filter"`
}

// SymbolFilter represents symbol filtering configuration
type SymbolFilter struct {
	QuoteAsset       string   `yaml:"quote_asset"`
	ExcludePatterns  []string `yaml:"exclude_patterns"`
}

// DatabaseConfig represents database configuration
type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Table    string `yaml:"table"`
}

// APIConfig represents API server configuration
type APIConfig struct {
	Port int    `yaml:"port"`
	Host string `yaml:"host"`
}

// ProxyConfig represents proxy configuration
type ProxyConfig struct {
	Enabled bool   `yaml:"enabled"`
	Type    string `yaml:"type"` // http, socks5
	Host    string `yaml:"host"`
	Port    int    `yaml:"port"`
}

// KafkaConfig represents Kafka configuration
type KafkaConfig struct {
	Brokers  []string           `yaml:"brokers"`
	Topic    string             `yaml:"topic"`
	Producer KafkaProducerConfig `yaml:"producer"`
	Consumer KafkaConsumerConfig `yaml:"consumer"`
}

// KafkaProducerConfig represents Kafka producer configuration
type KafkaProducerConfig struct {
	BatchSize       int    `yaml:"batch_size"`
	BatchTimeout    string `yaml:"batch_timeout"`
	Compression     string `yaml:"compression"`
	MaxMessageBytes int    `yaml:"max_message_bytes"`
}

// KafkaConsumerConfig represents Kafka consumer configuration
type KafkaConsumerConfig struct {
	GroupID           string `yaml:"group_id"`
	AutoOffsetReset   string `yaml:"auto_offset_reset"`
	SessionTimeout    string `yaml:"session_timeout"`
	HeartbeatInterval string `yaml:"heartbeat_interval"`
}

// BatchWriterConfig represents batch writer configuration
type BatchWriterConfig struct {
	BatchSize     int    `yaml:"batch_size"`
	BatchTimeout  string `yaml:"batch_timeout"`
	MaxRetries    int    `yaml:"max_retries"`
	RetryInterval string `yaml:"retry_interval"`
}

// ValidatorConfig represents validator service configuration
type ValidatorConfig struct {
	Enabled           bool   `yaml:"enabled"`
	CheckInterval     string `yaml:"check_interval"`
	MaxGapDuration    string `yaml:"max_gap_duration"`
	HistoryDays       int    `yaml:"history_days"`
	BatchSize         int    `yaml:"batch_size"`
	ConcurrentWorkers int    `yaml:"concurrent_workers"`
}

// LoadConfig loads configuration from file and environment variables
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

	// Load from file if exists
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

	// Override with environment variables
	overrideWithEnv(config)

	return config, nil
}

// overrideWithEnv overrides configuration with environment variables
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

	// Proxy configuration from environment
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

	// API configuration
	if apiPort := os.Getenv("API_PORT"); apiPort != "" {
		if p, err := strconv.Atoi(apiPort); err == nil {
			config.API.Port = p
		}
	}
	if apiHost := os.Getenv("API_HOST"); apiHost != "" {
		config.API.Host = apiHost
	}
}

// parseProxyURL parses proxy URL and updates proxy config
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

// GetDSN returns the ClickHouse DSN
func (c *Config) GetDSN() string {
	return fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s",
		c.Database.Username,
		c.Database.Password,
		c.Database.Host,
		c.Database.Port,
		c.Database.Database)
}

// GetProxyURL returns the proxy URL
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