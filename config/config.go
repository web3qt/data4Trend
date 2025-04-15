package config

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type SymbolConfig struct {
	Symbol      string   `yaml:"symbol"`
	DailyStart  string   `yaml:"daily_start"`
	HourlyStart string   `yaml:"hourly_start"`
	MinuteStart string   `yaml:"minute_start"`
	Enabled     bool     `yaml:"enabled" default:"true"`
	Intervals   []string `yaml:"intervals" default:"[\"1m\", \"1h\", \"1d\"]"`
}

type LogConfig struct {
	Level      string `yaml:"level"`
	JSONFormat bool   `yaml:"json_format"`
	OutputPath string `yaml:"output_path"`
}

type BinanceConfig struct {
	APIKey    string         `yaml:"api_key"`
	SecretKey string         `yaml:"secret_key"`
	Symbols   []SymbolConfig `yaml:"symbols"`
}

type Config struct {
	Log     LogConfig     `yaml:"log"`
	Binance BinanceConfig `yaml:"binance"`
	MySQL   struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
		Database string `yaml:"database"`
	} `yaml:"mysql"`
	HTTP struct {
		Timeout int    `yaml:"timeout"`
		Proxy   string `yaml:"proxy"`
	} `yaml:"http"`
	SymbolsConfigPath string `yaml:"symbols_config_path"`
	// 标记是否已初始化符号管理器
	symbolManager      *SymbolManager
	symbolsInitialized bool
}

func (c *Config) NewHTTPClient() *http.Client {
	transport := &http.Transport{}
	if c.HTTP.Proxy != "" {
		transport.Proxy = func(req *http.Request) (*url.URL, error) {
			return url.Parse(c.HTTP.Proxy)
		}
	}
	return &http.Client{
		Timeout:   time.Duration(c.HTTP.Timeout) * time.Second,
		Transport: transport,
	}
}

// GetSymbolManager 获取币种管理器
func (c *Config) GetSymbolManager() (*SymbolManager, error) {
	if c.symbolManager != nil {
		return c.symbolManager, nil
	}

	configPath := c.SymbolsConfigPath
	if configPath == "" {
		configPath = "config/symbols.yaml"
	}

	manager, err := NewSymbolManager(configPath, &c.Binance)
	if err != nil {
		return nil, err
	}

	c.symbolManager = manager
	c.symbolsInitialized = true

	// 更新配置中的币种列表（兼容旧代码）
	symbols := manager.GetAllSymbols()
	c.Binance.Symbols = symbols

	return manager, nil
}

// GetAllSymbols 获取所有启用的币种
func (c *Config) GetAllSymbols() ([]SymbolConfig, error) {
	manager, err := c.GetSymbolManager()
	if err != nil {
		return nil, err
	}

	return manager.GetAllSymbols(), nil
}

// replaceEnvVars 函数已被修改，不再替换环境变量
func replaceEnvVars(input string) string {
	// 不再替换环境变量，直接返回原始输入
	return input
}

// LoadEnv 函数已被移除，不再使用环境变量
func LoadEnv() error {
	// 不再加载环境变量文件
	log.Println("配置直接从config.yaml加载，不使用环境变量")
	return nil
}

func LoadConfig(path ...string) (*Config, error) {
	configPath := "config/config.yaml"
	if len(path) > 0 {
		configPath = path[0]
	}

	log.Println("开始加载配置文件...")

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	// 不再替换环境变量
	content := string(data)
	// 直接使用配置文件内容
	processedData := content

	var cfg Config
	if err := yaml.Unmarshal([]byte(processedData), &cfg); err != nil {
		return nil, fmt.Errorf("解析YAML配置失败: %w", err)
	}

	// 从环境变量获取Binance API密钥
	cfg.Binance.APIKey = os.Getenv("BINANCE_API_KEY")
	cfg.Binance.SecretKey = os.Getenv("BINANCE_SECRET_KEY")

	// 确保每个交易对的配置都有默认值
	for i := range cfg.Binance.Symbols {
		// 确保启用状态
		if !cfg.Binance.Symbols[i].Enabled {
			cfg.Binance.Symbols[i].Enabled = true
		}

		// 确保时间周期
		if len(cfg.Binance.Symbols[i].Intervals) == 0 {
			cfg.Binance.Symbols[i].Intervals = []string{"1m", "1h", "1d"}
		}
	}

	// 尝试初始化币种管理器（如果配置文件存在）
	if cfg.SymbolsConfigPath != "" {
		_, err := cfg.GetSymbolManager()
		if err != nil {
			log.Printf("警告: 无法加载币种配置文件: %v，将使用主配置中的币种", err)
		}
	}

	fmt.Printf("配置加载成功: MySQL数据库=%s, Binance交易对数量=%d\n",
		cfg.MySQL.Database, len(cfg.Binance.Symbols))

	return &cfg, nil
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		Log: LogConfig{
			Level:      "debug", // 改为 debug 级别
			JSONFormat: false,
			OutputPath: "",
		},
		Binance: BinanceConfig{
			APIKey:    "",
			SecretKey: "",
			Symbols:   []SymbolConfig{},
		},
		MySQL: struct {
			Host     string `yaml:"host"`
			Port     int    `yaml:"port"`
			User     string `yaml:"user"`
			Password string `yaml:"password"`
			Database string `yaml:"database"`
		}{
			Host:     "",
			Port:     0,
			User:     "",
			Password: "",
			Database: "",
		},
		HTTP: struct {
			Timeout int    `yaml:"timeout"`
			Proxy   string `yaml:"proxy"`
		}{
			Timeout: 0,
			Proxy:   "",
		},
		SymbolsConfigPath:  "",
		symbolManager:      nil,
		symbolsInitialized: false,
	}
}
