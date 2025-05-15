package trendscanner

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
	"github.com/web3qt/data4Trend/pkg/logging"
)

// TrendScannerConfig 趋势扫描器配置
type TrendScannerConfig struct {
	Database struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
		Name     string `yaml:"name"`
	} `yaml:"database"`

	MA struct {
		Period   int    `yaml:"period"`
		Interval string `yaml:"interval"`
	} `yaml:"ma"`

	Scan struct {
		Workers   int    `yaml:"workers"`
		Interval  string `yaml:"interval"`
		CSVOutput string `yaml:"csv_output"`
	} `yaml:"scan"`

	Trend struct {
		CheckPoints      []string `yaml:"check_points"`
		RequireStrictUp  bool     `yaml:"require_strict_up"`
		ConsecutiveKLines int     `yaml:"consecutive_klines"`
	} `yaml:"trend"`
}

// LoadConfig 从文件加载配置
func LoadConfig(configPath string) (*TrendScannerConfig, error) {
	// 如果未指定配置文件路径，使用默认路径
	if configPath == "" {
		configPath = "config/trend_scanner.yaml"
	}

	// 读取配置文件
	data, err := os.ReadFile(configPath)
	if err != nil {
		// 如果找不到配置文件，尝试创建默认配置
		if os.IsNotExist(err) {
			logging.Logger.Warnf("配置文件 %s 不存在，将使用默认配置", configPath)
			config := DefaultConfig()
			
			// 确保目录存在
			dir := filepath.Dir(configPath)
			if err := os.MkdirAll(dir, 0755); err != nil {
				return nil, fmt.Errorf("创建配置目录失败: %w", err)
			}
			
			// 保存默认配置
			if err := SaveConfig(config, configPath); err != nil {
				logging.Logger.Warnf("保存默认配置失败: %v", err)
			}
			
			return config, nil
		}
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	// 解析配置
	config := &TrendScannerConfig{}
	if err := yaml.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}

	// 验证配置
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("配置验证失败: %w", err)
	}

	return config, nil
}

// SaveConfig 保存配置到文件
func SaveConfig(config *TrendScannerConfig, configPath string) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("序列化配置失败: %w", err)
	}

	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("保存配置文件失败: %w", err)
	}

	return nil
}

// DefaultConfig 返回默认配置
func DefaultConfig() *TrendScannerConfig {
	config := &TrendScannerConfig{}
	
	// 数据库默认配置
	config.Database.Host = "localhost"
	config.Database.Port = 3306
	config.Database.User = "root"
	config.Database.Password = ""
	config.Database.Name = "data4trend"
	
	// MA线默认配置
	config.MA.Period = 81
	config.MA.Interval = "15m"
	
	// 扫描默认配置
	config.Scan.Workers = 4
	config.Scan.Interval = "1h"
	config.Scan.CSVOutput = "trend_results"
	
	// 趋势条件默认配置
	config.Trend.CheckPoints = []string{"10m", "30m", "1h", "4h", "1d"}
	config.Trend.RequireStrictUp = false
	config.Trend.ConsecutiveKLines = 10
	
	return config
}

// validateConfig 验证配置的有效性
func validateConfig(config *TrendScannerConfig) error {
	// 验证扫描间隔
	_, err := time.ParseDuration(config.Scan.Interval)
	if err != nil {
		return fmt.Errorf("无效的扫描间隔: %s", config.Scan.Interval)
	}
	
	// 验证MA间隔
	validIntervals := map[string]bool{
		"1m": true, "3m": true, "5m": true, "15m": true, "30m": true,
		"1h": true, "2h": true, "4h": true, "6h": true, "8h": true, "12h": true,
		"1d": true, "3d": true, "1w": true, "1M": true,
	}
	if !validIntervals[config.MA.Interval] {
		return fmt.Errorf("无效的K线间隔: %s", config.MA.Interval)
	}
	
	// 验证工作协程数
	if config.Scan.Workers < 1 {
		return fmt.Errorf("工作协程数必须大于0")
	}
	
	// 验证MA周期
	if config.MA.Period < 1 {
		return fmt.Errorf("MA周期必须大于0")
	}
	
	return nil
}

// GetScanInterval 获取扫描间隔的Duration类型
func (c *TrendScannerConfig) GetScanInterval() time.Duration {
	interval, err := time.ParseDuration(c.Scan.Interval)
	if err != nil {
		// 如果解析失败，返回默认值1小时
		return 1 * time.Hour
	}
	return interval
}

// GetCheckPointDurations 获取检查点的Duration类型数组
func (c *TrendScannerConfig) GetCheckPointDurations() []time.Duration {
	durations := make([]time.Duration, 0, len(c.Trend.CheckPoints))
	
	for _, cp := range c.Trend.CheckPoints {
		d, err := time.ParseDuration(cp)
		if err != nil {
			// 跳过无效的时间点
			continue
		}
		durations = append(durations, d)
	}
	
	return durations
} 