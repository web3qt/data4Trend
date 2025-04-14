package config

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// SaveConfig 保存配置到文件
func (c *Config) SaveConfig() error {
	configPath := "config/config.yaml"

	// 备份当前配置文件
	backupPath := fmt.Sprintf("config/config_backup_%s.yaml", time.Now().Format("20060102_150405"))
	if data, err := os.ReadFile(configPath); err == nil {
		if err := os.WriteFile(backupPath, data, 0644); err != nil {
			log.Printf("警告: 无法创建配置备份: %v", err)
		} else {
			log.Printf("已创建配置备份: %s", backupPath)
		}
	}

	// 将当前配置序列化为YAML
	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("序列化配置失败: %w", err)
	}

	// 写入到文件
	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("写入配置文件失败: %w", err)
	}

	log.Printf("配置已保存到: %s", configPath)
	return nil
}

// SaveCollectorState 保存收集器状态
func (c *Config) SaveCollectorState(states map[string]map[string]time.Time) error {
	// 如果没有指定符号配置路径，使用默认路径
	stateFilePath := "config/collector_state.yaml"

	// 创建状态目录（如果不存在）
	stateDir := filepath.Dir(stateFilePath)
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		return fmt.Errorf("创建状态目录失败: %w", err)
	}

	// 将状态信息序列化为易读的格式
	type intervalState struct {
		Interval  string `yaml:"interval"`
		StartTime string `yaml:"start_time"`
	}

	type symbolState struct {
		Symbol    string          `yaml:"symbol"`
		Intervals []intervalState `yaml:"intervals"`
		UpdatedAt string          `yaml:"updated_at"`
	}

	stateData := struct {
		States    []symbolState `yaml:"states"`
		UpdatedAt string        `yaml:"updated_at"`
	}{
		UpdatedAt: time.Now().Format(time.RFC3339),
	}

	// 转换状态数据
	for symbol, intervals := range states {
		symState := symbolState{
			Symbol:    symbol,
			UpdatedAt: time.Now().Format(time.RFC3339),
		}

		for interval, startTime := range intervals {
			symState.Intervals = append(symState.Intervals, intervalState{
				Interval:  interval,
				StartTime: startTime.Format(time.RFC3339),
			})
		}

		stateData.States = append(stateData.States, symState)
	}

	// 序列化为YAML
	data, err := yaml.Marshal(stateData)
	if err != nil {
		return fmt.Errorf("序列化状态数据失败: %w", err)
	}

	// 写入文件
	if err := os.WriteFile(stateFilePath, data, 0644); err != nil {
		return fmt.Errorf("写入状态文件失败: %w", err)
	}

	log.Printf("收集器状态已保存到: %s", stateFilePath)
	return nil
}

// UpdateSymbols 更新所有符号的配置
func (sm *SymbolManager) UpdateSymbols(symbols []SymbolConfig) error {
	// 首先获取锁
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.config == nil {
		return fmt.Errorf("符号管理器配置为空")
	}

	// 更新所有组中的时间点
	for _, symbol := range symbols {
		// 查找包含此符号的组
		for groupName, group := range sm.config.Groups {
			if !contains(group.Symbols, symbol.Symbol) {
				continue
			}

			// 更新组中的开始时间
			if group.StartTimes == nil {
				group.StartTimes = make(map[string]string)
			}

			if symbol.MinuteStart != "" {
				group.StartTimes["minute"] = symbol.MinuteStart
			}
			if symbol.HourlyStart != "" {
				group.StartTimes["hourly"] = symbol.HourlyStart
			}
			if symbol.DailyStart != "" {
				group.StartTimes["daily"] = symbol.DailyStart
			}

			// 更新组
			sm.config.Groups[groupName] = group
		}

		// 检查是否有特殊配置需要更新
		if specificConfig, exists := sm.config.Symbols[symbol.Symbol]; exists {
			if specificConfig.StartTimes == nil {
				specificConfig.StartTimes = make(map[string]string)
			}

			if symbol.MinuteStart != "" {
				specificConfig.StartTimes["minute"] = symbol.MinuteStart
			}
			if symbol.HourlyStart != "" {
				specificConfig.StartTimes["hourly"] = symbol.HourlyStart
			}
			if symbol.DailyStart != "" {
				specificConfig.StartTimes["daily"] = symbol.DailyStart
			}

			// 更新特殊配置
			sm.config.Symbols[symbol.Symbol] = specificConfig
		}
	}

	// 保存配置到文件
	return sm.SaveConfig()
}
