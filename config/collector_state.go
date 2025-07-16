package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v2"
)

// CollectorStateData 收集器状态数据
type CollectorStateData struct {
	UpdatedAt time.Time         `yaml:"updated_at"`
	States    []SymbolStateData `yaml:"states"`
}

// SymbolStateData 交易对状态数据
type SymbolStateData struct {
	SymbolState   SymbolInfo         `yaml:"symbol_state"`
	IntervalState []IntervalStateData `yaml:"interval_state"`
}

// SymbolInfo 交易对信息
type SymbolInfo struct {
	Symbol string `yaml:"symbol"`
}

// IntervalStateData 时间间隔状态数据
type IntervalStateData struct {
	Interval string    `yaml:"interval"`
	LastTime time.Time `yaml:"last_time"`
}

const stateFilePath = "config/collector_state.yaml"

// LoadCollectorState 加载收集器状态
func LoadCollectorState() (map[string]map[string]time.Time, error) {
	// 检查文件是否存在
	if _, err := os.Stat(stateFilePath); os.IsNotExist(err) {
		return make(map[string]map[string]time.Time), fmt.Errorf("状态文件不存在")
	}

	// 读取文件
	data, err := os.ReadFile(stateFilePath)
	if err != nil {
		return make(map[string]map[string]time.Time), err
	}

	// 解析YAML
	var stateData CollectorStateData
	if err := yaml.Unmarshal(data, &stateData); err != nil {
		return make(map[string]map[string]time.Time), err
	}

	// 转换为map格式
	states := make(map[string]map[string]time.Time)
	for _, symbolState := range stateData.States {
		symbol := symbolState.SymbolState.Symbol
		intervalStates := make(map[string]time.Time)
		for _, intervalState := range symbolState.IntervalState {
			intervalStates[intervalState.Interval] = intervalState.LastTime
		}
		states[symbol] = intervalStates
	}

	return states, nil
}

// SaveCollectorState 保存收集器状态
func SaveCollectorState(states map[string]map[string]time.Time) error {
	// 确保目录存在
	if err := os.MkdirAll("config", 0755); err != nil {
		return fmt.Errorf("创建config目录失败: %w", err)
	}

	// 转换为YAML格式
	var stateData CollectorStateData
	stateData.UpdatedAt = time.Now()

	for symbol, intervalStates := range states {
		symbolState := SymbolStateData{
			SymbolState: SymbolInfo{Symbol: symbol},
		}

		for interval, lastTime := range intervalStates {
			symbolState.IntervalState = append(symbolState.IntervalState, IntervalStateData{
				Interval: interval,
				LastTime: lastTime,
			})
		}

		stateData.States = append(stateData.States, symbolState)
	}

	// 序列化为YAML
	data, err := yaml.Marshal(&stateData)
	if err != nil {
		return fmt.Errorf("序列化状态数据失败: %w", err)
	}

	// 写入文件
	if err := os.WriteFile(stateFilePath, data, 0644); err != nil {
		return fmt.Errorf("写入状态文件失败: %w", err)
	}

	return nil
}

// UpdateCollectorState 更新特定交易对的状态
func UpdateCollectorState(symbol, interval string, lastTime time.Time) error {
	// 加载现有状态
	states, err := LoadCollectorState()
	if err != nil {
		// 如果加载失败，创建新的状态
		states = make(map[string]map[string]time.Time)
	}

	// 更新状态
	if states[symbol] == nil {
		states[symbol] = make(map[string]time.Time)
	}
	states[symbol][interval] = lastTime

	// 保存状态
	return SaveCollectorState(states)
}