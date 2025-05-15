package trendscanner

import (
	"context"
	"time"

	"gorm.io/gorm"
)

// ScanTask 定义了扫描任务的接口
type ScanTask interface {
	// Name 返回任务名称
	Name() string
	
	// Execute 执行扫描任务，返回扫描结果
	Execute(ctx context.Context, db *gorm.DB, symbol string) (*TaskResult, error)
	
	// IsEnabled 判断任务是否启用
	IsEnabled() bool
	
	// SetEnabled 设置任务启用状态
	SetEnabled(enabled bool)
	
	// Config 返回任务配置
	Config() map[string]interface{}
	
	// SetConfig 设置任务配置
	SetConfig(config map[string]interface{}) error
}

// BaseTask 实现了ScanTask接口的基础结构
type BaseTask struct {
	TaskName      string
	EnabledStatus bool
	ConfigParams  map[string]interface{}
}

// Name 返回任务名称
func (t *BaseTask) Name() string {
	return t.TaskName
}

// IsEnabled 判断任务是否启用
func (t *BaseTask) IsEnabled() bool {
	return t.EnabledStatus
}

// SetEnabled 设置任务启用状态
func (t *BaseTask) SetEnabled(enabled bool) {
	t.EnabledStatus = enabled
}

// Config 返回任务配置
func (t *BaseTask) Config() map[string]interface{} {
	return t.ConfigParams
}

// SetConfig 设置任务配置
func (t *BaseTask) SetConfig(config map[string]interface{}) error {
	t.ConfigParams = config
	return nil
}

// TaskResult 表示任务扫描结果
type TaskResult struct {
	Symbol        string
	TaskName      string
	Found         bool
	FoundTime     time.Time
	KLineTime     time.Time
	KLineEndTime  time.Time
	Values        map[string]float64 // 存储任务特定的值，如振幅、波动率等
	Descriptions  []string           // 存储结果的描述信息
}

// NewBaseTask 创建一个新的基础任务
func NewBaseTask(name string) *BaseTask {
	return &BaseTask{
		TaskName:      name,
		EnabledStatus: true,
		ConfigParams:  make(map[string]interface{}),
	}
}

// GetConfigFloat 获取配置中的浮点数值
func (t *BaseTask) GetConfigFloat(key string, defaultVal float64) float64 {
	if val, ok := t.ConfigParams[key]; ok {
		if floatVal, ok := val.(float64); ok {
			return floatVal
		}
	}
	return defaultVal
}

// GetConfigInt 获取配置中的整数值
func (t *BaseTask) GetConfigInt(key string, defaultVal int) int {
	if val, ok := t.ConfigParams[key]; ok {
		switch v := val.(type) {
		case int:
			return v
		case float64:
			return int(v)
		}
	}
	return defaultVal
}

// GetConfigString 获取配置中的字符串值
func (t *BaseTask) GetConfigString(key string, defaultVal string) string {
	if val, ok := t.ConfigParams[key]; ok {
		if strVal, ok := val.(string); ok {
			return strVal
		}
	}
	return defaultVal
}

// GetConfigBool 获取配置中的布尔值
func (t *BaseTask) GetConfigBool(key string, defaultVal bool) bool {
	if val, ok := t.ConfigParams[key]; ok {
		if boolVal, ok := val.(bool); ok {
			return boolVal
		}
	}
	return defaultVal
}

// GetConfigDuration 获取配置中的时间间隔
func (t *BaseTask) GetConfigDuration(key string, defaultVal time.Duration) time.Duration {
	if val, ok := t.ConfigParams[key]; ok {
		if strVal, ok := val.(string); ok {
			if duration, err := time.ParseDuration(strVal); err == nil {
				return duration
			}
		}
	}
	return defaultVal
} 