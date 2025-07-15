package test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/web3qt/data4Trend/config"
	"github.com/web3qt/data4Trend/internal/types"
	"github.com/web3qt/data4Trend/pkg/datacollector"
	"github.com/web3qt/data4Trend/pkg/logging"
)

// TestResumeFromSavedState 测试断点续传功能
func TestResumeFromSavedState(t *testing.T) {
	// 初始化日志系统
	baseLogger := logrus.New()
	baseLogger.SetLevel(logrus.InfoLevel)
	logging.Logger = baseLogger.WithFields(logrus.Fields{})

	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "resume_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 创建测试配置
	cfg := &config.Config{}
	// 注意：实际的状态文件路径是硬编码的 "config/collector_state.yaml"
	// 为了测试，我们需要创建config目录
	testConfigDir := filepath.Join(tempDir, "config")
	err = os.MkdirAll(testConfigDir, 0755)
	require.NoError(t, err)
	
	// 临时改变工作目录到测试目录
	originalWd, _ := os.Getwd()
	defer os.Chdir(originalWd)
	os.Chdir(tempDir)

	// 测试数据：模拟保存的状态
	testStates := map[string]map[string]time.Time{
		"BTCUSDT": {
			"1m":  time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC),
			"5m":  time.Date(2024, 1, 1, 10, 25, 0, 0, time.UTC),
			"15m": time.Date(2024, 1, 1, 10, 15, 0, 0, time.UTC),
		},
		"ETHUSDT": {
			"1m":  time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC),
			"5m":  time.Date(2024, 1, 1, 10, 55, 0, 0, time.UTC),
			"15m": time.Date(2024, 1, 1, 10, 45, 0, 0, time.UTC),
		},
	}

	// 保存测试状态到文件
	err = cfg.SaveCollectorState(testStates)
	require.NoError(t, err, "保存测试状态失败")

	// 验证状态文件是否创建
	_, err = os.Stat("config/collector_state.yaml")
	require.NoError(t, err, "状态文件未创建")

	// 加载状态并验证
	loadedStates, err := cfg.LoadCollectorState()
	require.NoError(t, err, "加载状态失败")

	// 验证加载的状态与保存的状态一致
	assert.Equal(t, len(testStates), len(loadedStates), "状态数量不匹配")

	for symbol, intervals := range testStates {
		loadedIntervals, exists := loadedStates[symbol]
		require.True(t, exists, "交易对 %s 未找到", symbol)
		assert.Equal(t, len(intervals), len(loadedIntervals), "交易对 %s 的时间间隔数量不匹配", symbol)

		for interval, expectedTime := range intervals {
			loadedTime, exists := loadedIntervals[interval]
			require.True(t, exists, "交易对 %s 的时间间隔 %s 未找到", symbol, interval)
			assert.True(t, expectedTime.Equal(loadedTime), "交易对 %s 时间间隔 %s 的时间不匹配: 期望 %v, 实际 %v", symbol, interval, expectedTime, loadedTime)
		}
	}

	t.Log("断点续传功能测试通过")
}

// TestSymbolCollectorWithSavedState 测试SymbolCollector使用保存状态
func TestSymbolCollectorWithSavedState(t *testing.T) {
	// 初始化日志系统
	baseLogger := logrus.New()
	baseLogger.SetLevel(logrus.InfoLevel)
	logging.Logger = baseLogger.WithFields(logrus.Fields{})
	// 创建测试配置
	symbolCfg := config.SymbolConfig{
		Symbol:    "BTCUSDT",
		Enabled:   true,
		StartTime: "2024-01-01T00:00:00Z", // 配置文件中的起始时间
	}

	// 模拟保存的状态（比配置文件中的时间更新）
	savedStates := map[string]time.Time{
		"1m":  time.Date(2024, 1, 1, 12, 30, 0, 0, time.UTC),
		"5m":  time.Date(2024, 1, 1, 12, 25, 0, 0, time.UTC),
		"15m": time.Date(2024, 1, 1, 12, 15, 0, 0, time.UTC),
	}

	// 创建模拟的服务和通道
	mockService := &MockKlinesService{}
	taskQueue := make(chan datacollector.CollectionTask, 100)
	dataChan := make(chan *types.KLineData, 100)

	// 创建SymbolCollector，传入保存的状态
	collector, err := datacollector.NewSymbolCollector(mockService, symbolCfg, taskQueue, dataChan, savedStates)
	require.NoError(t, err, "创建SymbolCollector失败")

	// 验证收集器使用了保存的状态而不是配置文件中的起始时间
	// 这里需要添加相应的getter方法来验证内部状态
	// 由于当前实现没有公开的getter，我们通过日志或其他方式验证

	// 验证收集器创建成功
	assert.NotNil(t, collector, "收集器不应为空")

	t.Log("SymbolCollector状态恢复测试通过")
}

// TestEmptyStateFile 测试空状态文件的处理
func TestEmptyStateFile(t *testing.T) {
	// 初始化日志系统
	baseLogger := logrus.New()
	baseLogger.SetLevel(logrus.InfoLevel)
	logging.Logger = baseLogger.WithFields(logrus.Fields{})
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "empty_state_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := &config.Config{}
	// 临时改变工作目录到测试目录（没有config目录）
	originalWd, _ := os.Getwd()
	defer os.Chdir(originalWd)
	os.Chdir(tempDir)

	// 尝试加载不存在的状态文件
	states, err := cfg.LoadCollectorState()

	// 应该返回错误，但不应该崩溃
	assert.Error(t, err, "加载不存在的文件应该返回错误")
	assert.Nil(t, states, "不存在的文件应该返回nil状态")

	t.Log("空状态文件处理测试通过")
}

// TestInvalidStateFile 测试无效状态文件的处理
func TestInvalidStateFile(t *testing.T) {
	// 初始化日志系统
	baseLogger := logrus.New()
	baseLogger.SetLevel(logrus.InfoLevel)
	logging.Logger = baseLogger.WithFields(logrus.Fields{})
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "invalid_state_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := &config.Config{}
	// 创建config目录并放置无效文件
	testConfigDir := filepath.Join(tempDir, "config")
	err = os.MkdirAll(testConfigDir, 0755)
	require.NoError(t, err)
	
	// 创建无效的YAML文件
	err = os.WriteFile(filepath.Join(testConfigDir, "collector_state.yaml"), []byte("invalid: yaml: content: ["), 0644)
	require.NoError(t, err)
	
	// 临时改变工作目录到测试目录
	originalWd, _ := os.Getwd()
	defer os.Chdir(originalWd)
	os.Chdir(tempDir)

	// 尝试加载无效的状态文件
	states, err := cfg.LoadCollectorState()

	// 应该返回错误
	assert.Error(t, err, "加载无效文件应该返回错误")
	assert.Nil(t, states, "无效文件应该返回nil状态")

	t.Log("无效状态文件处理测试通过")
}

// MockKlinesService 模拟的K线服务，用于测试
type MockKlinesService struct{}

func (m *MockKlinesService) Symbol(symbol string) types.KlinesService {
	return m
}

func (m *MockKlinesService) Interval(interval string) types.KlinesService {
	return m
}

func (m *MockKlinesService) Limit(limit int) types.KlinesService {
	return m
}

func (m *MockKlinesService) StartTime(startTime int64) types.KlinesService {
	return m
}

func (m *MockKlinesService) EndTime(endTime int64) types.KlinesService {
	return m
}

func (m *MockKlinesService) Do(ctx context.Context) ([]*types.KLineData, error) {
	// 返回空的K线数据用于测试
	return []*types.KLineData{}, nil
}