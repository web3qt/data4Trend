package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/web3qt/data4Trend/config"
	"github.com/web3qt/data4Trend/internal/types"
	"github.com/web3qt/data4Trend/pkg/datacollector"
	"github.com/web3qt/data4Trend/pkg/logging"
)

// ResumeDemo 演示断点续传功能
func main() {
	fmt.Println("=== 断点续传功能演示 ===")

	// 初始化日志系统
	baseLogger := logrus.New()
	baseLogger.SetLevel(logrus.InfoLevel)
	baseLogger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	logging.Logger = baseLogger.WithFields(logrus.Fields{})

	// 创建配置
	cfg := &config.Config{}

	// 模拟一些状态数据
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

	fmt.Println("\n1. 保存收集器状态...")
	// 保存状态到文件
	if err := cfg.SaveCollectorState(testStates); err != nil {
		log.Fatalf("保存状态失败: %v", err)
	}
	fmt.Println("✓ 状态已保存到 config/collector_state.yaml")

	fmt.Println("\n2. 加载收集器状态...")
	// 加载状态
	loadedStates, err := cfg.LoadCollectorState()
	if err != nil {
		log.Fatalf("加载状态失败: %v", err)
	}
	fmt.Printf("✓ 成功加载 %d 个交易对的状态\n", len(loadedStates))

	// 显示加载的状态
	fmt.Println("\n3. 加载的状态详情:")
	for symbol, intervals := range loadedStates {
		fmt.Printf("  交易对: %s\n", symbol)
		for interval, startTime := range intervals {
			fmt.Printf("    %s: %s\n", interval, startTime.Format("2006-01-02 15:04:05"))
		}
	}

	fmt.Println("\n4. 演示SymbolCollector使用保存的状态...")
	// 创建测试配置
	symbolCfg := config.SymbolConfig{
		Symbol:    "BTCUSDT",
		Enabled:   true,
		StartTime: "2024-01-01T00:00:00Z", // 配置文件中的起始时间
		Intervals: []string{"1m", "5m", "15m"},
	}

	// 获取该交易对的保存状态
	var symbolSavedStates map[string]time.Time
	if symbolStates, exists := loadedStates[symbolCfg.Symbol]; exists {
		symbolSavedStates = symbolStates
		fmt.Printf("✓ 找到交易对 %s 的保存状态，包含 %d 个时间间隔\n", symbolCfg.Symbol, len(symbolStates))
	} else {
		fmt.Printf("⚠ 未找到交易对 %s 的保存状态，将使用配置文件中的起始时间\n", symbolCfg.Symbol)
	}

	// 创建模拟的服务和通道
	mockService := &MockKlinesService{}
	taskQueue := make(chan datacollector.CollectionTask, 100)
	dataChan := make(chan *types.KLineData, 100)

	// 创建SymbolCollector，传入保存的状态
	collector, err := datacollector.NewSymbolCollector(mockService, symbolCfg, taskQueue, dataChan, symbolSavedStates)
	if err != nil {
		log.Fatalf("创建SymbolCollector失败: %v", err)
	}
	fmt.Printf("✓ 成功创建SymbolCollector，支持断点续传\n")

	fmt.Println("\n5. 模拟更新收集进度...")
	// 模拟更新状态
	updatedStates := map[string]map[string]time.Time{
		"BTCUSDT": {
			"1m":  time.Date(2024, 1, 1, 12, 30, 0, 0, time.UTC), // 更新的时间
			"5m":  time.Date(2024, 1, 1, 12, 25, 0, 0, time.UTC),
			"15m": time.Date(2024, 1, 1, 12, 15, 0, 0, time.UTC),
		},
		"ETHUSDT": {
			"1m":  time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC), // 更新的时间
			"5m":  time.Date(2024, 1, 1, 12, 55, 0, 0, time.UTC),
			"15m": time.Date(2024, 1, 1, 12, 45, 0, 0, time.UTC),
		},
	}

	// 保存更新的状态
	if err := cfg.SaveCollectorState(updatedStates); err != nil {
		log.Fatalf("保存更新状态失败: %v", err)
	}
	fmt.Println("✓ 收集进度已更新并保存")

	fmt.Println("\n6. 验证状态更新...")
	// 重新加载状态验证更新
	newLoadedStates, err := cfg.LoadCollectorState()
	if err != nil {
		log.Fatalf("重新加载状态失败: %v", err)
	}

	// 显示更新后的状态
	fmt.Println("  更新后的状态:")
	for symbol, intervals := range newLoadedStates {
		fmt.Printf("  交易对: %s\n", symbol)
		for interval, startTime := range intervals {
			fmt.Printf("    %s: %s\n", interval, startTime.Format("2006-01-02 15:04:05"))
		}
	}

	fmt.Println("\n=== 断点续传功能演示完成 ===")
	fmt.Println("\n主要特性:")
	fmt.Println("✓ 自动保存收集进度到 config/collector_state.yaml")
	fmt.Println("✓ 系统重启时自动恢复上次的收集进度")
	fmt.Println("✓ 支持多个交易对和时间间隔的独立状态管理")
	fmt.Println("✓ 优雅处理状态文件不存在或损坏的情况")
	fmt.Println("✓ 实时更新和持久化收集进度")

	// 等待用户中断
	fmt.Println("\n按 Ctrl+C 退出演示...")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	// 清理演示文件
	fmt.Println("\n清理演示文件...")
	if err := os.RemoveAll("config"); err != nil {
		fmt.Printf("清理文件失败: %v\n", err)
	} else {
		fmt.Println("✓ 演示文件已清理")
	}

	// 避免编译器警告
	_ = collector
}

// MockKlinesService 模拟的K线服务，用于演示
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
	// 返回空的K线数据用于演示
	return []*types.KLineData{}, nil
}