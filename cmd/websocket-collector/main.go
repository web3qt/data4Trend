package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/web3qt/data4Trend/config"
	"github.com/web3qt/data4Trend/internal/types"
	"github.com/web3qt/data4Trend/pkg/apiserver"
	"github.com/web3qt/data4Trend/pkg/datacollector"
	"github.com/web3qt/data4Trend/pkg/datastore"
	"github.com/web3qt/data4Trend/pkg/logging"
)

func main() {
	fmt.Println("===============================================")
	fmt.Println("  币安WebSocket 1分钟数据收集器")
	fmt.Println("  专门收集所有币安代币的1分钟K线数据")
	fmt.Println("  数据保留期：7天")
	fmt.Println("===============================================")

	// 命令行参数
	var configFile string
	var initDB bool
	var showHelp bool

	flag.StringVar(&configFile, "config", "config/websocket_1m.yaml", "配置文件路径")
	flag.BoolVar(&initDB, "init-db", false, "初始化数据库表结构")
	flag.BoolVar(&showHelp, "help", false, "显示帮助信息")
	flag.BoolVar(&showHelp, "h", false, "显示帮助信息 (简写)")
	flag.Parse()

	if showHelp {
		fmt.Println("WebSocket数据收集器 - 使用方法:")
		flag.PrintDefaults()
		return
	}

	// 检查配置文件
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		fmt.Printf("错误: 配置文件 %s 不存在\n", configFile)
		fmt.Printf("请确保配置文件存在，或使用 -config 指定正确的配置文件路径\n")
		os.Exit(1)
	}

	fmt.Printf("使用配置文件: %s\n", configFile)

	// 加载配置
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		fmt.Printf("加载配置失败: %v\n", err)
		os.Exit(1)
	}

	// 初始化日志
	logging.InitLogger(&cfg.Log)

	logging.Logger.Info("=====================================")
	logging.Logger.Info("  WebSocket数据收集器启动中...")
	logging.Logger.Info("=====================================")

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建ClickHouse配置
	clickhouseCfg := &datastore.ClickHouseConfig{
		Host:     cfg.ClickHouse.Host,
		Port:     cfg.ClickHouse.Port,
		HTTPPort: cfg.ClickHouse.HTTPPort,
		User:     cfg.ClickHouse.User,
		Password: cfg.ClickHouse.Password,
		Database: cfg.ClickHouse.Database,
	}

	logging.Logger.WithFields(logrus.Fields{
		"host":     cfg.ClickHouse.Host,
		"port":     cfg.ClickHouse.Port,
		"database": cfg.ClickHouse.Database,
		"user":     cfg.ClickHouse.User,
	}).Info("ClickHouse配置")

	// 创建数据通道
	dataChan := make(chan *types.KLineData, cfg.Performance.DataChannelBuffer)

	// 创建物化视图存储
	store, err := datastore.NewMaterializedClickHouseStore(clickhouseCfg, dataChan)
	if err != nil {
		logging.Logger.WithError(err).Fatal("创建ClickHouse存储失败")
	}

	logging.Logger.Info("ClickHouse存储创建成功")

	// 初始化数据库（如果需要）
	if initDB {
		logging.Logger.Info("开始初始化数据库表结构...")
		if err := initializeDatabase(store); err != nil {
			logging.Logger.WithError(err).Fatal("初始化数据库失败")
		}
		logging.Logger.Info("数据库初始化完成")
	}

	// 启动数据存储服务
	go func() {
		store.Start(ctx)
		logging.Logger.Info("数据存储服务已停止")
	}()
	logging.Logger.Info("数据存储服务已启动")

	// 创建WebSocket数据收集器
	collector := datacollector.NewWebSocketCollector(cfg)
	collector.DataChan = dataChan

	logging.Logger.Info("WebSocket数据收集器创建成功")

	// 启动API服务器（可选）
	var server *apiserver.Server
	if cfg.Server.Port > 0 {
		server = apiserver.NewServer(store)
		server.SetPort(cfg.Server.Port)

		go func() {
			logging.Logger.WithField("port", cfg.Server.Port).Info("启动API服务器")
			if err := server.Start(ctx); err != nil {
				logging.Logger.WithError(err).Error("API服务器启动失败")
			}
		}()
	}

	// 启动统计监控
	go startStatsMonitor(ctx, collector, cfg)

	// 启动健康检查
	go startHealthCheck(ctx, collector, cfg)

	fmt.Println("正在启动WebSocket数据收集器...")
	fmt.Println("这将连接到币安所有代币的1分钟K线WebSocket流")
	fmt.Println("数据将自动保留最近7天")

	// 启动WebSocket收集器
	go func() {
		if err := collector.Start(ctx); err != nil {
			logging.Logger.WithError(err).Fatal("WebSocket收集器启动失败")
		}
	}()

	logging.Logger.Info("WebSocket数据收集器启动成功")

	// 等待系统信号
	waitForSignal(cancel)

	// 优雅关闭
	logging.Logger.Info("正在停止服务...")
	collector.Stop()

	if server != nil {
		// API服务器会通过上下文取消自动停止
		logging.Logger.Info("API服务器已停止")
	}

	logging.Logger.Info("WebSocket数据收集器已完全停止")
	fmt.Println("服务已安全停止")
}

// waitForSignal 等待系统信号
func waitForSignal(cancel context.CancelFunc) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	fmt.Printf("\n收到信号 %v，正在停止服务...\n", sig)
	logging.Logger.WithField("signal", sig.String()).Info("收到停止信号")

	cancel()
}

// startStatsMonitor 启动统计监控
func startStatsMonitor(ctx context.Context, collector *datacollector.WebSocketCollector, cfg *config.Config) {
	// 检查是否启用统计
	if !cfg.Monitoring.EnableStats {
		return
	}

	interval := time.Duration(cfg.Monitoring.StatsIntervalMinutes) * time.Minute
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logging.Logger.WithField("interval", interval).Info("统计监控已启动")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 获取统计信息
			activeStreams := collector.GetActiveStreams()
			symbols := collector.GetSymbols()
			dataStats := collector.GetDataStats()

			totalDataPoints := 0
			for _, count := range dataStats {
				totalDataPoints += count
			}

			logging.Logger.WithFields(logrus.Fields{
				"active_streams":    activeStreams,
				"total_symbols":     len(symbols),
				"total_data_points": totalDataPoints,
				"symbols_with_data": len(dataStats),
			}).Info("统计信息")

			fmt.Printf("统计信息 - 活跃连接: %d, 总币种: %d, 数据点: %d\n",
				activeStreams, len(symbols), totalDataPoints)
		}
	}
}

// startHealthCheck 启动健康检查
func startHealthCheck(ctx context.Context, collector *datacollector.WebSocketCollector, cfg *config.Config) {
	if !cfg.Monitoring.EnableHealthCheck {
		return
	}

	interval := time.Duration(cfg.Monitoring.HealthCheckIntervalMinutes) * time.Minute
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logging.Logger.WithField("interval", interval).Info("健康检查已启动")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 执行健康检查
			activeStreams := collector.GetActiveStreams()
			totalSymbols := len(collector.GetSymbols())

			// 检查连接健康度
			connectionRatio := float64(activeStreams) / float64(totalSymbols)

			if connectionRatio < 0.9 { // 如果活跃连接少于90%
				logging.Logger.WithFields(logrus.Fields{
					"active_streams":   activeStreams,
					"total_symbols":    totalSymbols,
					"connection_ratio": connectionRatio,
				}).Warn("连接健康度低")
			} else {
				logging.Logger.WithFields(logrus.Fields{
					"active_streams":   activeStreams,
					"total_symbols":    totalSymbols,
					"connection_ratio": connectionRatio,
				}).Debug("连接健康度正常")
			}
		}
	}
}

// initializeDatabase 初始化数据库
func initializeDatabase(store datastore.Store) error {
	// 检查store是否实现了数据库初始化接口
	if initializer, ok := store.(interface{ InitializeDatabase() error }); ok {
		return initializer.InitializeDatabase()
	}

	logging.Logger.Warn("数据存储不支持自动初始化，请手动创建表结构")
	return nil
}
