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
	"github.com/web3qt/data4Trend/pkg/dataprocessor"
	"github.com/web3qt/data4Trend/pkg/datastore"
	"github.com/web3qt/data4Trend/pkg/logging"
)

func main() {
	// 添加命令行参数
	portFlag := flag.Int("port", 8080, "API服务器端口号")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 初始化配置
	// 加载配置
	cfg, err := config.LoadConfig()
	if err != nil {
		// 初始化前需要使用标准日志
		logFatal("加载配置失败: %v", err)
	}

	// 初始化日志
	logging.InitLogger(&cfg.Log)

	logging.Logger.Info("=====================================")
	logging.Logger.Info("  数据采集服务启动中...")
	logging.Logger.Info("=====================================")

	logging.Logger.Info("配置加载成功")
	logging.Logger.WithField("proxy", cfg.HTTP.Proxy).Info("HTTP代理设置")

	// 创建HTTP客户端并测试连接
	httpClient := cfg.NewHTTPClient()
	testResp, err := httpClient.Get("https://api.binance.com/api/v3/time")
	if err != nil {
		logging.Logger.WithError(err).Warn("无法连接到Binance API")
	} else {
		testResp.Body.Close()
		logging.Logger.WithField("status_code", testResp.StatusCode).Info("成功连接到Binance API")
	}

	// 打印关键配置信息
	logging.Logger.WithFields(logrus.Fields{
		"host":     cfg.MySQL.Host,
		"port":     cfg.MySQL.Port,
		"user":     cfg.MySQL.User,
		"database": cfg.MySQL.Database,
	}).Info("MySQL配置")

	mysqlCfg := &datastore.MySQLConfig{
		Host:     cfg.MySQL.Host,
		Port:     cfg.MySQL.Port,
		User:     cfg.MySQL.User,
		Password: cfg.MySQL.Password,
		Database: cfg.MySQL.Database,
	}

	// 初始化MySQL存储
	store, err := datastore.NewMySQLStore(mysqlCfg, nil) // 先不连接数据通道
	if err != nil {
		logging.Logger.WithError(err).Fatal("初始化MySQL存储失败")
	}

	// 创建API服务器，使用命令行指定的端口
	server := apiserver.NewServer(store)
	server.SetPort(*portFlag)

	// 启动完整数据服务
	logging.Logger.Info("启动完整数据服务...")

	// 创建数据收集器
	collector := datacollector.NewBinanceCollector(cfg)

	// 创建数据清洗器
	cleaner := dataprocessor.NewDataCleaner(collector.DataChan)

	// 连接数据流
	// 1. 收集器 -> 清洗器
	// 2. 清洗器 -> MySQL存储和API服务器（通过分发通道）
	apiChannel := server.GetInputChannel()

	// 创建一个分发通道来复制清洗器的输出数据
	dbChannel := make(chan *types.KLineData, 5000) // 增加MySQL存储通道的缓冲区大小

	// 设置MySQL存储的输入通道
	store.SetInputChannel(dbChannel)

	// 从清洗器分发数据到MySQL存储和API服务器
	go func() {
		// 创建缓存，用于存储因通道满而无法立即发送的数据
		var dbBacklog []*types.KLineData
		var apiBacklog []*types.KLineData
		const maxBacklogSize = 10000 // 设置最大缓存大小

		// 创建定时器，定期尝试发送缓存中的数据
		retryTicker := time.NewTicker(100 * time.Millisecond)
		defer retryTicker.Stop()

		// 创建统计计数器
		droppedCount := 0
		successCount := 0
		backlogCount := 0
		lastLogTime := time.Now()

		for {
			select {
			case <-ctx.Done():
				logging.Logger.WithFields(logrus.Fields{
					"db_backlog":  len(dbBacklog),
					"api_backlog": len(apiBacklog),
				}).Info("关闭数据分发器，剩余未处理数据")
				return

			case data, ok := <-cleaner.OutputChan:
				if !ok {
					logging.Logger.Warn("清洗器输出通道已关闭")
					return
				}

				// 检查数据是否为nil
				if data == nil {
					logging.Logger.Warn("收到nil数据点，跳过处理")
					continue
				}

				// 创建副本以防止数据竞争
				dataCopy := *data

				// 尝试发送到数据库通道
				select {
				case dbChannel <- &dataCopy:
					successCount++
				default:
					// 通道满，加入缓存
					dbBacklog = append(dbBacklog, &dataCopy)
					backlogCount++

					// 如果缓存过大，删除最旧的数据
					if len(dbBacklog) > maxBacklogSize {
						// 保留最新的数据，丢弃最旧的
						logging.Logger.WithFields(logrus.Fields{
							"dropped": len(dbBacklog) - maxBacklogSize,
							"symbol":  dbBacklog[0].Symbol,
						}).Warn("数据库缓存溢出，丢弃最旧数据")

						droppedCount += len(dbBacklog) - maxBacklogSize
						dbBacklog = dbBacklog[len(dbBacklog)-maxBacklogSize:]
					}
				}

				// 尝试发送到API通道
				select {
				case apiChannel <- data:
					// 成功发送到API通道
				default:
					// API通道不是关键路径，不缓存API数据
					// 或者根据需要实现类似的缓存机制
				}

			case <-retryTicker.C:
				// 尝试发送缓存中的数据库数据
				for len(dbBacklog) > 0 {
					select {
					case dbChannel <- dbBacklog[0]:
						// 成功发送，移除已发送的数据
						dbBacklog = dbBacklog[1:]
						successCount++
					default:
						// 通道仍然满，暂停重试
						break
					}
				}

				// 周期性地记录统计信息(每30秒)
				if time.Since(lastLogTime) > 30*time.Second {
					if backlogCount > 0 || droppedCount > 0 {
						logging.Logger.WithFields(logrus.Fields{
							"success":         successCount,
							"backlog_added":   backlogCount,
							"dropped":         droppedCount,
							"current_backlog": len(dbBacklog),
						}).Info("数据分发统计")
					}
					// 重置计数器
					successCount = 0
					backlogCount = 0
					droppedCount = 0
					lastLogTime = time.Now()
				}
			}
		}
	}()

	// 获取前200个市值最大的加密货币
	topCryptos, err := collector.FetchTopCryptocurrencies(ctx, 200)
	if err != nil {
		logging.Logger.WithError(err).Warn("获取前200个市值最大的加密货币失败，将使用配置中的币种")
	} else {
		logging.Logger.WithField("count", len(topCryptos)).Info("成功获取前N个市值最大的加密货币")
		
		// 获取符号管理器
		symbolManager, err := cfg.GetSymbolManager()
		if err != nil {
			logging.Logger.WithError(err).Warn("无法获取符号管理器")
		} else {
			// 更新主组的符号列表
			mainGroup := symbolManager.GetGroup("main")
			if mainGroup != nil {
				// 设置获取到的交易对
				mainGroup.Symbols = topCryptos
				
				// 确保时间周期仅包含15m、4h和1d
				mainGroup.Intervals = []string{"15m", "4h", "1d"}
				
				// 更新轮询间隔 - 设置更合理的时间间隔以避免被限流
				mainGroup.PollIntervals = map[string]string{
					"15m": "15m",
					"4h":  "4h",
					"1d":  "24h",
				}
				
				// 保存更新后的配置到文件
				if err := symbolManager.SaveConfig(); err != nil {
					logging.Logger.WithError(err).Warn("保存币种配置失败")
				} else {
					logging.Logger.Info("已成功保存更新的币种配置")
				}
				
				// 重新加载配置到收集器
				if err := collector.ReloadSymbolConfig(); err != nil {
					logging.Logger.WithError(err).Warn("重新加载币种配置到收集器失败")
				} else {
					logging.Logger.WithFields(logrus.Fields{
						"symbols_count": len(mainGroup.Symbols),
						"intervals":     mainGroup.Intervals,
					}).Info("已更新主交易对组配置并重新加载到收集器")
				}
			}
		}
	}
	
	// 直接添加常见交易对到收集器，确保至少有这些币种被收集
	commonSymbols := []string{"BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT", "SOLUSDT", "DOTUSDT", "MATICUSDT", "LTCUSDT"}
	for _, symbol := range commonSymbols {
		symConfig := config.SymbolConfig{
			Symbol:      symbol,
			Enabled:     true,
			Intervals:   []string{"15m", "4h", "1d"},
			DailyStart:  "2023-10-01T00:00:00Z",
			HourlyStart: "2023-10-01T00:00:00Z",
			MinuteStart: "2023-10-01T00:00:00Z",
		}
		
		if err := collector.AddSymbol(symConfig); err != nil {
			logging.Logger.WithError(err).WithField("symbol", symbol).Warn("添加币种到收集器失败")
		} else {
			logging.Logger.WithField("symbol", symbol).Info("成功添加币种到收集器")
		}
	}

	// 启动各组件
	go func() {
		if err := server.Start(ctx); err != nil {
			logging.Logger.WithError(err).Fatal("启动API服务器失败")
		}
	}()

	go store.Start(ctx)
	go cleaner.Start(ctx)
	go collector.Start(ctx)

	// 等待中断信号
	waitForInterrupt(ctx, cancel)
}

// waitForInterrupt 等待中断信号并执行优雅关闭
func waitForInterrupt(ctx context.Context, cancel context.CancelFunc) {
	// 优雅关机
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	logging.Logger.Info("接收到关机信号，正在停止服务...")

	// 取消上下文，停止所有组件
	cancel()

	// 等待资源释放
	logging.Logger.Info("等待资源释放...")
	time.Sleep(3 * time.Second)
	logging.Logger.Info("服务已正常停止")
}

// logFatal 用于在日志初始化之前记录致命错误
func logFatal(format string, args ...interface{}) {
	// 初始化一个基本的日志配置
	logging.InitLogger(&config.LogConfig{
		Level:      "info",
		JSONFormat: false,
	})

	// 记录错误消息
	errorMsg := fmt.Sprintf(format, args...)
	logging.Logger.Error(errorMsg)

	// 打印到标准错误
	fmt.Fprintf(os.Stderr, "致命错误: %s\n", errorMsg)

	// 确保日志完全写入
	time.Sleep(100 * time.Millisecond)

	// 以非零状态码退出
	os.Exit(1)
}
