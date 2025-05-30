package main

import (
	"context"
	"flag"
	"fmt"
	"log"
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
	// 默认使用config/symbols.yaml作为配置文件
	var configFile string
	var showHelp bool
	
	// 定义命令行参数
	flag.StringVar(&configFile, "config", "config/symbols.yaml", "配置文件路径")
	portFlag := flag.Int("port", 8080, "API服务器端口号")
	enableTrendScannerFlag := flag.Bool("trend-scanner", false, "是否提示启动独立趋势扫描器")
	flag.BoolVar(&showHelp, "help", false, "显示帮助信息")
	flag.BoolVar(&showHelp, "h", false, "显示帮助信息 (简写)")
	
	// 解析命令行参数
	flag.Parse()
	
	// 如果请求了帮助，显示用法并退出
	if showHelp {
		fmt.Println("数据采集服务 - 使用方法:")
		flag.PrintDefaults()
		return
	}
	
	// 如果指定了配置文件作为位置参数而非标志
	if flag.NArg() > 0 {
		configFile = flag.Arg(0)
	}
	
	// 检查配置文件是否存在
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		log.Printf("警告: 配置文件 %s 不存在，将使用默认配置", configFile)
	} else {
		log.Printf("使用配置文件: %s", configFile)
	}

	// 加载配置
	fmt.Println("正在加载配置...")
	cfg, err := config.LoadConfig()
	if err != nil {
		// 初始化前需要使用标准日志
		logFatal("加载配置失败: %v", err)
	}
	fmt.Printf("配置加载成功，数据库: %s, API密钥设置: %v\n", 
		cfg.MySQL.Database, cfg.Binance.APIKey != "")

	// 创建binanceConfig
	binanceConfig := &config.BinanceConfig{
		APIKey:    cfg.Binance.APIKey,
		SecretKey: cfg.Binance.SecretKey,
	}

	// 现在传入binanceConfig
	symbolManager, err := config.NewSymbolManager(configFile, binanceConfig)
	if err != nil {
		log.Fatalf("加载币种配置失败: %v", err)
	}

	// 获取main组的时间设置
	mainGroup := symbolManager.GetGroup("main")
	if mainGroup == nil {
		log.Fatalf("配置文件中未找到main组")
	}
	
	// 确保main组的start_times已设置
	if mainGroup.StartTimes == nil || len(mainGroup.StartTimes) == 0 {
		log.Fatalf("main组的start_times未设置")
	}

	fmt.Println("===============================================")
	fmt.Println("  数据采集服务正在启动 - 调试模式")
	fmt.Println("===============================================")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	fmt.Println("正在获取前200个市值最大的币种...")
	topCryptos, err := collector.FetchTopCryptocurrencies(ctx, 200)
	if err != nil {
		logging.Logger.WithError(err).Error("获取前200个市值最大的加密货币失败")
		fmt.Printf("获取币种失败: %v\n", err)
		// 使用备用币种列表
		topCryptos = []string{
			"BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", 
			"DOGEUSDT", "SOLUSDT", "DOTUSDT", "MATICUSDT", "LTCUSDT",
			"AVAXUSDT", "LINKUSDT", "ATOMUSDT", "UNIUSDT", "ETCUSDT",
			"TRXUSDT", "XLMUSDT", "VETUSDT", "ICPUSDT", "FILUSDT",
			"THETAUSDT", "XMRUSDT", "FTMUSDT", "ALGOUSDT", "HBARUSDT"}
		fmt.Println("将使用备用币种列表继续运行")
	}
	
	// 如果获取到的交易对少于10个，使用备用列表
	if len(topCryptos) < 10 {
		logging.Logger.Warn("获取到的交易对数量过少，将使用备用币种列表")
		fmt.Println("获取到的交易对数量过少，将使用备用币种列表")
		topCryptos = []string{
			"BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", 
			"DOGEUSDT", "SOLUSDT", "DOTUSDT", "MATICUSDT", "LTCUSDT",
			"AVAXUSDT", "LINKUSDT", "ATOMUSDT", "UNIUSDT", "ETCUSDT",
			"TRXUSDT", "XLMUSDT", "VETUSDT", "ICPUSDT", "FILUSDT",
			"THETAUSDT", "XMRUSDT", "FTMUSDT", "ALGOUSDT", "HBARUSDT"}
	}
	
	logging.Logger.WithField("count", len(topCryptos)).Info("成功获取的加密货币数量")
	fmt.Printf("成功准备了%d个币种\n", len(topCryptos))
	
	// 显示main组配置的时间信息
	fmt.Println("时间配置信息:")
	if min, ok := mainGroup.StartTimes["minute"]; ok {
		fmt.Printf("分钟级数据开始时间: %s\n", min)
	}
	if hour, ok := mainGroup.StartTimes["hour"]; ok {
		fmt.Printf("小时级数据开始时间: %s\n", hour)
	}
	if day, ok := mainGroup.StartTimes["day"]; ok {
		fmt.Printf("日级数据开始时间: %s\n", day)
	}
	
	// 直接使用获取到的交易对和配置的时间启动收集器
	symbols := make([]config.SymbolConfig, 0, len(topCryptos))
	for _, symbol := range topCryptos {
		cfg := config.SymbolConfig{
			Symbol:    symbol,
			Enabled:   true,
			Intervals: mainGroup.Intervals,
		}
		
		// 直接使用main组配置的时间
		if min, ok := mainGroup.StartTimes["minute"]; ok {
			cfg.MinuteStart = min
		}
		if hour, ok := mainGroup.StartTimes["hour"]; ok {
			cfg.HourlyStart = hour
		}
		if day, ok := mainGroup.StartTimes["day"]; ok {
			cfg.DailyStart = day
		}
		
		symbols = append(symbols, cfg)
	}
	
	// 使用获取到的交易对启动收集器
	fmt.Println("正在启动收集器...")
	if err := collector.StartWithSymbols(ctx, symbols); err != nil {
		logging.Logger.WithError(err).Error("启动收集器失败")
		fmt.Printf("启动收集器失败: %v\n", err)
		return
	}
	
	logging.Logger.Info("成功启动收集器")
	fmt.Println("收集器启动成功!")

	// 启动各组件
	logging.Logger.Info("启动API服务器...")
	go server.Start(ctx)
	logging.Logger.Info("启动数据存储...")
	go store.Start(ctx)
	logging.Logger.Info("启动数据清洗器...")
	go cleaner.Start(ctx)
	logging.Logger.Info("启动数据收集器...")
	go collector.Start(ctx)
	
	// 不再在主程序中启动趋势扫描器，而是使用独立的趋势扫描器程序
	if *enableTrendScannerFlag {
		logging.Logger.Info("趋势扫描器已经被移至独立程序，请使用 ./trendScanner 启动")
		fmt.Println("请使用 ./trendScanner 启动趋势扫描器！")
	}

	// 等待程序被中断
	waitForInterrupt(ctx, cancel)
	
	// 优雅地关闭资源
	logging.Logger.Info("正在关闭服务...")
	
	logging.Logger.Info("数据采集服务已关闭")
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
