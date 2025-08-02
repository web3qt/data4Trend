package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"data4trend/pkg/api"
	"data4trend/pkg/backfill"
	"data4trend/pkg/batchwriter"
	"data4trend/pkg/binance"
	"data4trend/pkg/config"
	"data4trend/pkg/integrity"
	"data4trend/pkg/kafka"
	"data4trend/pkg/monitoring"
	"data4trend/pkg/storage"
	"data4trend/pkg/websocket"
)

var (
	configPath = flag.String("config", "config/config.yaml", "Path to configuration file")
	logLevel   = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	version    = "1.0.0"
	buildTime  = "unknown"
)

func main() {
	flag.Parse()

	// 初始化日志器
	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
		TimestampFormat: time.RFC3339,
	})

	// 设置日志级别
	if level, err := logrus.ParseLevel(*logLevel); err == nil {
		logger.SetLevel(level)
	} else {
		logger.Warnf("Invalid log level '%s', using 'info'", *logLevel)
		logger.SetLevel(logrus.InfoLevel)
	}

	logger.Infof("Starting Data4Trend Binance WebSocket Collector v%s (built: %s)", version, buildTime)

	// 加载配置
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logger.Fatalf("Failed to load configuration: %v", err)
	}

	logger.Infof("Loaded configuration from: %s", *configPath)
	logger.Infof("Database: %s@%s:%d/%s", cfg.Database.Username, cfg.Database.Host, cfg.Database.Port, cfg.Database.Database)
	logger.Infof("API server: %s:%d", cfg.API.Host, cfg.API.Port)

	if cfg.Proxy.Enabled {
		logger.Infof("Proxy enabled: %s", cfg.GetProxyURL())
	} else {
		logger.Info("Direct connection (no proxy)")
	}

	// 初始化交易对服务并获取交易对
	logger.Info("Initializing symbol service...")
	symbolService := binance.NewSymbolService(cfg, logger)
	symbols, err := symbolService.GetSymbolsWithRetry(3)
	if err != nil {
		logger.Fatalf("Failed to fetch symbols: %v", err)
	}

	// 用获取的交易对更新配置
	cfg.Symbols = symbols
	logger.Infof("Monitoring %d symbols with %s interval", len(cfg.Symbols), cfg.Interval)
	logger.Debugf("Symbols: %v", cfg.Symbols)

	// 初始化存储
	logger.Info("Initializing ClickHouse storage...")
	storage, err := storage.NewClickHouseStorage(cfg, logger)
	if err != nil {
		logger.Fatalf("Failed to initialize storage: %v", err)
	}
	defer storage.Close()
	logger.Info("ClickHouse storage initialized successfully")

	// 初始化Kafka生产者
	logger.Info("Initializing Kafka producer...")
	kafkaProducer, err := kafka.NewProducer(cfg, logger)
	if err != nil {
		logger.Fatalf("Failed to initialize Kafka producer: %v", err)
	}
	defer kafkaProducer.Close()

	// 初始化批量写入器
	logger.Info("Initializing batch writer...")
	batchWriter, err := batchwriter.NewBatchWriter(cfg, storage, logger)
	if err != nil {
		logger.Fatalf("Failed to initialize batch writer: %v", err)
	}
	batchWriter.Start()

	// 初始化批量消息处理器
	batchHandler := batchwriter.NewBatchMessageHandler(batchWriter, logger)

	// 初始化Kafka消费者
	logger.Info("Initializing Kafka consumer...")
	kafkaConsumer, err := kafka.NewConsumer(cfg, logger, batchHandler)
	if err != nil {
		logger.Fatalf("Failed to initialize Kafka consumer: %v", err)
	}

	// 初始化WebSocket客户端
	logger.Info("Initializing WebSocket client...")
	websocketClient := websocket.NewClient(cfg, kafkaProducer, logger)

	// 初始化监控系统
	logger.Info("Initializing monitoring system...")
	monitor := monitoring.NewMonitor(storage, websocketClient, logger)
	monitor.LogSystemInfo()
	monitor.Start()

	// 初始化合并的BackfillValidator服务
	logger.Info("Initializing BackfillValidator service...")
	backfillValidatorService := backfill.NewBackfillValidatorService(cfg, storage, logger)
	
	// 启动BackfillValidator服务
	if cfg.Validator.Enabled {
		if err := backfillValidatorService.Start(); err != nil {
			logger.Errorf("Failed to start BackfillValidator service: %v", err)
		} else {
			logger.Info("BackfillValidator service started successfully")
		}
	}

	// 初始化数据完整性服务（使用合并服务的backfill功能）
	logger.Info("Initializing data integrity service...")
	integrityService := integrity.NewDataIntegrityService(cfg, storage, backfillValidatorService.GetBackfillService(), logger)
	integrityService.Start()

	// 初始化API服务器
	logger.Info("Initializing API server...")
	apiServer := api.NewServer(cfg, storage, websocketClient, integrityService, backfillValidatorService, logger)

	// 设置优雅关闭
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start services
	var wg sync.WaitGroup

	// Start API server first
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := apiServer.Start(); err != nil {
			logger.Errorf("API server error: %v", err)
		}
	}()

	// Give API server time to start
	time.Sleep(2 * time.Second)
	logger.Info("API server started on http://localhost:8080")

	// Start Kafka consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := kafkaConsumer.Start(); err != nil {
			logger.Errorf("Kafka consumer error: %v", err)
		}
	}()

	// Start WebSocket client
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := websocketClient.Start(); err != nil {
			logger.Errorf("WebSocket client error: %v", err)
		}
	}()

	logger.Info("All services started successfully")
	logger.Info("Press Ctrl+C to stop...")

	// Wait for shutdown signal
	sig := <-sigChan
	logger.Infof("Received signal: %v", sig)
	logger.Info("Initiating graceful shutdown...")

	// Cancel context to signal shutdown
	cancel()

	// Stop services
	logger.Info("Stopping WebSocket client...")
	websocketClient.Stop()
	
	logger.Info("Stopping Kafka consumer...")
	if err := kafkaConsumer.Stop(); err != nil {
		logger.Errorf("Failed to stop Kafka consumer: %v", err)
	}
	
	logger.Info("Stopping batch writer...")
	if err := batchWriter.Stop(); err != nil {
		logger.Errorf("Failed to stop batch writer: %v", err)
	}
	
	logger.Info("Stopping data integrity service...")
	integrityService.Stop()
	
	logger.Info("Stopping data validation...")
	// The validator service is now part of backfillValidatorService, so no explicit stop needed here
	// backfillValidatorService.Stop() 

	// Wait for services to stop (with timeout)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("All services stopped gracefully")
	case <-time.After(30 * time.Second):
		logger.Warn("Shutdown timeout reached, forcing exit")
	}

	logger.Info("Data4Trend collector stopped")
}

// printBanner prints the application banner
func printBanner() {
	fmt.Println(`
██████╗  █████╗ ████████╗ █████╗ ██╗  ██╗████████╗██████╗ ███████╗███╗   ██╗██████╗ 
██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗██║  ██║╚══██╔══╝██╔══██╗██╔════╝████╗  ██║██╔══██╗
██║  ██║███████║   ██║   ███████║███████║   ██║   ██████╔╝█████╗  ██╔██╗ ██║██║  ██║
██║  ██║██╔══██║   ██║   ██╔══██║╚════██║   ██║   ██╔══██╗██╔══╝  ██║╚██╗██║██║  ██║
██████╔╝██║  ██║   ██║   ██║  ██║     ██║   ██║   ██║  ██║███████╗██║ ╚████║██████╔╝
╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝     ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝╚═╝  ╚═══╝╚═════╝ 
                                                                                      
                    Binance WebSocket Data Collector                                  
`)
}