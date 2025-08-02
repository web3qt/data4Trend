package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"

	"data4trend/pkg/backfill"
	"data4trend/pkg/config"
	"data4trend/pkg/storage"
)

func main() {
	// 解析命令行参数
	configPath := flag.String("config", "config/config.yaml", "配置文件路径")
	symbol := flag.String("symbol", "", "要回填的交易对 (可选，不指定则回填所有)")
	days := flag.Int("days", 5, "回填天数")
	validateOnly := flag.Bool("validate-only", false, "仅执行验证，不进行回填")
	flag.Parse()

	// 加载配置
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 设置日志级别
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	logger.Info("🚀 Starting Data4Trend BackfillValidator Service...")

	// 初始化存储层
	storage, err := storage.NewClickHouseStorage(cfg, logger)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	defer storage.Close()

	// 初始化合并的BackfillValidator服务
	backfillValidatorService := backfill.NewBackfillValidatorService(cfg, storage, logger)

	// 处理信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 启动BackfillValidator服务
	if cfg.Validator.Enabled {
		if err := backfillValidatorService.Start(); err != nil {
			logger.Fatalf("Failed to start BackfillValidator service: %v", err)
		}
		logger.Info("✅ BackfillValidator service started successfully")
	}

	// 根据参数执行不同的操作
	if *validateOnly {
		// 仅执行验证
		logger.Info("🔍 Running validation only...")
		if err := backfillValidatorService.ForceValidation(); err != nil {
			logger.Errorf("Validation failed: %v", err)
		} else {
			logger.Info("✅ Validation completed")
		}
	} else if *symbol != "" {
		// 回填单个交易对
		logger.Infof("🔄 Backfilling symbol: %s for %d days", *symbol, *days)
		backfillService := backfillValidatorService.GetBackfillService()
		result, err := backfillService.BackfillSymbolComplete(*symbol)
		if err != nil {
			logger.Errorf("Backfill failed: %v", err)
		} else {
			logger.Infof("✅ Backfill completed: %+v", result)
		}
	} else {
		// 回填所有交易对
		logger.Infof("🔄 Backfilling all symbols for %d days", *days)
		backfillService := backfillValidatorService.GetBackfillService()
		results, err := backfillService.BackfillAllSymbolsComplete()
		if err != nil {
			logger.Errorf("Backfill failed: %v", err)
		} else {
			logger.Infof("✅ Backfill completed: %d symbols processed", len(results))
		}
	}

	// 等待信号
	sig := <-sigChan
	logger.Infof("Received signal: %v", sig)
	logger.Info("🛑 Shutting down services...")

	// 停止服务
	backfillValidatorService.Stop()
	logger.Info("✅ Services stopped")
}
