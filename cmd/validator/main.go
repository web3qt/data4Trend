package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"data4trend/pkg/backfill"
	"data4trend/pkg/config"
	"data4trend/pkg/storage"
	"data4trend/pkg/validator"
)

func main() {
	// 命令行参数
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	logLevel := flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	runOnce := flag.Bool("once", false, "Run validation once and exit")
	showStats := flag.Bool("stats", false, "Show validator statistics and exit")
	flag.Parse()

	// 设置日志器
	logger := logrus.New()
	level, err := logrus.ParseLevel(*logLevel)
	if err != nil {
		logger.Fatalf("Invalid log level: %v", err)
	}
	logger.SetLevel(level)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
		TimestampFormat: "2006-01-02 15:04:05",
	})

	logger.Info("Starting Data Validator & Backfiller Service")

	// 加载配置
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logger.Fatalf("Failed to load config: %v", err)
	}

	// 验证验证器配置
	if !cfg.Validator.Enabled {
		logger.Warn("Validator is disabled in configuration")
		os.Exit(0)
	}

	logger.WithFields(logrus.Fields{
		"check_interval":     cfg.Validator.CheckInterval,
		"max_gap_duration":   cfg.Validator.MaxGapDuration,
		"history_days":       cfg.Validator.HistoryDays,
		"batch_size":         cfg.Validator.BatchSize,
		"concurrent_workers": cfg.Validator.ConcurrentWorkers,
	}).Info("Validator configuration loaded")

	// 初始化存储
	logger.Info("Connecting to ClickHouse...")
	storage, err := storage.NewClickHouseStorage(cfg, logger)
	if err != nil {
		logger.Fatalf("Failed to initialize storage: %v", err)
	}
	defer storage.Close()

	// 测试存储连接
	if err := storage.TestConnection(); err != nil {
		logger.Fatalf("Failed to connect to storage: %v", err)
	}
	logger.Info("Successfully connected to ClickHouse")

	// 初始化回补服务
	logger.Info("Initializing backfill service...")
	backfillService := backfill.NewBackfillService(cfg, storage, logger)

	// 初始化验证器服务
	logger.Info("Initializing validator service...")
	validatorService := validator.NewValidatorService(cfg, storage, backfillService, logger)

	// 处理统计请求
	if *showStats {
		showValidatorStats(validatorService, logger)
		return
	}

	// 处理单次运行模式
	if *runOnce {
		runValidationOnce(validatorService, logger)
		return
	}

	// 启动验证器服务
	logger.Info("Starting validator service...")
	validatorService.Start()

	logger.Info("Validator service started successfully")
	logger.Info("Press Ctrl+C to stop the service")

	// 设置信号处理以优雅关闭
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 等待关闭信号
	sig := <-sigChan
	logger.WithField("signal", sig).Info("Received shutdown signal")

	// 优雅关闭
	logger.Info("Stopping validator service...")
	validatorService.Stop()
	logger.Info("Validator service stopped successfully")

	logger.Info("Data Validator & Backfiller Service shutdown complete")
}

// showValidatorStats 显示当前验证器统计信息
func showValidatorStats(validatorService *validator.ValidatorService, logger *logrus.Logger) {
	stats := validatorService.GetStats()

	logger.Info("=== Validator Service Statistics ===")
	logger.WithField("last_check_time", stats.LastCheckTime.Format("2006-01-02 15:04:05")).Info("Stat")
	logger.WithField("total_checks", stats.TotalChecks).Info("Stat")
	logger.WithField("gaps_detected", stats.GapsDetected).Info("Stat")
	logger.WithField("gaps_fixed", stats.GapsFixed).Info("Stat")
	logger.WithField("backfill_errors", stats.BackfillErrors).Info("Stat")
	logger.WithField("data_coverage_pct", stats.DataCoverage).Info("Stat")
	logger.WithField("symbols_checked", stats.SymbolsChecked).Info("Stat")
	logger.WithField("total_missing_minutes", stats.TotalMissingMinutes).Info("Stat")
	logger.WithField("continuous_days", stats.ContinuousDays).Info("Stat")
	logger.Info("=== End Statistics ===")
}

// runValidationOnce 执行单次验证运行并退出
func runValidationOnce(validatorService *validator.ValidatorService, logger *logrus.Logger) {
	logger.Info("Running single validation check...")

	start := time.Now()

	// 定义验证时间范围（最近24小时）
	endTime := time.Now()
	startTime := endTime.Add(-24 * time.Hour)

	logger.WithFields(logrus.Fields{
		"start_time": startTime.Format("2006-01-02 15:04:05"),
		"end_time":   endTime.Format("2006-01-02 15:04:05"),
	}).Info("Validation time range")

	// 对时间范围运行验证
	result := validatorService.ValidateDataRange(startTime, endTime)

	duration := time.Since(start)
	stats := validatorService.GetStats()

	logger.WithFields(logrus.Fields{
		"duration":      duration,
		"success":       result.Success,
		"symbols_total": result.SymbolsTotal,
		"symbols_valid": result.SymbolsValid,
		"gaps_found":    len(result.GapsFound),
		"gaps_fixed":    result.GapsFixed,
		"total_checks":  stats.TotalChecks,
		"gaps_detected": stats.GapsDetected,
		"gaps_fixed_total": stats.GapsFixed,
	}).Info("Validation completed")

	if !result.Success {
		logger.WithField("error", result.ErrorMessage).Error("Validation failed")
		os.Exit(1)
	}

	logger.Info("Single validation run completed successfully")
}