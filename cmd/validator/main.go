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
	// Command line flags
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	logLevel := flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	runOnce := flag.Bool("once", false, "Run validation once and exit")
	showStats := flag.Bool("stats", false, "Show validator statistics and exit")
	flag.Parse()

	// Setup logger
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

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logger.Fatalf("Failed to load config: %v", err)
	}

	// Validate validator configuration
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

	// Initialize storage
	logger.Info("Connecting to ClickHouse...")
	storage, err := storage.NewClickHouseStorage(cfg, logger)
	if err != nil {
		logger.Fatalf("Failed to initialize storage: %v", err)
	}
	defer storage.Close()

	// Test storage connection
	if err := storage.TestConnection(); err != nil {
		logger.Fatalf("Failed to connect to storage: %v", err)
	}
	logger.Info("Successfully connected to ClickHouse")

	// Initialize backfill service
	logger.Info("Initializing backfill service...")
	backfillService := backfill.NewBackfillService(cfg, storage, logger)

	// Initialize validator service
	logger.Info("Initializing validator service...")
	validatorService := validator.NewValidatorService(cfg, storage, backfillService, logger)

	// Handle stats request
	if *showStats {
		showValidatorStats(validatorService, logger)
		return
	}

	// Handle run-once mode
	if *runOnce {
		runValidationOnce(validatorService, logger)
		return
	}

	// Start validator service
	logger.Info("Starting validator service...")
	validatorService.Start()

	logger.Info("Validator service started successfully")
	logger.Info("Press Ctrl+C to stop the service")

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	sig := <-sigChan
	logger.WithField("signal", sig).Info("Received shutdown signal")

	// Graceful shutdown
	logger.Info("Stopping validator service...")
	validatorService.Stop()
	logger.Info("Validator service stopped successfully")

	logger.Info("Data Validator & Backfiller Service shutdown complete")
}

// showValidatorStats displays current validator statistics
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

// runValidationOnce performs a single validation run and exits
func runValidationOnce(validatorService *validator.ValidatorService, logger *logrus.Logger) {
	logger.Info("Running single validation check...")

	start := time.Now()

	// Define validation time range (last 24 hours)
	endTime := time.Now()
	startTime := endTime.Add(-24 * time.Hour)

	logger.WithFields(logrus.Fields{
		"start_time": startTime.Format("2006-01-02 15:04:05"),
		"end_time":   endTime.Format("2006-01-02 15:04:05"),
	}).Info("Validation time range")

	// Run validation for the time range
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