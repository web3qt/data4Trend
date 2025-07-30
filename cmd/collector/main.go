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
	"data4trend/pkg/binance"
	"data4trend/pkg/config"
	"data4trend/pkg/monitoring"
	"data4trend/pkg/storage"
	"data4trend/pkg/validation"
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

	// Initialize logger
	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
		TimestampFormat: time.RFC3339,
	})

	// Set log level
	if level, err := logrus.ParseLevel(*logLevel); err == nil {
		logger.SetLevel(level)
	} else {
		logger.Warnf("Invalid log level '%s', using 'info'", *logLevel)
		logger.SetLevel(logrus.InfoLevel)
	}

	logger.Infof("Starting Data4Trend Binance WebSocket Collector v%s (built: %s)", version, buildTime)

	// Load configuration
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

	// Initialize symbol service and fetch symbols
	logger.Info("Initializing symbol service...")
	symbolService := binance.NewSymbolService(cfg, logger)
	symbols, err := symbolService.GetSymbolsWithRetry(3)
	if err != nil {
		logger.Fatalf("Failed to fetch symbols: %v", err)
	}

	// Update config with fetched symbols
	cfg.Symbols = symbols
	logger.Infof("Monitoring %d symbols with %s interval", len(cfg.Symbols), cfg.Interval)
	logger.Debugf("Symbols: %v", cfg.Symbols)

	// Initialize storage
	logger.Info("Initializing ClickHouse storage...")
	storage, err := storage.NewClickHouseStorage(cfg, logger)
	if err != nil {
		logger.Fatalf("Failed to initialize storage: %v", err)
	}
	defer storage.Close()
	logger.Info("ClickHouse storage initialized successfully")

	// Initialize WebSocket client
	logger.Info("Initializing WebSocket client...")
	websocketClient := websocket.NewClient(cfg, storage, logger)

	// Initialize monitoring
	logger.Info("Initializing monitoring system...")
	monitor := monitoring.NewMonitor(storage, websocketClient, logger)
	monitor.LogSystemInfo()
	monitor.Start()

	// Initialize data validation
	logger.Info("Initializing data validation system...")
	validator := validation.NewDataValidator(storage, cfg, logger)
	validator.Start()

	// Initialize API server
	logger.Info("Initializing API server...")
	apiServer := api.NewServer(cfg, storage, websocketClient, validator, logger)

	// Setup graceful shutdown
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start services
	var wg sync.WaitGroup

	// Start WebSocket client
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := websocketClient.Start(); err != nil {
			logger.Errorf("WebSocket client error: %v", err)
		}
	}()

	// Start API server
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := apiServer.Start(); err != nil {
			logger.Errorf("API server error: %v", err)
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
	logger.Info("Stopping data validation...")
	validator.Stop()
	
	logger.Info("Stopping WebSocket client...")
	websocketClient.Stop()

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