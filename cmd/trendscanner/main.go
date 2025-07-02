package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"

	"github.com/web3qt/data4Trend/config"
	"github.com/web3qt/data4Trend/pkg/datastore"
	"github.com/web3qt/data4Trend/pkg/logging"
	"github.com/web3qt/data4Trend/pkg/trendscanner"
)

func main() {
	// 命令行参数
	var (
		configPath = flag.String("config", "config/trend_scanner.yaml", "趋势扫描器配置文件路径")
		dbHost     = flag.String("db-host", "", "ClickHouse主机地址（覆盖配置文件）")
		dbPort     = flag.Int("db-port", 0, "ClickHouse端口（覆盖配置文件）")
		dbUser     = flag.String("db-user", "", "ClickHouse用户名（覆盖配置文件）")
		dbPass     = flag.String("db-pass", "", "ClickHouse密码（覆盖配置文件）")
		dbName     = flag.String("db-name", "", "ClickHouse数据库名称（覆盖配置文件）")
		maPeriod   = flag.Int("ma-period", 0, "MA周期（覆盖配置文件）")
		interval   = flag.String("interval", "", "K线间隔（覆盖配置文件）")
		workers    = flag.Int("workers", 0, "工作协程数（覆盖配置文件）")
		scanInterval = flag.Duration("scan-interval", 0, "扫描间隔（覆盖配置文件）")
		consecutiveKLines = flag.Int("consecutive-klines", 0, "连续K线数量（覆盖配置文件）")
	)

	flag.Parse()

	// 设置日志
	logging.InitLogger(&config.LogConfig{
		Level:      "info",
		JSONFormat: false,
	})

	// 加载主配置文件以获取ClickHouse配置
	mainCfg, err := config.LoadConfig()
	if err != nil {
		logging.Logger.WithError(err).Fatal("加载主配置文件失败")
	}

	// 加载趋势扫描器配置文件
	trendCfg, err := trendscanner.LoadConfig(*configPath)
	if err != nil {
		logging.Logger.WithError(err).Error("加载趋势扫描器配置文件失败，将使用默认配置")
		trendCfg = trendscanner.DefaultConfig()
	}

	// 设置ClickHouse配置，命令行参数覆盖配置文件
	clickhouseCfg := &datastore.ClickHouseConfig{
		Host:     mainCfg.ClickHouse.Host,
		Port:     mainCfg.ClickHouse.Port,
		HTTPPort: mainCfg.ClickHouse.HTTPPort,
		User:     mainCfg.ClickHouse.User,
		Password: mainCfg.ClickHouse.Password,
		Database: mainCfg.ClickHouse.Database,
	}

	if *dbHost != "" {
		clickhouseCfg.Host = *dbHost
	}
	if *dbPort != 0 {
		clickhouseCfg.Port = *dbPort
	}
	if *dbUser != "" {
		clickhouseCfg.User = *dbUser
	}
	if *dbPass != "" {
		clickhouseCfg.Password = *dbPass
	}
	if *dbName != "" {
		clickhouseCfg.Database = *dbName
	}

	// 应用趋势配置参数覆盖
	if *maPeriod != 0 {
		trendCfg.MA.Period = *maPeriod
	}
	if *interval != "" {
		trendCfg.MA.Interval = *interval
	}
	if *workers != 0 {
		trendCfg.Scan.Workers = *workers
	}
	if *scanInterval != 0 {
		trendCfg.Scan.Interval = scanInterval.String()
	}
	if *consecutiveKLines != 0 {
		trendCfg.Trend.ConsecutiveKLines = *consecutiveKLines
	}

	// 连接ClickHouse数据库
	logging.Logger.WithFields(logrus.Fields{
		"host":     clickhouseCfg.Host,
		"port":     clickhouseCfg.Port,
		"user":     clickhouseCfg.User,
		"database": clickhouseCfg.Database,
	}).Info("连接ClickHouse数据库")

	store, err := datastore.NewClickHouseStore(clickhouseCfg, nil)
	if err != nil {
		logging.Logger.WithError(err).Fatal("连接ClickHouse数据库失败")
	}

	logging.Logger.WithFields(logrus.Fields{
		"host":     clickhouseCfg.Host,
		"port":     clickhouseCfg.Port,
		"database": clickhouseCfg.Database,
	}).Info("成功连接到ClickHouse数据库")

	// 创建上下文，以支持优雅关闭
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建并启动趋势扫描器
	scanner := trendscanner.NewTrendScannerWithConfig(ctx, store, trendCfg)

	// 确保CSV输出目录存在
	if trendCfg.Scan.CSVOutput != "" {
		if err := os.MkdirAll(trendCfg.Scan.CSVOutput, 0755); err != nil {
			logging.Logger.WithError(err).Error("创建CSV输出目录失败")
		}
	}

	// 打印配置信息
	logging.Logger.WithFields(logrus.Fields{
		"ma_period":        trendCfg.MA.Period,
		"interval":         trendCfg.MA.Interval,
		"workers":          trendCfg.Scan.Workers,
		"scan_interval":    trendCfg.Scan.Interval,
		"csv_output":       trendCfg.Scan.CSVOutput,
		"consecutive_klines": trendCfg.Trend.ConsecutiveKLines,
	}).Info("趋势扫描器配置")

	// 启动扫描器
	go scanner.Start()

	// 设置信号处理
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 等待信号
	sig := <-sigCh
	logging.Logger.WithField("signal", sig.String()).Info("收到信号，开始优雅关闭")

	// 停止扫描器
	scanner.Stop()

	logging.Logger.Info("应用程序已关闭")
} 