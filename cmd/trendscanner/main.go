package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/web3qt/data4Trend/config"
	"github.com/web3qt/data4Trend/pkg/logging"
	"github.com/web3qt/data4Trend/pkg/trendscanner"
)

func main() {
	// 命令行参数
	var (
		configPath = flag.String("config", "config/trend_scanner.yaml", "趋势扫描器配置文件路径")
		dbHost     = flag.String("db-host", "", "MySQL主机地址（覆盖配置文件）")
		dbPort     = flag.Int("db-port", 0, "MySQL端口（覆盖配置文件）")
		dbUser     = flag.String("db-user", "", "MySQL用户名（覆盖配置文件）")
		dbPass     = flag.String("db-pass", "", "MySQL密码（覆盖配置文件）")
		dbName     = flag.String("db-name", "", "MySQL数据库名称（覆盖配置文件）")
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

	// 加载配置文件
	cfg, err := trendscanner.LoadConfig(*configPath)
	if err != nil {
		logging.Logger.WithError(err).Error("加载配置文件失败，将使用默认配置")
		cfg = trendscanner.DefaultConfig()
	}

	// 命令行参数覆盖配置文件
	if *dbHost != "" {
		cfg.Database.Host = *dbHost
	}
	if *dbPort != 0 {
		cfg.Database.Port = *dbPort
	}
	if *dbUser != "" {
		cfg.Database.User = *dbUser
	}
	if *dbPass != "" {
		cfg.Database.Password = *dbPass
	}
	if *dbName != "" {
		cfg.Database.Name = *dbName
	}
	if *maPeriod != 0 {
		cfg.MA.Period = *maPeriod
	}
	if *interval != "" {
		cfg.MA.Interval = *interval
	}
	if *workers != 0 {
		cfg.Scan.Workers = *workers
	}
	if *scanInterval != 0 {
		cfg.Scan.Interval = scanInterval.String()
	}
	if *consecutiveKLines != 0 {
		cfg.Trend.ConsecutiveKLines = *consecutiveKLines
	}

	// 连接MySQL数据库
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		cfg.Database.User, cfg.Database.Password, cfg.Database.Host, cfg.Database.Port, cfg.Database.Name)
	
	logging.Logger.WithFields(logrus.Fields{
		"host": cfg.Database.Host,
		"port": cfg.Database.Port,
		"user": cfg.Database.User,
		"db":   cfg.Database.Name,
	}).Info("连接数据库")

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		logging.Logger.WithError(err).Fatal("连接MySQL数据库失败")
	}

	// 获取底层数据库连接以测试连接
	sqlDB, err := db.DB()
	if err != nil {
		logging.Logger.WithError(err).Fatal("获取底层数据库连接失败")
	}

	// 测试数据库连接
	if err = sqlDB.Ping(); err != nil {
		logging.Logger.WithError(err).Fatal("无法连接到MySQL数据库")
	}

	logging.Logger.WithFields(logrus.Fields{
		"host": cfg.Database.Host,
		"port": cfg.Database.Port,
		"name": cfg.Database.Name,
	}).Info("成功连接到MySQL数据库")

	// 创建上下文，以支持优雅关闭
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建并启动趋势扫描器
	scanner := trendscanner.NewTrendScannerWithConfig(ctx, db, cfg)

	// 确保CSV输出目录存在
	if cfg.Scan.CSVOutput != "" {
		if err := os.MkdirAll(cfg.Scan.CSVOutput, 0755); err != nil {
			logging.Logger.WithError(err).Error("创建CSV输出目录失败")
		}
	}

	// 打印配置信息
	logging.Logger.WithFields(logrus.Fields{
		"ma_period":        cfg.MA.Period,
		"interval":         cfg.MA.Interval,
		"workers":          cfg.Scan.Workers,
		"scan_interval":    cfg.Scan.Interval,
		"csv_output":       cfg.Scan.CSVOutput,
		"consecutive_klines": cfg.Trend.ConsecutiveKLines,
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

	// 关闭数据库连接
	if err := sqlDB.Close(); err != nil {
		logging.Logger.WithError(err).Error("关闭数据库连接出错")
	}

	logging.Logger.Info("应用程序已关闭")
} 