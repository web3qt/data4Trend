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
	"github.com/web3qt/data4Trend/pkg/datacollector"
	"github.com/web3qt/data4Trend/pkg/datastore"
	"github.com/web3qt/data4Trend/pkg/logging"
)

func main() {
	// 命令行参数
	var (
		configPath = flag.String("config", "config/symbols.yaml", "配置文件路径")
		dbHost     = flag.String("db-host", "", "ClickHouse主机地址（覆盖配置文件）")
		dbPort     = flag.Int("db-port", 0, "ClickHouse端口（覆盖配置文件）")
		dbUser     = flag.String("db-user", "", "ClickHouse用户名（覆盖配置文件）")
		dbPass     = flag.String("db-pass", "", "ClickHouse密码（覆盖配置文件）")
		dbName     = flag.String("db-name", "", "ClickHouse数据库名称（覆盖配置文件）")
		initDB     = flag.Bool("init-db", false, "初始化数据库表结构")
		logLevel   = flag.String("log-level", "info", "日志级别 (debug, info, warn, error)")
	)

	flag.Parse()

	// 初始化日志
	logging.InitLogger(&config.LogConfig{
		Level:      *logLevel,
		JSONFormat: false,
		OutputPath: "",
	})

	logging.Logger.Info("启动Data4Trend数据收集器（物化视图架构）")

	// 加载配置
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logging.Logger.WithError(err).Fatal("加载配置文件失败")
	}

	// 命令行参数覆盖配置文件
	if *dbHost != "" {
		cfg.ClickHouse.Host = *dbHost
	}
	if *dbPort != 0 {
		cfg.ClickHouse.Port = *dbPort
	}
	if *dbUser != "" {
		cfg.ClickHouse.User = *dbUser
	}
	if *dbPass != "" {
		cfg.ClickHouse.Password = *dbPass
	}
	if *dbName != "" {
		cfg.ClickHouse.Database = *dbName
	}

	// 验证ClickHouse配置
	if cfg.ClickHouse.Host == "" || cfg.ClickHouse.Port == 0 || cfg.ClickHouse.Database == "" {
		logging.Logger.Fatal("ClickHouse配置不完整，请检查配置文件或命令行参数")
	}

	logging.Logger.WithFields(logrus.Fields{
		"host":     cfg.ClickHouse.Host,
		"port":     cfg.ClickHouse.Port,
		"database": cfg.ClickHouse.Database,
		"user":     cfg.ClickHouse.User,
	}).Info("ClickHouse配置")

	// 创建数据通道
	dataChan := make(chan *types.KLineData, 10000)

	// 创建ClickHouse配置
	clickhouseCfg := &datastore.ClickHouseConfig{
		Host:     cfg.ClickHouse.Host,
		Port:     cfg.ClickHouse.Port,
		HTTPPort: cfg.ClickHouse.HTTPPort,
		User:     cfg.ClickHouse.User,
		Password: cfg.ClickHouse.Password,
		Database: cfg.ClickHouse.Database,
	}

	// 创建物化视图存储
	store, err := datastore.NewMaterializedClickHouseStore(clickhouseCfg, dataChan)
	if err != nil {
		logging.Logger.WithError(err).Fatal("创建ClickHouse存储失败")
	}

	logging.Logger.Info("ClickHouse存储创建成功（物化视图架构）")

	// 如果需要初始化数据库
	if *initDB {
		logging.Logger.Info("开始初始化数据库表结构...")
		if err := initializeDatabase(store); err != nil {
			logging.Logger.WithError(err).Fatal("初始化数据库失败")
		}
		logging.Logger.Info("数据库初始化完成")
	}

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动数据存储服务
	go store.Start(ctx)
	logging.Logger.Info("数据存储服务已启动")

	// 创建数据收集器
	collector := datacollector.NewBinanceCollector(cfg)
	if collector == nil {
		logging.Logger.Fatal("创建Binance收集器失败")
	}

	// 设置数据通道
	collector.DataChan = dataChan

	// 启动数据收集器
	logging.Logger.Info("启动数据收集器...")
	err = collector.Start(ctx)
	if err != nil {
		logging.Logger.WithError(err).Fatal("启动数据收集器失败")
	}

	logging.Logger.Info("数据收集器启动成功")

	// 设置信号处理
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 等待信号
	sig := <-sigCh
	logging.Logger.WithField("signal", sig.String()).Info("收到信号，开始优雅关闭")

	// 取消上下文，停止所有服务
	cancel()

	// 等待一段时间让服务优雅关闭
	time.Sleep(2 * time.Second)

	// 关闭数据通道
	close(dataChan)

	logging.Logger.Info("应用程序已关闭")
}

// initializeDatabase 初始化数据库表结构
func initializeDatabase(store *datastore.MaterializedClickHouseStore) error {
	logging.Logger.Info("开始执行数据库初始化脚本...")

	// 读取SQL脚本
	sqlScript, err := os.ReadFile("scripts/clickhouse-init-materialized-views.sql")
	if err != nil {
		return fmt.Errorf("读取SQL脚本失败: %w", err)
	}

	// 执行SQL脚本
	ctx := context.Background()
	err = store.GetConn().Exec(ctx, string(sqlScript))
	if err != nil {
		return fmt.Errorf("执行SQL脚本失败: %w", err)
	}

	logging.Logger.Info("数据库表结构初始化完成")
	return nil
}