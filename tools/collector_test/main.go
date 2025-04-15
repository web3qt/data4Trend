package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/web3qt/data4Trend/config"
	"github.com/web3qt/data4Trend/pkg/datacollector"
	"github.com/web3qt/data4Trend/pkg/logging"
)

func main() {
	// 直接设置logrus输出到控制台
	logrus.SetOutput(os.Stdout)
	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	})

	// 初始化日志
	logging.InitLogger(&config.LogConfig{
		Level:      "debug",
		JSONFormat: false,
		OutputPath: "",
	})

	// 创建完整配置
	cfg := &config.Config{
		Log: config.LogConfig{
			Level:      "debug",
			JSONFormat: false,
			OutputPath: "",
		},
		Binance: config.BinanceConfig{
			APIKey:    "",
			SecretKey: "",
			Symbols:   []config.SymbolConfig{},
		},
		MySQL: struct {
			Host     string `yaml:"host"`
			Port     int    `yaml:"port"`
			User     string `yaml:"user"`
			Password string `yaml:"password"`
			Database string `yaml:"database"`
		}{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Password: "123456",
			Database: "data4trend",
		},
		HTTP: struct {
			Timeout int    `yaml:"timeout"`
			Proxy   string `yaml:"proxy"`
		}{
			Timeout: 30,
			Proxy:   "",
		},
	}

	fmt.Println("配置初始化完成")

	// 添加一个测试交易对
	testSymbol := config.SymbolConfig{
		Symbol:      "BTCUSDT",
		Enabled:     true,
		Intervals:   []string{"1h"},
		HourlyStart: time.Now().Add(-24 * time.Hour).Format(time.RFC3339),
	}
	cfg.Binance.Symbols = append(cfg.Binance.Symbols, testSymbol)

	fmt.Println("=== 开始测试 BinanceCollector ===")
	fmt.Println("创建收集器...")

	// 捕获 panic
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("程序异常: %v\n", r)
		}
	}()

	collector := datacollector.NewBinanceCollector(cfg)
	if collector == nil {
		fmt.Println("创建收集器失败")
		return
	}

	fmt.Println("收集器创建成功，开始启动...")

	// 创建一个通道来接收来自收集器的数据
	dataChan := collector.DataChan
	go func() {
		for data := range dataChan {
			fmt.Printf("收到数据: %s %s Open=%f Close=%f\n",
				data.Symbol, data.Interval, data.Open, data.Close)
		}
	}()

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动收集器
	fmt.Println("正在启动收集器...")
	err := collector.Start(ctx)
	if err != nil {
		fmt.Printf("启动收集器失败: %v\n", err)
		return
	}

	fmt.Println("收集器启动成功，等待数据...")

	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	fmt.Println("收到中断信号，关闭程序...")
	cancel()
	time.Sleep(time.Second) // 给收集器一点时间关闭

	fmt.Println("=== 测试完成 ===")
}
