package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/web3qt/data4Trend/config"
	"github.com/web3qt/data4Trend/pkg/logging"
	"gopkg.in/yaml.v3"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// 收集器状态结构体
type CollectorState struct {
	Symbol    string     `yaml:"symbol"`
	Intervals []Interval `yaml:"intervals"`
	UpdatedAt string     `yaml:"updated_at"`
}

type Interval struct {
	Interval  string `yaml:"interval"`
	StartTime string `yaml:"start_time"`
}

type StateConfig struct {
	States []CollectorState `yaml:"states"`
}

func main() {
	// 设置命令行参数
	var (
		resetDate         string
		configPath        string
		collectorStateYml string
		dryRun            bool
	)

	flag.StringVar(&resetDate, "date", "2025-01-01", "重置数据收集的开始日期 (格式为 YYYY-MM-DD)")
	flag.StringVar(&configPath, "config", "config/config.yaml", "配置文件路径")
	flag.StringVar(&collectorStateYml, "state", "config/collector_state.yaml", "收集器状态文件路径")
	flag.BoolVar(&dryRun, "dry-run", false, "dry-run 模式，只显示操作但不执行")
	flag.Parse()

	// 初始化日志
	logging.InitLogger(&config.LogConfig{
		Level:      "info",
		JSONFormat: false,
		OutputPath: "",
	})

	// 解析重置日期
	startDate, err := time.Parse("2006-01-02", resetDate)
	if err != nil {
		logrus.Fatalf("解析日期格式错误: %v", err)
	}

	// 加载配置
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		logrus.Fatalf("加载配置失败: %v", err)
	}

	// 连接数据库
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		cfg.MySQL.User, cfg.MySQL.Password, cfg.MySQL.Host, cfg.MySQL.Port, cfg.MySQL.Database)

	logrus.Infof("正在连接数据库: %s:%d/%s", cfg.MySQL.Host, cfg.MySQL.Port, cfg.MySQL.Database)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		logrus.Fatalf("连接数据库失败: %v", err)
	}

	// 获取数据库中所有表
	var tables []string
	if err := db.Raw("SHOW TABLES").Scan(&tables).Error; err != nil {
		logrus.Fatalf("获取表列表失败: %v", err)
	}

	logrus.Infof("找到 %d 个表", len(tables))

	// 清空数据库表
	for _, table := range tables {
		if table == "connection_test" {
			logrus.Infof("跳过系统表: %s", table)
			continue
		}

		logrus.Infof("清空表: %s", table)
		if !dryRun {
			if err := db.Exec(fmt.Sprintf("TRUNCATE TABLE `%s`", table)).Error; err != nil {
				logrus.Warnf("清空表 %s 失败: %v", table, err)
			} else {
				logrus.Infof("表 %s 已清空", table)
			}
		}
	}

	// 更新收集器状态
	logrus.Infof("更新收集器状态文件: %s", collectorStateYml)

	// 读取当前收集器状态文件
	stateContent, err := os.ReadFile(collectorStateYml)
	if err != nil {
		logrus.Fatalf("读取收集器状态文件失败: %v", err)
	}

	var state StateConfig
	if err := yaml.Unmarshal(stateContent, &state); err != nil {
		logrus.Fatalf("解析收集器状态失败: %v", err)
	}

	// 设置时区
	loc, _ := time.LoadLocation("Asia/Shanghai")

	// 创建标准时间点
	standardTimes := map[string]time.Time{
		"1m":  time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, loc),
		"5m":  time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, loc),
		"15m": time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, loc),
		"30m": time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, loc),
		"1h":  time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, loc),
		"2h":  time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, loc),
		"4h":  time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, loc),
		"6h":  time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, loc),
		"8h":  time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, loc),
		"12h": time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, loc),
		"1d":  time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, loc),
		"3d":  time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, loc),
		"1w":  time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, loc),
	}

	// 更新每个收集器的时间
	for i := range state.States {
		for j := range state.States[i].Intervals {
			interval := state.States[i].Intervals[j].Interval
			if standardTime, ok := standardTimes[interval]; ok {
				state.States[i].Intervals[j].StartTime = standardTime.Format(time.RFC3339)
			}
		}
		state.States[i].UpdatedAt = time.Now().In(loc).Format(time.RFC3339)
	}

	// 将更新后的状态写回文件
	if !dryRun {
		// 备份原文件
		backupPath := collectorStateYml + ".bak"
		if err := os.WriteFile(backupPath, stateContent, 0644); err != nil {
			logrus.Warnf("备份状态文件失败: %v", err)
		} else {
			logrus.Infof("原状态文件已备份到: %s", backupPath)
		}

		// 写入新文件
		newContent, err := yaml.Marshal(state)
		if err != nil {
			logrus.Fatalf("序列化状态失败: %v", err)
		}

		if err := os.WriteFile(collectorStateYml, newContent, 0644); err != nil {
			logrus.Fatalf("写入状态文件失败: %v", err)
		}

		logrus.Infof("收集器状态已更新为从 %s 开始", resetDate)
	} else {
		logrus.Info("DRY-RUN 模式: 跳过写入文件")
	}

	logrus.Info("操作完成!")
}
