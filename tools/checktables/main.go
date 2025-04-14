package main

import (
	"fmt"
	"log"

	"github.com/web3qt/data4Trend/config"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func main() {
	// 加载配置
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	// 构建DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		cfg.MySQL.User, cfg.MySQL.Password, cfg.MySQL.Host, cfg.MySQL.Port, cfg.MySQL.Database)

	// 连接数据库
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		log.Fatalf("连接数据库失败: %v", err)
	}

	// 获取数据库中的所有表
	var tables []string
	db.Raw(`
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = DATABASE() 
		AND table_name NOT IN ('connection_test', 'kline')
		ORDER BY table_name
	`).Pluck("table_name", &tables)

	fmt.Printf("数据库中的表 (%d):\n", len(tables))
	for i, table := range tables {
		// 获取表中的记录数
		var count int64
		db.Table(table).Count(&count)

		fmt.Printf("%d. %s: %d 条记录\n", i+1, table, count)

		// 如果有记录，显示最新的一条记录
		if count > 0 {
			type Record struct {
				IntervalType string
				OpenTime     string
				ClosePrice   float64
			}

			var record Record
			db.Table(table).
				Select("interval_type, open_time, close_price").
				Order("open_time DESC").
				Limit(1).
				Scan(&record)

			fmt.Printf("   最新记录: %s 周期, 时间: %s, 收盘价: %f\n",
				record.IntervalType, record.OpenTime, record.ClosePrice)
		}
	}

	if len(tables) == 0 {
		fmt.Println("没有找到任何表，检查币种数据是否正在写入。")
	}
}
