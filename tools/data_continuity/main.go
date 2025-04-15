package main

import (
	"fmt"
	"log"
	"sort"
	"time"

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

	// 获取所有表（币种）
	var tables []string
	db.Raw(`
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = DATABASE() 
		AND table_name NOT IN ('connection_test')
		AND table_name IN (SELECT LOWER(REPLACE(symbol, 'USDT', '')) FROM (
			SELECT DISTINCT symbol FROM kline
			UNION
			SELECT DISTINCT CASE 
				WHEN UPPER(table_name) LIKE '%USDT' THEN UPPER(table_name)
				ELSE CONCAT(UPPER(table_name), 'USDT')
			END as symbol
			FROM information_schema.tables 
			WHERE table_schema = DATABASE() 
			AND table_name NOT IN ('connection_test', 'kline', 'data4trend', 'system_log', 'trade_data')
		) as symbols)
	`).Pluck("table_name", &tables)

	fmt.Printf("找到 %d 个币种表\n", len(tables))

	// 检查每个币种的数据连贯性
	for _, table := range tables {
		fmt.Printf("\n检查币种表: %s\n", table)
		checkContinuity(db, table)
	}
}

// 检查一个币种的数据连贯性
func checkContinuity(db *gorm.DB, table string) {
	// 获取所有可用的时间周期
	var intervals []string
	db.Table(table).Distinct("interval_type").Pluck("interval_type", &intervals)

	fmt.Printf("时间周期: %v\n", intervals)

	// 对每个时间周期检查数据连贯性
	for _, interval := range intervals {
		checkIntervalContinuity(db, table, interval)
	}
}

// 检查一个时间周期的数据连贯性
func checkIntervalContinuity(db *gorm.DB, table, interval string) {
	type KLineData struct {
		ID           uint
		IntervalType string
		OpenTime     time.Time
		CloseTime    time.Time
		ClosePrice   float64
	}

	// 获取所有数据，按时间排序
	var klines []KLineData
	result := db.Table(table).
		Where("interval_type = ?", interval).
		Order("open_time ASC").
		Select("id, interval_type, open_time, close_time, close_price").
		Find(&klines)

	if result.Error != nil {
		fmt.Printf("查询失败: %v\n", result.Error)
		return
	}

	if len(klines) == 0 {
		fmt.Printf("时间周期 %s: 无数据\n", interval)
		return
	}

	// 计算预期的时间间隔
	expectedDuration := calculateExpectedDuration(interval)
	if expectedDuration == 0 {
		fmt.Printf("时间周期 %s: 无法识别的周期格式\n", interval)
		return
	}

	// 检查数据点之间的间隔
	gaps := 0
	duplicates := 0
	var lastTime time.Time
	var gapPeriods []string
	var duplicatePeriods []string

	// 先按时间排序确保顺序正确
	sort.Slice(klines, func(i, j int) bool {
		return klines[i].OpenTime.Before(klines[j].OpenTime)
	})

	for i, kline := range klines {
		if i > 0 {
			// 检查与上一个数据点的时间差
			timeDiff := kline.OpenTime.Sub(lastTime)

			// 检查是否有缺口（间隔大于预期）
			if timeDiff > time.Duration(float64(expectedDuration)*1.5) {
				gaps++
				gapPeriod := fmt.Sprintf("%s - %s", lastTime.Format("2006-01-02 15:04:05"), kline.OpenTime.Format("2006-01-02 15:04:05"))
				gapPeriods = append(gapPeriods, gapPeriod)
			}

			// 检查是否有重复（间隔小于预期）
			if timeDiff < time.Duration(float64(expectedDuration)*0.5) && timeDiff > 0 {
				duplicates++
				duplicatePeriod := fmt.Sprintf("%s - %s", lastTime.Format("2006-01-02 15:04:05"), kline.OpenTime.Format("2006-01-02 15:04:05"))
				duplicatePeriods = append(duplicatePeriods, duplicatePeriod)
			}
		}
		lastTime = kline.OpenTime
	}

	// 计算日期范围
	startDate := klines[0].OpenTime.Format("2006-01-02 15:04:05")
	endDate := klines[len(klines)-1].OpenTime.Format("2006-01-02 15:04:05")
	duration := klines[len(klines)-1].OpenTime.Sub(klines[0].OpenTime)

	// 计算连续性百分比
	expectedPoints := int(duration/expectedDuration) + 1
	continuityPercentage := float64(len(klines)) / float64(expectedPoints) * 100

	fmt.Printf("时间周期 %s: 数据点: %d, 日期范围: %s - %s, 总时长: %s\n",
		interval, len(klines), startDate, endDate, duration)
	fmt.Printf("数据缺口: %d, 数据重复: %d, 连续性: %.2f%%\n", gaps, duplicates, continuityPercentage)

	// 如果有缺口，显示具体缺口时段
	if len(gapPeriods) > 0 {
		fmt.Printf("缺口时段 (最多显示5个): \n")
		for i, period := range gapPeriods {
			if i >= 5 {
				fmt.Printf("... 还有 %d 个缺口\n", len(gapPeriods)-5)
				break
			}
			fmt.Printf("  - %s\n", period)
		}
	}
}

// 计算预期的时间间隔
func calculateExpectedDuration(interval string) time.Duration {
	switch interval {
	case "1m":
		return 1 * time.Minute
	case "5m":
		return 5 * time.Minute
	case "15m":
		return 15 * time.Minute
	case "30m":
		return 30 * time.Minute
	case "1h":
		return 1 * time.Hour
	case "2h":
		return 2 * time.Hour
	case "4h":
		return 4 * time.Hour
	case "6h":
		return 6 * time.Hour
	case "8h":
		return 8 * time.Hour
	case "12h":
		return 12 * time.Hour
	case "1d":
		return 24 * time.Hour
	case "3d":
		return 3 * 24 * time.Hour
	case "1w":
		return 7 * 24 * time.Hour
	default:
		return 0
	}
}
