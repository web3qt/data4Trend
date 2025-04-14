package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type KLineData struct {
	OpenTime time.Time
	Symbol   string
	Interval string
}

func main() {
	// 连接数据库
	db, err := sql.Open("mysql", "root:123456@tcp(localhost:3306)/kline_data?parseTime=true")
	if err != nil {
		log.Fatalf("连接数据库失败: %v", err)
	}
	defer db.Close()

	// 检查数据库连接
	if err := db.Ping(); err != nil {
		log.Fatalf("数据库连接失败: %v", err)
	}

	// 查询特定表和K线周期的数据
	symbol := "btc"
	if len(os.Args) > 1 {
		symbol = os.Args[1]
	}

	interval := "5m"
	if len(os.Args) > 2 {
		interval = os.Args[2]
	}

	limit := 100
	if len(os.Args) > 3 {
		fmt.Sscanf(os.Args[3], "%d", &limit)
	}

	fmt.Printf("检查 %s 表 %s 周期的K线数据间隔\n", symbol, interval)

	// 检查数据表结构
	checkTableStmt := fmt.Sprintf("DESCRIBE %s", symbol)
	rows, err := db.Query(checkTableStmt)
	if err != nil {
		log.Fatalf("检查表结构失败: %v", err)
	}
	fmt.Println("表结构信息:")
	var field, fieldType, null, key string
	var defaultVal, extra sql.NullString
	for rows.Next() {
		err := rows.Scan(&field, &fieldType, &null, &key, &defaultVal, &extra)
		if err != nil {
			log.Printf("解析表结构失败: %v", err)
			continue
		}
		fmt.Printf("  字段: %s, 类型: %s\n", field, fieldType)
	}
	rows.Close()

	// 执行查询
	queryStmt := fmt.Sprintf(
		"SELECT open_time FROM %s WHERE interval_type = ? ORDER BY open_time ASC LIMIT ?",
		symbol,
	)
	fmt.Printf("执行查询: %s\n", queryStmt)

	rows, err = db.Query(queryStmt, interval, limit)
	if err != nil {
		log.Fatalf("查询数据失败: %v", err)
	}
	defer rows.Close()

	// 收集所有K线数据
	var klines []KLineData
	for rows.Next() {
		var openTimeStr string
		if err := rows.Scan(&openTimeStr); err != nil {
			log.Printf("解析行数据失败: %v", err)
			continue
		}

		// 尝试解析时间字符串（RFC3339格式）
		openTime, err := time.Parse(time.RFC3339, openTimeStr)
		if err != nil {
			log.Printf("解析RFC3339时间失败 (%s): %v", openTimeStr, err)
			
			// 尝试其他时间格式
			openTime, err = time.Parse("2006-01-02 15:04:05", openTimeStr)
			if err != nil {
				log.Printf("解析标准时间格式也失败 (%s): %v", openTimeStr, err)
				continue
			}
		}

		klines = append(klines, KLineData{
			OpenTime: openTime,
			Symbol:   symbol,
			Interval: interval,
		})
	}

	// 检查是否有结果
	if len(klines) == 0 {
		log.Fatalf("未找到K线数据")
	}

	fmt.Printf("共找到 %d 条K线数据\n", len(klines))

	// 按时间排序
	sort.Slice(klines, func(i, j int) bool {
		return klines[i].OpenTime.Before(klines[j].OpenTime)
	})

	// 打印前5条和最后5条数据
	fmt.Println("\n前5条数据:")
	for i := 0; i < len(klines) && i < 5; i++ {
		fmt.Printf("%d. %s\n", i+1, klines[i].OpenTime.Format(time.RFC3339))
	}

	if len(klines) > 10 {
		fmt.Println("\n最后5条数据:")
		for i := len(klines) - 5; i < len(klines); i++ {
			fmt.Printf("%d. %s\n", i+1, klines[i].OpenTime.Format(time.RFC3339))
		}
	}

	// 检查K线间隔
	fmt.Println("\nK线时间间隔检查:")
	expectedDuration := calculateIntervalDuration(interval)
	validCount := 0
	invalidCount := 0

	for i := 1; i < len(klines); i++ {
		duration := klines[i].OpenTime.Sub(klines[i-1].OpenTime)
		isValid := duration == expectedDuration

		if i <= 5 || i >= len(klines)-5 {
			fmt.Printf("%d->%d: %s->%s, 间隔=%s, 期望=%s, 是否正确=%v\n",
				i, i+1,
				klines[i-1].OpenTime.Format(time.RFC3339),
				klines[i].OpenTime.Format(time.RFC3339),
				duration,
				expectedDuration,
				isValid,
			)
		}

		if isValid {
			validCount++
		} else {
			invalidCount++
		}
	}

	fmt.Printf("\n总结: 总检查=%d, 正确=%d, 错误=%d, 正确率=%.2f%%\n",
		len(klines)-1, validCount, invalidCount, float64(validCount)*100.0/float64(len(klines)-1))
}

// calculateIntervalDuration 计算周期的时间间隔
func calculateIntervalDuration(interval string) time.Duration {
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
