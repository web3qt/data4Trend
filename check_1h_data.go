package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// 数据库连接配置
	dsn := "root:123456@tcp(localhost:3306)/data4trend?parseTime=true"

	// 连接数据库
	fmt.Println("正在连接到数据库...")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("无法连接到数据库: %v", err)
	}
	defer db.Close()

	// 检查连接
	err = db.Ping()
	if err != nil {
		log.Fatalf("数据库连接测试失败: %v", err)
	}
	fmt.Println("数据库连接成功!")

	// 检查BTC表中1h数据的统计信息
	fmt.Println("\n=== BTC 1小时数据分析 ===")
	
	// 查询1h数据总数
	var count1h int
	err = db.QueryRow("SELECT COUNT(*) FROM btc WHERE interval_type = '1h'").Scan(&count1h)
	if err != nil {
		log.Fatalf("查询1h数据数量失败: %v", err)
	}
	fmt.Printf("BTC 1h数据总数: %d 条\n", count1h)

	// 查询1h数据的时间范围
	var earliestTime, latestTime time.Time
	err = db.QueryRow("SELECT MIN(open_time), MAX(open_time) FROM btc WHERE interval_type = '1h'").Scan(&earliestTime, &latestTime)
	if err != nil {
		log.Fatalf("查询1h数据时间范围失败: %v", err)
	}
	fmt.Printf("最早1h数据: %s\n", earliestTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("最新1h数据: %s\n", latestTime.Format("2006-01-02 15:04:05"))

	// 检查2019年的数据
	var count2019 int
	err = db.QueryRow("SELECT COUNT(*) FROM btc WHERE interval_type = '1h' AND open_time >= '2019-01-01' AND open_time < '2020-01-01'").Scan(&count2019)
	if err != nil {
		log.Fatalf("查询2019年1h数据失败: %v", err)
	}
	fmt.Printf("2019年1h数据数量: %d 条\n", count2019)

	// 显示最早的几条1h记录
	fmt.Println("\n最早的10条1h记录:")
	rows, err := db.Query(`
		SELECT id, open_time, close_time, open_price, high_price, low_price, close_price, volume 
		FROM btc 
		WHERE interval_type = '1h'
		ORDER BY open_time ASC 
		LIMIT 10
	`)
	if err != nil {
		log.Fatalf("查询最早1h记录失败: %v", err)
	}
	defer rows.Close()

	fmt.Println("ID | 开盘时间 | 收盘时间 | 开盘价 | 最高价 | 最低价 | 收盘价 | 交易量")
	fmt.Println("--------------------------------------------------------")

	for rows.Next() {
		var id int
		var openTime, closeTime time.Time
		var open, high, low, close, volume float64

		if err := rows.Scan(&id, &openTime, &closeTime, &open, &high, &low, &close, &volume); err != nil {
			log.Fatalf("读取记录失败: %v", err)
		}

		fmt.Printf("%d | %s | %s | %.2f | %.2f | %.2f | %.2f | %.2f\n",
			id, openTime.Format("2006-01-02 15:04:05"),
			closeTime.Format("2006-01-02 15:04:05"),
			open, high, low, close, volume)
	}

	// 检查其他几个主要币种的1h数据
	fmt.Println("\n=== 其他主要币种1h数据统计 ===")
	symbols := []string{"eth", "bnb", "ada", "sol", "xrp"}
	
	for _, symbol := range symbols {
		var count int
		var earliest, latest time.Time
		
		query := fmt.Sprintf("SELECT COUNT(*), MIN(open_time), MAX(open_time) FROM %s WHERE interval_type = '1h'", symbol)
		err = db.QueryRow(query).Scan(&count, &earliest, &latest)
		if err != nil {
			fmt.Printf("%s: 查询失败: %v\n", symbol, err)
			continue
		}
		
		fmt.Printf("%s: %d条记录, 时间范围: %s 到 %s\n", 
			symbol, count, 
			earliest.Format("2006-01-02 15:04"), 
			latest.Format("2006-01-02 15:04"))
	}
} 