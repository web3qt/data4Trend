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

	// 查询BTC表中的记录数
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM btc").Scan(&count)
	if err != nil {
		log.Fatalf("查询记录数失败: %v", err)
	}

	fmt.Printf("btc表中有 %d 条记录\n", count)

	// 查询最近的记录
	fmt.Println("\nBTC表中的记录:")
	rows, err := db.Query(`
		SELECT id, interval_type, open_time, close_time, open_price, high_price, low_price, close_price, volume 
		FROM btc 
		ORDER BY open_time DESC 
		LIMIT 15
	`)
	if err != nil {
		log.Fatalf("查询记录失败: %v", err)
	}
	defer rows.Close()

	fmt.Println("ID | 时间周期 | 开盘时间 | 收盘时间 | 开盘价 | 最高价 | 最低价 | 收盘价 | 交易量")
	fmt.Println("--------------------------------------------------------")

	for rows.Next() {
		var id int
		var intervalStr string
		var openTime, closeTime time.Time
		var open, high, low, close, volume float64

		if err := rows.Scan(&id, &intervalStr, &openTime, &closeTime, &open, &high, &low, &close, &volume); err != nil {
			log.Fatalf("读取记录失败: %v", err)
		}

		fmt.Printf("%d | %s | %s | %s | %.2f | %.2f | %.2f | %.2f | %.2f\n",
			id, intervalStr, openTime.Format("2006-01-02 15:04:05"),
			closeTime.Format("2006-01-02 15:04:05"),
			open, high, low, close, volume)
	}
} 