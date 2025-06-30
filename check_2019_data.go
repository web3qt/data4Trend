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
	fmt.Println("正在连接到数据库检查2019年1h数据...")
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

	// 首先获取所有表名
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		log.Fatalf("查询表失败: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}
		tables = append(tables, tableName)
	}

	// 检查几个主要币种的2019年1h数据
	targetSymbols := []string{"btc", "eth", "bnb", "ada", "sol", "xrp", "lto", "bttc"}
	
	fmt.Println("\n=== 检查2019年1h数据收集进度 ===")
	
	totalFound := 0
	for _, symbol := range targetSymbols {
		// 检查表是否存在
		tableExists := false
		for _, table := range tables {
			if table == symbol {
				tableExists = true
				break
			}
		}
		
		if !tableExists {
			fmt.Printf("❓ %s: 表不存在\n", symbol)
			continue
		}
		
		var count int
		
		// 检查2019年的1h数据数量
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE interval_type = '1h' AND open_time >= '2019-01-01' AND open_time < '2020-01-01'", symbol)
		err = db.QueryRow(query).Scan(&count)
		if err != nil {
			fmt.Printf("❌ %s: 查询失败 - %v\n", symbol, err)
			continue
		}
		
		if count > 0 {
			// 获取时间范围
			var earliest, latest time.Time
			rangeQuery := fmt.Sprintf("SELECT MIN(open_time), MAX(open_time) FROM %s WHERE interval_type = '1h' AND open_time >= '2019-01-01' AND open_time < '2020-01-01'", symbol)
			err = db.QueryRow(rangeQuery).Scan(&earliest, &latest)
			if err != nil {
				fmt.Printf("✅ %s: %d条2019年1h记录\n", symbol, count)
			} else {
				fmt.Printf("✅ %s: %d条2019年1h记录, 时间范围: %s 到 %s\n", 
					symbol, count, 
					earliest.Format("2006-01-02 15:04"), 
					latest.Format("2006-01-02 15:04"))
			}
			totalFound += count
		} else {
			fmt.Printf("⏳ %s: 暂无2019年1h数据\n", symbol)
		}
	}
	
	if totalFound > 0 {
		fmt.Printf("\n🎉 总计找到 %d 条2019年1h数据记录！\n", totalFound)
	} else {
		fmt.Println("\n⏳ 暂未发现2019年1h数据，系统可能还在收集中...")
	}
	
	// 检查最近是否有历史数据被插入
	fmt.Println("\n=== 检查最近插入的历史数据样本 ===")
	checkSymbols := []string{"btc", "eth", "lto"}
	for _, symbol := range checkSymbols {
		// 检查表是否存在
		tableExists := false
		for _, table := range tables {
			if table == symbol {
				tableExists = true
				break
			}
		}
		
		if !tableExists {
			continue
		}
		
		// 查找最近插入的最早的1h数据
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE interval_type = '1h' AND created_at >= NOW() - INTERVAL 10 MINUTE", symbol)
		var recentCount int
		err = db.QueryRow(query).Scan(&recentCount)
		if err != nil || recentCount == 0 {
			continue
		}
		
		// 获取最近插入数据的时间范围
		rangeQuery := fmt.Sprintf("SELECT MIN(open_time), MAX(open_time) FROM %s WHERE interval_type = '1h' AND created_at >= NOW() - INTERVAL 10 MINUTE", symbol)
		var earliest, latest time.Time
		err = db.QueryRow(rangeQuery).Scan(&earliest, &latest)
		if err != nil {
			fmt.Printf("📊 %s: 最近10分钟插入了%d条1h数据\n", symbol, recentCount)
		} else {
			fmt.Printf("📊 %s: 最近10分钟插入了%d条1h数据，时间范围: %s 到 %s\n", 
				symbol, recentCount, 
				earliest.Format("2006-01-02 15:04"), 
				latest.Format("2006-01-02 15:04"))
		}
	}
} 