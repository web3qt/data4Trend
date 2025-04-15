package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// 数据库连接配置
	dbHost := "localhost"
	dbPort := 3306
	dbUser := "root"
	dbPass := "123456"
	dbName := "data4trend"

	// 从环境变量读取配置（如果有）
	if host := os.Getenv("MYSQL_HOST"); host != "" {
		dbHost = host
	}
	if user := os.Getenv("MYSQL_USER"); user != "" {
		dbUser = user
	}
	if pass := os.Getenv("MYSQL_PASSWORD"); pass != "" {
		dbPass = pass
	}
	if name := os.Getenv("MYSQL_DATABASE"); name != "" {
		dbName = name
	}

	// 构建连接字符串
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", 
		dbUser, dbPass, dbHost, dbPort, dbName)

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

	// 查询所有表
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		log.Fatalf("查询表失败: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			log.Fatalf("读取表名失败: %v", err)
		}
		tables = append(tables, tableName)
	}

	fmt.Printf("数据库中有 %d 个表\n", len(tables))
	
	// 只显示前20个表名
	if len(tables) > 0 {
		fmt.Println("表列表(前20个):")
		count := 20
		if len(tables) < count {
			count = len(tables)
		}
		for i := 0; i < count; i++ {
			fmt.Printf("- %s\n", tables[i])
		}
	}

	// 查找像是币种的表(通常是小写字母)
	coinTables := make([]string, 0)
	for _, table := range tables {
		// 排除常见的非币种表
		if table != "connection_test" && len(table) <= 10 {
			coinTables = append(coinTables, table)
		}
	}
	
	if len(coinTables) == 0 {
		fmt.Println("警告: 数据库中没有找到币种表!")
		return
	}
	
	fmt.Printf("\n找到 %d 个可能的币种表\n", len(coinTables))
	
	// 检查币种表中是否有数据
	totalRecords := 0
	tablesWithData := 0
	
	fmt.Println("\n币种表记录统计:")
	for _, table := range coinTables {
		var count int
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
		err = db.QueryRow(countQuery).Scan(&count)
		if err != nil {
			fmt.Printf("- %s: 查询失败: %v\n", table, err)
			continue
		}
		
		if count > 0 {
			tablesWithData++
			totalRecords += count
			fmt.Printf("- %s: %d 条记录\n", table, count)
		}
	}
	
	fmt.Printf("\n总结: %d 个币种表中有数据，共计 %d 条记录\n", tablesWithData, totalRecords)
	
	// 如果至少有一个表有数据，查询其中一些记录作为示例
	if tablesWithData > 0 {
		// 找到第一个有数据的表
		var sampleTable string
		for _, table := range coinTables {
			var count int
			countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
			if err := db.QueryRow(countQuery).Scan(&count); err == nil && count > 0 {
				sampleTable = table
				break
			}
		}
		
		if sampleTable != "" {
			fmt.Printf("\n%s 表中的10条样本记录:\n", sampleTable)
			query := fmt.Sprintf(`
				SELECT id, interval_type, open_time, close_time, open_price, high_price, low_price, close_price, volume 
				FROM %s 
				ORDER BY open_time DESC 
				LIMIT 10
			`, sampleTable)
			rows, err := db.Query(query)
			if err != nil {
				fmt.Printf("查询样本记录失败: %v\n", err)
				return
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
					fmt.Printf("读取记录失败: %v\n", err)
					continue
				}

				fmt.Printf("%d | %s | %s | %s | %.2f | %.2f | %.2f | %.2f | %.2f\n",
					id, intervalStr, openTime.Format("2006-01-02 15:04:05"),
					closeTime.Format("2006-01-02 15:04:05"),
					open, high, low, close, volume)
			}
		}
	} else {
		fmt.Println("\n警告: 所有币种表中都没有数据!")
	}
} 