package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
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
	
	if len(tables) == 0 {
		fmt.Println("警告: 数据库中没有找到任何表!")
		return
	}
	
	// 显示将要删除的表
	fmt.Println("\n以下是将要删除的表:")
	for _, table := range tables {
		fmt.Printf("- %s\n", table)
	}
	
	// 询问用户是否要删除所有表
	var answer string
	for {
		fmt.Print("\n确定要删除所有表吗? 此操作不可恢复! (y/n): ")
		fmt.Scanln(&answer)
		answer = strings.ToLower(answer)
		if answer == "y" || answer == "n" {
			break
		}
		fmt.Println("请输入 y 或 n")
	}
	
	if answer == "n" {
		fmt.Println("操作已取消")
		return
	}
	
	// 删除所有表
	fmt.Println("\n开始删除所有表...")
	
	// 开始时间计时
	startTime := time.Now()
	
	// 先禁用外键约束检查
	_, err = db.Exec("SET FOREIGN_KEY_CHECKS = 0")
	if err != nil {
		fmt.Printf("警告: 无法禁用外键约束: %v\n", err)
	}
	
	// 删除所有表
	deletedTables := 0
	for _, table := range tables {
		dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table)
		_, err := db.Exec(dropQuery)
		if err != nil {
			fmt.Printf("- %s: 删除失败: %v\n", table, err)
		} else {
			fmt.Printf("- %s: 已删除\n", table)
			deletedTables++
		}
	}
	
	// 重新启用外键约束检查
	_, err = db.Exec("SET FOREIGN_KEY_CHECKS = 1")
	if err != nil {
		fmt.Printf("警告: 无法重新启用外键约束: %v\n", err)
	}
	
	// 结束时间计时
	duration := time.Since(startTime)
	
	fmt.Printf("\n删除完成! 共删除 %d 个表，耗时 %v\n", deletedTables, duration)
	
	// 验证是否所有表都已删除
	var tableCount int
	err = db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ?", dbName).Scan(&tableCount)
	if err != nil {
		fmt.Printf("警告: 无法验证数据库是否为空: %v\n", err)
	} else {
		if tableCount == 0 {
			fmt.Println("数据库现在已完全清空!")
		} else {
			fmt.Printf("警告: 数据库中仍有 %d 个表!\n", tableCount)
		}
	}
} 