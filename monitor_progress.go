package main

import (
	"database/sql"
	"fmt"
	"log"
	"os/exec"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type CoinProgress struct {
	Symbol    string
	Count1h   int
	Count15m  int
	Count1d   int
	Earliest  string
	Latest    string
	Status    string
}

func main() {
	// 数据库连接配置
	dsn := "root:123456@tcp(localhost:3306)/data4trend?parseTime=true"

	// 连接数据库
	fmt.Println("🔍 正在监控数据收集进度...")
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

	fmt.Printf("\n📊 === 数据收集进度监控 === %s\n", time.Now().Format("2006-01-02 15:04:05"))
	
	// 1. 查看当前正在处理的币种（从日志）
	fmt.Printf("\n🔄 **当前正在处理的币种：**\n")
	showCurrentProcessing()
	
	// 2. 查看数据库中所有表
	tables := getAllTables(db)
	fmt.Printf("\n📈 **数据库表统计：** 共 %d 个币种表\n", len(tables))
	
	// 3. 分析各币种的数据收集进度
	fmt.Printf("\n📋 **各币种数据收集进度详情：**\n")
	progresses := analyzeCoinProgress(db, tables)
	
	// 按1h数据数量排序
	sort.Slice(progresses, func(i, j int) bool {
		return progresses[i].Count1h > progresses[j].Count1h
	})
	
	// 显示前30个币种的详细进度
	fmt.Printf("   （显示前30个币种，按1h数据量排序）\n")
	fmt.Printf("   %-12s %-8s %-8s %-8s %-12s %-12s %s\n", "币种", "1h数据", "15m数据", "1d数据", "最早时间", "最新时间", "状态")
	fmt.Printf("   %s\n", strings.Repeat("-", 90))
	
	for i, progress := range progresses {
		if i >= 30 {
			break
		}
		
		earliest := "N/A"
		latest := "N/A"
		if progress.Earliest != "" && len(progress.Earliest) >= 10 {
			earliest = progress.Earliest[:10]
		}
		if progress.Latest != "" && len(progress.Latest) >= 10 {
			latest = progress.Latest[:10]
		}
		
		status := getProgressStatus(progress)
		fmt.Printf("   %-12s %-8d %-8d %-8d %-12s %-12s %s\n", 
			strings.ToUpper(progress.Symbol), progress.Count1h, progress.Count15m, 
			progress.Count1d, earliest, latest, status)
	}
	
	// 4. 统计汇总
	fmt.Printf("\n📊 **收集进度汇总：**\n")
	showProgressSummary(progresses)
	
	// 5. 显示系统状态
	fmt.Printf("\n⚙️  **系统状态：**\n")
	showSystemStatus()
	
	fmt.Printf("\n💡 **说明：**\n")
	fmt.Printf("   - 🟢 活跃收集：正在积极收集历史数据\n")
	fmt.Printf("   - 🟡 进行中：有部分数据，继续收集中\n") 
	fmt.Printf("   - 🔴 等待中：等待开始收集或刚开始\n")
	fmt.Printf("   - 重新运行此脚本可查看最新进度\n")
}

func showCurrentProcessing() {
	// 查看最近的日志，了解当前正在处理的币种
	cmd := exec.Command("tail", "-20", "logs/dataFeeder.log")
	output, err := cmd.Output()
	if err != nil {
		fmt.Printf("   无法读取日志文件: %v\n", err)
		return
	}
	
	lines := strings.Split(string(output), "\n")
	symbols := make(map[string]bool)
	
	for _, line := range lines {
		if strings.Contains(line, "symbol=") {
			// 提取symbol信息
			parts := strings.Split(line, "symbol=")
			if len(parts) > 1 {
				symbolPart := strings.Split(parts[1], " ")[0]
				symbolPart = strings.TrimSpace(symbolPart)
				if symbolPart != "" && strings.HasSuffix(symbolPart, "USDT") {
					symbols[symbolPart] = true
				}
			}
		}
	}
	
	if len(symbols) == 0 {
		fmt.Printf("   📝 暂时没有检测到正在处理的币种（可能在准备阶段）\n")
		return
	}
	
	fmt.Printf("   📝 最近正在处理的币种：")
	count := 0
	for symbol := range symbols {
		if count > 0 {
			fmt.Printf(", ")
		}
		fmt.Printf("%s", symbol)
		count++
		if count >= 10 { // 只显示前10个
			break
		}
	}
	if len(symbols) > 10 {
		fmt.Printf(" 等%d个币种", len(symbols))
	}
	fmt.Printf("\n")
}

func getAllTables(db *sql.DB) []string {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		log.Printf("查询表失败: %v", err)
		return []string{}
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
	return tables
}

func analyzeCoinProgress(db *sql.DB, tables []string) []CoinProgress {
	var progresses []CoinProgress
	
	for _, table := range tables {
		progress := CoinProgress{Symbol: table}
		
		// 查询各时间间隔的数据量
		db.QueryRow("SELECT COUNT(*) FROM `"+table+"` WHERE interval_type = '1h'").Scan(&progress.Count1h)
		db.QueryRow("SELECT COUNT(*) FROM `"+table+"` WHERE interval_type = '15m'").Scan(&progress.Count15m)
		db.QueryRow("SELECT COUNT(*) FROM `"+table+"` WHERE interval_type = '1d'").Scan(&progress.Count1d)
		
		// 查询时间范围（主要看1h数据）
		if progress.Count1h > 0 {
			db.QueryRow("SELECT MIN(open_time), MAX(open_time) FROM `"+table+"` WHERE interval_type = '1h'").Scan(&progress.Earliest, &progress.Latest)
		}
		
		progresses = append(progresses, progress)
	}
	
	return progresses
}

func getProgressStatus(progress CoinProgress) string {
	if progress.Count1h > 1000 {
		return "🟢 活跃收集"
	} else if progress.Count1h > 0 {
		return "🟡 进行中"
	} else {
		return "🔴 等待中"
	}
}

func showProgressSummary(progresses []CoinProgress) {
	active := 0
	inProgress := 0
	waiting := 0
	total1h := 0
	total15m := 0
	total1d := 0
	
	for _, progress := range progresses {
		total1h += progress.Count1h
		total15m += progress.Count15m
		total1d += progress.Count1d
		
		if progress.Count1h > 1000 {
			active++
		} else if progress.Count1h > 0 {
			inProgress++
		} else {
			waiting++
		}
	}
	
	fmt.Printf("   🟢 活跃收集币种：%d个\n", active)
	fmt.Printf("   🟡 进行中币种：%d个\n", inProgress)
	fmt.Printf("   🔴 等待中币种：%d个\n", waiting)
	fmt.Printf("   📊 总数据量：1h(%d条) 15m(%d条) 1d(%d条)\n", total1h, total15m, total1d)
}

func showSystemStatus() {
	// 检查dataFeeder进程
	cmd := exec.Command("ps", "aux")
	output, err := cmd.Output()
	if err == nil && strings.Contains(string(output), "dataFeeder") {
		fmt.Printf("   ✅ dataFeeder进程正在运行\n")
	} else {
		fmt.Printf("   ❌ dataFeeder进程未运行\n")
	}
	
	// 检查收集器状态文件大小
	cmd = exec.Command("wc", "-l", "config/collector_state.yaml")
	output, err = cmd.Output()
	if err == nil {
		lines := strings.TrimSpace(string(output))
		fmt.Printf("   📄 收集器状态文件：%s\n", lines)
	}
} 