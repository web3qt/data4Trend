package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strings"
	"syscall"
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

type MonitorStats struct {
	ActiveCoins     int
	InProgressCoins int
	WaitingCoins    int
	Total1h         int
	Total15m        int
	Total1d         int
}

func main() {
	dsn := "root:123456@tcp(localhost:3306)/data4trend?parseTime=true"
	
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("无法连接到数据库: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("数据库连接测试失败: %v", err)
	}

	// 设置信号处理，优雅退出
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	fmt.Printf("🚀 实时数据收集监控器启动\n")
	fmt.Printf("⏱️  每30秒自动刷新，按 Ctrl+C 退出\n\n")

	var lastStats MonitorStats

	// 立即显示第一次
	showRealTimeProgress(db, &lastStats)

	for {
		select {
		case <-ticker.C:
			clearScreen()
			showRealTimeProgress(db, &lastStats)
		case <-c:
			fmt.Printf("\n👋 监控器已停止\n")
			return
		}
	}
}

func clearScreen() {
	cmd := exec.Command("clear")
	cmd.Stdout = os.Stdout
	cmd.Run()
}

func showRealTimeProgress(db *sql.DB, lastStats *MonitorStats) {
	now := time.Now().Format("2006-01-02 15:04:05")
	fmt.Printf("🔄 === 实时数据收集监控 === %s\n\n", now)

	// 1. 当前处理状态
	fmt.Printf("🎯 **当前处理状态：**\n")
	showCurrentActivity()

	// 2. 获取所有表和进度
	tables := getAllTables(db)
	progresses := analyzeCoinProgress(db, tables)
	
	// 3. 计算统计数据
	currentStats := calculateStats(progresses)
	
	// 4. 显示变化
	fmt.Printf("\n📊 **收集状态汇总：**\n")
	showStatsComparison(currentStats, *lastStats)
	
	// 5. 显示热门币种进度
	fmt.Printf("\n🏆 **热门币种进度（前15个）：**\n")
	sort.Slice(progresses, func(i, j int) bool {
		return progresses[i].Count1h > progresses[j].Count1h
	})
	
	fmt.Printf("   %-10s %-7s %-7s %-7s %-11s %s\n", "币种", "1h", "15m", "1d", "最新时间", "状态")
	fmt.Printf("   %s\n", strings.Repeat("-", 65))
	
	for i, progress := range progresses {
		if i >= 15 {
			break
		}
		
		latest := "N/A"
		if progress.Latest != "" && len(progress.Latest) >= 16 {
			latest = progress.Latest[5:16] // 显示月-日 时:分
		}
		
		status := getProgressStatus(progress)
		fmt.Printf("   %-10s %-7d %-7d %-7d %-11s %s\n", 
			strings.ToUpper(progress.Symbol), progress.Count1h, progress.Count15m, 
			progress.Count1d, latest, status)
	}

	// 6. 显示最近活跃的币种
	fmt.Printf("\n🔥 **最近活跃币种：**\n")
	showRecentlyActive(progresses)

	// 7. 系统状态
	fmt.Printf("\n⚙️  **系统状态：**\n")
	showSystemStatus()

	*lastStats = currentStats
	
	fmt.Printf("\n⏰ 下次更新：30秒后 | 按 Ctrl+C 退出\n")
}

func showCurrentActivity() {
	// 查看最近日志中的活动
	cmd := exec.Command("tail", "-5", "logs/dataFeeder.log")
	output, err := cmd.Output()
	if err != nil {
		fmt.Printf("   📝 无法读取日志文件\n")
		return
	}
	
	lines := strings.Split(string(output), "\n")
	activeSymbols := make(map[string]bool)
	
	for _, line := range lines {
		if strings.Contains(line, "symbol=") && strings.Contains(line, "开始写入数据点") {
			parts := strings.Split(line, "symbol=")
			if len(parts) > 1 {
				symbolPart := strings.Split(parts[1], " ")[0]
				symbolPart = strings.TrimSpace(symbolPart)
				if symbolPart != "" && strings.HasSuffix(symbolPart, "USDT") {
					activeSymbols[symbolPart] = true
				}
			}
		}
	}
	
	if len(activeSymbols) == 0 {
		fmt.Printf("   📝 系统正在准备中...\n")
		return
	}
	
	fmt.Printf("   🟢 正在收集：")
	count := 0
	for symbol := range activeSymbols {
		if count > 0 {
			fmt.Printf(", ")
		}
		fmt.Printf("%s", symbol)
		count++
	}
	fmt.Printf("\n")
}

func getAllTables(db *sql.DB) []string {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
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
		
		db.QueryRow("SELECT COUNT(*) FROM `"+table+"` WHERE interval_type = '1h'").Scan(&progress.Count1h)
		db.QueryRow("SELECT COUNT(*) FROM `"+table+"` WHERE interval_type = '15m'").Scan(&progress.Count15m)
		db.QueryRow("SELECT COUNT(*) FROM `"+table+"` WHERE interval_type = '1d'").Scan(&progress.Count1d)
		
		if progress.Count1h > 0 {
			db.QueryRow("SELECT MIN(open_time), MAX(open_time) FROM `"+table+"` WHERE interval_type = '1h'").Scan(&progress.Earliest, &progress.Latest)
		}
		
		progresses = append(progresses, progress)
	}
	
	return progresses
}

func calculateStats(progresses []CoinProgress) MonitorStats {
	stats := MonitorStats{}
	
	for _, progress := range progresses {
		stats.Total1h += progress.Count1h
		stats.Total15m += progress.Count15m
		stats.Total1d += progress.Count1d
		
		if progress.Count1h > 1000 {
			stats.ActiveCoins++
		} else if progress.Count1h > 0 {
			stats.InProgressCoins++
		} else {
			stats.WaitingCoins++
		}
	}
	
	return stats
}

func showStatsComparison(current, last MonitorStats) {
	delta1h := current.Total1h - last.Total1h
	delta15m := current.Total15m - last.Total15m
	delta1d := current.Total1d - last.Total1d
	
	fmt.Printf("   🟢 活跃收集币种：%d个\n", current.ActiveCoins)
	fmt.Printf("   🟡 进行中币种：%d个\n", current.InProgressCoins)
	fmt.Printf("   🔴 等待中币种：%d个\n", current.WaitingCoins)
	
	fmt.Printf("   📊 总数据量：")
	fmt.Printf("1h(%d条", current.Total1h)
	if delta1h > 0 {
		fmt.Printf(" ↗️+%d", delta1h)
	}
	fmt.Printf(") ")
	
	fmt.Printf("15m(%d条", current.Total15m)
	if delta15m > 0 {
		fmt.Printf(" ↗️+%d", delta15m)
	}
	fmt.Printf(") ")
	
	fmt.Printf("1d(%d条", current.Total1d)
	if delta1d > 0 {
		fmt.Printf(" ↗️+%d", delta1d)
	}
	fmt.Printf(")\n")
}

func showRecentlyActive(progresses []CoinProgress) {
	// 找出最近更新的币种（最新时间在今天的）
	today := time.Now().Format("2006-01-02")
	recentCount := 0
	
	fmt.Printf("   🔥 今日有新数据的币种：")
	for _, progress := range progresses {
		if strings.HasPrefix(progress.Latest, today) {
			recentCount++
		}
	}
	fmt.Printf("%d个\n", recentCount)
}

func getProgressStatus(progress CoinProgress) string {
	if progress.Count1h > 10000 {
		return "🟢 丰富"
	} else if progress.Count1h > 1000 {
		return "🟡 中等"
	} else if progress.Count1h > 0 {
		return "🔴 少量"
	} else {
		return "⚪ 空"
	}
}

func showSystemStatus() {
	// 检查进程状态
	cmd := exec.Command("ps", "aux")
	output, err := cmd.Output()
	if err == nil && strings.Contains(string(output), "dataFeeder") {
		fmt.Printf("   ✅ dataFeeder进程：运行中\n")
	} else {
		fmt.Printf("   ❌ dataFeeder进程：未运行\n")
	}
	
	// 日志文件大小
	if stat, err := os.Stat("logs/dataFeeder.log"); err == nil {
		sizeMB := float64(stat.Size()) / 1024 / 1024
		fmt.Printf("   📄 日志文件大小：%.1f MB\n", sizeMB)
	}
} 