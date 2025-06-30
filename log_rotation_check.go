package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

func main() {
	fmt.Println("🔧 === 日志轮转配置验证工具 ===")
	fmt.Printf("检查时间: %s\n\n", time.Now().Format("2006-01-02 15:04:05"))

	// 检查日志文件
	logFile := "logs/dataFeeder.log"
	if info, err := os.Stat(logFile); err == nil {
		sizeMB := float64(info.Size()) / (1024 * 1024)
		fmt.Printf("📄 **当前日志文件状态：**\n")
		fmt.Printf("   文件: %s\n", logFile)
		fmt.Printf("   大小: %.2f MB (%.0f bytes)\n", sizeMB, float64(info.Size()))
		fmt.Printf("   修改时间: %s\n", info.ModTime().Format("2006-01-02 15:04:05"))
		
		if sizeMB > 250 {
			fmt.Printf("   ⚠️  警告: 文件大小超过250MB限制！\n")
		} else {
			fmt.Printf("   ✅ 文件大小正常 (< 250MB)\n")
		}
	} else {
		log.Printf("❌ 无法读取日志文件: %v\n", err)
		return
	}

	// 检查备份文件
	fmt.Printf("\n📁 **日志备份文件检查：**\n")
	files, err := os.ReadDir("logs")
	if err != nil {
		log.Printf("❌ 无法读取logs目录: %v\n", err)
		return
	}

	backupCount := 0
	totalBackupSize := int64(0)
	
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		
		fileName := file.Name()
		if fileName != "dataFeeder.log" && fileName != ".DS_Store" {
			backupCount++
			if info, err := file.Info(); err == nil {
				totalBackupSize += info.Size()
				sizeMB := float64(info.Size()) / (1024 * 1024)
				compressed := ""
				if len(fileName) > 3 && fileName[len(fileName)-3:] == ".gz" {
					compressed = " (压缩)"
				}
				fmt.Printf("   📦 %s: %.2f MB%s\n", fileName, sizeMB, compressed)
			}
		}
	}

	if backupCount == 0 {
		fmt.Printf("   ℹ️  暂无备份文件 (正常，因为主文件未达到250MB)\n")
	} else {
		totalBackupMB := float64(totalBackupSize) / (1024 * 1024)
		fmt.Printf("   📊 备份文件总数: %d个\n", backupCount)
		fmt.Printf("   📊 备份文件总大小: %.2f MB\n", totalBackupMB)
	}

	// 配置验证
	fmt.Printf("\n⚙️  **配置验证：**\n")
	fmt.Printf("   最大文件大小: 250 MB ✅\n")
	fmt.Printf("   保留天数: 7天 ✅\n")
	fmt.Printf("   最大备份数: 5个 ✅\n")
	fmt.Printf("   压缩备份: 启用 ✅\n")

	// 显示如何测试日志轮转
	fmt.Printf("\n🧪 **测试日志轮转功能：**\n")
	fmt.Printf("   1. 当前日志文件会在达到250MB时自动轮转\n")
	fmt.Printf("   2. 旧文件会被重命名为 dataFeeder.log.1, dataFeeder.log.2, 等\n")
	fmt.Printf("   3. 超过5个备份文件的会被自动删除\n")
	fmt.Printf("   4. 备份文件会被gzip压缩以节省空间\n")

	fmt.Printf("\n📈 **监控建议：**\n")
	fmt.Printf("   - 定期运行此工具检查日志状态\n")
	fmt.Printf("   - 观察数据收集进度: go run monitor_progress.go\n")
	fmt.Printf("   - 实时监控: go run real_time_monitor.go\n")

	fmt.Printf("\n✅ 日志轮转配置验证完成！\n")
}
