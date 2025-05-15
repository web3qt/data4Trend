package trendscanner

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/web3qt/data4Trend/pkg/logging"
)

// CSVReporter 负责生成统一的CSV报告
type CSVReporter struct {
	outputDir    string
	fileMutex    sync.Mutex
	currentFile  string
}

// NewCSVReporter 创建一个新的CSV报告生成器
func NewCSVReporter(outputDir string) *CSVReporter {
	return &CSVReporter{
		outputDir: outputDir,
	}
}

// SaveResult 保存单个任务结果到统一的CSV文件
func (r *CSVReporter) SaveResult(result *TaskResult) error {
	if r.outputDir == "" {
		return fmt.Errorf("未设置CSV输出目录")
	}

	// 确保输出目录存在
	if err := os.MkdirAll(r.outputDir, 0755); err != nil {
		return fmt.Errorf("创建CSV输出目录失败: %w", err)
	}

	// 检查K线时间是否是当天的数据
	if !IsToday(result.KLineTime) {
		logging.Logger.WithFields(logrus.Fields{
			"symbol":     result.Symbol,
			"task":       result.TaskName,
			"kline_time": result.KLineTime.Format(time.RFC3339),
		}).Debug("CSV导出：跳过非当天的K线数据")
		return nil
	}

	// 生成当天的CSV文件名
	timestamp := time.Now().Format("20060102")
	filename := fmt.Sprintf("trend_scan_results_%s.csv", timestamp)
	filePath := filepath.Join(r.outputDir, filename)

	// 锁定文件操作
	r.fileMutex.Lock()
	defer r.fileMutex.Unlock()

	// 检查文件是否存在，如果不存在则创建并写入表头
	fileExists := false
	if _, err := os.Stat(filePath); err == nil {
		fileExists = true
	}

	// 打开文件，准备追加数据
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("打开CSV文件失败: %w", err)
	}
	defer file.Close()

	// 如果文件不存在，写入表头
	if !fileExists {
		header := "发现时间,币种,任务类型,条件描述,K线开始时间,K线结束时间,最后价格,值1,值2,值3,值4,值5\n"
		if _, err := file.WriteString(header); err != nil {
			return fmt.Errorf("写入CSV表头失败: %w", err)
		}
	}

	// 条件描述 (如果有)
	var condition string
	if len(result.Descriptions) > 0 {
		condition = result.Descriptions[0]
	}

	// 准备写入的数据行
	var values [5]float64
	valueKeys := []string{"", "", "", "", ""}
	i := 0
	for k, v := range result.Values {
		if i < 5 {
			values[i] = v
			valueKeys[i] = k
			i++
		}
	}

	// 格式化数据行
	dataLine := fmt.Sprintf("%s,%s,%s,\"%s\",%s,%s,%.8f,%.2f,%.2f,%.2f,%.8f,%d\n",
		result.FoundTime.Format("2006-01-02 15:04:05"),
		result.Symbol,
		result.TaskName,
		condition,
		result.KLineTime.Format("2006-01-02 15:04:05"),
		result.KLineEndTime.Format("2006-01-02 15:04:05"),
		safeGetValue(result.Values, "last_price", 0),
		values[0],
		values[1],
		values[2],
		values[3],
		int(values[4]),
	)

	// 写入数据行
	if _, err := file.WriteString(dataLine); err != nil {
		return fmt.Errorf("写入CSV数据失败: %w", err)
	}

	// 记录日志
	logging.Logger.WithFields(logrus.Fields{
		"symbol":    result.Symbol,
		"task":      result.TaskName,
		"condition": condition,
		"file":      filePath,
	}).Info("发现符合条件的币种")

	r.currentFile = filePath
	return nil
}

// SaveResults 保存多个任务结果到统一的CSV文件
func (r *CSVReporter) SaveResults(results []*TaskResult) error {
	for _, result := range results {
		if err := r.SaveResult(result); err != nil {
			logging.Logger.WithError(err).WithFields(logrus.Fields{
				"symbol": result.Symbol,
				"task":   result.TaskName,
			}).Error("保存结果到统一CSV失败")
		}
	}
	return nil
}

// GetCurrentFile 获取当前使用的CSV文件路径
func (r *CSVReporter) GetCurrentFile() string {
	r.fileMutex.Lock()
	defer r.fileMutex.Unlock()
	return r.currentFile
}

// 帮助函数

// getValueOrDefault 从结果值映射中获取值，如果不存在则返回默认值
func getValueOrDefault(values map[string]float64, key string, defaultVal float64) float64 {
	if val, ok := values[key]; ok {
		return val
	}
	return defaultVal
}

// safeGetValue 安全地从map中获取值，如果不存在则返回默认值
func safeGetValue(m map[string]float64, key string, defaultVal float64) float64 {
	if val, ok := m[key]; ok {
		return val
	}
	return defaultVal
} 