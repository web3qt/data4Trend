package trendscanner

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

	// 生成当天的CSV文件名
	timestamp := time.Now().Format("20060102")
	filename := fmt.Sprintf("trend_scan_results_%s.csv", timestamp)
	filePath := filepath.Join(r.outputDir, filename)

	r.fileMutex.Lock()
	defer r.fileMutex.Unlock()

	// 检查文件是否存在
	fileExists := false
	if _, err := os.Stat(filePath); err == nil {
		fileExists = true
	}

	// 打开文件（追加模式）
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("打开CSV文件失败: %w", err)
	}
	defer file.Close()

	// 如果文件不存在，写入CSV头
	if !fileExists {
		headers := []string{
			"时间戳",
			"币种",
			"任务类型",
			"条件",
			"K线开始时间",
			"K线结束时间",
			"最后价格",
		}

		// 添加常见的任务特定值
		commonValues := []string{
			"波动率",
			"振幅",
			"MA周期",
			"当前MA",
			"连续K线数",
		}
		headers = append(headers, commonValues...)

		// 写入标题行
		if _, err := file.WriteString(strings.Join(headers, ",") + "\n"); err != nil {
			return fmt.Errorf("写入CSV标题行失败: %w", err)
		}
	}

	// 准备任务条件描述
	condition := "未知条件"
	if len(result.Descriptions) > 0 {
		condition = result.Descriptions[0]
		condition = strings.ReplaceAll(condition, ",", ";") // 防止CSV格式问题
	}

	// 准备数据行的基本字段
	values := []string{
		result.FoundTime.Format("2006-01-02 15:04:05"),
		result.Symbol,
		result.TaskName,
		fmt.Sprintf("\"%s\"", condition),
		result.KLineTime.Format("2006-01-02 15:04:05"),
		result.KLineEndTime.Format("2006-01-02 15:04:05"),
		fmt.Sprintf("%.8f", getValueOrDefault(result.Values, "lastPrice", 0)),
	}

	// 添加常见的任务特定值
	values = append(values, 
		fmt.Sprintf("%.2f", getValueOrDefault(result.Values, "volatility", 0)),
		fmt.Sprintf("%.2f", getValueOrDefault(result.Values, "amplitude", 0)),
		fmt.Sprintf("%.0f", getValueOrDefault(result.Values, "ma_period", 0)),
		fmt.Sprintf("%.8f", getValueOrDefault(result.Values, "current_ma", 0)),
		fmt.Sprintf("%.0f", getValueOrDefault(result.Values, "above_ma_klines", 0)),
	)

	// 写入数据行
	if _, err := file.WriteString(strings.Join(values, ",") + "\n"); err != nil {
		return fmt.Errorf("写入CSV数据行失败: %w", err)
	}

	r.currentFile = filePath

	logging.Logger.WithFields(logrus.Fields{
		"symbol":  result.Symbol,
		"task":    result.TaskName,
		"file":    filePath,
		"condition": condition,
	}).Info("发现符合条件的币种")

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

// getValueOrDefault 从map中获取值，如果不存在则返回默认值
func getValueOrDefault(m map[string]float64, key string, defaultVal float64) float64 {
	if val, ok := m[key]; ok {
		return val
	}
	return defaultVal
} 