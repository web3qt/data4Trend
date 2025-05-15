package trendscanner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/web3qt/data4Trend/pkg/logging"
)

// TaskManager 任务管理器
type TaskManager struct {
	tasks       map[string]ScanTask
	taskResults map[string][]*TaskResult
	db          *gorm.DB
	csvOutputDir string
	mutex       sync.RWMutex
}

// NewTaskManager 创建一个新的任务管理器
func NewTaskManager(db *gorm.DB, csvOutputDir string) *TaskManager {
	return &TaskManager{
		tasks:       make(map[string]ScanTask),
		taskResults: make(map[string][]*TaskResult),
		db:          db,
		csvOutputDir: csvOutputDir,
		mutex:       sync.RWMutex{},
	}
}

// RegisterTask 注册一个扫描任务
func (m *TaskManager) RegisterTask(task ScanTask) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	m.tasks[task.Name()] = task
	logging.Logger.WithField("task", task.Name()).Info("注册扫描任务")
}

// GetTask 获取一个扫描任务
func (m *TaskManager) GetTask(name string) ScanTask {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	return m.tasks[name]
}

// GetAllTasks 获取所有扫描任务
func (m *TaskManager) GetAllTasks() []ScanTask {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	var tasks []ScanTask
	for _, task := range m.tasks {
		tasks = append(tasks, task)
	}
	
	return tasks
}

// ExecuteTasks 执行所有启用的扫描任务
func (m *TaskManager) ExecuteTasks(ctx context.Context, symbol string) ([]*TaskResult, error) {
	m.mutex.RLock()
	tasks := make([]ScanTask, 0, len(m.tasks))
	for _, task := range m.tasks {
		if task.IsEnabled() {
			tasks = append(tasks, task)
		}
	}
	m.mutex.RUnlock()
	
	var results []*TaskResult
	
	// 执行所有任务
	for _, task := range tasks {
		result, err := task.Execute(ctx, m.db, symbol)
		if err != nil {
			logging.Logger.WithError(err).WithFields(logrus.Fields{
				"task":   task.Name(),
				"symbol": symbol,
			}).Error("执行扫描任务失败")
			continue
		}
		
		if result != nil && result.Found {
			results = append(results, result)
			
			// 保存结果到CSV
			if m.csvOutputDir != "" {
				if err := m.saveResultToCSV(task.Name(), result); err != nil {
					logging.Logger.WithError(err).WithFields(logrus.Fields{
						"task":   task.Name(),
						"symbol": symbol,
					}).Error("保存结果到CSV失败")
				}
			}
		}
	}
	
	// 保存任务结果到内存中
	if len(results) > 0 {
		m.mutex.Lock()
		m.taskResults[symbol] = append(m.taskResults[symbol], results...)
		m.mutex.Unlock()
	}
	
	return results, nil
}

// ExecuteTask 执行指定的扫描任务
func (m *TaskManager) ExecuteTask(ctx context.Context, taskName string, symbol string) (*TaskResult, error) {
	m.mutex.RLock()
	task, exists := m.tasks[taskName]
	m.mutex.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("任务 %s 不存在", taskName)
	}
	
	if !task.IsEnabled() {
		return nil, fmt.Errorf("任务 %s 已禁用", taskName)
	}
	
	result, err := task.Execute(ctx, m.db, symbol)
	if err != nil {
		return nil, err
	}
	
	if result != nil && result.Found {
		// 保存结果到CSV
		if m.csvOutputDir != "" {
			if err := m.saveResultToCSV(taskName, result); err != nil {
				logging.Logger.WithError(err).WithFields(logrus.Fields{
					"task":   taskName,
					"symbol": symbol,
				}).Error("保存结果到CSV失败")
			}
		}
		
		// 保存任务结果到内存中
		m.mutex.Lock()
		m.taskResults[symbol] = append(m.taskResults[symbol], result)
		m.mutex.Unlock()
	}
	
	return result, nil
}

// saveResultToCSV 保存任务结果到CSV文件
func (m *TaskManager) saveResultToCSV(taskName string, result *TaskResult) error {
	if m.csvOutputDir == "" {
		return fmt.Errorf("未设置CSV输出目录")
	}
	
	// 创建任务结果目录
	taskDir := filepath.Join(m.csvOutputDir, taskName)
	if err := createDirIfNotExists(taskDir); err != nil {
		return err
	}
	
	// 创建文件名：任务名_日期.csv
	timestamp := time.Now().Format("20060102")
	filename := fmt.Sprintf("%s_%s.csv", taskName, timestamp)
	filePath := filepath.Join(taskDir, filename)
	
	// 检查文件是否存在
	fileExists := false
	if _, err := getFileInfo(filePath); err == nil {
		fileExists = true
	}
	
	// 打开文件
	file, err := openFileForAppend(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	
	// 如果文件不存在，写入CSV头
	if !fileExists {
		// 准备标题行
		headers := []string{
			"Symbol", "TaskName", "FoundTime", "KLineTime", "KLineEndTime",
		}
		
		// 添加任务特定的值列
		for key := range result.Values {
			headers = append(headers, key)
		}
		
		// 添加描述列
		headers = append(headers, "Description")
		
		// 写入标题行
		if _, err := file.WriteString(strings.Join(headers, ",") + "\n"); err != nil {
			return fmt.Errorf("写入CSV标题行失败: %w", err)
		}
	}
	
	// 准备数据行
	values := []string{
		result.Symbol,
		result.TaskName,
		result.FoundTime.Format(time.RFC3339),
		result.KLineTime.Format(time.RFC3339),
		result.KLineEndTime.Format(time.RFC3339),
	}
	
	// 添加任务特定的值
	for _, key := range getSortedKeys(result.Values) {
		values = append(values, fmt.Sprintf("%.8f", result.Values[key]))
	}
	
	// 添加描述（将多个描述合并为一个字符串）
	description := strings.Join(result.Descriptions, "; ")
	description = strings.ReplaceAll(description, ",", ";") // 避免CSV格式问题
	values = append(values, fmt.Sprintf("\"%s\"", description))
	
	// 写入数据行
	if _, err := file.WriteString(strings.Join(values, ",") + "\n"); err != nil {
		return fmt.Errorf("写入CSV数据行失败: %w", err)
	}
	
	logging.Logger.WithFields(logrus.Fields{
		"task":   taskName,
		"symbol": result.Symbol,
		"file":   filePath,
	}).Debug("任务结果已保存到CSV")
	
	return nil
}

// GetTaskResults 获取某个币种的所有任务结果
func (m *TaskManager) GetTaskResults(symbol string) []*TaskResult {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	return m.taskResults[symbol]
}

// ClearTaskResults 清空任务结果
func (m *TaskManager) ClearTaskResults() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	m.taskResults = make(map[string][]*TaskResult)
}

// 帮助函数

// createDirIfNotExists 创建目录（如果不存在）
func createDirIfNotExists(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return os.MkdirAll(dir, 0755)
	}
	return nil
}

// getFileInfo 获取文件信息
func getFileInfo(filePath string) (os.FileInfo, error) {
	return os.Stat(filePath)
}

// openFileForAppend 打开文件（追加模式）
func openFileForAppend(filePath string) (*os.File, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return os.Create(filePath)
	}
	return os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
}

// getSortedKeys 获取有序的键列表
func getSortedKeys(m map[string]float64) []string {
	// 获取所有键
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	
	// 对键进行排序，确保输出顺序一致
	sort.Strings(keys)
	
	return keys
} 