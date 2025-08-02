package monitoring

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"data4trend/internal/types"
	"data4trend/pkg/storage"
	"data4trend/pkg/websocket"
)

// Monitor 代表系统监控组件
type Monitor struct {
	storage   *storage.ClickHouseStorage
	websocket *websocket.Client
	logger    *logrus.Logger
	startTime time.Time
	mutex     sync.RWMutex
	stats     *types.MonitoringStats
}

// NewMonitor 创建一个新的监控实例
func NewMonitor(storage *storage.ClickHouseStorage, ws *websocket.Client, logger *logrus.Logger) *Monitor {
	return &Monitor{
		storage:   storage,
		websocket: ws,
		logger:    logger,
		startTime: time.Now(),
		stats: &types.MonitoringStats{
			Uptime:          0,
			MessagesTotal:   0,
			MessagesPerSec:  0,
			ErrorsTotal:     0,
			ErrorRate:       0,
			ActiveStreams:   0,
			HealthStatus:    "starting",
			Issues:          []string{},
			LastMessageTime: time.Now(),
		},
	}
}

// Start 启动监控系统
func (m *Monitor) Start() {
	m.logger.Info("Starting monitoring system...")

	// 启动周期性监控
	go m.periodicMonitoring()

	m.logger.Info("Monitoring system started")
}

// periodicMonitoring 执行周期性系统监控
func (m *Monitor) periodicMonitoring() {
	ticker := time.NewTicker(60 * time.Second) // 每分钟报告一次
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.updateStats()
			m.logReport()
		}
	}
}

// updateStats 更新监控统计信息
func (m *Monitor) updateStats() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 更新运行时间
	m.stats.Uptime = time.Since(m.startTime)

	// 获取WebSocket统计信息
	if m.websocket != nil {
		wsStats := m.websocket.GetStats()
		m.stats.MessagesTotal = wsStats.MessagesTotal
		m.stats.ErrorsTotal = wsStats.ErrorsTotal
		m.stats.ActiveStreams = wsStats.Connections
		m.stats.LastMessageTime = wsStats.LastMessageTime

		// 计算每秒消息数
		if m.stats.Uptime.Seconds() > 0 {
			m.stats.MessagesPerSec = float64(m.stats.MessagesTotal) / m.stats.Uptime.Seconds()
		}

		// 计算错误率
		if m.stats.MessagesTotal > 0 {
			m.stats.ErrorRate = float64(m.stats.ErrorsTotal) / float64(m.stats.MessagesTotal) * 100
		}
	}

	// 确定健康状态和问题
	m.updateHealthStatus()
}

// updateHealthStatus 更新健康状态并识别问题
func (m *Monitor) updateHealthStatus() {
	issues := []string{}

	// 检查是否正在接收数据
	if time.Since(m.stats.LastMessageTime) > 5*time.Minute {
		issues = append(issues, "No data received in the last 5 minutes")
	} else if time.Since(m.stats.LastMessageTime) > 2*time.Minute {
		issues = append(issues, "No data received in the last 2 minutes")
	}

	// 检查是否完全没有数据
	if m.stats.MessagesTotal == 0 && m.stats.Uptime > 2*time.Minute {
		issues = append(issues, "No data received yet")
	}

	// 检查错误率
	if m.stats.ErrorRate > 10 {
		issues = append(issues, "High error rate (>10%)")
	} else if m.stats.ErrorRate > 5 {
		issues = append(issues, "Elevated error rate (>5%)")
	}

	// 检查活跃流
	if m.stats.ActiveStreams == 0 {
		issues = append(issues, "No active WebSocket connections")
	}

	// 检查数据库连接
	if m.storage != nil {
		if err := m.storage.TestConnection(); err != nil {
			issues = append(issues, "Database connection failed")
		}
	}

	// 确定整体健康状态
	if len(issues) == 0 {
		m.stats.HealthStatus = "healthy"
	} else if m.containsCriticalIssues(issues) {
		m.stats.HealthStatus = "critical"
	} else {
		m.stats.HealthStatus = "warning"
	}

	m.stats.Issues = issues
}

// containsCriticalIssues 检查是否存在严重问题
func (m *Monitor) containsCriticalIssues(issues []string) bool {
	for _, issue := range issues {
		if issue == "Database connection failed" ||
			issue == "No active WebSocket connections" {
			return true
		}
	}
	return false
}

// logReport 记录监控报告
func (m *Monitor) logReport() {
	m.mutex.RLock()
	stats := *m.stats // 复制统计信息
	m.mutex.RUnlock()

	m.logger.Info("=== System Monitoring Report ===")
	m.logger.Infof("Uptime: %v, Messages: %d/%d, Errors: %d, Connections: %d",
		stats.Uptime.Truncate(time.Second),
		stats.MessagesTotal,
		int64(stats.MessagesPerSec*stats.Uptime.Seconds()),
		stats.ErrorsTotal,
		stats.ActiveStreams)

	m.logger.Infof("Performance: %.2f msg/s, Error rate: %.2f%%",
		stats.MessagesPerSec, stats.ErrorRate)

	m.logger.Infof("Health: %s, Issues: %v", stats.HealthStatus, stats.Issues)

	// 如果可用，记录数据库统计信息
	if m.storage != nil {
		if dbStats, err := m.storage.GetStats(); err == nil {
			m.logger.Infof("Database: %v records, %v symbols",
				dbStats["total_records"], dbStats["unique_symbols"])
			if latestTime, ok := dbStats["latest_record_time"].(time.Time); ok {
				m.logger.Infof("Latest record: %v ago",
					time.Since(latestTime).Truncate(time.Second))
			}
		}
	}
}

// GetStats 返回当前监控统计信息
func (m *Monitor) GetStats() *types.MonitoringStats {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	// 返回统计信息的副本
	stats := *m.stats
	return &stats
}

// GetHealthStatus 返回当前健康状态
func (m *Monitor) GetHealthStatus() string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.stats.HealthStatus
}

// GetIssues 返回当前问题
func (m *Monitor) GetIssues() []string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return append([]string{}, m.stats.Issues...) // 返回副本
}

// IsHealthy 如果系统健康则返回true
func (m *Monitor) IsHealthy() bool {
	return m.GetHealthStatus() == "healthy"
}

// LogSystemInfo 记录系统信息
func (m *Monitor) LogSystemInfo() {
	m.logger.Info("=== System Information ===")
	m.logger.Infof("Start time: %v", m.startTime.Format(time.RFC3339))
	m.logger.Infof("Monitoring interval: 60 seconds")
	m.logger.Info("=== End System Information ===")
}