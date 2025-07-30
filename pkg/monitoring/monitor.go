package monitoring

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"data4trend/internal/types"
	"data4trend/pkg/storage"
	"data4trend/pkg/websocket"
)

// Monitor represents the system monitoring component
type Monitor struct {
	storage   *storage.ClickHouseStorage
	websocket *websocket.Client
	logger    *logrus.Logger
	startTime time.Time
	mutex     sync.RWMutex
	stats     *types.MonitoringStats
}

// NewMonitor creates a new monitoring instance
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

// Start starts the monitoring system
func (m *Monitor) Start() {
	m.logger.Info("Starting monitoring system...")

	// Start periodic monitoring
	go m.periodicMonitoring()

	m.logger.Info("Monitoring system started")
}

// periodicMonitoring performs periodic system monitoring
func (m *Monitor) periodicMonitoring() {
	ticker := time.NewTicker(60 * time.Second) // Report every minute
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.updateStats()
			m.logReport()
		}
	}
}

// updateStats updates monitoring statistics
func (m *Monitor) updateStats() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Update uptime
	m.stats.Uptime = time.Since(m.startTime)

	// Get WebSocket stats
	if m.websocket != nil {
		wsStats := m.websocket.GetStats()
		m.stats.MessagesTotal = wsStats.MessagesTotal
		m.stats.ErrorsTotal = wsStats.ErrorsTotal
		m.stats.ActiveStreams = wsStats.Connections
		m.stats.LastMessageTime = wsStats.LastMessageTime

		// Calculate messages per second
		if m.stats.Uptime.Seconds() > 0 {
			m.stats.MessagesPerSec = float64(m.stats.MessagesTotal) / m.stats.Uptime.Seconds()
		}

		// Calculate error rate
		if m.stats.MessagesTotal > 0 {
			m.stats.ErrorRate = float64(m.stats.ErrorsTotal) / float64(m.stats.MessagesTotal) * 100
		}
	}

	// Determine health status and issues
	m.updateHealthStatus()
}

// updateHealthStatus updates the health status and identifies issues
func (m *Monitor) updateHealthStatus() {
	issues := []string{}

	// Check if we're receiving data
	if time.Since(m.stats.LastMessageTime) > 5*time.Minute {
		issues = append(issues, "No data received in the last 5 minutes")
	} else if time.Since(m.stats.LastMessageTime) > 2*time.Minute {
		issues = append(issues, "No data received in the last 2 minutes")
	}

	// Check if we have no data at all
	if m.stats.MessagesTotal == 0 && m.stats.Uptime > 2*time.Minute {
		issues = append(issues, "No data received yet")
	}

	// Check error rate
	if m.stats.ErrorRate > 10 {
		issues = append(issues, "High error rate (>10%)")
	} else if m.stats.ErrorRate > 5 {
		issues = append(issues, "Elevated error rate (>5%)")
	}

	// Check active streams
	if m.stats.ActiveStreams == 0 {
		issues = append(issues, "No active WebSocket connections")
	}

	// Check database connectivity
	if m.storage != nil {
		if err := m.storage.TestConnection(); err != nil {
			issues = append(issues, "Database connection failed")
		}
	}

	// Determine overall health status
	if len(issues) == 0 {
		m.stats.HealthStatus = "healthy"
	} else if m.containsCriticalIssues(issues) {
		m.stats.HealthStatus = "critical"
	} else {
		m.stats.HealthStatus = "warning"
	}

	m.stats.Issues = issues
}

// containsCriticalIssues checks if there are critical issues
func (m *Monitor) containsCriticalIssues(issues []string) bool {
	for _, issue := range issues {
		if issue == "Database connection failed" ||
			issue == "No active WebSocket connections" {
			return true
		}
	}
	return false
}

// logReport logs the monitoring report
func (m *Monitor) logReport() {
	m.mutex.RLock()
	stats := *m.stats // Copy stats
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

	// Log database stats if available
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

// GetStats returns current monitoring statistics
func (m *Monitor) GetStats() *types.MonitoringStats {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	// Return a copy of the stats
	stats := *m.stats
	return &stats
}

// GetHealthStatus returns the current health status
func (m *Monitor) GetHealthStatus() string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.stats.HealthStatus
}

// GetIssues returns current issues
func (m *Monitor) GetIssues() []string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return append([]string{}, m.stats.Issues...) // Return a copy
}

// IsHealthy returns true if the system is healthy
func (m *Monitor) IsHealthy() bool {
	return m.GetHealthStatus() == "healthy"
}

// LogSystemInfo logs system information
func (m *Monitor) LogSystemInfo() {
	m.logger.Info("=== System Information ===")
	m.logger.Infof("Start time: %v", m.startTime.Format(time.RFC3339))
	m.logger.Infof("Monitoring interval: 60 seconds")
	m.logger.Info("=== End System Information ===")
}