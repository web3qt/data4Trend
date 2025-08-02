package integrity

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"data4trend/pkg/backfill"
	"data4trend/pkg/config"
	"data4trend/pkg/storage"
)

// DataIntegrityService 确保数据完整性和连续性
type DataIntegrityService struct {
	config      *config.Config
	storage     *storage.ClickHouseStorage
	backfill    *backfill.BackfillService
	logger      *logrus.Logger
	ctx         context.Context
	cancel      context.CancelFunc
	mutex       sync.RWMutex
	lastCheck   time.Time
	isRunning   bool
	stats       *IntegrityStats
}

// IntegrityStats 跟踪完整性服务统计信息
type IntegrityStats struct {
	LastCheckTime     time.Time `json:"last_check_time"`
	TotalChecks       int64     `json:"total_checks"`
	GapsDetected      int64     `json:"gaps_detected"`
	GapsFixed         int64     `json:"gaps_fixed"`
	BackfillErrors    int64     `json:"backfill_errors"`
	DataCoverage      float64   `json:"data_coverage_pct"`
	OldestDataTime    time.Time `json:"oldest_data_time"`
	NewestDataTime    time.Time `json:"newest_data_time"`
	ContinuousDays    int       `json:"continuous_days"`
	mutex             sync.RWMutex
}

// NewDataIntegrityService 创建一个新的数据完整性服务
func NewDataIntegrityService(cfg *config.Config, storage *storage.ClickHouseStorage, backfill *backfill.BackfillService, logger *logrus.Logger) *DataIntegrityService {
	ctx, cancel := context.WithCancel(context.Background())
	return &DataIntegrityService{
		config:   cfg,
		storage:  storage,
		backfill: backfill,
		logger:   logger,
		ctx:      ctx,
		cancel:   cancel,
		stats: &IntegrityStats{
			LastCheckTime:  time.Now(),
			OldestDataTime: time.Now(),
			NewestDataTime: time.Now(),
		},
	}
}

// Start 启动数据完整性服务
func (dis *DataIntegrityService) Start() {
	dis.mutex.Lock()
	defer dis.mutex.Unlock()
	
	if dis.isRunning {
		dis.logger.Warn("Data integrity service is already running")
		return
	}
	
	dis.isRunning = true
	dis.logger.Info("Starting data integrity service...")
	
	// 运行初始完整性检查
	go dis.runInitialIntegrityCheck()
	
	// 启动定期完整性检查（每10分钟）
	go dis.periodicIntegrityCheck()
	
	// 启动连续缺口监控（每2分钟）
	go dis.continuousGapMonitoring()
}

// Stop 停止数据完整性服务
func (dis *DataIntegrityService) Stop() {
	dis.mutex.Lock()
	defer dis.mutex.Unlock()
	
	if !dis.isRunning {
		return
	}
	
	dis.logger.Info("Stopping data integrity service...")
	dis.cancel()
	dis.isRunning = false
}

// runInitialIntegrityCheck 执行全面的初始数据完整性检查
func (dis *DataIntegrityService) runInitialIntegrityCheck() {
	dis.logger.Info("Running initial data integrity check...")
	
	// 确保所有交易对有7天的历史数据
	endTime := time.Now().Truncate(time.Minute)
	startTime := endTime.Add(-7 * 24 * time.Hour)
	
	dis.ensureHistoricalDataCoverage(startTime, endTime)
	
	// 更新统计信息
	dis.updateIntegrityStats()
	
	dis.logger.Info("Initial data integrity check completed")
}

// periodicIntegrityCheck 运行定期全面完整性检查
func (dis *DataIntegrityService) periodicIntegrityCheck() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	
	for {
		select {
		case <-dis.ctx.Done():
			return
		case <-ticker.C:
			dis.runPeriodicCheck()
		}
	}
}

// continuousGapMonitoring 监控最近的数据缺口
func (dis *DataIntegrityService) continuousGapMonitoring() {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()
	
	for {
		select {
		case <-dis.ctx.Done():
			return
		case <-ticker.C:
			dis.checkRecentGaps()
		}
	}
}

// runPeriodicCheck 执行定期完整性检查
func (dis *DataIntegrityService) runPeriodicCheck() {
	dis.logger.Debug("Running periodic data integrity check...")
	
	// 检查过去24小时的缺口
	endTime := time.Now().Truncate(time.Minute)
	startTime := endTime.Add(-24 * time.Hour)
	
	// 检测并修复缺口
	totalGaps := dis.detectAndFixGaps(startTime, endTime)
	
	// 确保7天数据保留
	dis.ensureDataRetention()
	
	// 更新统计信息
	dis.updateIntegrityStats()
	
	dis.stats.mutex.Lock()
	dis.stats.TotalChecks++
	dis.stats.LastCheckTime = time.Now()
	dis.stats.mutex.Unlock()
	
	if totalGaps > 0 {
		dis.logger.Infof("Periodic check completed: detected and fixed %d gaps", totalGaps)
	} else {
		dis.logger.Debug("Periodic check completed: no gaps detected")
	}
}

// checkRecentGaps 检查过去30分钟的缺口
func (dis *DataIntegrityService) checkRecentGaps() {
	endTime := time.Now().Truncate(time.Minute)
	startTime := endTime.Add(-30 * time.Minute)
	
	gapsFixed := dis.detectAndFixGaps(startTime, endTime)
	if gapsFixed > 0 {
		dis.logger.Infof("Recent gap check: fixed %d gaps in last 30 minutes", gapsFixed)
	}
}

// ensureHistoricalDataCoverage 确保完整的历史数据覆盖
func (dis *DataIntegrityService) ensureHistoricalDataCoverage(startTime, endTime time.Time) {
	dis.logger.Infof("Ensuring historical data coverage from %s to %s", 
		startTime.Format("2006-01-02 15:04:05"), endTime.Format("2006-01-02 15:04:05"))
	
	totalGapsFixed := 0
	
	// 检查每个交易对
	for _, symbol := range dis.config.Symbols {
		select {
		case <-dis.ctx.Done():
			return
		default:
		}
		
		// 检测此交易对的缺口
		gaps, err := dis.storage.DetectDataGaps(symbol, startTime, endTime)
		if err != nil {
			dis.logger.Errorf("Failed to detect gaps for %s: %v", symbol, err)
			continue
		}
		
		if len(gaps) == 0 {
			continue
		}
		
		dis.logger.Infof("Symbol %s: found %d gaps, total missing: %d minutes", 
			symbol, len(gaps), dis.calculateTotalMissingMinutes(gaps))
		
		// 修复此交易对的缺口
		for _, gap := range gaps {
			select {
			case <-dis.ctx.Done():
				return
			default:
			}
			
			result, err := dis.backfill.BackfillGap(gap)
			if err != nil {
				dis.logger.Errorf("Failed to backfill gap for %s: %v", symbol, err)
				dis.stats.mutex.Lock()
				dis.stats.BackfillErrors++
				dis.stats.mutex.Unlock()
				continue
			}
			
			if result.Success {
				totalGapsFixed++
				dis.stats.mutex.Lock()
				dis.stats.GapsFixed++
				dis.stats.mutex.Unlock()
				dis.logger.Debugf("Successfully backfilled gap for %s: %d records inserted", 
					symbol, result.Inserted)
			}
			
			// 限制速率以避免压垮Binance API
			time.Sleep(200 * time.Millisecond)
		}
		
		// 交易对之间暂停
		time.Sleep(100 * time.Millisecond)
	}
	
	dis.logger.Infof("Historical data coverage check completed: fixed %d gaps", totalGapsFixed)
}

// detectAndFixGaps 检测并修复指定时间范围内的数据缺口
func (dis *DataIntegrityService) detectAndFixGaps(startTime, endTime time.Time) int {
	// 获取所有缺口
	allGaps, err := dis.storage.GetDataGapsForAllSymbols()
	if err != nil {
		dis.logger.Errorf("Failed to get data gaps: %v", err)
		return 0
	}
	
	totalGapsFixed := 0
	totalGapsDetected := 0
	
	// 处理每个交易对的缺口
	for symbol, gaps := range allGaps {
		select {
		case <-dis.ctx.Done():
			return totalGapsFixed
		default:
		}
		
		// 过滤指定时间范围内的缺口
		relevantGaps := dis.filterGapsByTimeRange(gaps, startTime, endTime)
		if len(relevantGaps) == 0 {
			continue
		}
		
		totalGapsDetected += len(relevantGaps)
		dis.logger.Debugf("Symbol %s: found %d gaps in time range", symbol, len(relevantGaps))
		
		// 修复每个缺口
		for _, gap := range relevantGaps {
			result, err := dis.backfill.BackfillGap(gap)
			if err != nil {
				dis.logger.Errorf("Failed to backfill gap for %s: %v", symbol, err)
				dis.stats.mutex.Lock()
				dis.stats.BackfillErrors++
				dis.stats.mutex.Unlock()
				continue
			}
			
			if result.Success {
				totalGapsFixed++
				dis.stats.mutex.Lock()
				dis.stats.GapsFixed++
				dis.stats.mutex.Unlock()
			}
			
			// 限制速率
			time.Sleep(150 * time.Millisecond)
		}
	}
	
	dis.stats.mutex.Lock()
	dis.stats.GapsDetected += int64(totalGapsDetected)
	dis.stats.mutex.Unlock()
	
	return totalGapsFixed
}

// ensureDataRetention 确保7天数据保留
func (dis *DataIntegrityService) ensureDataRetention() {
	// 检查是否有超过7天需要维护的数据
	// 并确保最近的数据完整
	endTime := time.Now().Truncate(time.Minute)
	startTime := endTime.Add(-7 * 24 * time.Hour)
	
	// 快速检查7天窗口内的主要缺口
	gapsFixed := dis.detectAndFixGaps(startTime, endTime)
	if gapsFixed > 0 {
		dis.logger.Infof("Data retention check: fixed %d gaps in 7-day window", gapsFixed)
	}
}

// filterGapsByTimeRange 过滤落在指定时间范围内的缺口
func (dis *DataIntegrityService) filterGapsByTimeRange(gaps []*storage.DataGap, startTime, endTime time.Time) []*storage.DataGap {
	var filtered []*storage.DataGap
	for _, gap := range gaps {
		// 检查缺口是否与时间范围重叠
		if gap.EndTime.After(startTime) && gap.StartTime.Before(endTime) {
			filtered = append(filtered, gap)
		}
	}
	return filtered
}

// calculateTotalMissingMinutes 计算缺口的总缺失分钟数
func (dis *DataIntegrityService) calculateTotalMissingMinutes(gaps []*storage.DataGap) int {
	total := 0
	for _, gap := range gaps {
		total += gap.Missing
	}
	return total
}

// updateIntegrityStats 更新完整性统计信息
func (dis *DataIntegrityService) updateIntegrityStats() {
	// 计算数据覆盖率和连续性指标
	// 这是一个简化的实现
	dis.stats.mutex.Lock()
	defer dis.stats.mutex.Unlock()
	
	// 从存储获取基本统计信息
	stats, err := dis.storage.GetStats()
	if err != nil {
		dis.logger.Errorf("Failed to get storage stats: %v", err)
		return
	}
	
	// 如果可用，更新时间戳
	if latestTime, ok := stats["latest_record_time"].(time.Time); ok {
		dis.stats.NewestDataTime = latestTime
	}
	
	// 计算大致的数据覆盖率（简化版）
	// 在实际实现中，这会更加复杂
	totalSymbols := len(dis.config.Symbols)
	expectedRecords := totalSymbols * 7 * 24 * 60 // 7 days * 24 hours * 60 minutes
	
	if totalRecords, ok := stats["total_records"].(int64); ok {
		if expectedRecords > 0 {
			dis.stats.DataCoverage = float64(totalRecords) / float64(expectedRecords) * 100
			if dis.stats.DataCoverage > 100 {
				dis.stats.DataCoverage = 100
			}
		}
	}
	
	// 估算连续天数（简化版）
	dis.stats.ContinuousDays = 7 // 现在假设7天，需要更复杂的计算
}

// GetStats 返回当前完整性统计信息
func (dis *DataIntegrityService) GetStats() *IntegrityStats {
	dis.stats.mutex.RLock()
	defer dis.stats.mutex.RUnlock()
	
	// 返回副本以避免竞态条件
	return &IntegrityStats{
		LastCheckTime:  dis.stats.LastCheckTime,
		TotalChecks:    dis.stats.TotalChecks,
		GapsDetected:   dis.stats.GapsDetected,
		GapsFixed:      dis.stats.GapsFixed,
		BackfillErrors: dis.stats.BackfillErrors,
		DataCoverage:   dis.stats.DataCoverage,
		OldestDataTime: dis.stats.OldestDataTime,
		NewestDataTime: dis.stats.NewestDataTime,
		ContinuousDays: dis.stats.ContinuousDays,
	}
}

// IsRunning 返回服务是否正在运行
func (dis *DataIntegrityService) IsRunning() bool {
	dis.mutex.RLock()
	defer dis.mutex.RUnlock()
	return dis.isRunning
}

// ForceIntegrityCheck 强制立即进行完整性检查
func (dis *DataIntegrityService) ForceIntegrityCheck() {
	dis.logger.Info("Force integrity check triggered")
	go dis.runPeriodicCheck()
}

// BackfillSymbolRange 为特定交易对和时间范围回补数据
func (dis *DataIntegrityService) BackfillSymbolRange(symbol string, startTime, endTime time.Time) error {
	dis.logger.Infof("Manual backfill requested for %s from %s to %s", 
		symbol, startTime.Format("2006-01-02 15:04:05"), endTime.Format("2006-01-02 15:04:05"))
	
	results, err := dis.backfill.BackfillSymbol(symbol, startTime, endTime)
	if err != nil {
		return fmt.Errorf("failed to backfill symbol %s: %w", symbol, err)
	}
	
	totalInserted := 0
	for _, result := range results {
		if result.Success {
			totalInserted += result.Inserted
		}
	}
	
	dis.logger.Infof("Manual backfill completed for %s: %d records inserted", symbol, totalInserted)
	return nil
}