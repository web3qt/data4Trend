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

// DataIntegrityService ensures data completeness and continuity
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

// IntegrityStats tracks integrity service statistics
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

// NewDataIntegrityService creates a new data integrity service
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

// Start starts the data integrity service
func (dis *DataIntegrityService) Start() {
	dis.mutex.Lock()
	defer dis.mutex.Unlock()
	
	if dis.isRunning {
		dis.logger.Warn("Data integrity service is already running")
		return
	}
	
	dis.isRunning = true
	dis.logger.Info("Starting data integrity service...")
	
	// Run initial integrity check
	go dis.runInitialIntegrityCheck()
	
	// Start periodic integrity checks (every 10 minutes)
	go dis.periodicIntegrityCheck()
	
	// Start continuous gap monitoring (every 2 minutes)
	go dis.continuousGapMonitoring()
}

// Stop stops the data integrity service
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

// runInitialIntegrityCheck performs comprehensive initial data integrity check
func (dis *DataIntegrityService) runInitialIntegrityCheck() {
	dis.logger.Info("Running initial data integrity check...")
	
	// Ensure 7 days of historical data for all symbols
	endTime := time.Now().Truncate(time.Minute)
	startTime := endTime.Add(-7 * 24 * time.Hour)
	
	dis.ensureHistoricalDataCoverage(startTime, endTime)
	
	// Update statistics
	dis.updateIntegrityStats()
	
	dis.logger.Info("Initial data integrity check completed")
}

// periodicIntegrityCheck runs periodic comprehensive integrity checks
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

// continuousGapMonitoring monitors for recent data gaps
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

// runPeriodicCheck performs periodic integrity check
func (dis *DataIntegrityService) runPeriodicCheck() {
	dis.logger.Debug("Running periodic data integrity check...")
	
	// Check last 24 hours for gaps
	endTime := time.Now().Truncate(time.Minute)
	startTime := endTime.Add(-24 * time.Hour)
	
	// Detect and fix gaps
	totalGaps := dis.detectAndFixGaps(startTime, endTime)
	
	// Ensure 7-day data retention
	dis.ensureDataRetention()
	
	// Update statistics
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

// checkRecentGaps checks for gaps in the last 30 minutes
func (dis *DataIntegrityService) checkRecentGaps() {
	endTime := time.Now().Truncate(time.Minute)
	startTime := endTime.Add(-30 * time.Minute)
	
	gapsFixed := dis.detectAndFixGaps(startTime, endTime)
	if gapsFixed > 0 {
		dis.logger.Infof("Recent gap check: fixed %d gaps in last 30 minutes", gapsFixed)
	}
}

// ensureHistoricalDataCoverage ensures complete historical data coverage
func (dis *DataIntegrityService) ensureHistoricalDataCoverage(startTime, endTime time.Time) {
	dis.logger.Infof("Ensuring historical data coverage from %s to %s", 
		startTime.Format("2006-01-02 15:04:05"), endTime.Format("2006-01-02 15:04:05"))
	
	totalGapsFixed := 0
	
	// Check each symbol
	for _, symbol := range dis.config.Symbols {
		select {
		case <-dis.ctx.Done():
			return
		default:
		}
		
		// Detect gaps for this symbol
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
		
		// Fix gaps for this symbol
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
			
			// Rate limiting to avoid overwhelming Binance API
			time.Sleep(200 * time.Millisecond)
		}
		
		// Pause between symbols
		time.Sleep(100 * time.Millisecond)
	}
	
	dis.logger.Infof("Historical data coverage check completed: fixed %d gaps", totalGapsFixed)
}

// detectAndFixGaps detects and fixes data gaps in the specified time range
func (dis *DataIntegrityService) detectAndFixGaps(startTime, endTime time.Time) int {
	// Get all gaps
	allGaps, err := dis.storage.GetDataGapsForAllSymbols()
	if err != nil {
		dis.logger.Errorf("Failed to get data gaps: %v", err)
		return 0
	}
	
	totalGapsFixed := 0
	totalGapsDetected := 0
	
	// Process gaps for each symbol
	for symbol, gaps := range allGaps {
		select {
		case <-dis.ctx.Done():
			return totalGapsFixed
		default:
		}
		
		// Filter gaps within the specified time range
		relevantGaps := dis.filterGapsByTimeRange(gaps, startTime, endTime)
		if len(relevantGaps) == 0 {
			continue
		}
		
		totalGapsDetected += len(relevantGaps)
		dis.logger.Debugf("Symbol %s: found %d gaps in time range", symbol, len(relevantGaps))
		
		// Fix each gap
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
			
			// Rate limiting
			time.Sleep(150 * time.Millisecond)
		}
	}
	
	dis.stats.mutex.Lock()
	dis.stats.GapsDetected += int64(totalGapsDetected)
	dis.stats.mutex.Unlock()
	
	return totalGapsFixed
}

// ensureDataRetention ensures 7-day data retention
func (dis *DataIntegrityService) ensureDataRetention() {
	// Check if we have data older than 7 days that needs to be maintained
	// and ensure recent data is complete
	endTime := time.Now().Truncate(time.Minute)
	startTime := endTime.Add(-7 * 24 * time.Hour)
	
	// Quick check for any major gaps in the 7-day window
	gapsFixed := dis.detectAndFixGaps(startTime, endTime)
	if gapsFixed > 0 {
		dis.logger.Infof("Data retention check: fixed %d gaps in 7-day window", gapsFixed)
	}
}

// filterGapsByTimeRange filters gaps that fall within the specified time range
func (dis *DataIntegrityService) filterGapsByTimeRange(gaps []*storage.DataGap, startTime, endTime time.Time) []*storage.DataGap {
	var filtered []*storage.DataGap
	for _, gap := range gaps {
		// Check if gap overlaps with the time range
		if gap.EndTime.After(startTime) && gap.StartTime.Before(endTime) {
			filtered = append(filtered, gap)
		}
	}
	return filtered
}

// calculateTotalMissingMinutes calculates total missing minutes from gaps
func (dis *DataIntegrityService) calculateTotalMissingMinutes(gaps []*storage.DataGap) int {
	total := 0
	for _, gap := range gaps {
		total += gap.Missing
	}
	return total
}

// updateIntegrityStats updates integrity statistics
func (dis *DataIntegrityService) updateIntegrityStats() {
	// Calculate data coverage and continuity metrics
	// This is a simplified implementation
	dis.stats.mutex.Lock()
	defer dis.stats.mutex.Unlock()
	
	// Get basic stats from storage
	stats, err := dis.storage.GetStats()
	if err != nil {
		dis.logger.Errorf("Failed to get storage stats: %v", err)
		return
	}
	
	// Update timestamps if available
	if latestTime, ok := stats["latest_record_time"].(time.Time); ok {
		dis.stats.NewestDataTime = latestTime
	}
	
	// Calculate approximate data coverage (simplified)
	// In a real implementation, this would be more sophisticated
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
	
	// Estimate continuous days (simplified)
	dis.stats.ContinuousDays = 7 // Assume 7 days for now, would need more complex calculation
}

// GetStats returns current integrity statistics
func (dis *DataIntegrityService) GetStats() *IntegrityStats {
	dis.stats.mutex.RLock()
	defer dis.stats.mutex.RUnlock()
	
	// Return a copy to avoid race conditions
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

// IsRunning returns whether the service is currently running
func (dis *DataIntegrityService) IsRunning() bool {
	dis.mutex.RLock()
	defer dis.mutex.RUnlock()
	return dis.isRunning
}

// ForceIntegrityCheck forces an immediate integrity check
func (dis *DataIntegrityService) ForceIntegrityCheck() {
	dis.logger.Info("Force integrity check triggered")
	go dis.runPeriodicCheck()
}

// BackfillSymbolRange backfills data for a specific symbol and time range
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