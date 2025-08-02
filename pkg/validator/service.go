package validator

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

// ValidatorService 提供全面的数据验证和回补功能
type ValidatorService struct {
	config           *config.Config
	validationConfig *ValidationConfig
	storage          *storage.ClickHouseStorage
	backfill         *backfill.BackfillService
	logger           *logrus.Logger
	ctx              context.Context
	cancel           context.CancelFunc
	mutex            sync.RWMutex
	lastCheck        time.Time
	isRunning        bool
	stats            *ValidatorStats
	checkTicker      *time.Ticker
}

// ValidatorStats 跟踪验证服务统计信息
type ValidatorStats struct {
	LastCheckTime        time.Time `json:"last_check_time"`
	TotalChecks          int64     `json:"total_checks"`
	GapsDetected         int64     `json:"gaps_detected"`
	GapsFixed            int64     `json:"gaps_fixed"`
	BackfillErrors       int64     `json:"backfill_errors"`
	DataCoverage         float64   `json:"data_coverage_pct"`
	OldestDataTime       time.Time `json:"oldest_data_time"`
	NewestDataTime       time.Time `json:"newest_data_time"`
	ContinuousDays       int       `json:"continuous_days"`
	SymbolsChecked       int       `json:"symbols_checked"`
	TotalMissingMinutes  int       `json:"total_missing_minutes"`
	LastBackfillDuration string    `json:"last_backfill_duration"`
	mutex                sync.RWMutex
}

// ValidationConfig 保存验证器服务的配置
type ValidationConfig struct {
	CheckInterval     time.Duration
	MaxGapDuration    time.Duration
	HistoryDays       int
	BatchSize         int
	ConcurrentWorkers int
	Enabled           bool
}

// GapInfo represents a detected data gap
type GapInfo struct {
	Symbol    string    `json:"symbol"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	Duration  string    `json:"duration"`
	Minutes   int       `json:"minutes"`
	Fixed     bool      `json:"fixed"`
	Error     string    `json:"error,omitempty"`
}

// ValidationResult represents the result of a validation check
type ValidationResult struct {
	Timestamp     time.Time  `json:"timestamp"`
	SymbolsTotal  int        `json:"symbols_total"`
	SymbolsValid  int        `json:"symbols_valid"`
	GapsFound     []*GapInfo `json:"gaps_found"`
	GapsFixed     int        `json:"gaps_fixed"`
	Duration      string     `json:"duration"`
	Success       bool       `json:"success"`
	ErrorMessage  string     `json:"error_message,omitempty"`
}

// NewValidatorService creates a new validator service
func NewValidatorService(cfg *config.Config, storage *storage.ClickHouseStorage, backfill *backfill.BackfillService, logger *logrus.Logger) *ValidatorService {
	ctx, cancel := context.WithCancel(context.Background())
	
	// Default validation config if not specified
	validationConfig := &ValidationConfig{
		CheckInterval:     5 * time.Minute,
		MaxGapDuration:    24 * time.Hour,
		HistoryDays:       7,
		BatchSize:         100,
		ConcurrentWorkers: 3,
		Enabled:           true,
	}
	
	// Override with config if available
	if cfg.Validator.Enabled {
		if checkInterval, err := time.ParseDuration(cfg.Validator.CheckInterval); err == nil {
			validationConfig.CheckInterval = checkInterval
		}
		if maxGapDuration, err := time.ParseDuration(cfg.Validator.MaxGapDuration); err == nil {
			validationConfig.MaxGapDuration = maxGapDuration
		}
		validationConfig.HistoryDays = cfg.Validator.HistoryDays
		validationConfig.BatchSize = cfg.Validator.BatchSize
		validationConfig.ConcurrentWorkers = cfg.Validator.ConcurrentWorkers
		validationConfig.Enabled = cfg.Validator.Enabled
	}
	
	return &ValidatorService{
		config:           cfg,
		validationConfig: validationConfig,
		storage:          storage,
		backfill:         backfill,
		logger:           logger,
		ctx:              ctx,
		cancel:           cancel,
		stats: &ValidatorStats{
			LastCheckTime: time.Now(),
		},
	}
}

// Start begins the validator service
func (vs *ValidatorService) Start() {
	vs.mutex.Lock()
	defer vs.mutex.Unlock()
	
	if vs.isRunning {
		vs.logger.Warn("Validator service is already running")
		return
	}
	
	vs.isRunning = true
	vs.logger.Info("Starting validator service")
	
	// Start periodic validation checks
	go vs.runPeriodicValidation()
	
	// Run initial validation
	go vs.runInitialValidation()
}

// Stop stops the validator service
func (vs *ValidatorService) Stop() {
	vs.mutex.Lock()
	defer vs.mutex.Unlock()
	
	if !vs.isRunning {
		return
	}
	
	vs.logger.Info("Stopping validator service")
	vs.isRunning = false
	
	if vs.checkTicker != nil {
		vs.checkTicker.Stop()
	}
	
	vs.cancel()
}

// runInitialValidation performs an initial validation check on startup
func (vs *ValidatorService) runInitialValidation() {
	// Skip initial validation if storage is not available (e.g., in tests)
	if vs.storage == nil {
		vs.logger.Debug("Skipping initial validation - no storage available")
		return
	}
	
	vs.logger.Info("Running initial data validation")
	
	// Check recent data (last 24 hours)
	endTime := time.Now()
	startTime := endTime.Add(-24 * time.Hour)
	
	result := vs.ValidateDataRange(startTime, endTime)
	if result.Success {
		vs.logger.Infof("Initial validation completed: %d gaps found, %d fixed", len(result.GapsFound), result.GapsFixed)
	} else {
		vs.logger.Errorf("Initial validation failed: %s", result.ErrorMessage)
	}
}

// runPeriodicValidation runs validation checks at regular intervals
func (vs *ValidatorService) runPeriodicValidation() {
	checkInterval := vs.validationConfig.CheckInterval
	
	vs.checkTicker = time.NewTicker(checkInterval)
	defer vs.checkTicker.Stop()
	
	for {
		select {
		case <-vs.ctx.Done():
			return
		case <-vs.checkTicker.C:
			vs.performPeriodicCheck()
		}
	}
}

// performPeriodicCheck performs a periodic validation check
func (vs *ValidatorService) performPeriodicCheck() {
	// Skip periodic check if storage is not available (e.g., in tests)
	if vs.storage == nil {
		vs.logger.Debug("Skipping periodic check - no storage available")
		return
	}
	
	vs.logger.Debug("Performing periodic data validation")
	
	// Check recent data (last 2 hours)
	endTime := time.Now()
	startTime := endTime.Add(-2 * time.Hour)
	
	result := vs.ValidateDataRange(startTime, endTime)
	
	vs.updateStats(result)
	
	if len(result.GapsFound) > 0 {
		vs.logger.Infof("Periodic validation found %d gaps, fixed %d", len(result.GapsFound), result.GapsFixed)
	}
}

// ValidateDataRange validates data continuity for a specific time range
func (vs *ValidatorService) ValidateDataRange(startTime, endTime time.Time) *ValidationResult {
	startValidation := time.Now()
	
	result := &ValidationResult{
		Timestamp:    startValidation,
		GapsFound:    []*GapInfo{},
		Success:      true,
	}
	
	// Get all symbols
	symbols, err := vs.storage.GetAllSymbols()
	if err != nil {
		result.Success = false
		result.ErrorMessage = fmt.Sprintf("Failed to get symbols: %v", err)
		return result
	}
	
	result.SymbolsTotal = len(symbols)
	
	// Check each symbol for gaps
	for _, symbol := range symbols {
		gaps, err := vs.storage.DetectDataGaps(symbol, startTime, endTime)
		if err != nil {
			vs.logger.Errorf("Failed to detect gaps for %s: %v", symbol, err)
			continue
		}
		
		if len(gaps) == 0 {
			result.SymbolsValid++
			continue
		}
		
		// Process gaps for this symbol
		for _, gap := range gaps {
			gapInfo := &GapInfo{
				Symbol:    gap.Symbol,
				StartTime: gap.StartTime,
				EndTime:   gap.EndTime,
				Duration:  gap.EndTime.Sub(gap.StartTime).String(),
				Minutes:   int(gap.EndTime.Sub(gap.StartTime).Minutes()),
			}
			
			// Attempt to fix the gap
			if vs.shouldFixGap(gap) {
				if err := vs.fixGap(gap); err != nil {
					gapInfo.Error = err.Error()
					vs.logger.Errorf("Failed to fix gap for %s [%v - %v]: %v", 
						gap.Symbol, gap.StartTime, gap.EndTime, err)
				} else {
					gapInfo.Fixed = true
					result.GapsFixed++
					vs.logger.Infof("Fixed gap for %s [%v - %v]", 
						gap.Symbol, gap.StartTime, gap.EndTime)
				}
			}
			
			result.GapsFound = append(result.GapsFound, gapInfo)
		}
	}
	
	result.Duration = time.Since(startValidation).String()
	return result
}

// shouldFixGap determines if a gap should be automatically fixed
func (vs *ValidatorService) shouldFixGap(gap *storage.DataGap) bool {
	// Don't fix gaps that are too large (might be intentional downtime)
	maxGapDuration := vs.validationConfig.MaxGapDuration
	
	gapDuration := gap.EndTime.Sub(gap.StartTime)
	if gapDuration > maxGapDuration {
		vs.logger.Warnf("Gap too large to auto-fix: %s [%v]", gap.Symbol, gapDuration)
		return false
	}
	
	// Don't fix very recent gaps (data might still be coming)
	if time.Since(gap.EndTime) < 5*time.Minute {
		return false
	}
	
	return true
}

// fixGap attempts to fix a data gap by backfilling
func (vs *ValidatorService) fixGap(gap *storage.DataGap) error {
	result, err := vs.backfill.BackfillGap(gap)
	if err != nil {
		return fmt.Errorf("backfill failed: %w", err)
	}
	
	if !result.Success {
		return fmt.Errorf("backfill unsuccessful: %s", result.ErrorMessage)
	}
	
	vs.logger.Debugf("Backfilled %d records for %s [%v - %v]", 
		result.Inserted, gap.Symbol, gap.StartTime, gap.EndTime)
	
	return nil
}

// updateStats updates the validator statistics
func (vs *ValidatorService) updateStats(result *ValidationResult) {
	vs.stats.mutex.Lock()
	defer vs.stats.mutex.Unlock()
	
	vs.stats.LastCheckTime = result.Timestamp
	vs.stats.TotalChecks++
	vs.stats.GapsDetected += int64(len(result.GapsFound))
	vs.stats.GapsFixed += int64(result.GapsFixed)
	vs.stats.SymbolsChecked = result.SymbolsTotal
	vs.stats.LastBackfillDuration = result.Duration
	
	// Calculate total missing minutes
	totalMissing := 0
	for _, gap := range result.GapsFound {
		if !gap.Fixed {
			totalMissing += gap.Minutes
		}
	}
	vs.stats.TotalMissingMinutes = totalMissing
	
	// Calculate data coverage
	if result.SymbolsTotal > 0 {
		vs.stats.DataCoverage = float64(result.SymbolsValid) / float64(result.SymbolsTotal) * 100
	}
}

// GetStats returns current validator statistics
func (vs *ValidatorService) GetStats() *ValidatorStats {
	vs.stats.mutex.RLock()
	defer vs.stats.mutex.RUnlock()
	
	// Create a copy to avoid race conditions
	stats := *vs.stats
	return &stats
}

// IsRunning returns whether the validator service is running
func (vs *ValidatorService) IsRunning() bool {
	vs.mutex.RLock()
	defer vs.mutex.RUnlock()
	return vs.isRunning
}

// ForceValidation triggers an immediate validation check
func (vs *ValidatorService) ForceValidation() *ValidationResult {
	vs.logger.Info("Force validation triggered")
	
	// Validate last 24 hours
	endTime := time.Now()
	startTime := endTime.Add(-24 * time.Hour)
	
	result := vs.ValidateDataRange(startTime, endTime)
	vs.updateStats(result)
	
	return result
}

// ValidateSymbol validates data continuity for a specific symbol
func (vs *ValidatorService) ValidateSymbol(symbol string, startTime, endTime time.Time) *ValidationResult {
	startValidation := time.Now()
	
	result := &ValidationResult{
		Timestamp:    startValidation,
		SymbolsTotal: 1,
		GapsFound:    []*GapInfo{},
		Success:      true,
	}
	
	// Detect gaps for the symbol
	gaps, err := vs.storage.DetectDataGaps(symbol, startTime, endTime)
	if err != nil {
		result.Success = false
		result.ErrorMessage = fmt.Sprintf("Failed to detect gaps: %v", err)
		return result
	}
	
	if len(gaps) == 0 {
		result.SymbolsValid = 1
		result.Duration = time.Since(startValidation).String()
		return result
	}
	
	// Process gaps
	for _, gap := range gaps {
		gapInfo := &GapInfo{
			Symbol:    gap.Symbol,
			StartTime: gap.StartTime,
			EndTime:   gap.EndTime,
			Duration:  gap.EndTime.Sub(gap.StartTime).String(),
			Minutes:   int(gap.EndTime.Sub(gap.StartTime).Minutes()),
		}
		
		// Attempt to fix the gap
		if vs.shouldFixGap(gap) {
			if err := vs.fixGap(gap); err != nil {
				gapInfo.Error = err.Error()
			} else {
				gapInfo.Fixed = true
				result.GapsFixed++
			}
		}
		
		result.GapsFound = append(result.GapsFound, gapInfo)
	}
	
	result.Duration = time.Since(startValidation).String()
	return result
}

// GetValidationHistory returns recent validation results
func (vs *ValidatorService) GetValidationHistory(limit int) ([]*ValidationResult, error) {
	// This would typically be stored in a database or cache
	// For now, return empty slice as this is a basic implementation
	return []*ValidationResult{}, nil
}