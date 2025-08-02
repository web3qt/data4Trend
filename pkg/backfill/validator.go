package backfill

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"data4trend/pkg/config"
	"data4trend/pkg/storage"
)

// ValidatorStats 代表验证器统计信息
type ValidatorStats struct {
	mu sync.RWMutex

	LastCheckTime        time.Time `json:"last_check_time"`
	TotalChecks          int       `json:"total_checks"`
	GapsDetected         int       `json:"gaps_detected"`
	GapsFixed            int       `json:"gaps_fixed"`
	BackfillErrors       int       `json:"backfill_errors"`
	DataCoveragePercent  float64   `json:"data_coverage_pct"`
	SymbolsChecked       int       `json:"symbols_checked"`
	TotalMissingMinutes  int       `json:"total_missing_minutes"`
	ContinuousDays       int       `json:"continuous_days"`
	OldestDataTime       time.Time `json:"oldest_data_time"`
	NewestDataTime       time.Time `json:"newest_data_time"`
	LastBackfillDuration string    `json:"last_backfill_duration"`
}

// GapInfo 代表数据缺口信息
type GapInfo struct {
	Symbol      string    `json:"symbol"`
	StartTime   time.Time `json:"start_time"`
	EndTime     time.Time `json:"end_time"`
	Missing     int       `json:"missing_count"`
	Duration    string    `json:"duration"`
	WillAutoFix bool      `json:"will_auto_fix"`
}

// BackfillValidatorService 合并的Backfill和Validator服务
type BackfillValidatorService struct {
	config      *config.Config
	storage     *storage.ClickHouseStorage
	backfill    *BackfillService
	logger      *logrus.Logger
	stats       *ValidatorStats
	ctx         context.Context
	cancel      context.CancelFunc
	isRunning   bool
	mu          sync.RWMutex
}

// NewBackfillValidatorService 创建新的合并服务
func NewBackfillValidatorService(cfg *config.Config, storage *storage.ClickHouseStorage, logger *logrus.Logger) *BackfillValidatorService {
	ctx, cancel := context.WithCancel(context.Background())
	
	backfillService := NewBackfillService(cfg, storage, logger)
	
	return &BackfillValidatorService{
		config:    cfg,
		storage:   storage,
		backfill:  backfillService,
		logger:    logger,
		stats:     &ValidatorStats{},
		ctx:       ctx,
		cancel:    cancel,
		isRunning: false,
	}
}

// Start 启动验证和自动回填服务
func (bvs *BackfillValidatorService) Start() error {
	bvs.mu.Lock()
	defer bvs.mu.Unlock()

	if bvs.isRunning {
		return fmt.Errorf("service is already running")
	}

	bvs.isRunning = true
	bvs.logger.Info("🚀 Starting BackfillValidator service...")

	// 启动周期性验证
	go bvs.runPeriodicValidation()

	return nil
}

// Stop 停止服务
func (bvs *BackfillValidatorService) Stop() {
	bvs.mu.Lock()
	defer bvs.mu.Unlock()

	if !bvs.isRunning {
		return
	}

	bvs.logger.Info("🛑 Stopping BackfillValidator service...")
	bvs.isRunning = false
	bvs.cancel()
}

// IsRunning 检查服务是否正在运行
func (bvs *BackfillValidatorService) IsRunning() bool {
	bvs.mu.RLock()
	defer bvs.mu.RUnlock()
	return bvs.isRunning
}

// runPeriodicValidation 运行周期性验证
func (bvs *BackfillValidatorService) runPeriodicValidation() {
	checkInterval, err := time.ParseDuration(bvs.config.Validator.CheckInterval)
	if err != nil {
		bvs.logger.Errorf("Invalid check interval: %v", err)
		checkInterval = 5 * time.Minute
	}

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	// 立即执行一次验证
	bvs.runValidation()

	for {
		select {
		case <-bvs.ctx.Done():
			return
		case <-ticker.C:
			bvs.runValidation()
		}
	}
}

// runValidation 执行验证检查
func (bvs *BackfillValidatorService) runValidation() {
	bvs.stats.mu.Lock()
	bvs.stats.LastCheckTime = time.Now()
	bvs.stats.TotalChecks++
	bvs.stats.mu.Unlock()

	bvs.logger.Info("🔍 [VALIDATOR] Starting data validation check...")
	startTime := time.Now()

	// 1. 检测数据缺口
	gaps, err := bvs.detectDataGaps()
	if err != nil {
		bvs.logger.Errorf("❌ [VALIDATOR] Failed to detect gaps: %v", err)
		return
	}

	bvs.stats.mu.Lock()
	bvs.stats.GapsDetected = len(gaps)
	bvs.stats.mu.Unlock()

	if len(gaps) == 0 {
		bvs.logger.Info("✅ [VALIDATOR] No data gaps detected")
		return
	}

	bvs.logger.Infof("📊 [VALIDATOR] Found %d gaps, starting auto-fix...", len(gaps))

	// 2. 自动修复小缺口
	fixedCount := 0
	for _, gap := range gaps {
		if bvs.shouldAutoFix(gap) {
			bvs.logger.Infof("🔧 [VALIDATOR] Auto-fixing gap for %s: %s to %s (%d missing)",
				gap.Symbol, gap.StartTime.Format("2006-01-02 15:04:05"),
				gap.EndTime.Format("2006-01-02 15:04:05"), gap.Missing)

			result, err := bvs.backfill.BackfillSymbolRange(gap.Symbol, gap.StartTime, gap.EndTime)
			if err != nil {
				bvs.logger.Errorf("❌ [VALIDATOR] Failed to auto-fix gap for %s: %v", gap.Symbol, err)
				bvs.stats.mu.Lock()
				bvs.stats.BackfillErrors++
				bvs.stats.mu.Unlock()
				continue
			}

			if result.Success {
				fixedCount++
				bvs.stats.mu.Lock()
				bvs.stats.GapsFixed++
				bvs.stats.mu.Unlock()
				bvs.logger.Infof("✅ [VALIDATOR] Successfully auto-fixed gap for %s: %d records", gap.Symbol, result.Inserted)
			}

			// 限制速率
			time.Sleep(150 * time.Millisecond)
		} else {
			bvs.logger.Infof("⚠️ [VALIDATOR] Gap for %s too large for auto-fix (%s), manual intervention required",
				gap.Symbol, gap.Duration)
		}
	}

	duration := time.Since(startTime)
	bvs.stats.mu.Lock()
	bvs.stats.LastBackfillDuration = duration.String()
	bvs.stats.mu.Unlock()

	bvs.logger.Infof("🎉 [VALIDATOR] Validation completed in %v: %d gaps detected, %d gaps fixed",
		duration, len(gaps), fixedCount)
}

// detectDataGaps 检测数据缺口
func (bvs *BackfillValidatorService) detectDataGaps() ([]*GapInfo, error) {
	// 获取所有交易对
	symbols, err := bvs.storage.GetAllSymbols()
	if err != nil {
		return nil, fmt.Errorf("failed to get symbols: %w", err)
	}

	var allGaps []*GapInfo
	maxGapDuration, _ := time.ParseDuration(bvs.config.Validator.MaxGapDuration)

	// 检查每个交易对的数据缺口
	for _, symbol := range symbols {
		gaps, err := bvs.storage.DetectDataGaps(symbol, time.Now().Add(-24*time.Hour), time.Now())
		if err != nil {
			bvs.logger.Warnf("Failed to detect gaps for %s: %v", symbol, err)
			continue
		}

		for _, gap := range gaps {
			gapDuration := gap.EndTime.Sub(gap.StartTime)
			if gapDuration <= maxGapDuration {
				allGaps = append(allGaps, &GapInfo{
					Symbol:      gap.Symbol,
					StartTime:   gap.StartTime,
					EndTime:     gap.EndTime,
					Missing:     gap.Missing,
					Duration:    gapDuration.String(),
					WillAutoFix: bvs.shouldAutoFix(&GapInfo{Duration: gapDuration.String()}),
				})
			}
		}
	}

	return allGaps, nil
}

// shouldAutoFix 判断是否应该自动修复
func (bvs *BackfillValidatorService) shouldAutoFix(gap *GapInfo) bool {
	if !bvs.config.Validator.AutoBackfill {
		return false
	}

	threshold, err := time.ParseDuration(bvs.config.Validator.BackfillThreshold)
	if err != nil {
		threshold = 1 * time.Hour
	}

	gapDuration, err := time.ParseDuration(gap.Duration)
	if err != nil {
		return false
	}

	return gapDuration <= threshold
}

// ForceValidation 强制执行一次验证
func (bvs *BackfillValidatorService) ForceValidation() error {
	if !bvs.IsRunning() {
		return fmt.Errorf("service is not running")
	}

	bvs.logger.Info("🔧 [VALIDATOR] Force validation triggered")
	bvs.runValidation()
	return nil
}

// GetStats 获取统计信息
func (bvs *BackfillValidatorService) GetStats() interface{} {
	bvs.stats.mu.RLock()
	defer bvs.stats.mu.RUnlock()
	
	// 创建副本避免并发问题
	stats := *bvs.stats
	return &stats
}

// GetBackfillService 获取backfill服务实例
func (bvs *BackfillValidatorService) GetBackfillService() *BackfillService {
	return bvs.backfill
}

// ValidateDataRange 验证指定时间范围的数据
func (bvs *BackfillValidatorService) ValidateDataRange(startTime, endTime time.Time) ([]*GapInfo, error) {
	bvs.logger.Infof("🔍 [VALIDATOR] Validating data range: %s to %s",
		startTime.Format("2006-01-02 15:04:05"),
		endTime.Format("2006-01-02 15:04:05"))

	symbols, err := bvs.storage.GetAllSymbols()
	if err != nil {
		return nil, fmt.Errorf("failed to get symbols: %w", err)
	}

	var gaps []*GapInfo
	for _, symbol := range symbols {
		symbolGaps, err := bvs.storage.DetectDataGaps(symbol, startTime, endTime)
		if err != nil {
			bvs.logger.Warnf("Failed to detect gaps for %s: %v", symbol, err)
			continue
		}

		for _, gap := range symbolGaps {
			gapDuration := gap.EndTime.Sub(gap.StartTime)
			gaps = append(gaps, &GapInfo{
				Symbol:      gap.Symbol,
				StartTime:   gap.StartTime,
				EndTime:     gap.EndTime,
				Missing:     gap.Missing,
				Duration:    gapDuration.String(),
				WillAutoFix: bvs.shouldAutoFix(&GapInfo{Duration: gapDuration.String()}),
			})
		}
	}

	return gaps, nil
}

// ValidateSymbol 验证指定交易对的数据
func (bvs *BackfillValidatorService) ValidateSymbol(symbol string, startTime, endTime time.Time) ([]*GapInfo, error) {
	bvs.logger.Infof("🔍 [VALIDATOR] Validating symbol %s: %s to %s",
		symbol, startTime.Format("2006-01-02 15:04:05"),
		endTime.Format("2006-01-02 15:04:05"))

	gaps, err := bvs.storage.DetectDataGaps(symbol, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to detect gaps for %s: %w", symbol, err)
	}

	var gapInfos []*GapInfo
	for _, gap := range gaps {
		gapDuration := gap.EndTime.Sub(gap.StartTime)
		gapInfos = append(gapInfos, &GapInfo{
			Symbol:      gap.Symbol,
			StartTime:   gap.StartTime,
			EndTime:     gap.EndTime,
			Missing:     gap.Missing,
			Duration:    gapDuration.String(),
			WillAutoFix: bvs.shouldAutoFix(&GapInfo{Duration: gapDuration.String()}),
		})
	}

	return gapInfos, nil
} 