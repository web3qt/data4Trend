package validation

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"data4trend/pkg/config"
	"data4trend/pkg/storage"
)

// DataValidator 代表数据验证组件
type DataValidator struct {
	storage   *storage.ClickHouseStorage
	config    *config.Config
	logger    *logrus.Logger
	mutex     sync.RWMutex
	lastCheck time.Time
	ctx       context.Context
	cancel    context.CancelFunc
}

// ValidationResult 代表数据验证结果
type ValidationResult struct {
	Timestamp        time.Time                    `json:"timestamp"`
	OverallStatus    string                       `json:"overall_status"`
	TotalSymbols     int                          `json:"total_symbols"`
	HealthySymbols   int                          `json:"healthy_symbols"`
	Issues           []ValidationIssue            `json:"issues"`
	DataGaps         map[string][]*storage.DataGap `json:"data_gaps"`
	DuplicateRecords map[string]int               `json:"duplicate_records"`
	AnomalousData    []AnomalousDataPoint         `json:"anomalous_data"`
	DataQuality      DataQualityMetrics           `json:"data_quality"`
}

// ValidationIssue 代表特定的验证问题
type ValidationIssue struct {
	Symbol      string    `json:"symbol"`
	IssueType   string    `json:"issue_type"`
	Severity    string    `json:"severity"`
	Description string    `json:"description"`
	Timestamp   time.Time `json:"timestamp"`
	Count       int       `json:"count,omitempty"`
}

// AnomalousDataPoint 代表异常数据点
type AnomalousDataPoint struct {
	Symbol      string    `json:"symbol"`
	Timestamp   time.Time `json:"timestamp"`
	Field       string    `json:"field"`
	Value       float64   `json:"value"`
	Expected    float64   `json:"expected"`
	Deviation   float64   `json:"deviation"`
	Description string    `json:"description"`
}

// DataQualityMetrics 代表整体数据质量指标
type DataQualityMetrics struct {
	CompletenessScore float64 `json:"completeness_score"`
	AccuracyScore     float64 `json:"accuracy_score"`
	ConsistencyScore  float64 `json:"consistency_score"`
	TimelinessScore   float64 `json:"timeliness_score"`
	OverallScore      float64 `json:"overall_score"`
}

// NewDataValidator 创建新的数据验证器实例
func NewDataValidator(storage *storage.ClickHouseStorage, config *config.Config, logger *logrus.Logger) *DataValidator {
	ctx, cancel := context.WithCancel(context.Background())
	return &DataValidator{
		storage:   storage,
		config:    config,
		logger:    logger,
		lastCheck: time.Now(),
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Start 启动周期性数据验证
func (v *DataValidator) Start() {
	v.logger.Info("Starting data validation service...")
	
	// 运行初始验证
	go v.runValidation()
	
	// 启动周期性验证（每30分钟）
	go v.periodicValidation()
}

// Stop 停止数据验证服务
func (v *DataValidator) Stop() {
	v.logger.Info("Stopping data validation service...")
	v.cancel()
}

// periodicValidation 周期性运行验证检查
func (v *DataValidator) periodicValidation() {
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()
	
	for {
		select {
		case <-v.ctx.Done():
			return
		case <-ticker.C:
			v.runValidation()
		}
	}
}

// runValidation 执行全面的数据验证
func (v *DataValidator) runValidation() {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	
	v.logger.Info("Starting comprehensive data validation...")
	start := time.Now()
	
	result := &ValidationResult{
		Timestamp:        start,
		Issues:           []ValidationIssue{},
		DataGaps:         make(map[string][]*storage.DataGap),
		DuplicateRecords: make(map[string]int),
		AnomalousData:    []AnomalousDataPoint{},
	}
	
	// 1. 检查数据完整性（缺口）
	v.checkDataCompleteness(result)
	
	// 2. 检查重复记录
	v.checkDuplicateRecords(result)
	
	// 3. 检查异常数据
	v.checkAnomalousData(result)
	
	// 4. 检查数据时效性
	v.checkDataTimeliness(result)
	
	// 5. 计算数据质量指标
	v.calculateDataQuality(result)
	
	// 6. 确定整体状态
	v.determineOverallStatus(result)
	
	// 7. 存储验证结果
	v.storeValidationResults(result)
	
	v.lastCheck = time.Now()
	duration := time.Since(start)
	v.logger.Infof("Data validation completed in %v. Overall status: %s", duration, result.OverallStatus)
	
	// Log critical issues
	v.logCriticalIssues(result)
}

// checkDataCompleteness checks for missing data gaps
func (v *DataValidator) checkDataCompleteness(result *ValidationResult) {
	v.logger.Debug("Checking data completeness...")
	
	// Check gaps for all symbols in the last 24 hours
	gaps, err := v.storage.GetDataGapsForAllSymbols()
	if err != nil {
		v.logger.Errorf("Failed to check data gaps: %v", err)
		result.Issues = append(result.Issues, ValidationIssue{
			Symbol:      "system",
			IssueType:   "data_gap_check_failed",
			Severity:    "critical",
			Description: fmt.Sprintf("Failed to check data gaps: %v", err),
			Timestamp:   time.Now(),
		})
		return
	}
	
	result.DataGaps = gaps
	
	// Analyze gaps and create issues
	for symbol, symbolGaps := range gaps {
		totalMissing := 0
		for _, gap := range symbolGaps {
			totalMissing += gap.Missing
		}
		
		if totalMissing > 0 {
			severity := "info"
			if totalMissing > 60 { // More than 1 hour missing
				severity = "warning"
			}
			if totalMissing > 180 { // More than 3 hours missing
				severity = "critical"
			}
			
			result.Issues = append(result.Issues, ValidationIssue{
				Symbol:      symbol,
				IssueType:   "data_gaps",
				Severity:    severity,
				Description: fmt.Sprintf("Missing %d minutes of data in %d gaps", totalMissing, len(symbolGaps)),
				Timestamp:   time.Now(),
				Count:       totalMissing,
			})
		}
	}
}

// checkDuplicateRecords checks for duplicate records
func (v *DataValidator) checkDuplicateRecords(result *ValidationResult) {
	v.logger.Debug("Checking for duplicate records...")
	
	duplicates, err := v.storage.GetDuplicateRecords()
	if err != nil {
		v.logger.Errorf("Failed to check duplicates: %v", err)
		result.Issues = append(result.Issues, ValidationIssue{
			Symbol:      "system",
			IssueType:   "duplicate_check_failed",
			Severity:    "warning",
			Description: fmt.Sprintf("Failed to check duplicates: %v", err),
			Timestamp:   time.Now(),
		})
		return
	}
	
	result.DuplicateRecords = duplicates
	
	// Create issues for symbols with duplicates
	for symbol, count := range duplicates {
		severity := "info"
		if count > 10 {
			severity = "warning"
		}
		if count > 50 {
			severity = "critical"
		}
		
		result.Issues = append(result.Issues, ValidationIssue{
			Symbol:      symbol,
			IssueType:   "duplicate_records",
			Severity:    severity,
			Description: fmt.Sprintf("Found %d duplicate records", count),
			Timestamp:   time.Now(),
			Count:       count,
		})
	}
}

// checkAnomalousData checks for anomalous data points
func (v *DataValidator) checkAnomalousData(result *ValidationResult) {
	v.logger.Debug("Checking for anomalous data...")
	
	anomalousData, err := v.storage.GetAnomalousData()
	if err != nil {
		v.logger.Errorf("Failed to check anomalous data: %v", err)
		result.Issues = append(result.Issues, ValidationIssue{
			Symbol:      "system",
			IssueType:   "anomaly_check_failed",
			Severity:    "warning",
			Description: fmt.Sprintf("Failed to check anomalous data: %v", err),
			Timestamp:   time.Now(),
		})
		return
	}
	
	// Convert to AnomalousDataPoint format
	for _, data := range anomalousData {
		anomalousPoint := AnomalousDataPoint{
			Symbol:      data["symbol"].(string),
			Timestamp:   data["timestamp"].(time.Time),
			Field:       "price_change",
			Value:       data["close_price"].(float64),
			Expected:    data["open_price"].(float64),
			Deviation:   data["price_change_pct"].(float64),
			Description: data["description"].(string),
		}
		result.AnomalousData = append(result.AnomalousData, anomalousPoint)
		
		// Create issue for extreme anomalies
		if math.Abs(anomalousPoint.Deviation) > 100 {
			result.Issues = append(result.Issues, ValidationIssue{
				Symbol:      anomalousPoint.Symbol,
				IssueType:   "extreme_anomaly",
				Severity:    "critical",
				Description: fmt.Sprintf("Extreme price movement: %.2f%%", anomalousPoint.Deviation),
				Timestamp:   time.Now(),
			})
		}
	}
}

// checkDataTimeliness checks if data is being received in a timely manner
func (v *DataValidator) checkDataTimeliness(result *ValidationResult) {
	v.logger.Debug("Checking data timeliness...")
	
	staleData, err := v.storage.GetStaleDataSymbols()
	if err != nil {
		v.logger.Errorf("Failed to check data timeliness: %v", err)
		result.Issues = append(result.Issues, ValidationIssue{
			Symbol:      "system",
			IssueType:   "timeliness_check_failed",
			Severity:    "warning",
			Description: fmt.Sprintf("Failed to check data timeliness: %v", err),
			Timestamp:   time.Now(),
		})
		return
	}
	
	// Create issues for stale data
	for symbol, delay := range staleData {
		severity := "info"
		if delay > 10*time.Minute {
			severity = "warning"
		}
		if delay > 30*time.Minute {
			severity = "critical"
		}
		
		result.Issues = append(result.Issues, ValidationIssue{
			Symbol:      symbol,
			IssueType:   "stale_data",
			Severity:    severity,
			Description: fmt.Sprintf("Data is %v old", delay.Truncate(time.Second)),
			Timestamp:   time.Now(),
		})
	}
}

// calculateDataQuality calculates overall data quality metrics
func (v *DataValidator) calculateDataQuality(result *ValidationResult) {
	v.logger.Debug("Calculating data quality metrics...")
	
	// Calculate completeness score based on data gaps
	completenessScore := v.calculateCompletenessScore(result.DataGaps)
	
	// Calculate accuracy score based on anomalous data
	accuracyScore := v.calculateAccuracyScore(result.AnomalousData)
	
	// Calculate consistency score based on duplicates
	consistencyScore := v.calculateConsistencyScore(result.DuplicateRecords)
	
	// Calculate timeliness score
	timelinessScore := 95.0 // Placeholder
	
	// Calculate overall score
	overallScore := (completenessScore + accuracyScore + consistencyScore + timelinessScore) / 4
	
	result.DataQuality = DataQualityMetrics{
		CompletenessScore: completenessScore,
		AccuracyScore:     accuracyScore,
		ConsistencyScore:  consistencyScore,
		TimelinessScore:   timelinessScore,
		OverallScore:      overallScore,
	}
}

// calculateCompletenessScore calculates completeness score based on data gaps
func (v *DataValidator) calculateCompletenessScore(gaps map[string][]*storage.DataGap) float64 {
	if len(gaps) == 0 {
		return 100.0
	}
	
	totalMissing := 0
	totalSymbols := len(v.config.Symbols)
	expectedMinutes := 24 * 60 // 24 hours * 60 minutes
	
	for _, symbolGaps := range gaps {
		for _, gap := range symbolGaps {
			totalMissing += gap.Missing
		}
	}
	
	totalExpected := totalSymbols * expectedMinutes
	if totalExpected == 0 {
		return 100.0
	}
	
	completenessRatio := float64(totalExpected-totalMissing) / float64(totalExpected)
	return math.Max(0, completenessRatio*100)
}

// calculateAccuracyScore calculates accuracy score based on anomalous data
func (v *DataValidator) calculateAccuracyScore(anomalies []AnomalousDataPoint) float64 {
	// Placeholder implementation
	if len(anomalies) == 0 {
		return 100.0
	}
	return math.Max(0, 100.0-float64(len(anomalies))*2)
}

// calculateConsistencyScore calculates consistency score based on duplicates
func (v *DataValidator) calculateConsistencyScore(duplicates map[string]int) float64 {
	// Placeholder implementation
	if len(duplicates) == 0 {
		return 100.0
	}
	
	totalDuplicates := 0
	for _, count := range duplicates {
		totalDuplicates += count
	}
	
	return math.Max(0, 100.0-float64(totalDuplicates)*0.1)
}

// determineOverallStatus determines the overall validation status
func (v *DataValidator) determineOverallStatus(result *ValidationResult) {
	criticalIssues := 0
	warningIssues := 0
	
	for _, issue := range result.Issues {
		switch issue.Severity {
		case "critical":
			criticalIssues++
		case "warning":
			warningIssues++
		}
	}
	
	if criticalIssues > 0 {
		result.OverallStatus = "critical"
	} else if warningIssues > 0 || result.DataQuality.OverallScore < 80 {
		result.OverallStatus = "warning"
	} else {
		result.OverallStatus = "healthy"
	}
	
	result.TotalSymbols = len(v.config.Symbols)
	result.HealthySymbols = result.TotalSymbols - len(result.DataGaps) - len(result.DuplicateRecords)
}

// storeValidationResults stores validation results to database
func (v *DataValidator) storeValidationResults(result *ValidationResult) {
	v.logger.Debug("Storing validation results...")
	
	err := v.storage.StoreValidationResult(
		result.Timestamp,
		result.OverallStatus,
		result.TotalSymbols,
		result.HealthySymbols,
		result.DataQuality.CompletenessScore,
		result.DataQuality.AccuracyScore,
		result.DataQuality.ConsistencyScore,
		result.DataQuality.TimelinessScore,
		result.DataQuality.OverallScore,
		len(result.Issues),
	)
	
	if err != nil {
		v.logger.Errorf("Failed to store validation results: %v", err)
	} else {
		v.logger.Debug("Validation results stored successfully")
	}
}

// logCriticalIssues logs critical validation issues
func (v *DataValidator) logCriticalIssues(result *ValidationResult) {
	criticalIssues := []ValidationIssue{}
	for _, issue := range result.Issues {
		if issue.Severity == "critical" {
			criticalIssues = append(criticalIssues, issue)
		}
	}
	
	if len(criticalIssues) > 0 {
		v.logger.Errorf("Found %d critical data validation issues:", len(criticalIssues))
		for _, issue := range criticalIssues {
			v.logger.Errorf("  - %s [%s]: %s", issue.Symbol, issue.IssueType, issue.Description)
		}
	}
}

// GetLastValidationResult returns the last validation result
func (v *DataValidator) GetLastValidationResult() *ValidationResult {
	v.mutex.RLock()
	defer v.mutex.RUnlock()
	
	// This would typically load from database
	// For now, return a placeholder
	return &ValidationResult{
		Timestamp:     v.lastCheck,
		OverallStatus: "unknown",
		Issues:        []ValidationIssue{},
	}
}

// RunManualValidation runs a manual validation check
func (v *DataValidator) RunManualValidation() *ValidationResult {
	v.logger.Info("Running manual data validation...")
	v.runValidation()
	return v.GetLastValidationResult()
}