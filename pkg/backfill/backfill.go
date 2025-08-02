package backfill

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"data4trend/internal/types"
	"data4trend/pkg/config"
	"data4trend/pkg/storage"
)

// BinanceKlineResponse 代表来自Binance K线API的响应
type BinanceKlineResponse [][]interface{}

// RateLimiter 速率限制器
type RateLimiter struct {
	mu           sync.Mutex
	lastRequest  time.Time
	requestCount int
	windowStart  time.Time
}

// BackfillProgress 代表回填进度
type BackfillProgress struct {
	mu sync.RWMutex

	IsRunning     bool                       `json:"is_running"`
	StartTime     time.Time                  `json:"start_time"`
	CurrentSymbol string                     `json:"current_symbol"`
	TotalSymbols  int                        `json:"total_symbols"`
	Processed     int                        `json:"processed"`
	SuccessCount  int                        `json:"success_count"`
	FailedCount   int                        `json:"failed_count"`
	Results       map[string]*BackfillResult `json:"results"`
	LastUpdate    time.Time                  `json:"last_update"`
}

// BackfillService 处理数据回补操作
type BackfillService struct {
	config      *config.Config
	storage     *storage.ClickHouseStorage
	logger      *logrus.Logger
	client      *http.Client
	progress    *BackfillProgress
	rateLimiter *RateLimiter
}

// NewBackfillService 创建新的回补服务
func NewBackfillService(cfg *config.Config, storage *storage.ClickHouseStorage, logger *logrus.Logger) *BackfillService {
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// 如果配置了代理则设置代理
	if cfg.Proxy.Enabled {
		proxyURL, err := url.Parse(cfg.GetProxyURL())
		if err == nil {
			client.Transport = &http.Transport{
				Proxy: http.ProxyURL(proxyURL),
			}
			logger.Infof("Backfill service using proxy: %s", cfg.GetProxyURL())
		}
	}

	return &BackfillService{
		config:      cfg,
		storage:     storage,
		logger:      logger,
		client:      client,
		progress:    &BackfillProgress{Results: make(map[string]*BackfillResult)},
		rateLimiter: &RateLimiter{},
	}
}

// BackfillResult 代表回补操作的结果
type BackfillResult struct {
	Symbol       string    `json:"symbol"`
	StartTime    time.Time `json:"start_time"`
	EndTime      time.Time `json:"end_time"`
	Requested    int       `json:"requested_count"`
	Fetched      int       `json:"fetched_count"`
	Inserted     int       `json:"inserted_count"`
	Duration     string    `json:"duration"`
	Success      bool      `json:"success"`
	ErrorMessage string    `json:"error_message,omitempty"`
}

// waitForRateLimit 等待速率限制
func (bs *BackfillService) waitForRateLimit() {
	bs.rateLimiter.mu.Lock()
	defer bs.rateLimiter.mu.Unlock()

	now := time.Now()
	
	// 检查是否需要重置窗口
	if now.Sub(bs.rateLimiter.windowStart) >= time.Minute {
		bs.rateLimiter.requestCount = 0
		bs.rateLimiter.windowStart = now
	}

	// 检查是否超过限制（每分钟最多1200个请求，但保守起见使用1000）
	if bs.rateLimiter.requestCount >= 1000 {
		// 等待到下一个窗口
		sleepTime := time.Minute - now.Sub(bs.rateLimiter.windowStart)
		if sleepTime > 0 {
			bs.logger.Debugf("Rate limit reached, waiting %v", sleepTime)
			time.Sleep(sleepTime)
			bs.rateLimiter.requestCount = 0
			bs.rateLimiter.windowStart = time.Now()
		}
	}

	// 确保请求间隔至少100ms
	if now.Sub(bs.rateLimiter.lastRequest) < 100*time.Millisecond {
		time.Sleep(100*time.Millisecond - now.Sub(bs.rateLimiter.lastRequest))
	}

	bs.rateLimiter.requestCount++
	bs.rateLimiter.lastRequest = time.Now()
}

// FetchHistoricalKlines 从币安获取历史K线数据
func (bs *BackfillService) FetchHistoricalKlines(symbol string, startTime, endTime time.Time) ([]*types.KlineData, error) {
	// 等待速率限制
	bs.waitForRateLimit()

	// 使用币安官方推荐的data-api.binance.vision端点
	apiURL := "https://data-api.binance.vision/api/v3/klines"

	// 将时间转换为毫秒
	startMs := startTime.UnixMilli()
	endMs := endTime.UnixMilli()

	// 构建请求URL，使用正确的参数
	reqURL := fmt.Sprintf("%s?symbol=%s&interval=1m&startTime=%d&endTime=%d&limit=1000",
		apiURL, symbol, startMs, endMs)

	bs.logger.Debugf("Fetching historical data: %s", reqURL)

	// 发起HTTP请求
	resp, err := bs.client.Get(reqURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch data from Binance: %w", err)
	}
	defer resp.Body.Close()

	// 检查响应状态
	if resp.StatusCode == http.StatusTooManyRequests {
		// 429错误，等待更长时间
		bs.logger.Warnf("Rate limit exceeded for %s, waiting 60 seconds", symbol)
		time.Sleep(60 * time.Second)
		return nil, fmt.Errorf("rate limit exceeded, please retry")
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Binance API error (status %d): %s", resp.StatusCode, string(body))
	}

	// 解析响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var binanceData BinanceKlineResponse
	if err := json.Unmarshal(body, &binanceData); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// 转换为内部格式
	klines := make([]*types.KlineData, 0, len(binanceData))
	for _, item := range binanceData {
		if len(item) < 12 {
			continue
		}

		// 解析每个字段
		openTime, _ := item[0].(float64)
		closeTime, _ := item[6].(float64)
		open, _ := item[1].(string)
		high, _ := item[2].(string)
		low, _ := item[3].(string)
		close, _ := item[4].(string)
		volume, _ := item[5].(string)

		kline := &types.KlineData{
			Symbol:    symbol,
			OpenTime:  int64(openTime),
			CloseTime: int64(closeTime),
			Open:      open,
			High:      high,
			Low:       low,
			Close:     close,
			Volume:    volume,
			CreatedAt: time.Now(),
		}

		klines = append(klines, kline)
	}

	bs.logger.Debugf("Fetched %d klines for %s", len(klines), symbol)
	return klines, nil
}

// BackfillSymbolComplete 为特定交易对回填完整的5天数据
func (bs *BackfillService) BackfillSymbolComplete(symbol string) (*BackfillResult, error) {
	startTime := time.Now()

	// 计算时间范围：从当前时间往前推5天
	endTime := time.Now().Truncate(time.Minute)                                 // 当前时间，精确到分钟
	startTimeRange := endTime.AddDate(0, 0, -bs.config.Backfill.DaysToBackfill) // 5天前

	result := &BackfillResult{
		Symbol:    symbol,
		StartTime: startTimeRange,
		EndTime:   endTime,
		Success:   false,
	}

	bs.logger.Infof("🚀 [BACKFILL] Starting complete backfill for %s: %s to %s",
		symbol, startTimeRange.Format("2006-01-02 15:04:05"),
		endTime.Format("2006-01-02 15:04:05"))

	// 计算总分钟数
	totalMinutes := int(endTime.Sub(startTimeRange).Minutes())
	result.Requested = totalMinutes

	bs.logger.Infof("📊 [BACKFILL] Need to fetch %d minutes of data for %s", totalMinutes, symbol)

	// 分批获取数据，每次最多1000条记录（约16.7小时的数据）
	allKlines := make([]*types.KlineData, 0, totalMinutes)
	currentStart := startTimeRange

	// 添加重试机制
	maxRetries := 3
	retryDelay := 5 * time.Second

	for currentStart.Before(endTime) {
		currentEnd := currentStart.Add(time.Duration(bs.config.Backfill.BatchSize) * time.Minute)
		if currentEnd.After(endTime) {
			currentEnd = endTime
		}

		bs.logger.Debugf("📡 [BACKFILL] Fetching %s: %s to %s",
			symbol, currentStart.Format("2006-01-02 15:04:05"),
			currentEnd.Format("2006-01-02 15:04:05"))

		// 从Binance获取历史数据，带重试机制
		var klines []*types.KlineData
		var err error
		
		for retry := 0; retry < maxRetries; retry++ {
			klines, err = bs.FetchHistoricalKlines(symbol, currentStart, currentEnd)
			if err == nil {
				break
			}
			
			if retry < maxRetries-1 {
				bs.logger.Warnf("⚠️ [BACKFILL] Retry %d/%d for %s: %v", retry+1, maxRetries, symbol, err)
				time.Sleep(retryDelay)
				retryDelay *= 2 // 指数退避
			}
		}

		if err != nil {
			bs.logger.Errorf("❌ [BACKFILL] Failed to fetch data for %s after %d retries: %v", symbol, maxRetries, err)
			result.ErrorMessage = err.Error()
			result.Duration = time.Since(startTime).String()
			return result, err
		}

		allKlines = append(allKlines, klines...)
		result.Fetched += len(klines)

		bs.logger.Debugf("📥 [BACKFILL] Fetched %d records for %s (batch)", len(klines), symbol)

		// 请求间隔，避免触发币安限制
		requestInterval, _ := time.ParseDuration(bs.config.Backfill.RequestInterval)
		time.Sleep(requestInterval)

		currentStart = currentEnd
	}

	if len(allKlines) == 0 {
		bs.logger.Warnf("⚠️ [BACKFILL] No data returned from Binance API for %s", symbol)
		result.ErrorMessage = "No data returned from Binance API"
		result.Duration = time.Since(startTime).String()
		return result, fmt.Errorf("no data returned for %s", symbol)
	}

	bs.logger.Infof("📥 [BACKFILL] Total fetched %d records for %s from Binance API", len(allKlines), symbol)

	// 去重和排序数据
	uniqueKlines := bs.deduplicateAndSortKlines(allKlines)
	bs.logger.Infof("🔍 [BACKFILL] After deduplication: %d unique records for %s", len(uniqueKlines), symbol)

	// 将数据插入数据库
	bs.logger.Infof("💾 [BACKFILL] Inserting %d records into database for %s...", len(uniqueKlines), symbol)
	err := bs.storage.BatchInsertKlineData(uniqueKlines)
	if err != nil {
		bs.logger.Errorf("❌ [BACKFILL] Failed to insert data for %s: %v", symbol, err)
		result.ErrorMessage = err.Error()
		result.Duration = time.Since(startTime).String()
		return result, fmt.Errorf("failed to insert data: %w", err)
	}

	result.Inserted = len(uniqueKlines)
	result.Success = true
	result.Duration = time.Since(startTime).String()

	bs.logger.Infof("✅ [BACKFILL] Successfully backfilled %s: %d/%d records inserted in %s",
		symbol, result.Inserted, result.Requested, result.Duration)

	return result, nil
}

// deduplicateAndSortKlines 去重和排序K线数据
func (bs *BackfillService) deduplicateAndSortKlines(klines []*types.KlineData) []*types.KlineData {
	if len(klines) == 0 {
		return klines
	}

	// 使用map去重，以OpenTime为key
	uniqueMap := make(map[int64]*types.KlineData)
	for _, kline := range klines {
		uniqueMap[kline.OpenTime] = kline
	}

	// 转换回slice
	uniqueKlines := make([]*types.KlineData, 0, len(uniqueMap))
	for _, kline := range uniqueMap {
		uniqueKlines = append(uniqueKlines, kline)
	}

	// 按OpenTime排序
	for i := 0; i < len(uniqueKlines)-1; i++ {
		for j := i + 1; j < len(uniqueKlines); j++ {
			if uniqueKlines[i].OpenTime > uniqueKlines[j].OpenTime {
				uniqueKlines[i], uniqueKlines[j] = uniqueKlines[j], uniqueKlines[i]
			}
		}
	}

	return uniqueKlines
}

// BackfillAllSymbolsComplete 为所有交易对回填完整的5天数据
func (bs *BackfillService) BackfillAllSymbolsComplete() (map[string]*BackfillResult, error) {
	// 重置进度
	bs.progress.mu.Lock()
	bs.progress.IsRunning = true
	bs.progress.StartTime = time.Now()
	bs.progress.LastUpdate = time.Now()
	bs.progress.mu.Unlock()

	bs.logger.Info("🚀 [BACKFILL] Starting complete backfill for all symbols")

	// 获取所有交易对
	symbols, err := bs.storage.GetAllSymbols()
	if err != nil {
		bs.logger.Errorf("❌ [BACKFILL] Failed to get symbols: %v", err)
		bs.progress.mu.Lock()
		bs.progress.IsRunning = false
		bs.progress.CurrentSymbol = ""
		bs.progress.LastUpdate = time.Now()
		bs.progress.mu.Unlock()
		return nil, fmt.Errorf("failed to get symbols: %w", err)
	}

	if len(symbols) == 0 {
		bs.logger.Info("✅ [BACKFILL] No symbols found")
		bs.progress.mu.Lock()
		bs.progress.IsRunning = false
		bs.progress.CurrentSymbol = ""
		bs.progress.LastUpdate = time.Now()
		bs.progress.mu.Unlock()
		return map[string]*BackfillResult{}, nil
	}

	bs.logger.Infof("📊 [BACKFILL] Found %d symbols to backfill", len(symbols))

	// 设置总符号数
	bs.progress.mu.Lock()
	bs.progress.TotalSymbols = len(symbols)
	bs.progress.mu.Unlock()

	// 逐个处理交易对
	allResults := make(map[string]*BackfillResult)
	totalSymbols := len(symbols)

	for i, symbol := range symbols {
		// 更新当前处理的符号
		bs.progress.mu.Lock()
		bs.progress.CurrentSymbol = symbol
		bs.progress.mu.Unlock()

		bs.logger.Infof("🔄 [BACKFILL] Processing %s (%d/%d)",
			symbol, i+1, totalSymbols)

		// 回填单个交易对的完整数据
		result, err := bs.BackfillSymbolComplete(symbol)
		allResults[symbol] = result

		if err != nil {
			bs.logger.Errorf("❌ [BACKFILL] Failed to backfill %s: %v", symbol, err)
		}

		// 更新进度
		bs.progress.mu.Lock()
		bs.progress.Processed++
		bs.progress.LastUpdate = time.Now()
		if result.Success {
			bs.progress.SuccessCount++
		} else {
			bs.progress.FailedCount++
		}
		bs.progress.mu.Unlock()

		// 交易对之间更长的延迟，避免触发币安限制
		symbolInterval, _ := time.ParseDuration(bs.config.Backfill.SymbolInterval)
		time.Sleep(symbolInterval)
	}

	// 完成处理
	bs.progress.mu.Lock()
	bs.progress.IsRunning = false
	bs.progress.CurrentSymbol = ""
	bs.progress.LastUpdate = time.Now()
	bs.progress.Results = allResults
	bs.progress.mu.Unlock()

	// 统计总体结果
	totalSuccess := 0
	totalFetched := 0
	totalInserted := 0
	for _, result := range allResults {
		if result.Success {
			totalSuccess++
		}
		totalFetched += result.Fetched
		totalInserted += result.Inserted
	}

	bs.logger.Infof("🎉 [BACKFILL] All symbols completed: %d/%d symbols successfully backfilled, %d records fetched, %d records inserted",
		totalSuccess, totalSymbols, totalFetched, totalInserted)

	return allResults, nil
}

// BackfillSymbolRange 为特定交易对在指定时间范围内回填数据
func (bs *BackfillService) BackfillSymbolRange(symbol string, startTime, endTime time.Time) (*BackfillResult, error) {
	startTimeProcess := time.Now()

	result := &BackfillResult{
		Symbol:    symbol,
		StartTime: startTime,
		EndTime:   endTime,
		Success:   false,
	}

	bs.logger.Infof("🚀 [BACKFILL] Starting range backfill for %s: %s to %s",
		symbol, startTime.Format("2006-01-02 15:04:05"),
		endTime.Format("2006-01-02 15:04:05"))

	// 计算总分钟数
	totalMinutes := int(endTime.Sub(startTime).Minutes())
	result.Requested = totalMinutes

	// 分批获取数据
	allKlines := make([]*types.KlineData, 0, totalMinutes)
	currentStart := startTime

	for currentStart.Before(endTime) {
		currentEnd := currentStart.Add(time.Duration(bs.config.Backfill.BatchSize) * time.Minute)
		if currentEnd.After(endTime) {
			currentEnd = endTime
		}

		// 从Binance获取历史数据
		klines, err := bs.FetchHistoricalKlines(symbol, currentStart, currentEnd)
		if err != nil {
			bs.logger.Errorf("❌ [BACKFILL] Failed to fetch data for %s: %v", symbol, err)
			result.ErrorMessage = err.Error()
			result.Duration = time.Since(startTimeProcess).String()
			return result, err
		}

		allKlines = append(allKlines, klines...)
		result.Fetched += len(klines)

		// 请求间隔
		requestInterval, _ := time.ParseDuration(bs.config.Backfill.RequestInterval)
		time.Sleep(requestInterval)

		currentStart = currentEnd
	}

	if len(allKlines) == 0 {
		bs.logger.Warnf("⚠️ [BACKFILL] No data returned from Binance API for %s", symbol)
		result.ErrorMessage = "No data returned from Binance API"
		result.Duration = time.Since(startTimeProcess).String()
		return result, fmt.Errorf("no data returned for %s", symbol)
	}

	// 将数据插入数据库
	err := bs.storage.BatchInsertKlineData(allKlines)
	if err != nil {
		bs.logger.Errorf("❌ [BACKFILL] Failed to insert data for %s: %v", symbol, err)
		result.ErrorMessage = err.Error()
		result.Duration = time.Since(startTimeProcess).String()
		return result, fmt.Errorf("failed to insert data: %w", err)
	}

	result.Inserted = len(allKlines)
	result.Success = true
	result.Duration = time.Since(startTimeProcess).String()

	bs.logger.Infof("✅ [BACKFILL] Successfully backfilled %s: %d/%d records inserted in %s",
		symbol, result.Inserted, result.Requested, result.Duration)

	return result, nil
}

// BackfillGap 回填特定的数据缺口
func (bs *BackfillService) BackfillGap(gap *storage.DataGap) (*BackfillResult, error) {
	startTime := time.Now()

	result := &BackfillResult{
		Symbol:    gap.Symbol,
		StartTime: gap.StartTime,
		EndTime:   gap.EndTime,
		Requested: gap.Missing,
		Success:   false,
	}

	bs.logger.Infof("🚀 [BACKFILL] Starting gap backfill for %s: %s to %s (%d missing records)",
		gap.Symbol, gap.StartTime.Format("2006-01-02 15:04:05"),
		gap.EndTime.Format("2006-01-02 15:04:05"), gap.Missing)

	// 计算时间范围
	gapDuration := gap.EndTime.Sub(gap.StartTime)
	totalMinutes := int(gapDuration.Minutes()) + 1 // +1 因为包含开始和结束时间

	// 分批获取数据
	allKlines := make([]*types.KlineData, 0, totalMinutes)
	currentStart := gap.StartTime

	for currentStart.Before(gap.EndTime) || currentStart.Equal(gap.EndTime) {
		currentEnd := currentStart.Add(time.Duration(bs.config.Backfill.BatchSize) * time.Minute)
		if currentEnd.After(gap.EndTime) {
			currentEnd = gap.EndTime
		}

		bs.logger.Debugf("📡 [BACKFILL] Fetching gap data for %s: %s to %s",
			gap.Symbol, currentStart.Format("2006-01-02 15:04:05"),
			currentEnd.Format("2006-01-02 15:04:05"))

		// 从Binance获取历史数据
		klines, err := bs.FetchHistoricalKlines(gap.Symbol, currentStart, currentEnd)
		if err != nil {
			bs.logger.Errorf("❌ [BACKFILL] Failed to fetch gap data for %s: %v", gap.Symbol, err)
			result.ErrorMessage = err.Error()
			result.Duration = time.Since(startTime).String()
			return result, err
		}

		allKlines = append(allKlines, klines...)
		result.Fetched += len(klines)

		bs.logger.Debugf("📥 [BACKFILL] Fetched %d records for %s gap (batch)", len(klines), gap.Symbol)

		// 请求间隔，避免触发币安限制
		requestInterval, _ := time.ParseDuration(bs.config.Backfill.RequestInterval)
		time.Sleep(requestInterval)

		currentStart = currentEnd
	}

	if len(allKlines) == 0 {
		bs.logger.Warnf("⚠️ [BACKFILL] No data returned from Binance API for %s gap", gap.Symbol)
		result.ErrorMessage = "No data returned from Binance API"
		result.Duration = time.Since(startTime).String()
		return result, fmt.Errorf("no data returned for %s gap", gap.Symbol)
	}

	bs.logger.Infof("📥 [BACKFILL] Total fetched %d records for %s gap from Binance API", len(allKlines), gap.Symbol)

	// 将数据插入数据库
	bs.logger.Infof("💾 [BACKFILL] Inserting %d records into database for %s gap...", len(allKlines), gap.Symbol)
	err := bs.storage.BatchInsertKlineData(allKlines)
	if err != nil {
		bs.logger.Errorf("❌ [BACKFILL] Failed to insert gap data for %s: %v", gap.Symbol, err)
		result.ErrorMessage = err.Error()
		result.Duration = time.Since(startTime).String()
		return result, fmt.Errorf("failed to insert gap data: %w", err)
	}

	result.Inserted = len(allKlines)
	result.Success = true
	result.Duration = time.Since(startTime).String()

	bs.logger.Infof("✅ [BACKFILL] Successfully backfilled gap for %s: %d/%d records inserted in %s",
		gap.Symbol, result.Inserted, result.Requested, result.Duration)

	return result, nil
}

// GetBackfillStatus 返回当前回补状态
func (bs *BackfillService) GetBackfillStatus() (map[string]interface{}, error) {
	// 获取进度信息
	progress := bs.GetProgress()

	status := map[string]interface{}{
		"last_check": time.Now(),
		"progress":   progress,
		"backfill_config": map[string]interface{}{
			"days_to_backfill":       bs.config.Backfill.DaysToBackfill,
			"batch_size":             bs.config.Backfill.BatchSize,
			"request_interval":       bs.config.Backfill.RequestInterval,
			"symbol_interval":        bs.config.Backfill.SymbolInterval,
			"max_concurrent_symbols": bs.config.Backfill.MaxConcurrentSymbols,
		},
	}

	return status, nil
}

// GetProgress 获取当前回填进度
func (bs *BackfillService) GetProgress() *BackfillProgress {
	bs.progress.mu.RLock()
	defer bs.progress.mu.RUnlock()

	// 创建副本以避免并发访问问题
	progress := &BackfillProgress{
		IsRunning:     bs.progress.IsRunning,
		StartTime:     bs.progress.StartTime,
		CurrentSymbol: bs.progress.CurrentSymbol,
		TotalSymbols:  bs.progress.TotalSymbols,
		Processed:     bs.progress.Processed,
		SuccessCount:  bs.progress.SuccessCount,
		FailedCount:   bs.progress.FailedCount,
		Results:       make(map[string]*BackfillResult),
		LastUpdate:    bs.progress.LastUpdate,
	}

	// 复制结果
	for symbol, result := range bs.progress.Results {
		progress.Results[symbol] = result
	}

	return progress
}

// updateProgress 更新进度信息
func (bs *BackfillService) updateProgress(symbol string, success bool) {
	bs.progress.mu.Lock()
	defer bs.progress.mu.Unlock()

	bs.progress.CurrentSymbol = symbol
	bs.progress.Processed++
	bs.progress.LastUpdate = time.Now()

	if success {
		bs.progress.SuccessCount++
	} else {
		bs.progress.FailedCount++
	}
}

// resetProgress 重置进度
func (bs *BackfillService) resetProgress() {
	bs.progress.mu.Lock()
	defer bs.progress.mu.Unlock()

	bs.progress.IsRunning = false
	bs.progress.StartTime = time.Time{}
	bs.progress.CurrentSymbol = ""
	bs.progress.TotalSymbols = 0
	bs.progress.Processed = 0
	bs.progress.SuccessCount = 0
	bs.progress.FailedCount = 0
	bs.progress.Results = make(map[string]*BackfillResult)
	bs.progress.LastUpdate = time.Now()
}
