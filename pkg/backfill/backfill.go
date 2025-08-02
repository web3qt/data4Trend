package backfill

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/sirupsen/logrus"

	"data4trend/internal/types"
	"data4trend/pkg/config"
	"data4trend/pkg/storage"
)

// BinanceKlineResponse 代表来自Binance K线API的响应
type BinanceKlineResponse [][]interface{}

// BackfillService 处理数据回补操作
type BackfillService struct {
	config  *config.Config
	storage *storage.ClickHouseStorage
	logger  *logrus.Logger
	client  *http.Client
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
		config:  cfg,
		storage: storage,
		logger:  logger,
		client:  client,
	}
}

// BackfillResult 代表回补操作的结果
type BackfillResult struct {
	Symbol        string    `json:"symbol"`
	StartTime     time.Time `json:"start_time"`
	EndTime       time.Time `json:"end_time"`
	Requested     int       `json:"requested_count"`
	Fetched       int       `json:"fetched_count"`
	Inserted      int       `json:"inserted_count"`
	Duration      string    `json:"duration"`
	Success       bool      `json:"success"`
	ErrorMessage  string    `json:"error_message,omitempty"`
}

// FetchHistoricalKlines 从Binance API获取历史K线数据
func (bs *BackfillService) FetchHistoricalKlines(symbol string, startTime, endTime time.Time) ([]*types.KlineData, error) {
	// Binance K线API端点
	apiURL := "https://api.binance.com/api/v3/klines"
	
	// 将时间转换为毫秒
	startMs := startTime.UnixMilli()
	endMs := endTime.UnixMilli()
	
	// 构建请求URL
	reqURL := fmt.Sprintf("%s?symbol=%s&interval=1m&startTime=%d&endTime=%d&limit=1000", 
		apiURL, symbol, startMs, endMs)
	
	bs.logger.Debugf("Fetching historical data: %s", reqURL)
	
	// 发起HTTP请求
	resp, err := bs.client.Get(reqURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch data from Binance: %w", err)
	}
	defer resp.Body.Close()
	
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

// BackfillGap 填补特定的数据缺口
func (bs *BackfillService) BackfillGap(gap *storage.DataGap) (*BackfillResult, error) {
	startTime := time.Now()
	result := &BackfillResult{
		Symbol:    gap.Symbol,
		StartTime: gap.StartTime,
		EndTime:   gap.EndTime,
		Requested: gap.Missing,
		Success:   false,
	}
	
	bs.logger.Infof("Starting backfill for %s: %s to %s (%d missing)", 
		gap.Symbol, gap.StartTime.Format("2006-01-02 15:04:05"), 
		gap.EndTime.Format("2006-01-02 15:04:05"), gap.Missing)
	
	// 从Binance获取历史数据
	klines, err := bs.FetchHistoricalKlines(gap.Symbol, gap.StartTime, gap.EndTime)
	if err != nil {
		result.ErrorMessage = err.Error()
		result.Duration = time.Since(startTime).String()
		return result, err
	}
	
	result.Fetched = len(klines)
	
	if len(klines) == 0 {
		result.ErrorMessage = "No data returned from Binance API"
		result.Duration = time.Since(startTime).String()
		return result, fmt.Errorf("no data returned for %s", gap.Symbol)
	}
	
	// 将数据插入数据库
	err = bs.storage.BatchInsertKlineData(klines)
	if err != nil {
		result.ErrorMessage = err.Error()
		result.Duration = time.Since(startTime).String()
		return result, fmt.Errorf("failed to insert data: %w", err)
	}
	
	result.Inserted = len(klines)
	result.Success = true
	result.Duration = time.Since(startTime).String()
	
	bs.logger.Infof("Backfill completed for %s: %d/%d records inserted in %s", 
		gap.Symbol, result.Inserted, result.Requested, result.Duration)
	
	return result, nil
}

// BackfillSymbol 为特定交易对在时间范围内回补所有缺口
func (bs *BackfillService) BackfillSymbol(symbol string, startTime, endTime time.Time) ([]*BackfillResult, error) {
	bs.logger.Infof("Starting backfill for symbol %s from %s to %s", 
		symbol, startTime.Format("2006-01-02 15:04:05"), endTime.Format("2006-01-02 15:04:05"))
	
	// 检测缺口
	gaps, err := bs.storage.DetectDataGaps(symbol, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to detect gaps: %w", err)
	}
	
	if len(gaps) == 0 {
		bs.logger.Infof("No gaps found for %s", symbol)
		return []*BackfillResult{}, nil
	}
	
	bs.logger.Infof("Found %d gaps for %s", len(gaps), symbol)
	
	// 回补每个缺口
	results := make([]*BackfillResult, 0, len(gaps))
	for _, gap := range gaps {
		result, err := bs.BackfillGap(gap)
		results = append(results, result)
		
		if err != nil {
			bs.logger.Errorf("Failed to backfill gap for %s: %v", symbol, err)
		}
		
		// 速率限制：请求间等待100ms以避免触及Binance限制
		time.Sleep(100 * time.Millisecond)
	}
	
	return results, nil
}

// BackfillAllSymbols 为过去24小时内所有交易对回补缺口
func (bs *BackfillService) BackfillAllSymbols() (map[string][]*BackfillResult, error) {
	bs.logger.Info("Starting backfill for all symbols")
	
	// 获取所有缺口
	allGaps, err := bs.storage.GetDataGapsForAllSymbols()
	if err != nil {
		return nil, fmt.Errorf("failed to get gaps: %w", err)
	}
	
	if len(allGaps) == 0 {
		bs.logger.Info("No gaps found for any symbol")
		return map[string][]*BackfillResult{}, nil
	}
	
	bs.logger.Infof("Found gaps in %d symbols", len(allGaps))
	
	// 回补每个交易对
	allResults := make(map[string][]*BackfillResult)
	for symbol, gaps := range allGaps {
		bs.logger.Infof("Processing %d gaps for %s", len(gaps), symbol)
		
		symbolResults := make([]*BackfillResult, 0, len(gaps))
		for _, gap := range gaps {
			result, err := bs.BackfillGap(gap)
			symbolResults = append(symbolResults, result)
			
			if err != nil {
				bs.logger.Errorf("Failed to backfill gap for %s: %v", symbol, err)
			}
			
			// 速率限制
			time.Sleep(100 * time.Millisecond)
		}
		
		allResults[symbol] = symbolResults
		
		// 交易对之间更长的延迟
		time.Sleep(500 * time.Millisecond)
	}
	
	bs.logger.Info("Backfill completed for all symbols")
	return allResults, nil
}

// GetBackfillStatus 返回当前回补状态
func (bs *BackfillService) GetBackfillStatus() (map[string]interface{}, error) {
	// 获取所有交易对的缺口
	allGaps, err := bs.storage.GetDataGapsForAllSymbols()
	if err != nil {
		return nil, err
	}
	
	totalGaps := 0
	totalMissing := 0
	symbolsWithGaps := len(allGaps)
	
	for _, gaps := range allGaps {
		totalGaps += len(gaps)
		for _, gap := range gaps {
			totalMissing += gap.Missing
		}
	}
	
	status := map[string]interface{}{
		"symbols_with_gaps": symbolsWithGaps,
		"total_gaps":        totalGaps,
		"total_missing":     totalMissing,
		"gaps_by_symbol":    allGaps,
		"last_check":        time.Now(),
	}
	
	return status, nil
}