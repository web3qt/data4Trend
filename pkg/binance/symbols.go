package binance

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"data4trend/pkg/config"
)

// ExchangeInfo 代表Binance交易所信息响应
type ExchangeInfo struct {
	Symbols []SymbolInfo `json:"symbols"`
}

// SymbolInfo 代表来自Binance的交易对信息
type SymbolInfo struct {
	Symbol               string `json:"symbol"`
	BaseAsset            string `json:"baseAsset"`
	QuoteAsset           string `json:"quoteAsset"`
	Status               string `json:"status"`
	IsSpotTradingAllowed bool   `json:"isSpotTradingAllowed"`
}

// SymbolService 处理从Binance动态获取交易对
type SymbolService struct {
	config *config.Config
	logger *logrus.Logger
	client *http.Client
}

// NewSymbolService 创建一个新的交易对服务
func NewSymbolService(cfg *config.Config, logger *logrus.Logger) *SymbolService {
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
			logger.Infof("Symbol service using proxy: %s", cfg.GetProxyURL())
		}
	}

	return &SymbolService{
		config: cfg,
		logger: logger,
		client: client,
	}
}

// FetchSymbols 从Binance API获取所有交易对
func (s *SymbolService) FetchSymbols() ([]string, error) {
	// 如果启用自动获取，总是从API获取
	if s.config.WebSocket.AutoFetchSymbols {
		s.logger.Info("Fetching symbols from Binance API...")
	} else {
		// 如果禁用自动获取，使用配置的交易对
		if len(s.config.Symbols) > 0 {
			s.logger.Info("Using configured symbols list")
			return s.config.Symbols, nil
		}
		// 如果没有配置，回退到默认交易对
		defaultSymbols := []string{"BTCUSDT", "ETHUSDT", "BNBUSDT"}
		s.logger.Warn("No symbols configured, using default symbols")
		return defaultSymbols, nil
	}

	// 从Binance获取交易所信息
	apiURL := "https://api.binance.com/api/v3/exchangeInfo"
	resp, err := s.client.Get(apiURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch exchange info: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Binance API error (status %d): %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var exchangeInfo ExchangeInfo
	if err := json.Unmarshal(body, &exchangeInfo); err != nil {
		return nil, fmt.Errorf("failed to parse exchange info: %w", err)
	}

	// 根据配置过滤交易对
	symbols := s.filterSymbols(exchangeInfo.Symbols)

	s.logger.Infof("Fetched %d symbols from Binance API", len(symbols))
	return symbols, nil
}

// filterSymbols 根据配置过滤交易对
func (s *SymbolService) filterSymbols(symbolInfos []SymbolInfo) []string {
	var filteredSymbols []string
	filter := s.config.WebSocket.SymbolFilter

	for _, symbolInfo := range symbolInfos {
		// 检查交易对是否活跃且允许现货交易
		if symbolInfo.Status != "TRADING" || !symbolInfo.IsSpotTradingAllowed {
			continue
		}

		// 按计价资产过滤（默认：USDT）
		quoteAsset := filter.QuoteAsset
		if quoteAsset == "" {
			quoteAsset = "USDT"
		}
		if symbolInfo.QuoteAsset != quoteAsset {
			continue
		}

		// 排除匹配模式的交易对
		if s.shouldExcludeSymbol(symbolInfo.Symbol, filter.ExcludePatterns) {
			continue
		}

		filteredSymbols = append(filteredSymbols, symbolInfo.Symbol)
	}

	return filteredSymbols
}

// shouldExcludeSymbol 检查是否应根据模式排除交易对
func (s *SymbolService) shouldExcludeSymbol(symbol string, excludePatterns []string) bool {
	for _, pattern := range excludePatterns {
		if strings.Contains(symbol, pattern) {
			return true
		}
	}
	return false
}

// GetSymbolsWithRetry 使用重试逻辑获取交易对
func (s *SymbolService) GetSymbolsWithRetry(maxRetries int) ([]string, error) {
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		symbols, err := s.FetchSymbols()
		if err == nil {
			return symbols, nil
		}

		lastErr = err
		s.logger.Warnf("Failed to fetch symbols (attempt %d/%d): %v", attempt, maxRetries, err)

		if attempt < maxRetries {
			// 重试前等待
			waitTime := time.Duration(attempt) * 5 * time.Second
			s.logger.Infof("Retrying in %v...", waitTime)
			time.Sleep(waitTime)
		}
	}

	return nil, fmt.Errorf("failed to fetch symbols after %d attempts: %w", maxRetries, lastErr)
}
