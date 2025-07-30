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

// ExchangeInfo represents Binance exchange info response
type ExchangeInfo struct {
	Symbols []SymbolInfo `json:"symbols"`
}

// SymbolInfo represents symbol information from Binance
type SymbolInfo struct {
	Symbol     string `json:"symbol"`
	BaseAsset  string `json:"baseAsset"`
	QuoteAsset string `json:"quoteAsset"`
	Status     string `json:"status"`
	IsSpotTradingAllowed bool `json:"isSpotTradingAllowed"`
}

// SymbolService handles dynamic symbol fetching from Binance
type SymbolService struct {
	config *config.Config
	logger *logrus.Logger
	client *http.Client
}

// NewSymbolService creates a new symbol service
func NewSymbolService(cfg *config.Config, logger *logrus.Logger) *SymbolService {
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Set proxy if configured
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

// FetchSymbols fetches all symbols from Binance API
func (s *SymbolService) FetchSymbols() ([]string, error) {
	// If auto fetch is disabled, use configured symbols
	if !s.config.WebSocket.AutoFetchSymbols {
		if len(s.config.Symbols) > 0 {
			s.logger.Info("Using configured symbols list")
			return s.config.Symbols, nil
		}
		// Fallback to default symbols if none configured
		defaultSymbols := []string{"BTCUSDT", "ETHUSDT", "BNBUSDT"}
		s.logger.Warn("No symbols configured, using default symbols")
		return defaultSymbols, nil
	}

	s.logger.Info("Fetching symbols from Binance API...")

	// Fetch exchange info from Binance
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

	// Filter symbols based on configuration
	symbols := s.filterSymbols(exchangeInfo.Symbols)

	s.logger.Infof("Fetched %d symbols from Binance API", len(symbols))
	return symbols, nil
}

// filterSymbols filters symbols based on configuration
func (s *SymbolService) filterSymbols(symbolInfos []SymbolInfo) []string {
	var filteredSymbols []string
	filter := s.config.WebSocket.SymbolFilter

	for _, symbolInfo := range symbolInfos {
		// Check if symbol is active and spot trading is allowed
		if symbolInfo.Status != "TRADING" || !symbolInfo.IsSpotTradingAllowed {
			continue
		}

		// Filter by quote asset (default: USDT)
		quoteAsset := filter.QuoteAsset
		if quoteAsset == "" {
			quoteAsset = "USDT"
		}
		if symbolInfo.QuoteAsset != quoteAsset {
			continue
		}

		// Exclude symbols matching patterns
		if s.shouldExcludeSymbol(symbolInfo.Symbol, filter.ExcludePatterns) {
			continue
		}

		filteredSymbols = append(filteredSymbols, symbolInfo.Symbol)
	}

	return filteredSymbols
}

// shouldExcludeSymbol checks if a symbol should be excluded based on patterns
func (s *SymbolService) shouldExcludeSymbol(symbol string, excludePatterns []string) bool {
	for _, pattern := range excludePatterns {
		if strings.Contains(symbol, pattern) {
			return true
		}
	}
	return false
}

// GetSymbolsWithRetry fetches symbols with retry logic
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
			// Wait before retry
			waitTime := time.Duration(attempt) * 5 * time.Second
			s.logger.Infof("Retrying in %v...", waitTime)
			time.Sleep(waitTime)
		}
	}

	return nil, fmt.Errorf("failed to fetch symbols after %d attempts: %w", maxRetries, lastErr)
}