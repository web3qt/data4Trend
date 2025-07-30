package types

import (
	"time"
)

// KlineData represents a single kline/candlestick data point
type KlineData struct {
	Symbol    string    `json:"symbol" db:"symbol"`
	OpenTime  int64     `json:"open_time" db:"open_time"`
	CloseTime int64     `json:"close_time" db:"close_time"`
	Open      string    `json:"open" db:"open"`
	High      string    `json:"high" db:"high"`
	Low       string    `json:"low" db:"low"`
	Close     string    `json:"close" db:"close"`
	Volume    string    `json:"volume" db:"volume"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
}

// BinanceKlineEvent represents the WebSocket kline event from Binance
type BinanceKlineEvent struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	Kline     struct {
		Symbol               string `json:"s"`
		OpenTime             int64  `json:"t"`
		CloseTime            int64  `json:"T"`
		Interval             string `json:"i"`
		FirstTradeID         int64  `json:"f"`
		LastTradeID          int64  `json:"L"`
		Open                 string `json:"o"`
		Close                string `json:"c"`
		High                 string `json:"h"`
		Low                  string `json:"l"`
		Volume               string `json:"v"`
		NumberOfTrades       int64  `json:"n"`
		IsClosed             bool   `json:"x"`
		QuoteAssetVolume     string `json:"q"`
		TakerBuyBaseVolume   string `json:"V"`
		TakerBuyQuoteVolume  string `json:"Q"`
		Ignore               string `json:"B"`
	} `json:"k"`
}

// SymbolInfo represents trading pair information
type SymbolInfo struct {
	Symbol     string `json:"symbol"`
	BaseAsset  string `json:"baseAsset"`
	QuoteAsset string `json:"quoteAsset"`
	Status     string `json:"status"`
}

// ExchangeInfo represents Binance exchange information
type ExchangeInfo struct {
	Timezone   string       `json:"timezone"`
	ServerTime int64        `json:"serverTime"`
	Symbols    []SymbolInfo `json:"symbols"`
}

// MonitoringStats represents system monitoring statistics
type MonitoringStats struct {
	Uptime          time.Duration `json:"uptime"`
	MessagesTotal   int64         `json:"messages_total"`
	MessagesPerSec  float64       `json:"messages_per_sec"`
	ErrorsTotal     int64         `json:"errors_total"`
	ErrorRate       float64       `json:"error_rate"`
	ActiveStreams   int           `json:"active_streams"`
	HealthStatus    string        `json:"health_status"`
	Issues          []string      `json:"issues"`
	LastMessageTime time.Time     `json:"last_message_time"`
}