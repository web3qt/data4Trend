package types

import (
	"time"
)

// KlineData 代表单个K线/蜡烛图数据点
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

// BinanceKlineEvent 代表来自Binance的WebSocket K线事件
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

// SymbolInfo 代表交易对信息
type SymbolInfo struct {
	Symbol     string `json:"symbol"`
	BaseAsset  string `json:"baseAsset"`
	QuoteAsset string `json:"quoteAsset"`
	Status     string `json:"status"`
}

// ExchangeInfo 代表Binance交易所信息
type ExchangeInfo struct {
	Timezone   string       `json:"timezone"`
	ServerTime int64        `json:"serverTime"`
	Symbols    []SymbolInfo `json:"symbols"`
}

// MonitoringStats 代表系统监控统计信息
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