package types

import (
	"context"
	"time"
)

// KLineData 定义了K线数据结构
type KLineData struct {
	OpenTime  time.Time
	CloseTime time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
	Symbol    string
	Interval  string
}

// KlinesService 定义了K线服务接口
type KlinesService interface {
	Symbol(symbol string) KlinesService
	Interval(interval string) KlinesService
	Limit(limit int) KlinesService
	StartTime(startTime int64) KlinesService
	EndTime(endTime int64) KlinesService
	Do(ctx context.Context) ([]*KLineData, error)
}
