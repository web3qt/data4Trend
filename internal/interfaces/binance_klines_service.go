package interfaces

import (
	"context"
	"strconv"
	"time"

	"github.com/adshao/go-binance/v2"
	"github.com/web3qt/dataFeeder/internal/types"
)

// BinanceKlinesServiceAdapter 适配器，将binance.KlinesService适配到我们的KlinesService接口
type BinanceKlinesServiceAdapter struct {
	service  *binance.KlinesService
	symbol   string
	interval string
}

// NewBinanceKlinesServiceAdapter 创建一个新的适配器
func NewBinanceKlinesServiceAdapter(service *binance.KlinesService) *BinanceKlinesServiceAdapter {
	return &BinanceKlinesServiceAdapter{
		service: service,
	}
}

// Symbol 设置交易对
func (a *BinanceKlinesServiceAdapter) Symbol(symbol string) KlinesService {
	a.symbol = symbol
	a.service.Symbol(symbol)
	return a
}

// Interval 设置时间间隔
func (a *BinanceKlinesServiceAdapter) Interval(interval string) KlinesService {
	a.interval = interval
	a.service.Interval(interval)
	return a
}

// Limit 设置返回的K线数量限制
func (a *BinanceKlinesServiceAdapter) Limit(limit int) KlinesService {
	a.service.Limit(limit)
	return a
}

// StartTime 设置开始时间
func (a *BinanceKlinesServiceAdapter) StartTime(startTime int64) KlinesService {
	a.service.StartTime(startTime)
	return a
}

// EndTime 设置结束时间
func (a *BinanceKlinesServiceAdapter) EndTime(endTime int64) KlinesService {
	a.service.EndTime(endTime)
	return a
}

// Do 执行请求并将binance.Kline转换为types.KLineData
func (a *BinanceKlinesServiceAdapter) Do(ctx context.Context) ([]*types.KLineData, error) {
	klines, err := a.service.Do(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]*types.KLineData, len(klines))
	for i, kline := range klines {
		openTime := time.Unix(kline.OpenTime/1000, (kline.OpenTime%1000)*int64(time.Millisecond))
		closeTime := time.Unix(kline.CloseTime/1000, (kline.CloseTime%1000)*int64(time.Millisecond))

		open, _ := strconv.ParseFloat(kline.Open, 64)
		high, _ := strconv.ParseFloat(kline.High, 64)
		low, _ := strconv.ParseFloat(kline.Low, 64)
		close, _ := strconv.ParseFloat(kline.Close, 64)
		volume, _ := strconv.ParseFloat(kline.Volume, 64)

		result[i] = &types.KLineData{
			Symbol:    a.symbol,
			Interval:  a.interval,
			OpenTime:  openTime,
			CloseTime: closeTime,
			Open:      open,
			High:      high,
			Low:       low,
			Close:     close,
			Volume:    volume,
		}
	}

	return result, nil
}
