package interfaces

import (
	"context"

	"github.com/web3qt/dataFeeder/internal/types"
)

// KlinesService 定义了K线服务的接口
type KlinesService interface {
	Symbol(symbol string) KlinesService
	Interval(interval string) KlinesService
	Limit(limit int) KlinesService
	StartTime(startTime int64) KlinesService
	EndTime(endTime int64) KlinesService
	Do(ctx context.Context) ([]*types.KLineData, error)
}
