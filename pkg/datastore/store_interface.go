package datastore

import (
	"context"
	"time"

	"github.com/web3qt/data4Trend/internal/types"
)

// Store 定义数据存储接口
type Store interface {
	// 查询方法
	QueryHistoryData(ctx context.Context, symbol, start, end, pageSize, pageToken string) ([]*types.KLineData, string, error)
	QueryKlines(ctx context.Context, symbol string, interval string, limit int) ([]*types.KLineData, error)
	QueryHistoryKlines(ctx context.Context, symbol string, interval string, startTime time.Time, endTime time.Time) ([]*types.KLineData, error)
	
	// 管理方法
	GetAvailableSymbols(ctx context.Context) ([]map[string]interface{}, error)
	GetStats(ctx context.Context) (map[string]interface{}, error)
	
	// 数据操作方法
	SaveKLineData(ctx context.Context, data *types.KLineData) error
	DeleteKLinesInTimeRange(ctx context.Context, symbol string, interval string, startTime time.Time, endTime time.Time) error
	
	// 数据质量方法
	CheckDataGaps(ctx context.Context, symbol, interval string, startTime, endTime time.Time) ([]types.DataGap, error)
	FixDataGap(ctx context.Context, symbol, interval string, startTime, endTime time.Time) error
	
	// 通道管理
	SetInputChannel(input <-chan *types.KLineData)
	
	// 生命周期方法
	Start(ctx context.Context)
} 