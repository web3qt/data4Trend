package datastore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/sirupsen/logrus"
	"github.com/web3qt/data4Trend/internal/types"
	"github.com/web3qt/data4Trend/pkg/logging"
)

// IntervalClickHouseStore 按时间级别分表的ClickHouse存储实现
type IntervalClickHouseStore struct {
	conn          clickhouse.Conn
	db            *sql.DB
	dsn           string
	inputChan     <-chan *types.KLineData
	createdTables map[string]bool // 缓存已创建的表
	tablesMu      sync.RWMutex    // 保护 createdTables 的互斥锁
	idCounter     uint64          // ID计数器
	idMu          sync.Mutex      // 保护ID计数器的互斥锁
}

// NewIntervalClickHouseStore 创建新的按时间级别分表的ClickHouse存储
func NewIntervalClickHouseStore(cfg *ClickHouseConfig, input <-chan *types.KLineData) (*IntervalClickHouseStore, error) {
	// 验证配置
	if cfg.Host == "" || cfg.Port == 0 || cfg.User == "" || cfg.Database == "" {
		logging.Logger.Error("ClickHouse配置不完整")
		return nil, fmt.Errorf("ClickHouse配置不完整")
	}

	logging.Logger.WithFields(logrus.Fields{
		"host": cfg.Host,
		"port": cfg.Port,
	}).Info("连接ClickHouse（按时间级别分表）")

	// 建立native连接 - 用于高性能操作
	nativeConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.User,
			Password: cfg.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:     time.Second * 30,
		MaxOpenConns:    50,
		MaxIdleConns:    20,
		ConnMaxLifetime: time.Hour,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Debug: false,
	})
	if err != nil {
		logging.Logger.WithError(err).Error("ClickHouse native连接失败")
		return nil, fmt.Errorf("ClickHouse native连接失败: %w", err)
	}

	// 建立database/sql连接 - 用于标准SQL操作
	sqlDB := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.User,
			Password: cfg.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: time.Second * 30,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})

	// 设置连接池参数
	sqlDB.SetMaxOpenConns(50)
	sqlDB.SetMaxIdleConns(20)
	sqlDB.SetConnMaxLifetime(time.Hour)

	logging.Logger.Info("ClickHouse连接池配置成功（按时间级别分表）")

	// 测试数据库连接
	if err := nativeConn.Ping(context.Background()); err != nil {
		logging.Logger.WithError(err).Error("ClickHouse连接测试失败")
		return nil, fmt.Errorf("ClickHouse连接测试失败: %w", err)
	}

	return &IntervalClickHouseStore{
		conn:          nativeConn,
		db:            sqlDB,
		inputChan:     input,
		createdTables: make(map[string]bool),
		tablesMu:      sync.RWMutex{},
		idCounter:     1,
		idMu:          sync.Mutex{},
	}, nil
}

// getTableName 根据时间级别获取表名
func (s *IntervalClickHouseStore) getTableName(interval string) string {
	// 标准化时间级别名称
	interval = strings.ToLower(interval)

	// 1分钟数据存储在 klines_1m 表中
	if interval == "1m" {
		return "klines_1m"
	}

	// 其他时间间隔使用分表
	return fmt.Sprintf("kline_%s", interval)
}

// Start 启动数据写入协程
func (s *IntervalClickHouseStore) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		// 移除未使用的变量
		const maxRetryQueueSize = 10000
		count := 0

		for {
			select {
			case <-ctx.Done():
				logging.Logger.Info("ClickHouse存储协程退出（按时间级别分表）")
				return

			case data := <-s.inputChan:
				count++
				if err := s.writeDataPoint(data); err != nil {
					logging.Logger.WithError(err).Error("写入数据失败（按时间级别分表）")
				}

			case <-ticker.C:
				logging.Logger.WithField("count", count).Info("已处理数据点数量（按时间级别分表）")
			}
		}
	}()
}

func (s *IntervalClickHouseStore) writeDataPoint(data *types.KLineData) error {
	if data == nil {
		return fmt.Errorf("数据为空")
	}

	// 根据时间级别确定表名
	tableName := s.getTableName(data.Interval)

	// 确保表存在
	if err := s.ensureTableExists(tableName, data.Interval); err != nil {
		return fmt.Errorf("确保表存在失败: %w", err)
	}



	// 使用批量插入提高性能
	batch, err := s.conn.PrepareBatch(context.Background(), fmt.Sprintf(`
		INSERT INTO %s (symbol, open_time, close_time, open, high, low, close, volume, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, interval)
	`, tableName))
	if err != nil {
		return fmt.Errorf("准备批量插入失败: %w", err)
	}


	// 使用兼容性字段处理
	openPrice := data.OpenPrice
	if openPrice == 0 {
		openPrice = data.Open
	}
	highPrice := data.HighPrice
	if highPrice == 0 {
		highPrice = data.High
	}
	lowPrice := data.LowPrice
	if lowPrice == 0 {
		lowPrice = data.Low
	}
	closePrice := data.ClosePrice
	if closePrice == 0 {
		closePrice = data.Close
	}

	err = batch.Append(
		data.Symbol,
		data.OpenTime,
		data.CloseTime,
		openPrice,
		highPrice,
		lowPrice,
		closePrice,
		data.Volume,
		data.QuoteAssetVolume,
		data.NumberOfTrades,
		data.TakerBuyBaseVolume,
		data.TakerBuyQuoteVolume,
		data.Interval,
	)
	if err != nil {
		return fmt.Errorf("添加数据到批量插入失败: %w", err)
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("执行批量插入失败: %w", err)
	}

	logging.Logger.WithFields(logrus.Fields{
		"symbol":    data.Symbol,
		"interval":  data.Interval,
		"open_time": data.OpenTime,
		"table":     tableName,
	}).Debug("成功写入K线数据（按时间级别分表）")

	return nil
}

func (s *IntervalClickHouseStore) ensureTableExists(tableName, interval string) error {
	s.tablesMu.RLock()
	exists := s.createdTables[tableName]
	s.tablesMu.RUnlock()

	if exists {
		return nil
	}

	// 检查表是否存在
	var count uint64
	err := s.db.QueryRow("SELECT COUNT(*) FROM system.tables WHERE database = ? AND name = ?", "data4trend", tableName).Scan(&count)
	if err != nil {
		return fmt.Errorf("检查表存在性失败: %w", err)
	}

	if count == 0 {
		// 创建表 - 使用优化的按时间级别分表结构
		createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s
		(
			id UInt64,
			symbol LowCardinality(String),
			open_time DateTime64(3),
			close_time DateTime64(3),
			open_price Decimal64(8),
			high_price Decimal64(8),
			low_price Decimal64(8),
			close_price Decimal64(8),
			volume Decimal64(8),
			created_at DateTime64(3) DEFAULT now64(),
			updated_at DateTime64(3) DEFAULT now64()
		)
		ENGINE = MergeTree()
		PARTITION BY toYYYYMM(open_time)
		ORDER BY (symbol, open_time)
		SETTINGS 
			index_granularity = 8192,
			allow_nullable_key = 0
		`, tableName)

		if err := s.conn.Exec(context.Background(), createTableSQL); err != nil {
			return fmt.Errorf("创建表 %s 失败: %w", tableName, err)
		}

		logging.Logger.WithFields(logrus.Fields{
			"table":    tableName,
			"interval": interval,
		}).Info("成功创建ClickHouse按时间级别分表")
	}

	// 缓存表存在状态
	s.tablesMu.Lock()
	s.createdTables[tableName] = true
	s.tablesMu.Unlock()

	return nil
}

// QueryKlines 查询K线数据
func (s *IntervalClickHouseStore) QueryKlines(ctx context.Context, symbol string, interval string, limit int) ([]*types.KLineData, error) {
	tableName := s.getTableName(interval)

	query := fmt.Sprintf(`
		SELECT symbol, open_time, close_time, open_price, high_price, low_price, close_price, volume
		FROM %s 
		WHERE symbol = ?
		ORDER BY open_time DESC 
		LIMIT ?
	`, tableName)

	rows, err := s.db.QueryContext(ctx, query, symbol, limit)
	if err != nil {
		return nil, fmt.Errorf("查询K线数据失败: %w", err)
	}
	defer rows.Close()

	var results []*types.KLineData
	for rows.Next() {
		var kline types.KLineData
		err := rows.Scan(
			&kline.Symbol,
			&kline.OpenTime,
			&kline.CloseTime,
			&kline.OpenPrice,
			&kline.HighPrice,
			&kline.LowPrice,
			&kline.ClosePrice,
			&kline.Volume,
		)
		if err != nil {
			return nil, fmt.Errorf("扫描K线数据失败: %w", err)
		}
		kline.Interval = interval
		results = append(results, &kline)
	}

	return results, nil
}

// SaveKLineData 保存K线数据
func (s *IntervalClickHouseStore) SaveKLineData(ctx context.Context, data *types.KLineData) error {
	return s.writeDataPoint(data)
}

// SetInputChannel 设置输入通道
func (s *IntervalClickHouseStore) SetInputChannel(input <-chan *types.KLineData) {
	s.inputChan = input
}

// 实现Store接口的其他方法
func (s *IntervalClickHouseStore) QueryHistoryData(ctx context.Context, symbol, start, end, pageSize, pageToken string) ([]*types.KLineData, string, error) {
	return nil, "", fmt.Errorf("暂未实现")
}

func (s *IntervalClickHouseStore) QueryHistoryKlines(ctx context.Context, symbol string, interval string, startTime time.Time, endTime time.Time) ([]*types.KLineData, error) {
	return nil, fmt.Errorf("暂未实现")
}

func (s *IntervalClickHouseStore) GetAvailableSymbols(ctx context.Context) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("暂未实现")
}

func (s *IntervalClickHouseStore) GetStats(ctx context.Context) (map[string]interface{}, error) {
	return nil, fmt.Errorf("暂未实现")
}

func (s *IntervalClickHouseStore) DeleteKLinesInTimeRange(ctx context.Context, symbol string, interval string, startTime time.Time, endTime time.Time) error {
	return fmt.Errorf("暂未实现")
}

func (s *IntervalClickHouseStore) CheckDataGaps(ctx context.Context, symbol, interval string, startTime, endTime time.Time) ([]types.DataGap, error) {
	return nil, fmt.Errorf("暂未实现")
}

func (s *IntervalClickHouseStore) FixDataGap(ctx context.Context, symbol, interval string, startTime, endTime time.Time) error {
	return fmt.Errorf("暂未实现")
}

// GetDB 获取数据库连接
func (s *IntervalClickHouseStore) GetDB() *sql.DB {
	return s.db
}

// GetConn 获取ClickHouse原生连接
func (s *IntervalClickHouseStore) GetConn() clickhouse.Conn {
	return s.conn
}

// calculateIntervalDuration 计算时间间隔的持续时间
// calculateIntervalDuration 函数已在 clickhouse_store.go 中定义，无需重复
