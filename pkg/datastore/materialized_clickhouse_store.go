package datastore

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/sirupsen/logrus"
	"github.com/web3qt/data4Trend/internal/types"
	"github.com/web3qt/data4Trend/pkg/logging"
)

// MaterializedClickHouseStore 基于物化视图的ClickHouse存储实现（最佳实践）
type MaterializedClickHouseStore struct {
	conn          clickhouse.Conn
	db            *sql.DB
	dsn           string
	inputChan     <-chan *types.KLineData
	createdTables map[string]bool // 缓存已创建的表
	tablesMu      sync.RWMutex    // 保护 createdTables 的互斥锁
	idCounter     uint64          // ID计数器
	idMu          sync.Mutex      // 保护ID计数器的互斥锁
}

// NewMaterializedClickHouseStore 创建新的基于物化视图的ClickHouse存储
func NewMaterializedClickHouseStore(cfg *ClickHouseConfig, input <-chan *types.KLineData) (*MaterializedClickHouseStore, error) {
	// 验证配置
	if cfg.Host == "" || cfg.Port == 0 || cfg.User == "" || cfg.Database == "" {
		logging.Logger.Error("ClickHouse配置不完整")
		return nil, fmt.Errorf("ClickHouse配置不完整")
	}

	logging.Logger.WithFields(logrus.Fields{
		"host": cfg.Host,
		"port": cfg.Port,
	}).Info("连接ClickHouse（物化视图架构）")

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

	logging.Logger.Info("ClickHouse连接池配置成功（物化视图架构）")

	// 测试数据库连接
	if err := nativeConn.Ping(context.Background()); err != nil {
		logging.Logger.WithError(err).Error("ClickHouse连接测试失败")
		return nil, fmt.Errorf("ClickHouse连接测试失败: %w", err)
	}

	return &MaterializedClickHouseStore{
		conn:          nativeConn,
		db:            sqlDB,
		inputChan:     input,
		createdTables: make(map[string]bool),
	}, nil
}

// getTableNameForInterval 根据时间间隔获取对应的表名
func (s *MaterializedClickHouseStore) getTableNameForInterval(interval string) string {
	switch interval {
	case "1m":
		return "klines_1m" // 1分钟数据存储在原始表中
	case "5m":
		return "kline_5m"
	case "15m":
		return "kline_15m"
	case "1h":
		return "kline_1h"
	case "4h":
		return "kline_4h"
	case "1d":
		return "kline_1d"
	default:
		// 对于不支持的间隔，使用统一视图
		return "v_kline_unified"
	}
}

// Start 启动数据存储服务
func (s *MaterializedClickHouseStore) Start(ctx context.Context) {
	logging.Logger.Info("启动ClickHouse数据存储服务（物化视图架构）")

	for {
		select {
		case <-ctx.Done():
			logging.Logger.Info("ClickHouse数据存储服务停止")
			return
		case data := <-s.inputChan:
			if data != nil {
				if err := s.writeDataPoint(data); err != nil {
					logging.Logger.WithError(err).Error("写入数据失败")
				}
			}
		}
	}
}

// writeDataPoint 写入单个数据点（只写入原始表，物化视图自动聚合）
func (s *MaterializedClickHouseStore) writeDataPoint(data *types.KLineData) error {
	if data == nil {
		return fmt.Errorf("数据为空")
	}

	// 只有1分钟数据才写入原始表，其他粒度由物化视图自动生成
	if data.Interval != "1m" {
		logging.Logger.WithFields(logrus.Fields{
			"symbol":   data.Symbol,
			"interval": data.Interval,
		}).Debug("跳过非1分钟数据，将由物化视图自动聚合")
		return nil
	}

	// 准备插入语句
	query := `
		INSERT INTO klines_1m (
			symbol, open_time, close_time, 
			open, high, low, close, volume
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`

	// 执行插入
	err := s.conn.Exec(context.Background(), query,
		data.Symbol,
		data.OpenTime,
		data.CloseTime,
		data.Open,
		data.High,
		data.Low,
		data.Close,
		data.Volume,
	)

	if err != nil {
		logging.Logger.WithError(err).WithFields(logrus.Fields{
			"symbol":    data.Symbol,
			"interval":  data.Interval,
			"open_time": data.OpenTime,
		}).Error("插入数据到klines_1m表失败")
		return fmt.Errorf("插入数据失败: %w", err)
	}

	logging.Logger.WithFields(logrus.Fields{
		"symbol":    data.Symbol,
		"interval":  data.Interval,
		"open_time": data.OpenTime,
	}).Debug("数据写入成功，物化视图将自动聚合")

	return nil
}

// QueryKlines 查询K线数据
func (s *MaterializedClickHouseStore) QueryKlines(ctx context.Context, symbol string, interval string, limit int) ([]*types.KLineData, error) {
	tableName := s.getTableNameForInterval(interval)

	var query string
	if tableName == "v_kline_unified" {
		// 使用统一视图查询
		query = fmt.Sprintf(`
			SELECT symbol, interval_type, open_time, close_time, open_price, high_price, low_price, close_price, volume
			FROM %s 
			WHERE symbol = ? AND interval_type = ?
			ORDER BY open_time DESC 
			LIMIT ?
		`, tableName)
	} else {
		// 使用专用表查询
		query = fmt.Sprintf(`
			SELECT symbol, '%s' as interval_type, open_time, close_time, open_price, high_price, low_price, close_price, volume
			FROM %s 
			WHERE symbol = ?
			ORDER BY open_time DESC 
			LIMIT ?
		`, interval, tableName)
	}

	rows, err := s.db.QueryContext(ctx, query, symbol, interval, limit)
	if err != nil {
		return nil, fmt.Errorf("查询失败: %w", err)
	}
	defer rows.Close()

	var results []*types.KLineData
	for rows.Next() {
		var kline types.KLineData
		var intervalType string

		err := rows.Scan(
			&kline.Symbol,
			&intervalType,
			&kline.OpenTime,
			&kline.CloseTime,
			&kline.Open,
			&kline.High,
			&kline.Low,
			&kline.Close,
			&kline.Volume,
		)
		if err != nil {
			return nil, fmt.Errorf("扫描行失败: %w", err)
		}

		kline.Interval = intervalType
		results = append(results, &kline)
	}

	return results, nil
}

// QueryHistoryKlines 查询历史K线数据
func (s *MaterializedClickHouseStore) QueryHistoryKlines(ctx context.Context, symbol string, interval string, startTime time.Time, endTime time.Time) ([]*types.KLineData, error) {
	tableName := s.getTableNameForInterval(interval)

	var query string
	var args []interface{}

	if tableName == "v_kline_unified" {
		// 使用统一视图查询
		query = fmt.Sprintf(`
			SELECT symbol, interval_type, open_time, close_time, open_price, high_price, low_price, close_price, volume
			FROM %s 
			WHERE symbol = ? AND interval_type = ? AND open_time >= ? AND open_time <= ?
			ORDER BY open_time ASC
		`, tableName)
		args = []interface{}{symbol, interval, startTime, endTime}
	} else {
		// 使用专用表查询
		query = fmt.Sprintf(`
			SELECT symbol, '%s' as interval_type, open_time, close_time, open_price, high_price, low_price, close_price, volume
			FROM %s 
			WHERE symbol = ? AND open_time >= ? AND open_time <= ?
			ORDER BY open_time ASC
		`, interval, tableName)
		args = []interface{}{symbol, startTime, endTime}
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("查询历史数据失败: %w", err)
	}
	defer rows.Close()

	var results []*types.KLineData
	for rows.Next() {
		var kline types.KLineData
		var intervalType string

		err := rows.Scan(
			&kline.Symbol,
			&intervalType,
			&kline.OpenTime,
			&kline.CloseTime,
			&kline.Open,
			&kline.High,
			&kline.Low,
			&kline.Close,
			&kline.Volume,
		)
		if err != nil {
			return nil, fmt.Errorf("扫描行失败: %w", err)
		}

		kline.Interval = intervalType
		results = append(results, &kline)
	}

	return results, nil
}

// QueryHistoryData 分页查询历史数据
func (s *MaterializedClickHouseStore) QueryHistoryData(ctx context.Context, symbol, start, end, pageSize, pageToken string) ([]*types.KLineData, string, error) {
	// 解析分页参数
	limit := 1000
	if pageSize != "" {
		if parsedLimit, err := strconv.Atoi(pageSize); err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	offset := 0
	if pageToken != "" {
		if parsedOffset, err := strconv.Atoi(pageToken); err == nil && parsedOffset >= 0 {
			offset = parsedOffset
		}
	}

	// 使用统一视图查询所有时间粒度的数据
	query := `
		SELECT symbol, interval_type, open_time, close_time, open_price, high_price, low_price, close_price, volume
		FROM v_kline_unified
		WHERE symbol = ?
	`
	args := []interface{}{symbol}

	// 添加时间范围条件
	if start != "" {
		query += " AND open_time >= ?"
		args = append(args, start)
	}
	if end != "" {
		query += " AND open_time <= ?"
		args = append(args, end)
	}

	query += " ORDER BY open_time DESC, interval_type"
	query += " LIMIT ? OFFSET ?"
	args = append(args, limit, offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("查询历史数据失败: %w", err)
	}
	defer rows.Close()

	var results []*types.KLineData
	for rows.Next() {
		var kline types.KLineData
		var intervalType string

		err := rows.Scan(
			&kline.Symbol,
			&intervalType,
			&kline.OpenTime,
			&kline.CloseTime,
			&kline.Open,
			&kline.High,
			&kline.Low,
			&kline.Close,
			&kline.Volume,
		)
		if err != nil {
			return nil, "", fmt.Errorf("扫描行失败: %w", err)
		}

		kline.Interval = intervalType
		results = append(results, &kline)
	}

	// 计算下一页token
	nextPageToken := ""
	if len(results) == limit {
		nextPageToken = strconv.Itoa(offset + limit)
	}

	return results, nextPageToken, nil
}

// GetAvailableSymbols 获取可用的交易对
func (s *MaterializedClickHouseStore) GetAvailableSymbols(ctx context.Context) ([]map[string]interface{}, error) {
	query := `
		SELECT 
			symbol,
			count() as total_records,
			min(open_time) as first_record,
			max(open_time) as last_record
		FROM klines_1m
		GROUP BY symbol
		ORDER BY symbol
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("查询可用交易对失败: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var symbol string
		var totalRecords int64
		var firstRecord, lastRecord time.Time

		err := rows.Scan(&symbol, &totalRecords, &firstRecord, &lastRecord)
		if err != nil {
			return nil, fmt.Errorf("扫描行失败: %w", err)
		}

		results = append(results, map[string]interface{}{
			"symbol":        symbol,
			"total_records": totalRecords,
			"first_record":  firstRecord,
			"last_record":   lastRecord,
		})
	}

	return results, nil
}

// GetStats 获取存储统计信息
func (s *MaterializedClickHouseStore) GetStats(ctx context.Context) (map[string]interface{}, error) {
	// 查询表统计信息
	query := `SELECT * FROM v_table_stats`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("查询统计信息失败: %w", err)
	}
	defer rows.Close()

	tableStats := make([]map[string]interface{}, 0)
	for rows.Next() {
		var tableName string
		var totalRows int64
		var sizeBytes int64
		var sizeReadable string
		var lastModified time.Time

		err := rows.Scan(&tableName, &totalRows, &sizeBytes, &sizeReadable, &lastModified)
		if err != nil {
			return nil, fmt.Errorf("扫描统计信息失败: %w", err)
		}

		tableStats = append(tableStats, map[string]interface{}{
			"table_name":    tableName,
			"total_rows":    totalRows,
			"size_bytes":    sizeBytes,
			"size_readable": sizeReadable,
			"last_modified": lastModified,
		})
	}

	return map[string]interface{}{
		"architecture": "materialized_views",
		"description":  "单一事实表 + 物化视图自动聚合架构",
		"table_stats":  tableStats,
		"timestamp":    time.Now(),
	}, nil
}

// SaveKLineData 保存K线数据（兼容接口）
func (s *MaterializedClickHouseStore) SaveKLineData(ctx context.Context, data *types.KLineData) error {
	return s.writeDataPoint(data)
}

// SetInputChannel 设置输入通道
func (s *MaterializedClickHouseStore) SetInputChannel(input <-chan *types.KLineData) {
	s.inputChan = input
}

// DeleteKLinesInTimeRange 删除指定时间范围的K线数据
func (s *MaterializedClickHouseStore) DeleteKLinesInTimeRange(ctx context.Context, symbol string, interval string, startTime time.Time, endTime time.Time) error {
	// 只能删除原始表中的数据，聚合表会自动更新
	if interval != "1m" {
		return fmt.Errorf("只能删除1分钟原始数据，其他粒度由物化视图自动维护")
	}

	query := `
		ALTER TABLE klines_1m DELETE 
		WHERE symbol = ? AND open_time >= ? AND open_time <= ?
	`

	err := s.conn.Exec(ctx, query, symbol, startTime, endTime)
	if err != nil {
		return fmt.Errorf("删除数据失败: %w", err)
	}

	logging.Logger.WithFields(logrus.Fields{
		"symbol":     symbol,
		"interval":   interval,
		"start_time": startTime,
		"end_time":   endTime,
	}).Info("删除数据成功，物化视图将自动更新")

	return nil
}

// CheckDataGaps 检查数据缺口
func (s *MaterializedClickHouseStore) CheckDataGaps(ctx context.Context, symbol, interval string, startTime, endTime time.Time) ([]types.DataGap, error) {
	tableName := s.getTableNameForInterval(interval)

	// 计算预期的时间间隔
	intervalDuration, err := calculateMaterializedIntervalDuration(interval)
	if err != nil {
		return nil, fmt.Errorf("无效的时间间隔: %w", err)
	}

	query := fmt.Sprintf(`
		SELECT 
			open_time,
			LEAD(open_time) OVER (ORDER BY open_time) as next_open_time
		FROM %s
		WHERE symbol = ? AND open_time >= ? AND open_time <= ?
		ORDER BY open_time
	`, tableName)

	rows, err := s.db.QueryContext(ctx, query, symbol, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("查询数据失败: %w", err)
	}
	defer rows.Close()

	var gaps []types.DataGap
	for rows.Next() {
		var openTime time.Time
		var nextOpenTime *time.Time

		err := rows.Scan(&openTime, &nextOpenTime)
		if err != nil {
			return nil, fmt.Errorf("扫描行失败: %w", err)
		}

		if nextOpenTime != nil {
			expectedNext := openTime.Add(intervalDuration)
			if nextOpenTime.After(expectedNext) {
				gaps = append(gaps, types.DataGap{
					Symbol:    symbol,
					Interval:  interval,
					StartTime: expectedNext,
					EndTime:   *nextOpenTime,
				})
			}
		}
	}

	return gaps, nil
}

// FixDataGap 修复数据缺口（需要外部数据源）
func (s *MaterializedClickHouseStore) FixDataGap(ctx context.Context, symbol, interval string, startTime, endTime time.Time) error {
	return fmt.Errorf("数据缺口修复需要外部数据源，请使用数据收集器重新获取数据")
}

// GetDB 获取数据库连接
func (s *MaterializedClickHouseStore) GetDB() *sql.DB {
	return s.db
}

// GetConn 获取ClickHouse原生连接
func (s *MaterializedClickHouseStore) GetConn() clickhouse.Conn {
	return s.conn
}

// calculateMaterializedIntervalDuration 计算时间间隔的持续时间（物化视图版本）
func calculateMaterializedIntervalDuration(interval string) (time.Duration, error) {
	switch interval {
	case "1m":
		return time.Minute, nil
	case "5m":
		return 5 * time.Minute, nil
	case "15m":
		return 15 * time.Minute, nil
	case "1h":
		return time.Hour, nil
	case "4h":
		return 4 * time.Hour, nil
	case "1d":
		return 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("不支持的时间间隔: %s", interval)
	}
}
