package datastore

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/sirupsen/logrus"
	"github.com/web3qt/data4Trend/internal/types"
	"github.com/web3qt/data4Trend/pkg/logging"
)

// ClickHouseKLine ClickHouse K线数据模型
type ClickHouseKLine struct {
	ID           uint64    `json:"id"`
	Symbol       string    `json:"symbol"`
	IntervalType string    `json:"interval_type"`
	OpenTime     time.Time `json:"open_time"`
	CloseTime    time.Time `json:"close_time"`
	OpenPrice    float64   `json:"open_price"`
	HighPrice    float64   `json:"high_price"`
	LowPrice     float64   `json:"low_price"`
	ClosePrice   float64   `json:"close_price"`
	Volume       float64   `json:"volume"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type ClickHouseStore struct {
	conn          clickhouse.Conn
	db            *sql.DB
	dsn           string
	inputChan     <-chan *types.KLineData
	createdTables map[string]bool // 缓存已创建的表
	tablesMu      sync.RWMutex    // 保护 createdTables 的互斥锁
	idCounter     uint64          // ID计数器
	idMu          sync.Mutex      // 保护ID计数器的互斥锁
}

// ClickHouseConfig ClickHouse存储配置
type ClickHouseConfig struct {
	Host     string
	Port     int
	HTTPPort int
	User     string
	Password string
	Database string
}



func NewClickHouseStore(cfg *ClickHouseConfig, input <-chan *types.KLineData) (*ClickHouseStore, error) {
	// 验证配置
	if cfg.Host == "" || cfg.Port == 0 || cfg.User == "" || cfg.Database == "" {
		logging.Logger.Error("ClickHouse配置不完整")
		return nil, fmt.Errorf("ClickHouse配置不完整")
	}

	logging.Logger.WithFields(logrus.Fields{
		"host": cfg.Host,
		"port": cfg.Port,
	}).Info("连接ClickHouse")

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
		DialTimeout:      time.Second * 30,
		MaxOpenConns:     50,
		MaxIdleConns:     20,
		ConnMaxLifetime:  time.Hour,
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

	logging.Logger.Info("ClickHouse连接池配置成功")

	// 测试数据库连接
	if err := nativeConn.Ping(context.Background()); err != nil {
		logging.Logger.WithError(err).Error("ClickHouse连接测试失败")
		return nil, fmt.Errorf("ClickHouse连接测试失败: %w", err)
	}

	// 测试查询
	var testResult []map[string]interface{}
	rows, err := sqlDB.Query("SELECT * FROM connection_test")
	if err != nil {
		logging.Logger.WithError(err).Warn("ClickHouse测试查询失败")
	} else {
		defer rows.Close()
		for rows.Next() {
			var id uint32
			var testTime time.Time
			if err := rows.Scan(&id, &testTime); err == nil {
				testResult = append(testResult, map[string]interface{}{
					"id":        id,
					"test_time": testTime,
				})
			}
		}
		logging.Logger.WithField("result", testResult).Info("ClickHouse测试成功")
	}

	return &ClickHouseStore{
		conn:          nativeConn,
		db:            sqlDB,
		inputChan:     input,
		createdTables: make(map[string]bool),
		tablesMu:      sync.RWMutex{},
		idCounter:     1,
		idMu:          sync.Mutex{},
	}, nil
}

func (s *ClickHouseStore) Start(ctx context.Context) {
	go func() {
		count := 0
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		logging.Logger.Info("ClickHouse存储服务启动，等待数据...")

		// 添加调试信息，每10秒检查一次通道状态
		debugTicker := time.NewTicker(10 * time.Second)
		defer debugTicker.Stop()

		// 创建重试队列
		var retryQueue []*types.KLineData
		const maxRetryQueueSize = 5000
		retryTicker := time.NewTicker(5 * time.Second)
		defer retryTicker.Stop()

		// 连接状态
		isConnected := true
		reconnectTimer := time.NewTimer(10 * time.Second)
		defer reconnectTimer.Stop()
		if !reconnectTimer.Stop() {
			<-reconnectTimer.C
		}

		for {
			select {
			case <-ctx.Done():
				// 关闭数据库连接
				if s.conn != nil {
					s.conn.Close()
				}
				if s.db != nil {
					s.db.Close()
				}
				logging.Logger.Info("ClickHouse连接已关闭")
				return
			case data := <-s.inputChan:
				logging.Logger.WithFields(logrus.Fields{
					"symbol":    data.Symbol,
					"interval":  data.Interval,
					"open_time": data.OpenTime,
				}).Debug("收到K线数据")

				count++
				if !isConnected {
					// 数据库连接断开，加入重试队列
					if len(retryQueue) < maxRetryQueueSize {
						retryQueue = append(retryQueue, data)
						logging.Logger.WithField("queue_size", len(retryQueue)).Debug("添加到重试队列")
					} else {
						logging.Logger.Warn("重试队列已满，丢弃数据")
					}
				} else if err := s.writeDataPoint(data); err != nil {
					// 写入失败，检查是否是连接问题
					if isConnectionError(err) {
						isConnected = false
						// 将当前失败的数据加入重试队列
						if len(retryQueue) < maxRetryQueueSize {
							retryQueue = append(retryQueue, data)
						}
						logging.Logger.WithError(err).Error("数据库连接断开，启动重连流程")
						reconnectTimer.Reset(10 * time.Second)
					} else {
						logging.Logger.WithError(err).Error("写入数据失败")
					}
				}

			case <-ticker.C:
				logging.Logger.WithField("count", count).Info("已处理数据点数量")

			case <-debugTicker.C:
				logging.Logger.WithFields(logrus.Fields{
					"processed_count": count,
					"queue_size":      len(retryQueue),
					"connected":       isConnected,
				}).Debug("ClickHouse存储状态")

			case <-retryTicker.C:
				if !isConnected {
					// 尝试重连
					if err := s.checkConnection(); err == nil {
						isConnected = true
						logging.Logger.Info("数据库重连成功")
					}
				}

				// 处理重试队列
				if isConnected && len(retryQueue) > 0 {
					// 批量处理重试队列中的数据
					batchSize := 100
					processed := 0
					for i := 0; i < len(retryQueue) && processed < batchSize; i++ {
						if err := s.writeDataPoint(retryQueue[i]); err != nil {
							if isConnectionError(err) {
								isConnected = false
								break
							}
							logging.Logger.WithError(err).Warn("重试数据写入失败")
						}
						processed++
					}

					if processed > 0 {
						retryQueue = retryQueue[processed:]
						logging.Logger.WithFields(logrus.Fields{
							"processed":    processed,
							"remaining":    len(retryQueue),
							"queue_size":   len(retryQueue),
						}).Info("处理重试队列")
					}
				}

			case <-reconnectTimer.C:
				if !isConnected {
					if err := s.checkConnection(); err == nil {
						isConnected = true
						logging.Logger.Info("数据库重连成功")
					} else {
						logging.Logger.WithError(err).Warn("数据库重连失败，10秒后重试")
						reconnectTimer.Reset(10 * time.Second)
					}
				}
			}
		}
	}()
}

func (s *ClickHouseStore) checkConnection() error {
	return s.conn.Ping(context.Background())
}

func (s *ClickHouseStore) writeDataPoint(data *types.KLineData) error {
	if data == nil {
		return fmt.Errorf("数据为空")
	}

	// 确保表存在 - 使用统一的kline表
	tableName := "kline"
	if err := s.ensureTableExists(tableName); err != nil {
		return fmt.Errorf("确保表存在失败: %w", err)
	}

	// 生成唯一ID
	s.idMu.Lock()
	id := s.idCounter
	s.idCounter++
	s.idMu.Unlock()

	// 使用批量插入提高性能
	batch, err := s.conn.PrepareBatch(context.Background(), fmt.Sprintf(`
		INSERT INTO %s (id, symbol, interval_type, open_time, close_time, open_price, high_price, low_price, close_price, volume, created_at, updated_at)
	`, tableName))
	if err != nil {
		return fmt.Errorf("准备批量插入失败: %w", err)
	}

	now := time.Now()
	// 使用兼容性字段，优先使用新字段
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
		id,
		data.Symbol,
		data.Interval,
		data.OpenTime,
		data.CloseTime,
		openPrice,
		highPrice,
		lowPrice,
		closePrice,
		data.Volume,
		now,
		now,
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
	}).Debug("成功写入K线数据")

	return nil
}

func (s *ClickHouseStore) ensureTableExists(tableName string) error {
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
		// 创建表 - 使用与主表相同的结构
		createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s
		(
			id UInt64,
			symbol String,
			interval_type String,
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
		ORDER BY (interval_type, open_time)
		SETTINGS index_granularity = 8192
		`, tableName)

		if err := s.conn.Exec(context.Background(), createTableSQL); err != nil {
			return fmt.Errorf("创建表 %s 失败: %w", tableName, err)
		}

		logging.Logger.WithField("table", tableName).Info("成功创建ClickHouse表")
	}

	// 缓存表存在状态
	s.tablesMu.Lock()
	s.createdTables[tableName] = true
	s.tablesMu.Unlock()

	return nil
}

// QueryHistoryData 查询历史数据
func (s *ClickHouseStore) QueryHistoryData(ctx context.Context, symbol, start, end, pageSize, pageToken string) ([]*types.KLineData, string, error) {
	tableName := "kline"
	
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

	// 构建查询SQL
	var query string
	var args []interface{}

	if start != "" && end != "" {
		query = fmt.Sprintf(`
			SELECT symbol, interval_type, open_time, close_time, open_price, high_price, low_price, close_price, volume
			FROM %s 
			WHERE symbol = ? AND open_time >= ? AND open_time <= ?
			ORDER BY open_time ASC 
			LIMIT ? OFFSET ?
		`, tableName)
		
		startTime, err := time.Parse("2006-01-02 15:04:05", start)
		if err != nil {
			return nil, "", fmt.Errorf("解析开始时间失败: %w", err)
		}
		endTime, err := time.Parse("2006-01-02 15:04:05", end)
		if err != nil {
			return nil, "", fmt.Errorf("解析结束时间失败: %w", err)
		}
		
		args = []interface{}{symbol, startTime, endTime, limit, offset}
	} else {
		query = fmt.Sprintf(`
			SELECT symbol, interval_type, open_time, close_time, open_price, high_price, low_price, close_price, volume
			FROM %s 
			WHERE symbol = ?
			ORDER BY open_time DESC 
			LIMIT ? OFFSET ?
		`, tableName)
		args = []interface{}{symbol, limit, offset}
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("查询历史数据失败: %w", err)
	}
	defer rows.Close()

	var results []*types.KLineData
	for rows.Next() {
		var kline types.KLineData
		err := rows.Scan(
			&kline.Symbol,
			&kline.Interval,
			&kline.OpenTime,
			&kline.CloseTime,
			&kline.Open,
			&kline.High,
			&kline.Low,
			&kline.Close,
			&kline.Volume,
		)
		if err != nil {
			return nil, "", fmt.Errorf("扫描K线数据失败: %w", err)
		}
		results = append(results, &kline)
	}

	// 计算下一页token
	nextPageToken := ""
	if len(results) == limit {
		nextPageToken = strconv.Itoa(offset + limit)
	}

	return results, nextPageToken, nil
}

// QueryKlines 查询K线数据
func (s *ClickHouseStore) QueryKlines(ctx context.Context, symbol string, interval string, limit int) ([]*types.KLineData, error) {
	// ClickHouse使用统一的kline表存储所有数据，不像MySQL为每个交易对创建单独的表
	tableName := "kline"

	query := fmt.Sprintf(`
		SELECT symbol, interval_type, open_time, close_time, open_price, high_price, low_price, close_price, volume
		FROM %s 
		WHERE symbol = ? AND interval_type = ?
		ORDER BY open_time DESC 
		LIMIT ?
	`, tableName)

	rows, err := s.db.QueryContext(ctx, query, symbol, interval, limit)
	if err != nil {
		return nil, fmt.Errorf("查询K线数据失败: %w", err)
	}
	defer rows.Close()

	var results []*types.KLineData
	for rows.Next() {
		var kline types.KLineData
		err := rows.Scan(
			&kline.Symbol,
			&kline.Interval,
			&kline.OpenTime,
			&kline.CloseTime,
			&kline.Open,
			&kline.High,
			&kline.Low,
			&kline.Close,
			&kline.Volume,
		)
		if err != nil {
			return nil, fmt.Errorf("扫描K线数据失败: %w", err)
		}
		results = append(results, &kline)
	}

	return results, nil
}

// QueryHistoryKlines 查询历史K线数据
func (s *ClickHouseStore) QueryHistoryKlines(ctx context.Context, symbol string, interval string, startTime time.Time, endTime time.Time) ([]*types.KLineData, error) {
	tableName := "kline"

	query := fmt.Sprintf(`
		SELECT symbol, interval_type, open_time, close_time, open_price, high_price, low_price, close_price, volume
		FROM %s 
		WHERE symbol = ? AND interval_type = ? AND open_time >= ? AND open_time <= ?
		ORDER BY open_time ASC
	`, tableName)

	rows, err := s.db.QueryContext(ctx, query, symbol, interval, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("查询历史K线数据失败: %w", err)
	}
	defer rows.Close()

	var results []*types.KLineData
	for rows.Next() {
		var kline types.KLineData
		err := rows.Scan(
			&kline.Symbol,
			&kline.Interval,
			&kline.OpenTime,
			&kline.CloseTime,
			&kline.Open,
			&kline.High,
			&kline.Low,
			&kline.Close,
			&kline.Volume,
		)
		if err != nil {
			return nil, fmt.Errorf("扫描K线数据失败: %w", err)
		}
		results = append(results, &kline)
	}

	return results, nil
}

// GetAvailableSymbols 获取可用的币种
func (s *ClickHouseStore) GetAvailableSymbols(ctx context.Context) ([]map[string]interface{}, error) {
	query := `
		SELECT DISTINCT symbol
		FROM system.tables 
		WHERE database = 'data4trend' AND name LIKE '%usdt'
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("查询可用币种失败: %w", err)
	}
	defer rows.Close()

	var symbols []map[string]interface{}
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}
		
		// 从表名获取币种符号
		symbol := getSymbolFromTableName(tableName)
		if symbol != "" {
			symbols = append(symbols, map[string]interface{}{
				"symbol": symbol,
				"table":  tableName,
			})
		}
	}

	return symbols, nil
}

// GetStats 获取统计信息
func (s *ClickHouseStore) GetStats(ctx context.Context) (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	// 获取总记录数
	var totalRecords uint64
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM kline").Scan(&totalRecords)
	if err != nil {
		logging.Logger.WithError(err).Warn("获取总记录数失败")
	} else {
		stats["total_records"] = totalRecords
	}

	// 获取币种数量
	var symbolCount uint64
	err = s.db.QueryRowContext(ctx, "SELECT COUNT(DISTINCT symbol) FROM kline").Scan(&symbolCount)
	if err != nil {
		logging.Logger.WithError(err).Warn("获取币种数量失败")
	} else {
		stats["symbol_count"] = symbolCount
	}

	// 获取最新数据时间
	var latestTime time.Time
	err = s.db.QueryRowContext(ctx, "SELECT MAX(open_time) FROM kline").Scan(&latestTime)
	if err != nil {
		logging.Logger.WithError(err).Warn("获取最新数据时间失败")
	} else {
		stats["latest_time"] = latestTime
	}

	// 获取数据库大小信息
	var dbSize uint64
	err = s.db.QueryRowContext(ctx, `
		SELECT SUM(bytes_on_disk) 
		FROM system.parts 
		WHERE database = 'data4trend'
	`).Scan(&dbSize)
	if err != nil {
		logging.Logger.WithError(err).Warn("获取数据库大小失败")
	} else {
		stats["database_size_bytes"] = dbSize
	}

	return stats, nil
}

// SetInputChannel 设置输入通道
func (s *ClickHouseStore) SetInputChannel(input <-chan *types.KLineData) {
	s.inputChan = input
}

// SaveKLineData 保存K线数据
func (s *ClickHouseStore) SaveKLineData(ctx context.Context, data *types.KLineData) error {
	return s.writeDataPoint(data)
}

// DeleteKLinesInTimeRange 删除指定时间范围内的K线数据
func (s *ClickHouseStore) DeleteKLinesInTimeRange(ctx context.Context, symbol string, interval string, startTime time.Time, endTime time.Time) error {
	tableName := "kline"

	query := fmt.Sprintf(`
		DELETE FROM %s 
		WHERE symbol = ? AND interval_type = ? AND open_time >= ? AND open_time <= ?
	`, tableName)

	err := s.conn.Exec(ctx, query, symbol, interval, startTime, endTime)
	if err != nil {
		return fmt.Errorf("删除K线数据失败: %w", err)
	}

	return nil
}

// CheckDataGaps 检查数据缺口
func (s *ClickHouseStore) CheckDataGaps(ctx context.Context, symbol, interval string, startTime, endTime time.Time) ([]types.DataGap, error) {
	tableName := "kline"
	
	// 计算间隔持续时间
	intervalDuration, err := calculateIntervalDuration(interval)
	if err != nil {
		return nil, fmt.Errorf("不支持的间隔类型: %s, %w", interval, err)
	}

	query := fmt.Sprintf(`
		WITH expected_times AS (
			SELECT arrayJoin(range(toUnixTimestamp(?), toUnixTimestamp(?), %d)) * 1000 as expected_timestamp
		),
		actual_times AS (
			SELECT DISTINCT toUnixTimestamp(open_time) * 1000 as actual_timestamp
			FROM %s
			WHERE symbol = ? AND interval_type = ? 
			AND open_time >= ? AND open_time <= ?
		)
		SELECT expected_timestamp
		FROM expected_times
		LEFT JOIN actual_times ON expected_times.expected_timestamp = actual_times.actual_timestamp
		WHERE actual_times.actual_timestamp IS NULL
		ORDER BY expected_timestamp
	`, int64(intervalDuration.Seconds()), tableName)

	rows, err := s.db.QueryContext(ctx, query, startTime, endTime, symbol, interval, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("查询数据缺口失败: %w", err)
	}
	defer rows.Close()

	var gaps []types.DataGap
	var currentGapStart *time.Time
	var lastTimestamp int64

	for rows.Next() {
		var timestamp int64
		if err := rows.Scan(&timestamp); err != nil {
			continue
		}

		gapTime := time.Unix(timestamp/1000, 0)

		if currentGapStart == nil {
			// 开始新的缺口
			currentGapStart = &gapTime
		} else if timestamp-lastTimestamp > int64(intervalDuration.Seconds()*1000) {
			// 缺口结束，添加到结果中
			gaps = append(gaps, types.DataGap{
				Symbol:    symbol,
				Interval:  interval,
				StartTime: *currentGapStart,
				EndTime:   time.Unix(lastTimestamp/1000, 0),
				MissingCount: 1,
			})
			currentGapStart = &gapTime
		}

		lastTimestamp = timestamp
	}

	// 处理最后一个缺口
	if currentGapStart != nil {
		gaps = append(gaps, types.DataGap{
			Symbol:    symbol,
			Interval:  interval,
			StartTime: *currentGapStart,
			EndTime:   time.Unix(lastTimestamp/1000, 0),
			MissingCount: 1,
		})
	}

	return gaps, nil
}

// FixDataGap 修复数据缺口
func (s *ClickHouseStore) FixDataGap(ctx context.Context, symbol, interval string, startTime, endTime time.Time) error {
	// 这里应该调用外部数据源来获取缺失的数据
	// 具体实现依赖于数据获取逻辑
	logging.Logger.WithFields(logrus.Fields{
		"symbol":     symbol,
		"interval":   interval,
		"start_time": startTime,
		"end_time":   endTime,
	}).Info("修复数据缺口请求")

	return fmt.Errorf("修复数据缺口功能需要与数据收集器集成")
}

// GetDB 获取数据库连接
func (s *ClickHouseStore) GetDB() *sql.DB {
	return s.db
}

// GetConn 获取ClickHouse原生连接
func (s *ClickHouseStore) GetConn() clickhouse.Conn {
	return s.conn
}

// isConnectionError 检查错误是否为连接错误
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	
	errStr := err.Error()
	// 检查常见的连接错误关键词
	connectionErrors := []string{
		"connection refused",
		"connection reset",
		"network is unreachable",
		"broken pipe",
		"context deadline exceeded",
		"no such host",
		"timeout",
	}
	
	for _, connErr := range connectionErrors {
		if strings.Contains(strings.ToLower(errStr), connErr) {
			return true
		}
	}
	
	return false
}

// getSymbolFromTableName 从表名提取交易对符号（在ClickHouse中，我们使用统一表格，所以这个函数返回输入值）
func getSymbolFromTableName(tableName string) string {
	// 在ClickHouse实现中，我们使用统一的kline表，所以symbol是数据字段而不是表名
	// 这个函数主要用于兼容性，实际上在ClickHouse中不需要
	return tableName
}

// calculateIntervalDuration 计算时间间隔的持续时间
func calculateIntervalDuration(interval string) (time.Duration, error) {
	switch interval {
	case "1m":
		return 1 * time.Minute, nil
	case "3m":
		return 3 * time.Minute, nil
	case "5m":
		return 5 * time.Minute, nil
	case "15m":
		return 15 * time.Minute, nil
	case "30m":
		return 30 * time.Minute, nil
	case "1h":
		return 1 * time.Hour, nil
	case "2h":
		return 2 * time.Hour, nil
	case "4h":
		return 4 * time.Hour, nil
	case "6h":
		return 6 * time.Hour, nil
	case "8h":
		return 8 * time.Hour, nil
	case "12h":
		return 12 * time.Hour, nil
	case "1d":
		return 24 * time.Hour, nil
	case "3d":
		return 72 * time.Hour, nil
	case "1w":
		return 7 * 24 * time.Hour, nil
	case "1M":
		return 30 * 24 * time.Hour, nil // 近似值
	default:
		return 0, fmt.Errorf("未知的时间间隔: %s", interval)
	}
}



 