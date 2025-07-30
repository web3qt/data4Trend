package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/sirupsen/logrus"

	"data4trend/internal/types"
	"data4trend/pkg/config"
)

// ClickHouseStorage represents ClickHouse storage implementation
type ClickHouseStorage struct {
	conn   driver.Conn
	config *config.Config
	logger *logrus.Logger
}

// NewClickHouseStorage creates a new ClickHouse storage instance
func NewClickHouseStorage(cfg *config.Config, logger *logrus.Logger) (*ClickHouseStorage, error) {
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Database.Host, cfg.Database.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database.Database,
			Username: cfg.Database.Username,
			Password: cfg.Database.Password,
		},
		Protocol:         clickhouse.HTTP,
		DialTimeout:      time.Second * 30,
		MaxOpenConns:     10,
		MaxIdleConns:     5,
		ConnMaxLifetime:  time.Hour,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	storage := &ClickHouseStorage{
		conn:   conn,
		config: cfg,
		logger: logger,
	}

	// Initialize database and table
	if err := storage.initializeDatabase(); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	return storage, nil
}

// initializeDatabase creates database and table if they don't exist
func (s *ClickHouseStorage) initializeDatabase() error {
	ctx := context.Background()

	// Create database if not exists
	createDBQuery := fmt.Sprintf(`
		CREATE DATABASE IF NOT EXISTS %s
	`, s.config.Database.Database)

	if err := s.conn.Exec(ctx, createDBQuery); err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Create table if not exists
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			symbol String,
			open_time Int64,
			close_time Int64,
			open String,
			high String,
			low String,
			close String,
			volume String,
			created_at DateTime DEFAULT now()
		) ENGINE = MergeTree()
		ORDER BY (symbol, open_time)
		PARTITION BY toYYYYMM(toDateTime(open_time / 1000))
	`, s.config.Database.Database, s.config.Database.Table)

	if err := s.conn.Exec(ctx, createTableQuery); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	s.logger.Info("Database and table initialized successfully")
	return nil
}

// InsertKlineData inserts kline data into ClickHouse
func (s *ClickHouseStorage) InsertKlineData(data *types.KlineData) error {
	ctx := context.Background()

	query := fmt.Sprintf(`
		INSERT INTO %s.%s 
		(symbol, open_time, close_time, open, high, low, close, volume, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, s.config.Database.Database, s.config.Database.Table)

	err := s.conn.Exec(ctx, query,
		data.Symbol,
		data.OpenTime,
		data.CloseTime,
		data.Open,
		data.High,
		data.Low,
		data.Close,
		data.Volume,
		data.CreatedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to insert kline data: %w", err)
	}

	return nil
}

// BatchInsertKlineData inserts multiple kline data records
func (s *ClickHouseStorage) BatchInsertKlineData(dataList []*types.KlineData) error {
	if len(dataList) == 0 {
		return nil
	}

	ctx := context.Background()

	batch, err := s.conn.PrepareBatch(ctx, fmt.Sprintf(`
		INSERT INTO %s.%s 
		(symbol, open_time, close_time, open, high, low, close, volume, created_at)
	`, s.config.Database.Database, s.config.Database.Table))

	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, data := range dataList {
		err := batch.Append(
			data.Symbol,
			data.OpenTime,
			data.CloseTime,
			data.Open,
			data.High,
			data.Low,
			data.Close,
			data.Volume,
			data.CreatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to append to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	s.logger.Infof("Successfully inserted %d kline records", len(dataList))
	return nil
}

// GetKlineData retrieves kline data from ClickHouse
func (s *ClickHouseStorage) GetKlineData(symbol string, limit int, startTime, endTime *time.Time) ([]*types.KlineData, error) {
	ctx := context.Background()

	query := fmt.Sprintf(`
		SELECT symbol, open_time, close_time, open, high, low, close, volume, created_at
		FROM %s.%s
		WHERE symbol = ?
	`, s.config.Database.Database, s.config.Database.Table)

	args := []interface{}{symbol}

	if startTime != nil {
		query += " AND open_time >= ?"
		args = append(args, startTime.UnixMilli())
	}

	if endTime != nil {
		query += " AND open_time <= ?"
		args = append(args, endTime.UnixMilli())
	}

	query += " ORDER BY open_time DESC"

	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query kline data: %w", err)
	}
	defer rows.Close()

	var result []*types.KlineData
	for rows.Next() {
		var data types.KlineData
		err := rows.Scan(
			&data.Symbol,
			&data.OpenTime,
			&data.CloseTime,
			&data.Open,
			&data.High,
			&data.Low,
			&data.Close,
			&data.Volume,
			&data.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		result = append(result, &data)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return result, nil
}

// GetDuplicateRecords checks for duplicate records in the last 24 hours
func (s *ClickHouseStorage) GetDuplicateRecords() (map[string]int, error) {
	ctx := context.Background()
	result := make(map[string]int)
	
	query := fmt.Sprintf(`
		SELECT 
			symbol,
			COUNT(*) as duplicate_count
		FROM (
			SELECT 
				symbol, 
				open_time,
				COUNT(*) as cnt
			FROM %s.%s 
			WHERE created_at >= now() - INTERVAL 24 HOUR
			GROUP BY symbol, open_time
			HAVING cnt > 1
		) duplicates
		GROUP BY symbol
	`, s.config.Database.Database, s.config.Database.Table)
	
	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to check duplicates: %w", err)
	}
	defer rows.Close()
	
	for rows.Next() {
		var symbol string
		var count int
		if err := rows.Scan(&symbol, &count); err != nil {
			continue
		}
		result[symbol] = count
	}
	
	return result, nil
}

// GetStaleDataSymbols returns symbols with stale data (no updates in last 5 minutes)
func (s *ClickHouseStorage) GetStaleDataSymbols() (map[string]time.Duration, error) {
	ctx := context.Background()
	result := make(map[string]time.Duration)
	
	query := fmt.Sprintf(`
		SELECT 
			symbol,
			MAX(created_at) as last_update,
			now() - MAX(created_at) as delay_seconds
		FROM %s.%s 
		GROUP BY symbol
		HAVING delay_seconds > 300
	`, s.config.Database.Database, s.config.Database.Table)
	
	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to check stale data: %w", err)
	}
	defer rows.Close()
	
	for rows.Next() {
		var symbol string
		var lastUpdate time.Time
		var delaySeconds int64
		if err := rows.Scan(&symbol, &lastUpdate, &delaySeconds); err != nil {
			continue
		}
		result[symbol] = time.Duration(delaySeconds) * time.Second
	}
	
	return result, nil
}

// GetAnomalousData detects anomalous data points (extreme price movements)
func (s *ClickHouseStorage) GetAnomalousData() ([]map[string]interface{}, error) {
	ctx := context.Background()
	result := []map[string]interface{}{}
	
	// Check for extreme price movements (>50% change in 1 minute)
	query := fmt.Sprintf(`
		SELECT 
			symbol,
			open_time,
			open_price,
			close_price,
			(close_price - open_price) / open_price * 100 as price_change_pct
		FROM %s.%s 
		WHERE created_at >= now() - INTERVAL 24 HOUR
			AND abs((close_price - open_price) / open_price * 100) > 50
		ORDER BY abs(price_change_pct) DESC
		LIMIT 100
	`, s.config.Database.Database, s.config.Database.Table)
	
	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to check anomalous data: %w", err)
	}
	defer rows.Close()
	
	for rows.Next() {
		var symbol string
		var openTime int64
		var openPrice, closePrice, priceChangePct float64
		if err := rows.Scan(&symbol, &openTime, &openPrice, &closePrice, &priceChangePct); err != nil {
			continue
		}
		
		result = append(result, map[string]interface{}{
			"symbol":           symbol,
			"timestamp":        time.Unix(openTime/1000, 0),
			"open_price":       openPrice,
			"close_price":      closePrice,
			"price_change_pct": priceChangePct,
			"description":      fmt.Sprintf("Extreme price movement: %.2f%%", priceChangePct),
		})
	}
	
	return result, nil
}

// StoreValidationResult stores validation results to database
func (s *ClickHouseStorage) StoreValidationResult(timestamp time.Time, overallStatus string, totalSymbols, healthySymbols int, completenessScore, accuracyScore, consistencyScore, timelinessScore, overallScore float64, issuesCount int) error {
	ctx := context.Background()
	
	query := fmt.Sprintf(`
		INSERT INTO %s.data_quality_metrics (
			timestamp, overall_status, total_symbols, healthy_symbols,
			completeness_score, accuracy_score, consistency_score, 
			timeliness_score, overall_score, issues_count
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, s.config.Database.Database)
	
	err := s.conn.Exec(ctx, query, timestamp, overallStatus, totalSymbols, healthySymbols,
		completenessScore, accuracyScore, consistencyScore, timelinessScore, overallScore, issuesCount)
	
	if err != nil {
		return fmt.Errorf("failed to store validation result: %w", err)
	}
	
	return nil
}

// GetStats returns database statistics
func (s *ClickHouseStorage) GetStats() (map[string]interface{}, error) {
	ctx := context.Background()

	stats := make(map[string]interface{})

	// Get total count
	countQuery := fmt.Sprintf("SELECT count() FROM %s.%s", s.config.Database.Database, s.config.Database.Table)
	var totalCount uint64
	if err := s.conn.QueryRow(ctx, countQuery).Scan(&totalCount); err != nil {
		s.logger.Warnf("Failed to get total count: %v", err)
		totalCount = 0
	}
	stats["total_records"] = totalCount

	// Get latest record time
	latestQuery := fmt.Sprintf("SELECT max(created_at) FROM %s.%s", s.config.Database.Database, s.config.Database.Table)
	var latestTime time.Time
	if err := s.conn.QueryRow(ctx, latestQuery).Scan(&latestTime); err != nil {
		s.logger.Warnf("Failed to get latest time: %v", err)
	} else {
		stats["latest_record_time"] = latestTime
	}

	// Get symbol count
	symbolQuery := fmt.Sprintf("SELECT count(DISTINCT symbol) FROM %s.%s", s.config.Database.Database, s.config.Database.Table)
	var symbolCount uint64
	if err := s.conn.QueryRow(ctx, symbolQuery).Scan(&symbolCount); err != nil {
		s.logger.Warnf("Failed to get symbol count: %v", err)
		symbolCount = 0
	}
	stats["unique_symbols"] = symbolCount

	return stats, nil
}

// Close closes the ClickHouse connection
func (s *ClickHouseStorage) Close() error {
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}

// TestConnection tests the database connection
func (s *ClickHouseStorage) TestConnection() error {
	return s.conn.Ping(context.Background())
}

// DataGap represents a gap in the data
type DataGap struct {
	Symbol    string    `json:"symbol"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	Missing   int       `json:"missing_count"`
}

// DetectDataGaps detects missing data gaps for a symbol within a time range
func (s *ClickHouseStorage) DetectDataGaps(symbol string, startTime, endTime time.Time) ([]*DataGap, error) {
	ctx := context.Background()
	gaps := []*DataGap{}

	// Query to find gaps in 1-minute intervals
	query := fmt.Sprintf(`
		WITH 
			time_series AS (
				SELECT toDateTime(number * 60 + toUnixTimestamp(toDateTime('%s'))) as expected_time
				FROM numbers(dateDiff('minute', toDateTime('%s'), toDateTime('%s')) + 1)
			),
			actual_data AS (
				SELECT DISTINCT toDateTime(toInt64(open_time) / 1000) as actual_time
				FROM %s.%s 
				WHERE symbol = '%s' 
					AND toDateTime(toInt64(open_time) / 1000) >= toDateTime('%s') 
					AND toDateTime(toInt64(open_time) / 1000) <= toDateTime('%s')
			)
		SELECT expected_time
		FROM time_series
		LEFT JOIN actual_data ON time_series.expected_time = actual_data.actual_time
		WHERE actual_data.actual_time IS NULL
		ORDER BY expected_time
	`, 
		startTime.Format("2006-01-02 15:04:05"),
		startTime.Format("2006-01-02 15:04:05"),
		endTime.Format("2006-01-02 15:04:05"),
		s.config.Database.Database, s.config.Database.Table,
		symbol,
		startTime.Format("2006-01-02 15:04:05"),
		endTime.Format("2006-01-02 15:04:05"))

	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to detect gaps: %w", err)
	}
	defer rows.Close()

	// Collect missing timestamps
	missingTimes := []time.Time{}
	for rows.Next() {
		var missingTime time.Time
		if err := rows.Scan(&missingTime); err != nil {
			continue
		}
		missingTimes = append(missingTimes, missingTime)
	}

	// Group consecutive missing times into gaps
	if len(missingTimes) == 0 {
		return gaps, nil
	}

	gapStart := missingTimes[0]
	gapEnd := missingTimes[0]
	count := 1

	for i := 1; i < len(missingTimes); i++ {
		// Check if this timestamp is consecutive (1 minute after previous)
		if missingTimes[i].Sub(missingTimes[i-1]) == time.Minute {
			gapEnd = missingTimes[i]
			count++
		} else {
			// Gap ended, create a DataGap
			gaps = append(gaps, &DataGap{
				Symbol:    symbol,
				StartTime: gapStart,
				EndTime:   gapEnd,
				Missing:   count,
			})
			// Start new gap
			gapStart = missingTimes[i]
			gapEnd = missingTimes[i]
			count = 1
		}
	}

	// Add the last gap
	gaps = append(gaps, &DataGap{
		Symbol:    symbol,
		StartTime: gapStart,
		EndTime:   gapEnd,
		Missing:   count,
	})

	return gaps, nil
}

// GetDataGapsForAllSymbols detects data gaps for all symbols in the last 24 hours
func (s *ClickHouseStorage) GetDataGapsForAllSymbols() (map[string][]*DataGap, error) {
	ctx := context.Background()
	result := make(map[string][]*DataGap)

	// Get all symbols
	symbolQuery := fmt.Sprintf("SELECT DISTINCT symbol FROM %s.%s WHERE created_at >= now() - INTERVAL 24 HOUR", 
		s.config.Database.Database, s.config.Database.Table)
	
	rows, err := s.conn.Query(ctx, symbolQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to get symbols: %w", err)
	}
	defer rows.Close()

	symbols := []string{}
	for rows.Next() {
		var symbol string
		if err := rows.Scan(&symbol); err != nil {
			continue
		}
		symbols = append(symbols, symbol)
	}

	// Check gaps for each symbol in the last 24 hours
	endTime := time.Now()
	startTime := endTime.Add(-24 * time.Hour)

	for _, symbol := range symbols {
		gaps, err := s.DetectDataGaps(symbol, startTime, endTime)
		if err != nil {
			s.logger.Warnf("Failed to detect gaps for %s: %v", symbol, err)
			continue
		}
		if len(gaps) > 0 {
			result[symbol] = gaps
		}
	}

	return result, nil
}