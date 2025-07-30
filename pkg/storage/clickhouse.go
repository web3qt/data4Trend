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