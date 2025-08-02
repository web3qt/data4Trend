package batchwriter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"data4trend/internal/types"
	"data4trend/pkg/config"
	"data4trend/pkg/storage"
)

// BatchWriter 处理K线数据到ClickHouse的批量写入
type BatchWriter struct {
	config    *config.Config
	logger    *logrus.Logger
	storage   *storage.ClickHouseStorage
	batch     []*types.KlineData
	batchSize int
	timeout   time.Duration
	ctx       context.Context
	cancel    context.CancelFunc
	mutex     sync.Mutex
	stats     *WriterStats
	timer     *time.Timer
}

// WriterStats 跟踪批量写入器统计信息
type WriterStats struct {
	BatchesWritten   int64     `json:"batches_written"`
	RecordsWritten   int64     `json:"records_written"`
	WriteErrors      int64     `json:"write_errors"`
	LastWriteTime    time.Time `json:"last_write_time"`
	CurrentBatchSize int       `json:"current_batch_size"`
	mutex            sync.RWMutex
}

// NewBatchWriter 创建新的批量写入器
func NewBatchWriter(cfg *config.Config, storage *storage.ClickHouseStorage, logger *logrus.Logger) (*BatchWriter, error) {
	timeout, err := time.ParseDuration(cfg.BatchWriter.BatchTimeout)
	if err != nil {
		timeout = 60 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	bw := &BatchWriter{
		config:    cfg,
		logger:    logger,
		storage:   storage,
		batch:     make([]*types.KlineData, 0, cfg.BatchWriter.BatchSize),
		batchSize: cfg.BatchWriter.BatchSize,
		timeout:   timeout,
		ctx:       ctx,
		cancel:    cancel,
		stats:     &WriterStats{},
	}

	// Initialize timer
	bw.timer = time.NewTimer(bw.timeout)
	bw.timer.Stop() // Stop initially

	return bw, nil
}

// Start starts the batch writer
func (bw *BatchWriter) Start() {
	bw.logger.Info("Starting batch writer...")

	go bw.timerRoutine()

	bw.logger.Info("Batch writer started")
}

// AddKlineData adds kline data to the batch
func (bw *BatchWriter) AddKlineData(klineData *types.KlineData) error {
	bw.mutex.Lock()
	defer bw.mutex.Unlock()

	// Add to batch
	bw.batch = append(bw.batch, klineData)
	bw.updateCurrentBatchSize(len(bw.batch))

	// Start timer if this is the first item
	if len(bw.batch) == 1 {
		bw.timer.Reset(bw.timeout)
		bw.logger.Debugf("Started batch timer for %s (timeout: %v)", klineData.Symbol, bw.timeout)
	}

	// Check if batch is full
	if len(bw.batch) >= bw.batchSize {
		bw.logger.Debugf("Batch full (%d/%d), flushing immediately", len(bw.batch), bw.batchSize)
		return bw.flushBatch()
	}

	bw.logger.Debugf("Added kline data to batch: %s (batch size: %d/%d)", 
		klineData.Symbol, len(bw.batch), bw.batchSize)

	return nil
}

// timerRoutine handles timeout-based batch flushing
func (bw *BatchWriter) timerRoutine() {
	for {
		select {
		case <-bw.ctx.Done():
			return
		case <-bw.timer.C:
			bw.mutex.Lock()
			if len(bw.batch) > 0 {
				if err := bw.flushBatch(); err != nil {
					bw.logger.Errorf("Failed to flush batch on timeout: %v", err)
				}
			}
			bw.mutex.Unlock()
		}
	}
}

// flushBatch writes the current batch to ClickHouse
// Note: This method should be called with mutex locked
func (bw *BatchWriter) flushBatch() error {
	if len(bw.batch) == 0 {
		return nil
	}

	startTime := time.Now()
	batchToWrite := make([]*types.KlineData, len(bw.batch))
	copy(batchToWrite, bw.batch)
	batchSize := len(bw.batch)

	// Clear the batch
	bw.batch = bw.batch[:0]
	bw.updateCurrentBatchSize(0)
	bw.timer.Stop()

	bw.logger.Infof("Flushing batch of %d records to ClickHouse...", batchSize)

	// Write to ClickHouse with retries
	retryInterval, err := time.ParseDuration(bw.config.BatchWriter.RetryInterval)
	if err != nil {
		retryInterval = 1 * time.Second
	}

	var lastErr error
	for attempt := 0; attempt < bw.config.BatchWriter.MaxRetries; attempt++ {
		if err := bw.storage.InsertKlineDataBatch(batchToWrite); err != nil {
			lastErr = err
			bw.logger.Warnf("Batch write attempt %d/%d failed: %v", attempt+1, bw.config.BatchWriter.MaxRetries, err)
			if attempt < bw.config.BatchWriter.MaxRetries-1 {
				bw.logger.Debugf("Retrying in %v...", retryInterval)
				time.Sleep(retryInterval)
			}
			continue
		}

		// Success
		duration := time.Since(startTime)
		bw.updateStats(int64(batchSize), true)
		bw.logger.Infof("Successfully wrote batch of %d records to ClickHouse in %v", batchSize, duration)
		return nil
	}

	// All retries failed
	duration := time.Since(startTime)
	bw.updateStats(0, false)
	bw.logger.Errorf("Failed to write batch of %d records after %d attempts in %v: %v", 
		batchSize, bw.config.BatchWriter.MaxRetries, duration, lastErr)
	return fmt.Errorf("failed to write batch after %d attempts: %w", bw.config.BatchWriter.MaxRetries, lastErr)
}

// FlushAll flushes any remaining data in the batch
func (bw *BatchWriter) FlushAll() error {
	bw.mutex.Lock()
	defer bw.mutex.Unlock()

	return bw.flushBatch()
}

// updateStats updates writer statistics
func (bw *BatchWriter) updateStats(recordsWritten int64, success bool) {
	bw.stats.mutex.Lock()
	defer bw.stats.mutex.Unlock()

	if success {
		bw.stats.BatchesWritten++
		bw.stats.RecordsWritten += recordsWritten
		bw.stats.LastWriteTime = time.Now()
	} else {
		bw.stats.WriteErrors++
	}
}

// updateCurrentBatchSize updates current batch size in stats
func (bw *BatchWriter) updateCurrentBatchSize(size int) {
	bw.stats.mutex.Lock()
	defer bw.stats.mutex.Unlock()
	bw.stats.CurrentBatchSize = size
}

// GetStats returns writer statistics
func (bw *BatchWriter) GetStats() *WriterStats {
	bw.stats.mutex.RLock()
	defer bw.stats.mutex.RUnlock()

	return &WriterStats{
		BatchesWritten:   bw.stats.BatchesWritten,
		RecordsWritten:   bw.stats.RecordsWritten,
		WriteErrors:      bw.stats.WriteErrors,
		LastWriteTime:    bw.stats.LastWriteTime,
		CurrentBatchSize: bw.stats.CurrentBatchSize,
	}
}

// Stop stops the batch writer and flushes any remaining data
func (bw *BatchWriter) Stop() error {
	bw.logger.Info("Stopping batch writer...")

	// Cancel context to stop timer routine
	bw.cancel()

	// Flush any remaining data
	if err := bw.FlushAll(); err != nil {
		bw.logger.Errorf("Failed to flush remaining data: %v", err)
		return err
	}

	bw.logger.Info("Batch writer stopped")
	return nil
}