package validator

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"data4trend/pkg/config"
	"data4trend/pkg/storage"
)

func TestNewValidatorService(t *testing.T) {
	cfg := &config.Config{
		Validator: config.ValidatorConfig{
			Enabled:           true,
			CheckInterval:     "5m",
			MaxGapDuration:    "24h",
			HistoryDays:       7,
			BatchSize:         100,
			ConcurrentWorkers: 3,
		},
	}

	logger := logrus.New()

	validator := NewValidatorService(cfg, nil, nil, logger)

	assert.NotNil(t, validator)
	assert.Equal(t, cfg, validator.config)
	assert.Equal(t, logger, validator.logger)
	assert.NotNil(t, validator.validationConfig)
	assert.Equal(t, 5*time.Minute, validator.validationConfig.CheckInterval)
	assert.Equal(t, 24*time.Hour, validator.validationConfig.MaxGapDuration)
	assert.Equal(t, 7, validator.validationConfig.HistoryDays)
	assert.Equal(t, 100, validator.validationConfig.BatchSize)
	assert.Equal(t, 3, validator.validationConfig.ConcurrentWorkers)
	assert.True(t, validator.validationConfig.Enabled)
}

func TestValidatorService_Start_Stop(t *testing.T) {
	cfg := &config.Config{
		Validator: config.ValidatorConfig{
			Enabled:           true,
			CheckInterval:     "100ms", // Short interval for testing
			MaxGapDuration:    "1h",
			HistoryDays:       1,
			BatchSize:         10,
			ConcurrentWorkers: 1,
		},
	}

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel) // Reduce log noise in tests

	validator := NewValidatorService(cfg, nil, nil, logger)

	// Test Start
	validator.Start()
	assert.True(t, validator.IsRunning())

	// Wait a bit to ensure the service is running
	time.Sleep(50 * time.Millisecond)

	// Test Stop
	validator.Stop()
	assert.False(t, validator.IsRunning())
}

func TestValidatorService_shouldFixGap(t *testing.T) {
	cfg := &config.Config{
		Validator: config.ValidatorConfig{
			Enabled:           true,
			CheckInterval:     "5m",
			MaxGapDuration:    "1h",
			HistoryDays:       7,
			BatchSize:         100,
			ConcurrentWorkers: 3,
		},
	}

	validator := NewValidatorService(cfg, nil, nil, logrus.New())

	now := time.Now()

	// Test gap within acceptable duration
	smallGap := &storage.DataGap{
		Symbol:    "BTCUSDT",
		StartTime: now.Add(-30 * time.Minute),
		EndTime:   now.Add(-25 * time.Minute),
		Missing:   5,
	}
	assert.True(t, validator.shouldFixGap(smallGap))

	// Test gap exceeding maximum duration
	largeGap := &storage.DataGap{
		Symbol:    "BTCUSDT",
		StartTime: now.Add(-2 * time.Hour),
		EndTime:   now.Add(-1 * time.Hour),
		Missing:   60,
	}
	assert.False(t, validator.shouldFixGap(largeGap))

	// Test very recent gap (too recent to fix - within 5 minutes)
	recentGap := &storage.DataGap{
		Symbol:    "BTCUSDT",
		StartTime: now.Add(-2 * time.Minute),
		EndTime:   now.Add(-1 * time.Minute), // EndTime is 1 minute ago, within 5 minute threshold
		Missing:   1,
	}
	assert.False(t, validator.shouldFixGap(recentGap))
}

func TestValidatorService_GetStats(t *testing.T) {
	cfg := &config.Config{
		Validator: config.ValidatorConfig{
			Enabled:           true,
			CheckInterval:     "5m",
			MaxGapDuration:    "24h",
			HistoryDays:       7,
			BatchSize:         100,
			ConcurrentWorkers: 3,
		},
	}

	validator := NewValidatorService(cfg, nil, nil, logrus.New())

	stats := validator.GetStats()
	assert.NotNil(t, stats)
	assert.Equal(t, int64(0), stats.TotalChecks)
	assert.Equal(t, int64(0), stats.GapsDetected)
	assert.Equal(t, int64(0), stats.GapsFixed)
	assert.Equal(t, int64(0), stats.BackfillErrors)
	assert.Equal(t, 0, stats.SymbolsChecked)
	assert.Equal(t, 0, stats.TotalMissingMinutes)
	assert.Equal(t, 0, stats.ContinuousDays)
}