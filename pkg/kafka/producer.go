package kafka

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"

	"data4trend/internal/types"
	"data4trend/pkg/config"
)

// Producer represents a Kafka producer for kline data
type Producer struct {
	config   *config.Config
	logger   *logrus.Logger
	producer sarama.AsyncProducer
	topic    string
	stats    *ProducerStats
}

// ProducerStats tracks producer statistics
type ProducerStats struct {
	MessagesSent   int64     `json:"messages_sent"`
	MessagesErrors int64     `json:"messages_errors"`
	LastSentTime   time.Time `json:"last_sent_time"`
}

// NewProducer creates a new Kafka producer
func NewProducer(cfg *config.Config, logger *logrus.Logger) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	config.Producer.Retry.Backoff = 100 * time.Millisecond

	// Set compression
	switch cfg.Kafka.Producer.Compression {
	case "gzip":
		config.Producer.Compression = sarama.CompressionGZIP
	case "snappy":
		config.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		config.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		config.Producer.Compression = sarama.CompressionZSTD
	default:
		config.Producer.Compression = sarama.CompressionNone
	}

	// Set batch configuration
	if cfg.Kafka.Producer.BatchSize > 0 {
		config.Producer.Flush.Messages = cfg.Kafka.Producer.BatchSize
	}

	batchTimeout, err := time.ParseDuration(cfg.Kafka.Producer.BatchTimeout)
	if err != nil {
		batchTimeout = 1 * time.Second
	}
	config.Producer.Flush.Frequency = batchTimeout

	if cfg.Kafka.Producer.MaxMessageBytes > 0 {
		config.Producer.MaxMessageBytes = cfg.Kafka.Producer.MaxMessageBytes
	}

	producer, err := sarama.NewAsyncProducer(cfg.Kafka.Brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	p := &Producer{
		config:   cfg,
		logger:   logger,
		producer: producer,
		topic:    cfg.Kafka.Topic,
		stats:    &ProducerStats{},
	}

	// Start goroutines to handle success and error messages
	go p.handleSuccesses()
	go p.handleErrors()

	return p, nil
}

// SendKlineData sends kline data to Kafka
func (p *Producer) SendKlineData(klineData *types.KlineData) error {
	messageBytes, err := json.Marshal(klineData)
	if err != nil {
		return fmt.Errorf("failed to marshal kline data: %w", err)
	}

	message := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(klineData.Symbol),
		Value: sarama.ByteEncoder(messageBytes),
		Timestamp: time.Now(),
	}

	select {
	case p.producer.Input() <- message:
		p.logger.Debugf("Sent kline data to Kafka: %s", klineData.Symbol)
		return nil
	default:
		return fmt.Errorf("kafka producer input channel is full")
	}
}

// handleSuccesses handles successful message deliveries
func (p *Producer) handleSuccesses() {
	for success := range p.producer.Successes() {
		p.stats.MessagesSent++
		p.stats.LastSentTime = time.Now()
		p.logger.Debugf("Message sent successfully to partition %d offset %d", 
			success.Partition, success.Offset)
	}
}

// handleErrors handles message delivery errors
func (p *Producer) handleErrors() {
	for err := range p.producer.Errors() {
		p.stats.MessagesErrors++
		p.logger.Errorf("Failed to send message to Kafka: %v", err.Err)
	}
}

// GetStats returns producer statistics
func (p *Producer) GetStats() *ProducerStats {
	return p.stats
}

// Close closes the producer
func (p *Producer) Close() error {
	if err := p.producer.Close(); err != nil {
		return fmt.Errorf("failed to close Kafka producer: %w", err)
	}
	p.logger.Info("Kafka producer closed")
	return nil
}