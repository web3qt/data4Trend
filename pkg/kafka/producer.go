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

// Producer 代表用于K线数据的Kafka生产者
type Producer struct {
	config   *config.Config
	logger   *logrus.Logger
	producer sarama.AsyncProducer
	topic    string
	stats    *ProducerStats
}

// ProducerStats 跟踪生产者统计信息
type ProducerStats struct {
	MessagesSent   int64     `json:"messages_sent"`
	MessagesErrors int64     `json:"messages_errors"`
	LastSentTime   time.Time `json:"last_sent_time"`
}

// NewProducer 创建一个新的Kafka生产者
func NewProducer(cfg *config.Config, logger *logrus.Logger) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	config.Producer.Retry.Backoff = 100 * time.Millisecond
	
	// 设置通道缓冲区大小以防止通道满的问题
	if cfg.Kafka.Producer.ChannelBufferSize > 0 {
		config.ChannelBufferSize = cfg.Kafka.Producer.ChannelBufferSize
	} else {
		config.ChannelBufferSize = 2048  // 默认2048
	}
	
	// 设置刷新字节数
	if cfg.Kafka.Producer.FlushBytes > 0 {
		config.Producer.Flush.Bytes = cfg.Kafka.Producer.FlushBytes
	} else {
		config.Producer.Flush.Bytes = 16384  // 默认16KB
	}

	// 设置压缩方式
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

	// 设置批处理配置
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

	// 启动协程处理成功和错误消息
	go p.handleSuccesses()
	go p.handleErrors()

	return p, nil
}

// SendKlineData 发送K线数据到Kafka
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

	// 使用超时机制避免长时间阻塞
	sendTimeout := 5 * time.Second  // 默认超时时间
	if p.config.Kafka.Producer.SendTimeout != "" {
		if duration, err := time.ParseDuration(p.config.Kafka.Producer.SendTimeout); err == nil {
			sendTimeout = duration
		}
	}
	timeout := time.NewTimer(sendTimeout)
	defer timeout.Stop()
	
	select {
	case p.producer.Input() <- message:
		p.logger.Debugf("Sent kline data to Kafka: %s", klineData.Symbol)
		return nil
	case <-timeout.C:
		p.logger.Warnf("Timeout sending message for symbol %s, producer may be overloaded", klineData.Symbol)
		return fmt.Errorf("timeout sending message to kafka producer (5s)")
	default:
		// 通道满时记录警告但不立即失败，给系统一些时间处理
		p.logger.Warnf("Kafka producer input channel is full for symbol %s, will retry with timeout", klineData.Symbol)
		
		// 再次尝试，但这次使用阻塞模式和超时
		select {
		case p.producer.Input() <- message:
			p.logger.Debugf("Sent kline data to Kafka after retry: %s", klineData.Symbol)
			return nil
		case <-timeout.C:
			return fmt.Errorf("kafka producer input channel is full and timeout reached")
		}
	}
}

// handleSuccesses 处理成功的消息投递
func (p *Producer) handleSuccesses() {
	for success := range p.producer.Successes() {
		p.stats.MessagesSent++
		p.stats.LastSentTime = time.Now()
		p.logger.Debugf("Message sent successfully to partition %d offset %d", 
			success.Partition, success.Offset)
	}
}

// handleErrors 处理消息投递错误
func (p *Producer) handleErrors() {
	for err := range p.producer.Errors() {
		p.stats.MessagesErrors++
		p.logger.Errorf("Failed to send message to Kafka: %v", err.Err)
	}
}

// GetStats 返回生产者统计信息
func (p *Producer) GetStats() *ProducerStats {
	return p.stats
}

// Close 关闭生产者
func (p *Producer) Close() error {
	if err := p.producer.Close(); err != nil {
		return fmt.Errorf("failed to close Kafka producer: %w", err)
	}
	p.logger.Info("Kafka producer closed")
	return nil
}