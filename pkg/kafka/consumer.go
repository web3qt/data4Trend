package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"

	"data4trend/internal/types"
	"data4trend/pkg/config"
)

// Consumer 代表用于K线数据的Kafka消费者
type Consumer struct {
	config       *config.Config
	logger       *logrus.Logger
	consumerGroup sarama.ConsumerGroup
	topic        string
	groupID      string
	ctx          context.Context
	cancel       context.CancelFunc
	stats        *ConsumerStats
	mutex        sync.RWMutex
	messageHandler MessageHandler
}

// ConsumerStats 跟踪消费者统计信息
type ConsumerStats struct {
	MessagesReceived int64     `json:"messages_received"`
	MessagesErrors   int64     `json:"messages_errors"`
	LastReceivedTime time.Time `json:"last_received_time"`
}

// MessageHandler 定义处理消费消息的接口
type MessageHandler interface {
	HandleMessage(klineData *types.KlineData) error
}

// NewConsumer 创建一个新的Kafka消费者
func NewConsumer(cfg *config.Config, logger *logrus.Logger, handler MessageHandler) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// 解析会话超时时间
	sessionTimeout, err := time.ParseDuration(cfg.Kafka.Consumer.SessionTimeout)
	if err != nil {
		sessionTimeout = 30 * time.Second
	}
	config.Consumer.Group.Session.Timeout = sessionTimeout

	// 解析心跳间隔
	heartbeatInterval, err := time.ParseDuration(cfg.Kafka.Consumer.HeartbeatInterval)
	if err != nil {
		heartbeatInterval = 3 * time.Second
	}
	config.Consumer.Group.Heartbeat.Interval = heartbeatInterval

	// 设置自动偏移重置
	if cfg.Kafka.Consumer.AutoOffsetReset == "earliest" {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	consumerGroup, err := sarama.NewConsumerGroup(cfg.Kafka.Brokers, cfg.Kafka.Consumer.GroupID, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer group: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Consumer{
		config:        cfg,
		logger:        logger,
		consumerGroup: consumerGroup,
		topic:         cfg.Kafka.Topic,
		groupID:       cfg.Kafka.Consumer.GroupID,
		ctx:           ctx,
		cancel:        cancel,
		stats:         &ConsumerStats{},
		messageHandler: handler,
	}

	return c, nil
}

// Start 启动消费者
func (c *Consumer) Start() error {
	c.logger.Infof("Starting Kafka consumer group: %s", c.groupID)

	// 在协程中开始消费
	go func() {
		for {
			select {
			case <-c.ctx.Done():
				c.logger.Info("Kafka consumer context cancelled")
				return
			default:
				if err := c.consumerGroup.Consume(c.ctx, []string{c.topic}, c); err != nil {
					c.logger.Errorf("Error from consumer: %v", err)
					time.Sleep(1 * time.Second)
				}
			}
		}
	}()

	// 处理消费者错误
	go func() {
		for err := range c.consumerGroup.Errors() {
			c.stats.MessagesErrors++
			c.logger.Errorf("Consumer error: %v", err)
		}
	}()

	c.logger.Info("Kafka consumer started")
	return nil
}

// Setup 在新会话开始时运行，在ConsumeClaim之前
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	c.logger.Info("Kafka consumer session setup")
	return nil
}

// Cleanup 在会话结束时运行，一旦所有ConsumeClaim协程退出
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	c.logger.Info("Kafka consumer session cleanup")
	return nil
}

// ConsumeClaim 必须启动ConsumerGroupClaim消息的消费者循环
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			if err := c.processMessage(message); err != nil {
				c.stats.MessagesErrors++
				c.logger.Errorf("Failed to process message: %v", err)
			} else {
				c.stats.MessagesReceived++
				c.stats.LastReceivedTime = time.Now()
				session.MarkMessage(message, "")
			}

		case <-c.ctx.Done():
			return nil
		}
	}
}

// processMessage 处理Kafka消息
func (c *Consumer) processMessage(message *sarama.ConsumerMessage) error {
	var klineData types.KlineData
	if err := json.Unmarshal(message.Value, &klineData); err != nil {
		return fmt.Errorf("failed to unmarshal kline data: %w", err)
	}

	c.logger.Debugf("Received kline data from Kafka: %s", klineData.Symbol)

	// 使用提供的处理器处理消息
	if err := c.messageHandler.HandleMessage(&klineData); err != nil {
		return fmt.Errorf("failed to handle message: %w", err)
	}

	return nil
}

// GetStats 返回消费者统计信息
func (c *Consumer) GetStats() *ConsumerStats {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.stats
}

// Stop 停止消费者
func (c *Consumer) Stop() error {
	c.logger.Info("Stopping Kafka consumer...")
	c.cancel()

	if err := c.consumerGroup.Close(); err != nil {
		return fmt.Errorf("failed to close Kafka consumer group: %w", err)
	}

	c.logger.Info("Kafka consumer stopped")
	return nil
}