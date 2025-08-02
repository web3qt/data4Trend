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

// Consumer represents a Kafka consumer for kline data
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

// ConsumerStats tracks consumer statistics
type ConsumerStats struct {
	MessagesReceived int64     `json:"messages_received"`
	MessagesErrors   int64     `json:"messages_errors"`
	LastReceivedTime time.Time `json:"last_received_time"`
}

// MessageHandler defines the interface for handling consumed messages
type MessageHandler interface {
	HandleMessage(klineData *types.KlineData) error
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(cfg *config.Config, logger *logrus.Logger, handler MessageHandler) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// Parse session timeout
	sessionTimeout, err := time.ParseDuration(cfg.Kafka.Consumer.SessionTimeout)
	if err != nil {
		sessionTimeout = 30 * time.Second
	}
	config.Consumer.Group.Session.Timeout = sessionTimeout

	// Parse heartbeat interval
	heartbeatInterval, err := time.ParseDuration(cfg.Kafka.Consumer.HeartbeatInterval)
	if err != nil {
		heartbeatInterval = 3 * time.Second
	}
	config.Consumer.Group.Heartbeat.Interval = heartbeatInterval

	// Set auto offset reset
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

// Start starts the consumer
func (c *Consumer) Start() error {
	c.logger.Infof("Starting Kafka consumer group: %s", c.groupID)

	// Start consuming in a goroutine
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

	// Handle consumer errors
	go func() {
		for err := range c.consumerGroup.Errors() {
			c.stats.MessagesErrors++
			c.logger.Errorf("Consumer error: %v", err)
		}
	}()

	c.logger.Info("Kafka consumer started")
	return nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	c.logger.Info("Kafka consumer session setup")
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	c.logger.Info("Kafka consumer session cleanup")
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages()
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

// processMessage processes a Kafka message
func (c *Consumer) processMessage(message *sarama.ConsumerMessage) error {
	var klineData types.KlineData
	if err := json.Unmarshal(message.Value, &klineData); err != nil {
		return fmt.Errorf("failed to unmarshal kline data: %w", err)
	}

	c.logger.Debugf("Received kline data from Kafka: %s", klineData.Symbol)

	// Handle the message using the provided handler
	if err := c.messageHandler.HandleMessage(&klineData); err != nil {
		return fmt.Errorf("failed to handle message: %w", err)
	}

	return nil
}

// GetStats returns consumer statistics
func (c *Consumer) GetStats() *ConsumerStats {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.stats
}

// Stop stops the consumer
func (c *Consumer) Stop() error {
	c.logger.Info("Stopping Kafka consumer...")
	c.cancel()

	if err := c.consumerGroup.Close(); err != nil {
		return fmt.Errorf("failed to close Kafka consumer group: %w", err)
	}

	c.logger.Info("Kafka consumer stopped")
	return nil
}