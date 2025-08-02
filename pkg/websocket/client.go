package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"

	"data4trend/internal/types"
	"data4trend/pkg/config"
	"data4trend/pkg/kafka"
)

// Client represents a WebSocket client for Binance streams
type Client struct {
	config      *config.Config
	kafkaProducer *kafka.Producer
	logger      *logrus.Logger
	connections map[string]*websocket.Conn
	mutex       sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
	stats       *Stats
}

// Stats represents WebSocket client statistics
type Stats struct {
	Connections     int       `json:"connections"`
	MessagesTotal   int64     `json:"messages_total"`
	ErrorsTotal     int64     `json:"errors_total"`
	LastMessageTime time.Time `json:"last_message_time"`
	mutex           sync.RWMutex
}

// NewClient creates a new WebSocket client
func NewClient(cfg *config.Config, kafkaProducer *kafka.Producer, logger *logrus.Logger) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		config:        cfg,
		kafkaProducer: kafkaProducer,
		logger:        logger,
		connections:   make(map[string]*websocket.Conn),
		ctx:           ctx,
		cancel:        cancel,
		stats: &Stats{
			Connections:     0,
			MessagesTotal:   0,
			ErrorsTotal:     0,
			LastMessageTime: time.Now(),
		},
	}
}

// Start starts the WebSocket client and connects to all symbols
func (c *Client) Start() error {
	c.logger.Info("Starting WebSocket client...")

	// Start connections for all symbols
	for _, symbol := range c.config.Symbols {
		go c.connectSymbol(strings.ToLower(symbol))
		time.Sleep(100 * time.Millisecond) // Avoid overwhelming the server
	}

	// Start health check routine
	go c.healthCheck()

	c.logger.Infof("WebSocket client started with %d symbols", len(c.config.Symbols))
	return nil
}

// connectSymbol connects to a specific symbol stream
func (c *Client) connectSymbol(symbol string) {
	streamName := fmt.Sprintf("%s@kline_%s", symbol, c.config.Interval)
	websocketURL := fmt.Sprintf("%s/ws/%s", c.config.WebSocket.BaseURL, streamName)

	retries := 0
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		c.logger.Infof("Connecting to %s (attempt %d)", streamName, retries+1)

		// Create dialer with proxy support
		dialer := &websocket.Dialer{
			HandshakeTimeout: time.Duration(c.config.WebSocket.Timeout) * time.Second,
		}

		// Configure proxy if enabled
		if c.config.Proxy.Enabled {
			proxyURL := c.config.GetProxyURL()
			c.logger.Infof("Using proxy: %s", proxyURL)
			if proxyParsed, err := url.Parse(proxyURL); err == nil {
				dialer.Proxy = http.ProxyURL(proxyParsed)
			} else {
				c.logger.Errorf("Failed to parse proxy URL: %v", err)
			}
		}

		// Connect to WebSocket
		conn, _, err := dialer.Dial(websocketURL, nil)
		if err != nil {
			c.logger.Errorf("Failed to connect to %s: %v", streamName, err)
			c.incrementErrors()
			retries++
			if retries >= c.config.WebSocket.MaxRetries {
				c.logger.Errorf("Max retries reached for %s, giving up", streamName)
				return
			}
			time.Sleep(time.Duration(c.config.WebSocket.ReconnectInterval) * time.Second)
			continue
		}

		c.logger.Infof("Successfully connected to %s", streamName)
		retries = 0

		// Store connection
		c.mutex.Lock()
		c.connections[symbol] = conn
		c.stats.mutex.Lock()
		c.stats.Connections = len(c.connections)
		c.stats.mutex.Unlock()
		c.mutex.Unlock()

		// Handle messages
		c.handleConnection(conn, symbol)

		// Connection closed, clean up
		c.mutex.Lock()
		delete(c.connections, symbol)
		c.stats.mutex.Lock()
		c.stats.Connections = len(c.connections)
		c.stats.mutex.Unlock()
		c.mutex.Unlock()

		c.logger.Warnf("Connection to %s closed, will retry", streamName)
		time.Sleep(time.Duration(c.config.WebSocket.ReconnectInterval) * time.Second)
	}
}

// handleConnection handles messages from a WebSocket connection
func (c *Client) handleConnection(conn *websocket.Conn, symbol string) {
	defer conn.Close()

	// Set up ping/pong handlers
	conn.SetPingHandler(func(appData string) error {
		c.logger.Debugf("Received ping for %s", symbol)
		return conn.WriteMessage(websocket.PongMessage, []byte(appData))
	})

	conn.SetPongHandler(func(appData string) error {
		c.logger.Debugf("Received pong for %s", symbol)
		return nil
	})

	// Start ping routine
	go c.pingRoutine(conn, symbol)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// Read message
		_, message, err := conn.ReadMessage()
		if err != nil {
			c.logger.Errorf("Error reading message from %s: %v", symbol, err)
			c.incrementErrors()
			return
		}

		// Process message
		if err := c.processMessage(message, symbol); err != nil {
			c.logger.Errorf("Error processing message from %s: %v", symbol, err)
			c.incrementErrors()
		}
	}
}

// processMessage processes a WebSocket message
func (c *Client) processMessage(message []byte, symbol string) error {
	var event types.BinanceKlineEvent
	if err := json.Unmarshal(message, &event); err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	// Only process closed klines
	if !event.Kline.IsClosed {
		return nil
	}

	// Convert to KlineData
	klineData := &types.KlineData{
		Symbol:    strings.ToUpper(event.Kline.Symbol),
		OpenTime:  event.Kline.OpenTime,
		CloseTime: event.Kline.CloseTime,
		Open:      event.Kline.Open,
		High:      event.Kline.High,
		Low:       event.Kline.Low,
		Close:     event.Kline.Close,
		Volume:    event.Kline.Volume,
		CreatedAt: time.Now(),
	}

	// Send to Kafka
	if err := c.kafkaProducer.SendKlineData(klineData); err != nil {
		return fmt.Errorf("failed to send kline data to kafka: %w", err)
	}

	c.incrementMessages()
	c.logger.Debugf("Sent kline data for %s: %s", klineData.Symbol, klineData.Close)

	return nil
}

// pingRoutine sends periodic ping messages
func (c *Client) pingRoutine(conn *websocket.Conn, symbol string) {
	ticker := time.NewTicker(time.Duration(c.config.WebSocket.PingInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if err := conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
				c.logger.Errorf("Failed to send ping to %s: %v", symbol, err)
				return
			}
			c.logger.Debugf("Sent ping to %s", symbol)
		}
	}
}

// healthCheck performs periodic health checks
func (c *Client) healthCheck() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.mutex.RLock()
			activeConnections := len(c.connections)
			c.mutex.RUnlock()

			c.stats.mutex.RLock()
			messagesTotal := c.stats.MessagesTotal
			errorsTotal := c.stats.ErrorsTotal
			lastMessageTime := c.stats.LastMessageTime
			c.stats.mutex.RUnlock()

			c.logger.Infof("Health check: %d/%d streams active, %d messages, %d errors, last message: %v",
				activeConnections, len(c.config.Symbols), messagesTotal, errorsTotal,
				time.Since(lastMessageTime).Truncate(time.Second))
		}
	}
}

// incrementMessages increments the message counter
func (c *Client) incrementMessages() {
	c.stats.mutex.Lock()
	c.stats.MessagesTotal++
	c.stats.LastMessageTime = time.Now()
	c.stats.mutex.Unlock()
}

// incrementErrors increments the error counter
func (c *Client) incrementErrors() {
	c.stats.mutex.Lock()
	c.stats.ErrorsTotal++
	c.stats.mutex.Unlock()
}

// GetStats returns current statistics
func (c *Client) GetStats() *Stats {
	c.stats.mutex.RLock()
	defer c.stats.mutex.RUnlock()

	c.mutex.RLock()
	connections := len(c.connections)
	c.mutex.RUnlock()

	return &Stats{
		Connections:     connections,
		MessagesTotal:   c.stats.MessagesTotal,
		ErrorsTotal:     c.stats.ErrorsTotal,
		LastMessageTime: c.stats.LastMessageTime,
	}
}

// Stop stops the WebSocket client
func (c *Client) Stop() {
	c.logger.Info("Stopping WebSocket client...")
	c.cancel()

	// Close all connections
	c.mutex.Lock()
	for symbol, conn := range c.connections {
		c.logger.Infof("Closing connection for %s", symbol)
		conn.Close()
	}
	c.connections = make(map[string]*websocket.Conn)
	c.mutex.Unlock()

	c.logger.Info("WebSocket client stopped")
}