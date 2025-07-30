package api

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"

	"data4trend/pkg/config"
	"data4trend/pkg/storage"
	"data4trend/pkg/websocket"
)

// Server represents the API server
type Server struct {
	config    *config.Config
	storage   *storage.ClickHouseStorage
	websocket *websocket.Client
	logger    *logrus.Logger
	router    *gin.Engine
}

// NewServer creates a new API server
func NewServer(cfg *config.Config, storage *storage.ClickHouseStorage, ws *websocket.Client, logger *logrus.Logger) *Server {
	// Set gin mode
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(corsMiddleware())
	router.Use(loggingMiddleware(logger))

	server := &Server{
		config:    cfg,
		storage:   storage,
		websocket: ws,
		logger:    logger,
		router:    router,
	}

	server.setupRoutes()
	return server
}

// setupRoutes sets up API routes
func (s *Server) setupRoutes() {
	// Health check
	s.router.GET("/health", s.healthCheck)

	// API v1 routes
	v1 := s.router.Group("/api/v1")
	{
		v1.GET("/klines/:symbol", s.getKlines)
		v1.GET("/stats", s.getStats)
		v1.GET("/websocket/stats", s.getWebSocketStats)
		v1.GET("/symbols", s.getSymbols)
	}

	// Static files (if needed)
	s.router.Static("/static", "./static")
}

// healthCheck handles health check requests
func (s *Server) healthCheck(c *gin.Context) {
	// Test database connection
	if err := s.storage.TestConnection(); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":  "unhealthy",
			"message": "Database connection failed",
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":    "healthy",
		"timestamp": time.Now(),
		"version":   "1.0.0",
	})
}

// getKlines handles kline data requests
func (s *Server) getKlines(c *gin.Context) {
	symbol := c.Param("symbol")
	if symbol == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Symbol is required"})
		return
	}

	// Parse query parameters
	limitStr := c.DefaultQuery("limit", "100")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 || limit > 1000 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid limit parameter (1-1000)"})
		return
	}

	var startTime, endTime *time.Time

	// Parse start_time
	if startTimeStr := c.Query("start_time"); startTimeStr != "" {
		if startTimeInt, err := strconv.ParseInt(startTimeStr, 10, 64); err == nil {
			t := time.UnixMilli(startTimeInt)
			startTime = &t
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid start_time format (unix timestamp in milliseconds)"})
			return
		}
	}

	// Parse end_time
	if endTimeStr := c.Query("end_time"); endTimeStr != "" {
		if endTimeInt, err := strconv.ParseInt(endTimeStr, 10, 64); err == nil {
			t := time.UnixMilli(endTimeInt)
			endTime = &t
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid end_time format (unix timestamp in milliseconds)"})
			return
		}
	}

	// Get data from storage
	data, err := s.storage.GetKlineData(symbol, limit, startTime, endTime)
	if err != nil {
		s.logger.Errorf("Failed to get kline data: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve data"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"symbol": symbol,
		"count":  len(data),
		"data":   data,
	})
}

// getStats handles statistics requests
func (s *Server) getStats(c *gin.Context) {
	stats, err := s.storage.GetStats()
	if err != nil {
		s.logger.Errorf("Failed to get stats: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve statistics"})
		return
	}

	c.JSON(http.StatusOK, stats)
}

// getWebSocketStats handles WebSocket statistics requests
func (s *Server) getWebSocketStats(c *gin.Context) {
	stats := s.websocket.GetStats()
	c.JSON(http.StatusOK, stats)
}

// getSymbols handles symbols list requests
func (s *Server) getSymbols(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"symbols":  s.config.Symbols,
		"interval": s.config.Interval,
		"count":    len(s.config.Symbols),
	})
}

// Start starts the API server
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.config.API.Host, s.config.API.Port)
	s.logger.Infof("Starting API server on %s", addr)

	return s.router.Run(addr)
}

// corsMiddleware handles CORS
func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
	c.Header("Access-Control-Allow-Origin", "*")
	c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	c.Header("Access-Control-Allow-Headers", "Origin, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

// loggingMiddleware logs HTTP requests
func loggingMiddleware(logger *logrus.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		raw := c.Request.URL.RawQuery

		c.Next()

		latency := time.Since(start)
		clientIP := c.ClientIP()
		method := c.Request.Method
		statusCode := c.Writer.Status()

		if raw != "" {
			path = path + "?" + raw
		}

		logger.WithFields(logrus.Fields{
			"status":     statusCode,
			"latency":    latency,
			"client_ip":  clientIP,
			"method":     method,
			"path":       path,
		}).Info("HTTP Request")
	}
}