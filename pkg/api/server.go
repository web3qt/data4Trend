package api

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"

	"data4trend/pkg/backfill"
	"data4trend/pkg/config"
	"data4trend/pkg/integrity"
	"data4trend/pkg/storage"
	"data4trend/pkg/validation"
	"data4trend/pkg/websocket"
)

// Server 代表API服务器
type Server struct {
	config    *config.Config
	storage   *storage.ClickHouseStorage
	websocket *websocket.Client
	backfill  *backfill.BackfillService
	integrity *integrity.DataIntegrityService
	validator *validation.DataValidator
	logger    *logrus.Logger
	router    *gin.Engine
}

// NewServer 创建新的API服务器
func NewServer(cfg *config.Config, storage *storage.ClickHouseStorage, ws *websocket.Client, integrity *integrity.DataIntegrityService, validator *validation.DataValidator, logger *logrus.Logger) *Server {
	// 设置gin模式
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(corsMiddleware())
	router.Use(loggingMiddleware(logger))

	// 初始化回补服务
	backfillService := backfill.NewBackfillService(cfg, storage, logger)

	server := &Server{
		config:    cfg,
		storage:   storage,
		websocket: ws,
		backfill:  backfillService,
		integrity: integrity,
		validator: validator,
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
		
		// Backfill routes
		v1.GET("/backfill/status", s.getBackfillStatus)
		v1.POST("/backfill/symbol/:symbol", s.backfillSymbol)
		v1.POST("/backfill/all", s.backfillAll)
		
		// Data validation routes
		v1.GET("/validation/status", s.getValidationStatus)
		v1.POST("/validation/run", s.runValidation)
		v1.GET("/validation/gaps", s.getDataGaps)
		v1.GET("/validation/quality", s.getDataQuality)
		
		// Data integrity routes
		v1.GET("/integrity/status", s.getIntegrityStatus)
		v1.POST("/integrity/check", s.forceIntegrityCheck)
		v1.POST("/integrity/backfill/:symbol", s.backfillSymbolRange)
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

// getValidationStatus returns the current validation status
func (s *Server) getValidationStatus(c *gin.Context) {
	result := s.validator.GetLastValidationResult()
	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data":   result,
	})
}

// runValidation triggers a manual validation check
func (s *Server) runValidation(c *gin.Context) {
	s.logger.Info("Manual validation triggered via API")
	result := s.validator.RunManualValidation()
	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Validation completed",
		"data":    result,
	})
}

// getDataGaps returns data gaps for all symbols
func (s *Server) getDataGaps(c *gin.Context) {
	gaps, err := s.storage.GetDataGapsForAllSymbols()
	if err != nil {
		s.logger.Errorf("Failed to get data gaps: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status": "error",
			"error":  err.Error(),
		})
		return
	}
	
	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data":   gaps,
	})
}

// getDataQuality returns data quality metrics
func (s *Server) getDataQuality(c *gin.Context) {
	result := s.validator.GetLastValidationResult()
	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data": gin.H{
			"timestamp":    result.Timestamp,
			"overall_score": result.DataQuality.OverallScore,
			"metrics":      result.DataQuality,
			"issues_count": len(result.Issues),
			"gaps_count":   len(result.DataGaps),
		},
	})
}

// getIntegrityStatus returns the current data integrity status
func (s *Server) getIntegrityStatus(c *gin.Context) {
	stats := s.integrity.GetStats()
	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data":   stats,
	})
}

// forceIntegrityCheck triggers a manual integrity check
func (s *Server) forceIntegrityCheck(c *gin.Context) {
	s.logger.Info("Manual integrity check triggered via API")
	s.integrity.ForceIntegrityCheck()
	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Integrity check triggered",
	})
}

// backfillSymbolRange handles manual backfill requests for specific symbol and time range
func (s *Server) backfillSymbolRange(c *gin.Context) {
	symbol := c.Param("symbol")
	if symbol == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"status": "error",
			"error":  "Symbol parameter is required",
		})
		return
	}
	
	// Parse query parameters for time range
	startTimeStr := c.Query("start_time")
	endTimeStr := c.Query("end_time")
	
	if startTimeStr == "" || endTimeStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"status": "error",
			"error":  "start_time and end_time query parameters are required (format: 2006-01-02T15:04:05Z)",
		})
		return
	}
	
	startTime, err := time.Parse(time.RFC3339, startTimeStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status": "error",
			"error":  fmt.Sprintf("Invalid start_time format: %v", err),
		})
		return
	}
	
	endTime, err := time.Parse(time.RFC3339, endTimeStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status": "error",
			"error":  fmt.Sprintf("Invalid end_time format: %v", err),
		})
		return
	}
	
	// Validate time range
	if endTime.Before(startTime) {
		c.JSON(http.StatusBadRequest, gin.H{
			"status": "error",
			"error":  "end_time must be after start_time",
		})
		return
	}
	
	// Limit time range to prevent abuse
	maxDuration := 7 * 24 * time.Hour // 7 days
	if endTime.Sub(startTime) > maxDuration {
		c.JSON(http.StatusBadRequest, gin.H{
			"status": "error",
			"error":  "Time range cannot exceed 7 days",
		})
		return
	}
	
	s.logger.Infof("Manual backfill requested for %s from %s to %s", symbol, startTime, endTime)
	
	// Perform backfill
	err = s.integrity.BackfillSymbolRange(symbol, startTime, endTime)
	if err != nil {
		s.logger.Errorf("Failed to backfill symbol %s: %v", symbol, err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status": "error",
			"error":  err.Error(),
		})
		return
	}
	
	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": fmt.Sprintf("Backfill completed for %s", symbol),
		"symbol":  symbol,
		"start_time": startTime,
		"end_time":   endTime,
	})
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

// getBackfillStatus handles backfill status requests
func (s *Server) getBackfillStatus(c *gin.Context) {
	status, err := s.backfill.GetBackfillStatus()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to get backfill status",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data":   status,
	})
}

// backfillSymbol handles symbol-specific backfill requests
func (s *Server) backfillSymbol(c *gin.Context) {
	symbol := c.Param("symbol")
	if symbol == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Symbol parameter is required",
		})
		return
	}

	// Parse optional time range parameters
	startTimeStr := c.Query("start_time")
	endTimeStr := c.Query("end_time")

	// Default to last 24 hours if not specified
	endTime := time.Now()
	startTime := endTime.Add(-24 * time.Hour)

	if startTimeStr != "" {
		if parsed, err := time.Parse("2006-01-02T15:04:05Z", startTimeStr); err == nil {
			startTime = parsed
		}
	}

	if endTimeStr != "" {
		if parsed, err := time.Parse("2006-01-02T15:04:05Z", endTimeStr); err == nil {
			endTime = parsed
		}
	}

	s.logger.Infof("Starting backfill for symbol %s from %s to %s", 
		symbol, startTime.Format("2006-01-02 15:04:05"), endTime.Format("2006-01-02 15:04:05"))

	// Perform backfill
	results, err := s.backfill.BackfillSymbol(symbol, startTime, endTime)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Backfill failed",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"symbol": symbol,
		"start_time": startTime,
		"end_time": endTime,
		"results": results,
	})
}

// backfillAll handles backfill requests for all symbols
func (s *Server) backfillAll(c *gin.Context) {
	s.logger.Info("Starting backfill for all symbols")

	// Perform backfill for all symbols
	allResults, err := s.backfill.BackfillAllSymbols()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Backfill failed",
			"details": err.Error(),
		})
		return
	}

	// Calculate summary statistics
	totalSymbols := len(allResults)
	totalGaps := 0
	totalSuccess := 0
	totalFailed := 0

	for _, results := range allResults {
		totalGaps += len(results)
		for _, result := range results {
			if result.Success {
				totalSuccess++
			} else {
				totalFailed++
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"summary": gin.H{
			"total_symbols": totalSymbols,
			"total_gaps": totalGaps,
			"successful_backfills": totalSuccess,
			"failed_backfills": totalFailed,
		},
		"results": allResults,
	})
}