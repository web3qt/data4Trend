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
	"data4trend/pkg/websocket"
)

// ValidatorInterface 定义验证器接口
type ValidatorInterface interface {
	IsRunning() bool
	ForceValidation() error
	GetStats() interface{}
	ValidateDataRange(startTime, endTime time.Time) ([]*backfill.GapInfo, error)
	ValidateSymbol(symbol string, startTime, endTime time.Time) ([]*backfill.GapInfo, error)
}

// Server 代表API服务器
type Server struct {
	config    *config.Config
	storage   *storage.ClickHouseStorage
	websocket *websocket.Client
	backfill  *backfill.BackfillService
	integrity *integrity.DataIntegrityService
	validator ValidatorInterface
	logger    *logrus.Logger
	router    *gin.Engine
}

// NewServer 创建新的API服务器
func NewServer(cfg *config.Config, storage *storage.ClickHouseStorage, ws *websocket.Client, integrity *integrity.DataIntegrityService, validator ValidatorInterface, logger *logrus.Logger) *Server {
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
		v1.GET("/backfill/progress", s.getBackfillProgress)
		v1.POST("/backfill/symbol/:symbol", s.backfillSymbol)
		v1.POST("/backfill/symbol/:symbol/complete", s.backfillSymbolComplete)
		v1.POST("/backfill/all", s.backfillAll)
		v1.POST("/backfill/all/complete", s.backfillAllComplete)

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
	result := s.validator.IsRunning()
	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data":   result,
	})
}

// runValidation triggers a manual validation check
func (s *Server) runValidation(c *gin.Context) {
	s.logger.Info("Manual validation triggered via API")
	err := s.validator.ForceValidation()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Validation failed",
			"error":   err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Validation completed",
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
	result := s.validator.IsRunning()
	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data": gin.H{
			"is_running": result,
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
		"status":     "success",
		"message":    fmt.Sprintf("Backfill completed for %s", symbol),
		"symbol":     symbol,
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
			"status":    statusCode,
			"latency":   latency,
			"client_ip": clientIP,
			"method":    method,
			"path":      path,
		}).Info("HTTP Request")
	}
}

// getBackfillStatus handles backfill status requests
func (s *Server) getBackfillStatus(c *gin.Context) {
	status, err := s.backfill.GetBackfillStatus()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to get backfill status",
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

	// 如果是POST请求，尝试从请求体解析参数
	if c.Request.Method == "POST" {
		var requestBody struct {
			StartTime string `json:"start_time"`
			EndTime   string `json:"end_time"`
		}

		if err := c.ShouldBindJSON(&requestBody); err == nil {
			if requestBody.StartTime != "" {
				startTimeStr = requestBody.StartTime
			}
			if requestBody.EndTime != "" {
				endTimeStr = requestBody.EndTime
			}
		}
	}

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

	s.logger.Infof("🚀 [API] Starting range backfill for symbol %s from %s to %s",
		symbol, startTime.Format("2006-01-02 15:04:05"), endTime.Format("2006-01-02 15:04:05"))

	// Perform backfill
	result, err := s.backfill.BackfillSymbolRange(symbol, startTime, endTime)
	if err != nil {
		s.logger.Errorf("❌ [API] Backfill failed for %s: %v", symbol, err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Backfill failed",
			"details": err.Error(),
		})
		return
	}

	s.logger.Infof("✅ [API] Backfill completed for %s", symbol)
	c.JSON(http.StatusOK, gin.H{
		"status":     "success",
		"symbol":     symbol,
		"start_time": startTime,
		"end_time":   endTime,
		"result":     result,
	})
}

// backfillSymbolComplete handles complete backfill for a specific symbol (5 days)
func (s *Server) backfillSymbolComplete(c *gin.Context) {
	symbol := c.Param("symbol")
	if symbol == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Symbol parameter is required",
		})
		return
	}

	s.logger.Infof("🚀 [API] Starting complete backfill for symbol %s (5 days)", symbol)

	// Perform complete backfill
	result, err := s.backfill.BackfillSymbolComplete(symbol)
	if err != nil {
		s.logger.Errorf("❌ [API] Complete backfill failed for %s: %v", symbol, err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Complete backfill failed",
			"details": err.Error(),
		})
		return
	}

	s.logger.Infof("✅ [API] Complete backfill completed for %s", symbol)
	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"symbol": symbol,
		"result": result,
	})
}

// backfillAll handles backfill requests for all symbols (range-based)
func (s *Server) backfillAll(c *gin.Context) {
	s.logger.Info("Starting range backfill for all symbols")

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

	// Get all symbols
	symbols, err := s.storage.GetAllSymbols()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to get symbols",
			"details": err.Error(),
		})
		return
	}

	// Perform backfill for each symbol
	allResults := make(map[string]*backfill.BackfillResult)
	totalSuccess := 0
	totalFailed := 0

	for _, symbol := range symbols {
		result, err := s.backfill.BackfillSymbolRange(symbol, startTime, endTime)
		if err != nil {
			s.logger.Errorf("❌ [API] Backfill failed for %s: %v", symbol, err)
			totalFailed++
		} else {
			totalSuccess++
		}
		allResults[symbol] = result
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"summary": gin.H{
			"total_symbols":        len(symbols),
			"successful_backfills": totalSuccess,
			"failed_backfills":     totalFailed,
			"start_time":           startTime,
			"end_time":             endTime,
		},
		"results": allResults,
	})
}

// backfillAllComplete handles complete backfill for all symbols (5 days)
func (s *Server) backfillAllComplete(c *gin.Context) {
	s.logger.Info("Starting complete backfill for all symbols (5 days)")

	// Perform complete backfill for all symbols
	allResults, err := s.backfill.BackfillAllSymbolsComplete()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Complete backfill failed",
			"details": err.Error(),
		})
		return
	}

	// Calculate summary statistics
	totalSymbols := len(allResults)
	totalSuccess := 0
	totalFailed := 0
	totalFetched := 0
	totalInserted := 0

	for _, result := range allResults {
		if result.Success {
			totalSuccess++
		} else {
			totalFailed++
		}
		totalFetched += result.Fetched
		totalInserted += result.Inserted
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"summary": gin.H{
			"total_symbols":        totalSymbols,
			"successful_backfills": totalSuccess,
			"failed_backfills":     totalFailed,
			"total_fetched":        totalFetched,
			"total_inserted":       totalInserted,
		},
		"results": allResults,
	})
}

// getBackfillProgress 获取当前回填进度
func (s *Server) getBackfillProgress(c *gin.Context) {
	// 获取回填服务的进度信息
	progress := s.backfill.GetProgress()

	// 获取当前时间
	now := time.Now()

	// 检查过去24小时的数据缺口
	endTime := now.Truncate(time.Minute)
	startTime := endTime.Add(-24 * time.Hour)

	// 获取所有缺口
	allGaps, err := s.storage.GetDataGapsForAllSymbols()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": fmt.Sprintf("Failed to get data gaps: %v", err),
		})
		return
	}

	// 统计缺口信息
	totalSymbols := len(allGaps)
	totalGaps := 0
	totalMissing := 0
	symbolsWithGaps := []string{}

	for symbol, gaps := range allGaps {
		if len(gaps) > 0 {
			symbolsWithGaps = append(symbolsWithGaps, symbol)
			totalGaps += len(gaps)
			for _, gap := range gaps {
				totalMissing += gap.Missing
			}
		}
	}

	// 获取完整性服务统计
	integrityStats := s.integrity.GetStats()

	// 计算进度百分比
	var progressPercent float64
	if progress.TotalSymbols > 0 {
		progressPercent = float64(progress.Processed) / float64(progress.TotalSymbols) * 100
	}

	response := gin.H{
		"status": "success",
		"data": gin.H{
			"check_time": now,
			"time_range": gin.H{
				"start": startTime.Format("2006-01-02 15:04:05"),
				"end":   endTime.Format("2006-01-02 15:04:05"),
			},
			"gaps_summary": gin.H{
				"total_symbols":     totalSymbols,
				"symbols_with_gaps": len(symbolsWithGaps),
				"total_gaps":        totalGaps,
				"total_missing":     totalMissing,
			},
			"symbols_with_gaps": symbolsWithGaps,
			"integrity_stats":   integrityStats,
			"backfill_progress": gin.H{
				"is_running":       progress.IsRunning,
				"start_time":       progress.StartTime,
				"current_symbol":   progress.CurrentSymbol,
				"total_symbols":    progress.TotalSymbols,
				"processed":        progress.Processed,
				"success_count":    progress.SuccessCount,
				"failed_count":     progress.FailedCount,
				"progress_percent": progressPercent,
				"last_update":      progress.LastUpdate,
				"estimated_time_remaining": func() string {
					if !progress.IsRunning || progress.Processed == 0 {
						return "unknown"
					}
					elapsed := time.Since(progress.StartTime)
					avgTimePerSymbol := elapsed / time.Duration(progress.Processed)
					remainingSymbols := progress.TotalSymbols - progress.Processed
					estimatedRemaining := avgTimePerSymbol * time.Duration(remainingSymbols)
					return estimatedRemaining.String()
				}(),
			},
		},
	}

	c.JSON(http.StatusOK, response)
}
