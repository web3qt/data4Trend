package apiserver

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
	"github.com/web3qt/dataFeeder/internal/types"
	"github.com/web3qt/dataFeeder/pkg/datastore"
	"github.com/web3qt/dataFeeder/pkg/logging"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Server 定义API服务器
type Server struct {
	router        *gin.Engine
	mysqlStore    *datastore.MySQLStore
	port          int
	subscriptions map[string]map[string][]*websocket.Conn // symbol -> interval -> connections
	subsMutex     sync.RWMutex
	klineBuffer   map[string]map[string][]*types.KLineData // symbol -> interval -> kline data (recently received)
	bufferMutex   sync.RWMutex
	// 添加数据输入通道
	inputChan chan *types.KLineData
}

// NewServer 创建新的API服务器
func NewServer(store *datastore.MySQLStore) *Server {
	r := gin.Default()

	// 配置CORS
	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	s := &Server{
		router:        r,
		mysqlStore:    store,
		port:          8080,
		subscriptions: make(map[string]map[string][]*websocket.Conn),
		klineBuffer:   make(map[string]map[string][]*types.KLineData),
		inputChan:     make(chan *types.KLineData, 1000),
	}
	s.registerRoutes()
	return s
}

// 注册API路由
func (s *Server) registerRoutes() {
	apiGroup := s.router.Group("/api/v1")
	{
		apiGroup.GET("/klines", s.handleGetKlines)
		apiGroup.GET("/history", s.handleGetHistory)
		apiGroup.GET("/indicators", s.handleGetIndicators)
		apiGroup.GET("/stats", s.handleGetStats)
		apiGroup.GET("/symbols", s.handleGetSymbols)
		apiGroup.GET("/ws", s.handleWebSocket)                // websocket端点
		apiGroup.GET("/multi_klines", s.handleGetMultiKlines) // 多交易对K线数据查询
		apiGroup.GET("/check_gaps", s.handleCheckGaps)        // 检查数据缺口
		apiGroup.POST("/fix_gaps", s.handleFixGaps)           // 修复数据缺口
	}
}

// Start 启动API服务器
func (s *Server) Start(ctx context.Context) error {
	// 启动WebSocket广播器
	go s.klineBroadcaster(ctx)

	// 启动数据接收器
	go s.dataReceiver(ctx)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: s.router,
	}

	// 处理优雅关闭
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 关闭所有WebSocket连接
		s.closeAllConnections()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			logging.Logger.WithError(err).Error("关闭HTTP服务器时出错")
		}
	}()

	logging.Logger.WithFields(logrus.Fields{
		"port": s.port,
	}).Info("API服务器已启动")

	// 监听并阻塞主线程
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}

	return nil
}

// GetInputChannel 获取数据输入通道
func (s *Server) GetInputChannel() chan<- *types.KLineData {
	return s.inputChan
}

// SetPort 设置API服务器端口
func (s *Server) SetPort(port int) {
	s.port = port
	logging.Logger.WithField("port", port).Info("设置API服务器端口")
}

// dataReceiver 接收数据并存入缓冲区
func (s *Server) dataReceiver(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-s.inputChan:
			// 缓存数据
			s.bufferMutex.Lock()

			// 确保缓冲区存在
			if _, exists := s.klineBuffer[data.Symbol]; !exists {
				s.klineBuffer[data.Symbol] = make(map[string][]*types.KLineData)
			}
			if _, exists := s.klineBuffer[data.Symbol][data.Interval]; !exists {
				s.klineBuffer[data.Symbol][data.Interval] = make([]*types.KLineData, 0, 100)
			}

			// 添加数据到缓冲区
			buffer := s.klineBuffer[data.Symbol][data.Interval]

			// 保持缓冲区大小不超过100个数据点
			if len(buffer) >= 100 {
				buffer = buffer[1:]
			}

			// 追加新数据
			buffer = append(buffer, data)
			s.klineBuffer[data.Symbol][data.Interval] = buffer

			s.bufferMutex.Unlock()

			// 记录日志
			logging.Logger.WithFields(logrus.Fields{
				"symbol":    data.Symbol,
				"interval":  data.Interval,
				"open_time": data.OpenTime,
				"close":     data.Close,
			}).Debug("收到新K线数据")
		}
	}
}

// klineBroadcaster 广播K线数据
func (s *Server) klineBroadcaster(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond) // 每200毫秒检查一次
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.broadcastBufferedData()
		}
	}
}

// broadcastBufferedData 广播缓冲区数据
func (s *Server) broadcastBufferedData() {
	s.bufferMutex.RLock()
	defer s.bufferMutex.RUnlock()

	// 遍历缓冲区数据
	for symbol, intervals := range s.klineBuffer {
		for interval, buffer := range intervals {
			if len(buffer) == 0 {
				continue
			}

			// 获取订阅该数据的连接
			s.subsMutex.RLock()
			var conns []*websocket.Conn
			if symbolSubs, exists := s.subscriptions[symbol]; exists {
				if intervalConns, exists := symbolSubs[interval]; exists {
					conns = intervalConns
				}
			}
			s.subsMutex.RUnlock()

			// 如果没有订阅者，跳过
			if len(conns) == 0 {
				continue
			}

			// 向每个订阅者发送数据
			for _, conn := range conns {
				for _, data := range buffer {
					if err := conn.WriteJSON(data); err != nil {
						// 出错的连接将在下一次清理
						logging.Logger.WithError(err).Warn("向WebSocket客户端发送数据时出错")
					}
				}
			}

			// 广播后清空缓冲区
			s.bufferMutex.Lock()
			s.klineBuffer[symbol][interval] = s.klineBuffer[symbol][interval][:0]
			s.bufferMutex.Unlock()
		}
	}
}

// closeAllConnections 关闭所有WebSocket连接
func (s *Server) closeAllConnections() {
	s.subsMutex.Lock()
	defer s.subsMutex.Unlock()

	for symbol, intervals := range s.subscriptions {
		for interval, conns := range intervals {
			for _, conn := range conns {
				conn.Close()
			}
			s.subscriptions[symbol][interval] = nil
		}
		s.subscriptions[symbol] = nil
	}
	s.subscriptions = make(map[string]map[string][]*websocket.Conn)
}

// handleWebSocket 处理WebSocket连接
func (s *Server) handleWebSocket(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "websocket upgrade failed"})
		return
	}

	// 从查询参数获取订阅信息
	symbol := c.Query("symbol")
	interval := c.Query("interval")

	if symbol == "" || interval == "" {
		conn.WriteJSON(gin.H{"error": "symbol and interval are required"})
		conn.Close()
		return
	}

	// 注册订阅
	s.registerSubscription(symbol, interval, conn)

	// 发送欢迎消息
	conn.WriteJSON(gin.H{
		"type":    "welcome",
		"message": fmt.Sprintf("已成功订阅 %s %s K线数据", symbol, interval),
	})

	// 保持连接，直到客户端断开
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}

	// 客户端断开连接，取消订阅
	s.unregisterSubscription(symbol, interval, conn)
}

// registerSubscription 注册WebSocket订阅
func (s *Server) registerSubscription(symbol, interval string, conn *websocket.Conn) {
	s.subsMutex.Lock()
	defer s.subsMutex.Unlock()

	// 确保符号映射存在
	if _, exists := s.subscriptions[symbol]; !exists {
		s.subscriptions[symbol] = make(map[string][]*websocket.Conn)
	}
	// 确保间隔映射存在
	if _, exists := s.subscriptions[symbol][interval]; !exists {
		s.subscriptions[symbol][interval] = make([]*websocket.Conn, 0)
	}

	// 添加连接
	s.subscriptions[symbol][interval] = append(s.subscriptions[symbol][interval], conn)

	logging.Logger.WithFields(logrus.Fields{
		"symbol":   symbol,
		"interval": interval,
	}).Info("新的WebSocket订阅")
}

// unregisterSubscription 取消WebSocket订阅
func (s *Server) unregisterSubscription(symbol, interval string, conn *websocket.Conn) {
	s.subsMutex.Lock()
	defer s.subsMutex.Unlock()

	if intervals, exists := s.subscriptions[symbol]; exists {
		if conns, exists := intervals[interval]; exists {
			// 找到并移除连接
			for i, c := range conns {
				if c == conn {
					s.subscriptions[symbol][interval] = append(conns[:i], conns[i+1:]...)
					break
				}
			}

			// 如果没有连接，删除映射
			if len(s.subscriptions[symbol][interval]) == 0 {
				delete(s.subscriptions[symbol], interval)
			}
		}

		// 如果没有间隔，删除符号映射
		if len(s.subscriptions[symbol]) == 0 {
			delete(s.subscriptions, symbol)
		}
	}

	logging.Logger.WithFields(logrus.Fields{
		"symbol":   symbol,
		"interval": interval,
	}).Info("WebSocket订阅已取消")
}

// handleGetKlines 处理获取K线数据请求
func (s *Server) handleGetKlines(c *gin.Context) {
	symbol := c.Query("symbol")
	interval := c.Query("interval")
	limit := c.DefaultQuery("limit", "100")

	if symbol == "" || interval == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "symbol and interval are required"})
		return
	}

	// 解析limit
	limitInt, err := strconv.Atoi(limit)
	if err != nil || limitInt <= 0 {
		limitInt = 100
	}

	// 从数据库获取K线数据
	data, err := s.mysqlStore.QueryKlines(c.Request.Context(), symbol, interval, limitInt)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": data})
}

// handleGetHistory 处理获取历史数据请求
func (s *Server) handleGetHistory(c *gin.Context) {
	symbol := c.Query("symbol")
	interval := c.Query("interval")
	start := c.DefaultQuery("start", time.Now().Add(-24*time.Hour).Format(time.RFC3339))
	end := c.DefaultQuery("end", time.Now().Format(time.RFC3339))

	if symbol == "" || interval == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "symbol and interval are required"})
		return
	}

	// 解析时间
	startTime, err := time.Parse(time.RFC3339, start)
	if err != nil {
		startTime = time.Now().Add(-24 * time.Hour)
	}

	endTime, err := time.Parse(time.RFC3339, end)
	if err != nil {
		endTime = time.Now()
	}

	// 从数据库获取历史数据
	data, err := s.mysqlStore.QueryHistoryKlines(c.Request.Context(), symbol, interval, startTime, endTime)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": data})
}

// handleGetIndicators 处理获取技术指标请求
func (s *Server) handleGetIndicators(c *gin.Context) {
	// 预留技术指标接口
	c.JSON(http.StatusNotImplemented, gin.H{"message": "功能开发中"})
}

// handleGetStats 处理获取统计信息请求
func (s *Server) handleGetStats(c *gin.Context) {
	// 获取WebSocket订阅统计
	s.subsMutex.RLock()
	stats := make(map[string]interface{})

	totalSubscriptions := 0
	symbolStats := make(map[string]int)

	for symbol, intervals := range s.subscriptions {
		symbolTotal := 0
		for _, conns := range intervals {
			symbolTotal += len(conns)
			totalSubscriptions += len(conns)
		}
		symbolStats[symbol] = symbolTotal
	}

	stats["total_subscriptions"] = totalSubscriptions
	stats["symbols"] = symbolStats
	s.subsMutex.RUnlock()

	// 获取数据库统计
	dbStats, err := s.mysqlStore.GetStats(c.Request.Context())
	if err == nil {
		stats["database"] = dbStats
	}

	c.JSON(http.StatusOK, stats)
}

// handleGetSymbols 处理获取支持的交易对请求
func (s *Server) handleGetSymbols(c *gin.Context) {
	// 从数据库获取支持的交易对
	symbols, err := s.mysqlStore.GetAvailableSymbols(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"symbols": symbols})
}

// handleGetMultiKlines 处理获取多个交易对的K线数据请求
func (s *Server) handleGetMultiKlines(c *gin.Context) {
	symbolsStr := c.Query("symbols") // 格式: "BTCUSDT,ETHUSDT,BNBUSDT"
	interval := c.Query("interval")
	limit := c.DefaultQuery("limit", "100")

	if symbolsStr == "" || interval == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "symbols and interval are required"})
		return
	}

	symbols := strings.Split(symbolsStr, ",")

	// 解析limit
	limitInt, err := strconv.Atoi(limit)
	if err != nil || limitInt <= 0 {
		limitInt = 100
	}

	result := make(map[string][]*types.KLineData)

	// 并发获取每个交易对的K线数据
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, symbol := range symbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()

			// 从数据库获取K线数据
			data, err := s.mysqlStore.QueryKlines(c.Request.Context(), sym, interval, limitInt)
			if err == nil && len(data) > 0 {
				mu.Lock()
				result[sym] = data
				mu.Unlock()
			}
		}(symbol)
	}

	wg.Wait()

	c.JSON(http.StatusOK, gin.H{"data": result})
}

// handleCheckGaps 处理检查数据缺口请求
func (s *Server) handleCheckGaps(c *gin.Context) {
	symbol := c.Query("symbol")
	interval := c.Query("interval")
	startStr := c.DefaultQuery("start", time.Now().Add(-30*24*time.Hour).Format(time.RFC3339))
	endStr := c.DefaultQuery("end", time.Now().Format(time.RFC3339))

	// 参数验证
	if symbol == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "symbol parameter is required"})
		return
	}

	if interval == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "interval parameter is required"})
		return
	}

	// 解析时间
	startTime, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		startTime = time.Now().Add(-30 * 24 * time.Hour)
	}

	endTime, err := time.Parse(time.RFC3339, endStr)
	if err != nil {
		endTime = time.Now()
	}

	// 调用MySQL存储的检查缺口方法
	gaps, err := s.mysqlStore.CheckDataGaps(c.Request.Context(), symbol, interval, startTime, endTime)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 响应结果
	c.JSON(http.StatusOK, gin.H{
		"symbol":     symbol,
		"interval":   interval,
		"start_time": startTime.Format(time.RFC3339),
		"end_time":   endTime.Format(time.RFC3339),
		"gaps":       gaps,
		"gaps_count": len(gaps),
	})
}

// handleFixGaps 处理修复数据缺口请求
func (s *Server) handleFixGaps(c *gin.Context) {
	var req struct {
		Symbol   string `json:"symbol" binding:"required"`
		Interval string `json:"interval" binding:"required"`
		Start    string `json:"start"`
		End      string `json:"end"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 解析时间
	var startTime, endTime time.Time
	var err error

	if req.Start != "" {
		startTime, err = time.Parse(time.RFC3339, req.Start)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid start time format"})
			return
		}
	} else {
		startTime = time.Now().Add(-30 * 24 * time.Hour)
	}

	if req.End != "" {
		endTime, err = time.Parse(time.RFC3339, req.End)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid end time format"})
			return
		}
	} else {
		endTime = time.Now()
	}

	// 检查是否有缺口
	gaps, err := s.mysqlStore.CheckDataGaps(c.Request.Context(), req.Symbol, req.Interval, startTime, endTime)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if len(gaps) == 0 {
		c.JSON(http.StatusOK, gin.H{
			"message":  "No gaps found, no need to fix",
			"symbol":   req.Symbol,
			"interval": req.Interval,
		})
		return
	}

	// 启动修复任务（异步）
	go func() {
		ctx := context.Background()
		fixedGaps := 0

		for _, gap := range gaps {
			err := s.mysqlStore.FixDataGap(ctx, req.Symbol, req.Interval, gap.Start, gap.End)
			if err != nil {
				logging.Logger.WithFields(logrus.Fields{
					"symbol":   req.Symbol,
					"interval": req.Interval,
					"start":    gap.Start,
					"end":      gap.End,
					"error":    err,
				}).Error("修复数据缺口失败")
			} else {
				fixedGaps++
			}
		}

		logging.Logger.WithFields(logrus.Fields{
			"symbol":       req.Symbol,
			"interval":     req.Interval,
			"total_gaps":   len(gaps),
			"fixed_gaps":   fixedGaps,
			"failed_fixes": len(gaps) - fixedGaps,
		}).Info("数据缺口修复任务完成")
	}()

	// 立即返回响应，修复过程在后台进行
	c.JSON(http.StatusAccepted, gin.H{
		"message":    "Gap fixing task started",
		"symbol":     req.Symbol,
		"interval":   req.Interval,
		"gaps_count": len(gaps),
		"status":     "processing",
	})
}
