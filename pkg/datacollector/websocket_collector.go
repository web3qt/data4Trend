package datacollector

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	binance "github.com/adshao/go-binance/v2"
	"github.com/sirupsen/logrus"
	"github.com/web3qt/data4Trend/config"
	"github.com/web3qt/data4Trend/internal/types"
	"github.com/web3qt/data4Trend/pkg/logging"
)

// WebSocketCollector WebSocket数据收集器 - 专门收集1分钟K线数据
type WebSocketCollector struct {
	Client      *binance.Client
	DataChan    chan *types.KLineData
	config      *config.Config
	symbols     []string
	streams     map[string]chan struct{} // symbol -> stop channel
	streamsMu   sync.RWMutex
	dataManager *DataManager
	ctx         context.Context
	cancel      context.CancelFunc
}

// DataManager 管理数据存储和清理
type DataManager struct {
	dataStore map[string][]*types.KLineData // symbol -> kline data
	storeMu   sync.RWMutex
	maxDays   int // 保留天数
}

// NewDataManager 创建数据管理器
func NewDataManager(maxDays int) *DataManager {
	return &DataManager{
		dataStore: make(map[string][]*types.KLineData),
		maxDays:   maxDays,
	}
}

// AddData 添加数据
func (dm *DataManager) AddData(data *types.KLineData) {
	dm.storeMu.Lock()
	defer dm.storeMu.Unlock()

	symbol := data.Symbol
	dm.dataStore[symbol] = append(dm.dataStore[symbol], data)

	// 保持最近7天的数据
	cutoff := time.Now().AddDate(0, 0, -dm.maxDays)
	var filtered []*types.KLineData
	for _, kline := range dm.dataStore[symbol] {
		if kline.OpenTime.After(cutoff) {
			filtered = append(filtered, kline)
		}
	}
	dm.dataStore[symbol] = filtered
}

// GetDataStats 获取数据统计
func (dm *DataManager) GetDataStats() map[string]int {
	dm.storeMu.RLock()
	defer dm.storeMu.RUnlock()

	stats := make(map[string]int)
	for symbol, data := range dm.dataStore {
		stats[symbol] = len(data)
	}
	return stats
}

// NewWebSocketCollector 创建WebSocket收集器
func NewWebSocketCollector(cfg *config.Config) *WebSocketCollector {
	// 创建binance客户端
	client := binance.NewClient("", "") // 使用公共API，无需API密钥

	// 数据通道缓冲区大小
	dataChannelBuffer := cfg.Performance.DataChannelBuffer
	if dataChannelBuffer <= 0 {
		dataChannelBuffer = 50000
	}

	return &WebSocketCollector{
		Client:      client,
		DataChan:    make(chan *types.KLineData, dataChannelBuffer),
		config:      cfg,
		streams:     make(map[string]chan struct{}),
		dataManager: NewDataManager(7), // 保留7天数据
	}
}

// Start 启动WebSocket数据收集
func (wc *WebSocketCollector) Start(ctx context.Context) error {
	logging.Logger.Info("启动WebSocket数据收集器")

	wc.ctx, wc.cancel = context.WithCancel(ctx)

	// 清理器
	go wc.startDataCleaner()

	// 获取所有交易对
	symbols, err := wc.FetchAllCryptocurrencies(wc.ctx)
	if err != nil {
		return fmt.Errorf("获取交易对失败: %w", err)
	}
	wc.symbols = symbols

	logging.Logger.WithField("symbols_count", len(wc.symbols)).Info("准备开始WebSocket数据收集")

	// 分批启动WebSocket连接，避免同时创建过多连接
	batchSize := 100
	for i := 0; i < len(wc.symbols); i += batchSize {
		end := i + batchSize
		if end > len(wc.symbols) {
			end = len(wc.symbols)
		}

		// 启动当前批次的连接
		for j := i; j < end; j++ {
			symbol := wc.symbols[j]
			go wc.startSymbolStream(symbol)

			// 连接之间间隔，避免过快创建连接
			time.Sleep(500 * time.Millisecond)
		}

		// 批次之间较长间隔
		if end < len(wc.symbols) {
			logging.Logger.WithFields(logrus.Fields{
				"completed": end,
				"total":     len(wc.symbols),
			}).Info("完成一批WebSocket连接")
			time.Sleep(2 * time.Second)
		}
	}

	logging.Logger.WithField("total_symbols", len(wc.symbols)).Info("所有WebSocket连接已启动")

	// 等待上下文取消
	<-wc.ctx.Done()
	return nil
}

// startSymbolStream 为单个币种启动WebSocket流
func (wc *WebSocketCollector) startSymbolStream(symbol string) {
	logging.Logger.WithField("symbol", symbol).Debug("启动WebSocket流")

	wsKlineHandler := func(event *binance.WsKlineEvent) {
		// 只处理已完成的K线（Kline.IsFinal = true）
		if !event.Kline.IsFinal {
			return
		}

		// 转换数据格式
		klineData := &types.KLineData{
			Symbol:     event.Symbol,
			OpenTime:   time.Unix(0, event.Kline.StartTime*int64(time.Millisecond)),
			CloseTime:  time.Unix(0, event.Kline.EndTime*int64(time.Millisecond)),
			Interval:   "1m",
			OpenPrice:  parseFloat(event.Kline.Open),
			HighPrice:  parseFloat(event.Kline.High),
			LowPrice:   parseFloat(event.Kline.Low),
			ClosePrice: parseFloat(event.Kline.Close),
			Volume:     parseFloat(event.Kline.Volume),
		}

		// 添加到数据管理器
		wc.dataManager.AddData(klineData)

		// 发送到数据通道
		select {
		case wc.DataChan <- klineData:
			// 数据发送成功
		default:
			// 通道满，记录警告
			logging.Logger.WithField("symbol", symbol).Warn("数据通道已满，丢弃数据")
		}
	}

	errHandler := func(err error) {
		logging.Logger.WithFields(logrus.Fields{
			"symbol": symbol,
			"error":  err,
		}).Error("WebSocket连接错误")

		// 重连延迟
		time.Sleep(5 * time.Second)

		// 检查是否应该重连
		wc.streamsMu.RLock()
		stopChan, exists := wc.streams[symbol]
		wc.streamsMu.RUnlock()

		if exists {
			select {
			case <-stopChan:
				// 已被停止，不重连
				return
			default:
				// 重新启动连接
				logging.Logger.WithField("symbol", symbol).Info("重新启动WebSocket连接")
				go wc.startSymbolStream(symbol)
			}
		}
	}

	// 创建停止通道
	stopChan := make(chan struct{})
	wc.streamsMu.Lock()
	wc.streams[symbol] = stopChan
	wc.streamsMu.Unlock()

	// 启动WebSocket连接
	doneC, stopC, err := binance.WsKlineServe(symbol, "1m", wsKlineHandler, errHandler)
	if err != nil {
		logging.Logger.WithFields(logrus.Fields{
			"symbol": symbol,
			"error":  err,
		}).Error("启动WebSocket连接失败")
		return
	}

	logging.Logger.WithField("symbol", symbol).Info("WebSocket连接已建立")

	// 等待停止信号
	go func() {
		select {
		case <-stopChan:
			// 发送停止信号到binance websocket
			stopC <- struct{}{}
		case <-wc.ctx.Done():
			// 全局停止
			stopC <- struct{}{}
		}
	}()

	// 等待连接完成
	<-doneC
	logging.Logger.WithField("symbol", symbol).Info("WebSocket连接已关闭")
}

// startDataCleaner 启动数据清理器
func (wc *WebSocketCollector) startDataCleaner() {
	ticker := time.NewTicker(time.Hour) // 每小时清理一次
	defer ticker.Stop()

	logging.Logger.Info("数据清理器已启动")

	for {
		select {
		case <-wc.ctx.Done():
			logging.Logger.Info("数据清理器已停止")
			return
		case <-ticker.C:
			// 清理过期数据
			cutoff := time.Now().AddDate(0, 0, -7) // 7天前

			wc.dataManager.storeMu.Lock()
			for symbol, data := range wc.dataManager.dataStore {
				var filtered []*types.KLineData
				for _, kline := range data {
					if kline.OpenTime.After(cutoff) {
						filtered = append(filtered, kline)
					}
				}
				wc.dataManager.dataStore[symbol] = filtered
			}
			wc.dataManager.storeMu.Unlock()

			logging.Logger.Debug("数据清理完成")
		}
	}
}

// Stop 停止WebSocket收集器
func (wc *WebSocketCollector) Stop() {
	logging.Logger.Info("正在停止WebSocket数据收集器...")

	// 调用cancel函数，通知所有子goroutine停止
	if wc.cancel != nil {
		wc.cancel()
	}

	wc.streamsMu.Lock()
	defer wc.streamsMu.Unlock()

	// 关闭所有活动的WebSocket流
	for symbol, stopChan := range wc.streams {
		close(stopChan) // 关闭通道，通知goroutine退出
		delete(wc.streams, symbol)
		logging.Logger.WithField("symbol", symbol).Info("已关闭WebSocket流")
	}

	logging.Logger.Info("所有WebSocket流已成功关闭")
}

// GetActiveStreams 获取活跃流数量
func (wc *WebSocketCollector) GetActiveStreams() int {
	wc.streamsMu.RLock()
	defer wc.streamsMu.RUnlock()
	return len(wc.streams)
}

// GetSymbols 获取所有交易对
func (wc *WebSocketCollector) GetSymbols() []string {
	return wc.symbols
}

// GetDataStats 获取数据统计
func (wc *WebSocketCollector) GetDataStats() map[string]int {
	return wc.dataManager.GetDataStats()
}

// FetchAllCryptocurrencies 获取币安所有数字货币
func (wc *WebSocketCollector) FetchAllCryptocurrencies(ctx context.Context) ([]string, error) {
	logging.Logger.Info("获取币安所有数字货币")

	// 获取24小时价格变动信息（包含所有交易对）
	tickers, err := wc.Client.NewListPriceChangeStatsService().Do(ctx)
	if err != nil {
		logging.Logger.WithError(err).Error("获取24小时价格变动信息失败")
		// 如果API失败，返回一个基础的USDT交易对列表
		return []string{
			"BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT",
			"DOGEUSDT", "SOLUSDT", "DOTUSDT", "MATICUSDT", "LTCUSDT",
			"AVAXUSDT", "LINKUSDT", "ATOMUSDT", "UNIUSDT", "ETCUSDT",
			"TRXUSDT", "XLMUSDT", "VETUSDT", "ICPUSDT", "FILUSDT",
			"THETAUSDT", "XMRUSDT", "FTMUSDT", "ALGOUSDT", "HBARUSDT",
		}, nil
	}

	// 筛选出USDT交易对
	var usdtPairs []string
	for _, ticker := range tickers {
		if strings.HasSuffix(ticker.Symbol, "USDT") {
			usdtPairs = append(usdtPairs, ticker.Symbol)
		}
	}

	logging.Logger.WithFields(logrus.Fields{
		"total_pairs": len(usdtPairs),
		"all_tickers": len(tickers),
	}).Info("成功获取币安所有USDT交易对")

	return usdtPairs, nil
}

// parseFloat 解析字符串为float64
func parseFloat(s string) float64 {
	val := 0.0
	fmt.Sscanf(s, "%f", &val)
	return val
}
