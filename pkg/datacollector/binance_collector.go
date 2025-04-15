package datacollector

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	binance "github.com/adshao/go-binance/v2"
	"github.com/sirupsen/logrus"
	"github.com/web3qt/data4Trend/config"
	"github.com/web3qt/data4Trend/internal/types"
	"github.com/web3qt/data4Trend/internal/utils"
	"github.com/web3qt/data4Trend/pkg/logging"
)

// BinanceKlinesService 适配器
type BinanceKlinesService struct {
	service  *binance.KlinesService
	symbol   string // 添加字段保存设置的值
	interval string
}

func NewBinanceKlinesService(client *binance.Client) types.KlinesService {
	return &BinanceKlinesService{
		service: client.NewKlinesService(),
	}
}

func (b *BinanceKlinesService) Symbol(symbol string) types.KlinesService {
	b.symbol = symbol
	b.service.Symbol(symbol)
	return b
}

func (b *BinanceKlinesService) Interval(interval string) types.KlinesService {
	b.interval = interval
	b.service.Interval(interval)
	return b
}

func (b *BinanceKlinesService) Limit(limit int) types.KlinesService {
	b.service.Limit(limit)
	return b
}

func (b *BinanceKlinesService) StartTime(startTime int64) types.KlinesService {
	b.service.StartTime(startTime)
	return b
}

func (b *BinanceKlinesService) EndTime(endTime int64) types.KlinesService {
	b.service.EndTime(endTime)
	return b
}

func (b *BinanceKlinesService) Do(ctx context.Context) ([]*types.KLineData, error) {
	logging.Logger.WithFields(logrus.Fields{
		"symbol":   b.symbol,
		"interval": b.interval,
	}).Debug("执行Binance API调用")

	klines, err := b.service.Do(ctx)
	if err != nil {
		logging.Logger.WithFields(logrus.Fields{
			"symbol":   b.symbol,
			"interval": b.interval,
			"error":    err,
		}).Error("Binance API调用失败")
		return nil, err
	}

	logging.Logger.WithFields(logrus.Fields{
		"symbol":      b.symbol,
		"interval":    b.interval,
		"data_points": len(klines),
	}).Debug("Binance API调用成功")

	// 打印一些获取到的数据
	if len(klines) > 0 {
		firstKline := klines[0]
		logging.Logger.WithFields(logrus.Fields{
			"open_time":  time.Unix(firstKline.OpenTime/1000, 0),
			"close_time": time.Unix(firstKline.CloseTime/1000, 0),
			"open":       firstKline.Open,
			"high":       firstKline.High,
			"low":        firstKline.Low,
			"close":      firstKline.Close,
			"volume":     firstKline.Volume,
		}).Debug("第一条K线数据")
	}

	result := make([]*types.KLineData, len(klines))
	for i, k := range klines {
		result[i] = &types.KLineData{
			OpenTime:  time.Unix(k.OpenTime/1000, 0),
			CloseTime: time.Unix(k.CloseTime/1000, 0),
			Open:      utils.ParseFloat(k.Open),
			High:      utils.ParseFloat(k.High),
			Low:       utils.ParseFloat(k.Low),
			Close:     utils.ParseFloat(k.Close),
			Volume:    utils.ParseFloat(k.Volume),
			Symbol:    b.symbol,   // 使用保存的值
			Interval:  b.interval, // 使用保存的值
		}
	}
	return result, nil
}

type BinanceCollector struct {
	Client       *binance.Client
	DataChan     chan *types.KLineData
	Collectors   map[string]map[string]*SymbolCollector // symbol -> interval -> collector
	CollectorsMu sync.RWMutex
	config       *config.Config
	workers      int                 // 并发工作器数量
	workerPool   chan struct{}       // 工作器池，用于限制并发数量
	taskQueue    chan CollectionTask // 任务队列
	lastSaveHour int                 // 上次保存进度的小时
}

// CollectionTask 定义收集任务
type CollectionTask struct {
	Symbol    string
	Interval  string
	StartTime time.Time
	EndTime   time.Time
	Priority  int // 优先级，数字越小优先级越高
}

func NewBinanceCollector(cfg *config.Config) *BinanceCollector {
	if cfg == nil {
		logging.Logger.Panic("无效的Binance配置: 配置为空")
	}

	// 根据是否有API密钥决定使用认证客户端还是公共客户端
	var client *binance.Client
	if cfg.Binance.APIKey != "" && cfg.Binance.SecretKey != "" {
		// 如果提供了API密钥，使用认证客户端
		logging.Logger.Info("使用认证客户端创建Binance连接")
		client = binance.NewClient(cfg.Binance.APIKey, cfg.Binance.SecretKey)
	} else {
		// 否则使用公共客户端
		logging.Logger.Info("使用公共客户端创建Binance连接（无API密钥）")
		client = binance.NewClient("", "")
	}
	client.HTTPClient = cfg.NewHTTPClient()

	// 减少工作器数量，避免同时发出太多请求
	workers := 3

	// 记录配置
	logging.Logger.WithFields(logrus.Fields{
		"api_key_set":   cfg.Binance.APIKey != "",
		"proxy":         cfg.HTTP.Proxy,
		"worker_count":  workers,
		"symbols_count": len(cfg.Binance.Symbols),
	}).Info("初始化Binance收集器")

	return &BinanceCollector{
		Client:       client,
		DataChan:     make(chan *types.KLineData, 5000), // 显著增大缓冲区
		Collectors:   make(map[string]map[string]*SymbolCollector),
		config:       cfg,
		workers:      workers,
		workerPool:   make(chan struct{}, workers),
		taskQueue:    make(chan CollectionTask, 500), // 减小任务队列容量以控制内存使用
		lastSaveHour: -1,                              // 初始化为-1
	}
}

func (b *BinanceCollector) Start(ctx context.Context) error {
	// 启动工作器池
	for i := 0; i < b.workers; i++ {
		go b.worker(ctx)
	}

	// 启动周期性数据缺口检查
	go b.startPeriodicGapCheck(ctx)

	// 初始化并启动所有收集器
	symbolManager, err := b.config.GetSymbolManager()
	if err != nil {
		// 如果无法获取符号管理器，使用配置中的符号
		logging.Logger.WithError(err).Warn("无法获取符号管理器，使用配置中的符号")
		return b.startWithSymbols(ctx, b.config.Binance.Symbols)
	}

	// 获取所有启用的符号
	symbols := symbolManager.GetAllSymbols()
	logging.Logger.WithField("count", len(symbols)).Info("从符号管理器获取到币种")

	// 确保BTC和ETH交易对在列表中且被启用
	hasETH := false
	hasBTC := false

	for _, sym := range symbols {
		if sym.Symbol == "ETHUSDT" {
			hasETH = true
			sym.Enabled = true
		}
		if sym.Symbol == "BTCUSDT" {
			hasBTC = true
			sym.Enabled = true
		}
	}

	// 如果没有找到这些交易对，添加它们
	if !hasETH {
		logging.Logger.Info("添加ETHUSDT交易对")
		ethSymbol := config.SymbolConfig{
			Symbol:      "ETHUSDT",
			Enabled:     true,
			Intervals:   []string{"1m", "5m", "15m", "1h", "4h", "1d"},
			HourlyStart: time.Now().Add(-24 * time.Hour).Format(time.RFC3339),
		}
		symbols = append(symbols, ethSymbol)
	}

	if !hasBTC {
		logging.Logger.Info("添加BTCUSDT交易对")
		btcSymbol := config.SymbolConfig{
			Symbol:      "BTCUSDT",
			Enabled:     true,
			Intervals:   []string{"1m", "5m", "15m", "1h", "4h", "1d"},
			HourlyStart: time.Now().Add(-24 * time.Hour).Format(time.RFC3339),
		}
		symbols = append(symbols, btcSymbol)
	}

	return b.startWithSymbols(ctx, symbols)
}

// startWithSymbols 使用指定的符号列表启动收集器
func (b *BinanceCollector) startWithSymbols(ctx context.Context, symbols []config.SymbolConfig) error {
	logging.Logger.WithField("count", len(symbols)).Info("开始初始化Binance收集器")

	// 启动任务调度器
	go b.scheduler(ctx)

	// 检查是否有启用的交易对
	enabledCount := 0
	for _, symbolCfg := range symbols {
		if symbolCfg.Enabled {
			enabledCount++
		}
	}
	logging.Logger.WithFields(logrus.Fields{
		"enabled": enabledCount,
		"total":   len(symbols),
	}).Info("启用的交易对数量")

	if enabledCount == 0 {
		logging.Logger.Warn("没有启用的交易对，将不会收集任何数据")
	}

	// 批量创建收集器
	logging.Logger.Info("开始创建收集器")
	for i, symbolCfg := range symbols {
		logging.Logger.WithFields(logrus.Fields{
			"index":   i + 1,
			"symbol":  symbolCfg.Symbol,
			"enabled": symbolCfg.Enabled,
		}).Debug("初始化交易对")

		if !symbolCfg.Enabled {
			logging.Logger.WithField("symbol", symbolCfg.Symbol).Debug("跳过未启用的交易对")
			continue
		}

		// 检查开始时间
		if symbolCfg.MinuteStart != "" || symbolCfg.HourlyStart != "" || symbolCfg.DailyStart != "" {
			logging.Logger.WithFields(logrus.Fields{
				"symbol":       symbolCfg.Symbol,
				"minute_start": symbolCfg.MinuteStart,
				"hourly_start": symbolCfg.HourlyStart,
				"daily_start":  symbolCfg.DailyStart,
			}).Debug("交易对开始时间")
		} else {
			logging.Logger.WithField("symbol", symbolCfg.Symbol).Warn("交易对没有设置任何开始时间")
		}

		// 使用适配器创建服务
		service := NewBinanceKlinesService(b.Client)
		collector, err := NewSymbolCollector(service, symbolCfg, b.taskQueue, b.DataChan)
		if err != nil {
			logging.Logger.WithFields(logrus.Fields{
				"symbol": symbolCfg.Symbol,
				"error":  err,
			}).Error("创建交易对收集器失败，跳过此交易对")
			continue // 跳过此交易对，而不是整个失败
		}

		// 收集器添加到映射
		b.CollectorsMu.Lock()
		if _, ok := b.Collectors[symbolCfg.Symbol]; !ok {
			b.Collectors[symbolCfg.Symbol] = make(map[string]*SymbolCollector)
		}
		b.Collectors[symbolCfg.Symbol]["default"] = collector
		b.CollectorsMu.Unlock()

		// 尝试启动收集器
		err = collector.Start()
		if err != nil {
			logging.Logger.WithFields(logrus.Fields{
				"symbol": symbolCfg.Symbol,
				"error":  err,
			}).Error("启动交易对收集器失败，跳过此交易对")
			continue // 跳过此交易对，而不是整个失败
		}
	}

	// 如果没有成功创建任何收集器，记录警告但不返回错误
	b.CollectorsMu.RLock()
	collectorCount := len(b.Collectors)
	b.CollectorsMu.RUnlock()

	if collectorCount == 0 && enabledCount > 0 {
		logging.Logger.Warn("没有成功创建任何交易对收集器，但服务将继续运行")
	} else {
		logging.Logger.WithField("count", collectorCount).Info("成功创建的交易对收集器数量")
	}

	return nil
}

// worker 处理收集任务的工作器
func (b *BinanceCollector) worker(ctx context.Context) {
	for {
		// 获取一个工作器槽位
		select {
		case <-ctx.Done():
			return
		case b.workerPool <- struct{}{}:
			// 获取到一个工作器槽位，继续处理
		}

		// 获取任务
		var task CollectionTask
		select {
		case <-ctx.Done():
			// 取消上下文，退出工作器
			<-b.workerPool // 释放槽位
			return
		case task = <-b.taskQueue:
			// 获取到一个任务，处理它
		}

		// 执行任务
		b.executeTask(ctx, task)

		// 释放工作器槽位
		<-b.workerPool

		// 添加请求间隔，避免API请求过于频繁
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond): // 每个任务之间至少间隔500毫秒
			// 继续下一个任务
		}
	}
}

// executeTask 执行数据收集任务
func (b *BinanceCollector) executeTask(ctx context.Context, task CollectionTask) {
	symbol := task.Symbol
	interval := task.Interval
	startTime := task.StartTime
	endTime := task.EndTime

	logging.Logger.WithFields(logrus.Fields{
		"symbol":     symbol,
		"interval":   interval,
		"start_time": startTime.Format(time.RFC3339),
		"end_time":   endTime.Format(time.RFC3339),
	}).Debug("执行K线数据收集任务")

	// 计算最大持续时间
	maxDuration := b.getMaxDurationForInterval(interval)
	
	// 限制单次请求的时间范围，分批获取数据
	currentStart := startTime
	for currentStart.Before(endTime) {
		// 计算当前批次的结束时间
		currentEnd := currentStart.Add(maxDuration)
		if currentEnd.After(endTime) {
			currentEnd = endTime
		}
		
		// 如果时间段小于一个K线周期，跳过
		intervalDuration := calculateIntervalDuration(interval)
		if currentEnd.Sub(currentStart) < intervalDuration {
			break
		}

		// 创建Binance K线服务
		service := NewBinanceKlinesService(b.Client)

		// 设置K线服务参数
		service.Symbol(symbol).
			Interval(interval).
			StartTime(currentStart.UnixMilli()).
			EndTime(currentEnd.UnixMilli()).
			Limit(1000) // 使用最大限制以减少API调用次数

		// 执行K线数据获取
		klines, err := service.Do(ctx)
		
		// 处理错误
		if err != nil {
			logging.Logger.WithFields(logrus.Fields{
				"symbol":     symbol,
				"interval":   interval,
				"start_time": currentStart,
				"end_time":   currentEnd,
				"error":      err,
			}).Error("获取K线数据失败")
			
			// 检查是否是API限流错误
			if strings.Contains(err.Error(), "Too many requests") || 
			   strings.Contains(err.Error(), "rate limit") {
				logging.Logger.Warn("检测到API限流，暂停请求10秒")
				select {
				case <-ctx.Done():
					return
				case <-time.After(10 * time.Second):
					// 继续尝试
					continue
				}
			}
			
			// 其他错误，暂停一段时间后继续
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
				// 继续下一个时间段
				currentStart = currentEnd
				continue
			}
		}

		// 处理获取到的K线数据
		if len(klines) > 0 {
			logging.Logger.WithFields(logrus.Fields{
				"symbol":      symbol,
				"interval":    interval,
				"start_time":  currentStart,
				"end_time":    currentEnd,
				"data_points": len(klines),
			}).Debug("成功获取K线数据")

			// 检查并填补数据缺口
			if len(klines) > 1 {
				intervalDuration := calculateIntervalDuration(interval)
				b.checkAndFillGaps(ctx, symbol, interval, klines, intervalDuration)
			}

			// 发送数据到通道
			for _, kline := range klines {
				select {
				case <-ctx.Done():
					return
				case b.DataChan <- kline:
					// 数据发送成功
				}
			}

			// 更新收集器的起始时间
			if len(klines) > 0 {
				lastKline := klines[len(klines)-1]
				// 更新为最后一个K线的关闭时间加1毫秒
				newTime := lastKline.CloseTime.Add(time.Millisecond)
				b.updateCollectorStartTime(symbol, interval, newTime)
			}
		} else {
			logging.Logger.WithFields(logrus.Fields{
				"symbol":    symbol,
				"interval":  interval,
				"start":     currentStart,
				"end":       currentEnd,
			}).Debug("时间范围内没有K线数据")
		}

		// 移动到下一个时间段
		currentStart = currentEnd
		
		// 添加请求间隔，避免API请求过于频繁
		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond): // 每个批次之间至少间隔200毫秒
			// 继续下一个批次
		}
	}
}

// normalizeKlineOpenTime 将K线开始时间标准化为符合间隔的时间
func normalizeKlineOpenTime(t time.Time, interval string) time.Time {
	switch interval {
	case "1m":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
	case "5m":
		minute := t.Minute() - (t.Minute() % 5)
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), minute, 0, 0, t.Location())
	case "15m":
		minute := t.Minute() - (t.Minute() % 15)
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), minute, 0, 0, t.Location())
	case "30m":
		minute := t.Minute() - (t.Minute() % 30)
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), minute, 0, 0, t.Location())
	case "1h":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	case "2h":
		hour := t.Hour() - (t.Hour() % 2)
		return time.Date(t.Year(), t.Month(), t.Day(), hour, 0, 0, 0, t.Location())
	case "4h":
		hour := t.Hour() - (t.Hour() % 4)
		return time.Date(t.Year(), t.Month(), t.Day(), hour, 0, 0, 0, t.Location())
	case "6h":
		hour := t.Hour() - (t.Hour() % 6)
		return time.Date(t.Year(), t.Month(), t.Day(), hour, 0, 0, 0, t.Location())
	case "8h":
		hour := t.Hour() - (t.Hour() % 8)
		return time.Date(t.Year(), t.Month(), t.Day(), hour, 0, 0, 0, t.Location())
	case "12h":
		hour := t.Hour() - (t.Hour() % 12)
		return time.Date(t.Year(), t.Month(), t.Day(), hour, 0, 0, 0, t.Location())
	case "1d":
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case "3d":
		days := int(t.Day()) - ((int(t.Day()) - 1) % 3)
		return time.Date(t.Year(), t.Month(), days, 0, 0, 0, 0, t.Location())
	case "1w":
		// 计算到本周一的偏移
		dayOffset := int(t.Weekday())
		if dayOffset == 0 { // 周日
			dayOffset = 6
		} else {
			dayOffset--
		}
		return time.Date(t.Year(), t.Month(), t.Day()-dayOffset, 0, 0, 0, 0, t.Location())
	default:
		return t
	}
}

// updateCollectorStartTime 更新收集器的开始时间点
func (b *BinanceCollector) updateCollectorStartTime(symbol, interval string, newTime time.Time) {
	b.CollectorsMu.RLock()
	defer b.CollectorsMu.RUnlock()

	// 查找对应的收集器
	if symbolCollectors, exists := b.Collectors[symbol]; exists {
		if collector, exists := symbolCollectors["default"]; exists {
			// 查找对应的时间周期收集器
			collector.intervalsMu.RLock()
			defer collector.intervalsMu.RUnlock()

			if intervalCollector, exists := collector.intervals[interval]; exists {
				// 只有当新时间点比现有的更晚时才更新
				if newTime.After(intervalCollector.startTime) {
					// 计算下一个K线的开始时间
					var nextStartTime time.Time

					// 根据周期类型计算下一个开始时间
					switch interval {
					case "1m":
						nextStartTime = newTime.Add(1 * time.Minute)
					case "5m":
						nextStartTime = newTime.Add(5 * time.Minute)
					case "15m":
						nextStartTime = newTime.Add(15 * time.Minute)
					case "30m":
						nextStartTime = newTime.Add(30 * time.Minute)
					case "1h":
						nextStartTime = newTime.Add(1 * time.Hour)
					case "2h":
						nextStartTime = newTime.Add(2 * time.Hour)
					case "4h":
						nextStartTime = newTime.Add(4 * time.Hour)
					case "6h":
						nextStartTime = newTime.Add(6 * time.Hour)
					case "8h":
						nextStartTime = newTime.Add(8 * time.Hour)
					case "12h":
						nextStartTime = newTime.Add(12 * time.Hour)
					case "1d":
						nextStartTime = newTime.Add(24 * time.Hour)
					case "3d":
						nextStartTime = newTime.Add(3 * 24 * time.Hour)
					case "1w":
						nextStartTime = newTime.Add(7 * 24 * time.Hour)
					default:
						nextStartTime = newTime.Add(1 * time.Hour) // 默认增加1小时
					}

					// 更新收集器的开始时间
					intervalCollector.updateStartTime(nextStartTime)

					logging.Logger.WithFields(logrus.Fields{
						"symbol":     symbol,
						"interval":   interval,
						"new_time":   nextStartTime.Format(time.RFC3339),
						"close_time": newTime.Format(time.RFC3339),
					}).Info("更新收集器开始时间")

					// 每小时保存进度到配置文件
					hourNow := time.Now().Hour()
					if b.lastSaveHour != hourNow {
						b.lastSaveHour = hourNow
						go b.saveProgress()
					}
				}
			}
		}
	}
}

// saveProgress 保存进度到配置文件
func (b *BinanceCollector) saveProgress() {
	b.CollectorsMu.RLock()
	defer b.CollectorsMu.RUnlock()

	// 检查配置
	if b.config == nil {
		logging.Logger.Error("无法保存进度：配置为空")
		return
	}

	// 收集所有收集器的状态
	states := make(map[string]map[string]time.Time)

	// 遍历所有收集器，收集当前状态
	for symbol, collectors := range b.Collectors {
		for _, collector := range collectors {
			collector.intervalsMu.RLock()

			// 为每个符号创建间隔映射
			if _, exists := states[symbol]; !exists {
				states[symbol] = make(map[string]time.Time)
			}

			// 收集每个间隔的开始时间
			for interval, ic := range collector.intervals {
				states[symbol][interval] = ic.startTime
			}

			collector.intervalsMu.RUnlock()
		}
	}

	// 如果有状态数据
	if len(states) > 0 {
		// 使用配置的SaveCollectorState方法保存状态
		if err := b.config.SaveCollectorState(states); err != nil {
			logging.Logger.WithError(err).Error("保存收集器状态失败")
			return
		}

		logging.Logger.WithField("symbols_count", len(states)).Info("成功保存收集器状态")
	} else {
		logging.Logger.Debug("没有收集器状态需要保存")
	}
}

// getMaxDurationForInterval 根据间隔类型获取最大时间范围
func (b *BinanceCollector) getMaxDurationForInterval(interval string) time.Duration {
	switch interval {
	case "1m":
		return 24 * time.Hour // 1分钟K线最多获取1天
	case "5m":
		return 5 * 24 * time.Hour // 5分钟K线最多获取5天
	case "15m":
		return 10 * 24 * time.Hour // 15分钟K线最多获取10天
	case "1h":
		return 30 * 24 * time.Hour // 1小时K线最多获取30天
	case "4h":
		return 60 * 24 * time.Hour // 4小时K线最多获取60天
	case "1d":
		return 365 * 24 * time.Hour // 日K线最多获取1年
	default:
		return 30 * 24 * time.Hour // 默认限制30天
	}
}

// scheduler 周期性调度收集任务
func (b *BinanceCollector) scheduler(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second) // 每10秒检查一次需要执行的任务
	defer ticker.Stop()

	// 保存收集进度的定时器
	saveProgressTicker := time.NewTicker(15 * time.Minute)
	defer saveProgressTicker.Stop()

	// 任务计数器，用于限制每次迭代中添加的任务数量
	var tasksAdded int
	var lastTaskTime time.Time

	logging.Logger.Info("任务调度器已启动")

	for {
		select {
		case <-ctx.Done():
			logging.Logger.Info("任务调度器收到停止信号")
			return
			
		case <-saveProgressTicker.C:
			// 每15分钟保存一次进度
			b.saveProgress()
			
		case <-ticker.C:
			// 重置计数器
			tasksAdded = 0
			symbols := b.GetActiveSymbols()
			
			logging.Logger.WithField("count", len(symbols)).Debug("正在为活跃币种调度任务")
			
			// 随机打乱符号列表，避免总是按同一顺序处理
			rand.Shuffle(len(symbols), func(i, j int) {
				symbols[i], symbols[j] = symbols[j], symbols[i]
			})
			
			// 限制每次处理的符号数量
			maxSymbolsPerCycle := 10
			if len(symbols) > maxSymbolsPerCycle {
				symbols = symbols[:maxSymbolsPerCycle]
			}

			// 为每个活跃的币种调度任务
			for _, symbol := range symbols {
				b.CollectorsMu.RLock()
				intervals, ok := b.Collectors[symbol]
				b.CollectorsMu.RUnlock()

				if !ok {
					continue
				}

				// 对每个间隔检查是否需要调度任务
				for interval, collector := range intervals {
					// 跳过已禁用的收集器
					if !collector.IsEnabled() {
						continue
					}

					// 获取上次收集的时间
					lastCollect := collector.GetLastCollect()
					
					// 跳过最近才收集过的间隔
					pollInterval := collector.GetPollInterval()
					if time.Since(lastCollect) < pollInterval {
						continue
					}

					// 计算开始和结束时间
					now := time.Now()
					startTime := collector.GetStartTime()
					
					// 如果开始时间为零，使用配置的初始时间
					if startTime.IsZero() {
						startTime = collector.GetInitialStartTime()
					}
					
					// 限制单次任务的时间范围
					maxDuration := b.getMaxDurationForInterval(interval)
					endTime := startTime.Add(maxDuration)
					if endTime.After(now) {
						endTime = now
					}
					
					// 如果开始时间和结束时间相同或开始时间晚于结束时间，跳过
					if startTime.Equal(endTime) || startTime.After(endTime) {
						continue
					}

					// 创建并添加任务
					task := CollectionTask{
						Symbol:    symbol,
						Interval:  interval,
						StartTime: startTime,
						EndTime:   endTime,
						Priority:  collector.GetPriority(),
					}
					
					// 限制任务添加频率
					if tasksAdded > 0 && time.Since(lastTaskTime) < 100*time.Millisecond {
						// 添加小延迟，避免短时间内添加太多任务
						time.Sleep(100 * time.Millisecond)
					}
					
					// 更新收集器的上次收集时间
					collector.UpdateLastCollect()
					
					// 添加任务到队列
					select {
					case <-ctx.Done():
						return
					case b.taskQueue <- task:
						tasksAdded++
						lastTaskTime = time.Now()
						
						logging.Logger.WithFields(logrus.Fields{
							"symbol":     symbol,
							"interval":   interval,
							"start_time": startTime,
							"end_time":   endTime,
						}).Debug("已调度K线数据收集任务")
					default:
						// 任务队列已满，跳过
						logging.Logger.WithFields(logrus.Fields{
							"symbol":   symbol,
							"interval": interval,
						}).Warn("任务队列已满，跳过任务")
					}
					
					// 每添加几个任务后暂停一下，避免短时间内添加太多任务
					if tasksAdded%5 == 0 {
						select {
						case <-ctx.Done():
							return
						case <-time.After(500 * time.Millisecond):
							// 继续添加任务
						}
					}
				}
			}
			
			// 记录本次调度的任务数量
			if tasksAdded > 0 {
				logging.Logger.WithField("tasks_added", tasksAdded).Info("已调度任务")
			}
		}
	}
}

// AddSymbol 添加一个新的交易对收集器
func (b *BinanceCollector) AddSymbol(symbolCfg config.SymbolConfig) error {
	b.CollectorsMu.Lock()
	defer b.CollectorsMu.Unlock()

	// 检查交易对是否已存在
	if _, exists := b.Collectors[symbolCfg.Symbol]; exists {
		// 如果已存在，更新配置
		for _, intervalCollector := range b.Collectors[symbolCfg.Symbol] {
			if err := intervalCollector.UpdateConfig(symbolCfg); err != nil {
				return err
			}
		}
		return nil
	}

	// 创建新的交易对收集器
	service := NewBinanceKlinesService(b.Client)
	collector, err := NewSymbolCollector(service, symbolCfg, b.taskQueue, b.DataChan)
	if err != nil {
		return err
	}

	// 存储收集器
	b.Collectors[symbolCfg.Symbol] = make(map[string]*SymbolCollector)
	b.Collectors[symbolCfg.Symbol]["default"] = collector

	// 启动收集器
	return collector.Start()
}

// RemoveSymbol 移除一个交易对收集器
func (b *BinanceCollector) RemoveSymbol(symbol string) {
	b.CollectorsMu.Lock()
	defer b.CollectorsMu.Unlock()

	if collectors, exists := b.Collectors[symbol]; exists {
		// 停止所有收集器
		for _, collector := range collectors {
			collector.Stop()
		}
		// 移除收集器
		delete(b.Collectors, symbol)
	}
}

// UpdateSymbol 更新交易对配置
func (b *BinanceCollector) UpdateSymbol(symbolCfg config.SymbolConfig) error {
	b.CollectorsMu.Lock()
	defer b.CollectorsMu.Unlock()

	if collectors, exists := b.Collectors[symbolCfg.Symbol]; exists {
		// 更新所有收集器
		for _, collector := range collectors {
			if err := collector.UpdateConfig(symbolCfg); err != nil {
				return err
			}
		}
		return nil
	}

	// 如果不存在，添加新的收集器
	return b.AddSymbol(symbolCfg)
}

// 获取所有正在收集的币种
func (b *BinanceCollector) GetActiveSymbols() []string {
	b.CollectorsMu.RLock()
	defer b.CollectorsMu.RUnlock()

	symbols := make([]string, 0, len(b.Collectors))
	for symbol := range b.Collectors {
		symbols = append(symbols, symbol)
	}
	return symbols
}

// 获取收集器状态
func (b *BinanceCollector) GetCollectorStatus() map[string]map[string]string {
	b.CollectorsMu.RLock()
	defer b.CollectorsMu.RUnlock()

	status := make(map[string]map[string]string)
	for symbol, collectors := range b.Collectors {
		symbolStatus := make(map[string]string)
		for interval, collector := range collectors {
			if collector.enabled {
				symbolStatus[interval] = "running"
			} else {
				symbolStatus[interval] = "stopped"
			}
		}
		status[symbol] = symbolStatus
	}
	return status
}

// 设置工作器数量
func (b *BinanceCollector) SetWorkerCount(count int) {
	if count <= 0 {
		return
	}

	b.workers = count
	// 重新创建工作器池
	close(b.workerPool)
	b.workerPool = make(chan struct{}, count)
}

// calculateIntervalDuration 计算周期的时间间隔
func calculateIntervalDuration(interval string) time.Duration {
	switch interval {
	case "1m":
		return 1 * time.Minute
	case "5m":
		return 5 * time.Minute
	case "15m":
		return 15 * time.Minute
	case "30m":
		return 30 * time.Minute
	case "1h":
		return 1 * time.Hour
	case "2h":
		return 2 * time.Hour
	case "4h":
		return 4 * time.Hour
	case "6h":
		return 6 * time.Hour
	case "8h":
		return 8 * time.Hour
	case "12h":
		return 12 * time.Hour
	case "1d":
		return 24 * time.Hour
	case "3d":
		return 3 * 24 * time.Hour
	case "1w":
		return 7 * 24 * time.Hour
	default:
		return 0
	}
}

// checkAndFillGaps 检查并填补数据缺口
func (b *BinanceCollector) checkAndFillGaps(ctx context.Context, symbol, interval string, klines []*types.KLineData, expectedDuration time.Duration) bool {
	if len(klines) < 2 {
		return false
	}

	// 按时间排序
	sort.Slice(klines, func(i, j int) bool {
		return klines[i].OpenTime.Before(klines[j].OpenTime)
	})

	// 找出所有缺口
	var gaps []struct {
		start time.Time
		end   time.Time
	}

	for i := 1; i < len(klines); i++ {
		timeDiff := klines[i].OpenTime.Sub(klines[i-1].OpenTime)
		if timeDiff > time.Duration(float64(expectedDuration)*1.5) {
			// 发现缺口
			gap := struct {
				start time.Time
				end   time.Time
			}{
				start: klines[i-1].OpenTime.Add(expectedDuration),
				end:   klines[i].OpenTime,
			}
			gaps = append(gaps, gap)
		}
	}

	if len(gaps) == 0 {
		return false
	}

	logging.Logger.WithFields(logrus.Fields{
		"symbol":     symbol,
		"interval":   interval,
		"gaps_count": len(gaps),
	}).Info("检测到数据缺口")

	// 处理每个缺口
	for _, gap := range gaps {
		maxDuration := b.getMaxDurationForInterval(interval)
		if gap.end.Sub(gap.start) > maxDuration {
			// 缺口太大，分割处理
			currentStart := gap.start
			for currentStart.Before(gap.end) {
				currentEnd := currentStart.Add(maxDuration)
				if currentEnd.After(gap.end) {
					currentEnd = gap.end
				}

				// 创建填补缺口的任务
				gapTask := CollectionTask{
					Symbol:    symbol,
					Interval:  interval,
					StartTime: currentStart,
					EndTime:   currentEnd,
					Priority:  1, // 高优先级
				}

				logging.Logger.WithFields(logrus.Fields{
					"symbol":     symbol,
					"interval":   interval,
					"start_time": currentStart,
					"end_time":   currentEnd,
				}).Info("创建填补缺口任务")

				// 将任务添加到队列
				select {
				case b.taskQueue <- gapTask:
					logging.Logger.Debug("填补缺口任务已添加到队列")
				default:
					logging.Logger.Warn("任务队列已满，无法添加填补缺口任务")
				}

				currentStart = currentEnd
			}
		} else {
			// 直接创建填补缺口的任务
			gapTask := CollectionTask{
				Symbol:    symbol,
				Interval:  interval,
				StartTime: gap.start,
				EndTime:   gap.end,
				Priority:  1, // 高优先级
			}

			logging.Logger.WithFields(logrus.Fields{
				"symbol":     symbol,
				"interval":   interval,
				"start_time": gap.start,
				"end_time":   gap.end,
			}).Info("创建填补缺口任务")

			// 将任务添加到队列
			select {
			case b.taskQueue <- gapTask:
				logging.Logger.Debug("填补缺口任务已添加到队列")
			default:
				logging.Logger.Warn("任务队列已满，无法添加填补缺口任务")
			}
		}
	}

	return true
}

// startPeriodicGapCheck 启动周期性数据缺口检查
func (b *BinanceCollector) startPeriodicGapCheck(ctx context.Context) {
	// 每6小时执行一次完整检查
	ticker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()

	logging.Logger.Info("启动周期性数据缺口检查")

	// 首次启动后延迟5分钟执行第一次检查，让系统稳定
	time.Sleep(5 * time.Minute)

	// 执行一次初始检查
	b.checkDatabaseGaps(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.checkDatabaseGaps(ctx)
		}
	}
}

// checkDatabaseGaps 检查数据库中的数据缺口
func (b *BinanceCollector) checkDatabaseGaps(ctx context.Context) {
	logging.Logger.Info("开始执行数据库数据缺口检查")

	// 获取活跃的符号
	b.CollectorsMu.RLock()
	symbols := make([]string, 0, len(b.Collectors))
	for symbol := range b.Collectors {
		symbols = append(symbols, symbol)
	}
	b.CollectorsMu.RUnlock()

	// 检查每个符号的数据
	for _, symbol := range symbols {
		// 检查不同时间周期
		intervals := []string{"1m", "5m", "15m", "30m", "1h", "4h", "1d"}
		for _, interval := range intervals {
			// 获取该币种该时间周期的所有数据
			data, err := b.fetchHistoricalData(ctx, symbol, interval)
			if err != nil {
				logging.Logger.WithFields(logrus.Fields{
					"symbol":   symbol,
					"interval": interval,
					"error":    err,
				}).Error("获取历史数据失败")
				continue
			}

			if len(data) < 2 {
				logging.Logger.WithFields(logrus.Fields{
					"symbol":   symbol,
					"interval": interval,
					"count":    len(data),
				}).Debug("数据点太少，跳过检查")
				continue
			}

			// 检查数据缺口
			expectedDuration := calculateIntervalDuration(interval)
			if expectedDuration > 0 {
				b.checkAndFillGaps(ctx, symbol, interval, data, expectedDuration)
			}
		}
	}

	logging.Logger.Info("数据库数据缺口检查完成")
}

// fetchHistoricalData 从数据库获取历史数据
func (b *BinanceCollector) fetchHistoricalData(ctx context.Context, symbol, interval string) ([]*types.KLineData, error) {
	// 这里需要通过一些方式获取数据库数据
	// 由于没有直接访问数据库的接口，这里模拟实现

	// 将任务添加到队列，获取数据
	endTime := time.Now()
	startTime := endTime.Add(-b.getMaxDurationForInterval(interval))

	service := NewBinanceKlinesService(b.Client)
	return service.
		Symbol(symbol).
		Interval(interval).
		StartTime(startTime.UnixMilli()).
		EndTime(endTime.UnixMilli()).
		Limit(1000).
		Do(ctx)
}

// FetchTopCryptocurrencies 获取前N个市值最大的加密货币
func (b *BinanceCollector) FetchTopCryptocurrencies(ctx context.Context, limit int) ([]string, error) {
	logging.Logger.WithField("limit", limit).Info("获取前N个市值最大的加密货币")
	
	// 检查是否有API密钥
	apiKey := b.config.Binance.APIKey
	secretKey := b.config.Binance.SecretKey
	
	if apiKey == "" || secretKey == "" {
		logging.Logger.Info("API密钥未设置，将使用默认币种列表")
		// 使用常见交易对作为默认列表
		defaultSymbols := []string{
			"BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", 
			"DOGEUSDT", "SOLUSDT", "DOTUSDT", "MATICUSDT", "LTCUSDT",
			"AVAXUSDT", "LINKUSDT", "ATOMUSDT", "UNIUSDT", "ETCUSDT",
			"TRXUSDT", "XLMUSDT", "VETUSDT", "ICPUSDT", "FILUSDT",
			"THETAUSDT", "XMRUSDT", "FTMUSDT", "ALGOUSDT", "HBARUSDT",
			"AAVEUSDT", "EGLDUSDT", "AXSUSDT", "NEARUSDT", "EOSUSDT",
		}
		
		// 如果默认列表长度小于limit，返回整个列表
		if limit <= len(defaultSymbols) {
			return defaultSymbols[:limit], nil
		}
		return defaultSymbols, nil
	}
	
	// 获取24小时价格变动信息，包含市值信息
	tickers, err := b.Client.NewListPriceChangeStatsService().Do(ctx)
	if err != nil {
		logging.Logger.WithError(err).Error("获取24小时价格变动信息失败")
		return nil, err
	}
	
	logging.Logger.WithField("tickers_count", len(tickers)).Info("成功获取到交易对信息")
	
	// 过滤USDT交易对并排序
	usdtPairs := make([]*binance.PriceChangeStats, 0)
	for _, ticker := range tickers {
		if strings.HasSuffix(ticker.Symbol, "USDT") {
			usdtPairs = append(usdtPairs, ticker)
		}
	}
	
	logging.Logger.WithField("usdt_pairs_count", len(usdtPairs)).Info("过滤出USDT交易对数量")
	
	// 按交易量排序（使用交易量作为市值的代理指标）
	sort.Slice(usdtPairs, func(i, j int) bool {
		// 按交易量（QuoteVolume）降序排序
		return usdtPairs[i].QuoteVolume > usdtPairs[j].QuoteVolume
	})
	
	// 取前N个交易对
	count := limit
	if count > len(usdtPairs) {
		count = len(usdtPairs)
	}
	
	result := make([]string, count)
	for i := 0; i < count; i++ {
		result[i] = usdtPairs[i].Symbol
		logging.Logger.WithFields(logrus.Fields{
			"rank": i+1,
			"symbol": usdtPairs[i].Symbol,
			"volume": usdtPairs[i].QuoteVolume,
		}).Info("排名靠前的交易对")
	}
	
	// 确保BTC和ETH在列表中
	hasBTC := false
	hasETH := false
	
	for _, symbol := range result {
		if symbol == "BTCUSDT" {
			hasBTC = true
		}
		if symbol == "ETHUSDT" {
			hasETH = true
		}
	}
	
	// 如果没有BTC或ETH，手动添加它们
	if !hasBTC {
		logging.Logger.Info("手动添加BTCUSDT到列表")
		result = append(result, "BTCUSDT")
	}
	
	if !hasETH {
		logging.Logger.Info("手动添加ETHUSDT到列表")
		result = append(result, "ETHUSDT")
	}
	
	logging.Logger.WithFields(logrus.Fields{
		"requested": limit,
		"found":     len(result),
	}).Info("已获取前N个市值最大的加密货币")
	
	// 记录获取到的所有符号
	logging.Logger.WithField("symbols", strings.Join(result[:min(20, len(result))], ",")).Info("前20个交易对列表")
	
	return result, nil
}

// min 返回两个int中较小的一个
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ReloadSymbolConfig 重新加载符号配置
func (b *BinanceCollector) ReloadSymbolConfig() error {
	logging.Logger.Info("重新加载币种配置到收集器")
	
	// 获取符号管理器
	symbolManager, err := b.config.GetSymbolManager()
	if err != nil {
		return fmt.Errorf("获取符号管理器失败: %w", err)
	}
	
	// 获取所有启用的符号
	symbols := symbolManager.GetAllSymbols()
	if len(symbols) == 0 {
		return fmt.Errorf("未找到任何启用的币种")
	}
	
	logging.Logger.WithField("count", len(symbols)).Info("重新加载到的币种数量")
	
	// 清除现有的收集器
	b.CollectorsMu.Lock()
	b.Collectors = make(map[string]map[string]*SymbolCollector)
	b.CollectorsMu.Unlock()
	
	// 跟踪已添加的符号，避免重复添加
	addedSymbols := make(map[string]bool)
	
	// 为每个符号创建新的收集器
	for _, sym := range symbols {
		// 跳过禁用的符号
		if !sym.Enabled {
			continue
		}
		
		// 跳过已添加的符号
		if addedSymbols[sym.Symbol] {
			logging.Logger.WithField("symbol", sym.Symbol).Debug("跳过已添加的币种")
			continue
		}
		
		logging.Logger.WithFields(logrus.Fields{
			"symbol":    sym.Symbol,
			"intervals": sym.Intervals,
		}).Info("为币种创建收集器")
		
		// 创建收集器
		err := b.AddSymbol(sym)
		if err != nil {
			logging.Logger.WithFields(logrus.Fields{
				"symbol": sym.Symbol,
				"error":  err,
			}).Error("添加币种失败")
			continue
		}
		
		// 标记为已添加
		addedSymbols[sym.Symbol] = true
	}
	
	logging.Logger.WithField("added_symbols", len(addedSymbols)).Info("成功添加的币种数量")
	
	// 调度任务
	go b.scheduler(context.Background())
	
	return nil
}
