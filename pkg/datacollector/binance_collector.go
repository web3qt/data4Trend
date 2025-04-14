package datacollector

import (
	"context"
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

	// 默认使用10个工作器
	workers := 10

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
		taskQueue:    make(chan CollectionTask, 2000), // 增加任务队列容量
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

// worker 工作器从任务队列中取出任务并执行
func (b *BinanceCollector) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-b.taskQueue:
			// 获取工作槽
			b.workerPool <- struct{}{}

			// 执行任务
			b.executeTask(ctx, task)

			// 释放工作槽
			<-b.workerPool
		}
	}
}

// executeTask 执行单个收集任务
func (b *BinanceCollector) executeTask(ctx context.Context, task CollectionTask) {
	// 创建超时上下文
	taskCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	logging.Logger.WithFields(logrus.Fields{
		"symbol":     task.Symbol,
		"interval":   task.Interval,
		"start_time": task.StartTime.Format(time.RFC3339),
		"end_time":   task.EndTime.Format(time.RFC3339),
	}).Debug("执行收集任务")

	// 创建新的Kline服务
	service := NewBinanceKlinesService(b.Client)

	// 限制每个请求的数据点数量
	const maxDataPoints = 1000
	currentStart := task.StartTime

	for currentStart.Before(task.EndTime) {
		// 计算最大结束时间
		maxEnd := currentStart.Add(time.Duration(maxDataPoints) * calculateIntervalDuration(task.Interval))
		if maxEnd.After(task.EndTime) {
			maxEnd = task.EndTime
		}

		// 启动Klines查询
		klines, err := service.Symbol(task.Symbol).
			Interval(task.Interval).
			StartTime(currentStart.UnixMilli()).
			EndTime(maxEnd.UnixMilli()).
			Limit(maxDataPoints).
			Do(taskCtx)

		if err != nil {
			logging.Logger.WithFields(logrus.Fields{
				"symbol":     task.Symbol,
				"interval":   task.Interval,
				"start_time": currentStart.Format(time.RFC3339),
				"end_time":   maxEnd.Format(time.RFC3339),
				"error":      err,
			}).Error("收集K线数据失败")

			// 检查是否被限流
			if strings.Contains(err.Error(), "429") || strings.Contains(err.Error(), "418") {
				logging.Logger.Warn("被Binance限流，暂停10秒")
				time.Sleep(10 * time.Second)
			}
			return
		}

		// 检查是否获取到数据
		if len(klines) == 0 {
			logging.Logger.WithFields(logrus.Fields{
				"symbol":     task.Symbol,
				"interval":   task.Interval,
				"start_time": currentStart.Format(time.RFC3339),
				"end_time":   maxEnd.Format(time.RFC3339),
			}).Warn("未获取到K线数据")

			// 如果是新的收集任务，可能是没有数据，更新收集时间并退出
			if currentStart.Equal(task.StartTime) {
				b.updateCollectorStartTime(task.Symbol, task.Interval, maxEnd)
			}

			// 移动到下一个时间段
			currentStart = maxEnd
			continue
		}

		// 处理收集到的数据
		processedCount := 0
		var lastKlineTime time.Time

		// 确保K线按时间排序
		sort.Slice(klines, func(i, j int) bool {
			return klines[i].OpenTime.Before(klines[j].OpenTime)
		})

		// 根据间隔类型生成标准的K线开始时间
		for i, kline := range klines {
			// 计算标准化的开始时间（对齐到标准时间间隔）
			standardOpenTime := normalizeKlineOpenTime(kline.OpenTime, task.Interval)

			// 确保K线开始时间是标准化的
			if !kline.OpenTime.Equal(standardOpenTime) {
				// 记录异常时间不标准的K线数据
				logging.Logger.WithFields(logrus.Fields{
					"symbol":           task.Symbol,
					"interval":         task.Interval,
					"actual_open_time": kline.OpenTime.Format(time.RFC3339),
					"standard_time":    standardOpenTime.Format(time.RFC3339),
				}).Warn("K线开始时间不标准，将进行校正")

				// 校正开始时间和结束时间
				klines[i].OpenTime = standardOpenTime
				klines[i].CloseTime = standardOpenTime.Add(calculateIntervalDuration(task.Interval) - time.Millisecond)
			}

			// 发送数据到输出通道
			select {
			case b.DataChan <- kline:
				processedCount++
				lastKlineTime = kline.OpenTime
			case <-taskCtx.Done():
				logging.Logger.WithFields(logrus.Fields{
					"symbol":   task.Symbol,
					"interval": task.Interval,
				}).Warn("任务上下文取消")
				return
			}
		}

		// 更新收集器的开始时间，使用最后一个K线的时间加上一个周期
		if lastKlineTime.After(time.Time{}) {
			// 计算下一个周期的开始时间
			nextStartTime := lastKlineTime.Add(calculateIntervalDuration(task.Interval))

			logging.Logger.WithFields(logrus.Fields{
				"symbol":            task.Symbol,
				"interval":          task.Interval,
				"last_kline_time":   lastKlineTime.Format(time.RFC3339),
				"next_start_time":   nextStartTime.Format(time.RFC3339),
				"processed_count":   processedCount,
				"interval_duration": calculateIntervalDuration(task.Interval).String(),
			}).Info("任务完成，更新收集器时间")

			// 确保下一个开始时间是通过标准化时间计算的
			nextStandardTime := normalizeKlineOpenTime(nextStartTime, task.Interval)
			if !nextStartTime.Equal(nextStandardTime) {
				logging.Logger.WithFields(logrus.Fields{
					"symbol":       task.Symbol,
					"interval":     task.Interval,
					"calculated":   nextStartTime.Format(time.RFC3339),
					"standardized": nextStandardTime.Format(time.RFC3339),
				}).Warn("下一个开始时间不标准，进行校正")
				nextStartTime = nextStandardTime
			}

			b.updateCollectorStartTime(task.Symbol, task.Interval, nextStartTime)
		}

		// 移动到下一个时间范围
		if len(klines) > 0 {
			lastKline := klines[len(klines)-1]
			// 确保下一次获取数据的时间是标准化的时间点
			nextTime := lastKline.OpenTime.Add(calculateIntervalDuration(task.Interval))
			nextStandardTime := normalizeKlineOpenTime(nextTime, task.Interval)
			currentStart = nextStandardTime
		} else {
			currentStart = maxEnd
		}

		// 检查是否达到结束时间
		if currentStart.After(task.EndTime) || currentStart.Equal(task.EndTime) {
			break
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

// scheduler 任务调度器，按优先级排序任务
func (b *BinanceCollector) scheduler(ctx context.Context) {
	// 实现简单的调度逻辑
	logging.Logger.Info("启动任务调度器")

	// 任务计数
	taskCount := 0
	lastTaskCount := 0
	lastTaskTime := time.Now()

	// 每秒检查一次任务队列
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// 每分钟打印一次汇总统计
	statsTicker := time.NewTicker(1 * time.Minute)
	defer statsTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 统计任务队列长度
			queueLen := len(b.taskQueue)
			if queueLen > 0 {
				taskCount += queueLen
				logging.Logger.WithField("current_length", queueLen).WithField("total_tasks", taskCount).Info("任务队列状态")
			}
		case <-statsTicker.C:
			// 检查是否一分钟内没有生成任务
			if taskCount == lastTaskCount {
				logging.Logger.Warn("过去一分钟内没有生成新任务，任务队列可能停滞")
			} else {
				tasksPerMinute := taskCount - lastTaskCount
				logging.Logger.WithField("tasks_per_minute", tasksPerMinute).WithField("current_length", len(b.taskQueue)).Info("调度器统计")
				// 每5分钟检查一次数据通道
				elapsed := time.Since(lastTaskTime).Minutes()
				if elapsed >= 5 {
					logging.Logger.WithField("buffer_capacity", cap(b.DataChan)).WithField("current_length", len(b.DataChan)).Info("数据通道状态")
					lastTaskTime = time.Now()
				}
			}
			// 更新最后的任务计数
			lastTaskCount = taskCount
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
	
	// 获取所有交易对信息 - 不使用，因为我们直接用24小时交易量
	_, err := b.Client.NewExchangeInfoService().Do(ctx)
	if err != nil {
		logging.Logger.WithError(err).Error("获取交易所信息失败")
		return nil, err
	}
	
	// 获取24小时价格变动信息，包含市值信息
	tickers, err := b.Client.NewListPriceChangeStatsService().Do(ctx)
	if err != nil {
		logging.Logger.WithError(err).Error("获取24小时价格变动信息失败")
		return nil, err
	}
	
	// 过滤USDT交易对并排序
	usdtPairs := make([]*binance.PriceChangeStats, 0)
	for _, ticker := range tickers {
		if strings.HasSuffix(ticker.Symbol, "USDT") {
			usdtPairs = append(usdtPairs, ticker)
		}
	}
	
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
	}
	
	logging.Logger.WithFields(logrus.Fields{
		"requested": limit,
		"found":     count,
	}).Info("已获取前N个市值最大的加密货币")
	
	return result, nil
}
