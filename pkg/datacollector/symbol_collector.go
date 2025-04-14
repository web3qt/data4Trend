package datacollector

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/web3qt/data4Trend/config"
	"github.com/web3qt/data4Trend/internal/types"
	"github.com/web3qt/data4Trend/pkg/logging"
)

// SymbolCollector 管理单个交易对的多个时间周期K线收集
type SymbolCollector struct {
	service     types.KlinesService
	symbol      string
	intervals   map[string]*IntervalCollector
	intervalsMu sync.RWMutex
	dataChan    chan<- *types.KLineData
	taskQueue   chan<- CollectionTask // 任务队列
	startTimes  map[string]time.Time  // 不同周期的起始时间
	enabled     bool
	ctx         context.Context
	cancel      context.CancelFunc
}

// IntervalCollector 管理单个交易对的单个时间周期K线收集
type IntervalCollector struct {
	symbol    string
	interval  string
	dataChan  chan<- *types.KLineData
	taskQueue chan<- CollectionTask // 任务队列
	ticker    *time.Ticker
	startTime time.Time
	ctx       context.Context
	cancel    context.CancelFunc
	priority  int // 优先级，数字越小优先级越高
}

// NewSymbolCollector 创建新的交易对收集器
func NewSymbolCollector(service types.KlinesService, cfg config.SymbolConfig, taskQueue chan<- CollectionTask, dataChan chan<- *types.KLineData) (*SymbolCollector, error) {
	ctx, cancel := context.WithCancel(context.Background())

	logging.Logger.WithFields(logrus.Fields{
		"symbol":  cfg.Symbol,
		"enabled": cfg.Enabled,
	}).Debug("初始化收集器")

	logging.Logger.WithFields(logrus.Fields{
		"daily_start":  cfg.DailyStart,
		"hourly_start": cfg.HourlyStart,
		"minute_start": cfg.MinuteStart,
		"intervals":    cfg.Intervals,
	}).Debug("初始化收集器时间周期")

	if cfg.Symbol == "" {
		return nil, fmt.Errorf("交易对名称不能为空")
	}

	if len(cfg.Intervals) == 0 {
		logging.Logger.WithField("symbol", cfg.Symbol).Warn("交易对没有配置任何时间周期，使用默认周期1h")
		cfg.Intervals = []string{"1h"}
	}

	// 解析不同周期的起始时间
	startTimes := make(map[string]time.Time)

	// 检查配置的时间周期
	hasConfiguredIntervals := false

	// 如果没有设置任何起始时间，使用当前时间减去24小时
	defaultStartTime := time.Now().Add(-24 * time.Hour)

	// 解析日K线起始时间
	if cfg.DailyStart != "" && contains(cfg.Intervals, "1d") {
		hasConfiguredIntervals = true
		if t, err := time.Parse(time.RFC3339, cfg.DailyStart); err == nil {
			startTimes["1d"] = t
			logging.Logger.WithField("time", t).Debug("成功解析日K线起始时间")
		} else {
			logging.Logger.WithError(err).Warn("解析日K线起始时间失败，使用默认时间")
			startTimes["1d"] = defaultStartTime
		}
	}

	// 解析小时K线起始时间
	if cfg.HourlyStart != "" && contains(cfg.Intervals, "1h") {
		hasConfiguredIntervals = true
		if t, err := time.Parse(time.RFC3339, cfg.HourlyStart); err == nil {
			startTimes["1h"] = t
			logging.Logger.WithField("time", t).Debug("成功解析小时K线起始时间")
		} else {
			logging.Logger.WithError(err).Warn("解析小时K线起始时间失败，使用默认时间")
			startTimes["1h"] = defaultStartTime
		}
	}

	// 解析分钟K线起始时间
	if cfg.MinuteStart != "" && contains(cfg.Intervals, "1m") {
		hasConfiguredIntervals = true
		if t, err := time.Parse(time.RFC3339, cfg.MinuteStart); err == nil {
			startTimes["1m"] = t
			logging.Logger.WithField("time", t).Debug("成功解析分钟K线起始时间")
		} else {
			logging.Logger.WithError(err).Warn("解析分钟K线起始时间失败，使用默认时间")
			startTimes["1m"] = defaultStartTime
		}
	}

	// 解析其他K线周期
	for _, interval := range cfg.Intervals {
		// 如果已经解析过，跳过
		if _, exists := startTimes[interval]; exists {
			continue
		}

		// 根据周期类型选择合适的起始时间
		var startTimeStr string
		switch {
		case interval == "5m" || interval == "15m" || interval == "30m":
			startTimeStr = cfg.MinuteStart // 分钟级使用分钟起始时间
		case interval == "2h" || interval == "4h" || interval == "6h" || interval == "8h" || interval == "12h":
			startTimeStr = cfg.HourlyStart // 小时级使用小时起始时间
		case interval == "3d" || interval == "1w":
			startTimeStr = cfg.DailyStart // 日级以上使用日起始时间
		default:
			// 默认使用最近的时间
			startTimeStr = cfg.MinuteStart
		}

		if startTimeStr != "" {
			if t, err := time.Parse(time.RFC3339, startTimeStr); err == nil {
				startTimes[interval] = t
				logging.Logger.WithFields(logrus.Fields{
					"interval": interval,
					"time":     t,
				}).Debug("成功解析K线起始时间")
				hasConfiguredIntervals = true
			} else {
				logging.Logger.WithFields(logrus.Fields{
					"interval": interval,
					"error":    err,
				}).Warn("解析K线起始时间失败，使用默认时间")
				startTimes[interval] = defaultStartTime
			}
		} else {
			logging.Logger.WithField("interval", interval).Warn("未找到对应的起始时间配置，使用默认时间")
			startTimes[interval] = defaultStartTime
		}
	}

	// 检查是否有配置的时间周期
	if !hasConfiguredIntervals {
		logging.Logger.WithField("symbol", cfg.Symbol).Warn("交易对没有配置任何有效的时间周期，使用默认时间")
	}

	// 确保至少有一个时间周期
	if len(startTimes) == 0 {
		for _, interval := range cfg.Intervals {
			startTimes[interval] = defaultStartTime
			logging.Logger.WithFields(logrus.Fields{
				"symbol":   cfg.Symbol,
				"interval": interval,
				"time":     defaultStartTime,
			}).Warn("使用默认起始时间")
		}
	}

	logging.Logger.WithFields(logrus.Fields{
		"symbol":          cfg.Symbol,
		"intervals_count": len(startTimes),
	}).Debug("收集器初始化完成")

	return &SymbolCollector{
		service:    service,
		symbol:     cfg.Symbol,
		intervals:  make(map[string]*IntervalCollector),
		dataChan:   dataChan,
		taskQueue:  taskQueue,
		startTimes: startTimes,
		enabled:    cfg.Enabled,
		ctx:        ctx,
		cancel:     cancel,
	}, nil
}

// contains 检查字符串是否在切片中
func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

// Start 启动所有配置的时间周期收集器
func (sc *SymbolCollector) Start() error {
	if !sc.enabled {
		logging.Logger.WithField("symbol", sc.symbol).Info("收集器未启用")
		return nil
	}

	logging.Logger.WithFields(logrus.Fields{
		"symbol":          sc.symbol,
		"intervals_count": len(sc.startTimes),
	}).Info("启动收集器")

	if len(sc.startTimes) == 0 {
		return fmt.Errorf("收集器 %s 没有配置任何时间周期", sc.symbol)
	}

	sc.intervalsMu.Lock()
	defer sc.intervalsMu.Unlock()

	// 清理现有的收集器
	for _, collector := range sc.intervals {
		if collector.cancel != nil {
			collector.cancel()
		}
	}

	// 重置收集器映射
	sc.intervals = make(map[string]*IntervalCollector)

	// 为每个时间周期创建收集器
	intervalCount := 0
	for interval, startTime := range sc.startTimes {
		ctx, cancel := context.WithCancel(sc.ctx)

		// 根据时间周期确定轮询间隔和优先级
		var pollInterval time.Duration
		var priority int

		switch interval {
		case "1m":
			pollInterval = 1 * time.Minute
			priority = 1 // 最高优先级
		case "5m":
			pollInterval = 5 * time.Minute
			priority = 2
		case "15m":
			pollInterval = 15 * time.Minute
			priority = 3
		case "30m":
			pollInterval = 30 * time.Minute
			priority = 4
		case "1h":
			pollInterval = 1 * time.Hour
			priority = 5
		case "2h":
			pollInterval = 2 * time.Hour
			priority = 6
		case "4h":
			pollInterval = 4 * time.Hour
			priority = 7
		case "6h":
			pollInterval = 6 * time.Hour
			priority = 8
		case "8h":
			pollInterval = 8 * time.Hour
			priority = 9
		case "12h":
			pollInterval = 12 * time.Hour
			priority = 10
		case "1d":
			pollInterval = 24 * time.Hour
			priority = 11
		case "3d":
			pollInterval = 3 * 24 * time.Hour
			priority = 12
		case "1w":
			pollInterval = 7 * 24 * time.Hour
			priority = 13 // 最低优先级
		default:
			// 默认15分钟轮询
			pollInterval = 15 * time.Minute
			priority = 5
			logging.Logger.WithFields(logrus.Fields{
				"symbol":   sc.symbol,
				"interval": interval,
			}).Warn("未知的时间周期，使用默认15分钟轮询")
		}

		logging.Logger.WithFields(logrus.Fields{
			"symbol":        sc.symbol,
			"interval":      interval,
			"start_time":    startTime,
			"poll_interval": pollInterval,
			"priority":      priority,
		}).Debug("创建时间周期收集器")

		// 添加安全检查，确保轮询间隔不为零
		if pollInterval <= 0 {
			logging.Logger.WithFields(logrus.Fields{
				"symbol":   sc.symbol,
				"interval": interval,
			}).Warn("轮询间隔无效，使用默认15分钟")
			pollInterval = 15 * time.Minute
		}

		collector := &IntervalCollector{
			symbol:    sc.symbol,
			interval:  interval,
			dataChan:  sc.dataChan,
			taskQueue: sc.taskQueue,
			ticker:    time.NewTicker(pollInterval),
			startTime: startTime,
			ctx:       ctx,
			cancel:    cancel,
			priority:  priority,
		}

		sc.intervals[interval] = collector

		// 启动收集器
		go func(ic *IntervalCollector, srv types.KlinesService) {
			defer func() {
				if r := recover(); r != nil {
					logging.Logger.WithFields(logrus.Fields{
						"symbol":   ic.symbol,
						"interval": ic.interval,
						"panic":    r,
					}).Error("收集器运行时发生panic")
				}
			}()
			ic.start(srv)
		}(collector, sc.service)

		intervalCount++
	}

	logging.Logger.WithFields(logrus.Fields{
		"symbol":          sc.symbol,
		"intervals_count": intervalCount,
	}).Info("收集器启动成功")

	return nil
}

// Stop 停止所有收集器
func (sc *SymbolCollector) Stop() {
	sc.cancel()
}

// UpdateConfig 更新收集器配置
func (sc *SymbolCollector) UpdateConfig(cfg config.SymbolConfig) error {
	sc.enabled = cfg.Enabled
	sc.symbol = cfg.Symbol

	// 停止现有收集器
	sc.Stop()

	// 创建新的上下文
	ctx, cancel := context.WithCancel(context.Background())
	sc.ctx = ctx
	sc.cancel = cancel

	// 重新启动
	return sc.Start()
}

// GetStatus 获取收集器状态
func (sc *SymbolCollector) GetStatus() map[string]string {
	status := make(map[string]string)
	sc.intervalsMu.RLock()
	defer sc.intervalsMu.RUnlock()

	for interval := range sc.intervals {
		status[interval] = "running"
	}
	return status
}

// start 启动定时收集任务
func (ic *IntervalCollector) start(service types.KlinesService) {
	logging.Logger.WithFields(logrus.Fields{
		"symbol":     ic.symbol,
		"interval":   ic.interval,
		"start_time": ic.startTime,
	}).Info("开始执行时间周期收集器")

	// 立即执行一次数据获取
	ic.scheduleTask()

	logging.Logger.WithFields(logrus.Fields{
		"symbol":   ic.symbol,
		"interval": ic.interval,
		"period":   ic.ticker.Reset,
	}).Debug("设置定时任务")

	// 定时执行
	for {
		select {
		case <-ic.ctx.Done():
			ic.ticker.Stop()
			logging.Logger.WithFields(logrus.Fields{
				"symbol":   ic.symbol,
				"interval": ic.interval,
			}).Info("收集器停止")
			return
		case t := <-ic.ticker.C:
			logging.Logger.WithFields(logrus.Fields{
				"symbol":   ic.symbol,
				"interval": ic.interval,
				"time":     t,
			}).Debug("定时器触发")
			ic.scheduleTask()
		}
	}
}

// scheduleTask 将收集任务添加到队列
func (ic *IntervalCollector) scheduleTask() {
	// 检查开始时间是否在未来
	if ic.startTime.After(time.Now()) {
		logging.Logger.WithFields(logrus.Fields{
			"symbol":     ic.symbol,
			"interval":   ic.interval,
			"start_time": ic.startTime,
		}).Warn("开始时间在未来，跳过任务")
		// 自动调整开始时间到当前时间的前24小时
		adjustedTime := time.Now().Add(-24 * time.Hour)
		logging.Logger.WithFields(logrus.Fields{
			"symbol":         ic.symbol,
			"interval":       ic.interval,
			"new_start_time": adjustedTime,
		}).Info("自动调整开始时间")
		ic.startTime = adjustedTime
	}

	// 创建任务
	task := CollectionTask{
		Symbol:    ic.symbol,
		Interval:  ic.interval,
		StartTime: ic.startTime,
		EndTime:   time.Now(), // 当前时间作为结束时间
		Priority:  ic.priority,
	}

	logging.Logger.WithFields(logrus.Fields{
		"symbol":     ic.symbol,
		"interval":   ic.interval,
		"start_time": ic.startTime,
		"end_time":   task.EndTime,
	}).Debug("创建任务")

	// 将任务添加到队列
	select {
	case ic.taskQueue <- task:
		logging.Logger.WithFields(logrus.Fields{
			"symbol":     ic.symbol,
			"interval":   ic.interval,
			"start_time": ic.startTime,
		}).Debug("任务已添加到队列")
	default:
		logging.Logger.WithFields(logrus.Fields{
			"symbol":   ic.symbol,
			"interval": ic.interval,
		}).Warn("任务队列已满，无法添加任务")
	}
}

// updateStartTime 更新开始时间
func (ic *IntervalCollector) updateStartTime(newTime time.Time) {
	logging.Logger.WithFields(logrus.Fields{
		"symbol":   ic.symbol,
		"interval": ic.interval,
		"new_time": newTime,
	}).Debug("更新开始时间")
	ic.startTime = newTime
}
