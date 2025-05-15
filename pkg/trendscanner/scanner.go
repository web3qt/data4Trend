package trendscanner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/web3qt/data4Trend/pkg/logging"
)

// TrendScanner 趋势扫描器
type TrendScanner struct {
	ctx               context.Context
	db                *gorm.DB
	interval          string
	maPeriod          int
	workers           int
	scanInterval      time.Duration
	symbols           []string
	config            *TrendScannerConfig
	csvOutputDir      string
	checkPoints       []time.Duration
	strictUp          bool
	consecutiveKLines int
	maxDataAgeHours   int             // 新增：数据最大有效期（小时）
	taskManager       *TaskManager    // 新增任务管理器
	csvReporter       *CSVReporter    // 统一CSV报告生成器
}

// NewTrendScanner 创建新的趋势扫描器
func NewTrendScanner(ctx context.Context, db *gorm.DB, opt ...Option) *TrendScanner {
	s := &TrendScanner{
		ctx:          ctx,
		db:           db,
		interval:     "15m", // 默认使用15分钟K线
		maPeriod:     81,    // 默认MA81
		workers:      4,     // 默认4个工作协程
		scanInterval: 1 * time.Hour, // 默认1小时扫描一次
	}
	
	// 应用选项
	for _, o := range opt {
		o(s)
	}
	
	return s
}

// NewTrendScannerWithConfig 使用配置文件创建趋势扫描器
func NewTrendScannerWithConfig(ctx context.Context, db *gorm.DB, config *TrendScannerConfig) *TrendScanner {
	s := &TrendScanner{
		ctx:               ctx,
		db:                db,
		interval:          config.MA.Interval,
		maPeriod:          config.MA.Period,
		workers:           config.Scan.Workers,
		scanInterval:      config.GetScanInterval(),
		config:            config,
		csvOutputDir:      config.Scan.CSVOutput,
		checkPoints:       config.GetCheckPointDurations(),
		strictUp:          config.Trend.RequireStrictUp,
		consecutiveKLines: config.Trend.ConsecutiveKLines,
		maxDataAgeHours:   config.Scan.MaxDataAgeHours,
	}
	
	// 初始化任务管理器
	s.taskManager = NewTaskManager(db, config.Scan.CSVOutput, config.Scan.MaxDataAgeHours)
	
	// 初始化统一CSV报告生成器
	s.csvReporter = NewCSVReporter(config.Scan.CSVOutput)
	
	// 注册MA趋势任务 (作为内置任务)
	maTask := NewMATrendTask(s)
	s.taskManager.RegisterTask(maTask)
	
	// 注册振幅任务
	amplitudeTask := NewAmplitudeTask()
	// 从配置中加载设置
	if taskCfg, exists := config.Tasks["amplitude"]; exists {
		amplitudeTask.SetEnabled(taskCfg.Enabled)
		if taskCfg.Config != nil {
			amplitudeTask.SetConfig(taskCfg.Config)
		}
	}
	s.taskManager.RegisterTask(amplitudeTask)
	
	// 注册波动率任务
	volatilityTask := NewVolatilityTask()
	// 从配置中加载设置
	if taskCfg, exists := config.Tasks["volatility"]; exists {
		volatilityTask.SetEnabled(taskCfg.Enabled)
		if taskCfg.Config != nil {
			volatilityTask.SetConfig(taskCfg.Config)
		}
	}
	s.taskManager.RegisterTask(volatilityTask)
	
	return s
}

// Option 配置选项函数类型
type Option func(*TrendScanner)

// WithInterval 设置K线间隔
func WithInterval(interval string) Option {
	return func(s *TrendScanner) {
		s.interval = interval
	}
}

// WithMAPeriod 设置MA周期
func WithMAPeriod(period int) Option {
	return func(s *TrendScanner) {
		s.maPeriod = period
	}
}

// WithWorkers 设置工作协程数
func WithWorkers(workers int) Option {
	return func(s *TrendScanner) {
		s.workers = workers
	}
}

// WithScanInterval 设置扫描间隔
func WithScanInterval(interval time.Duration) Option {
	return func(s *TrendScanner) {
		s.scanInterval = interval
	}
}

// WithSymbols 设置指定的交易对列表
func WithSymbols(symbols []string) Option {
	return func(s *TrendScanner) {
		s.symbols = symbols
	}
}

// WithCSVOutputDir 设置CSV输出目录
func WithCSVOutputDir(dir string) Option {
	return func(s *TrendScanner) {
		s.csvOutputDir = dir
	}
}

// WithConsecutiveKLines 设置连续K线数量
func WithConsecutiveKLines(count int) Option {
	return func(s *TrendScanner) {
		s.consecutiveKLines = count
	}
}

// Start 启动趋势扫描器
func (s *TrendScanner) Start() {
	// 创建结果表
	if err := s.createResultsTable(); err != nil {
		logging.Logger.WithError(err).Error("创建结果表失败")
		return
	}

	// 确保CSV输出目录存在
	if s.csvOutputDir != "" {
		if err := os.MkdirAll(s.csvOutputDir, 0755); err != nil {
			logging.Logger.WithError(err).Error("创建CSV输出目录失败")
			// 不要因为CSV目录创建失败而退出
		}
	}

	logging.Logger.WithFields(logrus.Fields{
		"interval":        s.interval,
		"ma_period":       s.maPeriod,
		"workers":         s.workers,
		"scan_interval_h": s.scanInterval.Hours(),
		"csv_dir":         s.csvOutputDir,
	}).Info("趋势扫描器启动")

	// 创建工作协程池
	var wg sync.WaitGroup
	symbolCh := make(chan string, s.workers*2)

	// 启动工作协程
	for i := 0; i < s.workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for symbol := range symbolCh {
				// 执行所有任务
				results, err := s.taskManager.ExecuteTasks(s.ctx, symbol)
				if err != nil {
					logging.Logger.WithError(err).WithField("symbol", symbol).Error("执行扫描任务失败")
					continue
				}
				
				// 处理结果
				for _, result := range results {
					logging.Logger.WithFields(logrus.Fields{
						"task":       result.TaskName,
						"symbol":     result.Symbol,
						"found_time": result.FoundTime.Format(time.RFC3339),
					}).Info("发现符合条件的币种")
				}
				
				// 将结果保存到统一CSV文件
				if len(results) > 0 && s.csvReporter != nil {
					if err := s.csvReporter.SaveResults(results); err != nil {
						logging.Logger.WithError(err).WithField("symbol", symbol).Error("保存到统一CSV文件失败")
					}
				}
			}
		}(i)
	}

	// 定时执行扫描任务
	ticker := time.NewTicker(s.scanInterval)
	defer ticker.Stop()

	// 立即执行一次扫描
	s.executeScan(symbolCh)

	// 等待信号以优雅退出
	for {
		select {
		case <-ticker.C:
			s.executeScan(symbolCh)
		case <-s.ctx.Done():
			logging.Logger.Info("趋势扫描器收到停止信号")
			close(symbolCh)
			wg.Wait()
			logging.Logger.Info("趋势扫描器已停止")
			return
		}
	}
}

// Stop 停止趋势扫描器
func (s *TrendScanner) Stop() {
	logging.Logger.Info("停止趋势扫描器")
}

// executeScan 执行一次扫描任务
func (s *TrendScanner) executeScan(symbolCh chan<- string) {
	startTime := time.Now()
	logging.Logger.Info("开始执行趋势扫描...")
	
	// 获取所有可用的交易对
	var symbols []string
	var err error
	
	if len(s.symbols) > 0 {
		// 使用配置的币种
		symbols = s.symbols
		logging.Logger.WithField("count", len(symbols)).Info("使用配置的交易对列表")
	} else {
		// 从数据库获取所有表（币种）
		symbols, err = s.getAllSymbols()
		if err != nil {
			logging.Logger.WithError(err).Error("获取交易对列表失败")
			return
		}
		logging.Logger.WithField("count", len(symbols)).Info("从数据库获取交易对列表")
	}
	
	// 分发扫描任务
	for _, symbol := range symbols {
		select {
		case symbolCh <- symbol:
			// 发送成功
		case <-s.ctx.Done():
			// 收到停止信号
			return
		}
	}
	
	elapsedTime := time.Since(startTime)
	logging.Logger.WithField("elapsed_time", elapsedTime.String()).Info("扫描任务分发完成")
}

// TrendResult 表示趋势扫描结果
type TrendResult struct {
	ID           uint      `gorm:"primaryKey"`
	Symbol       string    `gorm:"index:idx_symbol"`
	Interval     string    `gorm:"type:varchar(10)"`
	FoundTime    time.Time `gorm:"index:idx_found_time"` // 发现时间，保留但不作为主要参考
	MAPeriod     int       `gorm:"type:int"`
	CurrentMA    float64   `gorm:"type:decimal(20,8)"`
	MA10MinAgo   float64   `gorm:"type:decimal(20,8)"`
	MA30MinAgo   float64   `gorm:"type:decimal(20,8)"`
	MAHourAgo    float64   `gorm:"type:decimal(20,8)"`
	MA4HoursAgo  float64   `gorm:"type:decimal(20,8)"`
	MADayAgo     float64   `gorm:"type:decimal(20,8)"`
	ConsistentUp bool      `gorm:"type:tinyint(1)"`
	KLineTime    time.Time `gorm:"index:idx_kline_time"` // K线开始时间
	KLineEndTime time.Time `gorm:"index:idx_kline_end_time"` // K线结束时间
	AboveMAKLines int       `gorm:"type:int"`
	
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

// TableName 指定表名
func (TrendResult) TableName() string {
	return "trend_results"
}

// getAllSymbols 获取数据库中所有可用的交易对列表
func (s *TrendScanner) getAllSymbols() ([]string, error) {
	// 查询数据库中所有的表（交易对）
	rows, err := s.db.Raw("SHOW TABLES").Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var symbols []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		// 跳过非交易对表格
		if tableName == "trend_results" {
			continue
		}
		symbols = append(symbols, tableName)
	}
	
	return symbols, nil
}

// calculateMA 计算简单移动平均线
func calculateMA(prices []float64) float64 {
	if len(prices) == 0 {
		return 0
	}
	
	sum := 0.0
	for _, price := range prices {
		sum += price
	}
	
	return sum / float64(len(prices))
}

// createResultsTable 创建或确保存在结果表
func (s *TrendScanner) createResultsTable() error {
	// 使用GORM自动迁移创建表结构
	err := s.db.AutoMigrate(&TrendResult{})
	if err != nil {
		return fmt.Errorf("创建趋势结果表失败: %w", err)
	}
	return nil
}

// SaveResult 保存趋势扫描结果到数据库
func (s *TrendScanner) SaveResult(result *TrendResult) error {
	// 使用GORM创建记录
	if err := s.db.Create(result).Error; err != nil {
		return fmt.Errorf("保存趋势结果失败: %w", err)
	}
	logging.Logger.WithFields(logrus.Fields{
		"symbol":        result.Symbol,
		"interval":      result.Interval,
		"ma_period":     result.MAPeriod,
		"current_ma":    result.CurrentMA,
		"ma_hour_ago":   result.MAHourAgo,
		"kline_start":   result.KLineTime.Format(time.RFC3339),
		"kline_end":     result.KLineEndTime.Format(time.RFC3339),
		"consistent_up": result.ConsistentUp,
	}).Info("发现上升趋势")
	return nil
}

// saveResultToCSV 保存趋势扫描结果到CSV文件
func (s *TrendScanner) saveResultToCSV(result *TrendResult) error {
	if s.csvOutputDir == "" {
		return fmt.Errorf("未设置CSV输出目录")
	}

	// 创建文件名：ma周期_时间间隔_日期.csv
	timestamp := time.Now().Format("20060102")
	filename := fmt.Sprintf("ma%d_%s_%s.csv", s.maPeriod, s.interval, timestamp)
	filePath := filepath.Join(s.csvOutputDir, filename)

	// 检查文件是否存在，不存在则创建并写入标题行
	var file *os.File
	var err error
	if _, err = os.Stat(filePath); os.IsNotExist(err) {
		file, err = os.Create(filePath)
		if err != nil {
			return fmt.Errorf("创建CSV文件失败: %w", err)
		}
		// 写入CSV头
		headerLine := "Symbol,Interval,KLineStartTime,KLineEndTime,FoundTime,MAPeriod,CurrentMA,MA10MinAgo,MA30MinAgo,MAHourAgo,MA4HoursAgo,MADayAgo,ConsistentUp,AboveMAKLines\n"
		if _, err = file.WriteString(headerLine); err != nil {
			file.Close()
			return fmt.Errorf("写入CSV头失败: %w", err)
		}
	} else {
		// 文件已存在，追加模式打开
		file, err = os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("打开CSV文件失败: %w", err)
		}
	}
	defer file.Close()

	// 写入数据行
	line := fmt.Sprintf("%s,%s,%s,%s,%s,%d,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%t,%d\n",
		result.Symbol,
		result.Interval,
		result.KLineTime.Format(time.RFC3339),
		result.KLineEndTime.Format(time.RFC3339),
		result.FoundTime.Format(time.RFC3339),
		result.MAPeriod,
		result.CurrentMA,
		result.MA10MinAgo,
		result.MA30MinAgo,
		result.MAHourAgo,
		result.MA4HoursAgo,
		result.MADayAgo,
		result.ConsistentUp,
		result.AboveMAKLines,
	)

	if _, err = file.WriteString(line); err != nil {
		return fmt.Errorf("写入CSV数据行失败: %w", err)
	}

	logging.Logger.WithFields(logrus.Fields{
		"symbol":      result.Symbol,
		"file":        filePath,
		"kline_start": result.KLineTime.Format(time.RFC3339),
		"kline_end":   result.KLineEndTime.Format(time.RFC3339),
	}).Info("趋势结果已保存到CSV")

	return nil
}

// parseIntervalDuration 解析K线间隔为time.Duration
func parseIntervalDuration(interval string) (time.Duration, error) {
	switch interval {
	case "1m":
		return 1 * time.Minute, nil
	case "3m":
		return 3 * time.Minute, nil
	case "5m":
		return 5 * time.Minute, nil
	case "15m":
		return 15 * time.Minute, nil
	case "30m":
		return 30 * time.Minute, nil
	case "1h":
		return 1 * time.Hour, nil
	case "2h":
		return 2 * time.Hour, nil
	case "4h":
		return 4 * time.Hour, nil
	case "6h":
		return 6 * time.Hour, nil
	case "8h":
		return 8 * time.Hour, nil
	case "12h":
		return 12 * time.Hour, nil
	case "1d":
		return 24 * time.Hour, nil
	case "3d":
		return 72 * time.Hour, nil
	case "1w":
		return 7 * 24 * time.Hour, nil
	case "1M":
		return 30 * 24 * time.Hour, nil // 近似值
	default:
		return 0, fmt.Errorf("未知的间隔格式: %s", interval)
	}
}

// getMaxCheckPointOffset 获取最大检查点偏移量
func (s *TrendScanner) getMaxCheckPointOffset() int {
	if len(s.checkPoints) == 0 {
		// 如果没有配置检查点，使用默认的最大偏移（1天 = 96个15分钟K线）
		return 96
	}
	
	// 解析各个检查点并找出最大的偏移量
	maxOffset := 0
	
	// 为15分钟K线计算偏移量
	if s.interval == "15m" {
		for _, duration := range s.checkPoints {
			minutesOffset := int(duration.Minutes())
			klineOffset := minutesOffset / 15 // 每15分钟一个K线
			if klineOffset > maxOffset {
				maxOffset = klineOffset
			}
		}
	} else {
		// 对于其他时间间隔，使用一个安全的默认值
		// 默认获取足够的数据以支持最远1天的MA计算
		intervalDuration, err := parseIntervalDuration(s.interval)
		if err != nil {
			return 96 // 默认值
		}
		
		// 计算一天内有多少个这样的K线
		oneDayMinutes := 24 * 60
		intervalMinutes := int(intervalDuration.Minutes())
		if intervalMinutes > 0 {
			maxOffset = oneDayMinutes / intervalMinutes
		} else {
			maxOffset = 96 // 默认值
		}
	}
	
	return maxOffset
}

// calculateOffsetForDuration 计算给定duration对应的K线偏移量
func (s *TrendScanner) calculateOffsetForDuration(duration time.Duration) int {
	intervalDuration, err := parseIntervalDuration(s.interval)
	if err != nil {
		// 无法解析间隔，使用默认值
		return 0
	}
	
	// 计算偏移的K线数量
	durationMinutes := int(duration.Minutes())
	intervalMinutes := int(intervalDuration.Minutes())
	
	if intervalMinutes > 0 {
		return durationMinutes / intervalMinutes
	}
	
	// 默认偏移量
	return 0
}

// checkConsistentUp 检查MA是否连续上升
func checkConsistentUp(maValues map[string]float64) bool {
	// 需要按照时间顺序检查MA值
	// 首先定义检查点顺序
	checkOrder := []string{"current", "10m", "30m", "1h", "4h", "1d"}
	
	var validValues []float64
	var validKeys []string
	
	for _, key := range checkOrder {
		if value, ok := maValues[key]; ok {
			validValues = append(validValues, value)
			validKeys = append(validKeys, key)
		}
	}
	
	// 需要至少两个点才能判断趋势
	if len(validValues) < 2 {
		return false
	}
	
	// 检查是否严格上升
	for i := 0; i < len(validValues)-1; i++ {
		if validValues[i] <= validValues[i+1] {
			logging.Logger.WithFields(logrus.Fields{
				"point1": validKeys[i],
				"value1": validValues[i],
				"point2": validKeys[i+1],
				"value2": validValues[i+1],
			}).Debug("MA不是严格上升")
			return false
		}
	}
	
	return true
} 