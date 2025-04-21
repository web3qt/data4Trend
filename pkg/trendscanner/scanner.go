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
	ctx          context.Context
	db           *gorm.DB
	interval     string
	maPeriod     int
	workers      int
	scanInterval time.Duration
	symbols      []string
	config       *TrendScannerConfig
	csvOutputDir string
	checkPoints  []time.Duration
	strictUp     bool
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
		ctx:          ctx,
		db:           db,
		interval:     config.MA.Interval,
		maPeriod:     config.MA.Period,
		workers:      config.Scan.Workers,
		scanInterval: config.GetScanInterval(),
		config:       config,
		csvOutputDir: config.Scan.CSVOutput,
		checkPoints:  config.GetCheckPointDurations(),
		strictUp:     config.Trend.RequireStrictUp,
	}
	
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
				result := s.scanSymbol(symbol)
				if result != nil {
					// 保存到数据库
					if err := s.SaveResult(result); err != nil {
						logging.Logger.WithError(err).WithField("symbol", symbol).Error("保存趋势结果到数据库失败")
					}
					
					// 保存到CSV
					if s.csvOutputDir != "" {
						if err := s.saveResultToCSV(result); err != nil {
							logging.Logger.WithError(err).WithField("symbol", symbol).Error("保存趋势结果到CSV失败")
						}
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

// scanSymbol 扫描单个币种的MA趋势
func (s *TrendScanner) scanSymbol(symbol string) *TrendResult {
	// 查询足够数量的K线数据来计算MA
	query := fmt.Sprintf(`
		SELECT id, interval_type, open_time, close_time, close_price 
		FROM %s 
		WHERE interval_type = ? 
		ORDER BY open_time DESC 
		LIMIT ?
	`, "`"+symbol+"`") // 使用MySQL的反引号语法
	
	// 计算需要的K线数据数量
	// 假设我们需要计算当前MA和最远检查点的MA
	// 对于MA81，如果最远检查点是1天（96个15分钟K线），则需要MA81+96=177个K线
	maxOffset := s.getMaxCheckPointOffset()
	requiredKLines := s.maPeriod + maxOffset
	
	rows, err := s.db.Raw(query, s.interval, requiredKLines).Rows()
	if err != nil {
		logging.Logger.WithError(err).WithField("symbol", symbol).Debug("查询K线数据失败")
		return nil
	}
	defer rows.Close()
	
	// 存储收盘价
	var prices []float64
	var times []time.Time
	var closeTimeList []time.Time
	
	for rows.Next() {
		var id int
		var intervalType string
		var openTime, closeTime time.Time
		var closePrice float64
		
		if err := rows.Scan(&id, &intervalType, &openTime, &closeTime, &closePrice); err != nil {
			logging.Logger.WithError(err).WithField("symbol", symbol).Debug("扫描K线数据失败")
			return nil
		}
		
		prices = append(prices, closePrice)
		times = append(times, openTime)
		closeTimeList = append(closeTimeList, closeTime)
	}
	
	// 数据点不足以计算MA
	if len(prices) < s.maPeriod {
		return nil
	}

	// 记录最新K线的时间，用于记录到结果中
	latestKLineTime := times[0]
	latestKLineEndTime := time.Time{}
	
	// 获取最新K线的结束时间
	if len(closeTimeList) > 0 {
		latestKLineEndTime = closeTimeList[0]
	}
	
	// 如果无法获取结束时间，则使用开始时间加上间隔时间估算
	if latestKLineEndTime.IsZero() {
		intervalDuration, err := parseIntervalDuration(s.interval)
		if err == nil {
			latestKLineEndTime = latestKLineTime.Add(intervalDuration)
		} else {
			// 无法解析间隔，使用开始时间
			latestKLineEndTime = latestKLineTime
		}
	}
	
	// 计算当前的MA值和历史MA值
	// 注意: prices是按照时间倒序排列的，最新的在前面
	// 计算当前MA
	currentMA := calculateMA(prices[:s.maPeriod])
	
	// 创建保存各个时间点MA值的map
	maValues := make(map[string]float64)
	maValues["current"] = currentMA
	
	// 计算各个检查点的MA值
	var isUptrend bool
	if len(s.checkPoints) > 0 {
		// 使用配置中的检查点计算MA
		isUptrend = true // 初始假设是上升趋势
		
		// 为各个检查点计算MA
		for _, duration := range s.checkPoints {
			// 计算对应的K线偏移量
			offset := s.calculateOffsetForDuration(duration)
			
			// 确保有足够的数据点
			if len(prices) >= s.maPeriod+offset {
				maValue := calculateMA(prices[offset:s.maPeriod+offset])
				
				// 存储值以便后续使用
				maValues[duration.String()] = maValue
				
				// 检查趋势
				if s.strictUp {
					// 严格上升模式
					if currentMA <= maValue {
						isUptrend = false
						break
					}
				} else {
					// 允许平稳模式
					if currentMA < maValue {
						isUptrend = false
						break
					}
				}
				
			} else {
				// 数据不足，不判断更远的检查点
				break
			}
		}
	} else {
		// 使用硬编码的检查点（向后兼容）
		// 计算10分钟前的MA (1个15分钟K线)
		ma10MinAgo := 0.0
		if len(prices) >= s.maPeriod+1 {
			ma10MinAgo = calculateMA(prices[1:s.maPeriod+1])
			maValues["10m"] = ma10MinAgo
		} else {
			return nil
		}
		
		// 计算30分钟前的MA (2个15分钟K线)
		ma30MinAgo := 0.0
		if len(prices) >= s.maPeriod+2 {
			ma30MinAgo = calculateMA(prices[2:s.maPeriod+2])
			maValues["30m"] = ma30MinAgo
		} else {
			return nil
		}
		
		// 计算1小时前的MA (4个15分钟K线)
		maHourAgo := 0.0
		if len(prices) >= s.maPeriod+4 {
			maHourAgo = calculateMA(prices[4:s.maPeriod+4])
			maValues["1h"] = maHourAgo
		} else {
			return nil
		}
		
		// 计算4小时前的MA (16个15分钟K线)
		ma4HoursAgo := 0.0
		if len(prices) >= s.maPeriod+16 {
			ma4HoursAgo = calculateMA(prices[16:s.maPeriod+16])
			maValues["4h"] = ma4HoursAgo
		} else {
			return nil
		}
		
		// 计算1天前的MA (96个15分钟K线)
		maDayAgo := 0.0
		if len(prices) >= s.maPeriod+96 {
			maDayAgo = calculateMA(prices[96:s.maPeriod+96])
			maValues["1d"] = maDayAgo
		} else {
			// 日前数据不足，不影响判断
			maDayAgo = 0
		}
		
		// 检查MA是否持续上升（如果strictUp为true）或保持平稳（如果strictUp为false）
		if s.strictUp {
			// 严格上升模式
			isUptrend = currentMA > ma10MinAgo && ma10MinAgo > ma30MinAgo && ma30MinAgo > maHourAgo && maHourAgo > ma4HoursAgo
		} else {
			// 允许平稳模式
			isUptrend = currentMA >= ma10MinAgo && ma10MinAgo >= ma30MinAgo && ma30MinAgo >= maHourAgo && maHourAgo >= ma4HoursAgo
		}
	}
	
	// 如果满足上升趋势条件，返回结果
	if isUptrend {
		result := &TrendResult{
			Symbol:       symbol,
			Interval:     s.interval,
			FoundTime:    time.Now(),
			MAPeriod:     s.maPeriod,
			CurrentMA:    maValues["current"],
			KLineTime:    latestKLineTime,    // 记录最新K线的开始时间
			KLineEndTime: latestKLineEndTime, // 记录最新K线结束时间
			ConsistentUp: checkConsistentUp(maValues),
		}
		
		// 设置标准检查点的值（如果有）
		if ma, ok := maValues["10m"]; ok {
			result.MA10MinAgo = ma
		}
		if ma, ok := maValues["30m"]; ok {
			result.MA30MinAgo = ma
		}
		if ma, ok := maValues["1h"]; ok {
			result.MAHourAgo = ma
		}
		if ma, ok := maValues["4h"]; ok {
			result.MA4HoursAgo = ma
		}
		if ma, ok := maValues["1d"]; ok {
			result.MADayAgo = ma
		}
		
		return result
	}
	
	return nil
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
		headerLine := "Symbol,Interval,KLineStartTime,KLineEndTime,FoundTime,MAPeriod,CurrentMA,MA10MinAgo,MA30MinAgo,MAHourAgo,MA4HoursAgo,MADayAgo,ConsistentUp\n"
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
	line := fmt.Sprintf("%s,%s,%s,%s,%s,%d,%.8f,%.8f,%.8f,%.8f,%.8f,%.8f,%t\n",
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
	for _, key := range checkOrder {
		if value, ok := maValues[key]; ok {
			validValues = append(validValues, value)
		}
	}
	
	// 需要至少两个点才能判断趋势
	if len(validValues) < 2 {
		return false
	}
	
	// 检查是否严格上升
	for i := 0; i < len(validValues)-1; i++ {
		if validValues[i] <= validValues[i+1] {
			return false
		}
	}
	
	return true
} 