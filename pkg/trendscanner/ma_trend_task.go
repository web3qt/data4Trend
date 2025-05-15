package trendscanner

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/web3qt/data4Trend/pkg/logging"
)

// MATrendTask MA趋势扫描任务
type MATrendTask struct {
	*BaseTask
	scanner *TrendScanner
}

// NewMATrendTask 创建一个新的MA趋势扫描任务
func NewMATrendTask(scanner *TrendScanner) *MATrendTask {
	task := &MATrendTask{
		BaseTask: NewBaseTask("ma_trend"),
		scanner:  scanner,
	}
	return task
}

// Execute 执行MA趋势扫描任务
func (t *MATrendTask) Execute(ctx context.Context, db *gorm.DB, symbol string) (*TaskResult, error) {
	// 沿用原有scanSymbol方法的实现
	s := t.scanner
	
	// 查询足够数量的K线数据来计算MA
	query := fmt.Sprintf(`
		SELECT id, interval_type, open_time, close_time, close_price 
		FROM %s 
		WHERE interval_type = ? 
		ORDER BY open_time DESC 
		LIMIT ?
	`, "`"+symbol+"`") // 使用MySQL的反引号语法
	
	// 计算需要的K线数据数量
	// 我们需要额外的K线来检查连续K线都在MA线上方
	// 因此，最小K线数量应该是MA周期 + 检查点偏移量 + 连续K线数量
	maxOffset := s.getMaxCheckPointOffset()
	requiredKLines := s.maPeriod + maxOffset + s.consecutiveKLines
	
	rows, err := db.Raw(query, s.interval, requiredKLines).Rows()
	if err != nil {
		logging.Logger.WithError(err).WithField("symbol", symbol).Debug("查询K线数据失败")
		return nil, err
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
			return nil, err
		}
		
		prices = append(prices, closePrice)
		times = append(times, openTime)
		closeTimeList = append(closeTimeList, closeTime)
	}
	
	// 数据点不足以计算MA
	if len(prices) < s.maPeriod + s.consecutiveKLines {
		return nil, nil
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
	
	// 检查连续K线是否都在MA线之上
	aboveMACount := 0 // 在MA线上方的连续K线数量
	isConsecutiveAboveMA := false
	
	// 计算MA值
	maValues := make([]float64, len(prices) - s.maPeriod + 1)
	
	// 计算不同窗口的MA值
	for i := 0; i <= len(prices) - s.maPeriod; i++ {
		maValues[i] = calculateMA(prices[i:i+s.maPeriod])
	}
	
	// 检查连续K线是否都在MA线上方
	// 注意：prices[0]是最新的价格，prices[1]是次新的，以此类推
	for i := 0; i < s.consecutiveKLines && i < len(prices); i++ {
		// MA值对应的索引需要偏移i
		if i + 1 >= len(maValues) {
			break // 没有足够的MA值用于比较
		}
		
		// 检查K线收盘价是否高于对应的MA值
		if prices[i] > maValues[i+1] {
			aboveMACount++
		} else {
			// 如果有一条K线不在MA线上方，重置计数器
			aboveMACount = 0
		}
		
		// 如果连续K线数量达到要求，则满足条件
		if aboveMACount >= s.consecutiveKLines {
			isConsecutiveAboveMA = true
			break
		}
	}
	
	// 只有当连续K线都在MA线上方时，才继续检查MA趋势
	if !isConsecutiveAboveMA {
		return nil, nil
	}
	
	// 计算当前的MA值和历史MA值
	// 注意: prices是按照时间倒序排列的，最新的在前面
	// 计算当前MA
	currentMA := maValues[0] // 最新的MA值
	
	// 创建保存各个时间点MA值的map
	maValueMap := make(map[string]float64)
	maValueMap["current"] = currentMA
	
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
			if offset < len(maValues) {
				maValue := maValues[offset]
				
				// 存储值以便后续使用
				maValueMap[duration.String()] = maValue
				
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
				// 数据不足以计算此时间点
				isUptrend = false
				break
			}
		}
	} else {
		// 无检查点时，使用strict_up模式直接检查maValues是否单调上升
		isUptrend = checkConsistentUp(maValueMap)
	}
	
	// 只有当MA趋势满足条件时，才返回结果
	if !isUptrend {
		return nil, nil
	}
	
	// 转换结果为TaskResult格式
	result := &TaskResult{
		Symbol:       symbol,
		TaskName:     t.Name(),
		Found:        true,
		FoundTime:    time.Now(),
		KLineTime:    latestKLineTime,
		KLineEndTime: latestKLineEndTime,
		Values: map[string]float64{
			"current_ma":     currentMA,
			"above_ma_klines": float64(aboveMACount),
			"ma_period":     float64(s.maPeriod),
			"last_price":    prices[0],
		},
		Descriptions: []string{
			fmt.Sprintf("%s的MA%d趋势向上", symbol, s.maPeriod),
			fmt.Sprintf("连续%d根K线在MA线上方", aboveMACount),
			fmt.Sprintf("当前MA值: %.8f，参考周期: %s", currentMA, s.interval),
		},
	}
	
	// 添加各检查点的MA值
	for _, duration := range s.checkPoints {
		key := duration.String()
		if val, ok := maValueMap[key]; ok {
			formattedKey := fmt.Sprintf("ma_%s_ago", key)
			result.Values[formattedKey] = val
		}
	}
	
	// 保存到数据库
	dbResult := &TrendResult{
		Symbol:        symbol,
		Interval:      s.interval,
		FoundTime:     result.FoundTime,
		MAPeriod:      s.maPeriod,
		CurrentMA:     currentMA,
		ConsistentUp:  true,
		KLineTime:     latestKLineTime,
		KLineEndTime:  latestKLineEndTime,
		AboveMAKLines: aboveMACount,
	}
	
	// 设置各个时间点的MA值
	for _, duration := range s.checkPoints {
		key := duration.String()
		if val, ok := maValueMap[key]; ok {
			switch key {
			case "10m":
				dbResult.MA10MinAgo = val
			case "30m":
				dbResult.MA30MinAgo = val
			case "1h":
				dbResult.MAHourAgo = val
			case "4h":
				dbResult.MA4HoursAgo = val
			case "1d", "24h":
				dbResult.MADayAgo = val
			}
		}
	}
	
	// 保存到数据库
	if err := s.SaveResult(dbResult); err != nil {
		logging.Logger.WithError(err).WithField("symbol", symbol).Error("保存趋势结果到数据库失败")
	}
	
	logging.Logger.WithFields(logrus.Fields{
		"symbol":        symbol,
		"interval":      s.interval,
		"ma_period":     s.maPeriod,
		"current_ma":    currentMA,
		"kline_start":   latestKLineTime.Format(time.RFC3339),
		"kline_end":     latestKLineEndTime.Format(time.RFC3339),
		"consistent_up": true,
	}).Info("发现MA上升趋势")
	
	return result, nil
} 