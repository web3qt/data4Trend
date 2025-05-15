package trendscanner

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/web3qt/data4Trend/pkg/logging"
)

// AmplitudeTask 振幅扫描任务
type AmplitudeTask struct {
	*BaseTask
}

// NewAmplitudeTask 创建一个新的振幅扫描任务
func NewAmplitudeTask() *AmplitudeTask {
	task := &AmplitudeTask{
		BaseTask: NewBaseTask("amplitude"),
	}

	// 设置默认配置
	task.ConfigParams = map[string]interface{}{
		"timeWindow":        "1h",       // 时间窗口
		"minAmplitude":      10.0,       // 最小振幅百分比
		"interval":          "15m",      // K线间隔
		"requiredDataCount": 4,          // 需要的数据点数量(1小时/15分钟=4个K线)
	}

	return task
}

// Execute 执行振幅扫描任务
func (t *AmplitudeTask) Execute(ctx context.Context, db *gorm.DB, symbol string) (*TaskResult, error) {
	// 获取配置
	interval := t.GetConfigString("interval", "15m")
	timeWindow := t.GetConfigString("timeWindow", "1h")
	minAmplitude := t.GetConfigFloat("minAmplitude", 10.0)
	requiredDataCount := t.GetConfigInt("requiredDataCount", 4)

	// 解析时间窗口
	windowDuration, err := parseIntervalDuration(timeWindow)
	if err != nil {
		return nil, fmt.Errorf("无效的时间窗口配置: %s, %w", timeWindow, err)
	}

	// 确定需要查询的K线数量
	queryLimit := requiredDataCount
	if queryLimit < 6 {
		queryLimit = 6 // 至少取6个点，确保有足够数据
	}
	
	// 计算时间过滤条件（最近N天的数据）
	// 为了确保能获取足够的历史数据进行计算，我们设置一个较大的时间窗口
	maxDataAge := 3 * 24 * time.Hour // 3天
	minTime := time.Now().Add(-maxDataAge)

	// 查询K线数据
	query := fmt.Sprintf(`
		SELECT id, interval_type, open_time, close_time, open_price, high_price, low_price, close_price 
		FROM %s 
		WHERE interval_type = ? 
		AND open_time > ?
		ORDER BY open_time DESC 
		LIMIT ?
	`, "`"+symbol+"`") // 使用MySQL的反引号语法

	rows, err := db.Raw(query, interval, minTime, queryLimit).Rows()
	if err != nil {
		logging.Logger.WithError(err).WithField("symbol", symbol).Debug("查询K线数据失败")
		return nil, err
	}
	defer rows.Close()

	// 存储K线数据
	type klineData struct {
		ID         int
		Interval   string
		OpenTime   time.Time
		CloseTime  time.Time
		OpenPrice  float64
		HighPrice  float64
		LowPrice   float64
		ClosePrice float64
	}

	var klines []klineData
	for rows.Next() {
		var kline klineData
		if err := rows.Scan(
			&kline.ID,
			&kline.Interval,
			&kline.OpenTime,
			&kline.CloseTime,
			&kline.OpenPrice,
			&kline.HighPrice,
			&kline.LowPrice,
			&kline.ClosePrice,
		); err != nil {
			logging.Logger.WithError(err).WithField("symbol", symbol).Debug("扫描K线数据失败")
			return nil, err
		}
		klines = append(klines, kline)
	}

	// 数据点不足
	if len(klines) < requiredDataCount {
		return nil, nil
	}

	// 记录最新K线的时间
	latestKLineTime := klines[0].OpenTime
	latestKLineEndTime := klines[0].CloseTime

	// 计算时间窗口内的最高价和最低价
	var highestPrice, lowestPrice float64 = 0, 999999999
	var timeWindowKLines []klineData

	// 获取时间窗口内的K线
	nowTime := latestKLineTime
	startTime := nowTime.Add(-windowDuration)

	for _, kline := range klines {
		if kline.OpenTime.After(startTime) || kline.OpenTime.Equal(startTime) {
			timeWindowKLines = append(timeWindowKLines, kline)
			
			// 更新最高价和最低价
			if kline.HighPrice > highestPrice {
				highestPrice = kline.HighPrice
			}
			if kline.LowPrice < lowestPrice {
				lowestPrice = kline.LowPrice
			}
		}
	}

	// 如果时间窗口内没有足够的K线数据
	if len(timeWindowKLines) < requiredDataCount {
		return nil, nil
	}

	// 计算振幅
	if lowestPrice <= 0 {
		return nil, nil // 防止除以零
	}
	
	amplitude := (highestPrice - lowestPrice) / lowestPrice * 100.0

	// 检查振幅是否超过阈值
	if amplitude >= minAmplitude {
		// 创建结果
		result := &TaskResult{
			Symbol:       symbol,
			TaskName:     t.Name(),
			Found:        true,
			FoundTime:    time.Now(),
			KLineTime:    latestKLineTime,
			KLineEndTime: latestKLineEndTime,
			Values: map[string]float64{
				"amplitude":   amplitude,
				"highPrice":   highestPrice,
				"lowPrice":    lowestPrice,
				"lastPrice":   klines[0].ClosePrice,
				"minAmplitude": minAmplitude,
			},
			Descriptions: []string{
				fmt.Sprintf("%s在%s内振幅达到%.2f%%", symbol, timeWindow, amplitude),
				fmt.Sprintf("最高价: %.8f, 最低价: %.8f", highestPrice, lowestPrice),
				fmt.Sprintf("振幅阈值: %.2f%%，K线间隔: %s", minAmplitude, interval),
			},
		}

		logging.Logger.WithFields(logrus.Fields{
			"symbol":    symbol,
			"amplitude": amplitude,
			"timeFrame": timeWindow,
			"highPrice": highestPrice,
			"lowPrice":  lowestPrice,
		}).Info("发现高振幅币种")

		return result, nil
	}

	return nil, nil
} 