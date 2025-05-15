package trendscanner

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/web3qt/data4Trend/pkg/logging"
)

// VolatilityTask 波动率扫描任务
type VolatilityTask struct {
	*BaseTask
}

// NewVolatilityTask 创建一个新的波动率扫描任务
func NewVolatilityTask() *VolatilityTask {
	task := &VolatilityTask{
		BaseTask: NewBaseTask("volatility"),
	}

	// 设置默认配置
	task.ConfigParams = map[string]interface{}{
		"minVolatility":     3.0,        // 最小波动率阈值（百分比）
		"timeWindow":        "1d",       // 计算波动率的时间窗口
		"interval":          "15m",      // K线间隔
		"requiredDataCount": 96,         // 需要的数据点数量(1天/15分钟=96个K线)
	}

	return task
}

// Execute 执行波动率扫描任务
func (t *VolatilityTask) Execute(ctx context.Context, db *gorm.DB, symbol string) (*TaskResult, error) {
	// 获取配置
	interval := t.GetConfigString("interval", "15m")
	timeWindow := t.GetConfigString("timeWindow", "1d")
	minVolatility := t.GetConfigFloat("minVolatility", 3.0)
	requiredDataCount := t.GetConfigInt("requiredDataCount", 96)

	// 解析时间窗口
	windowDuration, err := parseIntervalDuration(timeWindow)
	if err != nil {
		return nil, fmt.Errorf("无效的时间窗口配置: %s, %w", timeWindow, err)
	}

	// 查询K线数据
	query := fmt.Sprintf(`
		SELECT id, interval_type, open_time, close_time, open_price, high_price, low_price, close_price 
		FROM %s 
		WHERE interval_type = ? 
		ORDER BY open_time DESC 
		LIMIT ?
	`, "`"+symbol+"`") // 使用MySQL的反引号语法

	rows, err := db.Raw(query, interval, requiredDataCount+10).Rows() // 多取一些数据，以防需要
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

	// 检查数据点是否足够
	if len(klines) < requiredDataCount {
		return nil, nil
	}

	// 记录最新K线的时间
	latestKLineTime := klines[0].OpenTime
	latestKLineEndTime := klines[0].CloseTime

	// 获取时间窗口内的K线
	var windowKLines []klineData
	nowTime := latestKLineTime
	startTime := nowTime.Add(-windowDuration)

	for _, kline := range klines {
		if kline.OpenTime.After(startTime) || kline.OpenTime.Equal(startTime) {
			windowKLines = append(windowKLines, kline)
		}
	}

	// 如果时间窗口内没有足够的K线
	if len(windowKLines) < requiredDataCount/2 {
		return nil, nil
	}

	// 计算波动率（标准差/平均值 * 100%）
	// 1. 先计算收盘价的对数收益率
	if len(windowKLines) < 2 {
		return nil, nil
	}
	
	var returns []float64
	for i := 0; i < len(windowKLines)-1; i++ {
		// 计算相邻K线之间的对数收益率
		if windowKLines[i+1].ClosePrice <= 0 {
			continue // 避免对数无效
		}
		
		logReturn := math.Log(windowKLines[i].ClosePrice / windowKLines[i+1].ClosePrice)
		returns = append(returns, logReturn)
	}
	
	if len(returns) < 2 {
		return nil, nil
	}
	
	// 2. 计算标准差
	var sum, sumSquared float64
	for _, r := range returns {
		sum += r
		sumSquared += r * r
	}
	
	mean := sum / float64(len(returns))
	variance := (sumSquared / float64(len(returns))) - (mean * mean)
	
	if variance < 0 {
		variance = 0 // 防止计算误差导致的负方差
	}
	
	stdDev := math.Sqrt(variance)
	
	// 3. 计算年化波动率 (假设252个交易日/年，换算到对应时间窗口)
	// 公式: 波动率 = stdDev * sqrt(窗口数量/年)
	var tradingDaysPerYear float64 = 252
	var barsPerYear float64
	
	// 基于K线间隔和时间窗口推算年化系数
	intervalDuration, _ := parseIntervalDuration(interval)
	if intervalDuration <= 0 {
		intervalDuration = 15 * time.Minute // 默认15分钟
	}
	
	barsPerDay := 24.0 * 60.0 / (intervalDuration.Minutes())
	barsPerYear = barsPerDay * tradingDaysPerYear
	
	annualizationFactor := math.Sqrt(barsPerYear / float64(len(returns)))
	volatility := stdDev * annualizationFactor * 100.0 // 转为百分比
	
	// 检查波动率是否高于阈值
	if volatility >= minVolatility {
		// 创建结果
		result := &TaskResult{
			Symbol:       symbol,
			TaskName:     t.Name(),
			Found:        true,
			FoundTime:    time.Now(),
			KLineTime:    latestKLineTime,
			KLineEndTime: latestKLineEndTime,
			Values: map[string]float64{
				"volatility":  volatility,
				"stdDev":      stdDev,
				"returnCount": float64(len(returns)),
				"lastPrice":   klines[0].ClosePrice,
			},
			Descriptions: []string{
				fmt.Sprintf("%s在%s窗口内波动率为%.2f%%", symbol, timeWindow, volatility),
				fmt.Sprintf("基于%d个K线计算，标准差为%.6f", len(returns), stdDev),
			},
		}

		logging.Logger.WithFields(logrus.Fields{
			"symbol":     symbol,
			"volatility": volatility,
			"timeFrame":  timeWindow,
			"stdDev":     stdDev,
			"dataPoints": len(returns),
		}).Info("发现高波动率币种")

		return result, nil
	}

	return nil, nil
} 