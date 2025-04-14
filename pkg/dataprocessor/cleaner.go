package dataprocessor

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/web3qt/dataFeeder/internal/types"
	"github.com/web3qt/dataFeeder/pkg/logging"
)

type DataCleaner struct {
	inputChan  <-chan *types.KLineData
	OutputChan chan *types.KLineData
}

func NewDataCleaner(input <-chan *types.KLineData) *DataCleaner {
	logging.Logger.Info("初始化数据清洗器")
	return &DataCleaner{
		inputChan:  input,
		OutputChan: make(chan *types.KLineData, 1000),
	}
}

func (d *DataCleaner) Start(ctx context.Context) {
	go func() {
		logging.Logger.Info("数据清洗器启动，等待数据...")

		// 添加调试信息，每15秒检查一次通道状态
		debugTicker := time.NewTicker(15 * time.Second)
		defer debugTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				logging.Logger.Info("数据清洗器接收到关闭信号，停止清洗")
				return
			case data := <-d.inputChan:
				logging.Logger.WithFields(logrus.Fields{
					"symbol":    data.Symbol,
					"interval":  data.Interval,
					"open_time": data.OpenTime,
				}).Debug("数据清洗器收到数据")

				if cleaned := d.clean(data); cleaned != nil {
					logging.Logger.WithFields(logrus.Fields{
						"symbol":    cleaned.Symbol,
						"interval":  cleaned.Interval,
						"open_time": cleaned.OpenTime,
					}).Debug("数据清洗完成，发送到输出通道")
					d.OutputChan <- cleaned
				} else {
					logging.Logger.WithFields(logrus.Fields{
						"symbol":    data.Symbol,
						"interval":  data.Interval,
						"open_time": data.OpenTime,
					}).Warn("数据清洗失败，丢弃数据")
				}
			case <-debugTicker.C:
				logging.Logger.Debug("数据清洗器调试: 等待数据中...")
			}
		}
	}()
}

func (d *DataCleaner) clean(data *types.KLineData) *types.KLineData {
	logging.Logger.WithFields(logrus.Fields{
		"symbol":   data.Symbol,
		"interval": data.Interval,
		"open":     data.Open,
		"high":     data.High,
		"low":      data.Low,
		"close":    data.Close,
	}).Debug("清洗数据")

	// 基础数据校验
	if data.Symbol == "" || data.Interval == "" {
		logging.Logger.WithField("data", data).Warn("数据缺少必要字段，丢弃")
		return nil
	}

	if data.Open <= 0 || data.High <= 0 || data.Low <= 0 || data.Close <= 0 {
		logging.Logger.WithField("data", data).Warn("无效价格数据")
		return nil
	}

	if data.OpenTime.After(data.CloseTime) {
		logging.Logger.WithFields(logrus.Fields{
			"open_time":  data.OpenTime,
			"close_time": data.CloseTime,
		}).Warn("时间顺序错误")
		return nil
	}

	// 复制一份数据以避免修改原始数据
	cleanedData := *data

	// 根据不同的K线周期进行时间对齐
	alignedTime := cleanedData.OpenTime
	var intervalDuration time.Duration

	switch cleanedData.Interval {
	case "1m":
		intervalDuration = 1 * time.Minute
		alignedTime = cleanedData.OpenTime.Truncate(intervalDuration)
	case "5m":
		intervalDuration = 5 * time.Minute
		minute := cleanedData.OpenTime.Minute()
		minute = minute - (minute % 5)
		alignedTime = time.Date(
			cleanedData.OpenTime.Year(),
			cleanedData.OpenTime.Month(),
			cleanedData.OpenTime.Day(),
			cleanedData.OpenTime.Hour(),
			minute,
			0,
			0,
			cleanedData.OpenTime.Location(),
		)
	case "15m":
		intervalDuration = 15 * time.Minute
		minute := cleanedData.OpenTime.Minute()
		minute = minute - (minute % 15)
		alignedTime = time.Date(
			cleanedData.OpenTime.Year(),
			cleanedData.OpenTime.Month(),
			cleanedData.OpenTime.Day(),
			cleanedData.OpenTime.Hour(),
			minute,
			0,
			0,
			cleanedData.OpenTime.Location(),
		)
	case "30m":
		intervalDuration = 30 * time.Minute
		minute := cleanedData.OpenTime.Minute()
		minute = minute - (minute % 30)
		alignedTime = time.Date(
			cleanedData.OpenTime.Year(),
			cleanedData.OpenTime.Month(),
			cleanedData.OpenTime.Day(),
			cleanedData.OpenTime.Hour(),
			minute,
			0,
			0,
			cleanedData.OpenTime.Location(),
		)
	case "1h":
		intervalDuration = 1 * time.Hour
		alignedTime = time.Date(
			cleanedData.OpenTime.Year(),
			cleanedData.OpenTime.Month(),
			cleanedData.OpenTime.Day(),
			cleanedData.OpenTime.Hour(),
			0,
			0,
			0,
			cleanedData.OpenTime.Location(),
		)
	case "2h":
		intervalDuration = 2 * time.Hour
		hour := cleanedData.OpenTime.Hour()
		hour = hour - (hour % 2)
		alignedTime = time.Date(
			cleanedData.OpenTime.Year(),
			cleanedData.OpenTime.Month(),
			cleanedData.OpenTime.Day(),
			hour,
			0,
			0,
			0,
			cleanedData.OpenTime.Location(),
		)
	case "4h":
		intervalDuration = 4 * time.Hour
		hour := cleanedData.OpenTime.Hour()
		hour = hour - (hour % 4)
		alignedTime = time.Date(
			cleanedData.OpenTime.Year(),
			cleanedData.OpenTime.Month(),
			cleanedData.OpenTime.Day(),
			hour,
			0,
			0,
			0,
			cleanedData.OpenTime.Location(),
		)
	case "1d":
		intervalDuration = 24 * time.Hour
		alignedTime = time.Date(
			cleanedData.OpenTime.Year(),
			cleanedData.OpenTime.Month(),
			cleanedData.OpenTime.Day(),
			0,
			0,
			0,
			0,
			cleanedData.OpenTime.Location(),
		)
	default:
		// 如果是未知的周期，保持原样
		logging.Logger.WithField("interval", cleanedData.Interval).Warn("未知的K线周期，保持原始时间")
		intervalDuration = 15 * time.Minute // 默认15分钟作为安全值
	}

	// 检查时间是否需要对齐
	if !alignedTime.Equal(cleanedData.OpenTime) {
		logging.Logger.WithFields(logrus.Fields{
			"original": cleanedData.OpenTime,
			"aligned":  alignedTime,
			"interval": cleanedData.Interval,
		}).Debug("时间未对齐，进行调整")
		cleanedData.OpenTime = alignedTime
		cleanedData.CloseTime = alignedTime.Add(intervalDuration - time.Nanosecond)
	}

	logging.Logger.WithFields(logrus.Fields{
		"symbol":     cleanedData.Symbol,
		"interval":   cleanedData.Interval,
		"open_time":  cleanedData.OpenTime,
		"close_time": cleanedData.CloseTime,
	}).Debug("数据清洗完成")

	return &cleanedData
}
