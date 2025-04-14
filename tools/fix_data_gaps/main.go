package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	binance "github.com/adshao/go-binance/v2"
	"github.com/web3qt/data4Trend/config"
	"github.com/web3qt/data4Trend/internal/utils"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// KLineModel 数据库K线模型
type KLineModel struct {
	ID           uint      `gorm:"primaryKey" json:"-"`
	OpenTime     time.Time `gorm:"index" json:"open_time"`
	Symbol       string    `gorm:"index" json:"symbol"`
	IntervalType string    `gorm:"index" json:"interval"`
	Open         float64   `json:"open"`
	High         float64   `json:"high"`
	Low          float64   `json:"low"`
	Close        float64   `json:"close"`
	Volume       float64   `json:"volume"`
	CloseTime    time.Time `json:"close_time"`
	CreatedAt    time.Time `gorm:"autoCreateTime" json:"-"`
	UpdatedAt    time.Time `gorm:"autoUpdateTime" json:"-"`
}

// TableName 设置表名
func (KLineModel) TableName() string {
	return "kline"
}

var (
	symbol        = flag.String("symbol", "", "需要修复的币种，为空则修复所有币种")
	interval      = flag.String("interval", "", "需要修复的时间周期，为空则修复所有时间周期")
	startTimeFlag = flag.String("start", "", "开始时间 (格式: 2006-01-02)")
	endTimeFlag   = flag.String("end", "", "结束时间 (格式: 2006-01-02)")
	dryRun        = flag.Bool("dry-run", false, "仅检查不修复")
	concurrent    = flag.Int("concurrent", 5, "并发请求数")
	verbose       = flag.Bool("verbose", false, "详细日志")
)

func main() {
	flag.Parse()

	// 加载配置
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	// 构建DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		cfg.MySQL.User, cfg.MySQL.Password, cfg.MySQL.Host, cfg.MySQL.Port, cfg.MySQL.Database)

	// 设置日志级别
	logLevel := logger.Silent
	if *verbose {
		logLevel = logger.Info
	}

	// 连接数据库
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logLevel),
	})
	if err != nil {
		log.Fatalf("连接数据库失败: %v", err)
	}

	// 创建上下文
	ctx := context.Background()

	// 创建Binance客户端
	var client *binance.Client
	if cfg.Binance.APIKey != "" && cfg.Binance.SecretKey != "" {
		client = binance.NewClient(cfg.Binance.APIKey, cfg.Binance.SecretKey)
	} else {
		client = binance.NewClient("", "")
	}
	client.HTTPClient = cfg.NewHTTPClient()

	// 解析时间范围
	var startTime, endTime time.Time
	if *startTimeFlag != "" {
		startTime, err = time.Parse("2006-01-02", *startTimeFlag)
		if err != nil {
			log.Fatalf("解析开始时间失败: %v", err)
		}
	} else {
		// 默认30天前
		startTime = time.Now().Add(-30 * 24 * time.Hour)
	}

	if *endTimeFlag != "" {
		endTime, err = time.Parse("2006-01-02", *endTimeFlag)
		if err != nil {
			log.Fatalf("解析结束时间失败: %v", err)
		}
	} else {
		// 默认到当前时间
		endTime = time.Now()
	}

	// 获取需要修复的币种
	symbols := []string{}
	if *symbol != "" {
		symbols = []string{*symbol}
	} else {
		// 获取数据库中所有币种
		db.Model(&KLineModel{}).Distinct("symbol").Pluck("symbol", &symbols)
	}

	// 获取需要修复的时间周期
	intervals := []string{}
	if *interval != "" {
		intervals = []string{*interval}
	} else {
		// 默认修复所有时间周期
		intervals = []string{"1m", "5m", "15m", "30m", "1h", "4h", "1d"}
	}

	// 打印修复信息
	fmt.Printf("开始修复数据缺口:\n")
	fmt.Printf("币种: %v\n", symbols)
	fmt.Printf("时间周期: %v\n", intervals)
	fmt.Printf("时间范围: %s 到 %s\n", startTime.Format("2006-01-02"), endTime.Format("2006-01-02"))
	fmt.Printf("模式: %s\n", map[bool]string{true: "仅检查", false: "检查并修复"}[*dryRun])

	// 并发控制
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, *concurrent)

	// 遍历所有组合
	for _, sym := range symbols {
		for _, intvl := range intervals {
			wg.Add(1)
			go func(symbol, interval string) {
				defer wg.Done()
				semaphore <- struct{}{}        // 获取令牌
				defer func() { <-semaphore }() // 释放令牌

				fmt.Printf("检查 %s %s 的数据缺口...\n", symbol, interval)

				// 获取所有数据
				var klines []KLineModel
				db.Where("symbol = ? AND interval_type = ? AND open_time BETWEEN ? AND ?",
					symbol, interval, startTime, endTime).
					Order("open_time ASC").
					Find(&klines)

				// 检查数据缺口
				gaps := detectGaps(klines, interval)
				fmt.Printf("币种 %s 时间周期 %s: 发现 %d 个缺口\n", symbol, interval, len(gaps))

				if len(gaps) == 0 || *dryRun {
					// 没有缺口或仅检查模式
					return
				}

				// 修复缺口
				for i, gap := range gaps {
					fmt.Printf("修复缺口 %d/%d: %s - %s\n", i+1, len(gaps),
						gap.Start.Format("2006-01-02 15:04:05"),
						gap.End.Format("2006-01-02 15:04:05"))

					// 从Binance获取数据
					err := fetchAndFillGap(ctx, db, client, symbol, interval, gap.Start, gap.End)
					if err != nil {
						fmt.Printf("错误: 修复缺口失败: %v\n", err)
					}
				}

				fmt.Printf("完成 %s %s 的数据缺口修复\n", symbol, interval)
			}(sym, intvl)
		}
	}

	wg.Wait()
	fmt.Println("数据缺口检查与修复完成")
}

// Gap 表示数据缺口
type Gap struct {
	Start time.Time
	End   time.Time
}

// detectGaps 检测数据缺口
func detectGaps(klines []KLineModel, interval string) []Gap {
	if len(klines) < 2 {
		return nil
	}

	// 计算预期的时间间隔
	expectedDuration := calculateIntervalDuration(interval)
	if expectedDuration == 0 {
		fmt.Printf("警告: 无法识别的周期格式: %s\n", interval)
		return nil
	}

	// 排序
	sort.Slice(klines, func(i, j int) bool {
		return klines[i].OpenTime.Before(klines[j].OpenTime)
	})

	// 检查缺口
	var gaps []Gap
	for i := 1; i < len(klines); i++ {
		timeDiff := klines[i].OpenTime.Sub(klines[i-1].OpenTime)
		// 如果时间差大于预期的1.5倍，认为有缺口
		if timeDiff > time.Duration(float64(expectedDuration)*1.5) {
			gap := Gap{
				Start: klines[i-1].OpenTime.Add(expectedDuration),
				End:   klines[i].OpenTime,
			}
			gaps = append(gaps, gap)
		}
	}

	return gaps
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

// fetchAndFillGap 从Binance获取数据并填补缺口
func fetchAndFillGap(ctx context.Context, db *gorm.DB, client *binance.Client, symbol, interval string, startTime, endTime time.Time) error {
	// 限制时间范围，避免一次请求过多数据
	const maxBatchSize = 24 * time.Hour // 1天

	currentStart := startTime
	for currentStart.Before(endTime) {
		// 计算当前批次的结束时间
		currentEnd := currentStart.Add(maxBatchSize)
		if currentEnd.After(endTime) {
			currentEnd = endTime
		}

		// 从Binance获取数据
		klines, err := client.NewKlinesService().
			Symbol(symbol).
			Interval(interval).
			StartTime(currentStart.UnixMilli()).
			EndTime(currentEnd.UnixMilli()).
			Limit(1000).
			Do(ctx)

		if err != nil {
			return fmt.Errorf("从Binance获取数据失败: %w", err)
		}

		fmt.Printf("获取到 %d 条数据 (%s - %s)\n",
			len(klines),
			currentStart.Format("2006-01-02 15:04:05"),
			currentEnd.Format("2006-01-02 15:04:05"))

		// 转换并保存数据
		if len(klines) > 0 {
			// 批量插入
			var models []KLineModel
			for _, k := range klines {
				model := KLineModel{
					OpenTime:     time.Unix(k.OpenTime/1000, 0),
					CloseTime:    time.Unix(k.CloseTime/1000, 0),
					Symbol:       symbol,
					IntervalType: interval,
					Open:         utils.ParseFloat(k.Open),
					High:         utils.ParseFloat(k.High),
					Low:          utils.ParseFloat(k.Low),
					Close:        utils.ParseFloat(k.Close),
					Volume:       utils.ParseFloat(k.Volume),
				}
				models = append(models, model)
			}

			// 使用事务批量插入
			if len(models) > 0 {
				err = db.Transaction(func(tx *gorm.DB) error {
					// 对于每个数据点，使用upsert操作
					for _, model := range models {
						result := tx.Where("symbol = ? AND interval_type = ? AND open_time = ?",
							model.Symbol, model.IntervalType, model.OpenTime).
							Assign(model).
							FirstOrCreate(&model)

						if result.Error != nil {
							return result.Error
						}
					}
					return nil
				})

				if err != nil {
					return fmt.Errorf("保存数据失败: %w", err)
				}

				fmt.Printf("成功保存了 %d 条数据\n", len(models))
			}
		}

		// 更新下一批次的开始时间
		currentStart = currentEnd

		// 避免请求过快
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

// 转换二进制K线数据为模型
func convertKlineToModel(k *binance.Kline, symbol, interval string) KLineModel {
	return KLineModel{
		OpenTime:     time.Unix(k.OpenTime/1000, 0),
		CloseTime:    time.Unix(k.CloseTime/1000, 0),
		Symbol:       symbol,
		IntervalType: interval,
		Open:         utils.ParseFloat(k.Open),
		High:         utils.ParseFloat(k.High),
		Low:          utils.ParseFloat(k.Low),
		Close:        utils.ParseFloat(k.Close),
		Volume:       utils.ParseFloat(k.Volume),
	}
}
