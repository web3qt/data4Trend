// 用于将unknown_coin表中的数据迁移到单独的表中
package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/web3qt/dataFeeder/config"
	"github.com/web3qt/dataFeeder/pkg/logging"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// MigrationCoinKLine 数据库模型
type MigrationCoinKLine struct {
	ID           uint      `gorm:"primaryKey;autoIncrement"`
	IntervalType string    `gorm:"type:varchar(10);not null;uniqueIndex:idx_interval_open_time"`
	OpenTime     time.Time `gorm:"not null;uniqueIndex:idx_interval_open_time"`
	CloseTime    time.Time `gorm:"not null"`
	OpenPrice    float64   `gorm:"type:decimal(20,8);not null"`
	HighPrice    float64   `gorm:"type:decimal(20,8);not null"`
	LowPrice     float64   `gorm:"type:decimal(20,8);not null"`
	ClosePrice   float64   `gorm:"type:decimal(20,8);not null"`
	Volume       float64   `gorm:"type:decimal(30,8);not null"`
	CreatedAt    time.Time `gorm:"autoCreateTime"`
	UpdatedAt    time.Time `gorm:"autoUpdateTime"`
	coinSymbol   string    `gorm:"-"` // 不存储到数据库的字段，仅用于确定表名
}

// TableName 返回表名
func (k *MigrationCoinKLine) TableName() string {
	if k.coinSymbol != "" {
		return k.coinSymbol
	}
	return "unknown_coin"
}

// SetCoinSymbol 设置表名
func (k *MigrationCoinKLine) SetCoinSymbol(symbol string) {
	k.coinSymbol = symbol
}

func main() {
	// 初始化日志
	logging.InitLogger(&config.LogConfig{
		Level:      "info",
		JSONFormat: false,
	})

	// 加载配置
	cfg, err := config.LoadConfig()
	if err != nil {
		logging.Logger.WithError(err).Fatal("加载配置失败")
		os.Exit(1)
	}

	// 构建DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		cfg.MySQL.User, cfg.MySQL.Password, cfg.MySQL.Host, cfg.MySQL.Port, cfg.MySQL.Database)

	// 连接数据库
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.New(
			&logging.GormLogrusWriter{Logger: logging.Logger},
			logger.Config{
				SlowThreshold:             200 * time.Millisecond,
				LogLevel:                  logger.Info,
				IgnoreRecordNotFoundError: true,
				Colorful:                  false,
			},
		),
	})
	if err != nil {
		logging.Logger.WithError(err).Fatal("连接数据库失败")
		os.Exit(1)
	}

	// 需要迁移数据的交易对
	symbols := []string{"BTCUSDT", "ETHUSDT"}

	logging.Logger.Info("开始数据迁移...")

	for _, symbol := range symbols {
		targetTable := migrateCleanSymbol(symbol)

		// 检查目标表是否存在，不存在则创建
		if !tableExists(db, targetTable) {
			logging.Logger.WithField("table", targetTable).Info("创建目标表")
			err := createTable(db, targetTable)
			if err != nil {
				logging.Logger.WithError(err).WithField("table", targetTable).Error("创建表失败")
				continue
			}
		}

		// 计算源表中符合条件的数据数量
		var count int64
		db.Table("unknown_coin").Count(&count)
		logging.Logger.WithFields(logrus.Fields{
			"table": "unknown_coin",
			"count": count,
		}).Info("源表中的数据数量")

		// 将数据从unknown_coin表迁移到目标表
		logging.Logger.WithFields(logrus.Fields{
			"symbol": symbol,
			"table":  targetTable,
		}).Info("开始迁移数据")

		// 分批处理数据，每批1000条
		batchSize := 1000
		var processed int64 = 0

		for {
			// 从unknown_coin表读取一批数据
			var sourceData []MigrationCoinKLine
			db.Table("unknown_coin").Limit(batchSize).Offset(int(processed)).Find(&sourceData)

			if len(sourceData) == 0 {
				break // 没有更多数据
			}

			// 准备要写入的数据
			var targetData []MigrationCoinKLine
			for _, item := range sourceData {
				// 创建新数据对象
				newItem := MigrationCoinKLine{
					IntervalType: item.IntervalType,
					OpenTime:     item.OpenTime,
					CloseTime:    item.CloseTime,
					OpenPrice:    item.OpenPrice,
					HighPrice:    item.HighPrice,
					LowPrice:     item.LowPrice,
					ClosePrice:   item.ClosePrice,
					Volume:       item.Volume,
				}
				// 设置目标表
				newItem.SetCoinSymbol(targetTable)
				targetData = append(targetData, newItem)
			}

			// 批量写入目标表
			result := db.CreateInBatches(targetData, 100)
			if result.Error != nil {
				logging.Logger.WithError(result.Error).WithFields(logrus.Fields{
					"symbol": symbol,
					"table":  targetTable,
					"batch":  processed / int64(batchSize),
				}).Error("批量写入失败")
			} else {
				logging.Logger.WithFields(logrus.Fields{
					"symbol":        symbol,
					"table":         targetTable,
					"batch":         processed / int64(batchSize),
					"processed":     processed,
					"batch_size":    len(sourceData),
					"rows_affected": result.RowsAffected,
				}).Info("批量写入成功")
			}

			processed += int64(len(sourceData))
			if len(sourceData) < batchSize {
				break // 处理完毕
			}
		}

		logging.Logger.WithFields(logrus.Fields{
			"symbol":    symbol,
			"table":     targetTable,
			"processed": processed,
		}).Info("迁移完成")
	}

	logging.Logger.Info("所有数据迁移完成")
}

// migrateCleanSymbol 简化版的cleanSymbol函数
func migrateCleanSymbol(symbol string) string {
	if symbol == "" {
		return "unknown_coin"
	}

	cleanedSymbol := strings.ToLower(symbol)
	cleanedSymbol = strings.ReplaceAll(cleanedSymbol, "/", "_")
	cleanedSymbol = strings.ReplaceAll(cleanedSymbol, "-", "_")
	cleanedSymbol = strings.ReplaceAll(cleanedSymbol, ".", "_")

	if cleanedSymbol == "" {
		return "unknown_coin"
	}

	return cleanedSymbol
}

// 检查表是否存在
func tableExists(db *gorm.DB, tableName string) bool {
	var result int
	query := fmt.Sprintf("SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '%s' LIMIT 1", tableName)

	err := db.Raw(query).Scan(&result).Error
	if err != nil {
		logging.Logger.WithError(err).Errorf("检查表 %s 是否存在时出错", tableName)
		return false
	}

	return result == 1
}

// 创建表
func createTable(db *gorm.DB, tableName string) error {
	// 创建表SQL
	sql := fmt.Sprintf(`
CREATE TABLE %s (
  id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  interval_type varchar(10) NOT NULL,
  open_time datetime NOT NULL,
  close_time datetime NOT NULL,
  open_price decimal(20,8) NOT NULL,
  high_price decimal(20,8) NOT NULL,
  low_price decimal(20,8) NOT NULL,
  close_price decimal(20,8) NOT NULL,
  volume decimal(30,8) NOT NULL,
  created_at datetime DEFAULT CURRENT_TIMESTAMP,
  updated_at datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY idx_interval_open_time (interval_type, open_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`, tableName)

	return db.Exec(sql).Error
}
