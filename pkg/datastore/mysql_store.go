package datastore

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2"
	"github.com/sirupsen/logrus"
	"github.com/web3qt/data4Trend/config"
	"github.com/web3qt/data4Trend/internal/types"
	"github.com/web3qt/data4Trend/pkg/logging"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// KLine GORM模型，对应数据库中的kline表
type KLine struct {
	ID           uint      `gorm:"primaryKey;autoIncrement"`
	Symbol       string    `gorm:"type:varchar(20);not null"`
	IntervalType string    `gorm:"type:varchar(10);not null"`
	OpenTime     time.Time `gorm:"not null"`
	CloseTime    time.Time `gorm:"not null"`
	OpenPrice    float64   `gorm:"type:decimal(20,8);not null"`
	HighPrice    float64   `gorm:"type:decimal(20,8);not null"`
	LowPrice     float64   `gorm:"type:decimal(20,8);not null"`
	ClosePrice   float64   `gorm:"type:decimal(20,8);not null"`
	Volume       float64   `gorm:"type:decimal(30,8);not null"`
	CreatedAt    time.Time `gorm:"autoCreateTime"`
	UpdatedAt    time.Time `gorm:"autoUpdateTime"`
}

// TableName 指定表名
func (KLine) TableName() string {
	return "kline"
}

// CoinKLine GORM模型，对应数据库中的币种表
type CoinKLine struct {
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

// TableName 根据币种动态指定表名
func (k *CoinKLine) TableName() string {
	// 使用结构体中保存的币种作为表名
	if k.coinSymbol != "" {
		return k.coinSymbol
	}
	// 返回一个默认表名，但应该避免这种情况
	logging.Logger.WithField("symbol", "unknown").Warn("遇到未知币种，将使用默认表名")
	return "unknown_coin"
}

// SetCoinSymbol 设置币种名称，用于表名指定
func (k *CoinKLine) SetCoinSymbol(symbol string) {
	if symbol == "" {
		logging.Logger.Warn("尝试设置空的币种符号")
		k.coinSymbol = "unknown_coin"
		return
	}
	k.coinSymbol = symbol
}

type MySQLStore struct {
	db            *gorm.DB
	dsn           string
	inputChan     <-chan *types.KLineData
	createdTables map[string]bool // 缓存已创建的表
	tablesMu      sync.RWMutex    // 保护 createdTables 的互斥锁
}

// MySQLConfig 为MySQL存储配置
type MySQLConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

func NewMySQLStore(cfg *MySQLConfig, input <-chan *types.KLineData) (*MySQLStore, error) {
	// 验证配置
	if cfg.Host == "" || cfg.Port == 0 || cfg.User == "" || cfg.Database == "" {
		logging.Logger.Error("MySQL配置不完整")
		return nil, fmt.Errorf("MySQL配置不完整")
	}

	logging.Logger.WithFields(logrus.Fields{
		"host": cfg.Host,
		"port": cfg.Port,
	}).Info("连接MySQL")

	// 构建DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)

	logging.Logger.WithFields(logrus.Fields{
		"host":     cfg.Host,
		"port":     cfg.Port,
		"user":     cfg.User,
		"database": cfg.Database,
	}).Info("MySQL配置信息")

	// 配置GORM日志
	gormLogger := logger.New(
		&logging.GormLogrusWriter{Logger: logging.Logger},
		logger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  logger.Info,
			IgnoreRecordNotFoundError: true,
			Colorful:                  false,
		},
	)

	// 连接数据库
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: gormLogger,
	})
	if err != nil {
		logging.Logger.WithError(err).Error("MySQL连接失败")
		return nil, fmt.Errorf("MySQL连接失败: %w", err)
	}

	// 不再自动迁移全局表结构，而是按需创建币种表
	logging.Logger.Info("数据库连接成功，将按需创建币种表")

	// 设置连接池参数
	sqlDB, err := db.DB()
	if err != nil {
		logging.Logger.WithError(err).Error("获取底层数据库连接失败")
		return nil, fmt.Errorf("获取底层数据库连接失败: %w", err)
	}

	sqlDB.SetMaxOpenConns(25)
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetConnMaxLifetime(time.Hour)

	logging.Logger.Info("MySQL连接池配置成功")

	// 尝试创建一个临时表来验证连接
	testSQL := `
CREATE TABLE IF NOT EXISTS connection_test (
    id INT PRIMARY KEY,
    test_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO connection_test (id) VALUES (1) ON DUPLICATE KEY UPDATE test_time = CURRENT_TIMESTAMP;
SELECT * FROM connection_test;
`
	var testResult []map[string]interface{}
	if err := db.Raw(testSQL).Find(&testResult).Error; err != nil {
		logging.Logger.WithError(err).Warn("MySQL测试失败")
		// 不返回错误，继续执行
	} else {
		logging.Logger.WithField("result", testResult).Info("MySQL测试成功")
	}

	return &MySQLStore{
		db:            db,
		dsn:           dsn,
		inputChan:     input,
		createdTables: make(map[string]bool),
		tablesMu:      sync.RWMutex{},
	}, nil
}

func (s *MySQLStore) Start(ctx context.Context) {
	go func() {
		count := 0
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		logging.Logger.Info("MySQL存储服务启动，等待数据...")

		// 添加调试信息，每10秒检查一次通道状态
		debugTicker := time.NewTicker(10 * time.Second)
		defer debugTicker.Stop()

		// 创建重试队列
		var retryQueue []*types.KLineData
		const maxRetryQueueSize = 5000
		retryTicker := time.NewTicker(5 * time.Second)
		defer retryTicker.Stop()

		// 连接状态
		isConnected := true
		reconnectTimer := time.NewTimer(10 * time.Second)
		defer reconnectTimer.Stop()
		if !reconnectTimer.Stop() {
			<-reconnectTimer.C
		}

		for {
			select {
			case <-ctx.Done():
				// 关闭数据库连接
				if sqlDB, err := s.db.DB(); err == nil {
					sqlDB.Close()
					logging.Logger.Info("MySQL连接已关闭")
				}
				return
			case data := <-s.inputChan:
				logging.Logger.WithFields(logrus.Fields{
					"symbol":    data.Symbol,
					"interval":  data.Interval,
					"open_time": data.OpenTime,
				}).Debug("收到K线数据")

				count++
				if !isConnected {
					// 数据库连接断开，加入重试队列
					if len(retryQueue) < maxRetryQueueSize {
						retryQueue = append(retryQueue, data)
						logging.Logger.WithField("queue_size", len(retryQueue)).Debug("添加到重试队列")
					} else {
						logging.Logger.Warn("重试队列已满，丢弃数据")
					}
				} else if err := s.writeDataPoint(data); err != nil {
					// 写入失败，检查是否是连接问题
					if isConnectionError(err) {
						isConnected = false
						// 将当前失败的数据加入重试队列
						if len(retryQueue) < maxRetryQueueSize {
							retryQueue = append(retryQueue, data)
							logging.Logger.WithField("queue_size", len(retryQueue)).Debug("添加到重试队列")
						}
						// 启动重连定时器
						reconnectTimer.Reset(10 * time.Second)
						logging.Logger.Error("数据库连接失败，将在10秒后尝试重连")
					} else {
						// 非连接错误，可以继续尝试写入
						logging.Logger.WithError(err).Error("写入失败")
					}
				}
			case <-ticker.C:
				if count > 0 {
					logging.Logger.WithFields(logrus.Fields{
						"count":           count,
						"is_connected":    isConnected,
						"retry_queue_len": len(retryQueue),
					}).Info("MySQL存储统计: 过去1分钟处理的数据")
					count = 0
				} else {
					logging.Logger.WithFields(logrus.Fields{
						"is_connected":    isConnected,
						"retry_queue_len": len(retryQueue),
					}).Debug("MySQL存储统计: 过去1分钟没有收到数据")
				}
			case <-debugTicker.C:
				logging.Logger.Debug("MySQL存储调试: 等待数据中...")

			case <-reconnectTimer.C:
				// 尝试重新连接数据库
				if !isConnected {
					if err := s.checkConnection(); err != nil {
						logging.Logger.WithError(err).Error("重新连接数据库失败，将在10秒后重试")
						reconnectTimer.Reset(10 * time.Second)
					} else {
						logging.Logger.Info("数据库重新连接成功")
						isConnected = true
					}
				}

			case <-retryTicker.C:
				// 如果连接恢复，尝试处理重试队列
				if isConnected && len(retryQueue) > 0 {
					logging.Logger.WithField("queue_size", len(retryQueue)).Info("处理重试队列")

					// 处理队列中的前50条数据
					batchSize := 50
					if len(retryQueue) < batchSize {
						batchSize = len(retryQueue)
					}

					successCount := 0
					for i := 0; i < batchSize; i++ {
						if err := s.writeDataPoint(retryQueue[i]); err != nil {
							if isConnectionError(err) {
								isConnected = false
								reconnectTimer.Reset(10 * time.Second)
								logging.Logger.Error("重试时数据库连接失败，将暂停重试")
								break
							}
							logging.Logger.WithError(err).Error("重试写入失败")
						} else {
							successCount++
						}
					}

					// 移除成功写入的数据
					if successCount > 0 {
						retryQueue = retryQueue[successCount:]
						logging.Logger.WithFields(logrus.Fields{
							"success":   successCount,
							"remaining": len(retryQueue),
						}).Info("重试写入成功")
					}
				}
			}
		}
	}()
}

// isConnectionError 判断是否是数据库连接错误
func isConnectionError(err error) bool {
	errorMsg := err.Error()
	// 常见的MySQL连接错误
	connectionErrors := []string{
		"connection refused",
		"server has gone away",
		"lost connection",
		"connection reset by peer",
		"connection timed out",
		"can't connect",
		"broken pipe",
	}

	for _, errText := range connectionErrors {
		if strings.Contains(errorMsg, errText) {
			return true
		}
	}
	return false
}

// checkConnection 检查数据库连接
func (s *MySQLStore) checkConnection() error {
	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Ping()
}

func (s *MySQLStore) writeDataPoint(data *types.KLineData) error {
	// 检查币种是否为空
	if data.Symbol == "" {
		logging.Logger.Warn("收到空币种符号的数据点，无法写入")
		return fmt.Errorf("币种符号为空")
	}

	// 记录开始写入数据
	logging.Logger.WithFields(logrus.Fields{
		"symbol":     data.Symbol,
		"interval":   data.Interval,
		"open_time":  data.OpenTime.Format(time.RFC3339),
		"close_time": data.CloseTime.Format(time.RFC3339),
		"open":       data.Open,
		"high":       data.High,
		"low":        data.Low,
		"close":      data.Close,
		"volume":     data.Volume,
	}).Info("开始写入数据点")

	// 获取清理后的币种表名
	cleanedSymbol := cleanSymbol(data.Symbol)
	logging.Logger.WithFields(logrus.Fields{
		"original": data.Symbol,
		"cleaned":  cleanedSymbol,
		"interval": data.Interval,
	}).Info("币种符号清理结果")

	// 确保表存在
	err := s.ensureTableExists(cleanedSymbol)
	if err != nil {
		logging.Logger.WithFields(logrus.Fields{
			"symbol": data.Symbol,
			"table":  cleanedSymbol,
			"error":  err,
		}).Error("确保表存在失败")
		return err
	}

	// 将KLineData转换为CoinKLine模型
	kline := CoinKLine{
		IntervalType: data.Interval,
		OpenTime:     data.OpenTime,
		CloseTime:    data.CloseTime,
		OpenPrice:    data.Open,
		HighPrice:    data.High,
		LowPrice:     data.Low,
		ClosePrice:   data.Close,
		Volume:       data.Volume,
	}

	// 明确设置表名
	kline.SetCoinSymbol(cleanedSymbol)

	// 验证数据库连接状态
	sqlDB, err := s.db.DB()
	if err != nil {
		logging.Logger.WithError(err).Error("获取数据库连接失败")
		return err
	}

	if err := sqlDB.Ping(); err != nil {
		logging.Logger.WithError(err).Error("数据库连接Ping失败")
		return err
	}

	// 首先检查记录是否已存在，使用明确的表名
	var count int64
	tableName := kline.TableName()
	s.db.Table(tableName).
		Where("interval_type = ? AND open_time = ?", kline.IntervalType, kline.OpenTime).
		Count(&count)

	if count > 0 {
		logging.Logger.WithFields(logrus.Fields{
			"symbol":    data.Symbol,
			"table":     tableName,
			"interval":  data.Interval,
			"open_time": data.OpenTime.Format(time.RFC3339),
		}).Info("数据已存在，跳过插入")
		return nil
	}

	// 明确指定表名进行插入
	result := s.db.Table(tableName).Create(&kline)

	if result.Error != nil {
		logging.Logger.WithFields(logrus.Fields{
			"error":     result.Error,
			"symbol":    data.Symbol,
			"table":     tableName,
			"interval":  data.Interval,
			"open_time": data.OpenTime.Format(time.RFC3339),
		}).Error("写入失败")
		return result.Error
	}

	// 记录写入结果
	logging.Logger.WithFields(logrus.Fields{
		"symbol":        data.Symbol,
		"table":         tableName,
		"interval":      data.Interval,
		"open_time":     data.OpenTime.Format(time.RFC3339),
		"rows_affected": result.RowsAffected,
	}).Info("数据写入成功")

	return nil
}

// 确保表存在，如不存在则创建
func (s *MySQLStore) ensureTableExists(tableName string) error {
	// 验证表名非空
	if tableName == "" {
		logging.Logger.Error("尝试创建空表名")
		return fmt.Errorf("表名不能为空")
	}

	// 检查表是否已经在缓存中
	s.tablesMu.RLock()
	exists := s.createdTables[tableName]
	s.tablesMu.RUnlock()

	if exists {
		logging.Logger.WithField("table", tableName).Debug("表已在缓存中")
		return nil
	}

	// 表不在缓存中，需要创建
	s.tablesMu.Lock()
	defer s.tablesMu.Unlock()

	// 再次检查，可能在获取锁的过程中已被其他协程创建
	if s.createdTables[tableName] {
		logging.Logger.WithField("table", tableName).Debug("表在获取锁的过程中已被创建")
		return nil
	}

	// 首先检查表是否已存在于数据库中
	var count int64
	countResult := s.db.Raw("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?", tableName).Count(&count)

	if countResult.Error != nil {
		logging.Logger.WithFields(logrus.Fields{
			"table": tableName,
			"error": countResult.Error,
		}).Error("检查表是否存在时出错")
		return countResult.Error
	}

	if count > 0 {
		// 表已存在，添加到缓存并返回
		s.createdTables[tableName] = true
		logging.Logger.WithField("table", tableName).Info("表已存在，添加到缓存")
		return nil
	}

	// 创建模型结构的临时实例用于迁移
	temp := &CoinKLine{}
	temp.SetCoinSymbol(tableName)

	// 自动迁移表结构
	logging.Logger.WithField("table", tableName).Info("创建新表")

	// 记录表结构SQL
	createTableSQL := `
CREATE TABLE IF NOT EXISTS ` + tableName + ` (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  interval_type VARCHAR(10) NOT NULL,
  open_time DATETIME NOT NULL,
  close_time DATETIME NOT NULL,
  open_price DECIMAL(20,8) NOT NULL,
  high_price DECIMAL(20,8) NOT NULL,
  low_price DECIMAL(20,8) NOT NULL,
  close_price DECIMAL(20,8) NOT NULL,
  volume DECIMAL(30,8) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY idx_interval_open_time (interval_type, open_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

	logging.Logger.WithFields(logrus.Fields{
		"table": tableName,
		"sql":   createTableSQL,
	}).Info("准备执行创建表SQL")

	// 尝试直接执行SQL创建表
	if err := s.db.Exec(createTableSQL).Error; err != nil {
		logging.Logger.WithFields(logrus.Fields{
			"table": tableName,
			"error": err,
		}).Error("使用SQL创建表失败，尝试使用AutoMigrate")

		// 如果SQL执行失败，尝试使用AutoMigrate
		err = s.db.Table(tableName).AutoMigrate(temp)
		if err != nil {
			logging.Logger.WithFields(logrus.Fields{
				"table": tableName,
				"error": err,
			}).Error("创建表结构失败")
			return err
		}
	}

	// 验证表已创建
	s.db.Raw("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?", tableName).Count(&count)
	if count == 0 {
		errMsg := fmt.Sprintf("表 %s 创建失败，在数据库中未找到", tableName)
		logging.Logger.Error(errMsg)
		return fmt.Errorf(errMsg)
	}

	// 添加到已创建表缓存
	s.createdTables[tableName] = true
	logging.Logger.WithField("table", tableName).Info("创建表结构完成")

	return nil
}

// cleanSymbol 清理符号名称，使其适合作为表名
func cleanSymbol(symbol string) string {
	if symbol == "" {
		logging.Logger.Warn("尝试清理空的币种符号")
		return "unknown_coin"
	}

	// 保留原始符号的副本用于日志记录
	originalSymbol := symbol

	// 先转小写
	cleanedSymbol := strings.ToLower(symbol)

	// 替换特殊字符
	cleanedSymbol = strings.ReplaceAll(cleanedSymbol, "/", "_")
	cleanedSymbol = strings.ReplaceAll(cleanedSymbol, "-", "_")
	cleanedSymbol = strings.ReplaceAll(cleanedSymbol, ".", "_")

	// 分离币种和计价单位，例如从"btcusdt"提取"btc"
	if strings.HasSuffix(cleanedSymbol, "usdt") {
		cleanedSymbol = strings.TrimSuffix(cleanedSymbol, "usdt")
	} else if strings.HasSuffix(cleanedSymbol, "usdc") {
		cleanedSymbol = strings.TrimSuffix(cleanedSymbol, "usdc")
	} else if strings.HasSuffix(cleanedSymbol, "busd") {
		cleanedSymbol = strings.TrimSuffix(cleanedSymbol, "busd")
	}

	// 如果清理后为空，使用默认名称
	if cleanedSymbol == "" {
		logging.Logger.WithField("original", originalSymbol).Warn("清理后的币种符号为空")
		return "unknown_coin"
	}

	logging.Logger.WithFields(logrus.Fields{
		"original": originalSymbol,
		"cleaned":  cleanedSymbol,
	}).Debug("币种符号清理结果")

	return cleanedSymbol
}

// getTableNameForSymbol 获取指定交易对对应的表名
func getTableNameForSymbol(symbol string) string {
	// 标准化处理：确保始终得到同一币种的唯一表名
	// 先把所有符号统一转为大写USDT后缀格式，再清理
	uppercaseSymbol := strings.ToUpper(symbol)

	// 如果不是以USDT/USDC/BUSD结尾，添加USDT后缀
	if !strings.HasSuffix(uppercaseSymbol, "USDT") &&
		!strings.HasSuffix(uppercaseSymbol, "USDC") &&
		!strings.HasSuffix(uppercaseSymbol, "BUSD") {
		uppercaseSymbol = uppercaseSymbol + "USDT"
		logging.Logger.WithFields(logrus.Fields{
			"original":   symbol,
			"normalized": uppercaseSymbol,
		}).Debug("符号已标准化")
	}

	// 使用cleanSymbol处理标准化后的符号
	return cleanSymbol(uppercaseSymbol)
}

// getSymbolFromTableName 从表名反向获取交易对
func getSymbolFromTableName(tableName string) string {
	// 处理特殊情况
	if tableName == "unknown_coin" {
		return "UNKNOWN"
	}

	// 将表名转换为大写
	symbol := strings.ToUpper(tableName)

	// 确保以USDT结尾
	if !strings.HasSuffix(symbol, "USDT") {
		symbol = symbol + "USDT"
	}

	return symbol
}

// QueryHistoryData 查询历史数据
func (s *MySQLStore) QueryHistoryData(ctx context.Context, symbol, start, end, pageSize, pageToken string) ([]*types.KLineData, string, error) {
	// 验证时间格式
	startTime, err := time.Parse(time.RFC3339, start)
	if err != nil {
		logging.Logger.WithError(err).Error("无效的开始时间格式")
		return nil, "", fmt.Errorf("无效的开始时间格式: %w", err)
	}
	endTime, err := time.Parse(time.RFC3339, end)
	if err != nil {
		logging.Logger.WithError(err).Error("无效的结束时间格式")
		return nil, "", fmt.Errorf("无效的结束时间格式: %w", err)
	}

	// 解析分页令牌
	offset := 0
	tableNameFromToken := ""
	if pageToken != "" {
		parts := strings.Split(pageToken, ":")
		if len(parts) == 2 {
			tableNameFromToken = parts[0]
			_, err := fmt.Sscanf(parts[1], "offset=%d", &offset)
			if err != nil {
				logging.Logger.WithError(err).Error("无效的分页令牌格式")
				return nil, "", fmt.Errorf("无效的分页令牌: %w", err)
			}
		} else {
			logging.Logger.Error("无效的分页令牌结构")
			return nil, "", fmt.Errorf("无效的分页令牌结构")
		}
	}

	// 解析页大小
	limit, err := strconv.Atoi(pageSize)
	if err != nil || limit <= 0 {
		limit = 1000 // 默认值
	}

	// 获取表名
	tableName := ""
	if symbol != "" {
		tableName = getTableNameForSymbol(symbol)
	} else if tableNameFromToken != "" {
		tableName = tableNameFromToken
	} else {
		// 如果没有指定币种，使用第一个表
		var tables []string
		s.db.Raw(`
			SELECT table_name 
			FROM information_schema.tables 
			WHERE table_schema = DATABASE() 
			AND table_name NOT IN ('connection_test')
			LIMIT 1
		`).Pluck("table_name", &tables)

		if len(tables) > 0 {
			tableName = tables[0]
		} else {
			// 没有表可用
			return []*types.KLineData{}, "", nil
		}
	}

	// 首先检查表是否存在
	if !s.tableExists(tableName) {
		return []*types.KLineData{}, "", nil
	}

	// 使用GORM查询
	var coinKlines []CoinKLine
	result := s.db.Table(tableName).
		Where("open_time BETWEEN ? AND ?", startTime, endTime).
		Order("open_time ASC").
		Limit(limit).
		Offset(offset).
		Find(&coinKlines)

	if result.Error != nil {
		logging.Logger.WithError(result.Error).Error("MySQL查询失败")
		return nil, "", fmt.Errorf("查询MySQL失败: %w", result.Error)
	}

	// 将GORM模型转换为KLineData
	var data []*types.KLineData
	symbolName := strings.ToUpper(tableName) // 表名转为大写作为币种符号

	for _, kline := range coinKlines {
		data = append(data, &types.KLineData{
			Symbol:    symbolName,
			Interval:  kline.IntervalType,
			OpenTime:  kline.OpenTime,
			CloseTime: kline.CloseTime,
			Open:      kline.OpenPrice,
			High:      kline.HighPrice,
			Low:       kline.LowPrice,
			Close:     kline.ClosePrice,
			Volume:    kline.Volume,
		})
	}

	// 生成下一页令牌
	nextToken := ""
	if len(data) >= limit {
		nextToken = fmt.Sprintf("%s:offset=%d", tableName, offset+limit)
	}

	logging.Logger.WithFields(logrus.Fields{
		"symbol":     symbolName,
		"table":      tableName,
		"start_time": startTime,
		"end_time":   endTime,
		"count":      len(data),
		"has_next":   nextToken != "",
	}).Info("历史数据查询完成")

	return data, nextToken, nil
}

// QueryKlines 查询指定交易对和间隔的最近K线数据
func (s *MySQLStore) QueryKlines(ctx context.Context, symbol string, interval string, limit int) ([]*types.KLineData, error) {
	// 限制最大返回数量
	if limit > 1000 {
		limit = 1000
	}

	// 获取表名
	tableName := getTableNameForSymbol(symbol)
	logging.Logger.WithFields(logrus.Fields{
		"symbol":   symbol,
		"table":    tableName,
		"interval": interval,
		"limit":    limit,
	}).Info("查询K线数据")

	// 测试，尝试直接从unknown_coin表查询
	logging.Logger.WithFields(logrus.Fields{
		"symbol":   symbol,
		"interval": interval,
	}).Info("直接尝试从unknown_coin表查询数据")

	unknownCoinData, err := s.queryFromUnknownCoin(ctx, symbol, interval, limit)
	if err == nil && len(unknownCoinData) > 0 {
		logging.Logger.WithFields(logrus.Fields{
			"symbol":   symbol,
			"interval": interval,
			"count":    len(unknownCoinData),
		}).Info("成功从unknown_coin表获取到数据，直接返回")
		return unknownCoinData, nil
	}

	// 首先检查表是否存在
	if !s.tableExists(tableName) {
		logging.Logger.WithFields(logrus.Fields{
			"symbol": symbol,
			"table":  tableName,
		}).Warn("表不存在，尝试从unknown_coin表查询数据")

		// 如果表不存在，尝试从unknown_coin表查询
		return s.queryFromUnknownCoin(ctx, symbol, interval, limit)
	}

	// 检查表中是否有数据
	var count int64
	s.db.WithContext(ctx).
		Table(tableName).
		Where("interval_type = ?", interval).
		Count(&count)

	if count == 0 {
		logging.Logger.WithFields(logrus.Fields{
			"symbol":   symbol,
			"table":    tableName,
			"interval": interval,
		}).Warn("表存在但没有数据，尝试从unknown_coin表查询")

		// 如果表中没有数据，尝试从unknown_coin表查询
		return s.queryFromUnknownCoin(ctx, symbol, interval, limit)
	}

	var coinKlines []CoinKLine
	result := s.db.WithContext(ctx).
		Table(tableName).
		Where("interval_type = ?", interval).
		Order("open_time DESC").
		Limit(limit).
		Find(&coinKlines)

	if result.Error != nil {
		logging.Logger.WithFields(logrus.Fields{
			"symbol":   symbol,
			"table":    tableName,
			"interval": interval,
			"error":    result.Error,
		}).Error("查询K线数据失败")
		return nil, result.Error
	}

	// 将数据库模型转换为领域模型，并按时间正序排列
	data := make([]*types.KLineData, len(coinKlines))
	for i, k := range coinKlines {
		idx := len(coinKlines) - i - 1 // 倒序
		data[idx] = &types.KLineData{
			Symbol:    symbol, // 使用请求中的原始符号而不是从表名反推
			Interval:  k.IntervalType,
			OpenTime:  k.OpenTime,
			CloseTime: k.CloseTime,
			Open:      k.OpenPrice,
			High:      k.HighPrice,
			Low:       k.LowPrice,
			Close:     k.ClosePrice,
			Volume:    k.Volume,
		}
	}

	logging.Logger.WithFields(logrus.Fields{
		"symbol":   symbol,
		"table":    tableName,
		"interval": interval,
		"count":    len(data),
	}).Info("查询K线数据成功")

	return data, nil
}

// queryFromUnknownCoin 从unknown_coin表查询数据
func (s *MySQLStore) queryFromUnknownCoin(ctx context.Context, symbol, interval string, limit int) ([]*types.KLineData, error) {
	unknownTable := "unknown_coin"

	// 检查unknown_coin表是否存在并有数据
	if !s.tableExists(unknownTable) {
		logging.Logger.WithField("symbol", symbol).Warn("unknown_coin表不存在")
		return []*types.KLineData{}, nil
	}

	logging.Logger.WithFields(logrus.Fields{
		"symbol":   symbol,
		"interval": interval,
		"table":    unknownTable,
		"limit":    limit,
	}).Info("从unknown_coin表查询数据")

	// 检查unknown_coin表中是否有符合条件的数据
	var count int64
	result := s.db.WithContext(ctx).
		Table(unknownTable).
		Where("interval_type = ?", interval).
		Count(&count)

	if result.Error != nil {
		logging.Logger.WithError(result.Error).Error("检查unknown_coin表中的数据量时出错")
		return nil, result.Error
	}

	logging.Logger.WithFields(logrus.Fields{
		"symbol":   symbol,
		"interval": interval,
		"count":    count,
		"table":    unknownTable,
	}).Info("查询到unknown_coin表中有符合条件的记录")

	// 从unknown_coin表查询数据
	var coinKlines []CoinKLine
	result = s.db.WithContext(ctx).
		Table(unknownTable).
		Where("interval_type = ?", interval).
		Order("open_time DESC").
		Limit(limit).
		Find(&coinKlines)

	if result.Error != nil {
		logging.Logger.WithError(result.Error).Error("从unknown_coin表查询数据失败")
		return nil, result.Error
	}

	logging.Logger.WithFields(logrus.Fields{
		"symbol":   symbol,
		"interval": interval,
		"found":    len(coinKlines),
		"limit":    limit,
		"table":    unknownTable,
	}).Info("从unknown_coin表查询到数据")

	// 将数据库模型转换为领域模型，并按时间正序排列
	data := make([]*types.KLineData, len(coinKlines))
	for i, k := range coinKlines {
		idx := len(coinKlines) - i - 1 // 倒序
		data[idx] = &types.KLineData{
			Symbol:    symbol, // 使用请求中的原始符号
			Interval:  k.IntervalType,
			OpenTime:  k.OpenTime,
			CloseTime: k.CloseTime,
			Open:      k.OpenPrice,
			High:      k.HighPrice,
			Low:       k.LowPrice,
			Close:     k.ClosePrice,
			Volume:    k.Volume,
		}
	}

	logging.Logger.WithFields(logrus.Fields{
		"symbol":   symbol,
		"interval": interval,
		"count":    len(data),
	}).Info("从unknown_coin表查询数据成功")

	return data, nil
}

// tableExists 检查表是否存在
func (s *MySQLStore) tableExists(tableName string) bool {
	var result int
	query := fmt.Sprintf("SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '%s' LIMIT 1", tableName)

	err := s.db.Raw(query).Scan(&result).Error
	if err != nil {
		logging.Logger.WithError(err).Errorf("检查表 %s 是否存在时出错", tableName)
		return false
	}

	return result == 1
}

// QueryHistoryKlines 查询历史K线数据
func (s *MySQLStore) QueryHistoryKlines(ctx context.Context, symbol string, interval string, startTime time.Time, endTime time.Time) ([]*types.KLineData, error) {
	// 获取表名
	tableName := getTableNameForSymbol(symbol)

	// 首先检查表是否存在
	if !s.tableExists(tableName) {
		return []*types.KLineData{}, nil // 返回空结果
	}

	var coinKlines []CoinKLine

	// 构建查询
	query := s.db.WithContext(ctx).
		Table(tableName).
		Where("interval_type = ?", interval).
		Where("open_time >= ? AND open_time <= ?", startTime, endTime).
		Order("open_time ASC")

	// 限制最大返回数量
	if endTime.Sub(startTime) > 7*24*time.Hour {
		query = query.Limit(5000) // 时间范围大于7天，限制5000条
	} else {
		query = query.Limit(1000) // 否则限制1000条
	}

	result := query.Find(&coinKlines)
	if result.Error != nil {
		return nil, result.Error
	}

	// 转换为领域模型
	data := make([]*types.KLineData, len(coinKlines))
	for i, k := range coinKlines {
		data[i] = &types.KLineData{
			Symbol:    symbol,
			Interval:  k.IntervalType,
			OpenTime:  k.OpenTime,
			CloseTime: k.CloseTime,
			Open:      k.OpenPrice,
			High:      k.HighPrice,
			Low:       k.LowPrice,
			Close:     k.ClosePrice,
			Volume:    k.Volume,
		}
	}

	return data, nil
}

// GetAvailableSymbols 获取所有可用的交易对
func (s *MySQLStore) GetAvailableSymbols(ctx context.Context) ([]map[string]interface{}, error) {
	// 获取所有币种表
	var tables []string
	s.db.Raw(`
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = DATABASE() 
		AND table_name NOT IN ('connection_test')
	`).Pluck("table_name", &tables)

	// 查询每个币种表的可用信息
	result := make([]map[string]interface{}, 0, len(tables))
	for _, tableName := range tables {
		// 跳过系统表
		if tableName == "connection_test" {
			continue
		}

		// 币种符号（表名转为大写）
		symbol := strings.ToUpper(tableName)

		// 查询表中的可用间隔
		var intervals []string
		s.db.WithContext(ctx).
			Table(tableName).
			Distinct("interval_type").
			Pluck("interval_type", &intervals)

		if len(intervals) == 0 {
			continue // 跳过没有间隔数据的表
		}

		// 获取最早和最新数据时间
		var firstTime, lastTime time.Time
		s.db.WithContext(ctx).
			Table(tableName).
			Order("open_time ASC").
			Limit(1).
			Pluck("open_time", &firstTime)

		s.db.WithContext(ctx).
			Table(tableName).
			Order("open_time DESC").
			Limit(1).
			Pluck("open_time", &lastTime)

		// 获取数据总量
		var count int64
		s.db.WithContext(ctx).
			Table(tableName).
			Count(&count)

		// 组装结果
		result = append(result, map[string]interface{}{
			"symbol":     symbol,
			"intervals":  intervals,
			"first_time": firstTime,
			"last_time":  lastTime,
			"count":      count,
		})
	}

	return result, nil
}

// GetStats 获取数据库统计信息
func (s *MySQLStore) GetStats(ctx context.Context) (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	// 获取所有币种表
	var tables []string
	s.db.Raw(`
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = DATABASE() 
		AND table_name NOT IN ('connection_test')
	`).Pluck("table_name", &tables)

	// 计算币种数量
	stats["symbol_count"] = len(tables)

	// 计算所有表的数据总量和各间隔的数据量
	var totalCount int64
	intervals := make(map[string]int64)

	var firstTime time.Time
	var lastTime time.Time

	for i, tableName := range tables {
		// 对每个表计算总记录数
		var tableCount int64
		s.db.Table(tableName).Count(&tableCount)
		totalCount += tableCount

		// 计算每个间隔的记录数
		var intervalStats []struct {
			Interval string `gorm:"column:interval_type"`
			Count    int64  `gorm:"column:count"`
		}
		s.db.Table(tableName).
			Select("interval_type, count(*) as count").
			Group("interval_type").
			Find(&intervalStats)

		for _, stat := range intervalStats {
			intervals[stat.Interval] += stat.Count
		}

		// 计算最早和最新的记录时间
		var tableFirstTime, tableLastTime time.Time
		s.db.Table(tableName).
			Order("open_time ASC").
			Limit(1).
			Pluck("open_time", &tableFirstTime)

		s.db.Table(tableName).
			Order("open_time DESC").
			Limit(1).
			Pluck("open_time", &tableLastTime)

		// 更新全局最早和最新时间
		if i == 0 || tableFirstTime.Before(firstTime) {
			firstTime = tableFirstTime
		}

		if i == 0 || tableLastTime.After(lastTime) {
			lastTime = tableLastTime
		}
	}

	stats["total_records"] = totalCount
	stats["intervals"] = intervals
	stats["first_time"] = firstTime
	stats["last_time"] = lastTime

	// 添加表信息
	tableStats := make([]map[string]interface{}, 0, len(tables))
	for _, tableName := range tables {
		var count int64
		s.db.Table(tableName).Count(&count)

		tableStats = append(tableStats, map[string]interface{}{
			"table": tableName,
			"count": count,
		})
	}
	stats["tables"] = tableStats

	return stats, nil
}

// SetInputChannel 设置输入通道
func (s *MySQLStore) SetInputChannel(input <-chan *types.KLineData) {
	s.inputChan = input
}

// DataGap 表示数据缺口
type DataGap struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// CheckDataGaps 检查指定币种和时间周期的数据缺口
func (s *MySQLStore) CheckDataGaps(ctx context.Context, symbol, interval string, startTime, endTime time.Time) ([]DataGap, error) {
	// 检查数据缺口
	tableName := getCoinTableName(symbol)

	// 确保表存在
	if err := s.ensureTableExists(tableName); err != nil {
		return nil, fmt.Errorf("确保表 %s 存在失败: %w", tableName, err)
	}

	// 查询所有数据
	var klines []CoinKLine
	result := s.db.WithContext(ctx).
		Table(tableName).
		Where("interval_type = ? AND open_time BETWEEN ? AND ?", interval, startTime, endTime).
		Order("open_time ASC").
		Find(&klines)

	if result.Error != nil {
		return nil, fmt.Errorf("查询数据失败: %w", result.Error)
	}

	// 如果数据点太少，无法检查缺口
	if len(klines) < 2 {
		return nil, nil
	}

	// 计算预期的时间间隔
	expectedDuration := calculateIntervalDuration(interval)
	if expectedDuration == 0 {
		return nil, fmt.Errorf("无法识别的周期格式: %s", interval)
	}

	// 检查缺口
	var gaps []DataGap
	for i := 1; i < len(klines); i++ {
		timeDiff := klines[i].OpenTime.Sub(klines[i-1].OpenTime)
		// 如果时间差大于预期的1.5倍，认为有缺口
		if timeDiff > time.Duration(float64(expectedDuration)*1.5) {
			gap := DataGap{
				Start: klines[i-1].OpenTime.Add(expectedDuration),
				End:   klines[i].OpenTime,
			}
			gaps = append(gaps, gap)
		}
	}

	return gaps, nil
}

// FixDataGap 修复指定的数据缺口
func (s *MySQLStore) FixDataGap(ctx context.Context, symbol, interval string, startTime, endTime time.Time) error {
	// 获取Binance客户端和API密钥
	cfg, err := config.LoadConfig()
	if err != nil {
		return fmt.Errorf("加载配置失败: %w", err)
	}

	// 创建Binance客户端
	var client *binance.Client
	if cfg.Binance.APIKey != "" && cfg.Binance.SecretKey != "" {
		client = binance.NewClient(cfg.Binance.APIKey, cfg.Binance.SecretKey)
	} else {
		client = binance.NewClient("", "")
	}
	client.HTTPClient = cfg.NewHTTPClient()

	// 获取数据
	klines, err := client.NewKlinesService().
		Symbol(symbol).
		Interval(interval).
		StartTime(startTime.UnixMilli()).
		EndTime(endTime.UnixMilli()).
		Limit(1000).
		Do(ctx)

	if err != nil {
		return fmt.Errorf("从Binance获取数据失败: %w", err)
	}

	if len(klines) == 0 {
		return fmt.Errorf("未找到数据")
	}

	// 转换为数据库模型
	var models []CoinKLine
	for _, k := range klines {
		model := CoinKLine{
			OpenTime:     time.Unix(k.OpenTime/1000, 0),
			CloseTime:    time.Unix(k.CloseTime/1000, 0),
			IntervalType: interval,
			OpenPrice:    parseFloat(k.Open),
			HighPrice:    parseFloat(k.High),
			LowPrice:     parseFloat(k.Low),
			ClosePrice:   parseFloat(k.Close),
			Volume:       parseFloat(k.Volume),
		}
		models = append(models, model)
	}

	// 保存到数据库
	tableName := getCoinTableName(symbol)
	return s.saveKLinesToTable(ctx, models, tableName)
}

// saveKLinesToTable 保存K线数据到指定表
func (s *MySQLStore) saveKLinesToTable(ctx context.Context, models []CoinKLine, tableName string) error {
	// 确保表存在
	if err := s.ensureTableExists(tableName); err != nil {
		return fmt.Errorf("确保表 %s 存在失败: %w", tableName, err)
	}

	// 使用事务批量保存
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, model := range models {
			// 使用 upsert 操作
			result := tx.Table(tableName).
				Where("interval_type = ? AND open_time = ?", model.IntervalType, model.OpenTime).
				Assign(model).
				FirstOrCreate(&model)

			if result.Error != nil {
				return result.Error
			}
		}
		return nil
	})
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

// getCoinTableName 获取币种对应的表名
func getCoinTableName(symbol string) string {
	return getTableNameForSymbol(symbol)
}

// parseFloat 将字符串解析为float64
func parseFloat(str string) float64 {
	val, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return 0
	}
	return val
}

// 添加SaveKLineData方法
func (s *MySQLStore) SaveKLineData(ctx context.Context, data *types.KLineData) error {
	// 实现数据保存逻辑
	return nil
}

// DeleteKLinesInTimeRange 删除指定时间范围内的K线数据
func (s *MySQLStore) DeleteKLinesInTimeRange(ctx context.Context, symbol string, interval string, startTime time.Time, endTime time.Time) error {
	logging.Logger.WithFields(logrus.Fields{
		"symbol":     symbol,
		"interval":   interval,
		"start_time": startTime.Format(time.RFC3339),
		"end_time":   endTime.Format(time.RFC3339),
	}).Info("删除时间范围内的K线数据")

	// 验证输入
	if symbol == "" || interval == "" {
		return fmt.Errorf("交易对和时间间隔不能为空")
	}

	if startTime.After(endTime) {
		return fmt.Errorf("开始时间不能晚于结束时间")
	}

	// 验证间隔是否是支持的时间周期
	if interval != "15m" && interval != "4h" && interval != "1d" {
		return fmt.Errorf("不支持的时间间隔: %s，仅支持 15m, 4h, 1d", interval)
	}

	// 获取表名
	tableName := getCoinTableName(symbol)
	
	// 检查表是否存在
	if !s.tableExists(tableName) {
		return fmt.Errorf("交易对表不存在: %s", tableName)
	}

	// 执行删除操作
	result := s.db.Where("interval_type = ? AND open_time >= ? AND open_time <= ?", 
		interval, startTime, endTime).
		Delete(&CoinKLine{})
	
	if result.Error != nil {
		logging.Logger.WithFields(logrus.Fields{
			"symbol":     symbol,
			"interval":   interval,
			"start_time": startTime,
			"end_time":   endTime,
			"error":      result.Error,
		}).Error("删除K线数据失败")
		return fmt.Errorf("删除K线数据失败: %w", result.Error)
	}

	logging.Logger.WithFields(logrus.Fields{
		"symbol":      symbol,
		"interval":    interval,
		"start_time":  startTime,
		"end_time":    endTime,
		"rows_deleted": result.RowsAffected,
	}).Info("成功删除K线数据")

	return nil
}
