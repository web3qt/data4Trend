package logging

import (
	"io"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"github.com/web3qt/data4Trend/config"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Logger 是全局日志对象，使用Entry类型以支持WithFields方法
var Logger *logrus.Entry

// GormLogrusWriter 实现io.Writer接口，用于适配GORM日志到logrus
type GormLogrusWriter struct {
	Logger *logrus.Entry
}

// Write 实现io.Writer接口
func (w *GormLogrusWriter) Write(p []byte) (n int, err error) {
	w.Logger.Debug(string(p))
	return len(p), nil
}

// Printf 实现logger.Writer接口
func (w *GormLogrusWriter) Printf(format string, args ...interface{}) {
	w.Logger.Debugf(format, args...)
}

func InitLogger(cfg *config.LogConfig) {
	// 初始化基础Logger
	baseLogger := logrus.New()

	// 设置日志级别
	level, err := logrus.ParseLevel(cfg.Level)
	if err != nil {
		baseLogger.SetLevel(logrus.InfoLevel)
	} else {
		baseLogger.SetLevel(level)
	}

	// 设置输出格式
	if cfg.JSONFormat {
		baseLogger.SetFormatter(&logrus.JSONFormatter{})
	} else {
		baseLogger.SetFormatter(&logrus.TextFormatter{
			FullTimestamp: true,
		})
	}

	// 设置输出目标
	if cfg.OutputPath != "" {
		// 确保日志目录存在
		logDir := filepath.Dir(cfg.OutputPath)
		if err := os.MkdirAll(logDir, 0755); err != nil {
			baseLogger.WithError(err).Warn("创建日志目录失败，使用标准输出")
		} else {
			// 使用lumberjack进行日志轮转
			lumber := &lumberjack.Logger{
				Filename:   cfg.OutputPath,
				MaxSize:    cfg.MaxSize,    // 单个文件最大大小（MB）
				MaxAge:     cfg.MaxAge,     // 文件保留天数
				MaxBackups: cfg.MaxBackups, // 最大备份数量
				Compress:   cfg.Compress,   // 压缩备份文件
				LocalTime:  true,           // 使用本地时间
			}
			
			// 创建多输出Writer，同时输出到文件和控制台
			multiWriter := io.MultiWriter(lumber, os.Stdout)
			baseLogger.SetOutput(multiWriter)
			
			baseLogger.WithFields(logrus.Fields{
				"file":        cfg.OutputPath,
				"max_size":    cfg.MaxSize,
				"max_age":     cfg.MaxAge,
				"max_backups": cfg.MaxBackups,
				"compress":    cfg.Compress,
			}).Info("日志轮转已配置")
		}
	}

	// 初始化全局Logger为Entry类型，以支持WithFields方法
	Logger = baseLogger.WithFields(logrus.Fields{})
}
