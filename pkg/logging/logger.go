package logging

import (
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"github.com/web3qt/dataFeeder/config"
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
			file, err := os.OpenFile(cfg.OutputPath,
				os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
			if err == nil {
				baseLogger.SetOutput(file)
				baseLogger.Info("日志输出已设置到文件: " + cfg.OutputPath)
			} else {
				baseLogger.WithError(err).Warn("无法打开日志文件，使用标准输出")
			}
		}
	}

	// 初始化全局Logger为Entry类型，以支持WithFields方法
	Logger = baseLogger.WithFields(logrus.Fields{})
}
