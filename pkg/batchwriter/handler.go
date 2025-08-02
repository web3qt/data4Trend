package batchwriter

import (
	"github.com/sirupsen/logrus"

	"data4trend/internal/types"
)

// BatchMessageHandler 实现批量写入的消息处理器接口
type BatchMessageHandler struct {
	batchWriter *BatchWriter
	logger      *logrus.Logger
}

// NewBatchMessageHandler 创建一个新的批量消息处理器
func NewBatchMessageHandler(batchWriter *BatchWriter, logger *logrus.Logger) *BatchMessageHandler {
	return &BatchMessageHandler{
		batchWriter: batchWriter,
		logger:      logger,
	}
}

// HandleMessage 通过将K线数据添加到批次中来处理消息
func (h *BatchMessageHandler) HandleMessage(klineData *types.KlineData) error {
	h.logger.Debugf("Handling kline data message: %s", klineData.Symbol)
	return h.batchWriter.AddKlineData(klineData)
}