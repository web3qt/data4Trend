package batchwriter

import (
	"github.com/sirupsen/logrus"

	"data4trend/internal/types"
)

// BatchMessageHandler implements the MessageHandler interface for batch writing
type BatchMessageHandler struct {
	batchWriter *BatchWriter
	logger      *logrus.Logger
}

// NewBatchMessageHandler creates a new batch message handler
func NewBatchMessageHandler(batchWriter *BatchWriter, logger *logrus.Logger) *BatchMessageHandler {
	return &BatchMessageHandler{
		batchWriter: batchWriter,
		logger:      logger,
	}
}

// HandleMessage handles a kline data message by adding it to the batch
func (h *BatchMessageHandler) HandleMessage(klineData *types.KlineData) error {
	h.logger.Debugf("Handling kline data message: %s", klineData.Symbol)
	return h.batchWriter.AddKlineData(klineData)
}