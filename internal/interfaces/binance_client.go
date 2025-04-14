package interfaces

import (
	"net/http"

	"github.com/adshao/go-binance/v2"
)

// BinanceClient 定义了Binance客户端的接口
type BinanceClient interface {
	NewKlinesService() KlinesService
	HTTPClient() *http.Client
}

// BinanceClientImpl 包装官方客户端实现接口
type BinanceClientImpl struct {
	*binance.Client
}

func (c *BinanceClientImpl) NewKlinesService() KlinesService {
	return NewBinanceKlinesServiceAdapter(c.Client.NewKlinesService())
}

func (c *BinanceClientImpl) HTTPClient() *http.Client {
	return c.Client.HTTPClient
}

// MockBinanceClient 是BinanceClient的mock实现
type MockBinanceClient struct {
	KlinesService KlinesService
	Client        *http.Client
}

func (c *MockBinanceClient) NewKlinesService() KlinesService {
	return c.KlinesService
}

func (c *MockBinanceClient) HTTPClient() *http.Client {
	return c.Client
}
