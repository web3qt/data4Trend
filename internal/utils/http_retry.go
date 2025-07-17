package utils

import (
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

// RetryConfig 重试配置
type RetryConfig struct {
	MaxRetries    int
	BaseDelay     time.Duration
	MaxDelay      time.Duration
	BackoffFactor float64
}

// DefaultRetryConfig 默认重试配置
var DefaultRetryConfig = RetryConfig{
	MaxRetries:    3,
	BaseDelay:     1 * time.Second,
	MaxDelay:      30 * time.Second,
	BackoffFactor: 2.0,
}

// DoWithRetry 执行带重试的HTTP请求
func DoWithRetry(client *http.Client, req *http.Request, config RetryConfig) (*http.Response, error) {
	var lastErr error
	
	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		if attempt > 0 {
			// 计算退避延迟
			delay := calculateBackoff(attempt, config)
			
			logrus.WithFields(logrus.Fields{
				"attempt": attempt,
				"delay":   delay,
				"url":     req.URL.String(),
			}).Warn("HTTP请求重试")
			
			select {
			case <-time.After(delay):
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
		}
		
		// 执行请求
		resp, err := client.Do(req)
		if err == nil {
			// 检查响应状态
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return resp, nil
			}
			
			// 4xx 错误不重试
			if resp.StatusCode >= 400 && resp.StatusCode < 500 {
				return resp, nil
			}
			
			// 5xx 错误可以重试
			resp.Body.Close()
			lastErr = &HTTPError{
				StatusCode: resp.StatusCode,
				Message:    resp.Status,
			}
		} else {
			// 网络错误可以重试
			lastErr = err
		}
		
		// 如果是最后一个尝试，返回错误
		if attempt == config.MaxRetries {
			logrus.WithFields(logrus.Fields{
				"attempt": attempt + 1,
				"url":     req.URL.String(),
				"error":   lastErr,
			}).Error("HTTP请求最终失败")
			return nil, lastErr
		}
	}
	
	return nil, lastErr
}

// calculateBackoff 计算退避延迟
func calculateBackoff(attempt int, config RetryConfig) time.Duration {
	if attempt <= 0 {
		return 0
	}
	
	delay := float64(config.BaseDelay) * math.Pow(config.BackoffFactor, float64(attempt-1))
	delay = math.Min(delay, float64(config.MaxDelay))
	
	// 添加随机抖动，避免惊群效应
	jitter := 0.1 * delay
	delay += (rand.Float64() - 0.5) * 2 * jitter
	
	return time.Duration(delay)
}

// HTTPError 自定义HTTP错误
type HTTPError struct {
	StatusCode int
	Message    string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Message)
}

// CreateBinanceClient 创建Binance API客户端
func CreateBinanceClient(baseURL string, httpClient *http.Client) *http.Client {
	// 如果httpClient为nil，创建默认的
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 120 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
				TLSHandshakeTimeout: 30 * time.Second,
			},
		}
	}
	
	return httpClient
}