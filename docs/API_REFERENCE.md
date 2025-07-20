# API参考文档

## 概览

币安WebSocket数据收集器提供RESTful API接口，用于查询收集的1分钟K线数据和系统状态。

**Base URL**: `http://localhost:8080`

## 认证

当前版本不需要认证。所有API接口都是开放的。

## 数据查询接口

### 获取K线数据

**GET** `/api/klines`

查询指定交易对的K线数据。

#### 请求参数

| 参数 | 类型 | 必填 | 说明 | 默认值 |
|------|------|------|------|--------|
| symbol | string | 是 | 交易对符号，如BTCUSDT | - |
| limit | int | 否 | 返回数据条数，最大1000 | 100 |
| start_time | string | 否 | 开始时间 (ISO 8601格式) | - |
| end_time | string | 否 | 结束时间 (ISO 8601格式) | - |

#### 请求示例

```bash
# 获取BTCUSDT最新100条数据
curl "http://localhost:8080/api/klines?symbol=BTCUSDT&limit=100"

# 获取指定时间范围的数据
curl "http://localhost:8080/api/klines?symbol=BTCUSDT&start_time=2024-01-01T00:00:00Z&end_time=2024-01-02T00:00:00Z"
```

#### 响应示例

```json
{
  "code": 200,
  "message": "success",
  "data": [
    {
      "symbol": "BTCUSDT",
      "open_time": "2024-01-01T00:00:00Z",
      "close_time": "2024-01-01T00:01:00Z",
      "open": "42000.50",
      "high": "42100.00",
      "low": "41950.00",
      "close": "42050.75",
      "volume": "15.25000000",
      "quote_asset_volume": "641265.75000000",
      "number_of_trades": 856,
      "taker_buy_base_asset_volume": "8.75000000",
      "taker_buy_quote_asset_volume": "368125.50000000"
    }
  ],
  "total": 1
}
```

### 获取最新价格

**GET** `/api/prices`

获取所有交易对或指定交易对的最新价格。

#### 请求参数

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| symbol | string | 否 | 交易对符号，如BTCUSDT。为空则返回所有交易对 |

#### 请求示例

```bash
# 获取所有交易对最新价格
curl "http://localhost:8080/api/prices"

# 获取BTCUSDT最新价格
curl "http://localhost:8080/api/prices?symbol=BTCUSDT"
```

#### 响应示例

```json
{
  "code": 200,
  "message": "success",
  "data": [
    {
      "symbol": "BTCUSDT",
      "price": "42050.75",
      "time": "2024-01-01T00:01:00Z"
    },
    {
      "symbol": "ETHUSDT",
      "price": "2580.25",
      "time": "2024-01-01T00:01:00Z"
    }
  ]
}
```

### 获取交易对列表

**GET** `/api/symbols`

获取系统支持的所有交易对列表。

#### 请求示例

```bash
curl "http://localhost:8080/api/symbols"
```

#### 响应示例

```json
{
  "code": 200,
  "message": "success",
  "data": [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "ADAUSDT"
  ],
  "total": 4
}
```

## 系统监控接口

### 获取系统统计

**GET** `/api/stats`

获取系统运行统计信息。

#### 请求示例

```bash
curl "http://localhost:8080/api/stats"
```

#### 响应示例

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "timestamp": "2024-01-01T00:01:00Z",
    "active_symbols": 1500,
    "total_records": 15678900,
    "websocket_connections": 1500,
    "data_collection_rate": 1500.5,
    "memory_usage_mb": 256.75,
    "disk_usage_mb": 15234.50,
    "error_count": 0,
    "last_error": "",
    "uptime_seconds": 86400
  }
}
```

### 获取WebSocket连接状态

**GET** `/api/websocket/status`

获取所有WebSocket连接的状态信息。

#### 请求参数

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| symbol | string | 否 | 特定交易对符号 |
| status | string | 否 | 连接状态过滤：connected, disconnected, error |

#### 请求示例

```bash
# 获取所有连接状态
curl "http://localhost:8080/api/websocket/status"

# 获取特定交易对连接状态
curl "http://localhost:8080/api/websocket/status?symbol=BTCUSDT"

# 获取异常连接
curl "http://localhost:8080/api/websocket/status?status=error"
```

#### 响应示例

```json
{
  "code": 200,
  "message": "success",
  "data": [
    {
      "symbol": "BTCUSDT",
      "connection_status": "connected",
      "last_data_time": "2024-01-01T00:01:00Z",
      "reconnect_count": 0,
      "error_message": "",
      "connected_at": "2024-01-01T00:00:00Z"
    },
    {
      "symbol": "ETHUSDT",
      "connection_status": "error",
      "last_data_time": "2024-01-01T00:00:30Z",
      "reconnect_count": 3,
      "error_message": "connection timeout",
      "connected_at": "2024-01-01T00:00:00Z"
    }
  ],
  "total": 2
}
```

### 获取数据质量指标

**GET** `/api/data/quality`

获取数据质量监控指标。

#### 请求参数

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| symbol | string | 否 | 交易对符号 |
| date | string | 否 | 查询日期 (YYYY-MM-DD格式) |

#### 请求示例

```bash
# 获取所有交易对今日数据质量
curl "http://localhost:8080/api/data/quality"

# 获取BTCUSDT今日数据质量
curl "http://localhost:8080/api/data/quality?symbol=BTCUSDT"

# 获取指定日期数据质量
curl "http://localhost:8080/api/data/quality?date=2024-01-01"
```

#### 响应示例

```json
{
  "code": 200,
  "message": "success",
  "data": [
    {
      "symbol": "BTCUSDT",
      "date": "2024-01-01",
      "expected_records": 1440,
      "actual_records": 1438,
      "missing_records": 2,
      "duplicate_records": 0,
      "data_completeness_rate": 99.86,
      "last_updated": "2024-01-01T23:59:00Z"
    }
  ]
}
```

## 系统管理接口

### 健康检查

**GET** `/api/health`

检查系统健康状态。

#### 请求示例

```bash
curl "http://localhost:8080/api/health"
```

#### 响应示例

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "status": "healthy",
    "timestamp": "2024-01-01T00:01:00Z",
    "checks": {
      "database": "ok",
      "websocket_connections": "ok",
      "memory_usage": "ok",
      "disk_space": "ok"
    }
  }
}
```

### 重启WebSocket连接

**POST** `/api/websocket/restart`

重启指定或所有WebSocket连接。

#### 请求体

```json
{
  "symbol": "BTCUSDT",  // 可选，不指定则重启所有连接
  "force": false        // 可选，是否强制重启
}
```

#### 请求示例

```bash
# 重启所有WebSocket连接
curl -X POST "http://localhost:8080/api/websocket/restart" \
  -H "Content-Type: application/json" \
  -d '{}'

# 重启特定交易对连接
curl -X POST "http://localhost:8080/api/websocket/restart" \
  -H "Content-Type: application/json" \
  -d '{"symbol":"BTCUSDT"}'
```

#### 响应示例

```json
{
  "code": 200,
  "message": "WebSocket connections restarted successfully",
  "data": {
    "restarted_count": 1500,
    "failed_count": 0
  }
}
```

## WebSocket实时接口

### 实时K线数据流

**WebSocket** `/ws/klines`

订阅实时K线数据更新。

#### 连接示例

```javascript
const ws = new WebSocket('ws://localhost:8080/ws/klines');

// 订阅特定交易对
ws.send(JSON.stringify({
  action: 'subscribe',
  symbol: 'BTCUSDT'
}));

// 取消订阅
ws.send(JSON.stringify({
  action: 'unsubscribe',
  symbol: 'BTCUSDT'
}));
```

#### 数据格式

```json
{
  "type": "kline",
  "symbol": "BTCUSDT",
  "data": {
    "open_time": "2024-01-01T00:01:00Z",
    "close_time": "2024-01-01T00:02:00Z",
    "open": "42000.50",
    "high": "42100.00",
    "low": "41950.00",
    "close": "42050.75",
    "volume": "15.25000000",
    "quote_asset_volume": "641265.75000000",
    "number_of_trades": 856
  }
}
```

## 错误响应

所有API接口在出错时返回统一的错误格式：

```json
{
  "code": 400,
  "message": "Invalid symbol parameter",
  "error": "Symbol 'INVALID' not found"
}
```

### 错误代码

| 代码 | 说明 |
|------|------|
| 200 | 成功 |
| 400 | 请求参数错误 |
| 404 | 资源不存在 |
| 500 | 服务器内部错误 |
| 503 | 服务不可用 |

## 限流策略

- 每个IP每分钟最多请求1000次
- WebSocket连接每个IP最多10个并发连接
- 大批量数据查询(limit>500)每分钟最多10次

## 性能建议

1. **批量查询**: 尽量使用时间范围查询而不是多次单点查询
2. **分页**: 大量数据使用分页查询，避免一次查询过多数据
3. **缓存**: 客户端应实现适当的缓存策略
4. **WebSocket**: 实时数据使用WebSocket而不是轮询
5. **符合使用**: 只查询需要的字段和时间范围

## SDK和工具

### Python示例

```python
import requests
import websocket
import json

# REST API查询
def get_klines(symbol, limit=100):
    url = f"http://localhost:8080/api/klines"
    params = {"symbol": symbol, "limit": limit}
    response = requests.get(url, params=params)
    return response.json()

# WebSocket订阅
def on_message(ws, message):
    data = json.loads(message)
    print(f"Received: {data}")

def subscribe_klines(symbol):
    ws = websocket.WebSocketApp(
        "ws://localhost:8080/ws/klines",
        on_message=on_message
    )
    
    def on_open(ws):
        subscribe_msg = {
            "action": "subscribe",
            "symbol": symbol
        }
        ws.send(json.dumps(subscribe_msg))
    
    ws.on_open = on_open
    ws.run_forever()

# 使用示例
klines = get_klines("BTCUSDT", 10)
print(klines)
```

### Go示例

```go
package main

import (
    "encoding/json"
    "fmt"
    "net/http"
    "net/url"
)

type KlineResponse struct {
    Code    int           `json:"code"`
    Message string        `json:"message"`
    Data    []KlineData   `json:"data"`
    Total   int           `json:"total"`
}

type KlineData struct {
    Symbol                   string `json:"symbol"`
    OpenTime                string `json:"open_time"`
    CloseTime               string `json:"close_time"`
    Open                    string `json:"open"`
    High                    string `json:"high"`
    Low                     string `json:"low"`
    Close                   string `json:"close"`
    Volume                  string `json:"volume"`
    QuoteAssetVolume        string `json:"quote_asset_volume"`
    NumberOfTrades          int64  `json:"number_of_trades"`
}

func GetKlines(symbol string, limit int) (*KlineResponse, error) {
    baseURL := "http://localhost:8080/api/klines"
    params := url.Values{}
    params.Add("symbol", symbol)
    params.Add("limit", fmt.Sprintf("%d", limit))
    
    resp, err := http.Get(baseURL + "?" + params.Encode())
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()
    
    var result KlineResponse
    err = json.NewDecoder(resp.Body).Decode(&result)
    return &result, err
}

func main() {
    klines, err := GetKlines("BTCUSDT", 10)
    if err != nil {
        panic(err)
    }
    
    fmt.Printf("Retrieved %d klines for BTCUSDT\n", len(klines.Data))
}
``` 