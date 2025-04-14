package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("使用方法: go run tools/api_checker.go <API地址> [端点路径]")
		fmt.Println("例如: go run tools/api_checker.go http://localhost:8080 /api/v1/klines?symbol=BTCUSDT&interval=1h")
		os.Exit(1)
	}

	// 获取API地址
	apiHost := os.Args[1]
	fmt.Printf("测试API服务器: %s\n", apiHost)

	// 创建HTTP客户端
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	// 默认测试端点
	defaultEndpoints := []struct {
		name   string
		path   string
		method string
	}{
		{"获取K线数据", "/api/v1/klines?symbol=BTCUSDT&interval=1h&limit=10", "GET"},
		{"获取历史数据", "/api/v1/history?symbol=BTCUSDT&interval=1h&limit=10", "GET"},
		{"获取支持的交易对", "/api/v1/symbols", "GET"},
		{"获取系统统计信息", "/api/v1/stats", "GET"},
		{"检查数据缺口", "/api/v1/check_gaps?symbol=BTCUSDT&interval=1h", "GET"},
	}

	// 如果提供了特定端点，只测试该端点
	if len(os.Args) > 2 {
		customPath := os.Args[2]
		endpoints := []struct {
			name   string
			path   string
			method string
		}{
			{"自定义端点", customPath, "GET"},
		}
		testEndpoints(httpClient, apiHost, endpoints)
	} else {
		// 否则测试所有默认端点
		testEndpoints(httpClient, apiHost, defaultEndpoints)
	}

	fmt.Println("\nAPI测试完成!")
}

// testEndpoints 测试多个API端点
func testEndpoints(client *http.Client, apiHost string, endpoints []struct {
	name   string
	path   string
	method string
}) {
	for _, endpoint := range endpoints {
		fmt.Printf("\n测试 %s...\n", endpoint.name)

		req, err := http.NewRequest(endpoint.method, apiHost+endpoint.path, nil)
		if err != nil {
			fmt.Printf("创建请求失败: %v\n", err)
			continue
		}

		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("请求失败: %v\n", err)
			continue
		}

		// 读取响应
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			fmt.Printf("读取响应失败: %v\n", err)
			continue
		}

		// 检查状态码
		if resp.StatusCode != http.StatusOK {
			fmt.Printf("请求返回状态码: %d, 响应体: %s\n", resp.StatusCode, body)
			continue
		}

		// 尝试解析响应为JSON
		var result interface{}
		if err := json.Unmarshal(body, &result); err != nil {
			fmt.Printf("解析JSON响应失败: %v\n", err)
			continue
		}

		// 输出响应摘要
		fmt.Printf("%s 成功! 状态码: %d, 响应字节数: %d\n", endpoint.name, resp.StatusCode, len(body))

		// 如果是较短的响应，打印出来
		if len(body) < 1000 {
			fmt.Printf("响应内容: %s\n", string(body))
		} else {
			// 否则只打印摘要
			var jsonObj map[string]interface{}
			if err := json.Unmarshal(body, &jsonObj); err == nil {
				// 打印数据数量
				if data, ok := jsonObj["data"].([]interface{}); ok {
					fmt.Printf("数据项数量: %d\n", len(data))
					if len(data) > 0 {
						// 打印第一项数据
						firstItem, _ := json.Marshal(data[0])
						fmt.Printf("第一项数据: %s\n", string(firstItem))
					}
				}
			}
		}
	}
}
