package main

import (
	"fmt"
	"strings"
)

// cleanSymbol 清理符号名称，使其适合作为表名
func cleanSymbol(symbol string) string {
	if symbol == "" {
		fmt.Println("尝试清理空的币种符号")
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

	// 不再提取基础币种部分，保留完整的交易对名称
	// 例如：btcusdt保持为btcusdt，而不是提取为btc

	// 如果清理后为空，使用默认名称
	if cleanedSymbol == "" {
		fmt.Printf("清理后的币种符号为空: %s\n", originalSymbol)
		return "unknown_coin"
	}

	fmt.Printf("币种符号清理结果: %s -> %s\n", originalSymbol, cleanedSymbol)
	return cleanedSymbol
}

func main() {
	// 测试各种币种
	testSymbols := []string{
		"BTCUSDT",
		"ETHUSDT",
		"BTC",
		"ETH",
		"BTC-USDT",
		"ETH/USDT",
		"BTC.USDT",
		"UNKNOWN",
		"",
	}

	for _, symbol := range testSymbols {
		cleaned := cleanSymbol(symbol)
		fmt.Printf("%-10s -> %s\n", symbol, cleaned)
	}
}
