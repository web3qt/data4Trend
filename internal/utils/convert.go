package utils

import "fmt"

// ParseFloat 将字符串转换为浮点数
func ParseFloat(s string) float64 {
	var f float64
	fmt.Sscanf(s, "%f", &f)
	return f
}
