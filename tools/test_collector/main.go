package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Println("简单测试程序")
	fmt.Println("当前工作目录:", os.Getwd())
	fmt.Println("环境变量:", os.Environ())

	// 测试成功
	fmt.Println("测试成功")
}
