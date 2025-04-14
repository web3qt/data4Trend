#!/bin/bash

# 设置默认端口
PORT=${1:-8080}

echo "正在启动API服务器，端口: $PORT"

# 切换到项目根目录
cd "$(dirname "$0")/.." || exit 1

# 编译并运行dataFeeder程序
if go build -o dataFeeder cmd/main.go; then
  echo "编译成功，启动服务器..."
  ./dataFeeder -api-only -port $PORT
else
  echo "编译失败，请检查代码"
  exit 1
fi 