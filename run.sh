#!/bin/bash

# 设置执行权限
chmod +x ./run.sh

# 运行程序
echo "启动数据馈送系统..."
go run ./cmd/main.go