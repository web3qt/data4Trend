#!/bin/bash

# 设置执行权限
chmod +x ./reset_and_restart.sh

echo "========================================================"
echo "      重置数据库并重新启动Data4Trend数据采集服务        "
echo "========================================================"

# 设置数据收集开始时间（默认为2022年1月1日）
START_TIME=${1:-"2022-01-01T00:00:00Z"}
echo "将使用开始时间: $START_TIME"

# 停止现有的服务
echo "尝试停止现有服务..."
pkill -f "go run cmd/main.go" || pkill -f "dataFeeder" || true
echo "等待进程结束..."
sleep 2

# 重置数据库
echo "重置数据库..."
./reset_local_db.sh

# 编译应用
echo "重新编译应用..."
go build -o dataFeeder cmd/main.go

# 设置环境变量
export COLLECTION_START_TIME="$START_TIME"
echo "设置数据收集开始时间: $COLLECTION_START_TIME"

# 如果有API密钥，从环境文件加载
if [ -f .env ]; then
    echo "加载API密钥从.env文件..."
    source .env
fi

# 重新启动服务
echo "启动数据服务..."
./dataFeeder &

echo "服务已在后台启动"
echo "使用以下命令检查数据库: go run check_db.go"
echo "使用以下命令检查BTC数据: go run check_btc.go"
echo "查看日志: tail -f logs/dataFeeder.log"
echo "========================================================" 