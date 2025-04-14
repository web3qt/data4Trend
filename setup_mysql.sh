#!/bin/bash

# 设置执行权限
chmod +x ./setup_mysql.sh

# 检查MySQL是否已经运行
if ! docker ps | grep -q mysql; then
  echo "启动MySQL Docker容器..."
  docker-compose up -d mysql
  
  # 等待MySQL启动
  echo "等待MySQL启动..."
  sleep 10
  
  # 检查MySQL是否成功启动
  if ! docker ps | grep -q mysql; then
    echo "错误: MySQL启动失败，请检查Docker配置"
    exit 1
  fi
  
  echo "MySQL已成功启动"
else
  echo "MySQL已经在运行中"
fi

echo "MySQL配置信息:"
echo "主机: localhost"
echo "端口: 3306"
echo "用户名: root"
echo "密码: 123456"
echo "数据库: kline_data"

echo "\n您可以通过运行 ./run.sh 来启动数据馈送系统"