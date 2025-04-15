#!/bin/bash

# 设置执行权限
chmod +x ./reset_local_db.sh

echo "正在重置本地数据库..."

# MySQL连接参数
MYSQL_USER="root"
MYSQL_PASSWORD="123456"
MYSQL_HOST="localhost"
MYSQL_PORT="3306"

# 重置数据库
echo "删除并重新创建data4trend数据库..."
mysql -u${MYSQL_USER} -p${MYSQL_PASSWORD} -h${MYSQL_HOST} -P${MYSQL_PORT} -e "DROP DATABASE IF EXISTS data4trend; CREATE DATABASE data4trend CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

echo "数据库重置完成！"

# 编译应用
echo "重新编译应用..."
go build -o dataFeeder cmd/main.go

echo "启动应用程序..."
echo "使用以下命令启动应用: ./dataFeeder"
echo "使用以下命令检查数据库: ./check_local_db.sh" 