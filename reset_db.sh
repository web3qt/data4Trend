#!/bin/bash

# 设置执行权限
chmod +x ./reset_db.sh

echo "===== 数据库重置 & 启动工具 ====="

# 检查MySQL命令是否可用
if ! command -v mysql &> /dev/null
then
    echo "错误: mysql命令未找到，请安装MySQL客户端或确保它在你的PATH中"
    echo "提示: 你可以使用以下命令安装MySQL客户端:"
    echo "  macOS: brew install mysql-client"
    echo "  Linux: sudo apt-get install mysql-client 或 sudo yum install mysql"
    exit 1
fi

# MySQL连接参数
MYSQL_USER="root"
MYSQL_PASSWORD="123456"
MYSQL_HOST="localhost"
MYSQL_PORT="3306"
DB_NAME="data4trend"

# 清空数据库（如果存在）
echo "正在重置数据库..."
mysql -u${MYSQL_USER} -p${MYSQL_PASSWORD} -h${MYSQL_HOST} -P${MYSQL_PORT} -e "DROP DATABASE IF EXISTS ${DB_NAME}; CREATE DATABASE ${DB_NAME} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

if [ $? -ne 0 ]; then
  echo "数据库重置失败，请检查MySQL连接参数"
  exit 1
fi

echo "数据库重置成功，已创建空数据库: ${DB_NAME}"

# 编译应用
echo "正在编译应用..."
go build -o dataFeeder cmd/main.go

if [ $? -ne 0 ]; then
  echo "编译失败，请检查代码"
  exit 1
fi

echo "应用编译成功"

# 启动指令
echo ""
echo "===== 执行以下命令启动应用 ====="
echo "./dataFeeder"
echo ""
echo "程序将自动读取 config/symbols.yaml 配置文件" 