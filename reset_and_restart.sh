#!/bin/bash

# 设置执行权限
chmod +x ./reset_and_restart.sh

echo "正在停止所有服务..."
docker-compose down

echo "删除数据库数据卷..."
docker volume rm data4trend_mysql_data || true

echo "启动MySQL服务..."
docker-compose up -d mysql

echo "等待MySQL服务启动..."
sleep 10

echo "验证MySQL服务是否已启动..."
if ! docker-compose ps | grep -q "mysql.*Up"; then
  echo "错误: MySQL服务未能成功启动"
  exit 1
fi

echo "创建data4trend数据库..."
docker-compose exec mysql mysql -uroot -p123456 -e "DROP DATABASE IF EXISTS data4trend; CREATE DATABASE data4trend CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

echo "启动应用服务..."
docker-compose up -d app

echo "应用服务启动完成！"
echo "查看应用日志:"
echo "docker-compose logs -f app" 