#!/bin/bash

# 设置执行权限
chmod +x ./reset_docker_db.sh

echo "正在重置Docker MySQL数据库..."

# 停止现有容器
docker stop mysql-data4trend 2>/dev/null || true
docker rm mysql-data4trend 2>/dev/null || true

# 启动MySQL容器
echo "启动MySQL容器..."
docker run --name mysql-data4trend -e MYSQL_ROOT_PASSWORD=123456 -e MYSQL_DATABASE=data4trend -p 3306:3306 -d mysql:8.0 --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci

# 等待MySQL启动
echo "等待MySQL容器启动..."
sleep 15

# 确认数据库已创建
echo "确认data4trend数据库已创建..."
docker exec mysql-data4trend mysql -uroot -p123456 -e "SHOW DATABASES;"

# 更新应用配置
echo "正在更新配置文件..."
sed -i.bak 's/host: "localhost"/host: "localhost"/' config/config.yaml

# 编译应用
echo "重新编译应用..."
go build -o dataFeeder cmd/main.go

echo "启动应用程序..."
echo "使用以下命令启动应用: ./dataFeeder"
echo "使用以下命令检查数据库: ./check_docker_db.sh" 