#!/bin/bash
set -e

# 等待MySQL服务启动
until mysql -h localhost -u root -p"${MYSQL_ROOT_PASSWORD}" -e "SELECT 1"; do
  echo "等待MySQL启动..."
  sleep 2
done

# 创建数据库和用户
mysql -h localhost -u root -p"${MYSQL_ROOT_PASSWORD}" <<-EOSQL
    CREATE DATABASE IF NOT EXISTS ${MYSQL_DATABASE};
    CREATE USER IF NOT EXISTS '${MYSQL_USER}'@'%' IDENTIFIED BY '${MYSQL_PASSWORD}';
    GRANT ALL PRIVILEGES ON ${MYSQL_DATABASE}.* TO '${MYSQL_USER}'@'%';
    FLUSH PRIVILEGES;
EOSQL

# 创建表结构
mysql -h localhost -u root -p"${MYSQL_ROOT_PASSWORD}" ${MYSQL_DATABASE} <<-EOSQL
    CREATE TABLE IF NOT EXISTS kline (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        symbol VARCHAR(20) NOT NULL,
        interval_type VARCHAR(10) NOT NULL,
        open_time DATETIME NOT NULL,
        close_time DATETIME NOT NULL,
        open_price DECIMAL(20, 8) NOT NULL,
        high_price DECIMAL(20, 8) NOT NULL,
        low_price DECIMAL(20, 8) NOT NULL,
        close_price DECIMAL(20, 8) NOT NULL,
        volume DECIMAL(30, 8) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY idx_symbol_interval_opentime (symbol, interval_type, open_time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
EOSQL

echo "MySQL初始化完成"