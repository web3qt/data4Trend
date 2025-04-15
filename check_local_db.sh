#!/bin/bash

# 设置执行权限
chmod +x ./check_local_db.sh

echo "正在连接到本地MySQL数据库..."

# MySQL连接参数
MYSQL_USER="root"
MYSQL_PASSWORD="123456"
MYSQL_HOST="localhost"
MYSQL_PORT="3306"
MYSQL_DB="data4trend"

# 获取数据库中所有的表（每个代币一个表）
TABLES=$(mysql -u${MYSQL_USER} -p${MYSQL_PASSWORD} -h${MYSQL_HOST} -P${MYSQL_PORT} -e "USE ${MYSQL_DB}; SHOW TABLES;" | grep -v "Tables_in")

# 统计表数量
TABLE_COUNT=$(echo "$TABLES" | grep -v "^$" | wc -l)

echo "数据库中找到 $TABLE_COUNT 个代币表"

# 打印前10个表的名称
if [ $TABLE_COUNT -gt 0 ]; then
  echo "前10个代币表:"
  echo "$TABLES" | head -10
  
  # 检查第一个表的数据量
  FIRST_TABLE=$(echo "$TABLES" | head -1)
  if [ -n "$FIRST_TABLE" ]; then
    echo "检查表 $FIRST_TABLE 的数据..."
    ROW_COUNT=$(mysql -u${MYSQL_USER} -p${MYSQL_PASSWORD} -h${MYSQL_HOST} -P${MYSQL_PORT} -e "USE ${MYSQL_DB}; SELECT COUNT(*) FROM \`$FIRST_TABLE\`;" | grep -v "COUNT")
    echo "表 $FIRST_TABLE 中有 $ROW_COUNT 行数据"
    
    # 查看表中的不同间隔类型数量
    echo "间隔类型统计:"
    mysql -u${MYSQL_USER} -p${MYSQL_PASSWORD} -h${MYSQL_HOST} -P${MYSQL_PORT} -e "USE ${MYSQL_DB}; SELECT interval_type, COUNT(*) FROM \`$FIRST_TABLE\` GROUP BY interval_type;" | grep -v "interval_type"
    
    # 查看表中最新的10条数据
    echo "最新的10条数据:"
    mysql -u${MYSQL_USER} -p${MYSQL_PASSWORD} -h${MYSQL_HOST} -P${MYSQL_PORT} -e "USE ${MYSQL_DB}; SELECT id, interval_type, open_time, close_time FROM \`$FIRST_TABLE\` ORDER BY id DESC LIMIT 10;"
  fi
fi

# 检查表数量是否太少
if [ $TABLE_COUNT -lt 10 ]; then
  echo "警告: 数据库中的代币表数量不足10个，可能存在问题"
  echo "请检查应用日志以确定问题原因"
else
  echo "数据库中有足够的代币表，系统看起来运行正常"
fi 