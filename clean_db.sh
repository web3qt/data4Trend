#!/bin/bash

# 设置执行权限
chmod +x ./clean_db.sh

echo "===== 数据库清空工具 ====="
echo "警告: 此工具将删除所有表结构!"

# 检查是否已经编译了清理工具
if [ ! -f "./clean_db" ]; then
  echo "正在编译数据库清空工具..."
  go build -o clean_db clean_db.go
  
  if [ $? -ne 0 ]; then
    echo "编译失败，请检查错误信息"
    exit 1
  fi
  
  echo "编译成功!"
fi

# 确认用户是否真的要执行此操作
read -p "此操作将删除数据库中的所有表结构，是否继续？(y/n): " CONFIRM
if [ "$CONFIRM" != "y" ]; then
  echo "操作已取消"
  exit 0
fi

# 运行工具
echo "开始清空数据库..."
./clean_db

echo "脚本执行完成" 