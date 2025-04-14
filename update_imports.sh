#!/bin/bash

# 设置执行权限
chmod +x ./update_imports.sh

# 查找所有Go文件
find . -name "*.go" -type f | while read -r file; do
  echo "处理文件: $file"
  
  # 替换导入路径
  sed -i '' 's|github.com/web3qt/dataFeeder|github.com/web3qt/data4Trend|g' "$file"
done

echo "已完成所有文件的导入路径更新" 