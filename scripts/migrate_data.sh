#!/bin/bash

echo "开始数据迁移过程..."
echo "编译迁移工具..."

# 切换到项目根目录
cd "$(dirname "$0")/.."

# 编译并运行迁移工具
go run scripts/migrate_data.go

echo "数据迁移完成!" 