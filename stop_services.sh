#!/bin/bash

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🛑 Data4Trend 服务停止脚本${NC}"
echo "=================================="

# 停止数据收集服务
if [[ -f ".collector.pid" ]]; then
    COLLECTOR_PID=$(cat .collector.pid)
    if kill -0 $COLLECTOR_PID 2>/dev/null; then
        echo -e "${YELLOW}🔄 停止数据收集服务 (PID: $COLLECTOR_PID)...${NC}"
        kill $COLLECTOR_PID
        sleep 2
        
        # 强制停止如果还在运行
        if kill -0 $COLLECTOR_PID 2>/dev/null; then
            echo -e "${YELLOW}⚠️  强制停止数据收集服务...${NC}"
            kill -9 $COLLECTOR_PID
        fi
        
        echo -e "${GREEN}✅ 数据收集服务已停止${NC}"
    else
        echo -e "${YELLOW}⚠️  数据收集服务未运行${NC}"
    fi
    rm -f .collector.pid
else
    echo -e "${YELLOW}⚠️  未找到数据收集服务PID文件${NC}"
fi

# 停止数据回填服务
if [[ -f ".backfill.pid" ]]; then
    BACKFILL_PID=$(cat .backfill.pid)
    if kill -0 $BACKFILL_PID 2>/dev/null; then
        echo -e "${YELLOW}🔄 停止数据回填服务 (PID: $BACKFILL_PID)...${NC}"
        kill $BACKFILL_PID
        sleep 2
        
        # 强制停止如果还在运行
        if kill -0 $BACKFILL_PID 2>/dev/null; then
            echo -e "${YELLOW}⚠️  强制停止数据回填服务...${NC}"
            kill -9 $BACKFILL_PID
        fi
        
        echo -e "${GREEN}✅ 数据回填服务已停止${NC}"
    else
        echo -e "${YELLOW}⚠️  数据回填服务未运行${NC}"
    fi
    rm -f .backfill.pid
else
    echo -e "${YELLOW}⚠️  未找到数据回填服务PID文件${NC}"
fi

# 停止Kafka服务
echo -e "${YELLOW}🐳 停止Kafka服务...${NC}"
docker compose -f docker-compose.yml down

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Kafka服务已停止${NC}"
else
    echo -e "${YELLOW}⚠️  Kafka服务停止时出现警告${NC}"
fi

# 清理临时文件
echo -e "${YELLOW}🧹 清理临时文件...${NC}"
rm -f .collector.pid .backfill.pid

echo ""
echo -e "${GREEN}🎉 项目服务已停止！${NC}"
echo -e "${BLUE}💡 注意: ClickHouse服务未停止，其他项目可能仍在使用${NC}"
echo -e "${BLUE}   如需停止ClickHouse: ./manage_clickhouse.sh stop${NC}"
echo "==================================" 