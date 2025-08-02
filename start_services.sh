#!/bin/bash

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Data4Trend 服务启动脚本${NC}"
echo "=================================="

# 检查Docker是否运行
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}错误: Docker未运行，请先启动Docker${NC}"
    exit 1
fi

# 检查配置文件
CONFIG_FILE="config/config.yaml"
if [[ ! -f "$CONFIG_FILE" ]]; then
    echo -e "${RED}错误: 配置文件 $CONFIG_FILE 不存在${NC}"
    exit 1
fi

# 检查ClickHouse是否运行
echo -e "${YELLOW}🔍 检查ClickHouse连接...${NC}"
if ! curl -s http://localhost:8123/ping > /dev/null; then
    echo -e "${YELLOW}⚠️  ClickHouse未运行，请先启动ClickHouse:${NC}"
    echo -e "${BLUE}   ./manage_clickhouse.sh start${NC}"
    exit 1
fi
echo -e "${GREEN}✅ ClickHouse连接正常${NC}"

# 编译服务
echo -e "${YELLOW}🔨 编译服务...${NC}"
/usr/local/go/bin/go build -o bin/data4trend-collector cmd/collector/main.go
if [[ $? -ne 0 ]]; then
    echo -e "${RED}错误: 编译collector失败${NC}"
    exit 1
fi

/usr/local/go/bin/go build -o bin/backfill-validator cmd/backfill-validator/main.go
if [[ $? -ne 0 ]]; then
    echo -e "${RED}错误: 编译backfill-validator失败${NC}"
    exit 1
fi

echo -e "${GREEN}✅ 编译成功${NC}"

# 启动Kafka服务
echo -e "${YELLOW}🐳 启动Kafka服务...${NC}"
docker compose -f docker-compose.yml up -d kafka

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ 启动Kafka服务失败${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Kafka服务启动成功${NC}"

# 等待Kafka服务完全启动
echo -e "${YELLOW}⏳ 等待Kafka服务启动...${NC}"
sleep 15

# 检查Kafka连接
echo -e "${YELLOW}🔍 检查Kafka连接...${NC}"
for i in {1..10}; do
    if docker exec data4trend-kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Kafka连接成功${NC}"
        break
    fi
    echo "等待Kafka启动... ($i/10)"
    sleep 3
done

# 创建Kafka主题
echo -e "${YELLOW}📝 创建Kafka主题...${NC}"
docker exec data4trend-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic binance_klines --partitions 3 --replication-factor 1 --if-not-exists

# 创建日志目录
mkdir -p logs

# 启动数据收集服务
echo -e "${YELLOW}🌐 启动WebSocket数据收集服务...${NC}"
echo -e "${BLUE}服务将在后台运行，日志输出到 logs/collector.log${NC}"

nohup ./bin/data4trend-collector -config $CONFIG_FILE > logs/collector.log 2>&1 &
COLLECTOR_PID=$!

# 保存PID
echo $COLLECTOR_PID > .collector.pid

# 等待服务启动
sleep 5

# 检查服务是否启动成功
if kill -0 $COLLECTOR_PID 2>/dev/null; then
    echo -e "${GREEN}✅ 数据收集服务启动成功 (PID: $COLLECTOR_PID)${NC}"
    echo -e "${BLUE}📊 服务状态: 正在收集Binance WebSocket数据${NC}"
    echo -e "${BLUE}📈 健康检查: curl http://localhost:8080/health${NC}"
    echo -e "${BLUE}📝 日志文件: logs/collector.log${NC}"
else
    echo -e "${RED}❌ 数据收集服务启动失败${NC}"
    echo -e "${YELLOW}查看日志: tail -f logs/collector.log${NC}"
    exit 1
fi

# 启动数据回填服务
echo -e "${YELLOW}🔄 启动数据回填服务...${NC}"
echo -e "${BLUE}服务将在后台运行，日志输出到 logs/backfill.log${NC}"

nohup ./bin/backfill-validator -config $CONFIG_FILE > logs/backfill.log 2>&1 &
BACKFILL_PID=$!

# 保存PID
echo $BACKFILL_PID > .backfill.pid

# 等待服务启动
sleep 5

# 检查服务是否启动成功
if kill -0 $BACKFILL_PID 2>/dev/null; then
    echo -e "${GREEN}✅ 数据回填服务启动成功 (PID: $BACKFILL_PID)${NC}"
    echo -e "${BLUE}📊 服务状态: 正在执行数据验证和回填${NC}"
    echo -e "${BLUE}📝 日志文件: logs/backfill.log${NC}"
else
    echo -e "${RED}❌ 数据回填服务启动失败${NC}"
    echo -e "${YELLOW}查看日志: tail -f logs/backfill.log${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}🎉 所有服务启动完成！${NC}"
echo "=================================="
echo -e "${BLUE}📋 服务状态:${NC}"
echo -e "${BLUE}   - WebSocket数据收集: 运行中 (PID: $COLLECTOR_PID)${NC}"
echo -e "${BLUE}   - 数据回填验证: 运行中 (PID: $BACKFILL_PID)${NC}"
echo -e "${BLUE}   - Kafka: 运行中${NC}"
echo -e "${BLUE}   - ClickHouse: 运行中${NC}"
echo ""
echo -e "${BLUE}📈 健康检查:${NC}"
echo -e "${BLUE}   - 数据收集: curl http://localhost:8080/health${NC}"
echo ""
echo -e "${BLUE}📝 日志文件:${NC}"
echo -e "${BLUE}   - 数据收集: logs/collector.log${NC}"
echo -e "${BLUE}   - 数据回填: logs/backfill.log${NC}"
echo ""
echo -e "${BLUE}🛑 停止服务: ./stop_services.sh${NC}" 