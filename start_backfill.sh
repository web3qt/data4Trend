#!/bin/bash

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🔄 Data4Trend 数据回填服务${NC}"
echo "=================================="

# 检查配置文件
CONFIG_FILE="config/config.yaml"
if [[ ! -f "$CONFIG_FILE" ]]; then
    echo -e "${RED}错误: 配置文件 $CONFIG_FILE 不存在${NC}"
    exit 1
fi

# 编译数据回填服务
echo -e "${YELLOW}�� 编译数据回填服务...${NC}"
/usr/local/go/bin/go build -o bin/backfill-validator cmd/backfill-validator/main.go
if [[ $? -ne 0 ]]; then
    echo -e "${RED}错误: 编译失败${NC}"
    exit 1
fi

# 检查ClickHouse是否运行
echo -e "${YELLOW}🔍 检查ClickHouse连接...${NC}"
if ! curl -s http://localhost:8123/ping > /dev/null; then
    echo -e "${YELLOW}⚠️  ClickHouse未运行，启动ClickHouse服务...${NC}"
    docker compose -f docker-compose.yml up -d clickhouse
    sleep 10
fi

# 解析命令行参数
DAYS=1
SYMBOL=""
VALIDATE_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -symbol)
            SYMBOL="$2"
            shift 2
            ;;
        -days)
            DAYS="$2"
            shift 2
            ;;
        -validate-only)
            VALIDATE_ONLY=true
            shift
            ;;
        -config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        -h|--help)
            echo "用法: $0 [选项]"
            echo "选项:"
            echo "  -symbol SYMBOL    指定交易对 (例如: BTCUSDT)"
            echo "  -days DAYS        回填天数 (默认: 1)"
            echo "  -validate-only    仅执行验证，不进行回填"
            echo "  -config FILE      配置文件路径 (默认: config/config.yaml)"
            echo "  -h, --help        显示此帮助信息"
            exit 0
            ;;
        *)
            echo -e "${RED}错误: 未知参数 $1${NC}"
            exit 1
            ;;
    esac
done

# 构建命令
CMD="./bin/backfill-validator -config $CONFIG_FILE"

if [[ "$VALIDATE_ONLY" == true ]]; then
    CMD="$CMD -validate-only"
    echo -e "${YELLOW}🔍 仅执行数据验证...${NC}"
else
    CMD="$CMD -days $DAYS"
    if [[ -n "$SYMBOL" ]]; then
        CMD="$CMD -symbol $SYMBOL"
        echo -e "${YELLOW}🔄 回填交易对 $SYMBOL，天数: $DAYS${NC}"
    else
        echo -e "${YELLOW}🔄 回填所有交易对，天数: $DAYS${NC}"
    fi
fi

# 创建日志目录
mkdir -p logs

# 启动数据回填服务
echo -e "${YELLOW}🚀 启动数据回填服务...${NC}"
echo -e "${BLUE}执行命令: $CMD${NC}"
echo -e "${BLUE}日志输出到 logs/backfill.log${NC}"

# 保存PID
echo $$ > .backfill.pid

# 执行回填
$CMD 2>&1 | tee logs/backfill.log

# 清理PID文件
rm -f .backfill.pid

echo ""
echo -e "${GREEN}🎉 数据回填服务执行完成！${NC}"
echo "==================================" 