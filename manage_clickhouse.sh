#!/bin/bash

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 检查Docker是否运行
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        echo -e "${RED}错误: Docker未运行，请先启动Docker${NC}"
        exit 1
    fi
}

# 创建共享网络
create_network() {
    if ! docker network ls | grep -q "shared-network"; then
        echo -e "${YELLOW}🌐 创建共享网络...${NC}"
        docker network create shared-network
        echo -e "${GREEN}✅ 共享网络创建成功${NC}"
    else
        echo -e "${GREEN}✅ 共享网络已存在${NC}"
    fi
}

# 启动ClickHouse
start_clickhouse() {
    echo -e "${BLUE}🚀 启动共享ClickHouse服务${NC}"
    echo "=================================="
    
    check_docker
    create_network
    
    echo -e "${YELLOW}🐳 启动ClickHouse容器...${NC}"
    docker compose -f docker-compose-clickhouse.yml up -d
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ ClickHouse容器启动成功${NC}"
        echo -e "${BLUE}📊 服务地址: localhost:8123${NC}"
        echo -e "${BLUE}🔗 连接信息:${NC}"
        echo -e "${BLUE}   - 数据库: data4trend${NC}"
        echo -e "${BLUE}   - 用户名: default${NC}"
        echo -e "${BLUE}   - 密码: 123456${NC}"
    else
        echo -e "${RED}❌ ClickHouse容器启动失败${NC}"
        exit 1
    fi
}

# 停止ClickHouse
stop_clickhouse() {
    echo -e "${BLUE}🛑 停止共享ClickHouse服务${NC}"
    echo "=================================="
    
    check_docker
    
    echo -e "${YELLOW}🐳 停止ClickHouse容器...${NC}"
    docker compose -f docker-compose-clickhouse.yml down
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ ClickHouse容器已停止${NC}"
    else
        echo -e "${YELLOW}⚠️  ClickHouse容器停止时出现警告${NC}"
    fi
}

# 检查ClickHouse状态
status_clickhouse() {
    echo -e "${BLUE}📊 ClickHouse服务状态${NC}"
    echo "=================================="
    
    check_docker
    
    # 检查容器状态
    if docker ps | grep -q "shared-clickhouse"; then
        echo -e "${GREEN}✅ ClickHouse容器正在运行${NC}"
        
        # 检查连接
        if curl -s http://localhost:8123/ping > /dev/null; then
            echo -e "${GREEN}✅ ClickHouse服务可访问${NC}"
        else
            echo -e "${YELLOW}⚠️  ClickHouse服务不可访问${NC}"
        fi
        
        # 显示容器信息
        echo -e "${BLUE}📋 容器信息:${NC}"
        docker ps --filter "name=shared-clickhouse" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    else
        echo -e "${RED}❌ ClickHouse容器未运行${NC}"
    fi
}

# 重启ClickHouse
restart_clickhouse() {
    echo -e "${BLUE}🔄 重启共享ClickHouse服务${NC}"
    echo "=================================="
    
    stop_clickhouse
    sleep 2
    start_clickhouse
}

# 显示帮助信息
show_help() {
    echo "用法: $0 [命令]"
    echo ""
    echo "命令:"
    echo "  start     启动ClickHouse服务"
    echo "  stop      停止ClickHouse服务"
    echo "  restart   重启ClickHouse服务"
    echo "  status    检查ClickHouse服务状态"
    echo "  help      显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0 start    # 启动ClickHouse"
    echo "  $0 status   # 检查状态"
    echo "  $0 stop     # 停止服务"
}

# 主逻辑
case "${1:-help}" in
    start)
        start_clickhouse
        ;;
    stop)
        stop_clickhouse
        ;;
    restart)
        restart_clickhouse
        ;;
    status)
        status_clickhouse
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        echo -e "${RED}错误: 未知命令 '$1'${NC}"
        echo ""
        show_help
        exit 1
        ;;
esac 