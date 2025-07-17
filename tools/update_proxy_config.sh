#!/bin/bash

echo "=== 更新代理和超时配置 ==="

# 检查参数
if [ $# -eq 0 ]; then
    echo "使用方法:"
    echo "  $0 <proxy_url> [timeout]"
    echo ""
    echo "示例:"
    echo "  $0 http://127.0.0.1:7890 120        # 设置代理和120秒超时"
    echo "  $0 none 60                          # 清除代理，设置60秒超时"
    echo "  $0 http://127.0.0.1:7890            # 只设置代理，超时使用默认值"
    echo ""
    echo "常见代理端口:"
    echo "  - ClashX: http://127.0.0.1:7890"
    echo "  - Clash for Windows: http://127.0.0.1:7890"
    echo "  - V2rayU: http://127.0.0.1:1087"
    echo "  - Shadowsocks: http://127.0.0.1:1080"
    exit 1
fi

PROXY_URL=$1
TIMEOUT=${2:-120}  # 默认120秒超时

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

CONFIG_FILE="$PROJECT_ROOT/config/config.yaml"
TREND_CONFIG_FILE="$PROJECT_ROOT/config/trend_scanner.yaml"

echo "项目根目录: $PROJECT_ROOT"
echo "配置文件: $CONFIG_FILE"
echo "趋势扫描配置: $TREND_CONFIG_FILE"

# 备份配置文件
backup_file() {
    local file=$1
    if [ -f "$file" ]; then
        cp "$file" "$file.backup.$(date +%Y%m%d_%H%M%S)"
        echo "已备份: $file"
    fi
}

# 更新配置文件
update_config() {
    local file=$1
    
    if [ ! -f "$file" ]; then
        echo "警告: 配置文件不存在: $file"
        return
    fi
    
    backup_file "$file"
    
    # 更新超时配置
    if grep -q "timeout:" "$file"; then
        sed -i.tmp "s/timeout:.*/timeout: $TIMEOUT/" "$file"
        echo "已更新超时: $TIMEOUT 秒"
    else
        # 如果没有timeout配置，添加到http部分
        if grep -q "http:" "$file"; then
            sed -i.tmp "/http:/a\\
  timeout: $TIMEOUT" "$file"
        else
            echo "http:" >> "$file"
            echo "  timeout: $TIMEOUT" >> "$file"
        fi
        echo "已添加超时配置: $TIMEOUT 秒"
    fi
    
    # 更新代理配置
    if [ "$PROXY_URL" = "none" ] || [ "$PROXY_URL" = "null" ] || [ "$PROXY_URL" = "" ]; then
        # 清除代理配置
        if grep -q "proxy:" "$file"; then
            sed -i.tmp "s/proxy:.*/proxy: \"\"/" "$file"
            echo "已清除代理配置"
        fi
    else
        # 设置代理
        if grep -q "proxy:" "$file"; then
            sed -i.tmp "s|proxy:.*|proxy: \"$PROXY_URL\"|" "$file"
            echo "已更新代理: $PROXY_URL"
        else
            # 如果没有proxy配置，添加到http部分
            if grep -q "http:" "$file"; then
                sed -i.tmp "/timeout:/a\\
  proxy: \"$PROXY_URL\"" "$file"
            else
                echo "http:" >> "$file"
                echo "  proxy: \"$PROXY_URL\"" >> "$file"
            fi
            echo "已添加代理配置: $PROXY_URL"
        fi
    fi
    
    # 清理临时文件
    rm -f "$file.tmp"
}

echo ""
echo "正在更新配置文件..."

# 更新主配置文件
update_config "$CONFIG_FILE"

# 更新趋势扫描配置文件
update_config "$TREND_CONFIG_FILE"

echo ""
echo "配置更新完成！"
echo ""
echo "建议的测试步骤："
echo "1. 运行连接测试: cd tools/test_binance_connection && go run main.go"
echo "2. 如果连接成功，运行主程序: cd $PROJECT_ROOT && go run cmd/main.go"
echo "3. 如果仍有问题，尝试不同的代理端口或联系网络管理员" 