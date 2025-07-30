# Data4Trend - 币安WebSocket数据收集器

基于Go语言开发的高性能币安交易所实时数据收集器，支持1分钟K线数据收集，存储到ClickHouse数据库，并提供REST API接口。

## ✨ 项目特性

- 🚀 **高性能**: Go语言开发，支持高并发WebSocket连接
- 📊 **实时数据**: 实时收集币安交易所K线数据
- 🗄️ **ClickHouse存储**: 高效的列式数据库存储
- 🌐 **REST API**: 提供完整的数据查询API
- 🔄 **自动重连**: 智能重连机制，确保数据连续性
- 🛡️ **代理支持**: 支持HTTP代理，避免网络限制
- 📈 **监控系统**: 内置监控和健康检查
- 🔧 **易于部署**: 单一二进制文件，配置简单

## 🚀 快速开始

### 启动程序
```bash
# 使用简化启动脚本（推荐）
./start_go_simple.sh

# 或手动启动
export HTTP_PROXY=http://127.0.0.1:7890
export HTTPS_PROXY=http://127.0.0.1:7890
./bin/data4trend-collector --config=config/config_go_simple.yaml --log-level=info
```

### 编译程序
```bash
# 编译二进制文件
go build -o bin/data4trend-collector cmd/collector/main.go
```

## 🌐 API接口

程序启动后，API服务器运行在 `http://localhost:8080`

### 健康检查
```bash
curl http://localhost:8080/health
```

### 获取统计信息
```bash
# 数据库统计
curl http://localhost:8080/api/v1/stats

# WebSocket连接统计
curl http://localhost:8080/api/v1/websocket/stats
```

### 获取K线数据
```bash
# 获取BTCUSDT最新5条数据
curl "http://localhost:8080/api/v1/klines/BTCUSDT?limit=5"

# 获取指定时间范围的数据
curl "http://localhost:8080/api/v1/klines/ETHUSDT?limit=10&start_time=1640995200000&end_time=1640998800000"
```

## 📊 数据库连接

### ClickHouse连接信息
- **主机**: localhost
- **HTTP端口**: 8123
- **用户名**: default
- **密码**: 123456
- **数据库**: data4trend

### 直接查询数据库
```bash
# 测试连接
curl -u default:123456 "http://localhost:8123" --data-binary "SELECT 1"

# 查看表
curl -u default:123456 "http://localhost:8123" --data-binary "SHOW TABLES FROM data4trend"

# 查看数据量
curl -u default:123456 "http://localhost:8123" --data-binary "SELECT count(*) FROM data4trend.klines_1m"
```

## 📁 项目结构

```
data4Trend/
├── cmd/collector/          # 主程序入口
├── pkg/                    # 核心包
│   ├── api/               # REST API服务器
│   ├── config/            # 配置管理
│   ├── monitoring/        # 监控系统
│   ├── storage/           # ClickHouse存储
│   └── websocket/         # WebSocket客户端
├── internal/              # 内部包
│   ├── types/             # 数据类型定义
│   └── utils/             # 工具函数
├── config/                # 配置文件
│   ├── config_go.yaml    # 完整配置
│   └── config_go_simple.yaml # 简化配置
├── scripts/               # 数据库脚本
└── docs/                  # 文档
```

## ⚙️ 配置说明

### 主要配置项

- **database**: ClickHouse数据库连接配置
- **websocket**: WebSocket连接配置
- **api**: REST API服务器配置
- **proxy**: HTTP代理配置
- **symbols**: 监控的交易对列表
- **interval**: 数据收集间隔

### 配置文件

- `config_go_simple.yaml`: 简化配置，监控10个主要交易对
- `config_go.yaml`: 完整配置，可自定义更多参数

## 📈 常用查询

```sql
-- 查看数据量
SELECT count() as total_records FROM data4trend.klines_1m;

-- 查看最新数据
SELECT * FROM data4trend.klines_1m ORDER BY open_time DESC LIMIT 10;

-- 查看特定交易对数据
SELECT * FROM data4trend.klines_1m WHERE symbol = 'BTCUSDT' ORDER BY open_time DESC LIMIT 10;

-- 查看数据统计
SELECT 
    symbol,
    count() as records,
    min(open_time) as first_time,
    max(open_time) as last_time
FROM data4trend.klines_1m 
GROUP BY symbol;
```

## 🔧 代理设置

如果需要使用代理访问币安API，请设置环境变量：

```bash
# 设置HTTP代理
export HTTP_PROXY=http://127.0.0.1:7890
export HTTPS_PROXY=http://127.0.0.1:7890

# 然后启动程序
./start_go_simple.sh
```

## 🎯 运行状态检查

### 程序正常运行的标志
- ✅ WebSocket连接成功日志
- ✅ API服务器启动在8080端口
- ✅ ClickHouse数据库连接成功
- ✅ 数据开始写入数据库

### 健康检查命令
```bash
# 检查API服务器
curl http://localhost:8080/health

# 检查数据库连接
curl -u default:123456 "http://localhost:8123" --data-binary "SELECT 1"

# 检查数据写入
curl http://localhost:8080/api/v1/stats
```

## 🛠️ 故障排除

### 常见问题

1. **WebSocket连接失败**
   - 检查网络连接
   - 确认代理设置正确
   - 查看是否被币安限制

2. **数据库连接失败**
   - 确认ClickHouse服务运行
   - 检查连接配置
   - 验证用户名密码

3. **API服务器无响应**
   - 检查8080端口是否被占用
   - 查看程序启动日志

### 日志查看
```bash
# 查看程序运行日志
./bin/data4trend-collector --config=config/config_go_simple.yaml --log-level=debug
```

## 📋 技术栈

- **语言**: Go 1.21+
- **数据库**: ClickHouse
- **WebSocket**: gorilla/websocket
- **HTTP路由**: gin-gonic/gin
- **配置**: YAML
- **日志**: logrus

## 📄 许可证

本项目采用 MIT 许可证。

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

---

**Data4Trend** - 专业的加密货币数据收集解决方案