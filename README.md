# 币安WebSocket数据收集器

实时收集币安交易所的1分钟K线数据，存储到ClickHouse数据库。

## 🚀 快速开始

### 启动程序
```bash
./scripts/start.sh
```

### 停止程序
```bash
./scripts/stop.sh
```

## 📊 数据库连接

### ClickHouse连接信息
- **主机**: localhost
- **HTTP端口**: 8123
- **原生端口**: 9000
- **用户名**: default
- **密码**: 123456
- **数据库**: data4trend

### 连接方式

#### Docker命令行连接
```bash
docker exec -it clickhouse clickhouse-client -u default --password 123456
```

#### HTTP接口连接
```bash
# 测试连接
curl -u default:123456 "http://localhost:8123/ping"

# 查看数据
curl -u default:123456 -X POST "http://localhost:8123" \
  --data-binary "SELECT count() FROM data4trend.klines_1m"
```

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

如果需要使用代理，请设置环境变量：

```bash
# 设置HTTP代理
export HTTP_PROXY=http://127.0.0.1:7890
export HTTPS_PROXY=http://127.0.0.1:7890

# 然后启动程序
./scripts/start.sh
```

## 📋 项目特性

- ✅ 实时数据收集
- ✅ 自动数据清理（7天）
- ✅ 代理支持
- ✅ 健康检查
- ✅ 错误重连
- ✅ 日志记录
- ✅ REST API

## 🎯 成功标志

程序正常运行时会看到：
- 连接成功的日志信息
- 数据开始收集
- 没有超时错误
- 数据库中有数据记录

## 📞 获取帮助

如果遇到问题：
1. 查看日志文件：`logs/collector_conservative.log`
2. 检查数据库连接：`curl -u default:123456 "http://localhost:8123/ping"`
3. 重启程序：`./scripts/stop.sh && ./scripts/start.sh`