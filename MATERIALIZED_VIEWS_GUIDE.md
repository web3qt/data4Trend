# Data4Trend 物化视图架构指南

## 概述

本项目已重构为基于ClickHouse最佳实践的物化视图架构。新架构采用单一事实表 + 物化视图自动聚合的设计模式，相比之前的分表架构具有以下优势：

### 架构优势

1. **存储效率**: 只存储1分钟原始数据，其他时间粒度通过物化视图自动聚合
2. **查询性能**: 预聚合数据提供更快的查询速度
3. **数据一致性**: 单一数据源确保所有时间粒度的数据一致性
4. **维护简单**: 减少了表的数量，简化了数据管理
5. **实时更新**: 物化视图自动、实时地更新聚合数据

## 架构设计

### 核心表结构

```sql
-- 原始数据表（1分钟K线）
kline_raw (
    id UInt64,
    symbol String,
    open_time DateTime,
    close_time DateTime,
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    volume Float64
)

-- 聚合表（由物化视图自动填充）
kline_5m, kline_15m, kline_1h, kline_4h, kline_1d
```

### 物化视图

每个时间粒度都有对应的物化视图，自动将1分钟数据聚合到相应的表中：

- `mv_kline_1m_to_5m`: 1分钟 → 5分钟
- `mv_kline_1m_to_15m`: 1分钟 → 15分钟
- `mv_kline_1m_to_1h`: 1分钟 → 1小时
- `mv_kline_1m_to_4h`: 1分钟 → 4小时
- `mv_kline_1m_to_1d`: 1分钟 → 1天

## 使用指南

### 1. 初始化数据库

首次使用需要初始化数据库表结构：

```bash
# 方法1: 使用启动脚本初始化
INIT_DB=true ./scripts/start-materialized.sh

# 方法2: 手动初始化
./bin/data-collector-materialized -init-db

# 方法3: 直接执行SQL脚本
clickhouse-client --host=localhost --port=9000 --multiquery < scripts/clickhouse-init-materialized-views.sql
```

### 2. 启动数据收集器

```bash
# 使用默认配置启动
./scripts/start-materialized.sh

# 自定义数据库连接
DB_HOST=your-host DB_PORT=9000 DB_NAME=your-db ./scripts/start-materialized.sh

# 手动启动
./bin/data-collector-materialized -config=config/symbols.yaml
```

### 3. 启动趋势扫描器

```bash
# 构建趋势扫描器
go build -o bin/trendscanner ./cmd/trendscanner

# 启动扫描器
./bin/trendscanner -config=config/trend_scanner.yaml
```

## 配置说明

### 环境变量

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| `DB_HOST` | localhost | ClickHouse主机地址 |
| `DB_PORT` | 9000 | ClickHouse端口 |
| `DB_USER` | default | ClickHouse用户名 |
| `DB_PASS` | "" | ClickHouse密码 |
| `DB_NAME` | data4trend | 数据库名称 |
| `LOG_LEVEL` | info | 日志级别 |
| `CONFIG_PATH` | config/symbols.yaml | 配置文件路径 |
| `INIT_DB` | false | 是否初始化数据库 |
| `BUILD` | false | 是否重新构建程序 |

### 命令行参数

```bash
./bin/data-collector-materialized \
    -config=config/symbols.yaml \     # 配置文件路径
    -db-host=localhost \               # ClickHouse主机
    -db-port=9000 \                    # ClickHouse端口
    -db-user=default \                 # ClickHouse用户
    -db-pass="" \                       # ClickHouse密码
    -db-name=data4trend \              # 数据库名称
    -init-db \                         # 初始化数据库
    -log-level=info                    # 日志级别
```

## 数据查询

### 查询不同时间粒度的数据

```sql
-- 查询1分钟数据（原始数据）
SELECT * FROM kline_raw WHERE symbol = 'BTCUSDT' ORDER BY open_time DESC LIMIT 100;

-- 查询5分钟数据（聚合数据）
SELECT * FROM kline_5m WHERE symbol = 'BTCUSDT' ORDER BY open_time DESC LIMIT 100;

-- 查询1小时数据（聚合数据）
SELECT * FROM kline_1h WHERE symbol = 'BTCUSDT' ORDER BY open_time DESC LIMIT 100;

-- 使用统一视图查询所有时间粒度
SELECT * FROM v_kline_unified WHERE symbol = 'BTCUSDT' AND interval_type = '1h';
```

### 监控查询

```sql
-- 查看表统计信息
SELECT * FROM v_table_stats;

-- 查看可用交易对
SELECT symbol, count() as records, min(open_time) as first, max(open_time) as last 
FROM kline_raw GROUP BY symbol;

-- 检查物化视图状态
SELECT 
    database,
    table,
    engine,
    total_rows,
    total_bytes
FROM system.tables 
WHERE database = 'data4trend' AND engine LIKE '%MaterializedView%';
```

## 性能优化

### 1. 索引优化

- 所有表都使用 `bloom_symbol` 索引优化符号查询
- 按 `open_time` 分区提高时间范围查询性能
- 使用 `(symbol, open_time)` 排序键优化常见查询模式

### 2. 压缩优化

- 使用 `LZ4` 压缩算法平衡压缩率和性能
- 数值字段使用 `Float64` 类型确保精度

### 3. 查询优化

- 优先查询聚合表而非实时计算
- 使用时间分区过滤减少扫描数据量
- 合理使用 `LIMIT` 控制返回数据量

## 迁移指南

### 从旧架构迁移

如果你之前使用的是分表架构，可以按以下步骤迁移：

1. **备份现有数据**
```sql
-- 导出现有数据
SELECT * FROM kline_1m INTO OUTFILE 'backup_1m.csv' FORMAT CSV;
```

2. **初始化新架构**
```bash
INIT_DB=true ./scripts/start-materialized.sh
```

3. **导入历史数据**
```sql
-- 导入1分钟数据到新的原始表
INSERT INTO kline_raw SELECT 
    row_number() OVER (ORDER BY open_time) as id,
    symbol, open_time, close_time, 
    open_price, high_price, low_price, close_price, volume
FROM kline_1m;
```

4. **验证数据**
```sql
-- 检查数据完整性
SELECT 
    'raw' as source, count() as cnt FROM kline_raw
UNION ALL
SELECT 
    '5m' as source, count() as cnt FROM kline_5m;
```

## 故障排除

### 常见问题

1. **物化视图不更新**
   - 检查物化视图状态：`SHOW CREATE TABLE mv_kline_1m_to_5m`
   - 重建物化视图：`DROP TABLE mv_kline_1m_to_5m; CREATE MATERIALIZED VIEW ...`

2. **数据不一致**
   - 检查原始数据：`SELECT count() FROM kline_raw`
   - 手动触发聚合：`INSERT INTO kline_5m SELECT ... FROM kline_raw`

3. **查询性能慢**
   - 检查是否使用了正确的表（聚合表 vs 原始表）
   - 确保查询条件包含 `symbol` 和时间范围
   - 使用 `EXPLAIN` 分析查询计划

### 日志分析

```bash
# 查看数据收集日志
tail -f logs/data-collector.log

# 查看ClickHouse日志
tail -f /var/log/clickhouse-server/clickhouse-server.log
```

## 最佳实践

1. **数据写入**：只向 `kline_raw` 表写入1分钟数据
2. **数据查询**：根据需要的时间粒度查询对应的聚合表
3. **监控**：定期检查物化视图状态和数据完整性
4. **备份**：定期备份原始数据表
5. **清理**：根据需要清理过期的历史数据

## 支持的时间粒度

- `1m`: 1分钟（原始数据）
- `5m`: 5分钟（聚合数据）
- `15m`: 15分钟（聚合数据）
- `1h`: 1小时（聚合数据）
- `4h`: 4小时（聚合数据）
- `1d`: 1天（聚合数据）

所有聚合数据都会在1分钟数据写入时自动、实时地更新。