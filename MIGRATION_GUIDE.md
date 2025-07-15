# 数据存储架构迁移指南

## 从统一表迁移到按时间级别分表

本指南帮助您将现有的统一K线数据表迁移到按时间级别分表的架构，这是时序数据的主流做法，可以显著提升查询性能。

## ⚠️ 重要修复

**已修复的问题：**
- ✅ **类型转换错误**：修复了 `float64` 到 `Decimal64(8)` 的转换问题
- ✅ **脏数据清理**：提供数据库完全重置功能  
- ✅ **字段映射**：修复了字段名不一致导致的查询错误
- ✅ **表结构优化**：使用 `Float64` 类型替代 `Decimal64(8)` 确保兼容性

## 📋 迁移概述

### 原有架构（统一表）
- 单表：`kline`
- 字段：包含 `interval_type` 字段区分时间级别
- 查询：需要通过 `WHERE interval_type = '1h'` 过滤

### 新架构（按时间级别分表）
- 分表：`kline_1m`, `kline_5m`, `kline_15m`, `kline_1h`, `kline_4h`, `kline_1d`
- 字段：移除 `interval_type` 字段，表名即表示时间级别
- 查询：直接查询对应表，无需过滤

## 🚀 快速开始

### 方法一：一键重置启动（推荐，修复脏数据）

```bash
# 1. 运行一键重置脚本（解决类型转换和脏数据问题）
./reset_and_start.sh
```

脚本会自动：
- 检查并启动ClickHouse
- **完全重置数据库**（清理脏数据）
- 创建新的表结构（使用 Float64 类型）
- 启动使用新架构的服务

### 方法二：渐进式迁移

```bash
# 1. 运行渐进式迁移脚本（保留现有数据）
./run_with_interval_tables.sh
```

脚本会自动：
- 检查并启动ClickHouse  
- 询问是否重置数据库
- 创建新的表结构
- 检测现有数据并提示迁移

### 方法三：手动执行

```bash
# 1. 确保ClickHouse运行
docker-compose up -d clickhouse

# 2. 创建新表结构
docker exec data4trend-clickhouse-1 clickhouse-client --host 127.0.0.1 --port 9000 --multiquery < scripts/clickhouse-init-interval-tables.sql

# 3. 迁移数据
docker exec data4trend-clickhouse-1 clickhouse-client --host 127.0.0.1 --port 9000 --multiquery < scripts/migrate_to_interval_tables.sql

# 4. 启动新架构服务
export USE_INTERVAL_TABLES=true
./main -config config/symbols.yaml -port 8080
```

## 📊 性能优势

### 查询性能对比

**原有查询（统一表）：**
```sql
SELECT * FROM kline 
WHERE symbol = 'BTCUSDT' 
AND interval_type = '1h' 
AND open_time >= '2024-01-01'
ORDER BY open_time DESC 
LIMIT 100;
```

**新架构查询（分表）：**
```sql
SELECT * FROM kline_1h 
WHERE symbol = 'BTCUSDT' 
AND open_time >= '2024-01-01'
ORDER BY open_time DESC 
LIMIT 100;
```

### 预期性能提升
- **查询速度**：提升 30-50%
- **索引效率**：显著提升
- **并行处理**：支持多表并行查询
- **存储优化**：更好的压缩比

## 🔧 配置说明

### 环境变量

| 变量名 | 值 | 说明 |
|--------|-----|------|
| `USE_INTERVAL_TABLES` | `true` | 启用按时间级别分表 |
| `USE_INTERVAL_TABLES` | `false` 或不设置 | 使用统一表（默认） |

### 支持的时间级别

| 时间级别 | 表名 | 说明 |
|----------|------|------|
| 1分钟 | `kline_1m` | 最小时间粒度 |
| 5分钟 | `kline_5m` | 短期交易分析 |
| 15分钟 | `kline_15m` | 主要分析时间级别 |
| 1小时 | `kline_1h` | 主要分析时间级别 |
| 4小时 | `kline_4h` | 中期趋势分析 |
| 1天 | `kline_1d` | 长期趋势分析 |

## 🔍 数据验证

### 迁移前检查
```sql
-- 查看原表数据分布
SELECT 
    interval_type,
    COUNT(*) as record_count,
    COUNT(DISTINCT symbol) as symbol_count,
    MIN(open_time) as earliest_data,
    MAX(open_time) as latest_data
FROM kline 
GROUP BY interval_type 
ORDER BY record_count DESC;
```

### 迁移后验证
```sql
-- 查看新表统计信息
SELECT * FROM v_table_stats;

-- 对比记录数量
SELECT 
    'Original' as source, 
    interval_type, 
    COUNT(*) as count 
FROM kline 
GROUP BY interval_type
UNION ALL
SELECT 'Migrated', '1h', COUNT(*) FROM kline_1h
UNION ALL
SELECT 'Migrated', '1d', COUNT(*) FROM kline_1d
ORDER BY interval_type, source;
```

## ⚠️ 注意事项

### 迁移前准备
1. **备份数据**：迁移脚本会自动创建 `kline_backup` 表
2. **停止写入**：确保没有数据写入过程在运行
3. **空间检查**：确保有足够的磁盘空间（约为原数据的1.2倍）

### 迁移期间
- 迁移过程中原表仍可查询
- 大数据量迁移可能需要较长时间
- 可以分批次迁移不同时间级别的数据

### 迁移后
- 验证数据完整性后再删除原表
- 更新应用程序的查询逻辑
- 监控新架构的性能表现

## 🔄 回滚计划

如果迁移后出现问题，可以快速回滚：

```sql
-- 方法1：重命名表（快速）
RENAME TABLE kline TO kline_interval_backup;
RENAME TABLE kline_backup TO kline;

-- 方法2：从备份恢复（安全）
DROP TABLE IF EXISTS kline;
CREATE TABLE kline AS kline_backup;
```

## 📈 监控和维护

### 性能监控
```sql
-- 查看表大小和行数
SELECT 
    table,
    formatReadableSize(sum(bytes_on_disk)) as size,
    sum(rows) as rows
FROM system.parts 
WHERE database = 'data4trend' 
AND table LIKE 'kline_%'
GROUP BY table
ORDER BY sum(bytes_on_disk) DESC;
```

### 定期维护
```sql
-- 优化表（建议每周执行）
OPTIMIZE TABLE kline_1h FINAL;
OPTIMIZE TABLE kline_1d FINAL;
```

## 🆘 常见问题

### Q: 出现 "converting float64 to Decimal is unsupported" 错误？
A: 这是类型转换问题，已修复。使用 `./reset_and_start.sh` 重新创建表结构

### Q: 迁移失败了怎么办？
A: 检查ClickHouse连接和磁盘空间，使用备份表恢复

### Q: 查询变慢了？
A: 检查索引是否正确创建，运行 `OPTIMIZE TABLE` 命令

### Q: 如何查询多个时间级别的数据？
A: 使用 UNION 查询或创建视图：
```sql
CREATE VIEW v_all_klines AS
SELECT *, '1h' as interval_type FROM kline_1h
UNION ALL
SELECT *, '1d' as interval_type FROM kline_1d;
```

### Q: 原有的趋势扫描器还能工作吗？
A: 需要更新趋势扫描器的查询逻辑，或保留统一视图做兼容

## 📞 技术支持

如果在迁移过程中遇到问题：
1. 查看日志文件 `logs/` 目录
2. 检查ClickHouse状态：`docker ps`
3. 验证表结构：`DESCRIBE table_name`
4. 提交Issue到项目仓库

---

**推荐阅读**：
- [ClickHouse MergeTree 引擎文档](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree)
- [时序数据库最佳实践](https://clickhouse.com/docs/en/guides/developer/time-series) 