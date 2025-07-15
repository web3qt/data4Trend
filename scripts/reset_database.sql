-- 完全重置数据库脚本
-- 警告：此脚本将删除所有现有数据！

USE data4trend;

-- 删除所有现有的K线相关表
DROP TABLE IF EXISTS kline;
DROP TABLE IF EXISTS kline_backup;
DROP TABLE IF EXISTS kline_legacy;
DROP TABLE IF EXISTS kline_1m;
DROP TABLE IF EXISTS kline_5m;
DROP TABLE IF EXISTS kline_15m;
DROP TABLE IF EXISTS kline_1h;
DROP TABLE IF EXISTS kline_4h;
DROP TABLE IF EXISTS kline_1d;

-- 删除物化视图
DROP VIEW IF EXISTS mv_kline_1m_to_5m;
DROP VIEW IF EXISTS v_kline_unified;
DROP VIEW IF EXISTS v_table_stats;

-- 删除连接测试表
DROP TABLE IF EXISTS connection_test;

-- 重新创建数据库（可选，用于完全清理）
-- DROP DATABASE IF EXISTS data4trend;
-- CREATE DATABASE data4trend;
-- USE data4trend;

SELECT 'Database reset completed. All tables dropped.' as message; 