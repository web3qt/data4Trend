use anyhow::{Context, Result};
use clickhouse::{Client, Row};
use log::{debug, error, info, warn};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use serde::{Serialize, Deserialize};

use crate::models::KLineData;
use crate::config::Config;

/// ClickHouse数据存储
pub struct ClickHouseStore {
    client: Client,
    config: Arc<Config>,
    data_rx: Option<mpsc::Receiver<KLineData>>,
}

impl ClickHouseStore {
    pub fn new(config: Arc<Config>, data_rx: mpsc::Receiver<KLineData>) -> Self {
        let client = Client::default()
            .with_url(format!("http://{}:{}", config.clickhouse.host, config.clickhouse.http_port))
            .with_user(config.clickhouse.username.clone())
            .with_password(config.clickhouse.password.clone())
            .with_database(config.clickhouse.database.clone());
        
        Self { 
            client, 
            config, 
            data_rx: Some(data_rx) 
        }
    }

    /// 启动数据存储服务
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting ClickHouse storage service");

        // 初始化数据库表
        self.init_db().await?;

        // 启动数据清理定时任务
        let mut cleaner = interval(Duration::from_secs(3600)); // 每小时清理一次
        let mut batch = Vec::with_capacity(self.config.performance.websocket_batch_size);
        
        if let Some(mut data_rx) = self.data_rx.take() {
            loop {
                tokio::select! {
                    Some(data) = data_rx.recv() => {
                        batch.push(data);
                        
                        // 批量写入
                        if batch.len() >= self.config.performance.websocket_batch_size {
                            if let Err(e) = self.batch_insert(&batch).await {
                                error!("Failed to insert batch: {}", e);
                            }
                            batch.clear();
                        }
                    }
                    _ = cleaner.tick() => {
                        // 处理剩余批次
                        if !batch.is_empty() {
                            if let Err(e) = self.batch_insert(&batch).await {
                                error!("Failed to insert remaining batch: {}", e);
                            }
                            batch.clear();
                        }
                        
                        if let Err(e) = self.clean_old_data().await {
                            error!("Failed to clean old data: {}", e);
                        }
                    }
                }
            }
        }
        
        Ok(())
    }

    pub async fn init_db(&self) -> Result<()> {
        let ddl = r"
        CREATE TABLE IF NOT EXISTS klines_1m (
            symbol String,
            open_time DateTime64(3),
            close_time DateTime64(3),
            open Decimal(20, 8),
            high Decimal(20, 8),
            low Decimal(20, 8),
            close Decimal(20, 8),
            volume Decimal(20, 8),
            quote_asset_volume Decimal(20, 8),
            number_of_trades UInt64,
            interval String,
            created_at DateTime DEFAULT now(),
            updated_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(open_time)
        ORDER BY (symbol, open_time)
        TTL toDateTime(open_time) + INTERVAL 7 DAY
        ";

        self.client.query(ddl).execute().await
            .context("Failed to create klines_1m table")?;
        
        info!("Initialized ClickHouse tables");
        Ok(())
    }

    async fn batch_insert(&self, data: &[KLineData]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        
        let mut insert = self.client.insert("klines_1m")?;
        
        for kline in data {
            insert.write(&KLineRow {
                symbol: kline.symbol.clone(),
                open_time: kline.open_time,
                close_time: kline.close_time,
                open: kline.open,
                high: kline.high,
                low: kline.low,
                close: kline.close,
                volume: kline.volume,
                quote_asset_volume: kline.quote_asset_volume,
                number_of_trades: kline.number_of_trades,
                interval: "1m".to_string(),
            }).await?;
        }
        
        insert.end().await?;
        debug!("Inserted {} records", data.len());
        Ok(())
    }

    async fn clean_old_data(&self) -> Result<()> {
        let sql = "ALTER TABLE klines_1m DELETE WHERE toDateTime(open_time) < now() - INTERVAL 7 DAY";
        
        self.client.query(sql).execute().await
            .context("Failed to clean old data")?;
        
        debug!("Cleaned old data");
        Ok(())
    }
    
    /// 获取指定符号的K线数据
    pub async fn get_klines(&self, symbol: &str, limit: u32) -> Result<Vec<KLineData>> {
        let sql = "SELECT * FROM klines_1m WHERE symbol = ? ORDER BY open_time DESC LIMIT ?";
        
        let mut cursor = self.client
            .query(sql)
            .bind(symbol)
            .bind(limit)
            .fetch::<KLineRow>()?;
        
        let mut results = Vec::new();
        while let Some(row) = cursor.next().await? {
            results.push(KLineData {
                symbol: row.symbol,
                open_time: row.open_time,
                close_time: row.close_time,
                open: row.open,
                high: row.high,
                low: row.low,
                close: row.close,
                volume: row.volume,
                quote_asset_volume: row.quote_asset_volume,
                number_of_trades: row.number_of_trades,
                interval: "1m".to_string(),
                timestamp: chrono::Utc::now(),
            });
        }
        
        Ok(results)
    }
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
struct KLineRow {
    symbol: String,
    open_time: i64,
    close_time: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    quote_asset_volume: f64,
    number_of_trades: u64,
    interval: String,
}