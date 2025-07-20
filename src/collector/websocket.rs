use anyhow::{Context, Result};
use futures_util::StreamExt;
use log::{debug, error, info, warn};
use reqwest;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{interval, sleep};
use tokio_tungstenite::connect_async;
use url::Url;

use crate::models::{KLineData, BinanceKlineEvent};
use crate::config::Config;

/// WebSocket数据收集器
pub struct WebSocketCollector {
    config: Arc<Config>,
    data_tx: mpsc::Sender<KLineData>,
    active_streams: Arc<Mutex<HashMap<String, bool>>>,
    http_client: reqwest::Client,
}

impl WebSocketCollector {
    pub fn new(config: Arc<Config>, data_tx: mpsc::Sender<KLineData>) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");
            
        Self {
            config,
            data_tx,
            active_streams: Arc::new(Mutex::new(HashMap::new())),
            http_client,
        }
    }

    /// 启动WebSocket收集器
    pub async fn start(self) -> Result<()> {
        info!("Starting WebSocket collector");
        
        // 获取交易对列表
        let symbols = self.fetch_symbols().await?;
        info!("Fetched {} symbols for monitoring", symbols.len());
        
        // 启动连接管理器
        let collector = Arc::new(self);
        let collector_clone = collector.clone();
        let connection_manager = tokio::spawn(async move {
            collector.manage_connections(symbols).await
        });
        
        // 启动健康检查
        let health_checker = tokio::spawn(async move {
            collector_clone.health_check_loop().await
        });
        
        // 等待任务完成
        tokio::select! {
            result = connection_manager => {
                if let Err(e) = result {
                    error!("Connection manager failed: {}", e);
                }
            }
            result = health_checker => {
                if let Err(e) = result {
                    error!("Health checker failed: {}", e);
                }
            }
        }
        
        Ok(())
    }

    async fn manage_connections(&self, symbols: Vec<String>) -> Result<()> {
        info!("Managing connections for {} symbols", symbols.len());
        
        // 启动所有符号的连接
        for symbol in &symbols {
            self.start_symbol_connection(symbol.clone()).await?;
            // 避免同时启动太多连接
            sleep(Duration::from_millis(100)).await;
        }
        
        // 定期检查和重连
        let mut reconnect_interval = interval(Duration::from_secs(30));
        
        loop {
            reconnect_interval.tick().await;
            
            let active_streams = self.active_streams.lock().await;
            let inactive_symbols: Vec<String> = symbols
                .iter()
                .filter(|symbol| !active_streams.get(*symbol).unwrap_or(&false))
                .cloned()
                .collect();
            drop(active_streams);
            
            for symbol in inactive_symbols {
                warn!("Reconnecting to inactive symbol: {}", symbol);
                if let Err(e) = self.start_symbol_connection(symbol).await {
                    error!("Failed to reconnect: {}", e);
                }
                sleep(Duration::from_millis(500)).await;
            }
        }
    }
    
    async fn start_symbol_connection(&self, symbol: String) -> Result<()> {
        let data_tx = self.data_tx.clone();
        let active_streams = self.active_streams.clone();
        
        // 标记为活跃
        {
            let mut streams = active_streams.lock().await;
            streams.insert(symbol.clone(), true);
        }
        
        tokio::spawn(async move {
            if let Err(e) = Self::connect_symbol(&symbol, data_tx, active_streams.clone()).await {
                error!("Failed to connect to {}: {}", symbol, e);
                // 标记为非活跃
                let mut streams = active_streams.lock().await;
                streams.insert(symbol, false);
            }
        });
        
        Ok(())
    }

    async fn connect_symbol(
        symbol: &str,
        data_tx: mpsc::Sender<KLineData>,
        active_streams: Arc<Mutex<HashMap<String, bool>>>,
    ) -> Result<()> {
        let url = format!(
            "wss://stream.binance.com:9443/ws/{}@kline_1m",
            symbol.to_lowercase()
        );
        let url = Url::parse(&url)?;

        let (ws_stream, _) = connect_async(url.as_str()).await?;
        let (_write, mut read) = ws_stream.split();

        // 标记为活跃
        {
            let mut streams = active_streams.lock().await;
            streams.insert(symbol.to_string(), true);
            debug!("Connected to {}", symbol);
        }

        while let Some(msg) = read.next().await {
            match msg {
                Ok(msg) => {
                    if let Ok(kline) = Self::process_message(&symbol, msg).await {
                        if data_tx.send(kline).await.is_err() {
                            error!("Failed to send data for {}", symbol);
                            break;
                        }
                    }
                }
                Err(e) => {
                    error!("WebSocket error for {}: {}", symbol, e);
                    break;
                }
            }
        }

        // 标记为非活跃
        {
            let mut streams = active_streams.lock().await;
            streams.insert(symbol.to_string(), false);
            debug!("Disconnected from {}", symbol);
        }

        Ok(())
    }

   async fn process_message(_symbol: &str, msg: tokio_tungstenite::tungstenite::Message) -> Result<KLineData> {
        let text = msg.to_text()?;
        let event: serde_json::Value = serde_json::from_str(text)?;

        if let Some(kline) = event["k"].as_object() {
            if !kline["x"].as_bool().unwrap_or(false) {
                return Err(anyhow::anyhow!("Kline is not final"));
            }

            let kline_event: BinanceKlineEvent = serde_json::from_str(&text)?;

            KLineData::from_binance_event(kline_event)
        } else {
            Err(anyhow::anyhow!("Invalid kline message"))
        }
    }

    async fn fetch_symbols(&self) -> Result<Vec<String>> {
        info!("Fetching symbols from Binance API");
        
        let url = "https://api.binance.com/api/v3/exchangeInfo";
        
        match self.http_client.get(url).send().await {
            Ok(response) => {
                let exchange_info: ExchangeInfo = response.json().await
                    .context("Failed to parse exchange info")?;
                
                let symbols: Vec<String> = exchange_info.symbols
                    .into_iter()
                    .filter(|s| s.status == "TRADING" && s.quote_asset == "USDT")
                    .filter(|s| s.base_asset_precision >= 4) // 过滤掉精度太低的
                    .take(50) // 限制数量避免过载
                    .map(|s| s.symbol)
                    .collect();
                
                info!("Fetched {} USDT trading pairs", symbols.len());
                Ok(symbols)
            }
            Err(e) => {
                warn!("Failed to fetch symbols from API: {}, using fallback list", e);
                // 回退到硬编码列表
                Ok(vec![
                    "BTCUSDT".to_string(),
                    "ETHUSDT".to_string(),
                    "BNBUSDT".to_string(),
                    "ADAUSDT".to_string(),
                    "XRPUSDT".to_string(),
                    "SOLUSDT".to_string(),
                    "DOTUSDT".to_string(),
                    "DOGEUSDT".to_string(),
                    "AVAXUSDT".to_string(),
                    "SHIBUSDT".to_string(),
                ])
            }
        }
    }
    
    async fn health_check_loop(&self) -> Result<()> {
        let mut health_interval = interval(Duration::from_secs(60));
        
        loop {
            health_interval.tick().await;
            
            let active_streams = self.active_streams.lock().await;
            let active_count = active_streams.values().filter(|&&v| v).count();
            let total_count = active_streams.len();
            
            info!("Health check: {}/{} streams active", active_count, total_count);
            
            if active_count == 0 && total_count > 0 {
                warn!("No active streams detected!");
            }
        }
    }
}

impl Clone for WebSocketCollector {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            data_tx: self.data_tx.clone(),
            active_streams: self.active_streams.clone(),
            http_client: self.http_client.clone(),
        }
    }
}



// 用于解析Binance API响应
#[derive(Debug, Deserialize)]
struct ExchangeInfo {
    symbols: Vec<SymbolInfo>,
}

#[derive(Debug, Deserialize)]
struct SymbolInfo {
    symbol: String,
    status: String,
    #[serde(rename = "baseAsset")]
    base_asset: String,
    #[serde(rename = "quoteAsset")]
    quote_asset: String,
    #[serde(rename = "baseAssetPrecision")]
    base_asset_precision: u32,
}