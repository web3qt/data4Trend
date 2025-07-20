use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use anyhow::Result;

/// K线数据结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KLineData {
    pub symbol: String,
    pub open_time: i64,
    pub close_time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_asset_volume: f64,
    pub number_of_trades: u64,
    pub interval: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct BinanceKlineEvent {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "k")]
    pub kline: BinanceKline,
}

#[derive(Debug, Deserialize)]
pub struct BinanceKline {
    #[serde(rename = "t")]
    pub start_time: i64,
    #[serde(rename = "T")]
    pub end_time: i64,
    #[serde(rename = "o")]
    pub open: String,
    #[serde(rename = "h")]
    pub high: String,
    #[serde(rename = "l")]
    pub low: String,
    #[serde(rename = "c")]
    pub close: String,
    #[serde(rename = "v")]
    pub volume: String,
    #[serde(rename = "q")]
    pub quote_asset_volume: String,
    #[serde(rename = "n")]
    pub number_of_trades: u64,
    #[serde(rename = "x")]
    pub is_final: bool,
}

impl KLineData {
    pub fn from_binance_event(event: BinanceKlineEvent) -> Result<Self> {
        let kline = event.kline;
        Ok(KLineData {
            symbol: event.symbol,
            open_time: kline.start_time,
            close_time: kline.end_time,
            open: kline.open.parse()?,
            high: kline.high.parse()?,
            low: kline.low.parse()?,
            close: kline.close.parse()?,
            volume: kline.volume.parse()?,
            quote_asset_volume: kline.quote_asset_volume.parse()?,
            number_of_trades: kline.number_of_trades,
            interval: "1m".to_string(), // 默认1分钟
            timestamp: Utc::now(),
        })
    }
}