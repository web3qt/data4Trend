use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// K线数据结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KLineData {
    pub symbol: String,
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
    pub open_price: f64,
    pub high_price: f64,
    pub low_price: f64,
    pub close_price: f64,
    pub volume: f64,
    pub quote_asset_volume: f64,
    pub number_of_trades: u64,
}

impl KLineData {
    /// 从币安WebSocket事件创建K线数据
    pub fn from_binance_event(event: BinanceKlineEvent) -> anyhow::Result<Self> {
        Ok(Self {
            symbol: event.symbol,
            open_time: DateTime::from_timestamp(event.kline.start_time / 1000, 0)
                .ok_or_else(|| anyhow::anyhow!("Invalid open time"))?,
            close_time: DateTime::from_timestamp(event.kline.end_time / 1000, 0)
                .ok_or_else(|| anyhow::anyhow!("Invalid close time"))?,
            open_price: event.kline.open.parse()?,
            high_price: event.kline.high.parse()?,
            low_price: event.kline.low.parse()?,
            close_price: event.kline.close.parse()?,
            volume: event.kline.volume.parse()?,
            quote_asset_volume: event.kline.quote_asset_volume.parse()?,
            number_of_trades: event.kline.number_of_trades,
        })
    }
}

/// 币安WebSocket K线事件
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceKlineEvent {
    pub symbol: String,
    pub kline: BinanceKline,
}

/// 币安K线数据
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceKline {
    pub start_time: i64,
    pub end_time: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub quote_asset_volume: String,
    pub number_of_trades: u64,
    pub is_final: bool,
}