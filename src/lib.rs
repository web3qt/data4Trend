//! 币安WebSocket数据收集器库
//! 
//! 这个库提供了完整的币安WebSocket数据收集、存储和API服务功能。

pub mod config;
pub mod models;
pub mod collector;
pub mod storage;
pub mod api;
pub mod monitoring;

pub use config::Config;
pub use models::KLineData;
pub use collector::WebSocketCollector;
pub use storage::ClickHouseStore;
pub use api::ApiServer;
pub use monitoring::MonitoringManager;