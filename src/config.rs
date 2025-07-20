use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub clickhouse: ClickHouseConfig,
    pub binance: BinanceConfig,
    pub performance: PerformanceConfig,
    pub monitoring: MonitoringConfig,
    pub server: ServerConfig,
    pub log: LogConfig,
    pub symbol_filter: SymbolFilterConfig,
    pub websocket: WebSocketConfig,
    pub data_management: DataManagementConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClickHouseConfig {
    pub host: String,
    pub port: u16,
    pub http_port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    pub connection_timeout: u64,
    pub query_timeout: u64,
    pub max_connections: usize,
    pub data_retention_days: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceConfig {
    pub base_url: String,
    pub ws_url: String,
    pub stream_url: String,
    pub api_key: Option<String>,
    pub secret_key: Option<String>,
    pub reconnect_interval: u64,
    pub ping_interval: u64,
    pub max_reconnect_attempts: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PerformanceConfig {
    pub workers: usize,
    pub data_channel_buffer: usize,
    pub websocket_batch_size: usize,
    pub batch_size: usize,
    pub batch_timeout: u64,
    pub max_concurrent_connections: usize,
    pub connection_timeout: u64,
    pub read_timeout: u64,
    pub write_timeout: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MonitoringConfig {
    pub enabled: bool,
    pub enable_stats: bool,
    pub stats_interval_minutes: u64,
    pub enable_health_check: bool,
    pub health_check_interval_minutes: u64,
    pub metrics_interval: u64,
    pub health_check_interval: u64,
    pub alert_thresholds: AlertThresholds,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AlertThresholds {
    pub error_rate_percent: f64,
    pub memory_usage_mb: f64,
    pub cpu_usage_percent: f64,
    pub no_data_timeout_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub max_connections: usize,
    pub request_timeout: u64,
    pub cors_enabled: bool,
    pub cors_origins: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LogConfig {
    pub level: String,
    pub format: String,
    pub json_format: bool,
    pub file_path: String,
    pub max_size: String,
    pub max_files: u32,
    pub compress: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SymbolFilterConfig {
    pub enabled: bool,
    pub include_patterns: Vec<String>,
    pub exclude_patterns: Vec<String>,
    pub min_volume_24h: u64,
    pub max_symbols: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WebSocketConfig {
    pub max_connections_per_stream: usize,
    pub buffer_size: usize,
    pub compression: bool,
    pub auto_reconnect: bool,
    pub heartbeat_interval: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DataManagementConfig {
    pub cleanup_enabled: bool,
    pub cleanup_interval: u64,
    pub backup_enabled: bool,
    pub backup_interval: u64,
    pub compression_enabled: bool,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let settings = config::Config::builder()
            .add_source(config::File::from(path.as_ref()))
            .build()?;
            
        settings.try_deserialize().map_err(Into::into)
    }
}