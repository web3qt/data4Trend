use anyhow::Result;
use clap::Parser;
use log::{error, info, LevelFilter};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::signal;

use data4trend::{
    config::Config,
    collector::websocket::WebSocketCollector,
    storage::clickhouse::ClickHouseStore,
    api::server::ApiServer,
    monitoring::MonitoringManager,
};

/// 币安WebSocket数据收集器
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// 配置文件路径
    #[arg(short, long, default_value = "config/config.yaml")]
    config: String,

    /// 初始化数据库表
    #[arg(long)]
    init_db: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // 解析命令行参数
    let args = Args::parse();

    // 初始化日志
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .init();

    info!("===============================================");
    info!("  币安WebSocket 1分钟数据收集器 (Rust版本)");
    info!("  专门收集所有币安代币的1分钟K线数据");
    info!("  数据保留期：7天");
    info!("===============================================");

    // 加载配置
    let config = Arc::new(Config::load(&args.config)?);
    info!("Loaded config from {}", &args.config);

    // 创建数据通道
    let (data_tx, data_rx) = mpsc::channel(config.performance.data_channel_buffer);

    // 创建存储服务
    let store = ClickHouseStore::new(Arc::clone(&config), data_rx);

    // 初始化数据库(如果需要)
    if args.init_db {
        info!("Initializing database tables...");
        store.init_db().await?;
        info!("Database initialized successfully");
        return Ok(());
    }

    // 启动存储服务
    let storage_handle = {
        let mut store_clone = store;
        tokio::spawn(async move {
            if let Err(e) = store_clone.start().await {
                error!("Storage service error: {}", e);
            }
        })
    };

    // 创建监控管理器
    let monitoring = Arc::new(MonitoringManager::new());
    info!("Monitoring system initialized");

    // 启动监控循环
    let monitoring_handle = {
        let monitoring_clone = Arc::clone(&monitoring);
        tokio::spawn(async move {
            monitoring_clone.start_monitoring_loop().await;
        })
    };

    // 为API创建一个新的存储实例（仅用于查询）
    let (_, dummy_rx) = mpsc::channel(1);
    let store_for_api = Arc::new(ClickHouseStore::new(Arc::clone(&config), dummy_rx));

    // 启动API服务
    let api_handle = {
        let api_server = ApiServer::new(Arc::clone(&store_for_api), config.server.port);
        tokio::spawn(async move {
            if let Err(e) = api_server.start().await {
                error!("API server error: {}", e);
            }
        })
    };

    // 启动数据收集器
    let collector_handle = {
        let collector = WebSocketCollector::new(Arc::clone(&config), data_tx);
        tokio::spawn(async move {
            if let Err(e) = collector.start().await {
                error!("WebSocket collector error: {}", e);
            }
        })
    };

    info!("All services started successfully");
    info!("Press Ctrl+C to stop the application");

    // 等待中断信号
    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("Received Ctrl+C, shutting down gracefully...");
        }
        result = storage_handle => {
            if let Err(e) = result {
                error!("Storage service task failed: {}", e);
            }
        }
        result = api_handle => {
            if let Err(e) = result {
                error!("API service task failed: {}", e);
            }
        }
        result = collector_handle => {
            if let Err(e) = result {
                error!("Collector service task failed: {}", e);
            }
        }
        result = monitoring_handle => {
            if let Err(e) = result {
                error!("Monitoring service task failed: {}", e);
            }
        }
    }

    info!("Application stopped");
    Ok(())
}
