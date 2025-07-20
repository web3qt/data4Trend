use anyhow::Result;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use log::{error, info};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;

use crate::models::KLineData;
use crate::storage::ClickHouseStore;

/// API服务器
pub struct ApiServer {
    store: Arc<ClickHouseStore>,
    port: u16,
    stats: Arc<tokio::sync::Mutex<ApiStats>>,
}

#[derive(Debug, Clone, Serialize)]
struct ApiStats {
    active_streams: usize,
    total_symbols: usize,
    data_points: u64,
    uptime_seconds: u64,
    last_update: DateTime<Utc>,
}

impl ApiServer {
    pub fn new(store: Arc<ClickHouseStore>, port: u16) -> Self {
        let stats = ApiStats {
            active_streams: 0,
            total_symbols: 0,
            data_points: 0,
            uptime_seconds: 0,
            last_update: Utc::now(),
        };
        
        Self { 
            store, 
            port,
            stats: Arc::new(tokio::sync::Mutex::new(stats)),
        }
    }

    pub async fn start(&self) -> Result<()> {
        let app_state = AppState {
            store: Arc::clone(&self.store),
            stats: Arc::clone(&self.stats),
        };
        
        let app = Router::new()
            .route("/health", get(health_check))
            .route("/api/klines/:symbol", get(get_klines))
            .route("/api/stats", get(get_stats))
            .route("/api/symbols", get(get_symbols))
            .with_state(app_state);

        let addr = format!("0.0.0.0:{}", self.port);
        info!("Starting API server on {}", addr);
        
        let listener = TcpListener::bind(&addr).await?;
        axum::serve(listener, app).await?;

        Ok(())
    }
    
    pub async fn update_stats(&self, active_streams: usize, total_symbols: usize) {
        let mut stats = self.stats.lock().await;
        stats.active_streams = active_streams;
        stats.total_symbols = total_symbols;
        stats.data_points += 1;
        stats.last_update = Utc::now();
    }
}

#[derive(Clone)]
struct AppState {
    store: Arc<ClickHouseStore>,
    stats: Arc<tokio::sync::Mutex<ApiStats>>,
}

#[derive(Debug, Deserialize)]
struct KlineQuery {
    limit: Option<u32>,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
}

async fn get_klines(
    Path(symbol): Path<String>,
    Query(query): Query<KlineQuery>,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, Json<serde_json::Value>)> {
    let limit = query.limit.unwrap_or(100).min(1000); // 限制最大1000条
    
    match state.store.get_klines(&symbol, limit).await {
        Ok(klines) => {
            info!("Retrieved {} klines for {}", klines.len(), symbol);
            Ok((StatusCode::OK, Json(klines)))
        }
        Err(e) => {
            error!("Failed to get klines for {}: {}", symbol, e);
            let error_response = json!({
                "error": "Failed to retrieve klines",
                "message": e.to_string()
            });
            Err((StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)))
        }
    }
}

async fn get_stats(State(state): State<AppState>) -> impl IntoResponse {
    let stats = state.stats.lock().await;
    (StatusCode::OK, Json(stats.clone()))
}

async fn get_symbols(State(_state): State<AppState>) -> impl IntoResponse {
    // 返回支持的交易对列表
    let symbols = vec![
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "XRPUSDT",
        "SOLUSDT", "DOTUSDT", "DOGEUSDT", "AVAXUSDT", "SHIBUSDT"
    ];
    
    let response = json!({
        "symbols": symbols,
        "count": symbols.len()
    });
    
    (StatusCode::OK, Json(response))
}

async fn health_check() -> impl IntoResponse {
    let health = json!({
        "status": "healthy",
        "timestamp": Utc::now(),
        "service": "binance-ws-collector"
    });
    
    (StatusCode::OK, Json(health))
}