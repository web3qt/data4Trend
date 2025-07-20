//! 监控模块
//! 
//! 提供系统监控、统计和健康检查功能

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::interval;
use serde::{Deserialize, Serialize};
use log::{info, warn, error};

/// 系统统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStats {
    pub uptime_seconds: u64,
    pub total_messages_received: u64,
    pub total_messages_processed: u64,
    pub total_errors: u64,
    pub active_connections: u64,
    pub memory_usage_mb: f64,
    pub cpu_usage_percent: f64,
    pub last_update: String,
}

/// 性能指标
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub messages_per_second: f64,
    pub avg_processing_time_ms: f64,
    pub error_rate_percent: f64,
    pub connection_success_rate: f64,
}

/// 健康检查状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: String,
    pub database_connected: bool,
    pub websocket_connected: bool,
    pub api_server_running: bool,
    pub last_data_received: Option<String>,
    pub issues: Vec<String>,
}

/// 监控管理器
pub struct MonitoringManager {
    start_time: Instant,
    messages_received: Arc<AtomicU64>,
    messages_processed: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
    active_connections: Arc<AtomicU64>,
    last_message_time: Arc<std::sync::Mutex<Option<Instant>>>,
}

impl MonitoringManager {
    /// 创建新的监控管理器
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            messages_received: Arc::new(AtomicU64::new(0)),
            messages_processed: Arc::new(AtomicU64::new(0)),
            errors: Arc::new(AtomicU64::new(0)),
            active_connections: Arc::new(AtomicU64::new(0)),
            last_message_time: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// 记录接收到的消息
    pub fn record_message_received(&self) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
        *self.last_message_time.lock().unwrap() = Some(Instant::now());
    }

    /// 记录处理的消息
    pub fn record_message_processed(&self) {
        self.messages_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录错误
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// 设置活跃连接数
    pub fn set_active_connections(&self, count: u64) {
        self.active_connections.store(count, Ordering::Relaxed);
    }

    /// 获取系统统计信息
    pub fn get_stats(&self) -> SystemStats {
        let uptime = self.start_time.elapsed().as_secs();
        let memory_usage = self.get_memory_usage();
        let cpu_usage = self.get_cpu_usage();

        SystemStats {
            uptime_seconds: uptime,
            total_messages_received: self.messages_received.load(Ordering::Relaxed),
            total_messages_processed: self.messages_processed.load(Ordering::Relaxed),
            total_errors: self.errors.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            memory_usage_mb: memory_usage,
            cpu_usage_percent: cpu_usage,
            last_update: chrono::Utc::now().to_rfc3339(),
        }
    }

    /// 获取性能指标
    pub fn get_performance_metrics(&self) -> PerformanceMetrics {
        let uptime = self.start_time.elapsed().as_secs() as f64;
        let total_received = self.messages_received.load(Ordering::Relaxed) as f64;
        let total_processed = self.messages_processed.load(Ordering::Relaxed) as f64;
        let total_errors = self.errors.load(Ordering::Relaxed) as f64;

        let messages_per_second = if uptime > 0.0 { total_received / uptime } else { 0.0 };
        let error_rate = if total_received > 0.0 { (total_errors / total_received) * 100.0 } else { 0.0 };
        let success_rate = if total_received > 0.0 { (total_processed / total_received) * 100.0 } else { 100.0 };

        PerformanceMetrics {
            messages_per_second,
            avg_processing_time_ms: 0.0, // 简化实现
            error_rate_percent: error_rate,
            connection_success_rate: success_rate,
        }
    }

    /// 获取健康状态
    pub fn get_health_status(&self) -> HealthStatus {
        let mut issues = Vec::new();
        let mut status = "healthy".to_string();

        // 检查最后接收数据的时间
        let last_data_received = {
            let last_time = self.last_message_time.lock().unwrap();
            match *last_time {
                Some(time) => {
                    let elapsed = time.elapsed();
                    if elapsed > Duration::from_secs(300) { // 5分钟没有数据
                        issues.push("No data received in the last 5 minutes".to_string());
                        status = "warning".to_string();
                    }
                    Some(chrono::Utc::now().to_rfc3339())
                }
                None => {
                    issues.push("No data received yet".to_string());
                    None
                }
            }
        };

        // 检查错误率
        let error_rate = self.get_performance_metrics().error_rate_percent;
        if error_rate > 10.0 {
            issues.push(format!("High error rate: {:.2}%", error_rate));
            status = "warning".to_string();
        }

        if error_rate > 50.0 {
            status = "critical".to_string();
        }

        HealthStatus {
            status,
            database_connected: true, // 简化实现
            websocket_connected: self.active_connections.load(Ordering::Relaxed) > 0,
            api_server_running: true, // 简化实现
            last_data_received,
            issues,
        }
    }

    /// 启动监控循环
    pub async fn start_monitoring_loop(&self) {
        let mut interval = interval(Duration::from_secs(60)); // 每分钟记录一次
        
        loop {
            interval.tick().await;
            
            let stats = self.get_stats();
            let performance = self.get_performance_metrics();
            let health = self.get_health_status();
            
            info!("=== System Monitoring Report ===");
            info!("Uptime: {}s, Messages: {}/{}, Errors: {}, Connections: {}", 
                stats.uptime_seconds, 
                stats.total_messages_received, 
                stats.total_messages_processed,
                stats.total_errors,
                stats.active_connections
            );
            info!("Performance: {:.2} msg/s, Error rate: {:.2}%", 
                performance.messages_per_second, 
                performance.error_rate_percent
            );
            info!("Health: {}, Issues: {:?}", health.status, health.issues);
            
            // 如果状态不健康，记录警告
            if health.status != "healthy" {
                warn!("System health status: {} - Issues: {:?}", health.status, health.issues);
            }
        }
    }

    /// 获取内存使用量（简化实现）
    fn get_memory_usage(&self) -> f64 {
        // 在实际实现中，可以使用系统调用获取真实的内存使用量
        0.0
    }

    /// 获取CPU使用率（简化实现）
    fn get_cpu_usage(&self) -> f64 {
        // 在实际实现中，可以使用系统调用获取真实的CPU使用率
        0.0
    }
}

impl Default for MonitoringManager {
    fn default() -> Self {
        Self::new()
    }
}