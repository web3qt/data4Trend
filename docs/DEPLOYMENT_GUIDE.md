# 部署指南

## 概览

本指南详细介绍如何在不同环境中部署币安WebSocket数据收集器。

## 环境要求

### 最低要求

- **操作系统**: Linux (Ubuntu 20.04+推荐) / macOS / Windows
- **CPU**: 2核心以上
- **内存**: 4GB RAM (推荐8GB)
- **存储**: 50GB可用空间 (数据7天保留)
- **网络**: 稳定的互联网连接，可访问币安API

### 推荐要求

- **CPU**: 4核心以上
- **内存**: 8GB RAM以上
- **存储**: 100GB SSD
- **网络**: 专线或高速宽带

## 安装方式

### 方式1: Docker部署 (推荐)

#### 前提条件

```bash
# 安装Docker和Docker Compose
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# 安装Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

#### 部署步骤

1. **准备项目文件**

```bash
# 克隆项目
git clone https://github.com/your-repo/websocket-collector.git
cd websocket-collector

# 创建必要目录
mkdir -p logs data/clickhouse
```

2. **配置环境变量**

创建 `.env` 文件：

```bash
# ClickHouse配置
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=9000
CLICKHOUSE_HTTP_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=123456
CLICKHOUSE_DATABASE=data4trend

# Binance API配置 (可选)
BINANCE_API_KEY=your_api_key
BINANCE_SECRET_KEY=your_secret_key

# 应用配置
APP_PORT=8080
LOG_LEVEL=info

# 数据管理
DATA_RETENTION_DAYS=7
```

3. **创建docker-compose.yml**

```yaml
version: '3.8'

services:
  clickhouse:
    image: clickhouse/clickhouse-server:23.8
    container_name: collector-clickhouse
    ports:
      - "9000:9000"
      - "8123:8123"
    environment:
      CLICKHOUSE_DB: data4trend
      CLICKHOUSE_USER: default
      CLICKHOUSE_PASSWORD: 123456
    volumes:
      - ./data/clickhouse:/var/lib/clickhouse
      - ./scripts/init_database.sql:/docker-entrypoint-initdb.d/init.sql
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
    healthcheck:
      test: ["CMD", "clickhouse-client", "--query", "SELECT 1"]
      interval: 30s
      timeout: 10s
      retries: 3

  collector:
    build: .
    container_name: websocket-collector
    depends_on:
      clickhouse:
        condition: service_healthy
    ports:
      - "8080:8080"
    environment:
      - CLICKHOUSE_HOST=clickhouse
      - CLICKHOUSE_PORT=9000
      - CLICKHOUSE_DATABASE=data4trend
      - CLICKHOUSE_USER=default
      - CLICKHOUSE_PASSWORD=123456
      - LOG_LEVEL=info
    volumes:
      - ./logs:/app/logs
      - ./config:/app/config
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/api/health"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  clickhouse_data:
```

4. **构建和启动**

```bash
# 构建镜像
docker-compose build

# 启动服务
docker-compose up -d

# 查看日志
docker-compose logs -f collector
```

#### Docker部署优化

1. **资源限制**

```yaml
services:
  collector:
    # ... 其他配置
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '2.0'
        reservations:
          memory: 1G
          cpus: '1.0'
```

2. **网络优化**

```yaml
networks:
  collector-network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.0.0/16

services:
  clickhouse:
    networks:
      - collector-network
  collector:
    networks:
      - collector-network
```

### 方式2: 二进制部署

#### 前提条件

```bash
# 安装Go (如果需要从源码构建)
wget https://golang.org/dl/go1.21.0.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.21.0.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin

# 安装ClickHouse
curl https://clickhouse.com/ | sh
sudo ./clickhouse install
```

#### 部署步骤

1. **构建应用**

```bash
# 克隆代码
git clone https://github.com/your-repo/websocket-collector.git
cd websocket-collector

# 安装依赖
go mod download

# 构建二进制文件
CGO_ENABLED=0 GOOS=linux go build -o bin/websocket-collector cmd/websocket-collector/main.go
```

2. **配置文件**

```bash
# 复制配置文件
cp config/websocket_1m.yaml.example config/websocket_1m.yaml

# 编辑配置
vim config/websocket_1m.yaml
```

3. **启动ClickHouse**

```bash
# 启动ClickHouse服务
sudo systemctl start clickhouse-server
sudo systemctl enable clickhouse-server

# 初始化数据库
clickhouse-client --query="$(cat scripts/init_database.sql)"
```

4. **启动应用**

```bash
# 创建systemd服务文件
sudo tee /etc/systemd/system/websocket-collector.service > /dev/null <<EOF
[Unit]
Description=Binance WebSocket Data Collector
After=network.target clickhouse-server.service
Requires=clickhouse-server.service

[Service]
Type=simple
User=collector
Group=collector
WorkingDirectory=/opt/websocket-collector
ExecStart=/opt/websocket-collector/bin/websocket-collector -config /opt/websocket-collector/config/websocket_1m.yaml
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# 创建用户和目录
sudo useradd --system --shell /bin/false collector
sudo mkdir -p /opt/websocket-collector
sudo cp -r . /opt/websocket-collector/
sudo chown -R collector:collector /opt/websocket-collector

# 启动服务
sudo systemctl daemon-reload
sudo systemctl enable websocket-collector
sudo systemctl start websocket-collector
```

### 方式3: Kubernetes部署

#### ConfigMap配置

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: collector-config
data:
  websocket_1m.yaml: |
    clickhouse:
      host: "clickhouse-service"
      port: 9000
      database: "data4trend"
      user: "default"
      password: "123456"
    
    performance:
      workers: 10
      data_channel_buffer: 50000
      websocket_batch_size: 50
    
    data_management:
      retention_days: 7
      cleanup_interval_hours: 6
```

#### Deployment配置

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: websocket-collector
spec:
  replicas: 1
  selector:
    matchLabels:
      app: websocket-collector
  template:
    metadata:
      labels:
        app: websocket-collector
    spec:
      containers:
      - name: collector
        image: websocket-collector:latest
        ports:
        - containerPort: 8080
        env:
        - name: CONFIG_PATH
          value: "/app/config/websocket_1m.yaml"
        volumeMounts:
        - name: config-volume
          mountPath: /app/config
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /api/health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /api/health
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 10
      volumes:
      - name: config-volume
        configMap:
          name: collector-config
```

#### Service配置

```yaml
apiVersion: v1
kind: Service
metadata:
  name: websocket-collector-service
spec:
  selector:
    app: websocket-collector
  ports:
  - port: 8080
    targetPort: 8080
  type: LoadBalancer
```

## 配置管理

### 环境变量配置

重要的环境变量：

```bash
# 数据库连接
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=9000
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=123456
export CLICKHOUSE_DATABASE=data4trend

# Binance API (可选)
export BINANCE_API_KEY=your_api_key
export BINANCE_SECRET_KEY=your_secret_key

# 应用配置
export APP_PORT=8080
export LOG_LEVEL=info
export CONFIG_PATH=/app/config/websocket_1m.yaml

# 性能调优
export WORKERS=10
export DATA_CHANNEL_BUFFER=50000
export WEBSOCKET_BATCH_SIZE=50

# 代理设置 (如果需要)
export HTTP_PROXY=http://proxy.example.com:8080
export HTTPS_PROXY=http://proxy.example.com:8080
```

### 配置文件模板

创建生产环境配置文件：

```yaml
# config/production.yaml
clickhouse:
  host: "prod-clickhouse.example.com"
  port: 9000
  database: "data4trend_prod"
  user: "collector_user"
  password: "${CLICKHOUSE_PASSWORD}"
  max_open_conns: 20
  max_idle_conns: 10
  conn_max_lifetime: "1h"

binance:
  api_key: "${BINANCE_API_KEY}"
  secret_key: "${BINANCE_SECRET_KEY}"

http:
  timeout: 30
  proxy: "${HTTP_PROXY}"

performance:
  workers: 20
  data_channel_buffer: 100000
  websocket_batch_size: 100
  connection_interval_ms: 50
  batch_interval_s: 1

log:
  level: "warn"
  json_format: true
  file_path: "/var/log/collector/collector.log"
  max_size: 100
  max_backups: 10
  max_age: 30
  compress: true

monitoring:
  enable_metrics: true
  stats_interval_minutes: 1
  health_check_interval_minutes: 1
  max_consecutive_errors: 10

data_management:
  retention_days: 7
  cleanup_interval_hours: 6
  max_cleanup_batch_size: 10000
```

## 监控和日志

### 监控集成

#### Prometheus监控

创建监控配置：

```yaml
# docker-compose.monitoring.yml
version: '3.8'

services:
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - ./monitoring/grafana/dashboards:/var/lib/grafana/dashboards
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning
```

Prometheus配置 (`monitoring/prometheus.yml`)：

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'websocket-collector'
    static_configs:
      - targets: ['collector:8080']
    metrics_path: '/metrics'
    scrape_interval: 10s
```

#### 健康检查

```bash
# 创建健康检查脚本
cat > scripts/health_check.sh << 'EOF'
#!/bin/bash

HEALTH_ENDPOINT="http://localhost:8080/api/health"
STATS_ENDPOINT="http://localhost:8080/api/stats"

# 检查健康状态
health_response=$(curl -s "$HEALTH_ENDPOINT")
health_status=$(echo "$health_response" | jq -r '.data.status')

if [ "$health_status" != "healthy" ]; then
    echo "CRITICAL: Service unhealthy - $health_response"
    exit 2
fi

# 检查数据收集率
stats_response=$(curl -s "$STATS_ENDPOINT")
collection_rate=$(echo "$stats_response" | jq -r '.data.data_collection_rate')

if (( $(echo "$collection_rate < 100" | bc -l) )); then
    echo "WARNING: Low collection rate - $collection_rate"
    exit 1
fi

echo "OK: Service healthy, collection rate: $collection_rate"
exit 0
EOF

chmod +x scripts/health_check.sh
```

### 日志管理

#### 日志轮转配置

```bash
# /etc/logrotate.d/websocket-collector
/opt/websocket-collector/logs/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
    create 644 collector collector
}
```

#### 集中化日志

ELK Stack配置示例：

```yaml
# docker-compose.logging.yml
version: '3.8'

services:
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.9.0
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
    ports:
      - "9200:9200"

  logstash:
    image: docker.elastic.co/logstash/logstash:8.9.0
    volumes:
      - ./logging/logstash.conf:/usr/share/logstash/pipeline/logstash.conf
    ports:
      - "5044:5044"

  kibana:
    image: docker.elastic.co/kibana/kibana:8.9.0
    ports:
      - "5601:5601"
    environment:
      - ELASTICSEARCH_HOSTS=http://elasticsearch:9200
```

## 性能优化

### 系统级优化

```bash
# 增加文件描述符限制
echo "* soft nofile 65536" >> /etc/security/limits.conf
echo "* hard nofile 65536" >> /etc/security/limits.conf

# TCP优化
cat >> /etc/sysctl.conf << EOF
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 65536 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
net.core.netdev_max_backlog = 30000
net.ipv4.tcp_congestion_control = bbr
EOF

sysctl -p
```

### 应用级优化

1. **内存优化**

```yaml
performance:
  workers: 20  # 根据CPU核心数调整
  data_channel_buffer: 100000  # 增大缓冲区
  websocket_batch_size: 100   # 批处理大小
  gc_percent: 100  # Go GC调优
```

2. **数据库优化**

```sql
-- ClickHouse优化设置
SET max_memory_usage = 4000000000;
SET max_threads = 8;
SET max_insert_block_size = 1048576;
```

## 备份和恢复

### 数据备份

```bash
# 创建备份脚本
cat > scripts/backup.sh << 'EOF'
#!/bin/bash

BACKUP_DIR="/backup/clickhouse"
DATE=$(date +%Y%m%d_%H%M%S)

# 创建备份目录
mkdir -p "$BACKUP_DIR/$DATE"

# 备份ClickHouse数据
clickhouse-client --query="BACKUP DATABASE data4trend TO Disk('default', '$BACKUP_DIR/$DATE/')"

# 清理旧备份 (保留7天)
find "$BACKUP_DIR" -type d -mtime +7 -exec rm -rf {} +

echo "Backup completed: $BACKUP_DIR/$DATE"
EOF

chmod +x scripts/backup.sh
```

### 自动备份

```bash
# 添加到crontab
echo "0 2 * * * /opt/websocket-collector/scripts/backup.sh" | crontab -
```

### 恢复数据

```bash
# 恢复脚本
cat > scripts/restore.sh << 'EOF'
#!/bin/bash

BACKUP_PATH=$1

if [ -z "$BACKUP_PATH" ]; then
    echo "Usage: $0 <backup_path>"
    exit 1
fi

# 停止服务
systemctl stop websocket-collector

# 恢复数据
clickhouse-client --query="RESTORE DATABASE data4trend FROM Disk('default', '$BACKUP_PATH')"

# 重启服务
systemctl start websocket-collector

echo "Restore completed from: $BACKUP_PATH"
EOF

chmod +x scripts/restore.sh
```

## 故障排除

### 常见问题

1. **WebSocket连接问题**

```bash
# 检查网络连接
curl -I https://stream.binance.com:9443/ws/btcusdt@kline_1m

# 检查防火墙
sudo ufw status
sudo iptables -L

# 检查代理设置
echo $HTTP_PROXY
echo $HTTPS_PROXY
```

2. **数据库连接问题**

```bash
# 检查ClickHouse状态
systemctl status clickhouse-server

# 测试连接
clickhouse-client --query="SELECT 1"

# 检查磁盘空间
df -h /var/lib/clickhouse
```

3. **性能问题**

```bash
# 监控资源使用
top -p $(pgrep websocket-collector)
iostat -x 1

# 检查网络连接数
ss -tuln | grep :8080
netstat -an | grep ESTABLISHED | wc -l
```

### 日志分析

```bash
# 常用日志查询命令
tail -f /var/log/websocket-collector/collector.log

# 过滤错误日志
grep -i error /var/log/websocket-collector/collector.log

# 统计连接状态
grep "WebSocket" /var/log/websocket-collector/collector.log | grep -c "connected"

# 分析数据收集速率
grep "collection_rate" /var/log/websocket-collector/collector.log | tail -10
```

## 安全考虑

### 网络安全

```bash
# 防火墙配置
sudo ufw allow 8080/tcp  # API端口
sudo ufw allow 9000/tcp  # ClickHouse端口 (仅内网)
sudo ufw enable

# 使用TLS/SSL
# 配置Nginx反向代理
sudo apt install nginx
```

Nginx配置示例：

```nginx
server {
    listen 443 ssl;
    server_name collector.example.com;
    
    ssl_certificate /etc/ssl/certs/collector.crt;
    ssl_certificate_key /etc/ssl/private/collector.key;
    
    location / {
        proxy_pass http://localhost:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### 访问控制

1. **API访问限制**

```yaml
# config/security.yaml
security:
  enable_api_key: true
  api_keys:
    - "your-api-key-here"
  rate_limiting:
    requests_per_minute: 1000
    burst: 100
  allowed_ips:
    - "192.168.1.0/24"
    - "10.0.0.0/8"
```

2. **数据库安全**

```sql
-- 创建专用用户
CREATE USER collector_user IDENTIFIED BY 'strong_password';
GRANT SELECT, INSERT, CREATE ON data4trend.* TO collector_user;
```

## 升级和维护

### 版本升级

```bash
# 创建升级脚本
cat > scripts/upgrade.sh << 'EOF'
#!/bin/bash

NEW_VERSION=$1
BACKUP_DIR="/backup/upgrade"
DATE=$(date +%Y%m%d_%H%M%S)

if [ -z "$NEW_VERSION" ]; then
    echo "Usage: $0 <new_version>"
    exit 1
fi

# 备份当前版本
mkdir -p "$BACKUP_DIR"
cp -r /opt/websocket-collector "$BACKUP_DIR/websocket-collector-$DATE"

# 停止服务
systemctl stop websocket-collector

# 下载新版本
wget "https://github.com/your-repo/websocket-collector/releases/download/$NEW_VERSION/websocket-collector-linux-amd64.tar.gz"
tar -xzf "websocket-collector-linux-amd64.tar.gz"

# 更新二进制文件
cp websocket-collector /opt/websocket-collector/bin/

# 启动服务
systemctl start websocket-collector

# 验证升级
sleep 10
curl -f http://localhost:8080/api/health

echo "Upgrade to $NEW_VERSION completed"
EOF

chmod +x scripts/upgrade.sh
```

### 定期维护

```bash
# 创建维护脚本
cat > scripts/maintenance.sh << 'EOF'
#!/bin/bash

# 清理日志
find /var/log/websocket-collector -name "*.log" -mtime +30 -delete

# 清理临时文件
find /tmp -name "collector_*" -mtime +1 -delete

# 检查磁盘空间
DISK_USAGE=$(df /var/lib/clickhouse | awk 'NR==2 {print $5}' | sed 's/%//')
if [ "$DISK_USAGE" -gt 80 ]; then
    echo "WARNING: Disk usage is ${DISK_USAGE}%"
fi

# 重启服务 (如果需要)
if [ "$1" == "--restart" ]; then
    systemctl restart websocket-collector
fi

echo "Maintenance completed"
EOF

chmod +x scripts/maintenance.sh

# 添加到crontab
echo "0 3 * * 0 /opt/websocket-collector/scripts/maintenance.sh" | crontab -
``` 