# 使用官方Rust镜像作为构建环境
FROM rust:1.76-alpine as builder

WORKDIR /app

# 安装构建依赖
RUN apk add --no-cache musl-dev

# 复制Cargo.toml和Cargo.lock
COPY Cargo.toml Cargo.lock ./

# 创建虚拟项目来缓存依赖
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
RUN rm -rf src

# 复制源代码
COPY . .

# 构建应用
RUN cargo build --release --bin binance_ws_collector

# 使用多阶段构建创建更小的镜像
FROM alpine:latest

WORKDIR /app

# 安装运行时依赖
RUN apk --no-cache add ca-certificates tzdata curl

# 设置时区为亚洲/上海
ENV TZ=Asia/Shanghai

# 从构建阶段复制二进制文件
COPY --from=builder /app/target/release/binance_ws_collector /app/binance_ws_collector

# 创建配置和日志目录
RUN mkdir -p /app/config /app/logs

# 暴露API端口
EXPOSE 8080

# 运行应用
CMD ["/app/binance_ws_collector", "--config", "/app/config/config.yaml"]