FROM golang:1.22-alpine as builder

WORKDIR /app

# 复制go.mod和go.sum文件
COPY go.mod go.sum ./
RUN go mod download

# 复制源代码
COPY . .

# 构建应用
RUN CGO_ENABLED=0 GOOS=linux go build -o dataFeeder cmd/main.go

# 使用多阶段构建创建更小的镜像
FROM alpine:latest

WORKDIR /app

# 安装依赖
RUN apk --no-cache add ca-certificates tzdata

# 设置时区为亚洲/上海
ENV TZ=Asia/Shanghai

# 从构建阶段复制二进制文件
COPY --from=builder /app/dataFeeder /app/dataFeeder

# 创建配置和日志目录
RUN mkdir -p /app/config /app/logs

# 暴露API端口
EXPOSE 8080

# 运行应用
CMD ["/app/dataFeeder"]