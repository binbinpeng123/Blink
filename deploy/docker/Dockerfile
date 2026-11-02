# syntax=docker/dockerfile:1
#
# Blink 运行镜像：多阶段构建，最终层是 scratch。
#
# 选 scratch 而不是 alpine/distroless 的理由：服务是纯静态二进制，
# 运行期不需要 shell、CA 包或时区库；镜像里少一个包，边端就少一份
# 需要打补丁的攻击面。代价是容器内无法 `kubectl exec -- sh` 排查，
# 因此日志必须足够完整（服务本身已输出结构化 JSON 日志）。
#
# 构建：
#   docker build -f deploy/docker/Dockerfile -t blink:dev .
# 或：
#   make image CONTAINER=podman
ARG GO_IMAGE=golang:1.27-alpine

FROM ${GO_IMAGE} AS build

# 版本信息由调用方注入；不给默认值时也能构建，只是 buildinfo 为 dev/unknown。
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown

WORKDIR /src

# 先单独复制 go.mod：本版本没有第三方依赖，这一层仍能让后续
# 依赖变化时复用缓存；go.sum 在引入依赖后需要一并复制。
COPY go.mod ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

# CGO 关闭后二进制是静态链接的，scratch 才能运行；
# netgo/osusergo 保证 DNS 与用户查询不依赖 glibc 的 NSS。
ENV CGO_ENABLED=0
RUN go build -trimpath -tags netgo,osusergo \
	-ldflags "-s -w \
	  -X github.com/binbinpeng123/Blink/internal/buildinfo.Version=${VERSION} \
	  -X github.com/binbinpeng123/Blink/internal/buildinfo.Commit=${COMMIT} \
	  -X github.com/binbinpeng123/Blink/internal/buildinfo.BuildTime=${BUILD_TIME}" \
	-o /out/blink ./cmd/blink

# 预先创建数据目录并以运行用户持有：没有挂载卷时服务也能以非 root 启动，
# 有命名卷时容器引擎会用该目录的属主初始化空卷。
RUN mkdir -p /out/var/lib/blink

FROM scratch

COPY --from=build /out/blink /usr/local/bin/blink
COPY --from=build --chown=65532:65532 /out/var/lib/blink /var/lib/blink

# 必须监听所有地址：容器内 127.0.0.1 不会被端口映射或 Service 转发到。
ENV BLINK_ADDR=0.0.0.0:9000 \
    BLINK_DATA_DIR=/var/lib/blink

# 65532 是 distroless 的 nonroot UID。scratch 没有 /etc/passwd，
# 只能用数字 UID；K8s 侧还需 fsGroup 保证数据卷可写。
USER 65532:65532

EXPOSE 9000

# 凭据没有默认值：未提供 BLINK_ACCESS_KEY / BLINK_SECRET_KEY 时容器会
# 立即退出并打印原因，而不是带着空凭据或内置凭据继续运行。
ENTRYPOINT ["/usr/local/bin/blink"]
