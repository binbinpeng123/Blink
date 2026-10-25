# Blink 构建与测试入口。
#
# 所有目标都不依赖网络：首个版本只用 Go 标准库。
# 版本信息通过 -ldflags 注入，未注入时二进制内的值为 dev/unknown。

BINARY      := blink
PKG         := github.com/binbinpeng123/Blink
CMD         := ./cmd/blink
BIN_DIR     := bin

VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT      ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_TIME  ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS     := -s -w \
               -X $(PKG)/internal/buildinfo.Version=$(VERSION) \
               -X $(PKG)/internal/buildinfo.Commit=$(COMMIT) \
               -X $(PKG)/internal/buildinfo.BuildTime=$(BUILD_TIME)

GOFLAGS     ?=
GO          := go

# 容器引擎：本机 Docker 守护进程未运行时可用 `make image CONTAINER=podman`。
CONTAINER   ?= docker
IMAGE       ?= blink:$(VERSION)
# 构建阶段基础镜像。国内网络拉不到 Docker Hub 时可指向镜像站，例如：
#   make image GO_IMAGE=docker.m.daocloud.io/library/golang:1.27-alpine
GO_IMAGE    ?= golang:1.27-alpine

.DEFAULT_GOAL := help

.PHONY: help
help: ## 显示可用目标
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## 构建二进制到 bin/blink（含版本注入）
	@mkdir -p $(BIN_DIR)
	$(GO) build $(GOFLAGS) -trimpath -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$(BINARY) $(CMD)

.PHONY: run
run: ## 本地运行（需要 BLINK_ACCESS_KEY / BLINK_SECRET_KEY 环境变量）
	$(GO) run $(CMD)

.PHONY: test
test: ## 运行全部单元测试
	$(GO) test ./...

.PHONY: test-race
test-race: ## 运行单元测试并开启竞态检测
	$(GO) test -race ./...

.PHONY: test-integration
test-integration: ## 运行集成测试（需要 -tags integration）
	$(GO) test -tags integration ./test/integration/...

.PHONY: bench
bench: ## 运行基准测试（无 benchmark 目标时不失败）
	$(GO) test -run '^$$' -bench . -benchmem ./...

.PHONY: cover
cover: ## 生成覆盖率报告
	$(GO) test -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1

.PHONY: vet
vet: ## 静态检查
	$(GO) vet ./...

.PHONY: fmt
fmt: ## 格式化代码
	gofmt -w cmd internal test 2>/dev/null || gofmt -w cmd internal

.PHONY: fmt-check
fmt-check: ## 检查格式（CI 用）
	@out=$$(gofmt -l cmd internal 2>/dev/null); \
	if [ -n "$$out" ]; then echo "以下文件未格式化:"; echo "$$out"; exit 1; fi

.PHONY: check
check: fmt-check vet test ## 提交前的完整检查

.PHONY: smoke
smoke: ## 启动服务并对三个非 S3 端点做冒烟检查
	@./scripts/health-smoke.sh

.PHONY: image
image: ## 构建容器镜像（CONTAINER=podman 可切换引擎）
	$(CONTAINER) build -f deploy/docker/Dockerfile \
		--build-arg GO_IMAGE=$(GO_IMAGE) \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-t $(IMAGE) .

.PHONY: image-run
image-run: ## 前台运行镜像；凭据从当前环境变量传入
	$(CONTAINER) run --rm -p 9000:9000 \
		-e BLINK_ACCESS_KEY -e BLINK_SECRET_KEY \
		-v blink-data:/var/lib/blink $(IMAGE)

.PHONY: k8s-dry-run
k8s-dry-run: ## 客户端校验 K8s 清单（不连接集群、不创建任何对象）
	kubectl apply --dry-run=client -f deploy/k8s/

.PHONY: clean
clean: ## 清理构建与测试产物
	rm -rf $(BIN_DIR) coverage.out