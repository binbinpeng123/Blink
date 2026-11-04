# Kubernetes / K3s 部署设计

评审状态：已允许开发
评审人：binbinpeng123
评审日期：2026-10-25
适用版本：v0.1.0（单节点，默认）；v0.2.0（集群模式，见"集群部署"）

## 目标

提供适用于单节点边缘设备和标准 Kubernetes 集群的可持久化部署方式。

## 部署形态

| 方案 | 结论 | 理由 |
| --- | --- | --- |
| **StatefulSet，`replicas: 1`** | **采用** | 稳定身份 + 有序滚动升级；单副本 RollingUpdate 等价于"先停旧、再起新"，不会出现两个进程同时写同一份数据 |
| Deployment | 否决 | 默认 RollingUpdate 允许新旧 Pod 重叠；RWO 卷只保证不跨节点，同节点多 Pod 仍可同时挂载，会破坏"单写者"前提 |
| DaemonSet | 否决 | 每个节点各起一个独立实例，彼此不复制，等于制造多份互不一致的数据 |
| StatefulSet，`replicas > 1` | **v0.2.0 起支持（集群模式）** | 需同时设置 `BLINK_CLUSTER_MODE=cluster`、静态 peers 与 `volumeClaimTemplates`；单机模式下调大副本数仍只会让客户端看到不同数据 |

v0.1.0 的服务没有"多副本共享同一份数据"的能力，也没有 leader 选举。**单机模式下把副本数调大不会提高可用性，只会让客户端看到不同数据。** 集群模式的完整拓扑、quorum 与修复语义见 [08 分布式存储设计](./08-distributed-storage.md)。

## 清单内容

| 文件 | 作用 |
| --- | --- |
| `deploy/docker/Dockerfile` | 多阶段构建，运行层为 `scratch`，仅含静态二进制 |
| `deploy/k8s/namespace.yaml` | 独立命名空间 `blink` |
| `deploy/k8s/configmap.yaml` | 非机密配置（监听地址、数据目录、区域、日志级别、fsync 策略） |
| `deploy/k8s/pvc.yaml` | 数据卷声明（RWO） |
| `deploy/k8s/service.yaml` | ClusterIP，端口 9000 |
| `deploy/k8s/statefulset.yaml` | 工作负载、探针、安全上下文、资源限制 |
| `deploy/k8s/examples/secret.example.yaml` | 凭据示例；复制为 `secret.yaml`（已 gitignore）后填入真实值 |
| `deploy/k8s/headless-service.yaml` | **v0.2.0**：`clusterIP: None`，暴露 9000 + 9100，供集群内 peers 互相寻址 |
| `deploy/k8s/networkpolicy.yaml` | **v0.2.0**：仅允许同 namespace 访问 9100 内部端口 |

`examples/` 不在 `kubectl apply -f deploy/k8s/` 的非递归范围内，避免用占位凭据把服务跑起来。

部署顺序：

```bash
cp deploy/k8s/examples/secret.example.yaml deploy/k8s/secret.yaml
# 编辑 secret.yaml 填入真实凭据
kubectl apply -f deploy/k8s/secret.yaml
kubectl apply -f deploy/k8s/
kubectl -n blink rollout status statefulset/blink
```

## 镜像

- 运行镜像基于 `scratch`：没有 shell、包管理器和 CA 包。这是"轻量、少补丁面"的直接结果，代价是**无法 `kubectl exec -- sh` 排查**，必须依赖结构化日志。
- 以非 root（UID/GID `65532`）运行；镜像内预建 `/var/lib/blink` 并由该 UID 持有，命名卷初始化时继承属主。
- 构建时通过 `ldflags` 注入 `VERSION` / `COMMIT` / `BUILD_TIME`；`blink_starting` 日志会输出它们，用于确认线上跑的是哪个构建。
- 生产环境必须使用不可变 tag 或 digest，禁止 `latest`。

```bash
make image                      # 默认 docker
make image CONTAINER=podman     # Docker 守护进程不可用时
# 拉不到 Docker Hub 时指定镜像站：
make image CONTAINER=podman GO_IMAGE=docker.m.daocloud.io/library/golang:1.27-alpine
# 清单默认引用 blink:dev，本地验证时可直接：
make image IMAGE=blink:dev
```

本机验证时 Docker daemon 未运行，且 Docker Hub 不可达，实际使用 Podman + 国内镜像站完成构建。这只是构建环境的差异，不影响镜像内容。

## 存储

- 数据卷：`ReadWriteOnce`，默认 20 GiB，挂载到 `/var/lib/blink`。
- 单机模式使用独立 PVC `blink-data`；**集群模式必须改用 `volumeClaimTemplates`**，每个 Pod 一块独立的卷，不能共享同一 PVC（共享会破坏"每节点各存一份副本"的前提）。
- K3s：内置 `local-path` StorageClass 可用；数据落在节点本地盘。单机模式下**节点故障即数据不可用**；集群模式下副本分布在不同节点，单节点盘故障由副本/EC 兜底，但副本本身仍落在各节点的本地盘上。
- 云上：使用支持 `WaitForFirstConsumer` 的盘类型（如云厂商的 ESSD topology 类），避免 Pod 被调度到无法挂盘的地域。
- 已有本地盘：先手工创建带 `nodeAffinity` 的 PV，再让 PVC 绑定，并用 `nodeSelector` 固定 Pod 所在节点。

**数据生命周期必须明确**：

| 操作 | 数据 |
| --- | --- |
| 删除 StatefulSet / Service / ConfigMap | **保留**（PVC 独立于工作负载；集群模式的 `volumeClaimTemplates` 随 StatefulSet 删除而回收，需显式保留策略） |
| 删除 PVC | **销毁** |
| 删除整个 namespace | **销毁**（包括 PVC） |

卸载前必须先做备份。单节点版本没有复制，也没有跨节点恢复能力。

## 集群部署（v0.2.0）

单机与集群共用同一个二进制，切换只靠配置。集群模式的完整语义见 [08 分布式存储设计](./08-distributed-storage.md)。

| 项 | 集群模式要求 |
| --- | --- |
| StatefulSet | `replicas` 必须与 `BLINK_CLUSTER_PEERS` 中的节点数**完全一致**；改用 `podManagementPolicy: Parallel` |
| 存储 | `volumeClaimTemplates`，每个 Pod 一块独立 RWO 卷；**不得共享 PVC** |
| 服务 | 保留普通 ClusterIP（S3 流量），新增 headless Service 提供稳定 Pod DNS |
| 端口 | 增加 `internal` 9100；`BLINK_INTERNAL_ADDR` 必须与 `BLINK_ADDR` 不同 |
| 身份 | `BLINK_NODE_ID` 取自 downward API `metadata.name`，且必须出现在 peers 列表中 |
| peers | `id@host:port,...`，使用 headless 短名（如 `blink-0.blink-headless:9100`），**必须包含自身** |
| 密钥 | `BLINK_CLUSTER_SECRET` 集群模式必填、≥32 字节，缺失即拒绝启动 |
| 网络 | NetworkPolicy 限制 9100 仅同 namespace 可达；**9100 不得经由 LoadBalancer/Ingress 暴露** |

```bash
# 仅校验清单，不部署
kubectl apply --dry-run=client -f deploy/k8s/
```

**就绪探针不检查 quorum**：见下文探针章节。

**升级约束**：集群模式要求所有节点运行**同一版本二进制**。滚动升级期间必须关闭 EC（`BLINK_EC_ENABLED=false`），因为旧版本二进制无法解释 `layout:"ec"` 的 meta，会按损坏处理。副本模式的小版本滚动升级可以逐 Pod 进行，但升级窗口内跨版本读写的边界语义未验证（见 `known-limitations.md`）。

## 凭据与配置

- 凭据只经 `Secret` 以环境变量注入（`BLINK_ACCESS_KEY` / `BLINK_SECRET_KEY`），ConfigMap 中不得出现凭据。
- 二进制**没有内置默认凭据**：Secret 缺失或值为空时 Pod 启动失败并打印明确原因，不会以空凭据继续服务。
- 凭据在进程启动时读取一次。**轮换凭据需要重启 Pod**；v0.1.0 不支持多组凭据或热加载。

## 探针

| 探针 | 端点 | 语义 |
| --- | --- | --- |
| startupProbe | `/healthz` | 进程是否已开始服务；成功后不再执行 |
| livenessProbe | `/healthz` | 进程存活；**不触碰存储**，避免磁盘抖动导致误杀 |
| readinessProbe | `/readyz` | 是否可接收流量；**磁盘满时仍返回 200**；**集群模式不检查 quorum** |

`/readyz` 在满盘时保持 200 是刻意的：若返回失败，K8s 会把唯一实例摘出 Service，连读取与删除都无法进行，用户就失去了"删数据自救"的途径。满盘只应体现在返回的容量字段与告警上。

集群模式沿用同一原则：`/readyz` **不检查 peers 可达性或 quorum**。若它检查 quorum，在 3 节点挂 2 个时，K8s 会把最后一个仍可读的节点也摘掉，把"部分可用"变成"完全不可用"。读写是否满足 quorum 由请求本身返回 `503 ServiceUnavailable`，不由探针决定。

## 安全上下文

- `runAsNonRoot: true`、`runAsUser/runAsGroup: 65532`、`fsGroup: 65532`（`fsGroupChangePolicy: OnRootMismatch`）。
- `readOnlyRootFilesystem: true`、`allowPrivilegeEscalation: false`、`capabilities.drop: [ALL]`、`seccompProfile: RuntimeDefault`。
- `automountServiceAccountToken: false`：服务不访问 API Server，不需要令牌。
- 所有配置均以文件/环境变量显式提供，不写镜像、不写 ConfigMap 中的机密。

## TLS

TLS 在 v0.1.0 为**可选**，通过挂载 Secret 启用：

```bash
kubectl -n blink create secret tls blink-tls --cert=server.crt --key=server.key
```

随后在 `statefulset.yaml` 中：取消 `volumes` 里 `tls` 段的注释，加入挂载 `/etc/blink/tls`，并设置：

```yaml
env:
  - name: BLINK_TLS_CERT
    value: /etc/blink/tls/tls.crt
  - name: BLINK_TLS_KEY
    value: /etc/blink/tls/tls.key
```

证书与私钥必须成对提供；只给其一时进程**拒绝启动**，不会静默退回明文 HTTP。

**限制**：目前只用自签证书 + 客户端跳过校验验证过；生产 CA、证书轮换、客户端信任链均未验证（见 `known-limitations.md`）。在 06 文档定稿前，启用 TLS 也不代表可以用于生产。

## 资源

默认 `requests: 100m / 128Mi`、`limits: 2 CPU / 1Gi`，只是起步值。

**内存限制需要特别注意**：通过 `sendfile` 读取大对象时，页缓存计入容器 cgroup 的内存用量。限制过小会触发频繁回收，甚至 OOM，表现为"读大文件时延迟抖动"。应按实际对象大小与并发数实测后调整，而不是照抄默认值。

## 升级与回滚

- 升级：更新镜像 tag/digest → `kubectl -n blink rollout status statefulset/blink`。
- 回滚：`kubectl -n blink rollout undo statefulset/blink`，或改回旧镜像。
- 顺序：StatefulSet 单副本滚动升级先终止旧 Pod；SIGTERM 后进程最多等待 10 s 完成在途请求，`terminationGracePeriodSeconds: 30` 留有余量。
- **降级保护**：数据目录中的格式版本若高于当前二进制，启动直接拒绝，避免旧版本写坏新格式数据。

## 验收状态（不得省略）

| 项 | 状态 |
| --- | --- |
| 镜像构建（linux/arm64，Podman + 镜像站） | 已执行，构建出 `scratch` 运行层镜像 |
| 容器以 UID `65532` 启动，数据卷文件属主 `65532:65532` | 已执行 |
| `/healthz` `/readyz` `/version`（版本信息由 ldflags 注入） | 已执行，均 200 |
| S3 建桶 / 上传 / 下载 / 列表 / HEAD（1 MiB） | 已执行，下载内容逐字节一致 |
| 容器重启后数据保留 | 已执行，对象仍可列出并完整下载 |
| `--read-only` 根文件系统 + `--memory 256m` | 已执行，可正常启动并完成写入 |
| `kubectl apply --dry-run=client` 清单校验 | 已执行（5 个对象） |
| 真实 K8s 安装、重启、升级、卸载 | **未执行**（无隔离开发集群，未在共享集群部署） |
| K3s `local-path` 实际绑定行为 | **未执行** |
| 集群清单（headless Service / `volumeClaimTemplates` / NetworkPolicy） | **待 v0.2.0 实现后 `--dry-run=client` 校验**；真实集群**未实测** |
| 多 Pod 跨节点读写 | **本机多进程模拟**验证；**未在真实 K8s 多节点验证** |

本文件最初要求的"在隔离开发集群完成安装、重启、升级、卸载，并验证卸载后数据保留配置"**尚未满足**。在拿到可用的开发机或本地 K3s 之前，不得声称部署验证通过。
