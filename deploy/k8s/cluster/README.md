# Blink 集群模式 Kubernetes 清单

同一份二进制通过 `BLINK_CLUSTER_MODE=cluster` 启用分布式能力。本目录是
v0.2.0 集群形态的完整、自包含清单；单机清单在上一层 `deploy/k8s/`，
两者**互斥**，不要在同一 namespace 里先后应用。

| 文件 | 作用 |
| --- | --- |
| `namespace.yaml` | 命名空间 `blink` |
| `configmap.yaml` | 非机密配置：peers、N/W/R、修复参数、EC 开关 |
| `service.yaml` | 面向 S3 客户端的 ClusterIP，仅 9000 |
| `headless-service.yaml` | `clusterIP: None`，为每个 Pod 提供稳定 DNS，暴露 9000 + 9100 |
| `statefulset.yaml` | 3 副本、`podManagementPolicy: Parallel`、`volumeClaimTemplates`、探针与安全上下文 |
| `networkpolicy.yaml` | 9100 仅同 namespace 可达；9000 保持集群入口可达 |
| `examples/secret.example.yaml` | 凭据示例；复制为 `secret.yaml`（已 gitignore）后填入真实值 |

## 部署

```bash
# 1. 凭据。BLINK_CLUSTER_SECRET 集群模式必填且 >= 32 字节。
cp deploy/k8s/cluster/examples/secret.example.yaml deploy/k8s/cluster/secret.yaml
openssl rand -base64 32          # 把输出填入 BLINK_CLUSTER_SECRET
# 编辑 secret.yaml，填入真实的 ACCESS_KEY / SECRET_KEY / CLUSTER_SECRET

# 2. 依次应用。secret 必须先于 StatefulSet，否则 Pod 会因缺少凭据拒绝启动。
kubectl apply -f deploy/k8s/cluster/namespace.yaml
kubectl apply -f deploy/k8s/cluster/secret.yaml
kubectl apply -f deploy/k8s/cluster/configmap.yaml
kubectl apply -f deploy/k8s/cluster/service.yaml
kubectl apply -f deploy/k8s/cluster/headless-service.yaml
kubectl apply -f deploy/k8s/cluster/statefulset.yaml
kubectl apply -f deploy/k8s/cluster/networkpolicy.yaml

kubectl -n blink rollout status statefulset/blink
```

只校验清单、不连接集群：

```bash
# 有可用开发集群时（仅客户端 dry-run，不会提交对象）：
kubectl create --dry-run=client --validate=false -f deploy/k8s/cluster/
```

没有集群时，`kubectl` 的 `dry-run=client` 仍需要 API 发现信息；可以用
`--cache-dir` 指向一份已有的发现缓存配合一个不会真正连接的 kubeconfig。
本次校验使用的就是这种方式，且没有执行 OpenAPI schema 校验（见下方验收状态）。

`examples/` 不在 `kubectl apply -f deploy/k8s/cluster/` 的非递归范围内，
不会被占位凭据一起应用。

## 必须保持一致的三处配置

| 位置 | 当前值 | 约束 |
| --- | --- | --- |
| `statefulset.yaml` `spec.replicas` | 3 | 必须等于 peers 中的节点数 |
| `configmap.yaml` `BLINK_CLUSTER_PEERS` | `blink-{0,1,2}@...:9100` | 格式 `id@host:port`，必须**包含自身**；id 必须等于 Pod 名 |
| `headless-service.yaml` | `blink-headless` | 必须等于 `statefulset.spec.serviceName`，peers 的 host 部分由此派生 |

启动时集群层会校验：`node-id` 出现在成员表中、ID/地址无重复、`1 <= W,R <= N`、
`W + R > N`、`BLINK_CLUSTER_SECRET >= 32` 字节、内部地址与外部地址不同。
任一项不满足都会**拒绝启动**并在日志中列出全部问题，不会静默退化成单机。

## 扩缩容

v0.2.0 **没有自动再平衡**。成员表是静态的，直接改 `replicas` 会得到一个
不在 peers 里的 Pod（它自己的 `BLINK_NODE_ID` 不在成员表中，启动即失败），
或让 peers 指向不存在的 Pod。

扩容/缩容必须：

1. 停止写入；
2. 离线迁移受一致性哈希变化影响的数据（当前没有内置迁移工具）；
3. 同时修改 `replicas`、`BLINK_CLUSTER_PEERS` 并重新应用。

## 纠删码

默认关闭。3 节点可设 `BLINK_EC_ENABLED=true` 使用默认 2+1；>= 6 节点为 4+2；
4–5 节点没有默认 profile，必须同时显式设置 `BLINK_EC_K` / `BLINK_EC_M`，
否则拒绝启动。只有 >= `BLINK_EC_MIN_SIZE`（默认 32 MiB）的对象走 EC，
小对象继续走副本。EC 不覆盖分片上传（MPU）。

**升级期间必须先关闭 EC**：旧版本二进制无法解释 `layout:"ec"` 的 meta。
全集群升级到同一版本后再重新开启。

## 升级与回滚

- 集群模式要求所有节点运行**同一版本二进制**；副本模式的小版本滚动升级
  可以逐 Pod 进行，但升级窗口内跨版本读写的边界语义未验证。
- 升级：更新镜像 tag/digest 后 `kubectl -n blink rollout status statefulset/blink`。
- `updateStrategy` 为 `RollingUpdate`。StatefulSet 的滚动更新默认逐 Pod
  （逆序）进行，并在每个 Pod Ready 后继续；`podManagementPolicy: Parallel`
  只影响创建/删除，不放宽滚动更新的顺序保证。
- 降级保护：数据目录的格式版本高于当前二进制时启动直接拒绝，避免旧版本
  写坏新格式数据。
- 回滚：`kubectl -n blink rollout undo statefulset/blink`，或改回旧镜像。

## 数据生命周期

| 操作 | 数据 |
| --- | --- |
| 删除 StatefulSet / Service / ConfigMap | **保留**（`volumeClaimTemplates` 生成的 PVC 默认保留） |
| 删除 PVC（`data-blink-0` 等） | **销毁该节点数据** |
| 删除 namespace | **销毁全部数据** |

删除整个 StatefulSet 前先确认 PVC 保留策略；卸载前必须备份。

## 网络与安全

- 9100 只应由集群内节点互访。`networkpolicy.yaml` 限制为同 namespace；
  不要为 9100 创建 NodePort / LoadBalancer / Ingress。
- 内部通信是明文 HTTP + HMAC-SHA256（共享密钥），不做 mTLS。它依赖网络隔离，
  不得跨不可信网络部署。HMAC 防篡改与重放，不防窃听。
- `NetworkPolicy` 需要支持它的 CNI。**K3s 默认的 flannel 不执行
  NetworkPolicy**：在这样的集群上，`networkpolicy.yaml` 可以成功创建，
  但不会产生实际隔离效果。需要使用 Calico/Cilium 等支持 NetworkPolicy 的 CNI，
  或由下层网络保证 9100 不可达。
- 运行层镜像基于 `scratch`，没有 shell，`kubectl exec -- sh` 不可用；排查依赖
  结构化日志。
- 容器以 UID/GID `65532` 非 root 运行，根文件系统只读，仅数据卷可写。

## 探针

`startupProbe` / `livenessProbe` 使用 `/healthz`（不触碰存储）；`readinessProbe`
使用 `/readyz`。`/readyz` **不检查 quorum**：2/3 节点故障时若把最后一个仍可读
的节点摘出 Service，用户会彻底失去读取与删除自救的途径。读写是否满足 quorum
由请求本身返回 `503`。

## 验收状态（诚实标注）

| 项 | 状态 |
| --- | --- |
| `kubectl create --dry-run=client --validate=false` 清单校验 | 已执行（离线，6 个对象；仅 API 识别与渲染，**未做 schema 校验**） |
| 真实 Kubernetes / K3s 集群安装、滚动升级、卸载 | **未执行**（无隔离开发集群；未在共享生产集群部署） |
| headless DNS 实际解析 | **未执行** |
| `volumeClaimTemplates` 实际绑定与 `local-path` 行为 | **未执行** |
| NetworkPolicy 实际生效（含 K3s flannel） | **未执行** |
| 多 Pod 跨节点读写 | 本机多进程模拟验证；**未在真实 K8s 多节点验证** |

在拿到隔离集群并完成上述验证之前，不得声称本目录的清单已在真实集群部署通过。
