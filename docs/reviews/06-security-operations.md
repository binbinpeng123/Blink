# 安全与运维设计

评审状态：已允许开发
评审人：binbinpeng123
评审日期：2026-11-08
适用版本：v0.5.0
关联分支：`feature/ai-native-core-12-security-ops`

## 背景

Blink 目前的鉴权只有一组来自 `--access-key` / `--secret-key`（或 `BLINK_ACCESS_KEY` / `BLINK_SECRET_KEY`）的静态凭据，校验通过后**任何持有该凭据的客户端都能读写全部桶与对象**。同时：

| 缺口 | 现状 | 影响 |
| --- | --- | --- |
| 多凭据与最小权限 | `sigv4.StaticProvider` 已支持多组凭据，但配置层只能注入一组，且没有授权模型 | 给 CI、推理节点、运维脚本分发同一把全权限密钥；泄露即全量失守，无法按桶/前缀收敛 |
| 凭据来源 | 只支持环境变量/命令行；命令行会出现在进程列表与 shell 历史 | 多凭据部署只能靠外部注入，缺少可审计的静态配置文件 |
| 审计日志 | 只有 `http_request`（状态码/字节数/耗时） | 无法回答"谁在什么时候对哪个对象做了什么"，也无法区分读/写/删除 |
| 指标 | 无 Prometheus 端点 | 边端无法接入现有监控；容量、鉴权失败、授权拒绝都不可观测 |
| 磁盘水位 | `HighWatermark` / `LowWatermark` / `MaxBucketCount` 已在配置中定义并校验，但**从未被 store/s3api 使用** | 磁盘写满后才发现；ENOSPC 只能靠系统调用失败兜底，无法提前止损 |
| `/readyz` 容量信息 | 恒返回 `{"status":"ok","ready":true}` | 运维无法从探针判断容量风险，只能登录机器 `df` |
| 集群满盘语义 | 内部端口把 `syscall.ENOSPC` 归为 500 | 协调者把"节点满盘"当成内部故障，客户端拿不到可行动的 507 |

本切片把"安全"与"可观测/容量保护"合并交付：它们共享同一批中间件与启动装配，拆开会导致两次改 `dispatch`/`Handler`/`main`，且授权拒绝与鉴权失败需要同一条指标出口。

## 调研结论

调研时间 2026-11-08。**当前网络无法访问 `docs.aws.amazon.com`，阿里云/火山引擎的 IAM 文档也无法逐页在线核对。** 下表基于公开 API 契约与这些系统在社区中的通行行为整理，实现后以本机端到端测试为准；未实测的部分在"仍未验证"中明确标注。

| 系统 | 与本切片相关的设计 | 借鉴 / 不借鉴 |
| --- | --- | --- |
| AWS S3 / IAM | 策略文档按 `Action` + `Resource`（`arn:aws:s3:::bucket/prefix*`）授权；桶级与对象级动作分离；`ListBuckets` 只返回有权限的桶；拒绝统一 `403 AccessDenied` 且不泄露资源是否存在 | **借鉴**动作/资源/前缀模型与"拒绝不泄露存在性"；**不引入**完整 IAM JSON、条件键、显式 Deny 优先级 |
| MinIO | 根凭据来自环境变量；`mc admin user add` 创建子用户并绑定 policy；审计走 webhook；Prometheus 端点默认需要鉴权或放在内网 | **借鉴**"根凭据保留全权限 + 子用户按策略收敛"的分层；**不引入**外部 webhook 审计（边端没有接收端） |
| 阿里云 OSS | RAM policy 支持 bucket/prefix 资源与动作集合；访问日志可投递到目标桶；监控走云监控 | **借鉴**前缀级资源授权；**不引入**日志投递到自身桶（会把审计数据与业务数据混在一起） |
| 火山引擎 TOS | IAM policy + 桶策略；提供用量与 QPS 指标 | **印证**容量与请求指标是最小可用监控集 |
| JuiceFS / Nydus 等边端存储 | 单二进制、配置以文件/环境变量注入，尽量减少外部依赖；Kubernetes 以 Secret 挂载配置文件 | **借鉴**静态凭据文件 + 挂载 Secret 的部署方式；不引入外部身份服务 |
| Kubernetes Secret 挂载 | 卷内文件通常是 `..data` 符号链接，默认权限 `0644`，需显式 `defaultMode` | **借鉴**接受符号链接；权限过宽时**拒绝启动**并提示设置 `defaultMode: 0400` |

**结论**：在边端不引入外部 IAM/OIDC 依赖，采用"环境变量全权限凭据（兼容既有部署）+ 只读凭据文件（多凭据、必须显式 rules）"的两层模型。授权只做动作 + 桶 + 前缀，不引入资源级 ACL；这是本版本明确的能力边界，写入 known-limitations。

## 本切片范围

### 1. 多凭据与最小权限

新增 `BLINK_CREDENTIALS_FILE`（命令行 `--credentials-file`），JSON 格式：

```json
{
  "credentials": [
    {
      "name": "ci",
      "access_key": "blink-ci",
      "secret_key": "……",
      "rules": [
        {"bucket": "models", "prefix": "llama/", "actions": ["read", "write"]},
        {"bucket": "artifacts", "actions": ["read", "list"]}
      ]
    }
  ]
}
```

| 项 | 决定 |
| --- | --- |
| 与既有环境变量凭据的关系 | `BLINK_ACCESS_KEY`/`BLINK_SECRET_KEY` 继续有效，**拥有全部权限**，保证 v0.4.0 及更早部署升级后行为不变 |
| 文件凭据权限 | 文件凭据**必须**显式给出非空 `rules`；缺失即拒绝启动，绝不默认全权限 |
| 文件大小 | ≤ 1 MiB；超过拒绝启动 |
| 文件权限（Unix） | 不得含 group/other 位（`mode & 0o077 == 0`，即 `0400`/`0600`）；K8s Secret 必须设置 `defaultMode: 0400` |
| 符号链接 | 允许（兼容 K8s `..data` 挂载），但权限检查作用于解析后的目标文件 |
| 未知字段 | `DisallowUnknownFields`；拼错的字段名必须暴露，而不是静默得到空 secret |
| 重复 Access Key | 与文件内其他凭据或环境变量凭据重复 → 拒绝启动（否则后者覆盖前者，权限语义不可预测） |
| 空 `access_key` / `secret_key` | 拒绝启动 |
| Secret 长度 | ≥ 16 字节（SigV4 的 HMAC 密钥下限；短 secret 可被离线穷举） |
| 凭据名称 | 仅用于日志/审计中的可读标识，不参与鉴权；可空 |
| 日志 | 只输出文件路径与凭据**数量**；secret 永远不进日志、错误信息或 `Redacted()` |

授权模型（新增 `internal/authz`）：

- `Action`：`read`、`write`、`delete`、`list`、`admin`。
- `Rule{Bucket, Prefix, Actions}`；`Bucket` 支持 `*`（全部桶），`Prefix` 为空表示桶内全部 key。
- `list` 与 `admin` 只允许 `Prefix == ""`（构造策略时校验）：列举与桶管理没有"前缀下的桶"这种语义。
- 动作映射：
  | 请求 | 动作 |
  | --- | --- |
  | `GET`/`HEAD` 对象、`GET ?uploadId`（ListParts）、复制/分片复制的**源**对象 | `read` |
  | `PUT` 对象、Create/UploadPart/Complete/Abort MPU、复制/分片复制的**目标** | `write` |
  | `DELETE` 对象 | `delete` |
  | `HEAD` 桶、`GET /{bucket}`（ListObjects）、`GET ?uploads` | `list` |
  | Create/Delete 桶、`?cors` 的 PUT/GET/DELETE | `admin` |
  | `POST /{bucket}?delete` | 请求级按 `delete` 检查桶，随后**逐 key** 检查 |
  | `GET /`（ListBuckets） | 不过滤动作，按"该凭据是否有任何规则可见该桶"过滤结果 |
- 拒绝统一 `403 AccessDenied`，日志 `s3_authz_denied`，**不区分**"桶不存在""key 不存在""无权限"。
- 环境变量凭据不进入策略表，视为 `allowAll`；文件凭据的每个 Access Key 都有显式策略。若签名通过但策略表查不到对应 AK（例如 Provider 被外部替换），按拒绝处理，绝不默认放行。

### 2. 审计日志

在鉴权成功之后、业务 handler 之前输出单行 JSON `s3_audit`：

| 字段 | 说明 |
| --- | --- |
| `action` | 本次请求被判定的动作；无法判定时为空 |
| `access_key` | 脱敏后的 AK（保留前 4 位） |
| `bucket` / `key` | 资源；key 按原样记录（审计需要），但**不记录** query string |
| `status` / `bytes` / `elapsed` | 响应结果 |
| `request_id` / `remote` | 与 `http_request` 对照 |
| `presigned` | 是否为预签名请求 |

**不记录**：请求体、`Authorization` 头原文、任何 `X-Amz-*` 签名参数、query string（预签名 URL 的签名与临时凭据就在 query 里）、Secret Key。

### 3. Prometheus 指标

新增 `GET /metrics`，`Content-Type: text/plain; version=0.0.4; charset=utf-8`：

| 指标 | 类型 | 标签 |
| --- | --- | --- |
| `blink_http_requests_total` | counter | `method`、`status` |
| `blink_http_response_bytes_total` | counter | `method` |
| `blink_http_request_duration_seconds` | histogram | `method` |
| `blink_auth_failures_total` | counter | `code`（S3 错误码） |
| `blink_authz_denied_total` | counter | `action` |
| `blink_disk_write_blocked` | gauge | 0/1 |
| `blink_disk_total_bytes` / `blink_disk_free_bytes` / `blink_disk_used_ratio` | gauge | 无 |
| `blink_credentials_total` | gauge | 无 |
| `blink_uptime_seconds` | gauge | 无 |
| `blink_build_info` | gauge=1 | `version`、`commit` |

- `/metrics` 加入免签白名单：Prometheus 抓取器通常不带 SigV4。**代价是任何能连上端口的人都能看到运行指标**，因此必须在 NetworkPolicy / 反向代理层限制来源；文档与启动日志都要写明。指标中不含 key、桶名、凭据。
- 标签基数受控：只使用有限集合（HTTP 方法、状态码、错误码、动作），不使用 bucket/key/request_id。

### 4. `/readyz` 容量字段

响应扩展为：

```json
{
  "status": "ok",
  "ready": true,
  "disk": {
    "available": true,
    "total_bytes": 994662584320,
    "free_bytes": 812345678901,
    "used_ratio": 0.1832,
    "high_watermark": 0.90,
    "low_watermark": 0.85,
    "write_blocked": false
  }
}
```

- 仍恒返回 `200`（已确认决策：容量不作为未就绪条件，否则 K8s 会摘除唯一实例，用户无法删除数据自救）。
- `statfs` 失败时 `available:false`，其余数值为 0，`ready` 仍为 `true`；错误只写日志。
- `write_blocked` 反映当前水位阻断状态（见下节）。

### 5. 磁盘高/低水位保护

- `store.Options` 新增 `HighWatermark`、`LowWatermark` 与 `DiskSpace`（`func(path string) (total, free uint64, err error)`），默认实现用 `syscall.Statfs`；非 Unix 平台返回错误并降级为 fail-open。
- 写入入口拦截：`PutObject`、`UploadPart`、`PutBlock`（覆盖内联小对象与集群副本）。命中水位返回 `store.ErrStorageFull`。
- 带滞回：已用率 `>= HighWatermark` 进入阻断；`<= LowWatermark` 才恢复，避免在阈值附近抖动。
- 检查结果缓存约 1 秒：每次 PUT 都 `statfs` 在边端是额外系统调用；1 秒的滞后远小于填满磁盘的时间。
- `statfs` 失败 **fail-open**（放行）：监控失败不应让可写的盘拒绝写入；真正写满时 `ENOSPC` 仍会映射为 507。
- `ErrStorageFull` → `507 InsufficientStorage`（S3 层与集群内部端口都映射）。
- 集群：`distributeBlock` / `distributeECBlock` 记录失败原因；当存活副本低于 W（或成功分片低于 K）且失败以满盘为主时，向协调者返回 `ErrStorageFull`，由协调者映射为 507；否则仍返回 503。
- 读、删除、List、健康检查**不受水位影响**——这是"满盘仍可自救"的前提。
- `MaxBucketCount` 本切片**不启用**：桶数量上限需要与删除、并发创建、集群元数据一致性一起设计，单独做会在集群模式产生不一致的软上限。保留配置字段并在 known-limitations 注明未生效。

### 6. TLS、内部通信与备份恢复（运维契约）

| 项 | 本版本契约 |
| --- | --- |
| TLS 终止 | 支持 Blink 直接终止（`--tls-cert`/`--tls-key`，TLS ≥ 1.2）；也支持前置 Ingress/反代终止。两种方式都必须显式配置，默认仍是 HTTP |
| 证书来源 | 运维负责；本版本不做 ACME 自动签发与轮换。建议由 K8s Secret/证书管理器挂载并以滚动重启生效 |
| 内部端口 | 9100 为明文 HTTP + HMAC 共享密钥；只能部署在内网/同 namespace，跨不可信网络必须加隧道或反代（既有 known-limitation） |
| 备份 | 数据目录自包含（`bucket.json`、`meta.json`、CAS 块、`disk.json`）。一致性备份要求**先停止写入**（SIGTERM 优雅关闭）或对数据目录做文件系统快照；不支持在线增量备份工具 |
| 恢复 | 停止进程 → 用备份替换整个数据目录 → 启动；启动清扫会处理 `tmp/` 孤儿。恢复后建议跑一次显式校验读或 `ScrubObject` 抽样 |
| 升级 | 先备份/快照，再替换二进制重启；数据格式向后兼容由 07 文档声明。回滚要求新版本未写入旧版本不认识的新字段；本切片的凭据文件与 stores 水位参数均为纯增量 |
| 日志保留 | 只写 stderr，单行 JSON；轮转与保留由 systemd/journald、容器 runtime 或日志采集器负责，本版本不内置轮转 |
| 告警（建议，不内置规则） | `507` 速率 > 0、`blink_disk_write_blocked == 1`、`blink_disk_used_ratio > 0.9`、`blink_auth_failures_total` 持续增长、`/readyz` 不可达 |

## 明确不做

- 外部身份源（STS、OIDC、LDAP、K8s ServiceAccount 投射）与动态凭据。
- 完整 IAM policy JSON、显式 Deny、条件键、资源级 ACL、桶策略、对象归属（owner）。
- 凭据自动轮换、secret 加密存储（KMS/Vault）；凭据文件本身是明文，安全边界由文件权限与挂载 Secret 提供。
- 审计日志外发（webhook/Kafka/SIEM）、防篡改存储、日志内置轮转。
- 自动备份、PITR、跨集群复制、升级编排。
- mTLS 内部通信、证书自动签发。
- 按凭据限流/配额；`MaxBucketCount` 的强制执行。
- 07 文档的版本兼容矩阵与发布检查清单（另行评审）。

## 验收标准

### 功能

1. 仅环境变量凭据启动：既有全部测试通过；对象读写行为与 v0.4.0 一致。
2. 凭据文件：
   - 合法文件 + 环境变量并存：两组凭据都能完成签名请求；
   - 文件凭据仅有 `models/llama/` 的 `read,write`：可 PUT/GET 该前缀；GET 其他 key、DELETE、ListBuckets 中不可见桶均 `403 AccessDenied`；
   - `list`/`admin` 规则带前缀 → 启动失败；
   - 空 rules、重复 AK、未知字段、权限 `0644`、超过 1 MiB → 启动失败且错误信息指明原因（不含 secret）。
3. `POST ?delete` 混合 key：有权限的删除成功，无权限的逐项 `AccessDenied`，HTTP 200。
4. `CopyObject`/`UploadPartCopy`：无源对象 `read` 权限 → `403`，且不执行复制。
5. `/metrics` 返回 200 与合法 Prometheus 文本；`http_requests_total`、`authz_denied_total` 随请求增长；不出现 key/桶名/secret。
6. `/readyz` 含 `disk` 字段；`statfs` 正常时数值合理，`write_blocked` 与实际水位一致；仍为 200。
7. 水位：
   - 构造已用率 ≥ 高水位 → PUT/UploadPart 返回 `507`，GET/DELETE/List 正常；
   - 回落到低水位以下 → 写入恢复；
   - `statfs` 故障 → 写不阻断（fail-open）。
8. 集群内部 `ErrStorageFull` → `507`；副本不足且非满盘 → `503`。
9. 审计日志：每个已签名 S3 请求一条 `s3_audit`，含动作/脱敏 AK/资源/状态；**不含** Authorization、query string、secret。

### 性能与回归

- 全量 `go test ./...` 通过；新增路径跑 `-race`。
- 水位检查在缓存有效期内不产生额外 `statfs`；对既有 `PutObject` 基准无可测量的吞吐退化。
- 不调整 05 文档的任何性能门槛。

### 测试

- `internal/authz`：规则匹配、通配桶、前缀边界（`llama/` 不匹配 `llamax/`）、非法规则。
- `internal/config`：凭据文件解析与全部拒绝分支（含权限位，Windows 跳过）。
- `internal/metrics`：Prometheus 文本格式、并发计数。
- `internal/store`：水位滞回、缓存、fail-open、恢复。
- `internal/s3api`：授权矩阵、DeleteObjects 逐 key、复制源、ListBuckets 过滤、metrics、readyz。
- `internal/cluster`：内部错误映射与满盘传播。
- 端到端：aws-cli 双凭据（受限凭据被拒/放行）、抓取 `/metrics`、`/readyz`、审计日志字段检查。

## 风险与回滚

| 风险 | 缓解 |
| --- | --- |
| 授权检查漏掉某条路由导致越权 | 检查放在 `dispatch` 的桶/key 解析之后、所有子分派之前；复制源与 DeleteObjects 在 handler 内补查；用表驱动测试覆盖全部方法×子资源组合 |
| 环境变量凭据全权限被误用于生产 | 启动日志输出凭据数量与"full-access 凭据存在"警告；文档要求生产使用文件凭据；后续可增加 `--require-least-privilege` |
| 凭据文件权限过严导致 K8s 启动失败 | 错误信息直接给出 `defaultMode: 0400` 修复建议；已在文档与 known-limitations 说明 |
| 水位误判阻断写入 | 滞回 + 1s 缓存 + statfs 失败 fail-open；读/删除不受影响；507 可被监控捕获 |
| `/metrics` 匿名暴露内部信息 | 指标不含敏感数据；文档要求 NetworkPolicy/反代限制；后续可加独立监听地址 |
| 审计日志泄露签名/URL | 明确白名单字段；测试断言日志中不出现 `Authorization`、`X-Amz-Signature` |
| 全量回归失败 | 授权/instrumentation 均可由配置关闭：无凭据文件时行为与旧版一致；`Capacity` 为 nil 时 readyz 省略 disk；`Metrics` 为 nil 时不注册 `/metrics` |

回滚：删除凭据文件与 `--credentials-file` 参数即回到单凭据全权限模式；水位可通过 `--disk-high-watermark` 调到 1.0 等价关闭（仍保留 statfs 观测）；代码回滚只需替换二进制，无数据格式变更。

## 实现与验证结果

实现日期：2026-11-08，分支 `feature/ai-native-core-12-security-ops`。

### 实现范围

| 模块 | 内容 |
| --- | --- |
| `internal/authz` | 规则模型与匹配（桶名支持 `*` 通配；key 前缀按 `/` 边界；`list`/`admin` 禁止带前缀）、`Allow`、`VisibleBucket`、`AllowAll` |
| `internal/config` | `BLINK_CREDENTIALS_FILE` / `--credentials-file` 解析：1 MiB 上限、`DisallowUnknownFields`、单 JSON 值、secret ≥ 16 字节、重复 AK 拒绝、非 Windows 平台权限位 `mode&0o077 != 0` 拒绝；与环境变量凭据合并；`CredentialCount()` |
| `internal/s3api/authz.go` | `classifyAction`（方法 × 子资源的动作判定）、`authorizeRequest`、`authorizeSource`（复制源读权限）、`errAccessDenied` |
| `internal/s3api/audit.go` | `withAudit` 中间件：已验证身份 + 动作 + 资源 + 结果，单行 `s3_audit` |
| `internal/metrics` + `internal/s3api/metrics.go` / `handler_metrics.go` | 指标注册表（标签基数受控）与 `GET/HEAD /metrics` |
| `internal/store/disk.go` / `disk_unix.go` / `disk_other.go` | `statfs` 容量探测、1s 缓存、高低水位滞回、`ErrStorageFull`、非 Unix fail-open |
| `internal/cluster` | 内部端口 `ErrStorageFull → 507`、`ErrUnavailable → 503`；`isStorageFull` 识别本地与 `PeerError{507}`；协调者在存活副本 < W 且失败含满盘时归一为 `ErrStorageFull` |
| `cmd/blink` | 凭据文件装配、`credentials_loaded` / `env_credential_full_access` / `metrics_endpoint_anonymous` 启动日志、容量注入 |
| 健康检查 | `/readyz` 增加 `disk` 字段；容量不可用时数值置 0、水位照常回显；恒 200 |

实现过程中实测到并修复两个缺陷：

1. **审计日志重复 `remote` 键**：`withRequestContext` 已把 `remote` 注入请求 logger，`withAudit` 又追加一次。slog 的 JSON handler 不去重，不同解析器对重复键的取值可能不同。已删除审计中重复属性，并在测试中断言每条审计日志中 `remote`、`status`、`action`、`access_key` 各恰好出现一次。
2. **根路径 ListBuckets 的审计 `action` 为空**：`classifyAction` 对根路径返回 `ok=false`（结果按可见桶过滤，不按单一动作授权），审计层未回填动作。已在审计层补 `action=list`，未改动 `classifyAction` 的授权语义（否则会误触 `Allow(list, "", "")` 而拒绝 ListBuckets）。

### 自动化验证

- `gofmt -l .`：无输出。
- `go vet ./...`：通过。
- `go test ./...`：全部 `ok`（authz / checksum / cluster / config / ec / logx / metrics / s3api / s3err / sigv4 / store）。
- `go test -race`：`s3api`、`store`、`config`、`authz`、`metrics`、`cluster` 全部 `ok`。

新增测试（共 41 个顶层函数，含子用例）：

| 包 | 测试 |
| --- | --- |
| `internal/authz`（7） | `TestAllowMatrix`、`TestWildcardBucket`、`TestBucketScopedCheck`、`TestAllowAll`、`TestNilPolicyDenies`、`TestInvalidRules`、`TestVisibleBucket` |
| `internal/config`（8） | `TestCredentialsFileLoads`、`TestCredentialsFileFromEnv`、`TestCredentialsFileCombinesWithEnvCredential`、`TestCredentialsFileRejectsLoosePermissions`、`TestCredentialsFileRejectsBadJSON`、`TestCredentialsFileRejectsDuplicateWithEnv`、`TestCredentialsFileRejectsOversize`、`TestCredentialsFileUnpairedEnvCredential` |
| `internal/metrics`（4） | `TestWriteTextFormat`、`TestConcurrentUpdates`、`TestLabelEscaping`、`TestWrongLabelCountIgnored` |
| `internal/store`（4） | `TestWatermarkBlocksAndRecovers`（含滞回）、`TestWatermarkFailsOpenOnStatError`、`TestWatermarkDisabled`、`TestWatermarkOptionValidation` |
| `internal/s3api`（10 + 5） | 授权：`TestClassifyAction`（18 个子用例，覆盖桶/对象/分片/批量删除/CORS 的方法矩阵）、`TestPolicyDeniesReadOutsidePrefix`、`TestPolicyDeniesWriteAndDelete`、`TestListBucketsFiltersInvisible`、`TestDeleteObjectsMixedPermissions`、`TestCopySourceRequiresRead`、`TestUploadPartCopyRequiresRead`、`TestUnknownCredentialIsDenied`、`TestAuditLogHidesSecrets`、`TestStorageFullMapsTo507`；可观测性：`TestMetricsEndpoint`、`TestMetricsAnonymous`、`TestMetricsNilRegistry`、`TestReadyzDiskFields`、`TestReadyzDiskUnavailable` |
| `internal/cluster`（3） | `TestWriteStorageFullWhenAllReplicasFull`（3 节点 + 注入满盘读数，端到端断言 `ErrStorageFull`）、`TestIsStorageFull`、`TestInternalStorageErrorStatus` |

### 端到端实测（aws-cli v2，单机 127.0.0.1:9099）

临时凭据文件写于 `/tmp`（`chmod 600`，未进入仓库；AK/SK 为测试用随机值，未硬编码进代码）：

- **管理员**：create-bucket、put/get、list-buckets 正常。
- **受限凭据**（仅 `models/llama/` 的 `read,write`）：
  - 前缀内 GET 200；前缀内 PUT 成功；
  - 前缀外 GET / PUT → `403 AccessDenied`；
  - 无 `delete` 动作 → DELETE → `403 AccessDenied`；
  - CreateBucket → `403 AccessDenied`；
  - ListBuckets 仅返回 `models`（不可见桶被过滤）。
- **混合权限 `POST ?delete`**（仅对 `llama/` 有 delete）：HTTP 200，`Deleted=[llama/del-me.txt]`，`Errors=[other/keep-me.txt → AccessDenied]`；随后 HEAD 校验前者 404、后者 200 —— 拒绝项确实未被删除。
- **`/metrics`**：匿名 GET 200，`Content-Type: text/plain; version=0.0.4; charset=utf-8`；含 `blink_http_requests_total`、`blink_authz_denied_total{action=...}`、`blink_disk_*`、`blink_credentials_total`、`blink_uptime_seconds`、`blink_build_info{version,commit}`。
- **`/readyz`**：200，`disk` 全字段返回（`available=true`、`total_bytes`/`free_bytes`/`used_ratio`、`high_watermark=0.9`、`low_watermark=0.85`、`write_blocked=false`）。
- **审计日志**：每个已签名 S3 请求一条 `s3_audit`，字段齐全；对全量日志统计 `secret` 与 `X-Amz-Signature` 出现次数均为 0；`"remote":` 在每条审计日志中恰好 1 次。

### 性能与回归

master worktree 与当前分支各跑 20 次 1 MiB `PutObject` 基准（同机、同参数）：

| 版本 | ns/op | 吞吐 | allocs/op |
| --- | --- | --- | --- |
| master | 21.60 ms | 48.55 MB/s | 87 |
| 当前分支 | 20.82 / 20.70 ms | 50.37 / 50.66 MB/s | 87–88 |

无可测量的吞吐退化（差异在噪声范围内）；水位检查命中 1s 缓存，不产生额外 `statfs`。05 文档的性能门槛未做任何调整。

### 未验证项与偏差（如实记录）

- **真实磁盘跨越水位未做 e2e**：受"本机压测总写入 ≤ 512 MiB"约束，没有通过真实填盘触发阻断/恢复；水位阻断、滞回、fail-open 由注入 `DiskSpace` 读数的单元测试覆盖（`internal/store/disk_test.go`、`internal/s3api/authz_test.go`、`internal/cluster/storage_full_test.go`）。
- **真实 ENOSPC 的 507 未做 e2e**：同上，由 `store.ErrStorageFull` 注入路径的单测与集群链路测试覆盖。
- **MinIO `mc` 客户端兼容性未测**：本切片只验证了 aws-cli v2。
- **TLS 直连未做 e2e**：`--tls-cert/--tls-key` 的既有路径未在本切片改动，未回归。
