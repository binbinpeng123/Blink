# 发布与兼容性策略

评审状态：待评审

适用版本：v0.6.0 起（首个按本策略发布的版本）
文档状态说明：本文只定义规则与检查项。CI、镜像签名、SBOM、多架构镜像和二进制分发**尚未实现**，
批准后作为 v0.6.0 的发布前置项另行实现；在实现并验证前，任何文档与 PR 都不得声称已具备。

## 1. 目的与范围

本文回答四个问题：

1. 版本号怎么涨、发布物叫什么、变更日志怎么写。
2. 哪些 S3 行为是承诺兼容的，哪些是明确返回 `501` 的。
3. 配置、磁盘数据布局、单机与集群升级时，哪些变更允许、哪些必须拒绝启动或禁止回滚。
4. 一次发布在什么条件下可以发出、什么条件下必须回滚或撤回。

**不在本文范围**：性能门槛（05）、S3 接口契约细节（02）、磁盘布局细节（03）、K8s 部署参数（04）、
安全与运维（06）、分布式一致性与 EC 限制（08）。本文引用这些文档，不重复定义。

## 2. 版本策略

### 2.1 版本号格式

采用 [SemVer 2.0.0](https://semver.org/lang/zh-CN/)：`MAJOR.MINOR.PATCH`，tag 形如 `v0.6.0`。
预发布版本追加 `-rc.N`（如 `v0.6.0-rc.1`），构建元数据不写入 tag，只写入镜像 label 与发布记录。

| 位 | 递增条件 |
| --- | --- |
| MAJOR | 不兼容的对外变更：删除/改名配置键、磁盘 `format_version` 提升且旧二进制无法读取、删除 S3 接口、改变默认安全行为 |
| MINOR | 向后兼容的新接口、新配置项、新能力；**0.x 阶段允许包含不兼容变更**（见 2.2） |
| PATCH | 仅缺陷修复、文档修正、依赖安全更新；不新增行为，不改变磁盘布局 |

同一 MAJOR 内的 PATCH 必须能直接替换二进制并使用原数据目录启动（`format_version` 不变）。

### 2.2 0.x 阶段的额外约定

首个对外版本属于 0.x。0.x 阶段按 SemVer 原文，MINOR 可以包含不兼容变更，但本项目管理上再加三条约束：

1. **破坏性变更必须单独成节**写在 `CHANGELOG.md` 的 `### Breaking Changes` 中，并给出迁移步骤；
   只写在普通条目里视为发布检查不通过。
2. **PATCH 版本永不包含破坏性变更**，与 1.0 之后一致。
3. **数据不可读属于最高级别破坏性变更**：一旦某个版本写出的数据无法被上一版本读取，必须在同一 MINOR
   内提供导出/重建工具或明确要求「升级前备份、降级需恢复备份」。

**进入 1.0 的门槛**（三项同时满足才发 v1.0.0）：

- 01 文档承诺的首个可用版本范围全部实现并通过 05 的验收；
- 磁盘 `format_version` 至少稳定两个 MINOR 未变；
- 至少一个 MINOR 周期内没有配置键被删除或改名。

### 2.3 版本号的来源与注入

- 唯一事实来源是 git tag；构建时 `Makefile` 用 `git describe --tags --always --dirty` 推导
  `VERSION`，并注入 `COMMIT` 与 `BUILD_TIME`。
- 注入路径为 `-ldflags -X`，落在 `internal/buildinfo`：`Version`、`Commit`、`BuildTime`。
  `GET /version` 返回 `{version, commit, build_time, go}`，供发布验证与问题定位使用。
- 未注入或直接从源码 `go run` 时固定为 `dev` / `unknown` / `unknown`。
  **`dev` 不是合法发布版本**：发布检查必须拒绝以 `dev` 构建的镜像。
- 工作区有未提交改动时 `git describe` 带 `-dirty` 后缀，该版本同样不得发布。

### 2.4 相互独立的版本轴

不要用一个数字表达所有兼容性。四个版本轴独立演进：

| 版本轴 | 当前值 | 变更规则 |
| --- | --- | --- |
| 产品版本 | 下一个发布为 `v0.6.0` | 本文 2.1 / 2.2 |
| 磁盘格式版本 | `store.FormatVersion = 1` | 只有旧二进制**无法安全读取**新数据时才 +1；见 6.1 |
| S3 接口范围 | 23 个接口 + 显式 `501` 清单 | 删除接口 = MAJOR；新增接口 = MINOR |
| 配置键集合 | 见 04/06 文档与 `internal/config` 包 | 删除/改名 = MAJOR；新增 = MINOR；见第 5 节 |

因此「v0.6.0」不代表磁盘格式变化，也不代表 API 范围变化；每次发布必须在发布记录中分别列出
产品版本、磁盘格式版本与 API 变更。

### 2.5 版本路线

文档中已使用的切片里程碑为 v0.1.0（核心存储）、v0.2.0（分布式）、v0.3.0（对象级操作）、
v0.4.0（大对象工具）、v0.5.0（安全与运维）；**这些只是评审与开发切片编号，仓库尚未打过任何 git tag**。
本文批准后，**首个按本策略正式发布的是 v0.6.0**，其内容以当时已合入的切片为准；
不为凑版本号提前合并未评审的实现，也不把历史切片补打成正式发布。

## 3. 变更日志与发布物

### 3.1 `CHANGELOG.md`

仓库当前**没有** `CHANGELOG.md`，v0.6.0 发布前必须创建。格式采用
[Keep a Changelog 1.1.0](https://keepachangelog.com/zh-CN/1.1.0/)，与 SemVer 对齐：

```markdown
## [0.6.0] - 2026-XX-XX

### Breaking Changes
- （没有则写“无”）

### Added
### Changed
### Fixed
### Security
### Known Limitations
- 指向 docs/design/known-limitations.md 的当次快照，或列出本次新增限制
```

规则：

- `CHANGELOG.md` 由 PR 作者在功能 PR 中追加到 `## [Unreleased]`，发布时由发布负责人改为版本号与日期。
- 每条目必须能让用户判断「我是否受影响」：写行为变化，不写内部重构。
- 修复类条目若关联安全或数据完整性，必须注明影响的版本范围。
- 不记录未验证的能力。标注「实验性」的条目必须在同一版本内保持默认关闭。

### 3.2 命名规则

| 发布物 | 命名 | 当前状态 |
| --- | --- | --- |
| 源码 tag | `v<MAJOR>.<MINOR>.<PATCH>[-rc.N]` | 规则已定；尚未打过 tag |
| 二进制归档 | `blink_<version>_<os>_<arch>.tar.gz` + `.sha256` | **未实现**（Makefile 只产出本机 `bin/blink`） |
| 容器镜像 | `blink:<version>`，同时打 `blink:<MAJOR>.<MINOR>` 与 `blink:latest` | 仅本地构建；**无镜像仓库推送** |
| 镜像摘要 | `blink@sha256:<digest>` | 发布记录必须记录 digest；当前手工 `docker inspect` 获取 |
| SBOM | `blink_<version>.spdx.json` | **未实现** |
| 签名 | cosign keyless 或维护者密钥 | **未实现** |

`latest` 只允许指向最新的**稳定**版本，不得指向 `-rc`。生产部署必须使用不可变 tag 或 digest；
`deploy/k8s/statefulset.yaml` 中的 `blink:dev` 仅是本地示例，发布检查必须确认清单示例未被误当作生产配置。

### 3.3 发布物清单

一个版本可发布，必须同时具备：

1. 通过 8.1 检查清单的测试报告（命令、输出摘要、硬件与环境）；
2. `CHANGELOG.md` 中该版本条目；
3. 容器镜像的 `repo@sha256:...` 摘要；
4. 部署验证结果（单机容器 + `kubectl --dry-run=client`；真实 K8s 多节点与 08 集群验收按 04/08 的**未验证**现状如实记录）；
5. 更新后的 `docs/design/known-limitations.md` 或确认无新增限制；
6. 发布说明中逐项列出：产品版本、磁盘格式版本、S3 接口变更、配置变更、已知限制。

缺少任一项的版本只能作为内部构建，不得对外称「已发布」。

## 4. S3 API 兼容性矩阵

### 4.1 承诺兼容的接口

以下接口按 02 文档的契约承诺兼容；每条都必须有自动化测试通过（05 验收原则 2：未通过自动化测试不得声明兼容）。

| # | 方法 | 路径 | 操作 | 验证依据 |
| --- | --- | --- | --- | --- |
| 1 | `PUT` | `/{bucket}` | CreateBucket | 单测 + aws-cli smoke |
| 2 | `HEAD` | `/{bucket}` | HeadBucket | 单测 + aws-cli smoke |
| 3 | `DELETE` | `/{bucket}` | DeleteBucket | 单测 + aws-cli smoke |
| 4 | `GET` | `/` | ListBuckets | 单测 + aws-cli smoke |
| 5 | `PUT` | `/{bucket}/{key}` | PutObject | 单测 + aws-cli smoke |
| 6 | `GET` | `/{bucket}/{key}` | GetObject（Range、条件请求） | 单测 + aws-cli smoke |
| 7 | `HEAD` | `/{bucket}/{key}` | HeadObject | 单测 + aws-cli smoke |
| 8 | `DELETE` | `/{bucket}/{key}` | DeleteObject | 单测 + aws-cli smoke |
| 9 | `GET` | `/{bucket}?list-type=2` | ListObjectsV2 | 单测 + aws-cli smoke |
| 10 | `POST` | `/{bucket}/{key}?uploads` | CreateMultipartUpload | 单测 + aws-cli smoke |
| 11 | `PUT` | `/{bucket}/{key}?partNumber=N&uploadId=X` | UploadPart | 单测 + aws-cli smoke |
| 12 | `PUT` | 同上 + `x-amz-copy-source` | UploadPartCopy | 单测 + `internal/cluster` 跨节点测试 |
| 13 | `POST` | `/{bucket}/{key}?uploadId=X` | CompleteMultipartUpload | 单测 + aws-cli smoke |
| 14 | `DELETE` | `/{bucket}/{key}?uploadId=X` | AbortMultipartUpload | 单测 + aws-cli smoke |
| 15 | `GET` | `/{bucket}/{key}?uploadId=X` | ListParts | 单测 + aws-cli smoke |
| 16 | `GET` | `/{bucket}?uploads` | ListMultipartUploads | 单测 + aws-cli smoke |
| 17 | `PUT` | `/{bucket}?cors` | PutBucketCORS | 单测 + `scripts/cluster-smoke.sh` |
| 18 | `GET` | `/{bucket}?cors` | GetBucketCORS | 单测 + `scripts/cluster-smoke.sh` |
| 19 | `DELETE` | `/{bucket}?cors` | DeleteBucketCORS | 单测 |
| 20 | `OPTIONS` | `/{bucket}`、`/{bucket}/{key}` | CORS 预检（认证前处理） | 单测 |
| 21 | `GET` | `/{bucket}?marker=...` 或 `list-type=1` | ListObjects V1 | 单测 |
| 22 | `PUT` | `/{bucket}/{key}` + `x-amz-copy-source` | CopyObject | 单测 + `internal/cluster` 跨节点测试 |
| 23 | `POST` | `/{bucket}?delete` | DeleteObjects（≤1000） | 单测 + aws-cli smoke |

非 S3 端点单独列出，不适用 SigV4：`/healthz`、`/readyz`、`/version`、`/metrics`（06 引入）。

### 4.2 明确不支持的接口

以下必须返回 `501 NotImplemented`，不得落成 404 或 500（清单以 02 文档为准）：

- Bucket 级：`?acl`、`?tagging`、`?lifecycle`、`?encryption`、`?versioning`、`?policy`、
  `?replication`、`?object-lock`、`GET ?versions`；
- Object 级：`?acl`、`?tagging`、`?legal-hold`、`?retention`、`POST ?restore`、`?select`、`GET ?torrent`、
  `?cors`（对象级 CORS，S3 本身无此语义）；
- 多区间 Range（逗号分隔）与任意未知查询参数组合。

**兼容性承诺的边界**：承诺的是「这些接口按 02 契约工作」，不承诺「AWS 全部 API 可用」。
用户依赖未列出接口时，`501` 是明确的、可预期的失败信号。

### 4.3 客户端验证矩阵

| 客户端 | 状态 | 依据 |
| --- | --- | --- |
| AWS CLI 2.36.2 | **已通过** | `scripts/aws-cli-smoke.sh` 12 步全绿（含 presign、sync、手工分片） |
| 任意 SigV4 SDK（协议层） | 应通过 | 按 02 契约与 SigV4 向量测试；未逐个厂商实测 |
| MinIO Client (`mc`) | **待验证** | 开发环境未安装，不得声称已通过 |
| s3fs / rclone | 待验证 | 不在首个版本验收范围 |
| Blink-web（浏览器直连） | **未验证** | 仓库内无 Blink-web 代码，仅 02 文档按该场景设计了 CORS；不得声称已联调 |

### 4.4 已知行为差异

这些不是缺陷而是明确取舍，发布说明必须保留链接到
[`docs/design/known-limitations.md`](../design/known-limitations.md)：

- **对象级 ACL / 桶策略**：未实现。授权模型是凭据文件中的动作级策略（06），与 IAM 语义不同。
- **版本控制**：未实现，覆盖写直接替换；删除不可恢复。
- **ETag**：单次 PUT 为内容 MD5；分片对象为 AWS 风格的分片 ETag，但**不保证跨实现一致**。
- **aws-cli 大对象服务端复制**：`--copy-props default` 会先调用 `GetObjectTagging`（`501`），
  需显式 `--copy-props none`。
- **条件请求**：支持 `If-Match` / `If-None-Match` / `If-Modified-Since` / `If-Unmodified-Since`
  的对象级语义；不承诺跨节点强一致（见 08）。
- **ListObjects V1/V2**：支持 prefix、delimiter、marker/continuation-token、max-keys；
  不承诺与 AWS 在极端 key 排序边界上的逐字节一致（按 UTF-8 字节序实现）。
- **跨节点语义**：条件写、并发协调器为尽力而为，不等价于 AWS 强串行化（08）。

## 5. 配置兼容规则

### 5.1 键与优先级

- 配置来源优先级：**CLI 参数 > 环境变量 > 内置默认值**；同名时高优先级整值覆盖，不做合并。
- 环境变量统一 `BLINK_` 前缀；配置键一旦发布即视为对外契约。
- **未知环境变量与未知 CLI 参数**：环境变量忽略（便于共享环境中携带无关变量）；
  未知 CLI 参数按标准库 `flag` 行为报错退出。发布说明不得依赖「忽略」来实现兼容。

### 5.2 变更规则

| 变更 | 允许的版本位 | 要求 |
| --- | --- | --- |
| 新增配置键 | MINOR | 必须有安全默认值；默认关闭的实验能力需在 CHANGELOG 标注 |
| 修改默认值 | MINOR | 必须在 `### Changed` 中写明旧值→新值及影响；安全相关默认值收紧可放 PATCH，但需显著标注 |
| 删除/改名配置键 | MAJOR（0.x 为 MINOR + Breaking Changes） | 至少保留一个 MINOR 的弃用期：旧键仍生效并输出一次 WARN |
| 删除/改名环境变量 | 同上 | 同上 |
| 改变参数语义（同名不同义） | MAJOR | 禁止静默改变 |

**弃用期从 v0.6.0 起计算**：v0.6.0 之前不存在对外配置承诺，但已写入 04/06 文档的键默认值仍按上表执行。

### 5.3 凭据与安全配置

- 凭据文件是**纯增量**兼容：新增凭据、修改策略、删除凭据都不涉及数据迁移。
  **当前没有热重载**：凭据文件只在启动时读取，变更后必须重启进程；发布说明不得声称「重载即可生效」。
- 新增策略动作或资源范围只做「更细粒度」扩展；收紧默认权限属于安全修复，可放 PATCH，
  但必须在 CHANGELOG 的 `### Security` 中说明。
- **不得**为了兼容旧配置而接受明文弱默认凭据；无凭据时按 06 的既定行为处理。

## 6. 数据布局与升级迁移

### 6.1 `format_version` 规则

磁盘布局版本定义在 `internal/store`：`FormatVersion = 1`。规则：

1. **只有旧二进制无法安全读取新数据时**才提升 `format_version`；仅新增可选字段不算。
2. 启动时发现磁盘 `format_version` **高于**二进制支持的版本 → **拒绝启动并给出明确错误**，
   不得尝试猜测或部分读取。
3. 发现低于当前版本 → 允许启动；若该版本需要就地迁移，必须先完成迁移再对外服务，
   迁移过程必须可中断、可重试、幂等。**当前只有 `format_version = 1`，不存在迁移路径**；
   未来提升版本号时必须同时提交迁移实现，不能只改常量。
4. 提升 `format_version` 的版本必须在 CHANGELOG 的 `### Breaking Changes` 中单独说明，
   并给出备份、迁移与回滚步骤。
5. `meta.json` 中新增的**可选**字段（如 `layout`、`ec`、`parts`）刻意**不**提升 `format_version`：
   旧二进制忽略未知字段即可继续读；`validateReplicaMeta` 只拒绝 `Version < 1` 或 `> FormatVersion`。

### 6.2 升级兼容矩阵

| 场景 | 支持 | 说明 |
| --- | --- | --- |
| 单机：同 MAJOR 内 PATCH 替换 | 支持 | 数据目录不动；回滚到旧 PATCH 同样安全 |
| 单机：MINOR 升级且 `format_version` 未变 | 支持 | 回滚到上一 MINOR 仍可读 |
| 单机：`format_version` 提升 | 有条件 | 升级后**不可回滚**到旧二进制；升级前必须备份 |
| 集群：所有节点同版本 | 支持 | 实现前提；08 文档仍待评审 |
| 集群：滚动升级（混合版本，无 EC） | 有条件 | 仅在 `format_version` 未变、且不启用 EC 时允许；协调者与存储节点都需满足 quorum。**该场景只有 `scripts/cluster-smoke.sh` 的本机多进程验证，未在真实多机集群演练** |
| 集群：混合版本 + EC | **不支持** | 旧二进制读到 EC meta 会判 `ErrCorrupt`；滚动升级期间必须关闭 EC |
| 降级：跨 `format_version` | **不支持** | 只能恢复升级前备份 |

### 6.3 单机升级步骤

1. 记录当前 `/version` 与数据目录路径；确认磁盘剩余空间满足 06 的低水位要求。
2. **备份**：停止写入后复制数据目录（或文件系统快照）。`format_version` 提升的版本必须备份。
3. 停止进程（优雅关闭，等待进行中的 PUT/MPU 结束或超时）。
4. 用新二进制启动，确认 `/version` 为新版本、`/healthz` 正常、`/readyz` 的 `disk` 字段正常。
5. 跑 `scripts/health-smoke.sh` 与最小读写验证（PUT/GET/HEAD/LIST/DELETE）。
6. 观察 `s3_audit` 与 `blink_disk_write_blocked` 至少一个写入周期。

失败时：停机、换回旧二进制、用原数据目录启动（仅当 `format_version` 未提升）；否则恢复备份。

### 6.4 集群升级步骤

前提：集群模式已在代码中实现（`internal/cluster`、`scripts/cluster-smoke.sh`），但 **08 文档仍为「待评审」，
其验收尚未完成**，因此本节步骤在 08 通过评审前只能视为待验证流程，不得声称生产可用。
**混合版本集群不支持 EC**。

1. 升级前确认集群内所有节点版本一致、EC 关闭、无进行中的修复队列。
2. 逐个节点升级，同一时刻只停一个节点；每升级一个节点，等待其重新加入并确认
   `/readyz` 与集群内读写正常后，再升级下一个。
3. 升级期间保持 `W`/`R` quorum 可满足；只剩一个节点时读写返回 `503`，属预期保护。
4. 全部节点升级完成后，再按需重新开启 EC。
5. 任一节点升级失败：立即回滚该节点到旧版本（要求 `format_version` 未变），
   在混合版本状态下**不得开启 EC**，直至所有节点版本一致。

**禁止**：跳过 quorum 直接整集群停机升级；在混合版本期间启用 EC；跨多个 MINOR 跳跃升级而不逐版本验证。

## 7. 镜像、架构与供应链

### 7.1 当前镜像事实

- 多阶段构建，最终层 `scratch`；`CGO_ENABLED=0`，`-tags netgo,osusergo`，纯静态二进制。
- 运行用户 `65532:65532`（非 root），无 shell；排障依赖结构化日志与 `/metrics`。
- 默认 `BLINK_ADDR=0.0.0.0:9000`、`BLINK_DATA_DIR=/var/lib/blink`，`EXPOSE 9000`；
  镜像内**不含任何默认凭据**，凭据必须由运行环境注入。
- 构建参数 `GO_IMAGE` 默认 `golang:1.27-alpine`，可覆盖；`VERSION`/`COMMIT`/`BUILD_TIME`
  由 Makefile 注入。

### 7.2 架构支持

| 平台 | 状态 | 说明 |
| --- | --- | --- |
| linux/amd64 | 编译验证通过 | 主要部署目标 |
| linux/arm64 | 编译验证通过；镜像曾按 04 记录用 Podman 在本机构建 | 边缘设备（Jetson/树莓派类）目标；镜像未发布 |
| darwin/arm64 | 编译验证通过 | 开发机；未跑全量测试 |
| windows/amd64 | 仅编译验证 | 未在 Windows 上运行测试，不得声称支持 |

**多架构镜像（manifest list）尚未实现**：当前 `make image` 只构建宿主架构。
v0.6.0 若要发布 `linux/amd64` + `linux/arm64` 镜像，需先补 `docker buildx` 构建矩阵并在两架构上跑 smoke；
在此之前，发布说明只能写「已在 <arch> 上验证」。

### 7.3 SBOM 与签名（未实现）

目标要求（批准后实现）：

- 每次发布生成 SPDX 或 CycloneDX SBOM，至少覆盖二进制与镜像层；因无第三方 Go 依赖，
  SBOM 主要记录 Go 标准库版本、构建镜像 digest 与构建参数。
- 镜像使用 cosign 签名（keyless 或维护者密钥），发布记录附签名校验命令。
- 发布前校验 `go version` 与 `GO_IMAGE` digest，禁止使用浮动 tag 构建正式版本。

**当前状态：无 SBOM、无签名、无 CI**。仓库没有 `.github/workflows`，所有检查均为本地 `make` 目标手工执行。
在补齐前，发布记录必须写明「供应链未签名」，不得暗示已具备可验证来源。

## 8. 发布前检查清单

发布负责人逐项执行并在 PR/发布记录中附输出摘要。任何一项失败即阻断发布。

### 8.1 代码与测试

- [ ] `make fmt-check` 通过
- [ ] `make vet` 通过
- [ ] `make test` 通过
- [ ] `make test-race` 通过
- [ ] `make test-integration` 可用并通过（需要 `-tags integration`）
  ——**当前该目标已失效**：它指向不存在的 `test/integration/`，执行会直接报
  `lstat ./test/integration/: no such file or directory`（2026-11-08 实测）。发布前必须修复目标或移除该项，
  并改用实际存在的跨节点测试（`internal/cluster` 与 `scripts/cluster-smoke.sh`）作为集群验收依据。
- [ ] `scripts/aws-cli-smoke.sh` 全绿（互操作硬性基准）
- [ ] `scripts/cluster-smoke.sh` 全绿（若本版本包含集群能力）
- [ ] 05 文档要求的性能回归已执行；未达标项按 05 记录客观原因，**不得下调门槛**

### 8.2 构建与部署

- [ ] `VERSION` 来自 tag 且无 `-dirty`；`/version` 返回的 `commit` 与 tag 指向一致
- [ ] `make image` 成功；记录镜像 `sha256` digest
- [ ] `make image-run`（凭据由环境变量注入，不写入文件/镜像）后可完成最小读写
- [ ] `make smoke` 通过
- [ ] `make k8s-dry-run` 通过（仅 `--dry-run=client`，不连接共享集群、不创建对象）
- [ ] 清单中的镜像引用已改为不可变 tag 或 digest（示例 `blink:dev` 不得用于发布）

### 8.3 文档与兼容性

- [ ] `CHANGELOG.md` 已更新，破坏性变更单独成节
- [ ] S3 兼容矩阵（第 4 节）与 02 文档一致；新增/删除接口已反映
- [ ] 配置键变更已按第 5 节标注弃用期
- [ ] `format_version` 是否变化已确认；若变化，迁移与回滚步骤已写入发布说明
- [ ] `docs/design/known-limitations.md` 已更新
- [ ] 本文第 7 节中仍为「未实现」的项已在发布说明中如实列出

### 8.4 集群发布附加项

- [ ] 所有节点版本一致；EC 状态与 08 的要求一致
- [ ] 升级顺序与 quorum 保护已演练（至少在本机多进程 smoke 中验证）
- [ ] 回滚路径已确认：`format_version` 未变才允许回滚二进制

### 8.5 当前已知发布阻塞项（2026-11-08 核实）

以下不是待评审的设计选择，而是仓库当前事实；在 v0.6.0 发布前必须逐项关闭或在发布说明中明确豁免：

| # | 阻塞项 | 证据 | 关闭条件 |
| --- | --- | --- | --- |
| 1 | `make test-integration` 指向不存在的 `test/integration/` | 实测报 `lstat ... no such file or directory` | 修复目标或移除，并以 `internal/cluster` / `cluster-smoke.sh` 作为集群验收 |
| 2 | 无 `CHANGELOG.md`、无 git tag、无 GitHub Release | 仓库文件与 tag 列表 | 创建 CHANGELOG，打 `v0.6.0` tag |
| 3 | 无 CI | 无 `.github/workflows` | 补齐可在干净环境复跑的流水线，或明确接受手工发布并记录执行人 |
| 4 | 无 SBOM、无镜像签名 | 第 7.3 节 | 生成 SBOM 并签名，或由评审人批准延后并在发布说明声明 |
| 5 | 无多架构 manifest | `make image` 只构建宿主架构 | 补 `buildx` 矩阵，或首版只声明单架构 |
| 6 | 真实 K8s 安装/升级/卸载未验证 | 04 文档「验收状态」 | 在有隔离集群后执行，或首版明确不支持 K8s 生产部署 |
| 7 | `mc`、s3fs、rclone 未验证 | 4.3 节 | 实测通过后再写入兼容矩阵 |

## 9. 回滚条件与流程

### 9.1 立即回滚条件

出现任一条即停止发布并回滚：

1. 数据丢失、对象内容不一致或元数据损坏；
2. 启动时对原有数据目录报错，或误判 `format_version`；
3. 认证/授权绕过，或凭据被写入日志/镜像；
4. `scripts/aws-cli-smoke.sh` 或核心读写路径失败率超过 5%；
5. 镜像无法启动、`/healthz` 不通过，或 `/readyz` 持续异常；
6. 磁盘水位保护失效（满盘仍接受写入，或未满盘误拒写入）。

### 9.2 回滚流程

1. 停止继续扩大影响：单机停止写入；集群将流量切到健康副本/上一版本节点。
2. 换回上一版本二进制或镜像 digest；**不得**用旧二进制打开已提升 `format_version` 的数据目录。
3. 启动后验证 `/version`、`/healthz`、`/readyz`，跑最小读写与 `scripts/health-smoke.sh`。
4. 若数据已不可由旧版本读取：停止服务，恢复升级前备份，再启动旧版本。
5. 在 `CHANGELOG.md` 与发布记录中标注该版本已撤回及原因；发布修复版本（PATCH）。

**不重写已推送的 tag**：撤回通过发布说明标注 + 删除/标记镜像 tag 实现；代码修复走新 PATCH 版本。

### 9.3 集群回滚

- 仅当 `format_version` 未变时允许逐节点回滚到上一版本；回滚期间保持 EC 关闭。
- 若已有节点写入了新格式数据，禁止回滚到不识别该格式的版本；只能前滚修复。
- 回滚后必须重新跑 `scripts/cluster-smoke.sh`，确认跨节点读写与修复队列正常。

## 10. 问题响应流程

| 级别 | 定义 | 目标响应 | 修复版本 |
| --- | --- | --- | --- |
| P0 | 数据丢失/损坏、认证绕过、无法启动 | 立即 | 当日 PATCH 或撤回 |
| P1 | 核心接口不可用、满盘保护失效、集群不可读写 | 24 小时内 | 尽快 PATCH |
| P2 | 非核心接口错误、兼容性回归、指标/审计缺失 | 下一 PATCH | 正常节奏 |
| P3 | 文档错误、体验问题 | 下一 MINOR | 随版本 |

流程：

1. 在 issue 中记录：版本、`/version` 输出、复现步骤、影响范围、是否涉及数据。
2. 安全类问题先私下修复，发布 `### Security` 条目后再公开细节。
3. 修复必须附带回归测试；无法自动化的（如真实磁盘 ENOSPC）在 known-limitations 中标注。
4. 发布后在发布记录中关联 issue 与修复版本。

## 11. 与其他文档的关系

- **02**：接口与错误码的事实来源；本文第 4 节只是发布视角的摘要表格。
- **03**：磁盘布局细节；本文只定义 `format_version` 的升级/回滚规则。
- **04**：部署参数与探针；本文引用其清单，不重复。
- **05**：测试与性能门槛；本文 8.1 直接引用，不另设门槛。
- **06**：安全与运维；凭据、审计、指标、水位的行为以 06 为准。
- **08**：集群与 EC 的兼容限制；本文 6.4 的集群升级步骤以其为前提。

## 12. 验收项

本策略生效后，每次发布必须满足：

1. 第 8 节清单全部通过，并在 PR 或发布记录中附证据（命令、输出摘要、镜像 digest）；
   8.5 中的阻塞项若获评审人明确豁免，必须在发布说明中逐条写明豁免理由。
2. 破坏性变更在 `CHANGELOG.md` 与迁移说明中**双重**标识。
3. 发布说明逐项列出：产品版本、磁盘格式版本、API 变更、配置变更、已知限制。
4. 未验证能力（多架构镜像、SBOM、签名、`mc`、s3fs/rclone、Windows 运行）不得写成已支持。
5. 回滚路径在该版本的发布记录中写清楚；`format_version` 提升的版本必须写明「不可降级」。

## 13. 待评审决策点

以下问题需要评审人明确，批准后写入实现：

1. **首个正式版本号**：是否接受 `v0.6.0` 作为本策略下首个发布？
2. **0.x 兼容承诺强度**：是否接受 2.2 的「MINOR 可破坏、PATCH 不可破坏」约定？
3. **发布前置能力**：v0.6.0 是否必须包含多架构镜像、SBOM、签名与 CI？
   本文建议：**CI + 多架构镜像 + SBOM 作为 v0.6.0 前置；镜像签名可延后至 v0.7.0**，
   但在补齐前发布说明必须写明未签名。
4. **二进制分发渠道**：是否需要官方 `blink_<version>_<os>_<arch>.tar.gz`？
   本文建议首版只发布容器镜像与源码 tag，二进制归档延后。
5. **集群 N-1 兼容**：是否要求滚动升级支持「新旧版本混合读写」（当前只在无 EC 时允许）？
   本文建议保持不支持 EC 的混合版本，不承诺更宽范围。
6. **发布节奏**：按切片发布还是按时间窗口发布？本文建议按切片，每次发布必须可独立回滚。
