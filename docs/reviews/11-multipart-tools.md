# 大对象工具：UploadPartCopy 与分片下载

评审状态：实现完成，待评审（2026-11-08）
评审人：待定
评审日期：待定
适用版本：v0.4.0
关联分支：`feature/ai-native-core-11-multipart-tools`

## 背景

切片 10 已交付 CopyObject、DeleteObjects 与 ListObjects V1。对照 AWS S3、阿里云 OSS、火山引擎 TOS 与 MinIO 后，**大对象**仍有两处直接影响互操作性的缺口：

| 缺口 | 主流行为 | 当前 Blink | 影响 |
| --- | --- | --- | --- |
| `UploadPartCopy` | 通过分片上传复制大对象/区间，单分片 ≤ 5 GiB | 未实现；`x-amz-copy-source-range` 一律 `501` | `aws s3 cp s3://… s3://…` 对超过 `s3.multipart_threshold`（默认 8 MiB）的对象**默认失败**（切片 10 已实测） |
| 分片下载 | `GET`/`HEAD` `?partNumber=N`，回 `206` 与 `x-amz-mp-parts-count` | `501` | 无法按分片校验/续传；部分推理框架按 part 并行拉取模型时直接失败 |

两项都属于"大对象工具"：一个负责把大对象**写进**分片会话，一个负责按分片**读出**。它们共享同一套分片编号与边界语义，合并在一个切片实现可以避免两次改动 `ObjectMeta` 与路由。

本切片不改变单次 `PutObject` / `CopyObject` 的 5 GiB 上限，也不改变块布局；`UploadPartCopy` 只复用已有的 `UploadPart` 与 CAS 块去重路径。

## 调研结论

调研时间 2026-11-08。**当前网络无法直连 `docs.aws.amazon.com`（WebFetch 两次被域名安全策略拒绝），因此本文中的 AWS 细节来自既有 API 契约知识与 aws-cli/botocore 的实际请求行为，未逐条在线核对原文。** 实现后以 aws-cli 2.36.2 的端到端行为作为最终互操作依据，并如实回填。

| 系统 | 与本切片相关的设计 | 借鉴 / 不借鉴 |
| --- | --- | --- |
| AWS S3 | `UploadPartCopy` 复用 `PUT ?partNumber&uploadId` + `x-amz-copy-source`；`x-amz-copy-source-range` 为 `bytes=first-last`；单分片上限 5 GiB；`GetObject`/`HeadObject` 的 `partNumber` 返回该分片并带 `x-amz-mp-parts-count`；非法分片号 `416 InvalidPartNumber` | **借鉴**请求形状、大小上限与错误码；`partNumber=1` 落在非分片对象上的细节按兼容性优先处理（见下文） |
| 阿里云 OSS | 分片复制 `UploadPartCopy` 支持 `x-oss-copy-source-range`，语义与 S3 基本一致 | **借鉴**区间语法与"源对象条件 + 区间"的组合 |
| 火山引擎 TOS | 分片上传/断点续传是其文档中出现频率最高的能力之一 | **印证**分片复制是边端大模型分发的刚需 |
| MinIO | 分片复制按源区间读取后写入目标分片；`GetObject` 的 `partNumber` 直接换算为绝对偏移 | **借鉴**"区间相对源对象、`Content-Range` 相对整个对象"的换算方式 |
| JuiceFS / Nydus | 大文件按 chunk 并行读取，读取端只关心"第几段" | **印证**分片下载对并行加载有价值；不引入新的外部组件 |

**结论**：本切片只补协议与元数据，不引入新后端方法、不改变块格式、不增加常驻开销。`UploadPartCopy` 在单机与集群走**同一条** `OpenObject` + `io.Pipe` + `UploadPart` 路径，天然继承两种模式的 quorum 语义。

## 本切片范围

### 1. UploadPartCopy

`PUT /{bucket}/{key}?partNumber=N&uploadId=U`，携带 `x-amz-copy-source: /{srcBucket}/{srcKey}`。

**关键路由约束**：`dispatchMultipart` 目前把所有带 `uploadId` + `partNumber` 的 `PUT` 无条件交给 `handleUploadPart`。必须在其中**先判断 `x-amz-copy-source` 是否非空**：

- 非空 → `handleUploadPartCopy`；
- 为空 → 现有 `handleUploadPart`。

否则分片复制会被当成"用空/异常请求体上传分片"，既不能工作，也不会落到通用 CopyObject 分支（那条分支不接受 `uploadId`）。

**实现路径**：

1. 复用 `parseCopySource`（切片 10）解析源桶/key；带 `versionId` → `501`。
2. 复用 `checkCopySourceConditions` 求值四个 `x-amz-copy-source-if-*`，不满足 → `412`。
3. `store.OpenObject(src.Bucket, src.Key)` 打开源对象；源不存在 → `404 NoSuchKey`；源桶不存在 → `404 NoSuchBucket`。
4. 解析 `x-amz-copy-source-range`：
   - 缺省 → 整个源对象；
   - 只接受 `bytes=first-last`（两端都必须给出，不支持 `first-`、`-suffix` 与多区间）；
   - `first > last`、`first >= size`、`last >= size` → `416 InvalidRange`，并带 `Content-Range: bytes */<源对象大小>`。
5. 校验本次复制的字节数 ≤ 5 GiB，超过 → `400 InvalidRequest`。
6. `io.Pipe`：goroutine 调 `reader.WriteRangeTo(pw, offset, length)`，主 goroutine 把 `pr` 交给 `store.UploadPart(..., store.UploadPartOptions{ExpectedSize: length})`。
   - `UploadPart` 提前返回时（`NoSuchUpload`、长度不符等），必须 `pr.CloseWithError(...)` 唤醒写端，避免 goroutine 泄漏；
   - 函数返回前等待写端结束，确保不会在 handler 返回后仍持有源 reader。
7. 响应 `200` + `<CopyPartResult><LastModified>…</LastModified><ETag>"…"</ETag></CopyPartResult>`；`ETag` 由目标分片重新计算（与源对象 ETag 无关）；有会话校验和时回传对应 `x-amz-checksum-*` 头。

| 项 | 决定 |
| --- | --- |
| 单分片上限 | 5 GiB（`5 << 30`）；超过 `400 InvalidRequest` |
| 源条件 | 与 CopyObject 同一套四个头；不满足 `412 PreconditionFailed` |
| 源不存在 | 先 `404`，再谈条件（与 CopyObject 一致） |
| 目标条件 | 分片复制没有目标对象级条件；`If-None-Match` 等写条件只在 Complete 生效 |
| 存储类型 | `x-amz-storage-class` 非 `STANDARD` → `501`；分片本身不携带存储类型 |
| 加密/标签头 | 与 CopyObject 相同：出现即 `501`，不静默忽略 |
| 元数据指令 | `x-amz-metadata-directive` 在分片复制上无定义，忽略 |
| 分片 ETag | 由 `UploadPart` 按目标分片内容重算；`CopyPartResult.ETag` 与之后 `ListParts` 一致 |
| 源为 EC 布局 | 走 `OpenObject`，由后端决定可读性；不可读时按存储错误返回，不伪装成功 |
| 零字节分片 | 允许（与 `UploadPart` 一致）；Complete 的"非末片 ≥ 5 MiB"规则不变 |

**不复制的内容**：源对象的分片边界不进入目标分片；目标是"一个新的分片"，其边界由本次区间长度决定。

### 2. 分片下载（`partNumber`）

`GET`/`HEAD` `/{bucket}/{key}?partNumber=N`。

当前 `ObjectMeta` 只有块表，没有分片边界；`CompleteMultipartUpload` 结束后 `multipart/<uploadId>/` 被删除，事后无法恢复"第 N 片从哪到哪"。因此必须把分片边界持久化到对象元数据：

```json
{
  "parts": [
    {"number": 1, "size": 5242880},
    {"number": 2, "size": 1048576}
  ]
}
```

- 偏移不落盘，按 `size` 的累计和推导，避免同一事实存两份。
- 只有分片上传产生的对象才有该字段；`PutObject` 与 `CopyObject` 产生的对象没有。
- 字段是**可选新增**：旧二进制读到新 `meta.json` 会忽略未知字段（Go `encoding/json` 默认行为）；新二进制读到旧 `meta.json` 得到 `parts == nil`，按非分片对象处理。因此**不提升 `format_version`**。

| 项 | 决定 |
| --- | --- |
| 编号校验 | 非整数或 `< 1` → `400 InvalidArgument`；`> 10000` → `400 InvalidArgument`（与 `UploadPart` 一致） |
| 分片存在 | 分片号不在 `parts` 中 → `416 InvalidPartNumber`（新增错误码） |
| 分片对象响应 | `206`，`Content-Range: bytes <start>-<end>/<对象总大小>`，并带 `x-amz-mp-parts-count: <分片数>` |
| 非分片对象 | `partNumber=1` 视为"整体对象即第一片"，返回 `200` 完整对象；`partNumber>1` → `416 InvalidPartNumber`。这是兼容性取舍：真实 S3 对该组合的行为未在线核对，选择"不伪造 `Content-Range`"的保守实现 |
| `Range` 组合 | `Range` 相对**分片**解析，`Content-Range` 使用**对象绝对偏移**；分片内越界 → `416 InvalidRange`，`Content-Range: bytes */<分片大小>` |
| `If-Range` | 仍按对象 ETag/修改时间判断；不成立时回退为**整个分片**（不是整个对象） |
| `If-Match` / `If-None-Match` | 仍对对象 ETag 求值；分片不改变条件语义 |
| 校验和 | `206` 分片响应不回传对象级校验和（与现有 Range 规则一致）；非分片对象 `partNumber=1` 且 `200` 时按完整响应处理 |
| `HEAD` | 与 `GET` 同状态码与头部，无响应体 |
| 空分片 | 分片长度为 0 时回 `200` + `Content-Length: 0`（不构造非法的 `Content-Range`）；正常客户端不会产生这种分片 |
| 带 `uploadId` 的 GET | 仍走 `ListParts`；`uploadId` + `partNumber` 继续 `501`（语义冲突） |

**`x-amz-mp-parts-count`** 只在 `partNumber` 请求且对象由分片上传产生时返回；普通 `GET`/`HEAD` 不返回，避免给所有响应增加头部。

### 3. 元数据贯穿点

新增 `ObjectMeta.Parts []PartDesc`（`PartDesc{Number int, Size int64}`）与 `ObjectInfo.Parts []ObjectPart`（含推导出的 `Offset`）。必须同步修改：

| 位置 | 改动 |
| --- | --- |
| `store.AssembleMultipart` | 串接块表时记录每个 `PartState.Size`，产出 `AssembledObject.Parts` |
| `store.CompleteMultipartUpload` | 写入 `meta.Parts`；返回的 `ObjectInfo` 同步填充 |
| `cluster.CompleteMultipartUpload` | 手工构造 `store.ObjectMeta` 时赋值 `Parts` |
| `store.validateReplicaMeta` | 校验：编号严格升序、1–10000、`size >= 0`、总长等于 `Size`、墓碑不得携带 `Parts` |
| `store.objectInfoFromMeta` | 映射 `Parts` 并计算 `Offset` |
| `cluster` 读路径的 `clusterReader.info` | 同步映射 `Parts` |
| `store.CopyObject` | **不复制** `Parts`：复制后的对象按单分片对象处理。单机 O(1) 复制虽然块表一致，但集群复制会重写内容与 ETag；两种模式保持一致比"单机多一个能力"更重要 |
| `cluster.CopyObject` | 流式重写本就不产生 `Parts`，无需额外处理 |

校验失败一律 `ErrCorrupt`，绝不降级成"没有分片边界"——后者会让 `partNumber` 静默返回错误区间。

### 4. 错误码

新增：

| Code | HTTP | 触发条件 |
| --- | --- | --- |
| `InvalidPartNumber` | 416 | 请求的 `partNumber` 在对象中不存在（或非分片对象请求 `partNumber>1`） |

`store.ErrInvalidPartNumber` 现有映射到 `400 InvalidArgument`，保持不变（它用于 `UploadPart`/`Complete` 的编号范围错误）；HTTP 层分片下载直接构造 `s3err.CodeInvalidPartNumber`，不改动既有映射。

## 明确不做

- 多区间 `Range`（逗号分隔）：仍 `501`。
- `UploadPartCopy` 对源对象做 checksum 复用：分片校验和由目标内容重新计算，不声明"与源一致"。
- 分片复制跨越 5 GiB 单分片：不做自动拆分；客户端必须自行按 ≤5 GiB 区间分片。
- 版本控制、对象标签、追加上传、生命周期：不涉及。
- 复制后保留分片边界：明确不保留，见上表。
- 分片边界的后台修复/重建：`Parts` 与块表同生命周期，随 `meta.json` 一起复制与仲裁。

## 验收标准

### 功能

1. `aws s3api upload-part-copy` 完整复制一个 6 MiB 源对象为一个分片，`CopyPartResult.ETag` 与随后 `list-parts` 一致。
2. `x-amz-copy-source-range: bytes=...` 复制区间后，Complete 出的对象内容等于源对象的对应区间。
3. 源条件不满足 → `412`；源不存在 → `404`；区间越界 → `416 InvalidRange`；单分片 > 5 GiB → `400 InvalidRequest`（以构造请求或单测覆盖，不实际写入 5 GiB）。
4. `aws s3 cp s3://b/src s3://b/dst` 对 **> 8 MiB** 对象（不调大 `s3.multipart_threshold`）成功，内容逐字节一致。这是切片 10 遗留的互操作缺口，必须端到端验证。
5. 分片上传（2 片）Complete 后：
   - `GET ?partNumber=1` 返回第一片，`206` + 绝对 `Content-Range` + `x-amz-mp-parts-count: 2`；
   - `HEAD ?partNumber=2` 返回第二片长度与相同头部；
   - `GET ?partNumber=3` → `416 InvalidPartNumber`；
   - `GET ?partNumber=1` + `Range: bytes=0-9` 返回分片内前 10 字节，`Content-Range` 为对象绝对偏移。
6. 普通 `PutObject` 对象：`GET ?partNumber=1` 返回完整对象（`200`）；`partNumber=2` → `416`。
7. 重启进程后分片下载仍可用（证明 `Parts` 已持久化，而非依赖内存状态）。
8. 集群模式（3 节点本机多进程）下，分片上传 + Complete + 分片下载通过；`Parts` 在各副本 `meta.json` 中一致。
9. CopyObject 一个分片对象后，目标按非分片对象处理（`partNumber=1` 返回整个对象，`partNumber=2` → `416`），且文档已写明。

### 性能

| 指标 | 门槛 | 说明 |
| --- | --- | --- |
| 分片下载首字节 | 与同区间 `Range` 请求同量级 | 只多一次元数据内的分片查找，不增加块 I/O |
| `ObjectMeta` 体积增量 | 10000 分片时 < 500 KiB | 每片约 30 字节 JSON；仍远低于 32 MiB 的元数据读取上限 |
| UploadPartCopy 吞吐 | 不低于 `PutObject` 同大小的 70% | 数据路径为"读源 → 重切块 → 写 CAS"，额外成本是源读取；不承诺零拷贝 |

所有数字仅为本机（Apple M4 / macOS / APFS）回归基线，不构成跨平台承诺；本机压测总写入 ≤ 512 MiB。

### 测试

- `internal/store/mpu_test.go` / `mpu_internal_test.go`：`Parts` 产出、校验、序列化兼容（无字段的旧 meta 可读）。
- `internal/s3api/handler_mpu_test.go`：UploadPartCopy 路由优先级、条件、区间、错误码。
- `internal/s3api/handler_object_read_test.go`：分片下载 206/416、Range 相对换算、非分片对象语义、`x-amz-mp-parts-count`。
- `internal/cluster/mpu_test.go`：集群 Complete 持久化 `Parts`、分片下载。
- `go vet ./...`、`go test ./...`、`go test -race ./internal/s3api ./internal/store ./internal/cluster` 全绿。

## 风险与回滚

| 风险 | 缓解 |
| --- | --- |
| 路由顺序错误导致分片复制被当成 UploadPart，或普通 UploadPart 被误判为复制 | 在 `dispatchMultipart` 的 PUT 分支内以 `x-amz-copy-source` 非空为唯一判据；单测固定两条路径 |
| `io.Pipe` 写端在 `UploadPart` 提前返回后泄漏 | `defer pr.CloseWithError(...)` + 返回前等待写端结束；单测覆盖 `NoSuchUpload` 提前失败 |
| `Parts` 与块表不一致（分片边界总和 ≠ 对象大小） | `validateReplicaMeta` 强校验；`AssembleMultipart` 从 `PartState.Size` 直接产出，不重新计数 |
| 旧数据没有 `Parts`，分片对象退化为"单分片" | 明确语义：`partNumber=1` 返回完整对象，更大编号 416；文档记录，作为升级兼容行为 |
| 复制后 `Parts` 被意外保留，导致 `partNumber` 返回与内容不符的区间 | 单机与集群复制都显式不写 `Parts`；测试断言复制后 `partNumber=2` → 416 |
| 5 GiB 分片边界判断使用 `int` 溢出 | 全链路使用 `int64`；单测覆盖 `5<<30` 与 `5<<30+1` |

回滚策略：删除 `dispatchMultipart` 的复制分支与 `handleUploadPartCopy`、把 `partNumber` 的 GET/HEAD 分支恢复为 `501`、忽略 `meta.json` 中的 `parts` 字段，即可回到当前行为。已写入的 `parts` 是纯附加字段，回滚后旧二进制可正常读取对象（只是失去分片下载能力），**无需数据迁移**。

## 实现与验证结果

验证环境：Apple M4 / macOS / APFS，Go 1.27.2，aws-cli 2.36.2，2026-11-08。所有性能数字仅为本机回归基线。

### 实现落点

| 层 | 文件 | 内容 |
| --- | --- | --- |
| store | `internal/store/meta.go`、`fsstore_mpu.go`、`fsstore_read.go`、`replica.go`、`fsstore_copy.go` | `PartDesc`/`ObjectMeta.Parts`、`AssembleMultipart` 产出边界、Complete 落盘、`ObjectInfo.Parts` 偏移推导、`validateParts` 强校验、CopyObject 不写 `Parts` |
| cluster | `internal/cluster/mpu.go`、`read.go` | Complete 写入并在返回值中映射 `Parts`；读路径 `PartBounds` 映射；流式复制不产生边界 |
| s3api | `internal/s3api/handler_mpu_copy.go`、`handler_object_read.go`、`router.go` | UploadPartCopy（`io.Pipe` → `UploadPart`）、`partNumber` 分片下载、按 `x-amz-copy-source` 分流、`Content-Range` 与 `x-amz-mp-parts-count` |
| 错误码 | `internal/s3err/errors.go` | 新增 `InvalidPartNumber`（416） |

### 自动化测试

- `go vet ./...`：通过。
- `go test ./...`：全绿（`internal/cluster` 17.8 s、`internal/s3api` 12.7 s、`internal/store` 28.8 s）。
- `go test -race ./internal/s3api ./internal/store ./internal/cluster`：全绿（13.6 s / 30.9 s / 20.5 s）。
- 覆盖点：
  - `handler_mpu_copy_test.go`：完整/区间复制、`CopyPartResult` 与 `ListParts` ETag 一致、源条件、越界 `416` + `Content-Range: bytes */<size>`、存储类型/SSE `501`、`NoSuchUpload` 提前失败（写端 goroutine 不泄漏）、同路由分流、`parseCopyPartRange` 14 个用例（含恰好 5 GiB 与超 5 GiB）。
  - `handler_object_read_test.go`：`partNumber` 的 206/416、`Range` 相对分片与绝对 `Content-Range`、`x-amz-mp-parts-count`、非分片对象语义。
  - `store/mpu_test.go`：边界持久化与重启恢复、`ObjectInfo.Parts` 偏移、CopyObject 丢弃边界、`validateParts` 六类损坏、10000 分片元数据体积。
  - `cluster/mpu_test.go`：3 节点分片上传/Complete 后各副本 `Parts` 一致、跨节点按 `Parts` 读取、集群 CopyObject 不保留边界。

### 单机端到端（aws-cli 2.36.2）

数据目录 `/tmp/blink-e2e-11/single`，bucket `e2e-11`，总写入约 100 MiB。源对象 24 MiB，SHA256 `96bc092060d6d8dfa3e1679db8b9c7b84363e9aa9523ffdfafb6557c8fa4f915`。

| 场景 | 结果 |
| --- | --- |
| `aws s3 cp s3://…/src s3://…/dst --copy-props none`（>8 MiB，自动走 UploadPartCopy） | 成功；ETag `"545695973debe0ea82b5128be9ab0b2a-3"`；下载后 SHA256 与源一致 |
| 手工 3×8 MiB `upload-part-copy`（区间 `0-8388607`、`8388608-16777215`、`16777216-25165823`） | `list-parts` ETag/Size 与 `CopyPartResult` 一致；Complete 后逐字节一致 |
| `get-object --part-number 2` | `206`，`Content-Range: bytes 8388608-16777215/25165824`，`x-amz-mp-parts-count: 3`，内容一致 |
| `head-object --part-number 2` | `Content-Length: 8388608`、`PartsCount: 3` |
| `get-object --part-number 3`（重启进程后） | 内容一致，证明边界来自持久化元数据 |
| `get-object --part-number 1 --range bytes=0-9` | `206`，`Content-Range: bytes 0-9/25165824` |
| `--part-number 4` | `416 InvalidPartNumber` |
| 区间越界 / 条件失败 / 会话不存在 / 源不存在 | `416 InvalidRange`（带 `Content-Range: bytes */<size>`）/ `412` / `404 NoSuchUpload` / `404 NoSuchKey` |
| 对分片对象执行 `copy-object` 后 | 目标 `partNumber=1` → `200` 整对象且无 `Content-Range`/`x-amz-mp-parts-count`；`partNumber=2` → `416` |

**aws-cli 限制（如实记录）**：`aws s3 cp s3://… s3://…` 默认 `--copy-props default` 会先对源对象调用 `GetObjectTagging`，本版本返回 `501`，因此**大对象服务端复制必须显式加 `--copy-props none`**。这是客户端在复制前主动请求标签所致，与 UploadPartCopy 本身是否可用无关。

### 集群端到端（3 节点本机多进程）

端口 19001–19003 / 内部 19101–19103，`BLINK_CLUSTER_MODE=cluster`、N=3/W=2/R=2、各节点独立数据目录。

| 场景 | 结果 |
| --- | --- |
| 24 MiB 源经 n2 上传，`s3 cp s3→s3 --copy-props none` 经 n3 | SHA256 一致，ETag `…-3` |
| `get-object --part-number 2/3` 分别经 n2/n3 | `206`、绝对 `Content-Range`、`PartsCount=3`，内容与源对应区间一致 |
| Complete 后三个节点的 `head-object --part-number 2` | 均返回 `Content-Length: 8388608`、`PartsCount: 3` |
| 手工跨节点分片复制：n1 建会话，n2/n3 各传一片，n1 Complete | 三片 ETag 与 `list-parts` 一致；下载 SHA256 与源一致 |
| 集群 `copy-object` 后 | 内容一致；目标无分片边界，`partNumber=2` → `416` |

**集群复制是流式复制，不是零拷贝**：本次验证只证明内容与语义正确，不构成对集群零拷贝的任何声明。

### 性能实测

| 指标 | 门槛 | 实测 | 结论 |
| --- | --- | --- | --- |
| 10000 分片的 `meta.json` 增量 | < 500 KiB | 309040 字节（`TestPartsMetadataSize`） | 通过 |
| UploadPartCopy 吞吐 / PutObject 同大小 | ≥ 70% | 64 MiB × 3 轮：PutObject 104.7/120.5/119.8 MiB/s，UploadPartCopy 133.2/146.4/139.3 MiB/s，比值 127%/122%/116% | 通过（localhost，CAS 去重与 page cache 会放大复制优势） |
| 分片下载首字节 / 同区间 `Range` | 同量级 | aws-cli 5 轮总耗时：`Range` 324–451 ms，`partNumber` 328–420 ms，CLI 启动开销主导，无法分辨差异 | 未观测到退化；未做服务端首字节单独采样 |

### 仍未验证 / 已知限制

- 5 GiB 单分片上限只由单测覆盖（构造 5 GiB 与 5 GiB+1 的区间），**未实际传输 5 GiB**。
- 集群模式未验证节点故障/读降级场景下的 `partNumber` 读取；正常三副本路径已验证。
- 未做跨机器网络下的吞吐与首字节测量；上表数字仅为本机基线。
- aws-cli 默认 `--copy-props default` 的 `GetObjectTagging` 仍返回 `501`，大对象复制需 `--copy-props none`（已在 `docs/design/known-limitations.md` 记录）。
- 复制后的对象不保留分片边界；旧 `meta.json`（无 `parts`）按单分片对象处理。
