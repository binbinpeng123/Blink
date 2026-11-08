# 对象级核心操作：服务端复制、批量删除与 ListObjects V1

评审状态：待评审
评审人：待定
评审日期：待定
适用版本：v0.3.0
关联分支：`feature/ai-native-core-10`

## 背景

v0.1.0（单机）与 v0.2.0（集群）已经覆盖 Bucket 生命周期、对象 PUT/GET/HEAD/DELETE、Range、条件请求、ListObjectsV2 与 Multipart 全流程。与主流对象存储（AWS S3、阿里云 OSS、火山引擎 TOS、MinIO）对照后，**对象级**仍有三处高频缺口：

| 缺口 | S3 / OSS / TOS 行为 | 当前 Blink | 影响 |
| --- | --- | --- | --- |
| `CopyObject` | `PUT` + `x-amz-copy-source`，服务端复制 | 未实现，落到普通 PUT（上传空体） | 模型/数据集改名、跨桶分发只能客户端下载再上传 |
| `DeleteObjects` | `POST /{bucket}?delete`，单次最多 1000 个 | 未实现（桶级 POST 回 501） | 数据集清理需 N 次往返；部分 SDK 的批量清理脚本直接失败 |
| ListObjects V1 | `GET /{bucket}?marker=...`（不带 `list-type`） | 501 | 老客户端（部分 S3 SDK、rclone 旧版、自研脚本）无法列举 |

这三项都是**协议兼容性**能力，不改变既有数据布局，风险可控。本切片同时给出 Blink 的差异化实现：**CopyObject 在单机模式下是 O(1) 元数据操作**，不搬运任何数据字节。

## 调研结论：主流系统怎么做

调研时间 2026-11-08，来源为各项目官网/镜像文档（GitHub 直连在当前网络不可达，使用可访问的镜像与官网页面）。

| 系统 | 与本切片相关的设计 | 借鉴 / 不借鉴 |
| --- | --- | --- |
| AWS S3 | CopyObject 单次上限 5 GiB，超过须用 `UploadPartCopy`；元数据指令 `COPY/REPLACE`；四个 `x-amz-copy-source-if-*` 条件 | **借鉴**契约与错误语义；`UploadPartCopy` 不在本切片 |
| 阿里云 OSS | CopyObject 支持同区域复制，元数据可选覆盖；批量删除与 ListObjects V1 长期保留 | **借鉴**兼容性优先 |
| 火山引擎 TOS | 官网功能页中「分片上传/断点续传」出现频率最高，其次是生命周期、追加上传、对象标签 | 本切片只取批量删除与复制；追加上传、标签留待后续 |
| MinIO | DeleteObjects 返回逐条结果（部分成功），`Quiet` 模式只回错误项 | **借鉴**响应形状与 Quiet 语义 |
| JuiceFS | Chunk（默认 64 MiB）→ Slice → Block（默认 4 MiB）的分层；元数据与数据分离 | 与 Blink 的「元数据 JSON + CAS 块」思路一致，**印证**块级去重可由内容寻址承担 |
| SeaweedFS | 小对象 blob 打包（Haystack/F4 等）以降低元数据成本 | **不借鉴**：与 Blink 的 CAS 布局冲突，且属于 LIST 性能问题（见 09 文档），不在本切片 |
| Alluxio | 计算侧分布式缓存、checkpoint 写回缓存 | **不借鉴**：与「无外部依赖、单二进制」的产品约束冲突 |
| Nydus | 按 chunk 粒度的按需加载与跨层去重 | **印证**：Blink 的 4 MiB 块 + 内容哈希已具备同等的块级去重与随机读能力 |

**结论**：本切片只做协议兼容与元数据级能力，不引入新的后台组件、不改变数据布局、不增加常驻开销。

## 本切片范围

### 1. CopyObject（核心创新点）

`PUT /{bucket}/{key}`，携带 `x-amz-copy-source: /{srcBucket}/{srcKey}`。

**单机实现（O(1) 复制）**：

1. 读取源对象 `meta.json`（命中 09 切片的元数据 LRU 时无磁盘 I/O）。
2. 目标 `meta.json` 直接引用**同一组 CAS 块**（块不可变，无需复制）。
3. 原子写入目标 `meta.json`，失效/更新缓存。

复制 1 KiB 与复制 100 GiB 的耗时都是"一次小文件读 + 一次小文件原子写"。这来自内容寻址布局的天然属性，不需要额外索引或引用计数。**该收益只对单机模式成立**。

**集群实现（流式复制）**：源对象经 quorum 读取，目标经 quorum 写入。数据会经过协调者一次，代价与对象大小线性相关。这样保证副本分布按目标 key 的环位置重新计算，不会出现"meta 指向目标节点上不存在的块"。文档与响应中不宣称集群复制是零拷贝。

**契约**：

| 项 | 决定 |
| --- | --- |
| 源路径格式 | `/bucket/key` 或 `bucket/key`，按 URL 路径规则解码；含 `?versionId` 时返回 501（无版本控制，不能静默忽略） |
| 元数据指令 | `x-amz-metadata-directive: COPY`（默认）/ `REPLACE`；其他值 400 |
| `REPLACE` 时 | 使用请求中的 `Content-Type` 与 `x-amz-meta-*`；未提供 `Content-Type` 时落 `application/octet-stream` |
| 源条件 | 支持 `x-amz-copy-source-if-match` / `if-none-match` / `if-modified-since` / `if-unmodified-since`；不满足分别回 412 / 304 语义（CopyObject 用 412） |
| 目标条件 | 支持 `If-None-Match: *`；其他写条件仍回 501 |
| 自复制 | 源与目标相同且 `COPY` → 400 `InvalidRequest`；`REPLACE` 允许，用于原地更新元数据 |
| 大小上限 | 源对象 > 5 GiB → 400 `InvalidRequest`（与 S3 一致）；更大的复制须走分片上传 |
| 存储类型 | `x-amz-storage-class` 非 `STANDARD` → 501 |
| ETag | 与源一致（内容未变）；`LastModified` 为本次复制时间 |
| 复制后校验和 | 保持源的 `Checksum` / `ChecksumAlgorithm` / `ChecksumType`（单机）；集群流式路径重新计算 ETag，校验和按未声明处理 |
| 响应 | `200` + `<CopyObjectResult><LastModified>…</LastModified><ETag>"…"</ETag></CopyObjectResult>` |
| 不支持 | `x-amz-copy-source-range`、`UploadPartCopy`、加密/标签相关头部 → 501 |

**不复制的内容**：对象的 `ObjVersion` 从目标 key 自己的版本序列继续，不复用源版本。

### 2. DeleteObjects（批量删除）

`POST /{bucket}?delete`，请求体为 S3 `Delete` XML。

| 项 | 决定 |
| --- | --- |
| 单次上限 | 1000 个 `Object`；超过 400 `MalformedXML` |
| 请求体上限 | 1 MiB，超过回 400 |
| `Content-MD5` | 可选；存在时校验（不匹配 400 `BadDigest`），缺失不拒绝 |
| 逐条结果 | 返回 `Deleted` 与 `Error` 两类条目；单条失败不影响其余条目 |
| 不存在的 key | 视为删除成功（与 `DeleteObject` 的幂等语义一致），不出现在 `Error` 中 |
| `Quiet` | `true` 时只返回 `Error` 条目 |
| 桶不存在 | 整体 404 `NoSuchBucket`，不逐条返回 |
| 并发 | 最多 8 个 worker 并行删除；结果按请求顺序输出，保证可复现 |
| `VersionId` | 出现时 501（无版本控制） |

### 3. ListObjects V1

`GET /{bucket}`（不带 `list-type` 或 `list-type=1`）。

| 项 | 决定 |
| --- | --- |
| 参数 | `prefix` / `delimiter` / `marker` / `max-keys` / `encoding-type` |
| 分页 | `marker` 为上一页最后一条；`NextMarker` 仅在 `delimiter` 非空且 `IsTruncated` 时返回（与 S3 一致） |
| 折叠前缀 | `marker` 以 `delimiter` 结尾时按公共前缀游标处理，跳过该前缀下全部 key |
| 响应 | `ListBucketResult`，不含 `KeyCount` / `ContinuationToken` / `StartAfter` |
| 复用 | 分页算法复用 `store.PageObjects`，不新增第二套边界逻辑 |
| `list-type` 其他值 | 保持 400 |

## 验收标准

### 功能

1. `aws s3 cp s3://b/src s3://b/dst` 走 CopyObject 成功，ETag 一致。
2. `aws s3 cp --metadata-directive REPLACE --content-type ...` 更新元数据后，源块文件数量不增加（单机）。
3. CopyObject 跨桶、跨 key、自复制（COPY 拒绝 / REPLACE 允许）、`If-None-Match: *` 全部符合上表。
4. `aws s3 rm --recursive`（内部使用 DeleteObjects）可清空 1000+ 对象；`Quiet` 与错误项行为经单测覆盖。
5. `aws s3 ls`（V1，不带 `list-type`）可列举、翻页与按 `/` 折叠。
6. 以上三项在集群模式（3 节点本机多进程）下同样通过。

### 性能

| 指标 | 门槛 | 说明 |
| --- | --- | --- |
| 单机 CopyObject 1 GiB 对象 | < 10 ms | 只读/写 meta，与对象大小无关 |
| 单机 CopyObject 后磁盘占用 | 不增加 | CAS 块共享 |
| DeleteObjects 1000 个小对象 | < 3 s | 8 worker；APFS 下限见 09 文档 |
| ListObjects V1 10 万对象 | 不劣于 V2 | 复用同一分页路径；不承诺 `<500 ms`（09 文档已知限制） |

所有数字仅为本机（Apple M4 / macOS / APFS）回归基线，不构成跨平台承诺。

### 测试

- `internal/store/copy_test.go`：块共享、版本递增、跨桶、元数据覆盖、自复制、条件、缓存失效。
- `internal/s3api/handler_copy_test.go`：头部解析、响应 XML、错误码、超大源。
- `internal/s3api/handler_delete_objects_test.go`：正常/Quiet/部分失败/1000 上限/Content-MD5。
- `internal/s3api/handler_list_test.go`：V1 分页、delimiter、NextMarker、URL 编码。
- `internal/cluster/copy_test.go`：跨节点复制与副本分布。
- `go test -race ./internal/store/ ./internal/cluster/ ./internal/s3api/` 全绿。

## 明确不做

- `UploadPartCopy` 与分片复制：需要新的内部协议，单列后续切片。
- 版本控制与 `versionId`：元数据布局改动大，须单独评审。
- 对象标签（Tagging）、追加上传、生命周期、跨区域复制：本切片不涉及。
- 多区间 Range：留待后续切片单独评审（需修改 `parseRange` 与响应编码）。
- 服务端加密、存储类型转换、跨区域复制。

## 风险与回滚

| 风险 | 缓解 |
| --- | --- |
| CopyObject 复制 EC 布局元数据到单机路径会导致分片缺失 | 单机 `Store.CopyObject` 拒绝 `Layout==ec` 的源（返回 501）；集群走流式重写，不继承布局 |
| 批量删除并发过高打满磁盘 | worker 上限 8；`DeleteObject` 仍按 key 加锁，跨 key 才并行 |
| V1 marker 歧义（对象 key 恰好以 delimiter 结尾） | 与 S3/MinIO 行为一致：按公共前缀游标处理；单测固定该语义 |
| 新增 `Backend.CopyObject` 方法导致集群误用内嵌 `Store` 实现 | `Cluster` 显式实现并覆盖；编译期接口断言 + 集群测试验证副本分布 |

回滚策略：三项功能均为新增代码路径，删除路由分支与 handler、移除 `Backend.CopyObject` 即可回到当前行为；不涉及磁盘格式变更（`CopyObject` 只写既有 `meta.json` 字段）。

## 实现与验证结果（2026-11-08 回填）

环境：Apple M4 / macOS（Darwin 24.6.0）/ APFS；单机进程 `--addr 127.0.0.1:19000`；客户端 aws-cli **2.36.2**；Go 1.27.2。以下数字仅为本机回归基线。

### 端到端（aws-cli）

| 场景 | 结果 |
| --- | --- |
| 6 MiB 同桶复制（`aws s3 cp s3://e2e-bucket/big/source.bin s3://e2e-bucket/big/copied.bin`） | `200`，ETag `"4b784f7758272d6645ec57e854f9f65e"`；复制前后下载内容的 SHA256 均为 `1bf5d1ab…2f4396` |
| 跨桶复制 + `COPY` | `Content-Type: image/png`、`x-amz-meta-owner: edge-01`、`x-amz-meta-stage: prod` 全部保留 |
| 跨桶复制 + `REPLACE` | `Content-Type: text/csv`、`x-amz-meta-owner: edge-02` 生效；`Content-Length` 仍为 6291456（内容未变） |
| 源 key 不存在 / 源桶或目标桶不存在 | `404 NoSuchKey` / `404 NoSuchBucket` |
| 自复制 + `COPY` | `400 InvalidRequest`，消息为 `源与目标相同，COPY 元数据指令没有意义`（不含 `store:` 内部前缀） |
| ListObjects V1（`aws s3api list-objects`） | 原始 XML 中 key 为 `models/copied.bin`（`/` 未编码）；marker 翻页、`delimiter=/` 折叠出 `models/` 均正确，与 V2 结果一致 |
| DeleteObjects 1000 个 1 KiB 对象（`aws s3api delete-objects`） | CLI 返回 1000 条 `<Deleted>`，删除后列表为空 |
| `Quiet=true` | 响应无 `<Deleted>` 条目，对象已删除 |
| 1001 条 `<Object>` / 空 `<Objects>` | `400 MalformedXML` |
| 桶不存在 | `404 NoSuchBucket`（整请求） |

注：aws-cli 2.36.2 的 `s3api list-objects` 会自动追加 `encoding-type=url` 并在客户端解码，因此响应中出现 `EncodingType: url` 属正常现象。本切片修正了编码字符集：unreserved 字符（`A-Za-z0-9-_.~`）与 `/` 原样保留，其余按字节大写百分号编码——此前把 `/` 编成 `%2F` 会导致客户端字符串比较失败。

### 性能

| 指标 | 门槛 | 实测 | 结论 |
| --- | --- | --- | --- |
| 单机 CopyObject 1 GiB | < 10 ms | **1 GiB 未实测**（本机压测写入上限 512 MiB）；以 256 MiB 代替 | 见下 |
| 单机 CopyObject 256 MiB | — | 4 次服务端耗时 8.37 / 9.72 / 13.38 / 15.33 ms，中位数 9.72 ms | 中位数达标，但样本波动越过门槛；**不得据此声称 1 GiB 达标** |
| 单机 CopyObject 后磁盘占用 | 不增加 | CAS 文件数 66 → 66（上传后 66，复制后仍 66） | 达标：确认零数据拷贝 |
| DeleteObjects 1000 个 1 KiB 对象 | < 3 s | 服务端 164.44 ms | 达标 |
| ListObjects V1 10 万对象 | 不劣于 V2 | **10 万对象未实测**；1000 对象时 V1 墙钟 0.362–0.398 s、V2 0.362–0.372 s，服务端 18.3–21.3 ms | 两者复用同一 `store.ListObjects` 路径，1000 对象上无退化；10 万对象结论不外推（09 文档已知 LIST 总量限制） |

256 MiB 复制的 ETag 为 `"813d824bc8ce9e25515253976df3878b-32"`。

### 测试

- `go vet ./...` 通过。
- `go test ./...` 全绿（`internal/cluster` 18.188 s、`internal/s3api` 9.451 s、`internal/store` 28.752 s，其余包均 `ok`）。
- `go test -race ./internal/s3api ./internal/store ./internal/cluster` 全绿（13.232 / 30.357 / 18.711 s）。

### aws-cli 互操作限制（必须同步到已知限制）

1. **`aws s3 cp s3://… s3://…` 对超过 8 MiB 的对象默认失败**：aws-cli 的 `s3.multipart_threshold` 默认为 8 MiB，超过即改用 `UploadPartCopy`（`x-amz-copy-source-range`），本版本返回 `501`，复制不会发生（目标 404）。实测 256 MiB 复制需在 `AWS_CONFIG_FILE` 中设 `s3.multipart_threshold = 5GB` 才会走 `CopyObject`。**不得声称 `s3 cp` 对大对象的服务端复制默认可用。**
2. **`s3 rm --recursive` 未使用 DeleteObjects**：aws-cli 2.36.2 实测逐条 `DELETE`（每条约 `204`）。验证批量删除接口必须显式使用 `aws s3api delete-objects`；验收标准第 4 条中"内部使用 DeleteObjects"的前提不成立。

## 未验证项（实现后仍未覆盖）

- Linux / NVMe 上的 CopyObject 与批量删除延迟。
- Windows 文件系统语义。
- 1 GiB CopyObject 与 10 万对象 ListObjects V1（本机写入与时间预算所限，见上表）。
- 集群模式下 1000 对象批量删除的 quorum 行为（仅有复制相关集群测试；未做集群批量删除压测）。
- `UploadPartCopy` 与超过 8 MiB 的 aws-cli 服务端复制（本版本明确不实现）。
