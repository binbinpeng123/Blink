# S3 接口与数据模型

评审状态：已允许开发
评审人：binbinpeng123
评审日期：2026-10-23
适用版本：v0.1.0（首个可用版本）

## 目标

定义 Blink 首个版本对外暴露的 S3 兼容边界，避免客户端、服务端和测试对行为产生不同理解。

**兼容性声明原则**：只有通过自动化互操作测试的 API 才能写入兼容性声明；未通过测试的接口一律标记为"待验证"，不得对外声称支持。

## 支持的 API 清单

### 基础路由（首个实现切片）

| # | 方法 | 路径 | 说明 |
| --- | --- | --- | --- |
| 1 | `PUT` | `/{bucket}` | CreateBucket |
| 2 | `HEAD` | `/{bucket}` | HeadBucket |
| 3 | `DELETE` | `/{bucket}` | DeleteBucket |
| 4 | `GET` | `/` | ListBuckets |
| 5 | `PUT` | `/{bucket}/{key}` | PutObject |
| 6 | `GET` | `/{bucket}/{key}` | GetObject（含 Range 与条件请求） |
| 7 | `HEAD` | `/{bucket}/{key}` | HeadObject |
| 8 | `DELETE` | `/{bucket}/{key}` | DeleteObject |
| 9 | `GET` | `/{bucket}?list-type=2` | ListObjectsV2 |
| 10 | `GET` | `/healthz`、`/readyz`、`/version` | 非 S3 端点，免签名 |

### 分片上传（独立切片，纳入首个可用版本）

| # | 方法 | 路径 | 说明 |
| --- | --- | --- | --- |
| 11 | `POST` | `/{bucket}/{key}?uploads` | CreateMultipartUpload |
| 12 | `PUT` | `/{bucket}/{key}?partNumber=N&uploadId=X` | UploadPart |
| 13 | `POST` | `/{bucket}/{key}?uploadId=X` | CompleteMultipartUpload |
| 14 | `DELETE` | `/{bucket}/{key}?uploadId=X` | AbortMultipartUpload |
| 15 | `GET` | `/{bucket}/{key}?uploadId=X` | ListParts |
| 16 | `GET` | `/{bucket}?uploads` | ListMultipartUploads |

**为什么分片上传必须纳入首个可用版本**：AWS CLI 对超过 8 MiB 的文件默认自动走分片上传，而边缘 AI 场景的头号负载正是 GB 级模型权重。若不实现，`aws s3 cp model.bin s3://models/` 会直接失败，README 中最核心的快速开始流程将跑不通。分片上传不影响 PutObject/GetObject 语义，因此拆分为独立切片以保持增量可验证。

## 明确不支持的接口

**必须显式返回 `501 NotImplemented`，不得落到泛化的 404 或 500。**

| 请求 | 响应 |
| --- | --- |
| `PUT /{bucket}/?acl`、`?tagging`、`?cors`、`?lifecycle`、`?encryption`、`?versioning`、`?policy`、`?replication`、`?object-lock` | `501 NotImplemented` |
| `GET /{bucket}/?versions` | `501 NotImplemented` |
| `POST /{bucket}?delete`（批量删除） | `501 NotImplemented` |
| `PUT /{bucket}/{key}?acl`、`?tagging`、`?legal-hold`、`?retention` | `501 NotImplemented` |
| `POST /{bucket}/{key}?restore`、`?select` | `501 NotImplemented` |
| `GET /{bucket}/{key}?torrent` | `501 NotImplemented` |
| `GET /{bucket}/{key}?partNumber=N`（分片下载） | `501 NotImplemented` |
| Range 请求含多个区间（逗号分隔） | `501 NotImplemented` |
| 任意未知查询参数组合 | `501 NotImplemented` |

## 统一错误体

所有错误响应 `Content-Type: application/xml`：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The specified key does not exist.</Message>
  <Resource>/my-bucket/missing.txt</Resource>
  <RequestId>17A0F3C2B8D4E5F6</RequestId>
  <HostId>blink</HostId>
</Error>
```

错误码集中定义在 `internal/s3err/errors.go`，每条含 `Code`、HTTP 状态码与消息：

| Code | HTTP | 触发条件 |
| --- | --- | --- |
| `NoSuchBucket` | 404 | 目标桶不存在 |
| `NoSuchKey` | 404 | 目标对象不存在 |
| `BucketAlreadyOwnedByYou` | 409 | 重复创建同名桶（AWS 语义；`aws s3 mb` 视为成功） |
| `BucketAlreadyExists` | 409 | 桶名被占用 |
| `BucketNotEmpty` | 409 | 删除非空桶 |
| `InvalidBucketName` | 400 | 桶名违反命名约束 |
| `InvalidArgument` | 400 | 参数非法 |
| `InvalidRange` | 416 | Range 超出对象范围 |
| `NoSuchUpload` | 404 | 分片上传不存在或已中止 |
| `EntityTooSmall` | 400 | 分片小于最小允许值 |
| `InvalidPart` | 400 | 分片不存在或 ETag 不匹配 |
| `InvalidPartOrder` | 400 | 分片序号未升序 |
| `BadDigest` | 400 | `Content-MD5` 或 checksum 不匹配 |
| `InvalidDigest` | 400 | `Content-MD5` 格式非法 |
| `MalformedXML` | 400 | 请求体 XML 非法 |
| `MethodNotAllowed` | 405 | 方法不适用于该资源 |
| `PreconditionFailed` | 412 | 条件请求未满足 |
| `NotImplemented` | 501 | 请求了不支持的特性 |
| `AccessDenied` | 403 | 已认证但无权限 |
| `InvalidAccessKeyId` | 403 | Access Key 不存在 |
| `SignatureDoesNotMatch` | 403 | 签名不匹配 |
| `RequestTimeTooSkewed` | 403 | 客户端时钟偏移超限 |
| `MissingSecurityHeader` | 400 | 缺少必需的签名头 |
| `AuthorizationHeaderMalformed` | 400 | Authorization 头格式非法 |
| `SlowDown` | 503 | 磁盘水位超限，请降低请求速率 |
| `InternalError` | 500 | 服务端内部错误 |

## 逐 API 契约要点

### CreateBucket `PUT /{bucket}`

- 请求体可空（`aws s3 mb` 发送空体）；`LocationConstraint` 可选并接受。
- 成功 `200`，`Location: /{bucket}`。
- 重复创建同名桶 → `409 BucketAlreadyOwnedByYou`。
- 桶名校验：长度 3–63，仅 `[a-z0-9.-]`，首尾必须为 `[a-z0-9]`，不含 `..`，不形如 IPv4，不以 `xn--` 开头。

### HeadBucket / DeleteBucket

- `HEAD /{bucket}` 成功 `200` 空体并返回 `x-amz-bucket-region`；不存在 `404`（HEAD 无响应体）。
- `DELETE /{bucket}`：空桶 `204`；非空 `409 BucketNotEmpty`。判定"非空"需同时检查对象目录与进行中的分片上传。

### ListBuckets `GET /`

- 返回 `<ListAllMyBucketsResult>`，含固定 `Owner`（`ID: "blink"`）与按名称字典序排列的 `Buckets/Bucket{Name, CreationDate}`。

### PutObject `PUT /{bucket}/{key}`

- 必需 `Content-Length`，缺失 → `411 Length Required`；key 为空 → `400 InvalidArgument`。
- 响应含 `ETag`、`Last-Modified`；若客户端通过 `x-amz-sdk-checksum-algorithm` 或 `x-amz-checksum-*` 声明算法，**必须回传对应 checksum**。
- `If-None-Match: *` 且对象已存在 → `412 PreconditionFailed`。
- 客户端中途断开 → 丢弃临时文件，不产生任何元数据。

### GetObject / HeadObject

- 响应头：`Content-Length`、`Content-Type`（默认 `application/octet-stream`）、`ETag`、`Last-Modified`、`Accept-Ranges: bytes`、`x-amz-meta-*`、`x-amz-storage-class`。
- **Range**：支持 `bytes=start-end`、`bytes=start-`、`bytes=-suffix`；越界 → `416` 且带 `Content-Range: bytes */<size>`；多区间 → `501`。
- **条件请求**：`If-Match` 不匹配 → `412`；`If-None-Match` 匹配 → `304`（带 ETag、无体）；`If-Modified-Since` 未修改 → `304`；`If-Unmodified-Since` 已修改 → `412`。
- `ChecksumMode: ENABLED` → 必须回传 checksum 头。
- 空对象必须显式 `Content-Length: 0`，不发送 `Transfer-Encoding: chunked`。
- HeadObject 不存在 → `404` 且**无响应体**。

### DeleteObject `DELETE /{bucket}/{key}`

- **幂等**：对象不存在也返回 `204 No Content`（S3 语义，客户端依赖）。
- 桶不存在 → `404 NoSuchBucket`（此项不幂等）。
- 删除仅移除当前元数据引用，数据块交由 GC 回收（见 03 文档）。

### ListObjectsV2 `GET /{bucket}?list-type=2`

- 参数：`prefix`、`delimiter`、`max-keys`（默认与上限均为 1000）、`continuation-token`、`start-after`、`encoding-type=url`、`fetch-owner`。
- **排序必须是 UTF-8 字节序**。
- `delimiter` 折叠为 `<CommonPrefixes>`。
- `continuation-token` 为不透明值（`base64(json)`），客户端不得解析。
- `encoding-type=url` 时 Key/Prefix/Delimiter/StartAfter 均需 URL 编码。
- 空桶 → `200` 且 `KeyCount=0`（**不是** 404）；桶不存在 → `404 NoSuchBucket`。

### 分片上传家族

- `UploadId` 使用 `crypto/rand` 生成。
- `CreateMultipartUpload` 必须持久化客户端声明的 checksum 算法，它决定后续分片与完成的校验行为。
- `UploadPart`：`partNumber` 范围 1–10000；除最后一个分片外，每片不小于 5 MiB。
- `CompleteMultipartUpload` 必须校验分片序号升序（否则 `400 InvalidPartOrder`）、分片存在且 ETag 匹配（否则 `400 InvalidPart`）、尺寸约束（否则 `400 EntityTooSmall`）。
- 完成后的 `ETag` 为 `"<md5(各分片 MD5 拼接)>-<N>"`。
- 完成后先原子提交元数据与块表，再删除分片上传的临时状态。
- `AbortMultipartUpload` → `204`，清理临时状态；不存在的 uploadId → `404 NoSuchUpload`。

## 对象属性语义

| 属性 | 规则 |
| --- | --- |
| `ETag`（单块/普通上传） | 对象内容的 MD5 十六进制 |
| `ETag`（分片上传） | `"<md5(分片MD5拼接)>-<分片数>"` |
| `Last-Modified` | UTC，秒级精度，RFC 1123 格式 |
| `Content-Type` | 客户端提供，否则默认 `application/octet-stream` |
| 用户元数据 | `x-amz-meta-*`，原样保存与返回，键大小写不敏感但保留原始大小写 |
| `x-amz-storage-class` | 回传 `STANDARD` |

## 一致性语义

**强一致（strong consistency）**：

- PUT 成功后，任意 GET/HEAD/LIST 立即可见（read-after-write）。
- DELETE 成功后，任意 GET 立即 `404`，LIST 立即不再出现（read-after-delete）。
- LIST 是强一致快照，无缓存延迟。
- 覆盖写完全原子：并发读者要么看到完整的旧版本，要么看到完整的新版本，不出现撕裂。
- 相对 AWS S3（自 2020 年 12 月起提供强一致）为**相等或更强**，不构成降级。

**唯一例外**：对象在进行 Range/分片读取期间被覆盖，读取结果不作保证（AWS 同样不保证），实际行为在 `docs/design/known-limitations.md` 中记录。

## 签名与认证

- 支持 **AWS Signature Version 4**，服务名为 `s3`。
- 认证凭据为静态 Access Key / Secret Key，从配置或环境变量读取；**不提供硬编码默认值**，缺失则拒绝启动。
- 支持 **Presigned URL**（GET 与 PUT），`X-Amz-Expires` 范围 1–604800 秒。
- 时钟偏移容差 **15 分钟**，超限返回 `403 RequestTimeTooSkewed` 并附 `ServerTime`。
- 免签名端点：`/healthz`、`/readyz`、`/version`。

### SigV4 实现的四个易错点

实现于 `internal/sigv4/`，协议规范本身不再赘述，但以下四点写错会导致客户端全线签名失败：

1. **CanonicalURI 使用未解码的原始路径，且不做 `path.Clean`**。S3 的 signing name 使 URI 规范化与其它 AWS 服务不同。必须使用原始请求路径字节，而非解码后的路径。
2. **CanonicalQueryString 自行解析原始查询串并按字节序排序**。同名参数按值排序；无值参数编码为 `name=`（如 `?uploads` → `uploads=`）。不得使用会解码、合并参数的标准查询解析器。
3. **CanonicalHeaders 严格按客户端声明的 `SignedHeaders` 取**，头名小写、值前后空白裁剪、内部连续空白折叠为单个空格、同名多值以逗号连接。多包含或漏包含任一签名头都会导致不匹配。
4. **HashedPayload 取自 `X-Amz-Content-SHA256`**。普通请求须校验请求体哈希；`UNSIGNED-PAYLOAD` 按字面量处理；HTTP 下该头缺失应拒绝请求。

签名密钥为四层 HMAC 派生，末层字面量为 `aws4_request`；签名比较必须使用常数时间比较。

**性能接缝**：签名校验的结果（请求体 SHA256）直接传给存储层，使存储层在写入过程中一次性完成哈希计算，避免为验签而将请求体缓冲到内存或读取两遍。这一点对边缘大对象至关重要。

## 验收要求

每个支持的 API 都应有请求示例、响应示例、错误示例和自动化互操作测试。兼容性声明只能基于通过测试的 API 发布。首个版本的硬性门槛是 `scripts/aws-cli-smoke.sh` 全部通过。
