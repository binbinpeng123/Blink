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
| 9a | `GET` | `/{bucket}?location` | GetBucketLocation |
| 10 | `GET` | `/healthz`、`/readyz`、`/version` | 非 S3 端点，免签名 |

### 分片上传（独立切片，纳入首个可用版本）

| # | 方法 | 路径 | 说明 |
| --- | --- | --- | --- |
| 11 | `POST` | `/{bucket}/{key}?uploads` | CreateMultipartUpload |
| 12 | `PUT` | `/{bucket}/{key}?partNumber=N&uploadId=X` | UploadPart |
| 12a | `PUT` | `/{bucket}/{key}?partNumber=N&uploadId=X` + `x-amz-copy-source` | UploadPartCopy（服务端分片复制，单分片 ≤ 5 GiB） |
| 13 | `POST` | `/{bucket}/{key}?uploadId=X` | CompleteMultipartUpload |
| 14 | `DELETE` | `/{bucket}/{key}?uploadId=X` | AbortMultipartUpload |
| 15 | `GET` | `/{bucket}/{key}?uploadId=X` | ListParts |
| 16 | `GET` | `/{bucket}?uploads` | ListMultipartUploads |

**为什么分片上传必须纳入首个可用版本**：AWS CLI 对超过 8 MiB 的文件默认自动走分片上传，而边缘 AI 场景的头号负载正是 GB 级模型权重。若不实现，`aws s3 cp model.bin s3://models/` 会直接失败，README 中最核心的快速开始流程将跑不通。分片上传不影响 PutObject/GetObject 语义，因此拆分为独立切片以保持增量可验证。

### 桶级 CORS（独立切片）

| # | 方法 | 路径 | 说明 |
| --- | --- | --- | --- |
| 17 | `PUT` | `/{bucket}?cors` | PutBucketCORS |
| 18 | `GET` | `/{bucket}?cors` | GetBucketCORS |
| 19 | `DELETE` | `/{bucket}?cors` | DeleteBucketCORS |
| 20 | `OPTIONS` | `/{bucket}`、`/{bucket}/{key}` | CORS 预检；在认证之前处理 |

**为什么需要 CORS**：浏览器控制台（Blink-web）直接跨源访问 S3 端点时，带 `Authorization` 的请求必然先触发预检，而预检由浏览器发出、**不带任何凭据**。只要预检被认证中间件拦下，前端就只能退回同源代理。桶级 CORS 让浏览器可以直连，且语义与 AWS S3 一致。

### 对象级兼容操作（v0.3.0）

| # | 方法 | 路径 | 说明 |
| --- | --- | --- | --- |
| 21 | `GET` | `/{bucket}?marker=...`（不带 `list-type` 或 `list-type=1`） | ListObjects V1 |
| 22 | `PUT` | `/{bucket}/{key}` + `x-amz-copy-source` | CopyObject（服务端复制） |
| 23 | `POST` | `/{bucket}?delete` | DeleteObjects（单次最多 1000 个） |
| 24 | `GET` | `/{bucket}/{key}?attributes` | GetObjectAttributes（按需返回 ETag/Checksum/ObjectParts/ObjectSize/StorageClass） |

### 表单上传（独立切片）

| # | 方法 | 路径 | 说明 |
| --- | --- | --- | --- |
| 25 | `POST` | `/{bucket}`（`Content-Type: multipart/form-data`） | POST Object（浏览器/移动端表单直传，SigV4 POST policy） |

**为什么需要 POST Object**：浏览器与移动端 SDK 通常无法在每次上传前调用后端换取预签名 URL，而是由后端签发一份**策略文档**（policy）连同签名一起下发给客户端，客户端用普通 HTML 表单即可直传。这条路径不经过前端代理，也不会把长期凭据发到客户端；在边缘推理场景里（端侧采集设备直接把样本/权重推上去）是成本最低的上传方式。它只写单个对象，与 PutObject 共享存储路径，因此拆为独立切片，不阻塞前者。

## 明确不支持的接口

**必须显式返回 `501 NotImplemented`，不得落到泛化的 404 或 500。**

| 请求 | 响应 |
| --- | --- |
| `PUT /{bucket}/?acl`、`?tagging`、`?lifecycle`、`?encryption`、`?versioning`、`?policy`、`?replication`、`?object-lock` | `501 NotImplemented` |
| `GET /{bucket}/?versions` | `501 NotImplemented` |
| `PUT /{bucket}/{key}?acl`、`?tagging`、`?legal-hold`、`?retention` | `501 NotImplemented` |
| `POST /{bucket}/{key}?restore`、`?select`、`?select-type` | `501 NotImplemented` |
| `GET /{bucket}/{key}?torrent` | `501 NotImplemented` |
| `GET /{bucket}?policyStatus` | `501 NotImplemented`（未实现的桶属性查询，**不得**回落为列举桶） |
| 任意方法 `/{bucket}/{key}?versionId=...` | `501 NotImplemented`（本版本无多版本） |
| 非 `GET /{bucket}?location` | `405 MethodNotAllowed` |
| `POST /{bucket}` 且 `Content-Type` 不是 `multipart/form-data`（且不带 `?delete`） | `501 NotImplemented`（不猜测意图；`?delete` 仍走 DeleteObjects） |
| `PUT`/`GET`/`DELETE /{bucket}/{key}?cors`（对象级 CORS，S3 无此语义） | `501 NotImplemented` |
| Range 请求含多个区间（逗号分隔） | `501 NotImplemented` |
| 未实现的子资源（上表显式列出的 `?acl`、`?tagging`、`?versioning` 等） | `501 NotImplemented` |
| 上述之外的未知查询参数（如 `?x-id=GetObject`、`?foo=bar`） | 按 S3 惯例**忽略**，不影响响应 |

## 未实现能力对应的请求头

除子资源外，还有一批能力**只通过请求头表达**（ACL、标签、Object Lock、服务端加密、网站重定向、对象所有权）。它们不改变路由，只在写请求的头部出现。**只要请求携带这些头，就视为客户端要求了本版本未实现的能力，必须 `501 NotImplemented`，不得静默忽略。** 静默忽略的后果不是"少一个功能"，而是语义降级：带 SSE 的写入以为落盘是密文（实际明文）、带 `x-amz-acl` 的建桶以为桶是私有的（实际是服务端默认权限）——客户端收到的 `200` 与真实状态不符，且这类偏差无法从响应里察觉。

| 请求 | 拒绝的请求头 | 响应 |
| --- | --- | --- |
| `PUT /{bucket}`（CreateBucket） | ACL 组、`x-amz-bucket-object-lock-enabled`、`x-amz-object-ownership`、SSE 全组（含默认加密 `x-amz-server-side-encryption`） | `501` |
| `PUT /{bucket}/{key}`（PutObject）、`POST /{bucket}/{key}?uploads`（CreateMultipartUpload） | ACL 组、`x-amz-tagging`、Object Lock 组、SSE 全组、`x-amz-website-redirect-location` | `501` |
| `PUT /{bucket}/{key}?partNumber=…`（UploadPart）、`POST /{bucket}/{key}?uploadId=…`（CompleteMultipartUpload） | SSE 全组（分片只能延续 Initiate 的加密设置，而本版本没有加密） | `501` |
| CopyObject、UploadPartCopy | 上述对象写入组全部，另加 `x-amz-tagging-directive` 与复制源 SSE-C 组 | `501` |

补充约定：

- **ACL 组**：`x-amz-acl`、`x-amz-grant-read`、`x-amz-grant-write`、`x-amz-grant-read-acp`、`x-amz-grant-write-acp`、`x-amz-grant-full-control`。
- **Object Lock 组**：`x-amz-object-lock-mode`、`x-amz-object-lock-retain-until-date`、`x-amz-object-lock-legal-hold`。
- **SSE 全组**：`x-amz-server-side-encryption`、`x-amz-server-side-encryption-customer-algorithm`、`x-amz-server-side-encryption-customer-key`、`x-amz-server-side-encryption-customer-key-md5`、`x-amz-server-side-encryption-aws-kms-key-id`、`x-amz-server-side-encryption-context`、`x-amz-server-side-encryption-bucket-key-enabled`。
- **复制源 SSE-C 组**：`x-amz-copy-source-server-side-encryption-customer-algorithm`、`-customer-key`、`-customer-key-md5`。源对象若真是 SSE-C 加密，少了这三个头也无法解密；本版本没有加密，同样按"未实现能力"拒绝。
- 检查发生在**读取请求体之前**：拒绝时不得留下对象、桶或分片会话（Initiate 被拒后客户端继续 UploadPart 应得到 `404 NoSuchUpload`）。
- `x-amz-storage-class` 单独处理（只接受 `STANDARD`，其余 `501`，见各 API 小节），不从属于上表。
- `x-amz-expected-bucket-owner` 与 `x-amz-request-payer` 属于账户/计费语义，不承诺任何落盘属性，按 S3 兼容实现的惯例**忽略**；忽略不会造成"以为已生效"的降级。
- 头名大小写不敏感（按 HTTP 语义匹配）。

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
| `BucketAlreadyExists` | 409 | 桶名被他人占用。**单租户单节点不可达**（没有"他人"），仅作为错误码定义保留，为将来多租户/集群预留 |
| `BucketNotEmpty` | 409 | 删除非空桶 |
| `InvalidBucketName` | 400 | 桶名违反命名约束 |
| `InvalidArgument` | 400 | 参数非法 |
| `InvalidRequest` | 400 | 请求语义不成立（如自复制且未 `REPLACE`、超过 CopyObject 大小上限） |
| `InvalidRange` | 416 | Range 超出对象范围 |
| `InvalidPartNumber` | 416 | 请求的 `partNumber` 在对象中不存在（或非分片对象请求 `partNumber > 1`） |
| `NoSuchUpload` | 404 | 分片上传不存在或已中止 |
| `EntityTooSmall` | 400 | 分片小于最小允许值；POST Object 表单内容低于 `content-length-range` 下限 |
| `EntityTooLarge` | 400 | POST Object 表单内容超过 `content-length-range` 上限 |
| `MalformedPOSTRequest` | 400 | POST Object 请求体不是合法的 `multipart/form-data`（含缺少 `file` 字段、`file` 不是最后一个字段、字段重复） |
| `InvalidPolicyDocument` | 400 | POST policy 文档不是合法 JSON、缺少 `expiration`/`conditions`、或条件元素语法非法 |
| `InvalidPart` | 400 | 分片不存在或 ETag 不匹配 |
| `InvalidPartOrder` | 400 | 分片序号未升序 |
| `BadDigest` | 400 | `Content-MD5` 或 checksum 不匹配 |
| `InvalidDigest` | 400 | `Content-MD5` 格式非法 |
| `MetadataTooLarge` | 400 | 用户元数据（`x-amz-meta-*`）键值 UTF-8 字节总和超过 2 KB |
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
| `SlowDown` | 503 | 请求速率超限（保留错误码；当前版本无自动限流） |
| `ServiceUnavailable` | 503 | 集群模式下无法凑齐写/读 quorum，或扣桶时节点不可达（见 08 文档）。**不复用 `SlowDown`**：后者语义是限流 |
| `InsufficientStorage` | 507 | 磁盘空间不足，写入被拒绝 |
| `NoSuchCORSConfiguration` | 404 | 桶存在但未配置 CORS |
| `AccessForbidden` | 403 | CORS 预检被拒（未启用或规则不匹配） |
| `InternalError` | 500 | 服务端内部错误 |

## 逐 API 契约要点

### CreateBucket `PUT /{bucket}`

- 请求体可空（`aws s3 mb` 发送空体）；`LocationConstraint` 可选并接受，但**不落盘** —— `GetBucketLocation` 始终返回服务端区域。
- 成功 `200`，`Location: /{bucket}`。
- 重复创建同名桶 → `409 BucketAlreadyOwnedByYou`。
- 桶名校验：长度 3–63，仅 `[a-z0-9.-]`，首尾必须为 `[a-z0-9]`，不含 `..`，不形如 IPv4，不以 `xn--` 开头。
- 带 ACL 组、`x-amz-bucket-object-lock-enabled`、`x-amz-object-ownership` 或 SSE 头 → `501 NotImplemented`（见「未实现能力对应的请求头」），且**不得创建桶**。

### HeadBucket / DeleteBucket

- `HEAD /{bucket}` 成功 `200` 空体并返回 `x-amz-bucket-region`；不存在 `404`（HEAD 无响应体）。
- `DELETE /{bucket}`：空桶 `204`；非空 `409 BucketNotEmpty`。判定"非空"需同时检查对象目录与进行中的分片上传。

### GetBucketLocation `GET /{bucket}?location`

- 桶不存在 → `404 NoSuchBucket`；成功 `200`，响应体为 `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`。
- 返回**服务端配置的区域**（`BLINK_REGION`，默认 `us-east-1`）。本版本不支持桶级独立区域：`CreateBucket` 收到的 `LocationConstraint` 仍被接受但不落盘。
- 按 AWS 语义，`us-east-1` 返回空的 `<LocationConstraint/>`，其余区域返回区域名。
- `?location` 必须先于通用方法分派处理：非 `GET` 一律 `405 MethodNotAllowed`，**不得**回落到 `PUT`/`DELETE /{bucket}` 而误创建或误删除桶。
- 带 key 的 `?location` 无 S3 语义 → `501 NotImplemented`。

### ListBuckets `GET /`

- 返回 `<ListAllMyBucketsResult>`，含固定 `Owner`（`ID: "blink"`）与按名称字典序排列的 `Buckets/Bucket{Name, CreationDate, BucketRegion}`。每条记录都带 `BucketRegion`（AWS 2024-11 起）：多区域客户端靠它决定后续请求打向哪个端点。
- 分页参数（AWS 2024-11 契约，本版本实现）：
  - `prefix`：只返回名字以它开头的桶；响应回显 `<Prefix>`，未指定时省略。
  - `bucket-region`：只返回该区域的桶。本服务只服务一个区域，指定其它区域返回**空结果**而不是忽略参数；未指定或等于服务端区域时不按区域过滤。
  - `max-buckets`：本页最多条数，默认与上限均为 `10000`。非整数或 `< 1` → `400 InvalidArgument`；`> 10000` 按上限**截断**（与 `max-keys` 一致，客户端写超大值本意是"尽量多"）。与 `max-keys` 不同，`0` 不是合法值。
  - `continuation-token`：上一页响应中 `<ContinuationToken>` 的值。token 绑定 `prefix` 与 `bucket-region`，不一致 → `400 InvalidArgument`；不可解码或缺少续页位置 → `400 InvalidArgument`。
- 响应中的 `<ContinuationToken>` 是**下一页**的位置，仅在结果被截断且后面确实还有可见桶时出现；**不得**回显请求里的 token —— 客户端把"响应给了 token"当作"还有下一页"，最后一页回显会使其无限翻页。
- 分页边界在**可见性过滤之后**：凭证看不到的桶不占额度、也不出现在续页位置里，否则客户端会为永远拿不到的桶反复翻页。
- 桶在列举与读取 `CreationDate` 之间被删除时跳过该条，不让整个列举失败。

### PutObject `PUT /{bucket}/{key}`

- 必需 `Content-Length`，缺失 → `411 Length Required`。空 key 不会进入本操作：路由层把 `PUT /{bucket}/` 解释为 CreateBucket（与 S3 一致），因此"key 为空"的 PutObject 请求在 HTTP 上不存在。
- 响应含 `ETag`、`Last-Modified`；若客户端通过 `x-amz-sdk-checksum-algorithm` 或 `x-amz-checksum-*` 声明算法，**必须回传对应 checksum**。只要回传了 checksum，**必须同时回传 `x-amz-checksum-type`**：单次 PUT 的类型恒为 `FULL_OBJECT`（整段直算），MPU 的 `COMPOSITE` 语义见后文校验和一节。只回校验值不回类型，客户端无法区分两者，也就无法复算。
- 客户端未声明算法时**不**回传 checksum 头（不能替客户端决定要校验什么）。
- **用户元数据大小上限**：`x-amz-meta-*` 的键（去掉前缀后）与值的 **UTF-8 字节数总和 ≤ 2048**，超限 → `400 MetadataTooLarge`（与 AWS 一致）。按字节而非字符计数——同样是 1024 个字符，ASCII 占 1 KB、中文占 3 KB，按字符计会让上限形同虚设；也不按 `len` 之外的口径做归一化（键已由 Go 规范化大小写，前缀不计入总和）。超限时**整条请求拒绝、不写入任何对象**，不静默截断：客户端以为元数据完整落盘、读回却少一半，比明确失败更难排查。
- ACL 组、`x-amz-tagging`、Object Lock 组、SSE 全组、`x-amz-website-redirect-location` → `501 NotImplemented`（见「未实现能力对应的请求头」），且不写入对象。检查先于读请求体，`Expect: 100-continue` 的客户端在发送正文之前就能拿到拒绝。
- 条件写（在存储层 key 锁内判定，检查与提交之间不插入其他写者）：
  - `If-None-Match: *` 且对象已存在 → `412 PreconditionFailed`。
  - `If-None-Match: "etag"`：当前对象 ETag **等于**给定值 → `412 PreconditionFailed`；不相等或对象不存在 → 放行。服务端按去掉 `W/` 与引号后的值做弱比较。
  - `If-Match: "etag"`：对象不存在或 ETag 不匹配 → `412 PreconditionFailed`；服务端按去掉 `W/` 与引号后的值比较。
  - `If-Match: *`：对象不存在 → `412 PreconditionFailed`，仅要求"存在"。
  - 两种条件同时出现时逐个求值，任一不满足即 `412`（宁可少写一次，不能出错放行）。
  - 逗号分隔的多值列表、空 ETag → `400 InvalidArgument`；静默取第一个会把"任一匹配"变成"只认第一个"。
  - **墓碑与已过期对象在逻辑上不存在**：`If-Match` 对它们一律 `412`，`If-None-Match`（`*` 或具体 ETag）对它们一律放行。否则客户端会因为一个自己已经看不见的对象拿到无法恢复的 412。
- 客户端中途断开 → 丢弃临时文件，不产生任何元数据。

### 流式上传（`Content-Encoding: aws-chunked`）

HTTPS 传输下 aws-cli/botocore 不会把对象字节直接作为请求体，而是发送 aws-chunked 分块框架。服务端必须在读取请求体前解帧，**落盘的只能是解帧后的对象字节**；否则 ETag 与校验和都"看起来正常"，但内容是框架本身，属于最难发现的数据损坏。

- 接受四种载荷哈希：`STREAMING-UNSIGNED-PAYLOAD`、`STREAMING-UNSIGNED-PAYLOAD-TRAILER`、`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`、`STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`。
- 必须带 `x-amz-decoded-content-length`（解帧后字节数），缺失 → `411 LengthRequired`，非法 → `400 InvalidArgument`。解帧器按它校验块长与总长，长度不符 → `400`.
- 签名分块逐块验证 `chunk-signature` 签名链，任何一块不匹配（含结束块与 trailer 签名）→ `403 SignatureDoesNotMatch`；未签名分块出现签名、或签名分块缺少签名 → `400 InvalidArgument`。
- `x-amz-trailer` 声明的 trailer 必须真的出现在请求体末尾；声明了校验和算法但没有值头、也没有在 `x-amz-trailer` 中声明对应名字 → `400 InvalidArgument`。trailer 中的校验和与解帧后内容不符 → `400 BadDigest`。
- 解帧后从 `Content-Encoding` 中移除 `aws-chunked`，保留其它编码（如 `gzip`），避免元数据里残留一个已被消除的编码。
- ETag、`Content-MD5`、对象校验和全部按**解帧后**的内容计算。PutObject 与 UploadPart 走同一条解帧路径。

**已用 aws-cli 2.36.2 实测核实**（HTTPS，2026-10-24）：`s3 cp` 与 `s3api put-object` 均发送 `STREAMING-UNSIGNED-PAYLOAD-TRAILER`，`X-Amz-Trailer: x-amz-checksum-crc32` 或 `x-amz-checksum-crc64nvme`；20 MiB 多分片上传的 3 个 UploadPart 全部走该路径（服务端日志 `signed:false`），下载后逐字节比对一致；显式 `--checksum-algorithm CRC32` 上传的 trailer 值与本地 `zlib.crc32` 一致。**签名分块模式仅有独立的签名向量单测，尚无真实客户端验证**。

### POST Object `POST /{bucket}`（`multipart/form-data`）

浏览器/移动端表单直传。请求不是 XML/JSON，而是 `multipart/form-data`：普通字段承载签名与元数据，最后一个名为 `file` 的 part 是对象内容。

**路由与解析**

- 桶级 `POST` 且 `Content-Type` 为 `multipart/form-data` → 本操作；带 `?delete` 仍走 DeleteObjects（`?delete` 优先）；其余桶级 POST → `501 NotImplemented`。
- `Content-Type` 的 boundary 缺失或非法 → `400 MalformedPOSTRequest`。字段名按小写匹配（`File`、`policy` 等价）。
- 同名普通字段出现多次 → `400 MalformedPOSTRequest`（无法判断哪一个是权威值）。单个字段值上限 64 KiB、普通字段总数上限 64，超限 → `400 MalformedPOSTRequest`。
- `file` 必须是**最后一个** part；其后还有 part → `400 MalformedPOSTRequest`。缺少 `file` → `400 MalformedPOSTRequest`。文件内容长度无法预先得知（multipart 没有整体 `Content-Length` 契约），因此存储层以"未知长度"写入，不要求 `Content-Length`。

**鉴权：SigV4 POST policy**

表单必须携带 `x-amz-algorithm`（恒为 `AWS4-HMAC-SHA256`）、`x-amz-credential`、`x-amz-date`、`policy`（base64）、`x-amz-signature`。

- 签名对象是 **base64 编码后的 policy 字符串本身**：`hex(HMAC-SHA256(signingKey(secret, dateStamp, region, "s3"), base64Policy))`。先解码再签名是最常见的实现错误，此处明确。
- `x-amz-credential` 格式与 Authorization 头一致（`AKID/dateStamp/region/s3/aws4_request`），其 dateStamp 必须等于 `x-amz-date` 的日期部分；`region` 必须等于服务端区域；Access Key 必须存在。任一不符 → `400 AuthorizationHeaderMalformed`（格式类）或 `403`（`SignatureDoesNotMatch` / `InvalidAccessKeyId`），与头部签名同一套错误码与状态码。
- POST policy 自带 `expiration`，**不叠加** 15 分钟时钟容差：`x-amz-date` 只用于派生签名密钥，请求是否有效由 policy 的 `expiration` 决定。因此离线签发、稍后上传的流程可用。
- 未带任何签名字段 → `403 AccessDenied`（与缺少签名头的普通请求一致）。

**Policy 文档**

`policy` 是 base64 编码的 JSON，解码后 ≤ 20 KiB（与 AWS 一致），结构：

```json
{
  "expiration": "2026-11-25T12:00:00Z",
  "conditions": [
    {"bucket": "models"},
    ["starts-with", "$key", "weights/"],
    ["eq", "$Content-Type", "application/octet-stream"],
    ["content-length-range", 0, 1048576],
    {"x-amz-meta-origin": "edge"}
  ]
}
```

- 非法 JSON、未知顶层字段、缺少 `expiration` 或 `conditions` → `400 InvalidPolicyDocument`。
- `expiration` 为 RFC3339 时间；已过期 → `403 AccessDenied`，消息 `Invalid according to Policy: Policy expired.`。
- 条件支持四种形式：`{"field": "value"}`（等价 `eq`）、`["eq", "$field", "value"]`、`["starts-with", "$field", "prefix"]`、`["content-length-range", min, max]`。其它形式或元素个数不符 → `400 InvalidPolicyDocument`。
- 字段名以小写比较，可带 `$` 前缀也可不带；**值区分大小写**，按精确匹配（`starts-with` 按前缀）。字段不存在时按空串参与匹配。
- `${filename}` 占位符在**表单值**与**条件值**两侧同时替换为 `file` part 的文件名后才比较，用于约束"文件名必须以某前缀开头"这类策略。
- **覆盖规则**：除 `file`、`policy`、`x-amz-signature` 与 `x-ignore-*` 外，表单里出现的每个字段都必须被某条条件覆盖；未被覆盖 → `403 AccessDenied`，消息 `Invalid according to Policy: Extra input fields: <field>`（与 AWS 一致）。缺字段不算失败——条件只要求"若出现则匹配"。。
- `key` 必须出现在表单中且被条件覆盖，否则同样按条件失败拒绝；空 `key` → `400 InvalidArgument`。
- 表单出现 `bucket` 字段时必须等于 URL 中的 bucket，否则条件失败（表单与 URL 指向不同桶会让"签给 A 桶上传到 B 桶"成立）。
- `content-length-range` 同时约束上下限：内容超过上限 → `400 EntityTooLarge`；低于下限 → `400 EntityTooSmall`。判定在流式写入过程中进行，超限时立即中止，不写入任何对象。

**元数据与校验和**

- 表单中的 `Content-Type`、`Cache-Control`、`Content-Disposition`、`Content-Encoding`、`Content-Language`、`Expires` 与 `x-amz-meta-*` 按与 PutObject 相同的规则落盘；`x-amz-meta-*` 同样受 2 KB 上限约束（超限 → `400 MetadataTooLarge`）。
- 对象 `Content-Type` 依次取表单 `Content-Type`、`file` part 自带的 `Content-Type`、默认 `application/octet-stream`。
- `x-amz-checksum-<alg>` 表单字段按 PutObject 的校验和语义校验并回传，同时回传 `x-amz-checksum-type: FULL_OBJECT`。
- 不支持表单字段 `Content-MD5`（没有任何浏览器表单会生成它，支持它只会扩大攻击面）：出现即 `400 InvalidArgument`。

**未实现能力字段**

表单字段等价于请求头。下列字段出现 → `501 NotImplemented`，且**在读 file 内容之前**拒绝，不留下对象：`acl`、ACL 授权组（`x-amz-grant-*`）、`tagging`、Object Lock 组（`x-amz-object-lock-*`）、SSE 组（`x-amz-server-side-encryption*`）、`x-amz-website-redirect-location`、`redirect`、`x-amz-security-token`（本版本无 STS）、非 `STANDARD` 的 `x-amz-storage-class`。SigV2 字段 `AWSAccessKeyId`/`signature` 同样 `501`（不支持 SigV2 签名，静默按 SigV4 失败会让老客户端误以为是密钥问题）。

**响应**

- 默认 `204 No Content`，带 `ETag`（带引号）、`Last-Modified` 与相对 `Location: /{bucket}/{urlencoded key}`。
- `success_action_status`：
  - `200` → `200` 空体；
  - `201` → `201`，体为 `<PostResponse><Location>…</Location><Bucket>…</Bucket><Key>…</Key><ETag>…</ETag></PostResponse>`，`Content-Type: application/xml; charset=utf-8`；
  - `204`（默认）→ `204` 空体；
  - 其它值 → `400 InvalidArgument`（不静默回落默认值，否则客户端按自己设定的状态码分支会走错）。
- `success_action_redirect`：合法 `http`/`https` 绝对 URL 时返回 `303 See Other` + `Location`（附加查询参数 `bucket`、`key`、`etag`，`etag` 不带引号），**优先于** `success_action_status`；值非法 → `400 InvalidArgument`。不校验 URL 主机（这是调用方自己的页面），只拒绝非 http(s) scheme 与无法解析的值。
- 失败时响应体与其它 API 一致（S3 XML 错误体），但浏览器表单拿不到正文，诊断依赖 `x-amz-request-id` 头。

### GetObject / HeadObject

- 响应头：`Content-Length`、`Content-Type`（默认 `application/octet-stream`）、`ETag`、`Last-Modified`、`Accept-Ranges: bytes`、`x-amz-meta-*`、`x-amz-storage-class`，以及对象上已持久化的标准 HTTP 元数据（`Cache-Control`、`Content-Disposition`、`Content-Encoding`、`Content-Language`、`Expires`，见「对象属性语义」）。
- **Range**：支持 `bytes=start-end`、`bytes=start-`、`bytes=-suffix`；越界 → `416` 且带 `Content-Range: bytes */<size>`；多区间 → `501`。
- **条件请求**：`If-Match` 不匹配 → `412`；`If-None-Match` 匹配 → `304`（带 ETag、无体）；`If-Modified-Since` 未修改 → `304`；`If-Unmodified-Since` 已修改 → `412`。`304` 额外回传 `Cache-Control` 与 `Expires`（RFC 7232 要求影响缓存判定的头保持一致）。
- **If-Range**（配合 `Range` 的续传校验）：值为 ETag 时按**强匹配**比较对象 ETag——弱标签（`W/"..."`）一律不匹配，因为 RFC 9110 §13.1.5 规定 If-Range 只接受强校验；值为 HTTP 日期时，仅当对象未被修改（`Last-Modified <= 该日期`）才匹配。匹配 → 正常 `206`；不匹配或值无法解析 → **忽略 `Range`，返回 `200` 整个对象**（服务端不能对可能已变化的实体做区间覆盖）。
  - 求值的永远是**对象本身**的 ETag/修改时间，与 `If-Match` 等常规条件头彼此独立：If-Range 只在"已经决定要发 206"这一步起作用，`If-None-Match`/`If-Modified-Since` 的 304/412 仍按 RFC 9110 §13.2.2 的优先级先行判定。
- `ChecksumMode: ENABLED` → 必须回传 checksum 头，并同时回传 `x-amz-checksum-type`。
- **部分响应（206）不回传任何 checksum 头**：存储的是整个对象的校验和，附在一段字节上会让按规范校验的客户端判定为不匹配。实测 `aws s3 cp` 下载 12 MiB 对象发出两个 Range 请求（`0-8388607`、`8388608-12582911`），依赖的正是这一条。
- 空对象必须显式 `Content-Length: 0`，不发送 `Transfer-Encoding: chunked`。
- HeadObject 不存在 → `404` 且**无响应体**。

#### 分片下载 `GET`/`HEAD /{bucket}/{key}?partNumber=N`

- 仅对分片上传产生的对象有意义：完成时把分片边界写入 `meta.json` 的 `parts`（`{number,size}`，偏移由累计和推导），不提升 `format_version`。
- 分片存在且未触发 If-Range 回退 → `206`，`Content-Range: bytes <start>-<end>/<对象总大小>`（绝对偏移），并带 `x-amz-mp-parts-count: <分片数>`。
- `Range` 相对**分片**解析，`Content-Range` 仍用对象绝对偏移；分片内越界 → `416 InvalidRange`，`Content-Range: bytes */<分片大小>`。
- 分片号不在对象的 `parts` 中 → `416 InvalidPartNumber`；`partNumber` 非整数或不在 1–10000 → `400 InvalidArgument`（与 `UploadPart` 同一套编号校验）。
- **`If-Range` 与 `partNumber` 同时出现**：If-Range 不成立时回退的是**整个分片**（`200` + 该分片的完整字节），不是整个对象。客户端按分片号续传时，它要的就是这一分片；把整个对象发回去会让 `x-amz-mp-parts-count` 与 `Content-Range` 的语义全部失配。
- 非分片对象：`partNumber=1` 返回 `200` 完整对象（不伪造 `Content-Range`）；`partNumber>1` → `416 InvalidPartNumber`。
- 空分片（长度为 0）→ `200` + `Content-Length: 0`，不构造非法 `Content-Range`。
- `206` 分片响应不回传对象级校验和；普通 `GET`/`HEAD` 不带 `x-amz-mp-parts-count`。
- 带 `uploadId` 的 `GET` 仍走 `ListParts`；`uploadId` + `partNumber` 继续 `501`。
- **复制后的对象不保留分片边界**：`CopyObject` 与 `UploadPartCopy` 的目标边界由本次上传决定，源对象边界不继承。

#### 响应头覆盖 `response-*`

预签名 URL 最常用的能力：在不复制对象的前提下，让同一次下载按调用方要求返回不同的响应头（典型场景是浏览器直链下载时临时指定 `Content-Disposition: attachment; filename=...`）。

- 支持的查询参数与覆盖目标：`response-cache-control` → `Cache-Control`、`response-content-disposition` → `Content-Disposition`、`response-content-encoding` → `Content-Encoding`、`response-content-language` → `Content-Language`、`response-content-type` → `Content-Type`、`response-expires` → `Expires`。
- 仅 `GET` 生效（含 `partNumber` 分片下载）。`HEAD` 携带这些参数时**忽略**：HEAD 没有响应体，覆盖实体头没有意义，S3 也只对 GET 定义该行为。
- `200`/`206` 覆盖全部六个头；`304` 只覆盖 `Cache-Control` 与 `Expires`——RFC 9110 要求 304 回传会影响缓存判定的头，而 `Content-Type` 等实体头不属于 304。
- 参数存在但值为空时视为**未提供**，保留对象自身元数据：返回空 `Content-Type` 会让部分客户端按未知类型处理甚至触发内容嗅探，与"显式清空"的收益不成比例。
- 值含控制字符（CR、LF、NUL 及其它 `< 0x20`、除 HTAB 外）→ `400 InvalidArgument`。不静默清洗成空格：客户端会以为覆盖按原值生效。
- 覆盖只改变本次响应头，不修改持久化元数据，也不影响 `ETag`、`Range`、`Content-Length`、checksum 语义。
- 本服务没有匿名读，所有请求都在鉴权之后；参数位于 SigV4 canonical query 中，未签名篡改会导致 `403 SignatureDoesNotMatch`（与其它签名失败一致；S3 对此类请求不回 401），因此不需要额外的"仅签名请求可用"检查。

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
- 编码字符集：RFC 3986 unreserved 字符（`A-Za-z0-9-_.~`）与路径分隔符 `/` 原样保留，其余按字节大写百分号编码。**`/` 必须保留**——客户端会拿编码后的值与原始 key 做字符串比较，`models%2Fsource.bin` 会被判成不存在（AWS、MinIO 亦保留 `/`）。
- 空桶 → `200` 且 `KeyCount=0`（**不是** 404）；桶不存在 → `404 NoSuchBucket`。

### ListObjects V1 `GET /{bucket}`、`GET /{bucket}?list-type=1`

- 与 V2 共用同一份扫描/折叠实现，差别只在起点的表示与续页协议。
- 参数：`prefix`、`delimiter`、`marker`、`max-keys`（默认与上限均为 1000）、`encoding-type=url`。
- `marker` 是上一页最后一条的明文 Key；**以 `delimiter` 结尾时视为公共前缀游标**，下一页跳过该前缀下的全部 key。
- `NextMarker` **只在 `delimiter` 非空且发生截断时返回**；无 `delimiter` 时客户端用最后一条 `Contents` 的 Key 续页。
- `encoding-type=url` 时 Key/Prefix/Delimiter/Marker/NextMarker 均编码，字符集与 V2 相同（`/` 原样保留）。
- `list-type` 只接受空、`1`、`2`；其他值 → `400 InvalidArgument`。

### CopyObject `PUT /{bucket}/{key}`（带 `x-amz-copy-source`）

- 源路径格式：`/{srcBucket}/{srcKey}` 或 `{srcBucket}/{srcKey}`，按 URL 路径规则解码；先按**未解码**的路径切分桶与 key，`%2F` 不会被误判为分隔符。
- 带 `?versionId` → `501 NotImplemented`（无版本控制，不静默忽略）。
- 元数据指令：`x-amz-metadata-directive` 为空或 `COPY` 时保留源 `Content-Type`、`x-amz-meta-*` 与标准 HTTP 元数据（`Cache-Control` 等）；`REPLACE` 时以请求头为准，**请求里没带的标准 HTTP 元数据会被清空**（而不是沿用源对象），缺省 `Content-Type` 落 `application/octet-stream`；其他值 → `400 InvalidArgument`。`REPLACE` 时请求带上的用户元数据同样受 2 KB 上限约束（超限 → `400 MetadataTooLarge`）；`COPY` 时请求里的 `x-amz-meta-*` 本就**不参与**目标元数据，因此不参与校验——为一份将被丢弃的头报错，会让"复制时顺手带了个无用头"变成无法完成的复制。
- 源条件：`x-amz-copy-source-if-match` / `if-none-match` / `if-modified-since` / `if-unmodified-since`，不满足 → `412 PreconditionFailed`；源不存在时优先 `404 NoSuchKey`。
- 目标条件：与 PutObject 同一套规则 —— `If-None-Match`（`*` 或单个 ETag）与 `If-Match`（单个 ETag 或 `*`），在存储层目标 key 锁内判定；不满足 → `412 PreconditionFailed`，非法多值 → `400 InvalidArgument`。目标上的墓碑与已过期对象同样视为不存在。
- 自复制：源与目标相同且 `COPY` → `400 InvalidRequest`；`REPLACE` 允许，用于原地更新元数据。
- 大小上限：源对象 > 5 GiB → `400 InvalidRequest`。
- `x-amz-storage-class` 只接受 `STANDARD`（缺省同义），其余 → `501 NotImplemented`。
- `x-amz-copy-source-range` **只在 CopyObject 上** → `501 NotImplemented`：分片复制需要独立协议，静默忽略会让客户端以为复制了一部分、实际复制了全部。`UploadPartCopy` 支持该头（见分片上传一节），两条路径的差异仅此一项。
- 复制操作的 ACL/标签/Object Lock/SSE/网站重定向/复制源 SSE-C 头 → `501`，完整列表见「未实现能力对应的请求头」。`UploadPartCopy` 用同一份列表。
- 响应：`200` + `<CopyObjectResult><LastModified>…</LastModified><ETag>"…"</ETag></CopyObjectResult>`。
- 单机模式是 **O(1) 元数据复制**（目标 meta 直接引用源块表，CAS 文件数不变）；集群模式为流式复制（源经 quorum 读、目标经 quorum 写），**不宣称零拷贝**。
- 客户端注意：aws-cli 的 `s3 cp s3://… s3://…` 在对象超过 `s3.multipart_threshold`（默认 8 MiB）时改用 `UploadPartCopy`，本版本已支持。但 aws-cli 2.36.2 的默认 `--copy-props default` 会在复制前调用 `GetObjectTagging`（本版本 `501`），因此**大对象服务端复制需显式加 `--copy-props none`**；这与 UploadPartCopy 能力无关。

### DeleteObjects `POST /{bucket}?delete`

- 请求体 `<Delete>`，单次最多 1000 个 `<Object>`；超过或 XML 非法 → `400 MalformedXML`；请求体上限 1 MiB。
- 可选 `Content-MD5`：格式非法 → `400 InvalidDigest`，不匹配 → `400 BadDigest`。
- `<VersionId>` 非空 → `501 NotImplemented`（整请求拒绝，不静默忽略）。
- 桶不存在 → 整请求 `404 NoSuchBucket`，不逐条返回。
- **部分成功是正常响应**：单条失败只出现在 `<Error>` 里，HTTP 仍为 `200`；`<Deleted>`/`<Error>` 按请求顺序返回。
- `<Quiet>true</Quiet>` 时只返回 `<Error>`。
- 删除是幂等的：删除不存在的 key 计入 `<Deleted>`。

### 校验和（checksum）

- 支持算法：`CRC32`、`CRC32C`、`CRC64NVME`、`SHA1`、`SHA256`；请求头 `x-amz-checksum-<alg>` 或 `x-amz-sdk-checksum-algorithm` 二选一即可，两者都给时必须一致（不一致 → `400 InvalidArgument`）。算法头已确定算法后，**其他算法**的 `x-amz-checksum-*` 值头一律 `400 InvalidArgument`，不静默丢弃。
- 类型规则（**仅 MPU**；单次 `PutObject` 恒为 `FULL_OBJECT`）：`CRC64NVME` 未声明类型时默认 `FULL_OBJECT`（分片完成时用 CRC combine 合并，**不重读数据**）；`CRC32`/`CRC32C` 默认 `COMPOSITE`；`SHA1`/`SHA256` 只支持 `COMPOSITE`。
- `COMPOSITE` 的值 = 对各分片**原始校验值字节**再算一次，形如 `<base64>-<分片数>`。
- 分片下载（206）不回传校验和；完整对象下载、HEAD（带 `x-amz-checksum-mode: ENABLED`）、分片完成响应都回传。

### GetObjectAttributes `GET /{bucket}/{key}?attributes`

- 必填请求头 `x-amz-object-attributes`：逗号分隔的属性名，合法值为 `ETag`、`Checksum`、`ObjectParts`、`StorageClass`、`ObjectSize`（大小写不敏感，忽略空白）。头缺失、为空、含空项或含未知属性 → `400 InvalidArgument`。
- 响应体根元素 `<GetObjectAttributesResponse xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`，只包含被请求的属性；未请求的属性**不出现**（不能用空元素冒充）。
- `ETag`：不带引号（与 GET/HEAD 响应头不同）。
- `Checksum`：仅在请求 `Checksum` 且对象有已持久化校验和时返回；算法值放在对应的 `<ChecksumCRC32>` / `<ChecksumCRC32C>` / `<ChecksumCRC64NVME>` / `<ChecksumSHA1>` / `<ChecksumSHA256>` 元素里，并带 `<ChecksumType>`（`FULL_OBJECT` / `COMPOSITE`）。对象无校验和时整个 `<Checksum>` 省略，而不是返回空元素。
- `ObjectSize`：对象字节数。
- `StorageClass`：本版本恒为 `STANDARD`。
- `ObjectParts`：请求时总会出现。非分片上传产生的对象返回空分片集合与 0 计数。
  - 参数 `max-parts`（缺省 1000，上限 1000，非负整数）与 `part-number-marker`（缺省 0，非负整数），语义与 ListParts 一致：只返回编号大于 marker 的分片。
  - 元素 `<MaxParts>` 回显**生效**的 `max-parts`（请求值超过 1000 时按 1000 截断后回显），`<PartNumberMarker>` 回显请求的 marker；`<IsTruncated>` 表示是否还有下一页；`<PartsCount>` 是本页返回的分片数，`<TotalPartsCount>` 是对象分片总数；截断时带 `<NextPartNumberMarker>`。
  - 每个 `<Part>` 含 `<PartNumber>`、`<Size>`，以及该分片的原始校验和（仅当对象有校验和时）。
- 桶不存在 → `404 NoSuchBucket`；对象不存在 → `404 NoSuchKey`；非 `GET` → `405 MethodNotAllowed`；带 `versionId` / `uploadId` / `partNumber` → `501 NotImplemented`（本版本无多版本，不允许把属性查询降级成对当前对象或某次上传的查询）。
- 响应头带 `Last-Modified`。
- 分片校验和从 `meta.json` 的 `parts[].checksum` 读取（可选字段，不提升 `format_version`；旧 meta 没有该字段时省略分片校验和，而不是回退为对象级校验和）。

### 分片上传家族

- `UploadId` 使用 `crypto/rand` 生成。
- `CreateMultipartUpload` 必须持久化客户端声明的 checksum 算法，它决定后续分片与完成的校验行为。
- `CreateMultipartUpload` 同时持久化 `Content-Type`、`x-amz-meta-*` 与标准 HTTP 元数据（`Cache-Control` 等）到会话状态，`Complete` 时原样写入对象元数据。`x-amz-meta-*` 受 2 KB 上限约束（超限 → `400 MetadataTooLarge`，且**不创建会话**——拒绝后客户端若继续 UploadPart 会得到 `404 NoSuchUpload`，这比"会话建了但元数据被砍"更早暴露问题）。分片请求（`UploadPart`）上的同名头**不参与**对象元数据：一次会话只有一个最终对象，属性只能在 Initiate 时声明一次。
- Initiate 上的 ACL/标签/Object Lock/SSE/网站重定向头 → `501`，且不创建会话；UploadPart 与 Complete 上的 SSE 头 → `501`（见「未实现能力对应的请求头」）。Complete 被拒时**不消耗会话**，去掉该头后可用同一 `uploadId` 重试。
- `UploadPart`：`partNumber` 范围 1–10000；除最后一个分片外，每片不小于 5 MiB。
- `ListParts`（以及 `GetObjectAttributes` 的 `ObjectParts`）的分页语义：`max-parts` 非负整数，`part-number-marker` 非负整数，只返回编号大于 marker 的分片，按编号升序。`max-parts=0` 时本页必为空；此时若仍有分片，`IsTruncated` 必须为 `true`，`NextPartNumberMarker` 回显请求的 marker（客户端须改用非零 `max-parts` 才能推进）——把"没让你看"报成 `IsTruncated=false` 会让客户端以为会话里没有分片。
- `UploadPartCopy`：与 `UploadPart` 同路由，以 `x-amz-copy-source` 非空为唯一判据分流。支持 `x-amz-copy-source-range: bytes=first-last`（两端必填，不支持开放/后缀/多区间）；缺省复制整个源对象。源条件与 CopyObject 同一套 `x-amz-copy-source-if-*`；源不存在 → `404 NoSuchKey`；区间越界 → `416 InvalidRange` 且带 `Content-Range: bytes */<源大小>`；单次复制 > 5 GiB → `400 InvalidRequest`。响应 `<CopyPartResult><LastModified>…</LastModified><ETag>"…"</ETag></CopyPartResult>`，ETag 按目标分片内容重算，与随后 `ListParts` 一致。
- 单机与集群的 `UploadPartCopy` 都走 `OpenObject` + `io.Pipe` + `UploadPart`：**不是零拷贝**，数据经协调者读一次、写一次（与集群 CopyObject 同理）。
- `CompleteMultipartUpload` 必须校验分片序号升序（否则 `400 InvalidPartOrder`）、分片存在且 ETag 匹配（否则 `400 InvalidPart`）、尺寸约束（否则 `400 EntityTooSmall`）。
- `CompleteMultipartUpload` 与 `PutObject`、`CopyObject` 目标共用同一套写条件：`If-None-Match`（`*` 或单个 ETag）与 `If-Match`（单个 ETag 或 `*`），失败 → `412 PreconditionFailed`。单机在存储层 key 锁内判定；集群在写前 `probeMetas` barrier 上尽力而为判定（与普通写入同一例外）。条件失败**不消耗分片会话**，客户端去掉条件后可用同一 `uploadId` 重试；接受后忽略会把"仅当目标版本未变才提交"降级成无条件覆盖。
- 完成后的 `ETag` 为 `"<md5(各分片 MD5 拼接)>-<N>"`。
- 完成后先原子提交元数据与块表，再删除分片上传的临时状态。
- `AbortMultipartUpload` → `204`，清理临时状态；不存在的 uploadId → `404 NoSuchUpload`。

**已用 aws-cli 2.36.2 实测核实**（`--debug` 抓报文，2026-10-24）：

| 场景 | 客户端实际行为 | 服务端必须如何应对 |
| --- | --- | --- |
| `create-multipart-upload` 不带 `--checksum-algorithm` | 不发任何算法头 | 会话初始算法为空 |
| 上述会话里 `upload-part` | **仍**自动带 `x-amz-checksum-crc64nvme` 与 `x-amz-sdk-checksum-algorithm: CRC64NVME` | 不能拒绝：**采用首个分片声明的算法并持久化**；此后混用其它算法一律 `400`，否则一个对象里会混两种校验和 |
| `complete-multipart-upload` | 请求体为 `<CompleteMultipartUpload><Part><ETag>hex</ETag><PartNumber>N</PartNumber></Part>…`，**无分片校验和、无 `x-amz-checksum-type` 头** | 校验和只能由会话状态 + 各分片已存校验和推导；完成响应在 XML 内含 `<ChecksumCRC64NVME>`/`<ChecksumType>` |
| 完成响应 | 客户端按 XML 解析（不读 `x-amz-checksum-*` 头） | 校验和必须写进 XML 体；头部副本可选但建议同时给 |
| `complete` 的 ETag 元素 | **不带引号**（botocore 已剥离） | 比对前统一去引号 |

上述 `MpuObjectSize` 不在必需路径上：aws-cli 的 `s3 cp` 与 `s3api` 均未发送，服务端不依赖它做校验。

### 桶级 CORS `?cors` 与 OPTIONS 预检

语义对齐 AWS S3（MinIO、Ceph RGW 亦同），差异只在于错误消息文本：

- **作用域**：仅 `PUT`/`GET`/`DELETE /{bucket}?cors`。带 key 的 `?cors` 无 S3 语义 → `501`；桶级其它方法 → `405 MethodNotAllowed`。
- **PUT**：请求体为 `<CORSConfiguration>`，成功 `200` 空体。非法文档 → `400 MalformedXML`，且**不落盘**（已有配置保持不变）。
- **GET**：未配置 → `404 NoSuchCORSConfiguration`（与"桶不存在"的 `404 NoSuchBucket` 可区分）；桶不存在 → `404 NoSuchBucket`。
- **DELETE**：成功 `204`，幂等；从未配置也返回 `204`。

**文档校验**（任一不满足即 `400 MalformedXML`）：

| 约束 | 取值 |
| --- | --- |
| 根元素 | `<CORSConfiguration>` |
| 规则数 | 1–100 |
| 每规则 | 至少一个 `<AllowedOrigin>` 与一个 `<AllowedMethod>` |
| 方法 | 仅 `GET`/`PUT`/`HEAD`/`POST`/`DELETE`（大小写不敏感，归一化为大写） |
| 通配符 | `AllowedOrigin`、`AllowedHeader` 每条至多一个 `*`，可出现在任意位置（与 AWS 一致，如 `https://*.example.com`） |
| `MaxAgeSeconds` | 非负整数；未给出时响应不出现 `Access-Control-Max-Age` |
| 请求体大小 | ≤ 64 KiB；根元素之后不得有尾随内容（拒绝拼接攻击） |

**OPTIONS 预检**（在认证**之前**处理，预检请求不带 `Authorization`）：

- 缺 `Origin` → `400 InvalidArgument`（`Insufficient information. Origin request header needed.`）；缺 `Access-Control-Request-Method` → `400 InvalidArgument`。
- 桶未配置 CORS → `403 AccessForbidden`，消息 `CORSResponse: CORS is not enabled for this bucket.`
- 无规则同时匹配 origin + method + 请求头 → `403 AccessForbidden`，消息 `CORSResponse: This CORS request is not allowed.`
- 匹配成功 → `200` 空体，带 `Access-Control-Allow-Origin`、`Access-Control-Allow-Methods`（该规则声明的全部方法）、`Access-Control-Allow-Headers`（回显请求的头列表）、`Access-Control-Expose-Headers`、`Access-Control-Max-Age`，以及 `Vary: Origin, Access-Control-Request-Headers, Access-Control-Request-Method`。
- 匹配顺序：按文档书写顺序，取**第一条**全匹配的规则。

**实际请求**（非 OPTIONS，带 `Origin`）：命中规则时注入 `Access-Control-Allow-Origin`（匹配模式为裸 `*` 时回 `*`，否则回显 origin）、`Access-Control-Expose-Headers` 与 `Vary`；**不**注入预检专用头。未命中时静默不加头，业务结果照常返回——这与 S3 一致，浏览器会自行拦截响应。

**凭据**：仅当回显具体 origin（即匹配模式不是裸 `*`）时附加 `Access-Control-Allow-Credentials: true`。裸 `*` 与凭据在浏览器中互斥，同时发送会被浏览器拒绝，因此不得无条件附加。

**存储**：配置落在 `buckets/<bucket>/cors.json`，与 `bucket.json` 分离——CORS 是可变配置，混进桶元数据会让"改 CORS"与"读桶元数据"互相阻塞。删桶时随桶目录一并清除，同名桶重建后不继承旧配置。

## 对象属性语义

| 属性 | 规则 |
| --- | --- |
| `ETag`（单块/普通上传） | 对象内容的 MD5 十六进制 |
| `ETag`（分片上传） | `"<md5(分片MD5拼接)>-<分片数>"` |
| `Last-Modified` | UTC，秒级精度，RFC 1123 格式 |
| `Content-Type` | 客户端提供，否则默认 `application/octet-stream` |
| 用户元数据 | `x-amz-meta-*`，键大小写不敏感（Go 的 `http.Header` 会规范成 `X-Amz-Meta-Owner`），**响应必须以全小写头名回传**。实测 botocore 按原始大小写匹配 `x-amz-meta-` 前缀，规范化后的大写头会被解析成 `Metadata.Owner` 而非 AWS 约定的 `Metadata.owner`，`--query Metadata.owner` 直接得到 `None`；真实 S3 返回的也是小写头名。键（去前缀）与值的 UTF-8 字节总和上限 2 KB，超限 `400 MetadataTooLarge`（见 PutObject 契约） |
| `x-amz-storage-class` | 回传 `STANDARD` |
| `Cache-Control` | 客户端提供则原样持久化并在 GET/HEAD 回传；不解释其语义 |
| `Content-Disposition` | 同上。浏览器直接打开对象时的下载/展示行为依赖它（如 `attachment; filename="model.bin"`） |
| `Content-Encoding` | 同上。`aws-chunked` 是 SigV4 传输层分块框架，**解帧后必须剥离**，其余编码（如 `gzip`）保留；多个编码按 `, ` 连接回传 |
| `Content-Language` | 同上 |
| `Expires` | 同上，保留客户端给出的 HTTP 日期文本（不解析、不重格式化），回传逐字节一致 |

标准 HTTP 元数据随 `meta.json` 持久化（字段见 03 文档），PUT/CopyObject/分片上传三条写路径行为一致；为空时不写入 JSON，也不在响应里回传空头。

## 一致性语义

**强一致（strong consistency）**：

- PUT 成功后，任意 GET/HEAD/LIST 立即可见（read-after-write）。
- DELETE 成功后，任意 GET 立即 `404`，LIST 立即不再出现（read-after-delete）。
- LIST 是强一致快照，无缓存延迟。
- 覆盖写完全原子：并发读者要么看到完整的旧版本，要么看到完整的新版本，不出现撕裂。
- 相对 AWS S3（自 2020 年 12 月起提供强一致）为**相等或更强**，不构成降级。

**唯一例外**：对象在进行 Range/分片读取期间被覆盖，读取结果不作保证（AWS 同样不保证），实际行为在 `docs/design/known-limitations.md` 中记录。

### 集群模式下的证明与例外（v0.2.0）

上述承诺在分布式模式由 `W+R>N` 的 quorum 交集保证，详见 [08 文档](./08-distributed-storage.md)：

- 默认 `N=3, W=2, R=2`。写集合与读集合必然交叠，读到的最高版本不早于已确认的写版本。
- 该证明依赖**静态成员**：不做自动成员变更、不做共识。网络分区时多数派可写，少数派返回 `503 ServiceUnavailable`。
- **例外一**：条件写（`If-None-Match`、`If-Match`）在并发协调者下是尽力而为，可能双成功，不等价于 AWS 的强串行化。
- **例外二**：`PUT` 返回 5xx 时可能已部分生效（部分副本已写入）。5xx 后重试必须幂等。
- **例外三**：集群 `LIST` 复杂度为 O(节点数 × 对象数)，不承诺单机模式的 `<500ms` 目标。
- `DELETE` 返回成功后，任意节点立即 404、LIST 不再返回该 key，由 tombstone 保证；tombstone 保留期内不会发生"删除后旧副本复活"。

## 签名与认证

- 支持 **AWS Signature Version 4**，服务名为 `s3`；支持 aws-chunked 流式载荷（未签名与签名分块，含 trailer）。
- 认证凭据为静态 Access Key / Secret Key，从配置或环境变量读取；**不提供硬编码默认值**，缺失则拒绝启动。
- 支持 **Presigned URL**（GET 与 PUT），`X-Amz-Expires` 范围 1–604800 秒。
- 时钟偏移容差 **15 分钟**，超限返回 `403 RequestTimeTooSkewed` 并附 `ServerTime`。
- 凭据来源：环境变量（单凭据，全权限）或凭据文件（多凭据，每个凭据可限定桶、key 前缀与动作）。签名通过但动作不允许时返回 `403 AccessDenied`；未签名的数据请求返回 `403 AccessDenied`。授权范围与语义见 06 文档。
- `CopyObject` / `UploadPartCopy` 同时要求**源对象 `read`** 与**目标 `write`** 权限；任一不满足即 `403 AccessDenied`，且不执行复制。
- 免签名端点：`/healthz`、`/readyz`、`/version`、`/metrics`。
- **CORS 预检（`OPTIONS`）在签名校验之前处理**：浏览器发预检时不会携带 `Authorization`，若先验签则所有跨源请求都会在预检阶段失败。这是浏览器规范要求的顺序，不是鉴权缺口——真正的 `PUT`/`GET`/`DELETE` 请求仍必须签名（回归测试 `TestCORSBypassesAuthOnlyForPreflight` 固定这一点）。

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
