# 测试与性能基准

评审状态：已允许开发
评审人：binbinpeng123
评审日期：2026-10-23
适用版本：v0.1.0（单节点，默认）；v0.2.0（集群与 EC，见"集群测试"）

## 测试层次

| 层次 | 范围 | 运行方式 |
| --- | --- | --- |
| 单元测试 | 元数据、路径、布局、校验和、错误处理、签名算法 | `go test ./...`，无网络、无磁盘依赖 |
| 模糊测试 | 所有解析客户端输入的纯函数（见"模糊测试"小节） | 种子随 `go test ./...`；有界探索用 `scripts/fuzz.sh` |
| 集成测试 | S3 API、文件系统、重启恢复 | `go test -tags integration ./test/integration/` |
| 互操作测试 | AWS CLI 真实二进制；纯标准库手写客户端 | `scripts/aws-cli-smoke.sh`、`scripts/tls-smoke.sh` 与 `test/interop/` |
| 故障测试 | 进程中断、磁盘满、对象损坏、并发冲突 | `faultFS` 注入 + 子进程 `SIGKILL` |
| 集群测试（v0.2.0） | 跨节点读写、quorum、tombstone、修复队列、跨节点 MPU | `scripts/cluster-smoke.sh 3`（本机 3 进程） |
| 纠删码测试（v0.2.0） | GF 运算、柯西矩阵求逆、任意 ≤m 片重建 | `internal/ec` 单测 + `cluster-smoke.sh 6 --ec` |
| 部署测试 | Docker、Kubernetes、K3s | 需开发机；本阶段只能做到 `--dry-run` 与本地构建 |
| 性能测试 | 小对象、大对象、范围读取、并发读写 | `go test -bench`，见下 |

### 单元测试重点

- **SigV4**：AWS 官方测试向量、CanonicalQueryString 排序、未解码 URI、头值空白折叠、时钟偏移边界、Presigned URL 有效期与篡改、`UNSIGNED-PAYLOAD` 处理、HTTP 下缺失 payload hash 的拒绝。
- **存储**：路径哈希确定性、块切分（整除与余数）、元数据往返与未知字段容忍、原子写入在故障注入下的最终状态、格式版本高于二进制时拒绝启动。
- **路由与响应**：分派表全表驱动、不支持参数返回 `501`、Range 解析边界（`bytes=0-0`、开放区间、后缀区间、越界、非法）、XML 转义。
- **CRC64NVME**：与已知向量比对，并验证分片校验和的合并结果等于整体计算值。
- **CORS**：配置往返（含 `MaxAgeSeconds` 指针语义）、重启后仍可读、未配置与桶不存在可区分、删除幂等、删桶后同名重建不继承旧配置、写入为深拷贝、损坏文件报 `ErrCorrupt`；协议层覆盖 `?cors` 三种方法、10 类非法 XML 全部回 `400` 且不破坏已有配置、100 条规则上限、单通配符匹配（`*`、前缀、后缀、大小写敏感 origin vs 不敏感 header）、预检成功/缺 Origin/缺方法/未配置/源不匹配/方法不匹配/请求头不匹配/桶不存在、实际请求头注入、以及"预检免签但配置读写仍需签名"。

### 模糊测试（协议解析基线）

单元测试只覆盖"想得到的输入"。协议解析层面对的是**客户端完全控制的字节**，
因此每个解析函数都配一个 Go 原生 `Fuzz` 目标，断言的不只是"不 panic"，
还包括解析成功时必须成立的不变量：

| 目标 | 核心不变量 |
| --- | --- |
| `FuzzParseRange` | 区间完整落在对象内；成功结果重写为 `bytes=start-end` 后解析一致 |
| `FuzzParseCopyPartRange` | 空串等价整段；非空时严格在界内；结果可重写再解析 |
| `FuzzS3URLEncode` | 输出只含 unreserved + `/` + 合法 `%XX`；`PathUnescape` 逐字节还原 |
| `FuzzDecodeListToken` | 能解码则 `After` 非空；重签后解码语义不变 |
| `FuzzParseTrailerNames` | 名字非空、小写、字符集合法、去重；与 `hasTrailerName` 一致 |
| `FuzzParseCopySource` | `Bucket`/`Key` 非空；重新百分号编码拼接后解析结果相同（覆盖"先切分后解码"语义） |
| `FuzzParseContentMD5` | 成功值为 16 字节 base64 对应的 32 位小写十六进制 |
| `FuzzParseDecodedContentLength` | 成功值等于去空白后的十进制字面值，且非负 |
| `FuzzDecodeCORSConfiguration` | 成功时 1–100 条规则、每条 ≥1 Origin 与 ≥1 Method、Method 为大写白名单成员、Origin/Header 至多一个 `*`、`MaxAgeSeconds` 非负；重复解析相等；**可回写**——编码成响应 XML 后再解码得同一配置 |
| `FuzzCorsPatternMatches` | `*` 匹配一切；无通配符时等价全等（或 `EqualFold`）；有通配符时匹配 ⇔ 长度足够且首尾对齐（非折叠时双向） |
| `FuzzSplitHeaderList` | 结果项非空且无首尾空白；空列表只来自"全分隔符/空白"；拼接后再拆分幂等 |
| `FuzzParseListParams` | `max-keys ∈ [0,1000]` 且缺省 1000；`encoding-type` 只能是空或 `url`；token 与游标一一对应、游标位置非空且与本次 `prefix`/`delimiter` 绑定；重复解析一致 |
| `FuzzParsePartNumber` | 成功值在 `1..10000` 且与十进制字面值相等；`FormatInt` 重写后解析可逆 |
| `FuzzCanonicalQueryString` | 规范化幂等；键值对按原始编码字节序非降，键中无 `=` |
| `FuzzParseChunkHeader` | 长度非负；签名缺省或 64 位十六进制；块头可重写再解析 |
| `FuzzParseTrailerLine` | 名字非空、已小写、字符集合法；`name:value` 可重写再解析 |
| `FuzzParseAuthorization` | 成功结果 `accessKeyID`/`signature` 非空、`dateStamp` 8 位、`signedHeaders` 非空；重复解析逐字段相等 |
| `FuzzParsePostPolicy` | 成功时过期时刻非零、每条条件 `op ∈ {eq, starts-with, content-length-range}`，字段名已去 `$`/空白并小写，`content-length-range` 满足 `0 ≤ min ≤ max`；重复解析结果相等 |
| `FuzzValidateBucketName` | 与**独立重写**的 AWS 规则做差分对照；失败必须是 `ErrInvalidBucketName`；确定性 |
| `FuzzParsePartFileName` | 编号落在 `1..10000`；重写为规范文件名后解析一致 |

运行方式：种子语料随 `go test ./...` 每次执行（毫秒级，充当回归）；有界随机探索用

```text
scripts/fuzz.sh            # 每个目标 20 s
scripts/fuzz.sh 60         # 每个目标 60 s
scripts/fuzz.sh 20 FuzzParseRange FuzzParseChunkHeader
```

脚本只解析内存字符串，**不启动服务、不写数据目录、不联网**。某目标失败时 Go
会自动把输入写入 `testdata/fuzz/<目标>/<哈希>`，此后 `go test` 会把它当回归用例执行。

**实测（2026-11-25，Apple M4 / macOS 24.6.0 / Go 1.27.2，`scripts/fuzz.sh 10`，
10 个 worker）**：19 个目标全部 PASS，合计约 **1 259 万次执行**；执行次数最高的为
`FuzzParseChunkHeader`（139 万）、`FuzzParsePartFileName`（130 万）、`FuzzParseAuthorization`（130 万）。

- 唯一被证伪的断言：`FuzzParseCopySource` 在 20 s 运行时约 2.5 s 找到 `%2F/0`
  （`{Bucket:"/", Key:"0"}`）。复核结论是**测试不变量过严而非产品缺陷**：`parseCopySource`
  先按第一个 `/` 切分再解码是设计正确的；解码出的非法桶名由下游 `store.CopyObject`
  入口的 `validateBucketName` 在拼接路径前拒绝（400），不存在路径穿越。断言已删除并加注释，
  该输入保留为回归种子（`internal/s3api/testdata/fuzz/FuzzParseCopySource/92f6a12f1a449e4d`）。
- `FuzzSplitHeaderList` 在 20 s 运行时 0.4 s 找到 `",　"`（逗号 + 全角空格）。
  复核结论同样是**测试不变量过严**：`splitHeaderList` 用 `strings.TrimSpace` 判定空白，
  它覆盖全部 Unicode 空白，因此这串确实只含分隔符；而断言里的 `strings.Trim` 只认
  ASCII 空白，把全角空格当成了"实际内容"。断言已改为 `strings.TrimFunc` + `unicode.IsSpace`，
  该输入保留为回归种子（`internal/s3api/testdata/fuzz/FuzzSplitHeaderList/d6c12b8553560571`）。
- `FuzzValidateBucketName` 在第 3 秒后出现 `0 execs/sec` 停顿（Go fuzzer 在最小化超长输入），
  11 s 内只完成约 3.8 万次执行，**探索深度明显低于其它目标**；这是记录事实，不视为通过证据的等价物。
- `FuzzDecodeCORSConfiguration` 10 s 内约 8.6 万次执行，同样低于同类目标：每次成功都要做
  XML 解码 + 一次重编码往返，单次成本高。它最初的 64 KiB 超限种子会让 fuzzer 反复最小化
  那个巨大输入（20 s 里有 9 s 是 `0 execs/sec`），种子已移出 fuzz 语料，超限行为改由
  确定性的 `TestBucketCORSValidationBodyLimit` 覆盖（同时验证"超限请求不破坏已有配置"）。

### HTTP handler 级 fuzz

纯解析函数的 fuzz 覆盖不到"中间件链 + 路由 + handler + 存储"的组合错误：某个头只在
路由之后才被读取、某条错误分支忘了写 XML、某个分支在部分写出响应后 panic——这些只有
把完整请求送进 `Server.Handler()` 才会暴露。

`FuzzServeHTTP`（`internal/s3api/fuzz_http_test.go`）构造完整 `http.Request`（方法、
路径、原始查询串、请求头块、请求体）送进全链路，连接真实的临时 store（`BlockSize` 与
内联阈值同为 64 KiB，请求体走内联、不产生 CAS 块文件），并显式关闭鉴权，以便 fuzz 探索
业务路由而不是反复落到 403。

不变量：

- **不 panic**：`recoverPanic` 只应在"没人在代码里 panic"时保持沉默。任何被兜底成
  `500 InternalError` 的输入都视为缺陷（响应体里出现 `InternalError` 即失败）。
- **状态码在契约集合内**：`200/204/206/304/307/400/403/404/405/409/411/412/413/416/501/503/507`。
  `307` 只可能来自 `ServeMux` 的路径清理（`//a`、`/..` 之类的规范化重定向），不属于
  S3 语义；它保留请求方法，是 Go 标准库当前的规范化重定向码。
- **错误体是合法 XML**：状态码 ≥ 400 且非 HEAD 时，`Content-Type` 必须是
  `application/xml`，响应体必须能解码成 `<Error>` 且 `<Code>` 非空。这是
  "统一错误体"契约的机器可验证形式——缺了它，客户端拿到的是 `net/http` 的纯文本兜底。
- **无体状态码不携带正文**：`204`、`304` 的响应体必须为空。

规模护栏：请求体 ≤ 1 KiB、路径与查询串 ≤ 512 B、头块 ≤ 1 KiB，超出直接跳过。目的是
不让一个巨大随机输入把整段时长耗在最小化与磁盘写入上（与 `FuzzDecodeCORSConfiguration`
移除 64 KiB 种子的教训相同）。

```text
go test -run=^$ -fuzz=^FuzzServeHTTP$ -fuzztime=20s ./internal/s3api
```

普通 `go test` 只跑种子语料，覆盖根路径列举、桶与对象 CRUD、`?delete`、`?uploads`、
复制、`?cors` 预检与 `?acl`/`?versionId` 拒绝等代表请求。

首次有界运行（90 s，约 220 万次执行）即发现三处缺陷，均已修复并转成确定性回归：

1. **状态码集合漏了 307**：`GET /..` 被 `ServeMux` 规范化重定向。这是契约缺口，
   不是 handler 缺陷——`307` 已进允许集合，并补了路径清理的种子。
2. **NUL 桶名返回 500**：桶级入口（`DeleteBucket`、`?cors`）在缓存未命中时直接拿桶名
   拼路径，`open` 的 `EINVAL` 被当作未知错误。修复是在 `loadBucketMeta` 缓存未命中
   处校验桶名；HTTP 级与存储级回归测试各一组。
3. **非法 UTF-8 对象 key 污染整个桶**：key 进 JSON 元数据时非法字节被替换成 U+FFFD，
   而对象目录名是对原始字节的哈希；此后 LIST 读回元数据与目录名不一致，把桶判成
   数据损坏（整桶 500）。修复是在 `validateKey` 入口拒绝非法 UTF-8。

**未覆盖**：集群/EC 路径与 TLS。并发下的状态机不做 fuzz，改由确定性并发用例覆盖（见「并发冲突」）。有界时长本身也不构成完备性证明。

### 鉴权链路 fuzz

`FuzzServeHTTP` 显式关闭鉴权，因此签名解析、凭据查找、错误响应这几段代码在随机输入下
没有全链路保护；纯解析目标 `FuzzParseAuthorization` 也看不到"鉴权中间件 + 路由"的组合。
`FuzzServeHTTPAuth`（`internal/s3api/fuzz_auth_test.go`）补这一段：

- **有效签名模式**：对模糊输入构造的请求，用测试凭据现场 `sigv4.Presign` 签名后再送进
  启用鉴权的 `Server.Handler()`。除 POST Object（签名材料在表单里）外，响应不得是任何
  鉴权失败码——有效签名必须抵达业务逻辑。输入查询串里原有的 `X-Amz-Algorithm` 等
  签名字段先剔除，避免与现场签发的参数重复；其余查询参数原样保留。
- **篡改模式**：签名后按输入选择一种破坏（追加签名字节、删除签名参数、替换 Access Key、
  签名后改路径、把 `X-Amz-Date` 改成很久以前、删除 `X-Amz-Algorithm`）。响应必须是
  `400` 或 `403`，错误码属于鉴权失败集合（`AccessDenied` / `SignatureDoesNotMatch` /
  `InvalidAccessKeyId` / `RequestTimeTooSkewed` / `AuthorizationHeaderMalformed` 等），
  不得出现 5xx 或 `InternalError`。
- **两种模式共同断言**：不 panic、状态码在契约集合内、`RequestId` 响应头与错误体一致、
  错误体是合法 S3 XML、响应正文不出现 Secret Key（鉴权失败日志与错误消息都不得回显凭据）。

```text
go test -run=^$ -fuzz=^FuzzServeHTTPAuth$ -fuzztime=20s ./internal/s3api
```

普通 `go test` 会跑种子语料：有效签名的桶/对象/分片/批量删除主链路，以及六种篡改。

实测（同日，10 worker，20 s）：

- `FuzzParsePostPolicy` 约 **156 万次**执行，无失败。
- `FuzzServeHTTPAuth` 约 **7–8 万次**执行，无失败；速率明显低于其它目标，运行中多次出现持续
  数秒的 `0 execs/sec`。用 `-parallel=1` 可复现同样的停顿、随后恢复满速，说明停顿来自
  fuzzer 最小化新覆盖输入（每一步都完整跑一遍 handler + 临时存储），不是死锁或请求悬挂。
- 首次运行暴露的是 **fuzz 装配代码自身的缺陷**：现场签发的 URL 经 `url.Parse` 往返时，
  查询串里的原始 `#` 被截成 fragment，有效签名被误判为匿名请求。改为按第一个 `?`
  切分路径与查询串（Presign 不改路径），并把该输入保留为种子；产品代码未改动。

### 集成测试重点

- Bucket 生命周期与幂等性；桶名校验端到端。
- 对象往返、零字节对象（ETag 必须为 `d41d8cd9...`）、大对象分块、用户元数据、`Content-Type` 默认值、`Content-MD5` 不匹配、`If-None-Match` 前置条件。
- 用户元数据 2 KB 上限：`PutObject`、`CreateMultipartUpload`、`REPLACE` 的 `CopyObject` 超限 → `400 MetadataTooLarge` 且不留下对象/会话；恰好 2048 字节的边界值原样往返。
- 未实现能力请求头：CreateBucket/PutObject/CreateMultipartUpload 上的 ACL、标签、Object Lock、SSE、网站重定向头 → `501 NotImplemented`，且不留下桶/对象/会话；UploadPart/Complete 的 SSE 头 → `501` 且不消耗会话；`x-amz-expected-bucket-owner` 等无落盘语义的头仍被忽略（不过度拒绝）。
- Range 全部形态、条件请求 304/412、多区间返回 501。
- ListObjectsV2 分页（断言无重复无遗漏）、前缀、分隔符折叠、`start-after`、UTF-8 字节序、`encoding-type=url`。
- 分片上传正常路径、ETag 格式、分片过小、序号错乱、ETag 不匹配、中止后完成返回 404。
- `GetObjectAttributes` 的 `ObjectParts` 与 `ListParts` 同分页口径：`max-parts=0` 时本页为空、`IsTruncated=true`、`NextPartNumberMarker` 回显请求的 marker（`TestGetObjectAttributesMultipartParts`、`TestListPartsMaxPartsZero`）。
- 认证：无签名、错误 Access Key、错误签名、免签名端点、Presigned GET/PUT。
- CORS：`PUT`/`GET`/`DELETE ?cors` 往返、`OPTIONS` 预检在无 `Authorization` 下返回 200、跨源实际请求带 `Access-Control-Allow-Origin`。
- 重启：对象保留、分片上传状态保留、格式版本拒绝启动、`bucket.json` 缺失或损坏时桶与对象仍可访问（`internal/store/bucket_heal_test.go`）。
- 垃圾回收：`bucket.json` 运行期损坏、`ListBuckets` 已跳过该桶时，GC 仍按桶目录枚举对象元数据，该桶引用的块不被回收，真正的孤儿块照常回收（`TestSweepGarbageProtectsObjectsUnderCorruptBucketMeta`）。
- 并发：同 key 并发写后内容与某一份完整输入逐字节相等；写入期间持续读取不出现撕裂。
- 损坏：直接改动 CAS 中一个字节后读取应报错而非返回错误数据；元数据损坏与块表截断的处理。JSON 合法但结构不自洽（块表截断/偏移空洞/长度非正、分片总长不符、未知布局）时，`OpenObject` 与 `CopyObject` 必须返回 `ErrCorrupt`（`TestOpenObjectRejectsStructurallyCorruptMeta`）。

## 故障测试方法

### 进程中断

1. **故障注入（主力手段）**：`internal/store/fs.go` 定义 `FileSystem` 接口，测试实现包装真实文件系统的 `faultFS`，在第 N 次 `Sync` 或 `Rename` 注入错误。这是唯一能精确、可复现地覆盖"写到一半"所有中间态的方法，因此作为主力。
2. **真实杀进程**：集成测试以子进程模式启动服务，父进程在随机延迟后发送 `SIGKILL`；恢复后断言结果只可能是"对象不存在"或"内容逐字节相等"。
3. **掉电模拟**：macOS 无法可靠模拟，**本阶段不做**，改由故障注入覆盖"fsync 后 rename 前"这一最关键窗口。此限制必须记录，不得声称已测试掉电。

**核心断言（不可妥协）**：任何时刻，对象要么不存在，要么内容完整。**绝不接受"对象存在但内容残缺"。**

### 磁盘满

不能用配置项模拟 —— 那只能测水位逻辑，测不到真实的 `ENOSPC` 路径。分三个层次：

1. **故障注入 `ENOSPC`**：断言返回 `507 InsufficientStorage`、临时文件被清理，且**读取与删除仍然工作**（用户须能通过删除自救）；腾出空间后自动恢复接受写入；水位滞回不抖动。
2. **真实小容量文件系统**（可选目标，默认不跑）：macOS 用稀疏磁盘镜像，Linux 用 `loop` 挂载。这是最真实的路径。
3. **填盘脚本** `scripts/fill-disk.sh`：用于开发机复测，不进 CI。

### 并发冲突

- 32 个 goroutine 并发写同一 key，断言最终内容与其中一份完整输入逐字节相等。
- 写入新版本期间持续 GET，断言每次拿到的是完整旧版本或完整新版本。
- 并发 LIST 与 PUT，断言 LIST 结果始终自洽。
- 分片会话状态机（`internal/store/mpu_concurrency_test.go`），断言"会话要么完整存在、要么彻底消失，对象要么不存在、要么内容完整"：
  - 16 个写者并发覆盖同一分片编号：全部成功，每个返回的 ETag 属于某份完整输入；ListParts 只看到一个分片且大小/ETag 匹配；Complete 后内容与该输入逐字节相等。
  - Complete 与 Abort 同抢一个会话（20 轮）：恰好一个成功；Complete 赢则对象完整、会话消失且 Abort 得 `NoSuchUpload`；Abort 赢则对象不存在、Complete 得 `NoSuchUpload`。
  - UploadPart 与 Abort 同抢（20 轮）：Abort 必成功，UploadPart 要么成功要么 `NoSuchUpload`；结束后会话与对象都不存在。
  - 8 个分片并发上传期间持续 ListParts：每页编号严格升序、大小为正、ETag 为 32 位十六进制；结束后恰好 8 个分片。

## 集群测试（v0.2.0）

设计见 [08 分布式存储设计](./08-distributed-storage.md)。验证手段是**本机多进程模拟**：起 3 或 6 个独立进程，各自数据目录与端口，用真实 S3 客户端经不同节点读写。这能覆盖协议与 quorum 逻辑，**不能**覆盖真实网络分区、跨主机时钟偏差与 K8s DNS。

### 单机零回归（硬门槛）

- `BLINK_CLUSTER_MODE=single`：不监听 9100、不启动修复/反熵 goroutine、不建立任何 Peer 连接。
- `go test -race ./...` 全绿；`scripts/aws-cli-smoke.sh` 原有 12 步全绿。
- `BenchmarkGetRangeSequential`、`BenchmarkList100kObjects` 相对基线回退 **<3%**；超过则不合入。

### 3 进程副本模式

`scripts/cluster-smoke.sh 3`（S3 端口 9001–9003，内部端口 19100–19102，数据目录独立，测试结束清理）：

1. 任意节点建桶 → 任意节点上传 → 其它两个节点 GET，`cmp` 逐字节一致。
2. HEAD 的 `Content-Length`/`ETag` 与上传一致；Range 读取与单机结果一致。
3. LIST 在任意节点返回完整结果，分页无重复无遗漏。
4. 跨节点分片上传：Create 在一节点、分片打在不同节点、Complete 在第三个节点，结果可读。
5. 删除后立即在**所有**节点返回 404，且 LIST 不再出现（tombstone 生效）。
6. `kill -9` 一个节点：剩余两节点读写正常；重启后修复队列补齐，三副本内容一致。
7. 杀两个节点：写/读返回 `503 ServiceUnavailable`，不返回错误数据，也不伪装成 404。
8. `BLINK_READ_DEGRADED=false`（默认）下，R=2 无法凑齐时返回 503；置 `true` 后可从单一副本读取。

### 6 进程 EC 模式

`scripts/cluster-smoke.sh 6 --ec`（`4+2`）：写入 ≥32 MiB 对象，手工删除其中 2 个 shard 后读取仍逐字节一致；删除 3 个 shard 必须报错而非返回错误数据。

### EC 单元测试（穷举）

对 `2+1` 与 `4+2` 两个 profile，**穷举所有 ≤m 片的丢失组合**，断言 `Reconstruct` 逐字节还原；同时验证矩阵求逆、单 shard 损坏检测、编码吞吐基准。未通过前不得默认开启 EC。

### 集群模式的指标预期

| 指标 | 预期 | 说明 |
| --- | --- | --- |
| 跨节点读写正确性 | 100% | 强制门槛，无妥协 |
| 单节点故障下可用性 | 读写均可用 | `W=2,R=2,N=3` |
| 集群 LIST | **不沿用单机 `<500ms`** | 成本约为单机的 N 倍；只做回归对照，不设绝对目标 |
| 写放大 | 约 N 倍（副本）/ 1.5 倍（EC） | 副本与 EC 的空间代价分别记录 |

## 互操作测试

固定端点 `http://127.0.0.1:9000`。凭据通过环境变量显式传入，不依赖开发者本机 `~/.aws` 配置。

`scripts/aws-cli-smoke.sh` 覆盖：健康检查 → Bucket 生命周期 → 小对象往返 → 大对象（自动分片上传）往返 → Range 读取 → HEAD → 显式 `CRC64NVME` 与 `SHA256` 校验算法 → `--checksum-mode ENABLED` → 手工分片上传全流程 → `sync` → `presign` → 不支持接口返回 501 → 清理。

另在 `test/interop/` 提供**纯标准库手写的 S3 客户端**。它与 `internal/sigv4` 的校验逻辑是独立实现，二者交互成功构成实现级交叉验证，且能构造 AWS CLI 构造不出的边界请求。

**MinIO Client (`mc`) 测试**：开发环境未安装 `mc`，本阶段**无法执行**，必须标记为"待补充"，**不得写成已通过**。

### HTTPS / aws-chunked 互操作

测试条件：aws-cli 2.36.2，`--endpoint-url https://127.0.0.1:9443 --no-verify-ssl`，自签证书（CN=`blink-local`，SAN `DNS:localhost,IP:127.0.0.1`），服务端 TLS 1.2+，本机 Apple M4 / macOS 24.6.0。执行日期 2026-10-24。

- 20 MiB 对象经默认 multipart 上传（3 个 UploadPart），再下载并逐字节 `cmp` 一致；同内容重复上传时块全部去重（日志 `deduped_blocks` 等于块数）。
- `s3 sync` 目录往返（含 2.9 MiB 随机二进制）`diff -r` 无差异。
- `s3api put-object --checksum-algorithm CRC32` 回传的校验和与本地 `zlib.crc32` 一致，证明 trailer 校验和是在**解帧后**内容上计算的。
- 服务端 debug 日志确认每个 PutObject/UploadPart 都进入解帧路径（`aws_chunked_decoded`，`signed:false`，trailer 为 `x-amz-checksum-crc32`/`crc64nvme`）。
- **未覆盖**：生产 CA、TLS 证书轮换。这些不得写成已通过。签名分块的端到端覆盖见下节（真实 aws-cli 不发送该形态）。

**签名分块端到端**（`internal/s3api/chunked_signed_test.go`，2026-11-25）：`sigv4` 包原有解帧与签名单测复用了产品代码的私有函数（`canonicalTrailer`、`stringToSign`），同错同对；本用例在 `s3api` 包内**独立实现** SigV4 头部签名与块签名链，走完整链路（鉴权中间件 → `decodeChunkedBody` → 存储）：

- `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` 多块 PUT → 200，ETag 为内容 MD5，签名 GET 回读逐字节一致。
- `...-PAYLOAD-TRAILER` + `x-amz-checksum-crc32`：回传校验和与内容一致；trailer 值伪造（签名链本身合法）→ 400 `BadDigest`。
- 篡改任一块 `chunk-signature` → 403 `SignatureDoesNotMatch`，对象未落盘（GET 404）；篡改 `x-amz-trailer-signature` → 403。
- 块链断裂（第二块仍以 seed 为 prev）→ 403；`x-amz-decoded-content-length` 与实际解帧长度不符 → 400 `InvalidArgument`。
- 鉴权关闭（无 `ChunkSigner`）时签名分块请求 → 500 `InternalError`，绝不跳过验签继续落盘。

**信任链与 TLS 策略**（`test/integration/tls_test.go`，2026-11-25）：测试现场用 ECDSA P-256 生成本地 CA 与服务器证书（SAN `DNS:localhost,IP:127.0.0.1`），客户端**只信任该 CA**，不再使用 `--no-verify-ssl`：

- 信任 CA 的客户端完成 `/healthz`、桶/对象往返与 Presigned URL 下载；断言协商版本 ≥ TLS 1.2、对端证书与 fixture 一致。
- 不信任 CA → `x509.UnknownAuthorityError`；主机名不匹配 → `x509.HostnameError`；客户端降到 TLS 1.1 → 握手失败；明文访问 TLS 端口 → `400` 或断开。
- 证书与私钥不匹配 → 二进制拒绝启动（错误含「加载 TLS 证书失败」）。

**生态客户端信任链**（`scripts/tls-smoke.sh`，2026-11-25）：脚本现场生成同一组本地 CA 与服务器证书，aws-cli 2.36.2 通过 `AWS_CA_BUNDLE` 校验信任链，**不使用 `--no-verify-ssl`**：

- `create-bucket`/`head-bucket`、1 KiB 对象 PUT/HEAD/GET（`cmp` 一致）、Presigned URL 经 `curl --cacert` 下载（`cmp` 一致）全部通过。
- 不设置 `AWS_CA_BUNDLE` 的 aws-cli 与默认信任库的 `curl` 均失败，证明信任链确实被校验而非跳过。
- `curl --tls-max 1.1` 握手失败，生态客户端侧再次验证 TLS ≥ 1.2。
- debug 日志确认 aws-cli 的 PutObject 进入 `aws_chunked_decoded`（`signed:false`，trailer `x-amz-checksum-crc64nvme`），即 TLS 与真实客户端的 aws-chunked 路径同时走通。
- **仍未覆盖**：生产 CA、证书轮换、mTLS（06 明确不做）。

## 记录指标

- 首字节延迟与完整对象读取延迟。
- 上传与下载吞吐、P50/P95/P99 延迟。
- 不同对象大小与并发数下的 CPU、内存、磁盘 I/O。
- 重启恢复耗时与恢复后的数据完整性。
- **写入字节数**（用于验证去重与分片完成的零重写）。

### 基准测试清单

```text
BenchmarkPutObjectSizes/{1KiB,64KiB,1MiB,16MiB}
BenchmarkGetObjectSizes/{同上}
BenchmarkGetRangeRandom/1GiB              模拟随机读
BenchmarkGetRangeSequential               模拟模型顺序加载
BenchmarkHTTPGetObject                    真实中间件链 + TCP 的端到端整对象 GET
BenchmarkPutSmallObject10k                 创新 D 对照组
BenchmarkList100kObjects                   创新 D 对照组
BenchmarkConcurrentPut/{4,16,64}（当前实现：BenchmarkConcurrentPut1MiB，64 worker）
BenchmarkConcurrentGet/{4,16,64}
BenchmarkDedupSecondCopy/1GiB              创新 A 验收
BenchmarkCompleteMultipart20GB             创新 B 验收
BenchmarkCRC64NVME/1GiB                    校验开销量化
BenchmarkCRC32C/1GiB                       对照组
BenchmarkSigV4Verify                       签名开销
```

**实际执行范围（2026-11-07）**：写入档位为 1 KiB / 64 KiB / 1 MiB / 16 MiB。
8 MiB 并入 16 MiB 档（16 MiB 同样跨 4 个 4 MiB 块）；128 MiB / 1 GiB 受
"本机压测总写入 ≤ 512 MiB"约束未执行，不得写成已通过。上表中
`BenchmarkGetRangeRandom/1GiB`、`BenchmarkDedupSecondCopy/1GiB`、
`BenchmarkCompleteMultipart20GB` 目前仍是**计划项**，未实现为可执行基准。

`BenchmarkHTTPGetObject` 已于 2026-11-24 实现（`test/bench/http_read_bench_test.go`），
补齐"存储读路径"与"HTTP 写出路径"之间的缺口：其余 GET 基准把响应写进
`io.Discard`（它自带 `ReaderFrom`），量不到中间件是否挡住了零拷贝。该基准走
真实的 `s3api` 中间件链与 `httptest.NewServer` 的 TCP 回环，16 MiB 对象、
`-benchtime=60x`；默认不随 `go test ./...` 运行。切片 28 用它测出
`statusRecorder` 未透传 `io.ReaderFrom` 导致的约 2.2× 吞吐差（569–612 → 1237–1327 MB/s）。

`BenchmarkPutSmallObject10k` 已于 2026-11-10 实现（`test/bench/small_object_bench_test.go`），
补齐创新 D 的前置基线：10,000 个 1 KiB 对象、默认内容内联、`-benchtime=1x`，
本机（Apple M4/APFS）总耗时 86.4 s、P50 8.8 ms、P95 10.9 ms、P99 13.0 ms、
115.7 objects/s。结论与本版本不做 D 的理由见 03 文档。该基准单次迭代写入
10 MiB，满足本机压测写入约束；默认不随 `go test ./...` 运行（无 `-bench` 时跳过）。

每个基准使用 `b.ReportAllocs()` 与 `b.SetBytes(n)` 输出吞吐，并额外用 `b.ReportMetric` 记录每操作写入字节数 —— 使创新 A 与 B 的验收（写入量不增长）能被基准直接断言，而不依赖人工观察。

## 验收门槛

**性能目标必须绑定硬件、对象大小、并发数和测试工具版本。没有可复现实验条件的数字不能作为项目承诺。**

开发机（Apple Silicon + APFS）的数据只能作为**回归基线**，用于检测退化，**不得**作为对外性能承诺，也不得外推到 Linux 或其它磁盘。

| 本机回归基线 | 目标 | 意义 |
| --- | --- | --- |
| 1 KiB PUT P50 | < 2 ms | 小对象元数据路径开销 |
| 1 MiB GET 吞吐 | > 500 MB/s | 常规读路径 |
| 1 GiB 顺序 GET 吞吐 | 显著高于用户态拷贝路径 | 零拷贝生效的证据（受本机写入约束未测；以 16 MiB `BenchmarkHTTPGetObject` 为替代证据，见 09 文档） |
| 64 并发 1 MiB PUT | > 300 MB/s | 并发写扩展性 |
| 100 万对象启动 | < 500 ms | 证明启动代价与规模弱相关 |
| 10 万对象 LIST（**单机**） | < 500 ms | 当前主要性能风险的基线；集群模式不沿用此目标 |
| 完成后写入量 | 不随对象大小增长 | 创新 B 验收 |

上述数字均在 macOS 上测得，`F_FULLFSYNC` 语义与 Linux 不同，会低估 Linux 上的表现。跨平台结论必须在目标平台上重新测量。

**当前实测与目标差距（不得省略）**：切片 9 后，10 万对象 LIST 在本机（Apple M4 / APFS，Go 1.27.2，`-count=5` 中位数）为 **4.1 s**，未达到表中的 `< 500 ms` 目标。切片 9 前的同一基准为 17.8 s。差距根因与后续方向见 `known-limitations.md`（每对象 JSON + 小对象打包未实施）。该目标继续保留，不因未达成而下调。

**性能切片 09 的实测结果**（Apple M4 / APFS / Go 1.27.2，完整条件与前后对照见
[09 文档](./09-edge-io-performance.md)）：

| 本机回归基线 | 目标 | 实测（优化后） | 结论 |
| --- | --- | --- | --- |
| 1 KiB PUT P50 | < 2 ms | 14.0–18.4 ms | **未达标**：APFS 上 2×fsync + 2×dirsync ≈ 9–16 ms，物理下限，非代码路径 |
| 1 MiB GET 吞吐 | > 500 MB/s | 5.8–5.9 GB/s（177–180 µs） | 达标 |
| 1 GiB 顺序 GET | 显著高于用户态拷贝 | 未测（写入量约束） | 未验证 |
| 64 并发 1 MiB PUT | > 300 MB/s | 97–104 MB/s | **未达标**：`F_FULLFSYNC` 设备层串行化，本机不可达 |
| 100 万对象启动 | < 500 ms | 未测 | 未验证 |
| 10 万对象 LIST（单机） | < 500 ms | 3.90–4.27 s | **未达标**，根因同上 |
| 完成后写入量 | 不随对象大小增长 | 实测等于对象大小（去重后反而更少） | 达标 |

同时记录**内存**回归基线（05 文档要求的"不同对象大小的内存"）：1 KiB PUT 从
4.22 MB/op 降到 10.5–11.6 KB，64 KiB 从 4.29 MB 降到 75–80 KB，1 MiB 从
≈4.2 MB 降到 1.13 MB。该收益是确定的，不依赖 fsync 语义，可外推到其它平台；
延迟数字则不可外推。

## 验收原则

1. 性能数字必须附带完整实验条件：硬件、操作系统、文件系统、磁盘类型、对象大小、并发数、工具版本。
2. 未通过自动化测试的接口不得声明兼容。
3. 所有创新项的性能收益在获得基准数据前均为**待验证假设**。
4. 故障测试必须给出"恢复后数据完整性"的断言，而不只是"服务能重新启动"。