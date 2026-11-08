# 测试与性能基准

评审状态：已允许开发
评审人：binbinpeng123
评审日期：2026-10-23
适用版本：v0.1.0（单节点，默认）；v0.2.0（集群与 EC，见"集群测试"）

## 测试层次

| 层次 | 范围 | 运行方式 |
| --- | --- | --- |
| 单元测试 | 元数据、路径、布局、校验和、错误处理、签名算法 | `go test ./...`，无网络、无磁盘依赖 |
| 集成测试 | S3 API、文件系统、重启恢复 | `go test -tags integration ./test/integration/` |
| 互操作测试 | AWS CLI 真实二进制；纯标准库手写客户端 | `scripts/aws-cli-smoke.sh` 与 `test/interop/` |
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

### 集成测试重点

- Bucket 生命周期与幂等性；桶名校验端到端。
- 对象往返、零字节对象（ETag 必须为 `d41d8cd9...`）、大对象分块、用户元数据、`Content-Type` 默认值、`Content-MD5` 不匹配、`If-None-Match` 前置条件。
- Range 全部形态、条件请求 304/412、多区间返回 501。
- ListObjectsV2 分页（断言无重复无遗漏）、前缀、分隔符折叠、`start-after`、UTF-8 字节序、`encoding-type=url`。
- 分片上传正常路径、ETag 格式、分片过小、序号错乱、ETag 不匹配、中止后完成返回 404。
- 认证：无签名、错误 Access Key、错误签名、免签名端点、Presigned GET/PUT。
- CORS：`PUT`/`GET`/`DELETE ?cors` 往返、`OPTIONS` 预检在无 `Authorization` 下返回 200、跨源实际请求带 `Access-Control-Allow-Origin`。
- 重启：对象保留、分片上传状态保留、格式版本拒绝启动。
- 并发：同 key 并发写后内容与某一份完整输入逐字节相等；写入期间持续读取不出现撕裂。
- 损坏：直接改动 CAS 中一个字节后读取应报错而非返回错误数据；元数据损坏与块表截断的处理。

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
- **未覆盖**：签名分块（真实 aws-cli 不发送）、客户端证书信任链、生产 CA、TLS 证书轮换。这些不得写成已通过。

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

每个基准使用 `b.ReportAllocs()` 与 `b.SetBytes(n)` 输出吞吐，并额外用 `b.ReportMetric` 记录每操作写入字节数 —— 使创新 A 与 B 的验收（写入量不增长）能被基准直接断言，而不依赖人工观察。

## 验收门槛

**性能目标必须绑定硬件、对象大小、并发数和测试工具版本。没有可复现实验条件的数字不能作为项目承诺。**

开发机（Apple Silicon + APFS）的数据只能作为**回归基线**，用于检测退化，**不得**作为对外性能承诺，也不得外推到 Linux 或其它磁盘。

| 本机回归基线 | 目标 | 意义 |
| --- | --- | --- |
| 1 KiB PUT P50 | < 2 ms | 小对象元数据路径开销 |
| 1 MiB GET 吞吐 | > 500 MB/s | 常规读路径 |
| 1 GiB 顺序 GET 吞吐 | 显著高于用户态拷贝路径 | 零拷贝生效的证据 |
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