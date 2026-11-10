# 边端读写性能优化（性能切片 09）

评审状态：**实现完成，待评审**
评审人：binbinpeng123
评审日期：2026-11-07
适用版本：v0.2.x（单机与集群模式共用）

> 编号说明：03/05 文档中"切片 9 已落地分片并行扫描与 LIST 部分字段解码"指
> 本阶段的前一批改动（已随跨节点 LIST 一起并入提交 `beac4b1`，当时未单独设
> 评审文档）。本文件记录的改动在其之后，仅涉及读路径与写内存，不改 LIST 代码。

## 目标

在不改变 S3 语义、不改动磁盘布局的前提下，降低单机与边端设备上的读延迟、写内存
占用与元数据重复解析。所有改动都必须有同一台机器、同一基准口径下的前后对照；
没有对照数据的优化一律回滚（03 文档"开发纪律"）。

本切片**不是**新的存储引擎，也不引入新的数据布局。它只回答一个问题：
在当前布局已经确定的前提下，哪些开销是"实现带来的"，而不是"设计带来的"。

## 实验条件

所有数字均在同一台机器、同一基准文件、同一口径下测得：

| 项 | 取值 |
| --- | --- |
| 硬件 | Apple M4，统一内存，内置 NVMe SSD |
| 操作系统 | macOS 15（Darwin 24.6.0） |
| 文件系统 | APFS（`F_FULLFSYNC` 语义） |
| Go | 1.27.2，`darwin/arm64` |
| 基准文件 | `test/bench/write_bench_test.go`、`test/bench/list_bench_test.go` |
| 运行参数 | `-benchtime=100ms~200ms -count=2`，取区间 |
| 数据目录 | `b.TempDir()`，每次运行独立 |

**口径说明（重要）**：`BenchmarkPutObjectSizes/16MiB` 的旧实现在计时区间内用字节
循环重新生成 16 MiB 请求体，CPU profile 显示生成本身占 15%，量到的不是存储路径。
本切片把内容模板移出计时区间，迭代内只做一次 memcpy + 改写 8 字节序号，**基线也
用同一份修正后的基准重新测过**。因此本文中 16 MiB 的基线数字（65–67 ms）低于早期
会话记录的 75 ms，后者属测量误差，不构成性能变化。

**未覆盖的档位**：05 文档列出 8 MiB / 128 MiB / 1 GiB 档位，本机压测总写入需
≤ 512 MiB，因此只测到 16 MiB；大对象连续性由 `BenchmarkGetRangeSequential`
覆盖。跨平台结论（尤其是 Linux/NVMe）未验证。

## 基线（改动前）

分支 `feature/performance-09-edge-io` 的起点（提交 `112e8ac`），在临时 worktree
中检出并使用同一份基准文件测得：

| 基准 | 结果 | 内存 |
| --- | --- | --- |
| PUT 1 KiB | 10.7–19.8 ms | 4.22 MB/op，75–88 allocs/op |
| PUT 64 KiB | 11.9–20.5 ms | 4.29 MB/op |
| PUT 1 MiB | 13.2–20.8 ms | ≈4.2 MB/op |
| PUT 16 MiB | 65.4–67.3 ms（249–257 MB/s） | 4.21 MB/op |
| GET 1 KiB | 113–159 µs | 19 allocs/op |
| GET 64 KiB | 121–143 µs | — |
| GET 1 MiB | 235–238 µs（≈4.4 GB/s） | — |
| GET 16 MiB | 2.52–2.56 ms | — |
| Range 64 KiB 顺序读 | 142–144 µs | 29 allocs/op |
| 64 并发 1 MiB PUT | 94.5–96.1 MB/s | — |
| LIST 10 万对象 | 3.94–3.95 s | 176 MB，1.306M allocs |

## 合入项

### A. 惰性块缓冲 + 声明长度提示

**问题**：`StreamBlocks` 无条件 `make([]byte, blockSize)`，即每次写入先分配 4 MiB
临时缓冲。1 KiB 对象也分配 4 MiB，1 MiB 对象同样 4 MiB。边端设备上并发上传数十
个对象时，这部分是实打实的 RSS 与 GC 压力。

**做法**（`internal/store/export.go`）：

1. 初始缓冲按小对象阈值分配，只有确认流超过阈值时才在**循环之前**增长到块长上限。
   增长必须发生在读循环之前：若等到循环里再增长，跨阈值对象会被切成
   "阈值 + 块大小"的块，破坏既有块边界（`TestPutObjectMultiBlock` 的 8/8/4 期望）。
2. 新增 `StreamBlocksSized(body, alg, tag, expected, fn)`：调用方已知
   `Content-Length` 时传入，缓冲按 `min(expected, blockSize)` 分配。单机
   `PutObject`、`UploadPart`、集群 `PutObject`、集群 `UploadPart` 全部接入。
3. 长度提示**只影响缓冲大小，不改变块边界**：只要实际长度 ≤ 声明长度，
   以声明的 `limit` 切出的块与按 `blockSize` 切出的块完全一致（末尾一个不满块）。
   声明与实际不符时块边界可能不同，但该次写入必然以 `ErrIncompleteBody` 失败，
   块表不会落盘。该不变式由 `TestStreamBlocksSizedKeepsBlockBoundaries`
   （0/1/7/8/9/15/16/17/31/32/33/40 B，阈值 8、块 16）与
   `TestStreamBlocksSizedIgnoresWrongHint` 守住。

**结果**（PUT，最终代码）：

| 尺寸 | 延迟 | 内存（前 → 后） |
| --- | --- | --- |
| 1 KiB | 14.0–18.4 ms | 4.22 MB → **10.5–11.6 KB**（↓99.7%） |
| 64 KiB | 10.4–18.6 ms | 4.29 MB → **75–80 KB**（↓98%） |
| 1 MiB | 12.4–15.0 ms | ≈4.2 MB → **1.13 MB**（↓73%） |
| 16 MiB | 64.0–69.4 ms | 4.28 MB（不变，本就受块大小约束） |

延迟在噪声范围内（写入由 fsync 主导，见"未达标项"），**收益是内存，不是延迟**。
不改变这一表述。

### B. 对象元数据 LRU 缓存

**问题**：每次 `OpenObject` 都要打开并 JSON 解码 `meta.json`。GET 1 KiB 的基线
19 allocs / 113–159 µs，其中绝大部分是元数据解析，数据本身只有 1 KiB。对边端
常见的小对象读取（配置、清单、推理结果），元数据成本盖过了数据成本。

**做法**：`internal/store/metacache.go`，有界 LRU（默认 4 MiB / 1024 条，单条
≤128 KiB，按块数估算开销；超限的块表不入缓存）。缓存的是只读的 `*ObjectMeta`，
调用方不得修改。写路径在元数据变化后立即更新或失效：

| 路径 | 动作 |
| --- | --- |
| `PutObject` | meta 落盘成功后 `put` |
| `CompleteMultipartUpload` | meta 落盘成功后 `put` |
| `PutReplicaMeta` | 写成功后 `put` |
| `DeleteObject` | `RemoveAll` **之前** `delete`（失败也不回退到旧值） |
| `DeleteReplicaMeta` | 取锁后、查盘前 `delete` |

`ls` / LIST 路径**不经过**该缓存（`fsstore_list.go` 直接读目录与 meta），因此
LIST 的数字不受影响，也不存在"缓存让 LIST 少扫盘"的假象。

**结果**（GET，最终代码）：

| 尺寸 | 延迟（前 → 后） | 内存 |
| --- | --- | --- |
| 1 KiB | 113–159 µs → **55.3–57.7 µs**（≈2.3x） | 19 → **7 allocs** |
| 64 KiB | 121–143 µs → **62.8–63.2 µs**（≈2.0x） | — |
| 1 MiB | 235–238 µs → **177–180 µs**（≈1.3x，5.8–5.9 GB/s） | — |
| 16 MiB | 2.52–2.56 ms → **2.42–2.44 ms**（≈4%） | — |
| Range 64 KiB 顺序读 | 142–144 µs → **67–69 µs**（≈2.1x） | 29 → **8 allocs** |

16 MiB 只提升 4%，符合预期：大对象的时间在块数据搬运上，元数据只占一次。

### 小对象内容内联（创新 D0，已落地）

**问题**：1 KiB 对象走 CAS 时，GET 仍要打开/`Stat` 一个块文件，PUT 要写块文件
并提交两次持久化屏障。数据只有 1 KiB，文件系统与元数据成本盖过了数据本身。

**做法**：`ExpectedSize` 在 `(0, 4 KiB]` 内且实际长度恰好匹配时，内容直接存进
`meta.json` 的 `inline_data`，只做一次原子元数据提交；读取时直接切片写出。
长度不符回退普通流式路径。完整设计与兼容性见 03 文档"创新 D0"。

**结果**（Apple M4 / APFS，`-benchtime=200x -count=3`，1 KiB 内容）：

| 基准 | CAS（关闭内联） | 内联 | 变化 |
| --- | --- | --- | --- |
| PUT 1 KiB | 8.29–8.82 ms | 8.14–8.80 ms | 中位 8.49 → 8.16 ms（−3.9%，落在噪声内） |
| GET 1 KiB | 52.9–65.4 µs | 335–685 ns | **≈100×** |
| LIST 1000 对象（1 KiB） | 16.1–16.3 ms | 18.2–19.1 ms | +14%～17% |
| LIST 1000 对象（4 KiB） | 16.3–16.8 ms | 18.7–19.7 ms | +15%～18% |

**解读**：

- PUT 的延迟仍由 `writeFileAtomic` 的 fsync 主导（APFS 上约 8 ms），少写一个
  CAS 块在延迟上测不出来；真实收益是**少一个持久化对象**（无 CAS 块文件、
  无孤儿块、低写放大），对 eMMC/SD 卡寿命有利，但本机无法验证。
- GET 的收益来自路径长度：命中元数据缓存后，内联对象只剩一次切片与
  `Write`，没有 `Stat`/`Open`/`sendfile`。
- LIST 的退化来自 `meta.json` 变大与 base64 膨胀。1 KiB 与 4 KiB 内容在同一
  量级（+14%～18%），说明当前规模下固定解析开销仍占主导。**这是默认值定为
  4 KiB 而不是更大的依据之一**；10 万对象规模未实测（受本机写入总量约束），
  不得外推。

### 块级哈希与整对象 MD5 并行（已落地，切片 16）

**问题**：16 MiB PUT 的 CPU profile 显示 `crypto/md5.block` 占 **62.83%**，
`sha256.blockSHA2` 10.62%，`hash/crc32.castagnoliUpdate` 7.96%，而真正写块的
`putBlock` 只占 8.85%。MD5 是 ETag 的定义、且是链式的，只能顺序计算；标准库
md5 没有 ARM 加密指令可用。SHA-256 与 CRC32C 走硬件加速，却和 MD5 串行执行，
慢的 MD5 占着核、快的两个排队。

**做法**：`StreamBlocksSized` 在 `GOMAXPROCS > 1` 且块长 ≥256 KiB 时，把该块的
SHA-256 与 CRC32C 放进一个 goroutine，与主 goroutine 的整对象 MD5 并行；回调
`fn` 返回前 `Wait`，因此块缓冲"回调期间有效"的契约与之前完全一致。小于阈值的块
（以及单核环境）保持原串行路径，避免调度开销吃掉小对象收益。

块边界、ETag、对象级校验和、错误传播路径均未改变；新增
`TestStreamBlocksParallelHashes`，在一个对象里同时覆盖 3 个并行块与 1 个串行块，
逐块与原始内容独立重算的 SHA-256/CRC32C 比对，并重算整对象 MD5；`-race` 通过。

**结果**（Apple M4 / 10 P，回调为空、不落盘，隔离哈希成本；`-benchtime=40x -count=5`）：

| 尺寸 | 耗时（前 → 后） | 吞吐（前 → 后） | 分配 |
| --- | --- | --- | --- |
| 1 MiB | 3.56–3.77 ms → **2.82–2.90 ms**（中位 −22.2%） | 287 → **368 MB/s** | 10 → 13 allocs，字节数基本不变 |
| 16 MiB | 53.7–55.1 ms → **40.4–43.2 ms**（中位 −25.5%） | 308 → **413 MB/s** | 16 → 29 allocs |

端到端 `BenchmarkPutObjectSizes/16MiB`（含 fsync 与落盘，共 4 个样本/边）：
基线 64.8–76.0 ms（中位 68.6 ms）→ 优化后 50.7–78.5 ms（中位 57.1 ms），
约 −16.8%。端到端样本少、方差大，只作为方向性验证；稳定收益以不落盘微基准为准。

**边界**：MD5 仍是唯一不能并行化的部分，也仍是理论上限。本次只是把 SHA-256/
CRC32C 藏进 MD5 的墙钟；进一步提升需要"MD5 与落盘重叠"或硬件加速 MD5，需另设
基准。

## 回滚项

### 大对象并行块写入（实测无收益，已完整回滚）

**假设**：16 MiB PUT 由"切块 → 逐块 fsync"串行主导，把块写入并行化（4 worker）
应当缩短大对象写入时间。

**实测**：实现 `localBlockWriter` + `blockWriteParallel=4` + `blockBufs sync.Pool`
后，16 MiB PUT 仍为 66 ms（新口径），与串行版本无差异；1 MiB 同样无差异。
CPU profile 显示 `crypto/md5.block` 占 54–60%。**该尺寸在此硬件上的瓶颈是 ETag
的 MD5 计算，不是块 IO 并行度。**

按 03 文档纪律（无前后对照的优化一律回滚），代码已完整移除：`localBlockWriter`、
`blockWriteParallel`、`Store.blockBufs` 与相关 `sync` import 均不在最终 diff 中。

## 未达标项与客观原因

以下门槛**未达到**，且不通过调参掩饰。原因经实测定位，不是"再优化一下"就能解决。

### 1 KiB PUT P50 < 2 ms —— 本机不可达

基线 10.7–19.8 ms，优化后 14.0–18.4 ms（无改善）。本机 APFS 上单次
`write + fsync` ≈ 4.0 ms，`rename + dirsync` ≈ 8.1 ms；一次 PUT 至少包含
2 次 fsync + 2 次 dirsync，总计 ≈ 9–16 ms。这与实测区间完全吻合。

**结论**：在 macOS/APFS 上该门槛不可达，原因是 `F_FULLFSYNC` 的持久化语义，而
不是代码路径。关闭 fsync（`--fsync-policy=meta` 或更弱的策略）能降低延迟，但那是
可靠性取舍，不是性能优化，默认值不因此改变。Linux/NVMe 上的实际表现**未验证**。

### 64 并发 1 MiB PUT > 300 MB/s —— 本机不可达

基线 94.5–96.1 MB/s，优化后 97–104 MB/s（≈+5%）。macOS 的 `F_FULLFSYNC` 在
设备层串行化，并发数无法转化为吞吐。这不是并发写路径的回退：单对象延迟没有变差，
聚合吞吐受限于每次落盘的持久化开销。

**未验证**：在 Linux（`fsync` 语义不同）或支持并发 flush 的 NVMe 队列上是否可达。
不得引用本机数字外推。

### 16 MiB PUT ≈ 250 MB/s 上限（切片 16 后有改善，但 MD5 仍是上限）

MD5（ETag）在此硬件上约 250 MB/s，且无法并行化，是大对象写入的硬上限。并行块
IO 无收益（见回滚项）。切片 16 把块级 SHA-256/CRC32C 移出 MD5 的关键路径后，
不落盘微基准达到 413 MB/s、端到端中位耗时下降约 17%（见上节）；但 MD5 本身仍然
串行，若未来需要显著更高写入吞吐，方向仍是"ETag 计算与块落盘重叠"或"硬件加速
MD5"，两者都需要新的基准对照。

### 10 万对象 LIST < 500 ms —— 仍未达标（沿用切片 9 结论）

本切片未改 LIST 路径，沿用既有结论：本机 3.90–4.27 s（基线 3.94–3.95 s，无回退，
满足接口抽取 <3% 的要求）。根因与后续方向见 `docs/design/known-limitations.md`
（每对象 JSON + 小对象打包未实施）。

内联（创新 D0）默认开启后，`meta.json` 对 4 KiB 以下对象按 base64 膨胀 4/3，
LIST 的读放大随内联对象占比上升。1000 对象微基准为 +14%～18%（见上节）；
10 万对象规模未实测，**不得用 1000 对象的比例直接外推**。

## 回滚条件

出现以下任一情况，回滚对应改动而不是放宽断言：

- 任何缓存一致性测试失败（覆盖写后读到旧元数据、删除后仍命中缓存、集群副本
  元数据与本地不一致）。
- 块边界发生变化（`TestStreamBlocksSizedKeepsBlockBoundaries`、
  `TestPutObjectMultiBlock`、`TestPutObjectExactInlineThreshold` 失败）。
- 单机接口抽取性能回退 ≥3%（以 `BenchmarkGetRangeSequential`、
  `BenchmarkList100kObjects` 前后对照为准）。
- 元数据缓存导致常驻内存增长超过 4 MiB + 1024 条的上限（应被 LRU 边界挡住）。
- 内联对象与同内容普通对象的 ETag/校验和不一致，或内联读写、复制、巡检、
  副本元数据校验任一测试失败。
- 内联导致 1000 对象 LIST 相对关闭时回退超过 25%（以
  `BenchmarkListSmallCAS`/`BenchmarkListSmallInline` 同机对照为准）。

## 未验证项（不得写成已通过）

- Linux / NVMe / eMMC / SD 卡上的任何一项延迟与吞吐。
- 多副本与 EC 模式下缓存的内存上限：每个节点各有一份 LRU，集群规模下的总量
  未测量。
- 1 GiB 单对象写入（受本机写入总量约束未跑）。
- 长稳（>24 h）运行下的缓存命中率与 GC 行为。
- 内联对象在 10 万对象规模下对 LIST 的影响；以及 eMMC/SD 卡上"少一次块写入"
  对写放大与介质寿命的实际收益。
- 集群模式不产生内联对象这一结论的端到端验证（当前由代码路径保证，未在
  多节点环境实测）。

## 验证命令

```text
go build ./...
go vet ./...
go test ./... -count=1

go test ./test/bench/ -run '^$' -bench 'BenchmarkPutObjectSizes' -benchtime=200ms -count=2
go test ./test/bench/ -run '^$' -bench 'BenchmarkGetObjectSizes' -benchtime=200ms -count=2
go test ./test/bench/ -run '^$' -bench 'BenchmarkGetRangeSequential' -benchtime=200ms -count=2
go test ./test/bench/ -run '^$' -bench 'BenchmarkConcurrentPut1MiB' -benchtime=200ms -count=2
go test ./test/bench/ -run '^$' -bench 'BenchmarkList100kObjects' -benchtime=1x -count=1

go test ./internal/store/ -run 'TestInline' -count=1
go test ./internal/store/ -run '^$' -bench 'Benchmark(Put|Get|List)Small' -benchtime=200x -count=3
```

LIST 基准会预置 10 万对象（约 176 MB 写入），单独运行并计入本机写入总量。
