# 分布式存储设计（跨节点读写、多副本与纠删码）

评审状态：待评审
评审人：—（待填写）
评审日期：—（待填写）
适用版本：v0.2.0（分布式版本）

> **修订关系**：本文档修订 01/03/04 中"不做多节点复制、不做纠删码、不支持 `replicas > 1`"的结论。按 `docs/reviews/README.md` 的记录规则，文档先于实现更新。

## 目标

在保持单二进制、零第三方依赖、边端可部署的前提下，让 Blink 支持多 Pod 跨节点读写：任意节点都可以接收 S3 请求，数据按一致性哈希落到 N 个副本，通过 quorum 保证 `PUT` 成功后任意节点立即可见；同时提供可选纠删码，在 3 节点与 ≥6 节点两种边端规模下把空间开销降到 1.5x。

三条不可妥协的约束（继承 01 文档）：

1. **单机模式零回归**：`cluster_mode=single` 不启动任何集群 goroutine、不监听内部端口、不经过任何额外函数调用跳转。
2. **不引入外部依赖**：无 etcd、无中心元数据服务、无第三方库；EC 自行实现。
3. **性能声明绑定硬件**：分布式路径的吞吐与延迟只在明确标注的测试条件下作为回归基线。

## 非目标

| 能力 | 不做的理由 |
| --- | --- |
| 自动再平衡 / 在线成员变更 | 需要迁移协调与安全窗口；v1 用静态成员 + 离线人工迁移，避免"半迁移"数据丢失 |
| 节点发现 / gossip / 共识（Raft） | 违背轻量目标；静态成员 + quorum 已满足边端可用性 |
| 跨区域/跨集群复制、S3 Replication API | 需要广域编排、冲突与带宽管理；`?replication` 继续返回 501 |
| EC 与副本混部（同对象双布局） | 写放大与元数据复杂；v1 按对象大小二选一 |
| EC 自动换节点/重编码 | 需要重写 meta 与数据迁移；v1 只原位重建 shard |
| 块级 GC / refcount | 现状本就没有块 GC，分布式不扩大范围；仅做 tombstone GC |
| 跨节点零拷贝 | 远程数据必须经过协调者用户态；只保证本地副本的 sendfile 快路径 |

## 术语

| 术语 | 含义 |
| --- | --- |
| **N / W / R** | 副本数 / 写 quorum / 读 quorum。默认 `3 / 2 / 2` |
| **副本集** | 某 key 经一致性哈希选出的 N 个节点，按环上顺序排列；第 1 个称 primary |
| **EC group** | 某 stripe 的 `k+m` 个 shard 所在节点集合 |
| **tombstone** | 逻辑删除标记。保留 `BLINK_TOMBSTONE_TTL` 后由确定性 owner 物理清理 |
| **HLC** | 混合逻辑时钟。`obj_version = (physical_ms, logical_counter)`，并发同值时用 `node_id` 决胜 |
| **durable quorum** | 块与 meta 均已落到 ≥W 个副本，此时向客户端返回成功 |

## 架构

### 分层

```
s3api ──► store.Backend（接口，由消费方定义）
                ├── *store.Store      single：直连本地文件系统，零额外开销
                └── *cluster.Cluster  cluster：扇出到本地 + 远端副本
```

`store.Backend` 在 `internal/store/backend.go` 定义，方法集与 `s3api` 实际调用的 `*store.Store` 方法一致。`*store.Store` 天然满足该接口，因此单机路径只是"把具体类型换成接口"，不改变任何实现。

`cluster.Cluster` 对每个节点持有句柄：本地为 `*store.Store`，远端为 `cluster.Peer`（内部 HTTP 客户端）。**复用现有单机实现作为副本引擎，不重写存储格式**——块切分、CAS 去重、原子 rename、CRC32C 全部沿用 `internal/store`，保证 ETag 与校验和语义与单机逐字节一致。

### 成员与放置

- 静态成员表：`BLINK_CLUSTER_PEERS=id@host:port,...`，必须包含自身。服务不发现、不投票、不选主。
- 一致性哈希：每节点 128 个虚拟节点，`fnv64a(nodeID + "#" + i)`；`Ring.Replicas(key, n)` 顺时针取首 n 个**不同**节点。
- 放置 key 为 `bucket + "/" + objectKey`。增删节点时只有落入变动区间的 key 需要迁移——这是选择一致性哈希而非取模的原因。
- 副本顺序即偏序：primary 用于条件写串行化与读修复优先源。
- v1 提供离线迁移命令 `blink-admin rebalance --dry-run|--apply`，不进入在线自动化。

### 写路径

```text
1. 任意节点作为协调者，Ring.Replicas(key, N) 得到 N 个目标
2. 复用 `store.StreamBlocks` 流式切块（4 MiB 块，CAS 内容寻址，Stat 命中即去重）；`OpenObject` 返回 `store.Reader` 接口，本地与集群读取器对 s3api 等价
3. 每个块扇出到 N 个目标：本地直写；远端 POST /internal/v1/block
4. 在途窗口 BLINK_WRITE_WINDOW=8（≈32 MiB/上传），共享 buffer pool + 全局准入背压
5. 块全部落盘后提交 meta，携带 obj_version=HLC(now, counter, nodeID)
6. meta 扇出到 N 个副本，副本对更低版本直接拒绝（防乱序覆盖）
7. 收到 W 个 ack 即返回 200；失败副本进入修复队列
```

**关键性质**

- 对象可见化仍是"一次原子 `rename(meta.json)`"，与单机一致：读者要么看到完整旧版本，要么看到完整新版本。
- `ETag` 由块内容 MD5 决定，与单机路径完全相同——分布式不改变对象标识。
- **失败语义**：够 W → 200；不够 W → `503 ServiceUnavailable`。已部分生效的写返回 5xx 是可能的（与 S3 一致），文档明确"5xx 后重试需幂等"。

### 读路径

```text
1. Ring.Replicas(key, N) → 并行向各副本取 meta，凑 R 份
2. 取最大 (obj_version, node_id)；最高版本是 tombstone → 404 NoSuchKey
3. 按 blocks[] 顺序并行预取（BLINK_READ_WINDOW=4）
4. 本地副本命中 → 现有 copyBlockSegment 的 sendfile 零拷贝
   远端块 → 内部接口流式转发
5. 发现副本落后/缺块 → 后台读修复
```

- R 份都取不到 → `503`，默认不降级；`BLINK_READ_DEGRADED=true` 才允许从任一可用副本读并记录降级。
- 所有副本均损坏（`ErrCorrupt`）→ `500 InternalError`，**不伪装成 404**——"对象不存在"与"数据损坏"必须可区分。
- Range 读取在网络侧以块为粒度传输，块内仍按偏移截取，不放大流量到整对象。

## 一致性模型

### 承诺

| 场景 | 保证 |
| --- | --- |
| `PUT` 返回 200 | 任意节点 `GET`/`HEAD`/`LIST` 立即可见 |
| `DELETE` 返回 204 | 任意节点 `GET` 立即 404，`LIST` 立即不再出现 |
| 覆盖写 | 读者只看到完整旧版本或完整新版本，不撕裂 |
| 并发覆盖 | LWW：`obj_version` 最大者胜；同版本用 `node_id` 字典序 |
| `LIST` | 全局聚合快照，无缓存延迟 |

### 为什么 `W+R>N` 足够

`W=2, R=2, N=3` 时任意写集合与读集合必然至少交叠一个节点（鸽巢原理）。读集合中的最高版本因此不早于已确认的写版本 → `GET` 立即可见。这不是最终一致，而是在**静态成员**下无需共识即可获得的强一致。

前提是**不做自动成员变更**：`W > N/2` 保证网络分区时只有多数派能写，不会双写；少数派返回 503。

### 条件的边界

- 条件写（`If-None-Match: *`、`If-Match`）在 v1 是**尽力而为**：协调者写前读 R 份 meta 做 barrier，但两个协调者同时命中同一 key 时仍可能双成功。这一点必须写入 02 文档，不得宣称与 AWS 完全等价。
- 同 key 高并发覆盖下，`obj_version` 由各协调者独立生成；HLC 用物理时间主导，时钟漂移小于毫秒级时行为稳定。极端时钟回拨可能导致旧版本胜出——记录为已知限制。

## 元数据一致性

### 桶控制面（刻意与数据面不对称）

| 操作 | 广播范围 | 成功条件 | 失败处理 |
| --- | --- | --- | --- |
| CreateBucket | 全部节点 | 所有**可达**节点 ack | 可达节点失败 → 503；并发同名创建幂等 |
| DeleteBucket | 全部节点 | **要求所有节点可达**且集群范围为空（对象 + 进行中 MPU） | 任一节点不可达 → 503 |
| PutBucketCORS | 全部节点 | 所有可达节点 ack | 失败 → 503 |
| GetBucketCORS | 本地；未命中向 peer 拉取 | 缓存正结果 | 全部 peer 无 → `NoSuchCORSConfiguration` |
| ListBuckets | 本地 + peer 并集 | 尽力合并 | 不可达节点仅记录日志 |

**理由**：删除桶后若宕机节点带着旧数据回来，会造成"已删除的桶复活"。这是唯一必须全员在线的操作。对象数据面则相反——容忍少数节点故障是分布式存储的基本价值。

远端应用桶变更时复用现有 `CreateBucket`/`DeleteBucket`/`PutBucketCORS`（`internal/store/store.go:232-279`、`cors.go:35-89`），确保 `Store.buckets`/`Store.cors` 缓存同步更新。

### LIST 跨节点聚合

```text
1. 向全部节点并行 POST /internal/v1/list，参数 {bucket, prefix, delimiter, after, limit=maxKeys+1}
2. 每节点返回本地 ReplicaEntry（含 obj_version/node_id/deleted），按 key 字节序局部有序
3. k 路归并；同 key 取最大 (obj_version, node_id)；deleted=true 丢弃
4. 全局折叠 delimiter、按 maxKeys 截断；Next 游标沿用现有 store.ListCursor 语义
```

**为什么每节点取 `maxKeys+1` 就够**：全局第 K 个 key 在该节点本地列表中位置 ≤ K（它前面最多 K-1 个不同 key，每个在该节点最多出现一次）。因此取前 K 条足以构造全局前 K。

**代价（最大性能风险）**：单机 LIST 已是全量扫描 + 排序，集群再乘节点数。3 节点 10 万对象预计仍为秒级，达不到 05 文档 `<500ms` 的目标。缓解：强制 prefix 收窄、每节点并行、限制 maxKeys；小对象索引继续延后。**05 文档必须同步下调集群 LIST 的预期，不得沿用单机目标。**

### tombstone 与 GC

- 集群模式 `DeleteObject` 写 tombstone：`objectMeta{Deleted:true, ObjVersion:next, NodeID:self}` 提交到 W 个副本。
- `BLINK_TOMBSTONE_TTL=168h` 后，由 `hash(bucket+"/"+key)` 的环后继作为 owner，扫描本地 tombstone，向全部副本确认"要么已有 ≥ 该版本，要么节点超过 TTL 不可达"，再物理删除 meta 目录。
- **CAS 块不删**：与现有行为一致（`internal/store/fsstore_read.go` 删除路径只删对象目录）。项目当前没有块 GC，tombstone GC 不能假装解决空间回收。

## 分片上传（MPU）

### uploadId 定位：不编码节点，靠环推导

保持现有 32 位小写十六进制格式（`validUploadID`，`internal/store/fsstore_mpu.go:190-201`）。任意节点由 uploadId 推导会话副本集：

```go
S = Ring.Replicas("__mpu__/"+uploadID, min(N, 3))   // 会话状态
O = Ring.Replicas(bucket+"/"+key, N)                 // 对象数据
```

优点：不改 uploadId 格式、不需要全局目录、不需要在 ID 里嵌节点；任何节点都能确定会话位置。128 位随机碰撞可忽略；创建时若 `S` 上已存在同 ID 会话，重新生成。

### 分工

| 操作 | 会话状态 `S` | 数据块 `O` | 成功条件 |
| --- | --- | --- | --- |
| Create | 广播 session 到 S（W） | 无 | S 上 W ack |
| UploadPart | partState 提交到 S（W） | 块写 O（N 副本） | S 上 W ack 且 O 上 W 节点块完成 |
| ListParts | 从 S 读 R 份，取最大版本 | 无 | R 个响应 |
| Complete | 从 S 读 R 份 partState，校验 ETag/顺序/大小 | 已在 O 的 CAS 中 | 对象 meta 提交到 O 的 W 个节点 |
| Abort | 删除 S 上会话（W/all） | 不动 CAS | W ack |
| ListMultipartUploads | 向**全部节点**查询本地会话后合并 | 无 | 尽力合并 |

Complete 完全复用单机"只串接块表、不搬数据"的逻辑（`internal/store/fsstore_mpu.go` 的 Complete 路径），partState 中的 Blocks 偏移重排后写入对象 meta。先提交对象 meta、再删除会话（与单机顺序一致），失败重试幂等。

同一 partNumber 并发重传按 LWW（版本/时间）处理，与 S3 重试语义一致。DeleteBucket 的非空判断必须 fan-out 全部节点查询对象与进行中会话——会话按 uploadId 散落，任何单节点都不完整。

## 纠删码（EC）

### 实现

纯 Go，零第三方依赖：

| 文件 | 内容 |
| --- | --- |
| `internal/ec/gf.go` | GF(2^8)，本原多项式 0x11d；对数/反对数表 |
| `internal/ec/cauchy.go` | 柯西矩阵构造 + 高斯消元求逆 |
| `internal/ec/codec.go` | Encode / Reconstruct / Verify |
| `internal/ec/codec_test.go` | 任意丢 ≤m 片的穷举恢复测试 |

选择柯西矩阵而非范德蒙德：任意 `k×k` 子矩阵可逆，且无需处理范德蒙德矩阵在部分子集上的奇异问题（实际实现中范德蒙德配合合适的域也能做到，但柯西的构造更直接）。

### profile

| 节点数 | profile | 空间开销 | 容错 | 说明 |
| --- | --- | --- | --- | --- |
| 3 | `2+1` | 1.5x | 1 节点 | 边端主流规模；与 3 副本同容错但省一半空间 |
| 4–5 | 无默认 | — | — | 开启 EC 时必须显式指定 `BLINK_EC_K`/`BLINK_EC_M`，否则**拒绝启动**，避免"以为开了 EC 其实还是副本" |
| ≥6 | `4+2` | 1.5x | 2 节点 | 大集群默认 |

### 触发与布局

- `BLINK_EC_ENABLED=true` 且对象 ≥ `BLINK_EC_MIN_SIZE=32MiB` 时启用；否则走副本。**默认关闭**。
- 每个存储块（默认 4 MiB）独立编码为 `k` 个数据片 + `m` 个校验片（即该块自成一个 stripe），分片落到 `k+m` 个节点；同一对象的所有块共享同一个分片组，组由 key 的环位置一次决定并写入 meta。
- meta 记录 `layout:"ec"`、`ec:{k,m}` 与 shard 组节点 ID。
- 小对象仍走副本，避免 shard 数量膨胀与元数据复杂化。

### 重建

任一派生 shard 缺失时，从本地/远端拉取 k 个健康 shard 重建，写入修复队列。**EC 写入要求全集群同版本二进制**：旧版本读到 EC meta 会判为 `ErrCorrupt`，因此滚动升级期间必须关闭 EC。

## 故障与修复

### 错误码

| 场景 | 行为 | 返回 |
| --- | --- | --- |
| 1 节点宕机 | 读写继续；失败副本进修复队列 | 正常 |
| 2 节点宕机（N=3） | 无法凑齐 W/R | `503 ServiceUnavailable` |
| R 个副本都不响应 | 默认不降级 | `503 ServiceUnavailable` |
| 开启 `BLINK_READ_DEGRADED` | 从任一可用副本读，记录 degraded | 200 + 日志/响应头 |
| 对象在所有响应副本中都不存在 | 不存在 | `404 NoSuchKey` |
| 数据损坏但可从其他副本恢复 | 换副本 + 后台修复 | 200 |
| 所有副本均损坏且无 EC 余量 | 不伪装 404 | `500 InternalError` |

新增 `s3err.CodeServiceUnavailable = "ServiceUnavailable"`（HTTP 503），同步写入 02 错误码表。**不复用 `SlowDown`**——其语义是请求限流，与本场景无关。

### 修复队列（简化 hinted handoff）

```go
type RepairTask struct {
    Bucket, Key string
    Version     uint64
    TargetNode  string
    Kind        string // "meta" | "blocks" | "shard"
    ShardIndex  int
    CreatedAt   time.Time
    Attempts    int
}
```

- 持久化：`<root>/cluster/hints/<targetNode>/<txid>.json`，成功即删；默认保留 72h。
- 内存优先队列 + 每节点 worker（默认并发 2），退避 1s→5s→30s→5m→1h。
- 写入成功但某副本失败时只入队**该副本**，不阻塞客户端。
- 反熵兜底：每 `BLINK_SCRUB_INTERVAL`（默认 24h）限速扫描本地 meta，与副本比对版本；修复缺失块/shard。

三道防线：读时修复（第一道）→ 修复队列（第二道）→ 周期反熵（最后兜底）。

### 修复风暴抑制

| 措施 | 取值/做法 | 状态 |
| --- | --- | --- |
| 确定性 owner | `hash(shard/key)` 的环后继负责，避免多节点重复修复 | 已实现（反熵） |
| 并发上限 | 每节点 `repair-max-concurrency` 个 repair worker（默认 2）；EC 重建暂与普通修复共用名额 | 已实现 |
| 退避 + 抖动 | 失败指数退避 `[1s,5s,30s,5m,1h]`，±30% 抖动 | 已实现 |
| 带宽/IO 限速 | 令牌桶，不超过本地磁盘顺序带宽的 20% | **未实现** |
| 优先级 | 可用 shard 数越接近 k 越优先；小对象优先于大对象 | **未实现** |
| 水位保护 | 磁盘高于高水位时暂停 repair 写入 | **未实现**（`Store` 尚无可用空间接口） |
| EC 重建单独限 1 | 避免重建挤占普通修复 | **未实现**，共用 worker 名额 |

## 部署

| 项 | 单机（现状） | 集群版 |
| --- | --- | --- |
| StatefulSet | `replicas: 1`，`OrderedReady` | `replicas` 与 peers 数一致，`podManagementPolicy: Parallel` |
| Service | 单 ClusterIP，仅 9000 | 新增 `blink-headless`（`clusterIP: None`，9000+9100），保留普通 ClusterIP 给客户端 |
| 存储 | 独立 PVC `blink-data` | `volumeClaimTemplates`，每 Pod 一块 20Gi RWO |
| 端口 | 9000 | 增加 `internal` 9100 |
| 网络 | — | `NetworkPolicy` 仅允许同 namespace 访问 9100 |
| 环境变量 | 现有 ConfigMap | 增加 `BLINK_NODE_ID`（downward API `metadata.name`）、peers、secret 与 EC/修复参数 |

Pod DNS：`blink-0.blink-headless.blink.svc.cluster.local`；peers 使用短名 `blink-0.blink-headless:9100`（同 namespace）。

**就绪探针不检查 quorum**：沿用 04 文档"满盘仍 200"的同一原则。若 `/readyz` 感知集群状态，2/3 节点故障时 K8s 会把最后一个可读节点也摘掉，用户彻底失去自救途径。集群降级通过日志与 `/version` 扩展字段暴露。

## 安全

- 内部端口独立于 S3 端口（默认 `0.0.0.0:9100`，`BLINK_INTERNAL_ADDR`），仅集群内网可达。
- 每个内部请求携带 `X-Blink-Node` / `X-Blink-Time` / `X-Blink-Sign = HMAC-SHA256(secret, method|path|time|bodyhash)`，重放窗口 5 分钟。
- `BLINK_CLUSTER_SECRET` 集群模式必填且 ≥32 字节；缺失或过短**拒绝启动**。
- 传输为明文 HTTP（内网）。不引入 mTLS，避免边端证书运维负担；文档明确"9100 不得暴露到不可信网络"。K8s 环境用 NetworkPolicy 兜底。

## 配置

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `BLINK_CLUSTER_MODE` | `single` | `single` / `cluster` |
| `BLINK_NODE_ID` | 集群必填 | 须出现在 peers 中 |
| `BLINK_CLUSTER_PEERS` | 空 | `id@host:port,...`，含自身 |
| `BLINK_INTERNAL_ADDR` | `0.0.0.0:9100` | 须与 `BLINK_ADDR` 不同 |
| `BLINK_CLUSTER_SECRET` | 空 | 集群必填，≥32 字节 |
| `BLINK_REPLICA_COUNT` / `WRITE_QUORUM` / `READ_QUORUM` | 3 / 2 / 2 | 启动校验 `1≤W,R≤N` 且 `W+R>N` |
| `BLINK_VNODES` | 128 | 每节点虚拟节点数 |
| `BLINK_EC_ENABLED` / `EC_K` / `EC_M` / `EC_MIN_SIZE` | false / 0 / 0 / 32MiB | 开启时按节点数选 profile |
| `BLINK_WRITE_WINDOW` / `READ_WINDOW` | 8 / 4 | 在途块数 |
| `BLINK_REPAIR_INTERVAL` / `SCRUB_INTERVAL` / `REPAIR_MAX_CONCURRENCY` | 30s / 24h / 2 | |
| `BLINK_TOMBSTONE_TTL` / `READ_DEGRADED` / `PEER_TIMEOUT` | 168h / false / 2s | |

单机模式下出现集群变量不改变行为，解析失败的变量只记录警告（`cluster_env_ignored`），保证老部署零回归。

## 验收要求

| 项 | 门槛 |
| --- | --- |
| 单机零回归 | `cluster_mode=single` 不启动 9100、不起 repair goroutine；`go test -race ./...` 全绿；`BenchmarkGetRangeSequential` 与 `BenchmarkList100kObjects` 相对基线回退 <3% |
| 跨节点读写 | 本机 3 进程：A 写 → B/C 读，`cmp` 逐字节一致 |
| 强一致 | 删除后任意节点立即 404 且 LIST 不再出现 |
| 故障 | 杀 1 节点读写正常；杀 2 节点返回 503 |
| 修复 | 节点重启后修复队列补齐，副本内容一致 |
| EC | 任意 ≤m 片丢失均能逐字节重建；3 进程 2+1、6 进程 4+2 端到端通过 |
| MPU | 3 节点 aws-cli 全流程，分片打不同节点，Complete 只串接块表 |
| 部署 | `kubectl apply --dry-run=client` 通过；真实集群部署**未实测**，须明确标注 |

K8s 的 headless DNS、`volumeClaimTemplates` 与 NetworkPolicy 在拿到隔离集群前**不得声称已验证**，与 `docs/design/known-limitations.md` 的诚实标注一致。

## 已知限制

- 无自动再平衡与在线成员变更；扩缩容需离线迁移。
- 条件写在并发协调者下是尽力而为，不等价于 AWS 的强串行化。
- 集群 LIST 复杂度为 O(节点数 × 对象数)，不承诺 05 文档的单机目标。
- EC 永久节点丢失后不能自动换组重编码；只能原位重建 shard。
- 失败 PUT 可能已部分生效；5xx 后重试必须幂等。
- 内部通信为明文 + HMAC，依赖网络隔离；不得跨不可信网络部署。
- 混合版本集群不支持 EC；滚动升级期间必须关闭 EC。
- 跨节点读取经过协调者用户态，无法使用 sendfile；本地副本仍走零拷贝。
