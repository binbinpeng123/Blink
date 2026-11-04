package cluster

import (
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/binbinpeng123/Blink/internal/store"
)

// Cluster 是集群模式下的存储后端。
//
// 它嵌入 *store.Store，分片上传（MPU）等尚未分布化的能力沿用本节点的
// 单机实现（切片 5 替换）。以下能力由本类型覆盖：
//   - 对象读写（PutObject/OpenObject/DeleteObject）：按一致性哈希扇出到
//     N 个副本，凑 W/R 法定人数；
//   - 桶控制面（CreateBucket/DeleteBucket/BucketExists/ListBuckets/
//     BucketCreatedAt/CORS）：跨节点广播，规则刻意与数据面不同 ——
//     建桶/写 CORS 只要求可达节点 ack，删桶要求全部节点可达且集群范围为空；
//   - LIST（ListObjects）：向全部节点要整桶清单，按 key 归并后分页。
//
// 每个节点的本地存储同时扮演两个角色：
//   - 单机实现的持有者（桶、MPU 等）；
//   - 一致性哈希环上本节点的"副本引擎"。
//
// 两者是同一份数据目录，不存在第二套落盘格式。
type Cluster struct {
	*store.Store

	cfg    Config
	ring   *Ring
	server *Server
	logger *slog.Logger

	// peers 不含自身：需要读写本地副本时直接走 *store.Store。
	peers map[string]*PeerClient

	// locks 是协调者自己的 key 锁。
	//
	// 不能复用 store.LockObject：PutReplicaMeta 内部会取同一把锁，
	// 在持有者外层再取会自死锁（sync.Mutex 不可重入）。这里的锁只负责
	// 让同一协调者上的并发写串行化，副本层的版本比较负责跨节点收敛。
	locks *stripedLocks
	clock *versionClock

	// now 允许测试注入时钟。
	now func() time.Time
}

// New 构造集群后端。
//
// cfg 必须已通过 Validate（NewServer 会再校验一次）；local 是本节点的
// 单机存储，绝不为 nil。任何构造失败都不应带着半配置启动。
func New(cfg Config, local *store.Store, logger *slog.Logger) (*Cluster, error) {
	server, err := NewServer(cfg, local, logger)
	if err != nil {
		return nil, err
	}

	peers := make(map[string]*PeerClient, len(cfg.Peers)-1)
	for _, p := range cfg.Peers {
		if p.ID == cfg.NodeID {
			continue
		}
		peers[p.ID] = NewPeerClient(p, cfg.NodeID, cfg.Secret, cfg.PeerTimeout)
	}
	if len(peers) != len(cfg.Peers)-1 {
		return nil, errors.New("cluster: 成员表中缺少自身节点")
	}

	return &Cluster{
		Store:  local,
		cfg:    cfg,
		ring:   server.Ring(),
		server: server,
		logger: logger,
		peers:  peers,
		locks:  newStripedLocks(256),
		clock:  &versionClock{},
		now:    time.Now,
	}, nil
}

// Ring 返回本节点的哈希环（只读）。
func (c *Cluster) Ring() *Ring {
	return c.ring
}

// InternalHandler 返回内部协议的 http.Handler，供 main 在独立端口监听。
func (c *Cluster) InternalHandler() http.Handler {
	return c.server.Handler()
}

// Close 释放到各节点的空闲连接。
//
// 不做会随进程退出自动回收，但优雅关闭时先断开连接能让对端
// 立刻看到 FIN，而不是等到 TCP keepalive 超时。
func (c *Cluster) Close() {
	for _, p := range c.peers {
		p.closeIdle()
	}
}

// targetsFor 返回 key 的 N 个副本节点，顺序即副本偏序（第 1 个是 primary）。
func (c *Cluster) targetsFor(bucket, key string) []target {
	ids := c.ring.Replicas(ReplicaKey(bucket, key), c.cfg.ReplicaCount)
	ts := make([]target, len(ids))
	for i, id := range ids {
		ts[i] = c.targetFor(id)
	}
	return ts
}

// allTargets 返回成员表中的**全部**节点（含本地），按节点 ID 排序。
//
// 与 targetsFor 不同，这里不做哈希放置：桶与 LIST 是集群范围的，
// 必须问遍每个节点。顺序取 PeerIDs()（已排序）而不是配置书写顺序，
// 让"取第一个有值的回答"这类兜底逻辑与成员表的书写方式无关。
func (c *Cluster) allTargets() []target {
	ids := c.cfg.PeerIDs()
	ts := make([]target, len(ids))
	for i, id := range ids {
		ts[i] = c.targetFor(id)
	}
	return ts
}

// targetFor 把一个节点 ID 映射成本地句柄或远端客户端。
func (c *Cluster) targetFor(id string) target {
	if id == c.cfg.NodeID {
		return target{id: id, local: c.Store}
	}
	return target{id: id, client: c.peers[id]}
}

// target 是一个副本目标：本地存储或远端客户端，二选一。
type target struct {
	id     string
	local  *store.Store
	client *PeerClient
}

// stripedLocks 是按 key 哈希分片的互斥锁组。
//
// 256 个分片让不同 key 的写入几乎不互相阻塞，同时把锁对象数量
// 限制在一个常量上 —— 按 key 建 map 的锁在 key 无限时就是内存泄漏。
type stripedLocks struct {
	m []sync.Mutex
}

func newStripedLocks(n int) *stripedLocks {
	if n <= 0 {
		n = 1
	}
	return &stripedLocks{m: make([]sync.Mutex, n)}
}

// Lock 返回解锁函数。
func (s *stripedLocks) Lock(key string) func() {
	i := hashString(key) % uint64(len(s.m))
	s.m[i].Lock()
	return s.m[i].Unlock
}

// versionClock 为每次写入分配严格递增的 obj_version。
//
// 分布式下的正确性来自两步：
//   - Next(seen) 取 max(已观察到的最大版本, 本地已发放的最大版本) + 1，
//     保证本节点不会发出比它见过的更旧的版本；
//   - 副本按 (ObjVersion, NodeID) 比较的规则让两个协调者同时写同一
//     key 时收敛到同一份（字典序更大的 NodeID 决胜），而不是永久分叉。
type versionClock struct {
	mu   sync.Mutex
	last uint64
}

// Next 返回下一个版本号；seen 是本次写入前观察到的最大 ObjVersion。
func (c *versionClock) Next(seen uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if seen > c.last {
		c.last = seen
	}
	c.last++
	return c.last
}

// newerMeta 判断 a 是否比 b 更新。
//
// 判定规则必须与 store.PutReplicaMeta 的 staleMeta 完全一致，
// 否则"读时选出的最新版本"可能被副本层拒绝写入。
func newerMeta(a, b *store.ObjectMeta) bool {
	if a.ObjVersion != b.ObjVersion {
		return a.ObjVersion > b.ObjVersion
	}
	return a.NodeID > b.NodeID
}
