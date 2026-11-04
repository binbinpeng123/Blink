package cluster

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
)

// vnode 是环上的一个虚拟节点。ord 是该虚拟节点在属主节点内的序号，
// 仅用于哈希碰撞时给出确定性的排序。
type vnode struct {
	hash uint64
	node string
	ord  int
}

// Ring 是一致性哈希环。
//
// 设计取舍：每个节点 128 个虚拟节点，按 fnv64a(node+"#"+i) 映射到 [0,2^64)，
// 查找时顺时针取首个**不同**节点的 vnode。相比"每节点一个点 + 跳跃"，
// 虚拟节点把负载方差压到可接受范围（128 点时 3 节点标准差约 10%），
// 同时保持实现足够小，适合边端设备。
//
// Ring 构造完成后不可变，可被任意 goroutine 并发读取。
type Ring struct {
	nodes  []string
	points []vnode
}

// NewRing 构造哈希环。nodes 允许重复，内部会去重并排序，
// 因此成员集合相同的两个进程一定得到完全相同的环。
func NewRing(nodes []string, vnodes int) (*Ring, error) {
	if vnodes < 1 || vnodes > maxVNodes {
		return nil, fmt.Errorf("vnodes 必须落在 [1, %d]，当前 %d", maxVNodes, vnodes)
	}

	uniq := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		if err := validateNodeID(n); err != nil {
			return nil, err
		}
		uniq[n] = struct{}{}
	}
	if len(uniq) == 0 {
		return nil, fmt.Errorf("哈希环的节点列表为空")
	}

	ids := make([]string, 0, len(uniq))
	for id := range uniq {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	points := make([]vnode, 0, len(ids)*vnodes)
	for _, id := range ids {
		for i := 0; i < vnodes; i++ {
			points = append(points, vnode{
				hash: hashString(id + "#" + strconv.Itoa(i)),
				node: id,
				ord:  i,
			})
		}
	}
	// 哈希碰撞在 64 位空间里极罕见，但一旦发生，跨进程的排序必须一致；
	// 因此用 (hash, node, ord) 三元组做全序，而不是依赖 sort.Slice 的不稳定性。
	sort.Slice(points, func(i, j int) bool {
		if points[i].hash != points[j].hash {
			return points[i].hash < points[j].hash
		}
		if points[i].node != points[j].node {
			return points[i].node < points[j].node
		}
		return points[i].ord < points[j].ord
	})

	return &Ring{nodes: ids, points: points}, nil
}

// Nodes 返回环上节点的排序副本，调用方可安全修改返回值。
func (r *Ring) Nodes() []string {
	out := make([]string, len(r.nodes))
	copy(out, r.nodes)
	return out
}

// Replicas 返回 key 的 n 个副本节点，按偏好顺序排列：
// 第 1 个是 primary，条件写与冲突裁决优先参考它。
//
// n 超过节点总数时返回全部节点；n<=0 返回 nil。
// 返回值保证互不重复 —— 同一节点的多个虚拟节点只取一次，
// 否则 N=3 在 3 节点集群上可能落成"三个副本在同一台机器"。
func (r *Ring) Replicas(key string, n int) []string {
	if n <= 0 || len(r.points) == 0 {
		return nil
	}
	if n > len(r.nodes) {
		n = len(r.nodes)
	}

	h := hashString(key)
	start := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if start == len(r.points) {
		start = 0
	}

	out := make([]string, 0, n)
	seen := make(map[string]struct{}, n)
	for i := 0; i < len(r.points) && len(out) < n; i++ {
		p := r.points[(start+i)%len(r.points)]
		if _, dup := seen[p.node]; dup {
			continue
		}
		seen[p.node] = struct{}{}
		out = append(out, p.node)
	}
	return out
}

// Owner 返回 key 的 primary 节点；环为空时返回空串。
func (r *Ring) Owner(key string) string {
	if rep := r.Replicas(key, 1); len(rep) > 0 {
		return rep[0]
	}
	return ""
}

// ReplicaKey 返回对象在环上使用的键。
//
// 用 "bucket/key" 而不是仅 key：不同桶的同名对象是不同对象，
// 只按 key 放置会让两个桶的写负载与故障域完全重合。
func ReplicaKey(bucket, key string) string {
	return bucket + "/" + key
}

// hashString 是环与键定位统一使用的哈希：FNV-1a 64 位。
//
// 选它的原因不是分布质量（这方面 xxhash/murmur 更好），而是：
// 标准库自带、零依赖、实现只有几行、在边端设备上够快。
// 虚拟节点数量弥补了单点分布质量，实测 100k key 的负载偏差可接受。
func hashString(s string) uint64 {
	h := fnv.New64a()
	// fnv.Write 不会返回错误（hash.Hash 的契约），忽略是安全的。
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}
