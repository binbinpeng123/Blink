package cluster

import (
	"context"
	"errors"
	"io/fs"
	"time"

	"github.com/binbinpeng123/Blink/internal/store"
)

// 本文件实现反熵（anti-entropy）扫描：周期性地把本地持有的每个对象
// 与它的其余副本比对，把"缺失/落后/缺块"的副本交给修复队列。
//
// 为什么需要它：读写路径上的修复只覆盖被访问到的 key，而写失败时
// 入队的 hint 可能因为进程崩溃而丢失（hint 落盘之前就崩了）。反熵是
// 最后一道防线 —— 它不依赖任何"故障发生过"的记录，只依赖数据本身。
//
// 扫描的代价必须被设计成可接受的：只在**本地**枚举对象（不扇出 LIST），
// 每个 key 只由环上的 primary 处理（确定性 owner，避免 N 个节点同时
// 扫同一份 key），并且严格串行 —— 顺序扫描本身就是限速器。

const (
	// scrubPace 是相邻两个 key 之间的最小间隔。
	// 反熵是后台流量：一次扫描跑 24 小时完全正常，跑 24 分钟把
	// 在线请求的 IO 挤掉则是事故。
	scrubPace = 5 * time.Millisecond
)

// scrubLoop 周期执行反熵扫描。
//
// 第一次扫描在完整周期之后：节点刚启动时正在重建缓存、处理积压请求，
// 此时立刻做一次全量扫描只会和恢复流量抢 IO。正常情况下启动后的
// 缺口由 hint 恢复与读修复覆盖，反熵只负责长期兜底。
func (r *repairer) scrubLoop() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.scrub)
	defer ticker.Stop()

	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
			r.scrubOnce()
		}
	}
}

// scrubOnce 扫描本地全部桶与对象，发现缺陷即入队。
//
// 导出给测试直接调用；生产路径只由 scrubLoop 驱动。
func (r *repairer) scrubOnce() {
	buckets, err := r.c.Store.ListBuckets()
	if err != nil {
		r.c.logger.Warn("scrub_list_buckets_failed", "err", err.Error())
		return
	}

	checked := 0
	for _, bucket := range buckets {
		if r.isClosed() {
			return
		}
		entries, err := r.c.Store.ScanReplicaEntries(bucket)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) && !errors.Is(err, store.ErrBucketNotFound) {
				r.c.logger.Warn("scrub_scan_failed", "bucket", bucket, "err", err.Error())
			}
			continue
		}
		for _, ent := range entries {
			if r.isClosed() {
				return
			}
			if r.c.ring.Replicas(ReplicaKey(bucket, ent.Key), 1)[0] != r.c.cfg.NodeID {
				// 不由本节点负责：别的节点扫同一份 key 时会处理。
				// 这条规则是反熵不产生 N 倍重复扫描的全部依据。
				continue
			}
			r.scrubEntry(bucket, ent)
			checked++
			if !r.sleep(scrubPace) {
				return
			}
		}
	}
	if checked > 0 {
		r.c.logger.Info("scrub_completed", "keys", checked)
	}
}

// scrubEntry 比对单个 key 的全部副本，把缺陷交给修复队列。
func (r *repairer) scrubEntry(bucket string, ent store.ReplicaEntry) {
	ts := r.c.targetsFor(bucket, ent.Key)
	probe := r.c.probeMetas(bucket, ent.Key, ts)

	responded, _ := countResponses(probe)
	if responded < r.c.cfg.ReadQuorum {
		// 副本不足 R：此刻无从判断谁落后，修复器也无从取得最新版本。
		// 不记警告 —— 集群短暂抖动是常态，下一轮扫描会重新发现。
		return
	}
	latest := latestMeta(probe)
	if latest == nil {
		return
	}

	for _, res := range probe {
		if res.err != nil {
			// 不可达的副本也要补：故障恢复后它正需要一份 hint。
			r.enqueue(res.id, repairMeta, bucket, ent.Key, latest.ObjVersion)
			continue
		}
		if !res.found || !sameVersion(res.meta, latest) {
			r.enqueue(res.id, repairMeta, bucket, ent.Key, latest.ObjVersion)
			continue
		}
		if latest.Deleted {
			continue
		}
		if r.targetHasAllBlocks(res.id, latest) {
			continue
		}
		r.enqueue(res.id, repairBlocks, bucket, ent.Key, latest.ObjVersion)
	}

	// EC 分片组里可能有不在副本集上的节点：它们只持有分片、没有 meta，
	// 上面的探测循环根本看不到它们。缺片必须在这里发现 —— 一个只做分片
	// 存储的节点掉线再恢复后不会收到任何 hint，只能靠这一轮扫描。
	if latest.Layout == store.LayoutEC && latest.EC != nil {
		replicas := make(map[string]bool, len(ts))
		for _, t := range ts {
			replicas[t.id] = true
		}
		for _, gid := range latest.EC.Group {
			if r.isClosed() {
				return
			}
			if replicas[gid] {
				// 副本成员已在上一轮按分片下标检查过。
				continue
			}
			if r.targetHasAllBlocks(gid, latest) {
				continue
			}
			r.enqueue(gid, repairBlocks, bucket, ent.Key, latest.ObjVersion)
		}
	}
}

// targetHasAllBlocks 检查某节点是否持有该版本要求它持有的全部数据。
//
// 副本布局下是"全部整块"；EC 布局下只检查该节点在分片组里的那一片。
// 副本节点本身不持有 EC 的整块，按整块检查会让每个副本永远被判缺块，
// 反熵每轮都重复入队。不在分片组里的节点没有数据要检查，直接视为齐全。
//
// 本地目标直接读盘；远端走 HEAD。探测失败按"缺块"处理而不是跳过：
// 一次网络抖动导致漏检没有意义，而入队本身是幂等的。
func (r *repairer) targetHasAllBlocks(id string, meta *store.ObjectMeta) bool {
	idx := ecShardIndex(meta, id)
	if meta.Layout == store.LayoutEC && idx < 0 {
		return true
	}

	t := r.c.targetFor(id)
	for i, b := range meta.Blocks {
		hash := b.Hash
		if meta.Layout == store.LayoutEC {
			if i >= len(b.Shards) {
				return false
			}
			hash = b.Shards[idx].Hash
		}
		ok, err := t.blockExists(context.Background(), hash)
		if err != nil || !ok {
			return false
		}
	}
	return true
}

// ecShardIndex 返回 id 在 meta 分片组中的下标；非 EC 或不在组里返回 -1。
func ecShardIndex(meta *store.ObjectMeta, id string) int {
	if meta == nil || meta.Layout != store.LayoutEC || meta.EC == nil {
		return -1
	}
	for i, gid := range meta.EC.Group {
		if gid == id {
			return i
		}
	}
	return -1
}

// isClosed 供长循环在关闭时提前退出。
func (r *repairer) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

// sleep 在 stop 关闭时提前返回 false。
func (r *repairer) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-r.stop:
		return false
	case <-t.C:
		return true
	}
}
