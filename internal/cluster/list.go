package cluster

import (
	"context"
	"fmt"
	"sync"

	"github.com/binbinpeng123/Blink/internal/store"
)

// ListObjects 跨节点列举桶内对象。
//
// 每个节点只知道它自己持有的副本，因此 LIST 必须问遍全部节点再把清单
// 归并成一份：
//   - 同一 key 可能以不同版本出现在多个节点上，取 (ObjVersion, NodeID)
//     最大的那份，与读路径选副本的规则一致；
//   - 归并后的最新版本是墓碑 ⇒ 该 key 不出现。墓碑必须先参与比较、
//     再被丢弃，顺序反过来就会让"已删除但旧副本还在"的 key 漏出来；
//   - 分页、delimiter 折叠、游标推进全部交给 store.PageObjects，与单机
//     LIST 是同一份代码，两种部署的页边界不会漂移。
//
// 为什么 responded ≥ R 就够：W+R>N 保证任意 R 个响应者与任一 key 的
// W 份已提交副本必有交集，因此每个已提交的 key 至少有一个响应者看得见。
//
// 代价说明：本期是**全量扫描** —— 每个节点返回整桶清单，协调者全量归并。
// 10 万条约 20 MB/节点，maxListResponseBytes 是硬上限。按 key 区间向
// 各节点要 top-K 是后续优化，不改变这里的归并语义。
func (c *Cluster) ListObjects(bucket string, opt store.ListOptions) (*store.ListResult, error) {
	if err := store.ValidateBucketName(bucket); err != nil {
		return nil, err
	}

	ts := c.allTargets()
	replies := make([]listReply, len(ts))
	var wg sync.WaitGroup
	for i, t := range ts {
		wg.Add(1)
		go func(i int, t target) {
			defer wg.Done()
			replies[i] = c.listOnNode(t, bucket)
		}(i, t)
	}
	wg.Wait()

	responded, present := 0, 0
	for _, r := range replies {
		if r.err != nil {
			continue
		}
		responded++
		if r.bucket {
			present++
		}
	}
	if responded < c.cfg.ReadQuorum {
		return nil, fmt.Errorf("%w: 仅 %d/%d 个节点响应 LIST，需要 %d（%v）",
			store.ErrUnavailable, responded, len(ts), c.cfg.ReadQuorum, firstListError(replies))
	}
	if present == 0 {
		// 没有任何响应者见过这个桶 ⇒ NoSuchBucket。空桶不在此列：
		// 它会被 present 计数命中，随后走到 KeyCount=0 的正常结果。
		return nil, store.ErrBucketNotFound
	}

	// latest 只保留每个 key 的最新版本。key 的空间由客户端决定，
	// 这里不预设规模上限 —— 分页参数只影响返回，不影响必须归并的量。
	latest := make(map[string]store.ReplicaEntry, 64)
	for _, r := range replies {
		if r.err != nil || !r.bucket {
			continue
		}
		for _, e := range r.entries {
			cur, ok := latest[e.Key]
			if !ok || newerEntry(e, cur) {
				latest[e.Key] = e
			}
		}
	}

	objs := make([]store.ListObject, 0, len(latest))
	for _, e := range latest {
		if e.Deleted {
			continue
		}
		objs = append(objs, store.ListObject{
			Key:      e.Key,
			Size:     e.Size,
			ETag:     e.ETag,
			Modified: e.Modified,
		})
	}
	return store.PageObjects(objs, opt), nil
}

// listReply 是单个节点的 LIST 结果。
//
// err 与 bucket 分开：节点不可达（err）不能计入法定人数；
// "这个节点没有该桶"（bucket=false）是正常响应，必须计入。
type listReply struct {
	id      string
	bucket  bool
	entries []store.ReplicaEntry
	err     error
}

// listOnNode 取单个节点的整桶清单。
func (c *Cluster) listOnNode(t target, bucket string) listReply {
	if t.local != nil {
		exists, err := t.local.BucketExists(bucket)
		if err != nil {
			return listReply{id: t.id, err: err}
		}
		if !exists {
			return listReply{id: t.id}
		}
		entries, err := t.local.ScanReplicaEntries(bucket)
		if err != nil {
			return listReply{id: t.id, err: err}
		}
		return listReply{id: t.id, bucket: true, entries: entries}
	}

	resp, err := t.client.ListReplicas(context.Background(), bucket)
	if err != nil {
		return listReply{id: t.id, err: err}
	}
	return listReply{id: t.id, bucket: resp.Bucket, entries: resp.Entries}
}

// newerEntry 判断 a 是否比 b 更新，规则与 newerMeta 完全一致。
func newerEntry(a, b store.ReplicaEntry) bool {
	if a.ObjVersion != b.ObjVersion {
		return a.ObjVersion > b.ObjVersion
	}
	return a.NodeID > b.NodeID
}

// firstListError 返回第一个非 nil 的错误，仅用于排障。
func firstListError(replies []listReply) error {
	for _, r := range replies {
		if r.err != nil {
			return r.err
		}
	}
	return nil
}
