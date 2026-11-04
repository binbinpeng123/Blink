package cluster

import (
	"context"
	"io"
	"sync"

	"github.com/binbinpeng123/Blink/internal/store"
)

// 本文件是集群读写的公共零件：把一次操作扇出到多个副本目标，
// 再按法定人数汇总结果。
//
// 扇出一律并行：副本之间没有顺序依赖，串行只会把延迟乘以 N。
// 汇总规则由调用方决定（写要 W 个成功、读要 R 个响应），
// 这里只负责"把每个目标的结果原样带回来"，不做取舍。

// metaResult 是一次副本元数据探测的结果。
//
// found=false 且 err=nil 表示目标明确回答"我没有这份副本"；
// err!=nil 表示目标不可达或回答不合法。两者在法定人数统计中
// 含义完全不同，因此必须在类型上分开。
type metaResult struct {
	id    string
	meta  *store.ObjectMeta
	found bool
	err   error
}

// probeMetas 并行读取全部目标的副本元数据。
//
// 返回值顺序与 ts 一致；本地目标同步调用，远端目标各起一个 goroutine。
// 本函数不设总超时：每个远端调用有自己的传输超时，最慢的目标
// 决定整体耗时，这与"等够 R 个响应"的语义一致。
func (c *Cluster) probeMetas(bucket, key string, ts []target) []metaResult {
	results := make([]metaResult, len(ts))
	var wg sync.WaitGroup
	for i, t := range ts {
		wg.Add(1)
		go func(i int, t target) {
			defer wg.Done()
			m, found, err := t.getMeta(context.Background(), bucket, key)
			results[i] = metaResult{id: t.id, meta: m, found: found, err: err}
		}(i, t)
	}
	wg.Wait()
	return results
}

// countResponses 统计成功响应的目标数与其中持有副本的数量。
//
// 只有 err=nil 的结果算"响应"：一个超时的节点既不能证明对象存在，
// 也不能证明它不存在，把它计入任何一侧都会破坏 W+R>N 的保证。
func countResponses(results []metaResult) (responded, found int) {
	for _, r := range results {
		if r.err != nil {
			continue
		}
		responded++
		if r.found {
			found++
		}
	}
	return responded, found
}

// liveCount 统计仍持有**活对象**（非墓碑）的副本数。
//
// 与 countResponses 的 found 刻意分开：墓碑也满足 found，但它证明的是
// "这个 key 已被删除"，不是"对象还在"。读路径用它判断存活副本是否够
// R 份，条件写用它判断 key 是否仍被占用。
func liveCount(results []metaResult) int {
	n := 0
	for _, r := range results {
		if r.err != nil || !r.found || r.meta == nil {
			continue
		}
		if !r.meta.Deleted {
			n++
		}
	}
	return n
}

// sameVersion 判断两份元数据是否描述了同一个版本。
//
// 判定与 newerMeta/staleMeta 一致：(ObjVersion, NodeID) 相同即同一版本。
// 版本号相同而 NodeID 不同是两次并发的写，读到哪个取决于仲裁结果；
// 对读路径而言它们不是"同一份数据"，不能互相替代。
func sameVersion(a, b *store.ObjectMeta) bool {
	if a == nil || b == nil {
		return false
	}
	return a.ObjVersion == b.ObjVersion && a.NodeID == b.NodeID
}

// latestMeta 返回响应中最新的副本元数据；没有副本时返回 nil。
func latestMeta(results []metaResult) *store.ObjectMeta {
	var latest *store.ObjectMeta
	for _, r := range results {
		if r.err != nil || !r.found || r.meta == nil {
			continue
		}
		if latest == nil || newerMeta(r.meta, latest) {
			latest = r.meta
		}
	}
	return latest
}

// firstError 返回第一个非 nil 的错误，仅用于日志与排障。
func firstError(results []metaResult) error {
	for _, r := range results {
		if r.err != nil {
			return r.err
		}
	}
	return nil
}

// distributeBlock 把一个块写向所有仍然存活的目标，并在返回前等全部完成。
//
// alive 现场更新：任何一块失败的目标会被移出，后续块不再写它，
// 它的 meta 也不会被提交 —— 避免造出"有 meta、缺块"的副本。
//
// 必须等全部 goroutine 结束才能返回：b.Data 是 StreamBlocks 复用的
// 缓冲，回调返回后内容就不再属于本次写入。
//
// 第二个返回值表示该块是否在所有仍存活的目标上都是去重命中
// （即本次没有产生任何新块文件）。
func (c *Cluster) distributeBlock(alive map[string]bool, ts []target, b store.Block) (bool, error) {
	type outcome struct {
		id    string
		wrote bool
		err   error
	}
	out := make([]outcome, len(ts))

	var wg sync.WaitGroup
	for i, t := range ts {
		if !alive[t.id] {
			continue
		}
		wg.Add(1)
		go func(i int, t target) {
			defer wg.Done()
			wrote, err := t.putBlock(context.Background(), b.Desc.Hash, b.Data)
			out[i] = outcome{id: t.id, wrote: wrote, err: err}
		}(i, t)
	}
	wg.Wait()

	deduped := true
	for _, o := range out {
		if o.id == "" {
			continue
		}
		if o.err != nil {
			delete(alive, o.id)
			c.logger.Warn("replica_block_failed",
				"peer", o.id, "hash", b.Desc.Hash, "err", o.err.Error())
			continue
		}
		if o.wrote {
			deduped = false
		}
	}
	return deduped, nil
}

// putMetaToAll 并行向 targets 提交副本元数据，返回成功数。
//
// 与块不同，meta 的失败**不**从 alive 中移除目标：这里的 targets
// 已经通过块写验证过，meta 写入失败只是这个版本没能落到该节点，
// 不影响下一块/下一版本是否还能写它。成功数由调用方与 W 比较。
func (c *Cluster) putMetaToAll(bucket, key string, m *store.ObjectMeta, ts []target) (ok int, errs []error) {
	errs = make([]error, len(ts))
	var success int
	var mu sync.Mutex

	var wg sync.WaitGroup
	for i, t := range ts {
		wg.Add(1)
		go func(i int, t target) {
			defer wg.Done()
			if err := t.putMeta(context.Background(), bucket, key, m); err != nil {
				errs[i] = err
				return
			}
			mu.Lock()
			success++
			mu.Unlock()
		}(i, t)
	}
	wg.Wait()
	return success, errs
}

// target 的副本操作：本地直连存储，远端走内部协议。

func (t target) getMeta(ctx context.Context, bucket, key string) (*store.ObjectMeta, bool, error) {
	if t.local != nil {
		return t.local.LoadReplicaMeta(bucket, key)
	}
	return t.client.GetMeta(ctx, bucket, key)
}

func (t target) putMeta(ctx context.Context, bucket, key string, m *store.ObjectMeta) error {
	if t.local != nil {
		return t.local.PutReplicaMeta(bucket, key, m)
	}
	return t.client.PutMeta(ctx, bucket, key, m)
}

func (t target) putBlock(ctx context.Context, hash string, data []byte) (bool, error) {
	if t.local != nil {
		return t.local.PutBlock(hash, data)
	}
	return t.client.PutBlock(ctx, hash, data)
}

func (t target) getBlock(ctx context.Context, b store.BlockDesc, offset, length int64, w io.Writer) error {
	if t.local != nil {
		return t.local.ReadBlockRange(w, b, offset, length)
	}
	return t.client.GetBlock(ctx, b, offset, length, w)
}
