package cluster

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/binbinpeng123/Blink/internal/store"
)

// 本文件是桶的控制面：创建、删除、查询与 CORS。
//
// 它与对象数据面**刻意不对称**：
//   - 建桶 / 写 CORS：本地先落盘，再广播给全部节点；广播失败的节点只告警，
//     不影响请求成功。桶是集群范围的命名空间，但一个暂时不可达的节点
//     不该挡住整个集群建桶；
//   - 删桶：要求**全部节点可达**且集群范围为空。删桶之后若某个宕机节点
//     带着旧桶（以及旧对象）回来，会造出"桶明明删了却还能读到数据"的
//     状态，这个错误比"删桶暂时失败"严重得多，因此宁可回 503；
//   - 读桶信息：本地优先，本地没有才问邻居。

// CreateBucket 在集群范围创建桶。
func (c *Cluster) CreateBucket(bucket string) error {
	if err := store.ValidateBucketName(bucket); err != nil {
		return err
	}
	// 先问邻居：桶是集群范围的命名空间，"A 建过、B 再建"必须回冲突，
	// 否则两个客户端会以为各自拥有同一个桶。
	if c.anyBucketExists(bucket) {
		return store.ErrBucketExists
	}
	if err := c.Store.CreateBucket(bucket); err != nil {
		return err
	}
	c.broadcastBucket(bucket, true)
	return nil
}

// DeleteBucket 在集群范围删除桶。
//
// 三条前置条件缺一不可：全部节点可达、至少一个节点有该桶、每个节点上
// 它都是空的。任何一条不满足都返回明确错误，不做"部分删除"。
func (c *Cluster) DeleteBucket(bucket string) error {
	if err := store.ValidateBucketName(bucket); err != nil {
		return err
	}

	ts := c.allTargets()
	states, errs := c.collectBucketStates(ts, bucket)
	for i, err := range errs {
		if err != nil {
			return fmt.Errorf("%w: 删除桶需要全部 %d 个节点可达，节点 %s 不可用（%v）",
				store.ErrUnavailable, len(ts), ts[i].id, err)
		}
	}

	any := false
	for _, s := range states {
		if !s.Exists {
			continue
		}
		any = true
		if !s.Empty {
			return store.ErrBucketNotEmpty
		}
	}
	if !any {
		return store.ErrBucketNotFound
	}

	// 本地删除：上面已确认本地为空，ErrBucketNotFound 说明本地本来就没有
	// （建桶时本节点不可达），对删除而言都是"删完之后没有"，不算失败。
	if err := c.Store.DeleteBucket(bucket); err != nil && !errors.Is(err, store.ErrBucketNotFound) {
		return err
	}
	if failed := c.broadcastBucket(bucket, false); len(failed) > 0 {
		return fmt.Errorf("%w: 桶已从本节点删除，但节点 %v 删除失败，请重试",
			store.ErrUnavailable, failed)
	}
	return nil
}

// BucketExists 判断桶是否存在于集群任意节点。
//
// 本地有就立即返回 true（零网络开销，也是绝大多数请求的路径）。
// 本地没有再问邻居：可达节点都没有就回答不存在 —— 不可达节点上
// 是否还留着这个桶无从得知，但把它报成 503 会让单个节点宕机
// 阻断整个集群的写入，代价远大于"极少数情况下判错"。
func (c *Cluster) BucketExists(bucket string) (bool, error) {
	if err := store.ValidateBucketName(bucket); err != nil {
		return false, err
	}
	ok, err := c.Store.BucketExists(bucket)
	if err != nil || ok {
		return ok, err
	}
	return c.anyBucketExists(bucket), nil
}

// ListBuckets 返回集群范围的桶名并集。
//
// 任何一个节点不可达都报错，而不是静默返回残缺列表：桶列表是用户
// 判断"我的数据在不在"的依据，少一个桶比报错更难排查。
func (c *Cluster) ListBuckets() ([]string, error) {
	names, err := c.Store.ListBuckets()
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(names))
	for _, n := range names {
		seen[n] = struct{}{}
	}

	ts := c.allTargets()
	got := make([][]string, len(ts))
	errs := make([]error, len(ts))
	var wg sync.WaitGroup
	for i, t := range ts {
		if t.local != nil {
			continue
		}
		wg.Add(1)
		go func(i int, t target) {
			defer wg.Done()
			// 结果带回主协程合并：seen 是普通 map，不在这里并发写。
			got[i], errs[i] = t.client.ListBuckets(context.Background())
		}(i, t)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			return nil, fmt.Errorf("%w: 节点 %s 不可达，无法列举完整桶列表（%v）",
				store.ErrUnavailable, ts[i].id, err)
		}
		for _, n := range got[i] {
			seen[n] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out, nil
}

// BucketCreatedAt 返回桶的创建时间。
func (c *Cluster) BucketCreatedAt(bucket string) (time.Time, error) {
	if err := store.ValidateBucketName(bucket); err != nil {
		return time.Time{}, err
	}
	created, err := c.Store.BucketCreatedAt(bucket)
	if err == nil {
		return created, nil
	}
	if !errors.Is(err, store.ErrBucketNotFound) {
		return time.Time{}, err
	}

	// 本地没有就逐节点问，按节点 ID 序取第一个有值的答案：
	// 各节点的 created_at 各自记录，可能相差几毫秒，但"哪个节点先答"
	// 必须是确定的，否则同一个桶的 CreationDate 会在两次请求间跳动。
	ts := c.allTargets()
	var firstErr error
	for _, t := range ts {
		if t.local != nil {
			continue
		}
		info, err := t.client.BucketInfo(context.Background(), bucket)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if info.Exists {
			return info.CreatedAt, nil
		}
	}
	if firstErr != nil {
		return time.Time{}, fmt.Errorf("%w: 查询桶创建时间失败（%v）", store.ErrUnavailable, firstErr)
	}
	return time.Time{}, store.ErrBucketNotFound
}

// PutBucketCORS 写入桶的 CORS 配置并广播。
func (c *Cluster) PutBucketCORS(bucket string, cfg *store.CORSConfiguration) error {
	exists, err := c.BucketExists(bucket)
	if err != nil {
		return err
	}
	if !exists {
		return store.ErrBucketNotFound
	}

	// 本地有桶就必须在本地也写入：本节点持有的副本可能直接服务于
	// CORS 判定。本地没有桶（建桶时本节点不可达）时跳过，由邻居兜底。
	switch err := c.Store.PutBucketCORS(bucket, cfg); {
	case errors.Is(err, store.ErrBucketNotFound):
		c.logger.Warn("cors_local_bucket_missing", "bucket", bucket)
	case err != nil:
		return err
	}

	ts := c.allTargets()
	var wg sync.WaitGroup
	for _, t := range ts {
		if t.local != nil {
			continue
		}
		wg.Add(1)
		go func(t target) {
			defer wg.Done()
			if err := t.client.PutCORS(context.Background(), bucket, cfg); err != nil {
				// CORS 是展示层配置，不是数据：单个节点没收到只影响路由到
				// 它的浏览器请求会被拒绝，下一次写入会修正。为此把一次
				// 已经成功的写入报成失败，反而会让客户端重复上传。
				c.logger.Warn("cors_broadcast_failed", "peer", t.id, "bucket", bucket, "err", err.Error())
			}
		}(t)
	}
	wg.Wait()
	return nil
}

// GetBucketCORS 读取桶的 CORS 配置。
func (c *Cluster) GetBucketCORS(bucket string) (*store.CORSConfiguration, error) {
	exists, err := c.BucketExists(bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, store.ErrBucketNotFound
	}

	cfg, err := c.Store.GetBucketCORS(bucket)
	if err == nil {
		return cfg, nil
	}
	if !errors.Is(err, store.ErrNoSuchCORSConfiguration) && !errors.Is(err, store.ErrBucketNotFound) {
		return nil, err
	}

	// 本地没有：可能配置是经别的节点写入的。按节点 ID 序取第一个有值的答案，
	// 保证同样的集群状态返回同样的结果。
	ts := c.allTargets()
	for _, t := range ts {
		if t.local != nil {
			continue
		}
		peerCfg, found, err := t.client.GetCORS(context.Background(), bucket)
		if err != nil {
			continue
		}
		if found {
			return peerCfg, nil
		}
	}
	return nil, store.ErrNoSuchCORSConfiguration
}

// DeleteBucketCORS 删除桶的 CORS 配置并广播。
func (c *Cluster) DeleteBucketCORS(bucket string) error {
	exists, err := c.BucketExists(bucket)
	if err != nil {
		return err
	}
	if !exists {
		return store.ErrBucketNotFound
	}

	// 删除是幂等的：未配置过也成功，本地没有桶同样忽略。
	if err := c.Store.DeleteBucketCORS(bucket); err != nil && !errors.Is(err, store.ErrBucketNotFound) {
		return err
	}

	ts := c.allTargets()
	var wg sync.WaitGroup
	for _, t := range ts {
		if t.local != nil {
			continue
		}
		wg.Add(1)
		go func(t target) {
			defer wg.Done()
			if err := t.client.DeleteCORS(context.Background(), bucket); err != nil {
				c.logger.Warn("cors_delete_failed", "peer", t.id, "bucket", bucket, "err", err.Error())
			}
		}(t)
	}
	wg.Wait()
	return nil
}

// anyBucketExists 报告是否有**任一远端**节点持有该桶。
//
// 本地不在查询范围内：调用方（CreateBucket）本来就要在本地落盘，
// 本地状态由 store.CreateBucket 自己判。探测失败只告警 ——
// "问不到"与"没有"在控制面上必须分开处理，但都不该阻止本地操作。
func (c *Cluster) anyBucketExists(bucket string) bool {
	ts := c.allTargets()
	found := make([]bool, len(ts))
	var wg sync.WaitGroup
	for i, t := range ts {
		if t.local != nil {
			continue
		}
		wg.Add(1)
		go func(i int, t target) {
			defer wg.Done()
			ok, err := t.client.BucketInfo(context.Background(), bucket)
			if err != nil {
				c.logger.Warn("bucket_probe_failed", "peer", t.id, "bucket", bucket, "err", err.Error())
				return
			}
			found[i] = ok.Exists
		}(i, t)
	}
	wg.Wait()
	for _, f := range found {
		if f {
			return true
		}
	}
	return false
}

// collectBucketStates 并行查询 ts 上各节点桶的存在性与空否。
func (c *Cluster) collectBucketStates(ts []target, bucket string) ([]bucketState, []error) {
	states := make([]bucketState, len(ts))
	errs := make([]error, len(ts))
	var wg sync.WaitGroup
	for i, t := range ts {
		wg.Add(1)
		go func(i int, t target) {
			defer wg.Done()
			states[i], errs[i] = c.bucketStateOn(t, bucket)
		}(i, t)
	}
	wg.Wait()
	return states, errs
}

// bucketStateOn 查询单个节点上桶的状态。
func (c *Cluster) bucketStateOn(t target, bucket string) (bucketState, error) {
	if t.local != nil {
		exists, err := t.local.BucketExists(bucket)
		if err != nil {
			return bucketState{}, err
		}
		if !exists {
			return bucketState{}, nil
		}
		empty, err := t.local.BucketIsEmpty(bucket)
		if err != nil {
			return bucketState{}, err
		}
		return bucketState{Exists: true, Empty: empty}, nil
	}
	return t.client.BucketState(context.Background(), bucket)
}

// broadcastBucket 把建桶/删桶广播给全部远端节点，返回失败的节点 ID。
//
// 建桶的失败不阻止请求成功；删桶的失败由调用方升级成错误。
func (c *Cluster) broadcastBucket(bucket string, create bool) []string {
	ts := c.allTargets()
	errs := make([]error, len(ts))
	var wg sync.WaitGroup
	for i, t := range ts {
		if t.local != nil {
			continue
		}
		wg.Add(1)
		go func(i int, t target) {
			defer wg.Done()
			if create {
				errs[i] = t.client.CreateBucket(context.Background(), bucket)
			} else {
				errs[i] = t.client.DeleteBucket(context.Background(), bucket)
			}
		}(i, t)
	}
	wg.Wait()

	var failed []string
	for i, err := range errs {
		if err == nil {
			continue
		}
		failed = append(failed, ts[i].id)
		c.logger.Warn("bucket_broadcast_failed",
			"peer", ts[i].id, "bucket", bucket, "create", create, "err", err.Error())
	}
	sort.Strings(failed)
	return failed
}
