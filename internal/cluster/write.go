package cluster

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/binbinpeng123/Blink/internal/store"
)

// PutObject 把一个对象写入环上选出的 N 个副本。
//
// 流程与单机路径刻意保持一致，只在"块落哪里"这一处分叉：
//
//	写前 meta barrier → 流式切块并扇出 → 校验长度/摘要 → 提交副本 meta
//
// 一致性由三步保证：
//   - 写前探测读 R 份 meta，得到已观察到的最大 ObjVersion，新版本从它之后编号；
//   - 每块必须在至少 W 个目标上落盘，否则整次写入失败；
//   - meta 只提交给拿到了全部块的目标，且要凑够 W 个 ack。
//
// 失败语义与 S3 相同：凑不够 W 回 5xx，但此时部分副本可能已经生效。
// 客户端重试必须幂等 —— S3 的 PUT 本身就是覆盖语义。
func (c *Cluster) PutObject(bucket, key string, body io.Reader, opt store.PutOptions) (*store.ObjectInfo, error) {
	if err := store.ValidateObjectPath(bucket, key); err != nil {
		return nil, err
	}
	if opt.ExpectedSize < 0 {
		return nil, fmt.Errorf("%w: 缺少 Content-Length", store.ErrIncompleteBody)
	}

	// 桶是集群范围的命名空间：只要任意节点上有它，就可以从本节点写入。
	// 直接看本地会把"在 A 建桶、经 B 写入"判成 NoSuchBucket。
	exists, err := c.BucketExists(bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, store.ErrBucketNotFound
	}

	// 同一协调者上同 key 的写入串行化，保证 versionClock 观察到的
	// seen 不会被本节点的并发写绕过。
	unlock := c.locks.Lock(bucket + "\x00" + key)
	defer unlock()

	ts := c.targetsFor(bucket, key)

	// 写前 meta barrier。
	//
	// 这不是为了严格的两阶段提交（那需要共识），而是为了：
	// 条件写能看见其他节点上已提交的对象，且新版本号不小于任何
	// 已经可见的版本。响应不足 R 时宁可失败也不猜。
	probe := c.probeMetas(bucket, key, ts)
	responded, _ := countResponses(probe)
	if responded < c.cfg.ReadQuorum {
		return nil, fmt.Errorf("%w: 仅 %d/%d 个副本可响应，无法确定写入版本（%v）",
			store.ErrUnavailable, responded, c.cfg.ReplicaCount, firstError(probe))
	}
	// 条件写问的是"这个 key 上还有没有活着的对象"。墓碑是删除的证据，
	// 不是对象：把它算进来会让"删掉后用同一个 key 重新上传"永远返回
	// PreconditionFailed。版本号仍然从包含墓碑的 latestMeta 取，
	// 否则新版本会小于墓碑而被副本拒收。
	if opt.IfNoneMatchAny && liveCount(probe) > 0 {
		return nil, store.ErrPreconditionFailed
	}
	// If-Match 比较的是"当前可见对象"的 ETag，因此用 latestLiveMeta：
	// latestMeta 会返回墓碑，而墓碑的 ETag 是删除前的旧值。
	//
	// 这是**尽力而为**的判定（02 文档例外一）：probe 与随后的 meta 提交
	// 之间没有全局锁，另一个协调者对同一 key 的并发写可以插在中间。条件
	// 写能挡住顺序覆盖与客户端重试带来的"读旧值写新值"，但不提供跨节点
	// 的线性化保证 —— 需要严格 CAS 的调用方必须接受这一点。
	if opt.IfMatchETag != "" || opt.IfMatchAny {
		live := latestLiveMeta(probe)
		if live == nil || (!opt.IfMatchAny && live.ETag != opt.IfMatchETag) {
			return nil, store.ErrPreconditionFailed
		}
	}

	var seen uint64
	if m := latestMeta(probe); m != nil {
		seen = m.ObjVersion
	}

	alive := make(map[string]bool, len(ts))
	for _, t := range ts {
		alive[t.id] = true
	}

	// EC 只对足够大的对象生效：小对象走副本，避免 k+m 份分片描述与
	// k+m 个 CAS 文件相对几百字节内容造成的放大。
	useEC := c.ecEnabled() && opt.ExpectedSize >= c.cfg.ECMinSize
	var ecw *ecWriteState
	if useEC {
		ecw, err = c.newECWrite(bucket, key)
		if err != nil {
			return nil, err
		}
	}

	var blockCount, dedupedBlocks int
	var shardBlocks [][]store.ShardDesc
	summary, err := c.Store.StreamBlocksSized(body, opt.ChecksumAlgorithm, key, opt.ExpectedSize, func(b store.Block) error {
		if ecw != nil {
			descs, deduped, err := c.distributeECBlock(ecw, b)
			if err != nil {
				return err
			}
			shardBlocks = append(shardBlocks, descs)
			blockCount++
			if deduped {
				dedupedBlocks++
			}
			return nil
		}
		deduped, full, err := c.distributeBlock(alive, ts, b)
		if err != nil {
			return err
		}
		if len(alive) < c.cfg.WriteQuorum {
			if full > 0 {
				return fmt.Errorf("%w: 块 %s 仅落在 %d 个副本上（%d 个节点已满），需要 %d",
					store.ErrStorageFull, b.Desc.Hash, len(alive), full, c.cfg.WriteQuorum)
			}
			return fmt.Errorf("%w: 某数据块仅落在 %d 个副本上，需要 %d（%v）",
				store.ErrUnavailable, len(alive), c.cfg.WriteQuorum, b.Desc.Hash)
		}
		blockCount++
		if deduped {
			dedupedBlocks++
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// 回调里的 BlockDesc 是值拷贝，分片表只能在 StreamBlocks 返回后
	// 按索引回填；在回调里改 b.Desc 不会传回 summary。
	if ecw != nil {
		if len(shardBlocks) != len(summary.Blocks) {
			return nil, fmt.Errorf("%w: 分片表 %d 项与块表 %d 项不一致",
				store.ErrCorrupt, len(shardBlocks), len(summary.Blocks))
		}
		for i := range summary.Blocks {
			summary.Blocks[i].Shards = shardBlocks[i]
		}
	}

	// 长度与摘要校验必须在 meta 落盘之前：此刻失败只会留下孤儿块，
	// 一旦 meta 提交，客户端就会看到一个"内容不对但确实存在"的对象。
	if summary.Size != opt.ExpectedSize {
		return nil, fmt.Errorf("%w: 收到 %d 字节，Content-Length 声明 %d",
			store.ErrIncompleteBody, summary.Size, opt.ExpectedSize)
	}
	if opt.ContentMD5 != "" && !strings.EqualFold(opt.ContentMD5, summary.ETag) {
		return nil, fmt.Errorf("%w: Content-MD5 声明 %s，实际 %s",
			store.ErrBadDigest, opt.ContentMD5, summary.ETag)
	}

	checksumType := ""
	declared := opt.Checksum
	if opt.ChecksumAlgorithm != "" {
		checksumType = store.ChecksumTypeFullObject
		if declared == "" && opt.LateChecksum != nil {
			if v, ok := opt.LateChecksum(); ok {
				declared = v
			}
		}
		if declared != "" && declared != summary.Checksum {
			return nil, fmt.Errorf("%w: %s 声明 %s，实际 %s",
				store.ErrBadDigest, opt.ChecksumAlgorithm, declared, summary.Checksum)
		}
	}

	contentType := opt.ContentType
	if contentType == "" {
		contentType = store.DefaultContentType
	}

	meta := &store.ObjectMeta{
		Version:           store.FormatVersion,
		Key:               key,
		Size:              summary.Size,
		ETag:              summary.ETag,
		ContentType:       contentType,
		Modified:          c.now().UTC().Truncate(time.Second),
		UserMeta:          opt.UserMeta,
		Blocks:            summary.Blocks,
		Checksum:          summary.Checksum,
		ChecksumAlgorithm: string(opt.ChecksumAlgorithm),
		ChecksumType:      checksumType,
		NodeID:            c.cfg.NodeID,
		ObjVersion:        c.clock.Next(seen),
	}
	if ecw != nil {
		meta.Layout = store.LayoutEC
		meta.EC = &store.ECLayout{K: ecw.codec.K(), M: ecw.codec.M(), Group: ecw.names}
	}

	// EC 的"块已落地"不等于副本节点持有数据：分片在 k+m 个组员上，
	// meta 仍提交给 N 个副本。因此 EC 路径用完整副本集，由 W 个 ack
	// 决定成败；不可达的节点会在这里各自失败，不拖累其余节点提交。
	live := ts
	if ecw == nil {
		live = make([]target, 0, len(ts))
		for _, t := range ts {
			if alive[t.id] {
				live = append(live, t)
			}
		}
		if len(live) < c.cfg.WriteQuorum {
			return nil, fmt.Errorf("%w: 仅 %d 个副本完成了块写入，需要 %d",
				store.ErrUnavailable, len(live), c.cfg.WriteQuorum)
		}
	}

	ok, errs := c.putMetaToAll(bucket, key, meta, live)
	if ok < c.cfg.WriteQuorum {
		for i, err := range errs {
			if err != nil {
				c.logger.Warn("replica_meta_failed", "peer", live[i].id, "err", err.Error())
			}
		}
		return nil, fmt.Errorf("%w: 副本元数据仅 %d 个节点确认，需要 %d",
			store.ErrUnavailable, ok, c.cfg.WriteQuorum)
	}

	// 写已经向客户端确认成功，但副本未必齐全。把缺口交给修复队列：
	//   - meta 提交失败的节点已经有块，只差 meta；
	//   - 块阶段被移出 alive 的节点可能缺任意块，从块补齐开始。
	// ErrStaleMeta 不修复：它表示对端已有更新版本，本任务已无意义。
	for i, err := range errs {
		if err != nil && !isStaleError(err) {
			c.repair.enqueue(live[i].id, repairMeta, bucket, key, meta.ObjVersion)
		}
	}
	if ecw != nil {
		// 分片没写成功的组员各自补分片；不按副本集过滤，否则组里
		// 不在副本集上的节点会被 repair.enqueue 直接丢弃。
		for id := range ecw.failed {
			c.repair.enqueue(id, repairBlocks, bucket, key, meta.ObjVersion)
		}
	} else {
		for _, t := range ts {
			if !alive[t.id] {
				c.repair.enqueue(t.id, repairBlocks, bucket, key, meta.ObjVersion)
			}
		}
	}

	return &store.ObjectInfo{
		Key:               key,
		Size:              summary.Size,
		ETag:              summary.ETag,
		Modified:          meta.Modified,
		ContentType:       contentType,
		UserMeta:          opt.UserMeta,
		Checksum:          summary.Checksum,
		ChecksumAlgorithm: opt.ChecksumAlgorithm,
		ChecksumType:      checksumType,
		BlockCount:        blockCount,
		DedupedBlocks:     dedupedBlocks,
		Version:           meta.ObjVersion,
	}, nil
}

// DeleteObject 在环上写下一条删除墓碑（tombstone）。
//
// S3 的 DELETE 是幂等的：对象不存在也回 204。因此这里**不**先判对象
// 是否存在，而是无条件提交一个更高的版本：
//   - 对象在 → 墓碑版本高于它，此后读与 LIST 都不可能再看到它；
//   - 对象不在 → 墓碑本身就是"这个 key 从此刻起不存在"的记录，重复
//     删除的第 N 次调用与第一次得到同一个结果。
//
// 为什么不直接删 meta：删除时不可达的节点恢复后会带着旧副本回来，
// "对象删了又出现"是最难向用户解释的一类错误。墓碑把删除本身变成一个
// 可比较的版本，任何比它旧的写回都会被副本层拒绝（ErrStaleMeta），
// 旧副本因此无法复活。块不在这里回收 —— 可能被其他 key 去重引用，
// 留给后续切片的墓碑 GC。
func (c *Cluster) DeleteObject(bucket, key string) error {
	if err := store.ValidateObjectPath(bucket, key); err != nil {
		return err
	}

	// 桶不存在时与 PUT 一样明确失败：否则"桶被删了"会伪装成
	// "对象删掉了"，两种状态对客户端的含义完全不同。
	exists, err := c.BucketExists(bucket)
	if err != nil {
		return err
	}
	if !exists {
		return store.ErrBucketNotFound
	}

	unlock := c.locks.Lock(bucket + "\x00" + key)
	defer unlock()

	ts := c.targetsFor(bucket, key)

	// 写前 meta barrier：新版本必须大于已经可见的任何版本（含之前的
	// 墓碑）。少于 R 个响应时无法确定该用哪个版本号，宁可回 503 ——
	// 猜一个版本可能比现有墓碑还低，副本会以 ErrStaleMeta 拒绝，
	// 删除就静默失效了。
	probe := c.probeMetas(bucket, key, ts)
	responded, _ := countResponses(probe)
	if responded < c.cfg.ReadQuorum {
		return fmt.Errorf("%w: 仅 %d/%d 个副本可响应，无法确定删除版本（%v）",
			store.ErrUnavailable, responded, c.cfg.ReplicaCount, firstError(probe))
	}
	var seen uint64
	if m := latestMeta(probe); m != nil {
		seen = m.ObjVersion
	}

	tomb := &store.ObjectMeta{
		Version:    store.FormatVersion,
		Key:        key,
		Modified:   c.now().UTC().Truncate(time.Second),
		NodeID:     c.cfg.NodeID,
		ObjVersion: c.clock.Next(seen),
		Deleted:    true,
	}

	// 提交给**全部**目标而不是只提交给 W 个：墓碑写到的节点越多，
	// 旧副本能存活的空间越小。W 个 ack 就足以向客户端确认删除，
	// 其余节点的失败留给后续切片的反熵修复。
	ok, errs := c.putMetaToAll(bucket, key, tomb, ts)
	if ok < c.cfg.WriteQuorum {
		for i, err := range errs {
			if err != nil {
				c.logger.Warn("tombstone_failed", "peer", ts[i].id, "err", err.Error())
			}
		}
		return fmt.Errorf("%w: 删除墓碑仅 %d 个副本确认，需要 %d",
			store.ErrUnavailable, ok, c.cfg.WriteQuorum)
	}

	// 墓碑缺一个副本，就意味着那个节点上可能留着一个永不消失的旧对象。
	// 删除已向客户端确认，必须把墓碑补到每一个目标。
	for i, err := range errs {
		if err != nil && !isStaleError(err) {
			c.repair.enqueue(ts[i].id, repairMeta, bucket, key, tomb.ObjVersion)
		}
	}
	return nil
}
