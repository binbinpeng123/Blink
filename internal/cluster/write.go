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

	// 桶的存在性暂时只看本节点：桶控制面在切片 4 才会广播到全集群。
	// 在此之前，"在 A 建桶、经 B 写入"应当明确失败，而不是悄悄写进
	// 一个 B 上并不存在的命名空间。
	exists, err := c.Store.BucketExists(bucket)
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
	responded, found := countResponses(probe)
	if responded < c.cfg.ReadQuorum {
		return nil, fmt.Errorf("%w: 仅 %d/%d 个副本可响应，无法确定写入版本（%v）",
			store.ErrUnavailable, responded, c.cfg.ReplicaCount, firstError(probe))
	}
	if opt.IfNoneMatchAny && found > 0 {
		return nil, store.ErrPreconditionFailed
	}

	var seen uint64
	if m := latestMeta(probe); m != nil {
		seen = m.ObjVersion
	}

	alive := make(map[string]bool, len(ts))
	for _, t := range ts {
		alive[t.id] = true
	}

	var blockCount, dedupedBlocks int
	summary, err := c.Store.StreamBlocks(body, opt.ChecksumAlgorithm, key, func(b store.Block) error {
		deduped, err := c.distributeBlock(alive, ts, b)
		if err != nil {
			return err
		}
		if len(alive) < c.cfg.WriteQuorum {
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

	live := make([]target, 0, len(ts))
	for _, t := range ts {
		if alive[t.id] {
			live = append(live, t)
		}
	}
	if len(live) < c.cfg.WriteQuorum {
		return nil, fmt.Errorf("%w: 仅 %d 个副本完成了块写入，需要 %d",
			store.ErrUnavailable, len(live), c.cfg.WriteQuorum)
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

// DeleteObject 从环上的全部副本删除对象。
//
// 当前实现直接删除各节点的 meta（块留给 GC），没有 tombstone：
// 一个在删除时不可达的节点，恢复后会把旧副本带回来。这一点由
// 切片 4 的 tombstone 修复，届时本方法的调用路径不变。
func (c *Cluster) DeleteObject(bucket, key string) error {
	if err := store.ValidateObjectPath(bucket, key); err != nil {
		return err
	}

	unlock := c.locks.Lock(bucket + "\x00" + key)
	defer unlock()

	ts := c.targetsFor(bucket, key)

	// 先确认对象确实存在：直接删的语义是"不存在就回 NoSuchKey"。
	// 响应不足 R 时无法区分"不存在"与"副本都失联"，宁可回 503。
	probe := c.probeMetas(bucket, key, ts)
	responded, found := countResponses(probe)
	if responded < c.cfg.ReadQuorum {
		return fmt.Errorf("%w: 仅 %d/%d 个副本可响应，无法确认对象状态（%v）",
			store.ErrUnavailable, responded, c.cfg.ReplicaCount, firstError(probe))
	}
	if found == 0 {
		return store.ErrNoSuchKey
	}

	ok, errs := c.deleteMetaFromAll(bucket, key, ts)
	if ok < c.cfg.WriteQuorum {
		for i, err := range errs {
			if err != nil {
				c.logger.Warn("replica_delete_failed", "peer", ts[i].id, "err", err.Error())
			}
		}
		return fmt.Errorf("%w: 删除仅 %d 个副本确认，需要 %d",
			store.ErrUnavailable, ok, c.cfg.WriteQuorum)
	}
	return nil
}
