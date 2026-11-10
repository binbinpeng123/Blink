package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"

	"github.com/binbinpeng123/Blink/internal/checksum"
	"github.com/binbinpeng123/Blink/internal/ec"
	"github.com/binbinpeng123/Blink/internal/store"
)

// OpenObject 打开环上最新版本的副本。
//
// 读路径承担三件事：
//   - 多份 meta 里选 (ObjVersion, NodeID) 最大的一份，保证写到 W 之后
//     从任意节点都能读到最新版本；
//   - 最新版本是墓碑 ⇒ 404：删除必须立刻对任意节点可见，不能被
//     "其它节点上还有旧副本"拖成"读得到"；
//   - 去重法定人数：至少 R 个副本响应且至少 R 份存活副本，否则回 503
//     而不是 404 —— 副本不够时"不存在"是不可知，不是事实。
//
// 顺带做读修复：落后/缺失的副本在返回数据之前就入队。让读请求顺带
// 修复的意义在于，被读到的热数据永远不会长期缺副本。
func (c *Cluster) OpenObject(bucket, key string) (store.Reader, error) {
	if err := store.ValidateObjectPath(bucket, key); err != nil {
		return nil, err
	}

	ts := c.targetsFor(bucket, key)
	results := c.probeMetas(bucket, key, ts)

	responded, found := countResponses(results)
	if responded < c.cfg.ReadQuorum {
		// 降级读只放宽"副本数不足"，不放宽"什么都不知道"：
		// found==0 时无论怎么降级都没有可读的数据，仍然回 503。
		if !c.cfg.ReadDegraded || found == 0 {
			return nil, fmt.Errorf("%w: 仅 %d/%d 个副本响应，需要 %d（%v）",
				store.ErrUnavailable, responded, c.cfg.ReplicaCount, c.cfg.ReadQuorum, firstError(results))
		}
		c.logger.Warn("read_degraded",
			"bucket", bucket, "key", key, "responded", responded, "need", c.cfg.ReadQuorum)
	}
	if found == 0 {
		return nil, store.ErrNoSuchKey
	}

	latest := latestMeta(results)
	if latest == nil {
		// countResponses 已保证 found>0；走到这里说明响应格式有误。
		return nil, fmt.Errorf("%w: 副本元数据缺失", store.ErrCorrupt)
	}

	// 在读数据之前先登记修复：墓碑路径也要做，否则删除后残留的活副本
	// 会在下一次读到时让对象"复活"。
	c.enqueueReadRepairs(bucket, key, results, latest)

	// 最新版本是墓碑 ⇒ 对象已被删除。这条判断必须在存活副本数之前：
	// 删除留下的墓碑本来就比任何活副本新，"存活副本不足 R"是删除后的
	// 正常状态，把它报成 503 会让客户端一直重试一个永远不存在的对象。
	if latest.Deleted {
		return nil, store.ErrNoSuchKey
	}
	if live := liveCount(results); live < c.cfg.ReadQuorum {
		if !c.cfg.ReadDegraded {
			return nil, fmt.Errorf("%w: 仅 %d 份存活副本可用，需要 %d",
				store.ErrUnavailable, live, c.cfg.ReadQuorum)
		}
		c.logger.Warn("read_degraded",
			"bucket", bucket, "key", key, "live", live, "need", c.cfg.ReadQuorum)
	}

	// 源顺序：本地优先（命中现有 sendfile 零拷贝路径），其次是持有
	// **同一版本**的远端副本；最后才是其余节点 —— 它们可能是旧版本，
	// 也可能因为 meta 提交失败而只有块没有 meta。块按内容寻址，
	// 旧版本副本上恰好存在本次要读的块时兜底使用无害。
	//
	// 只认同一版本是必须的：旧版本副本的块表指向别的块，把它排在前面
	// 会先拉一遍注定失败的数据，再回落到正确来源，白白付出带宽。
	hasLatest := make(map[string]bool, len(results))
	for _, r := range results {
		if r.err == nil && r.found && r.meta != nil && !r.meta.Deleted && sameVersion(r.meta, latest) {
			hasLatest[r.id] = true
		}
	}
	sources := make([]target, 0, len(ts))
	for _, t := range ts {
		if t.local != nil && hasLatest[t.id] {
			sources = append(sources, t)
		}
	}
	for _, t := range ts {
		if t.local == nil && hasLatest[t.id] {
			sources = append(sources, t)
		}
	}
	for _, t := range ts {
		if !hasLatest[t.id] {
			sources = append(sources, t)
		}
	}

	// EC 对象的数据不在副本集上：副本只负责 meta 仲裁，分片按 meta 中
	// 记录的组读取。组必须取自 meta 而不是重算环 —— 成员表变更后重算
	// 可能指向另一批节点，而分片还在旧组里。
	var ecr *ecReader
	if latest.Layout == store.LayoutEC {
		group, err := c.ecTargetsFor(latest)
		if err != nil {
			return nil, err
		}
		codec, err := ec.NewCodec(latest.EC.K, latest.EC.M)
		if err != nil {
			return nil, fmt.Errorf("%w: EC 参数 k=%d m=%d 非法: %v",
				store.ErrCorrupt, latest.EC.K, latest.EC.M, err)
		}
		ecr = &ecReader{codec: codec, group: group}
	}

	return &clusterReader{
		info: store.ObjectInfo{
			Key:               latest.Key,
			Size:              latest.Size,
			ETag:              latest.ETag,
			Modified:          latest.Modified,
			ContentType:       latest.ContentType,
			UserMeta:          latest.UserMeta,
			HTTPMeta:          latest.HTTPMeta,
			Checksum:          latest.Checksum,
			ChecksumAlgorithm: checksum.Algorithm(latest.ChecksumAlgorithm),
			ChecksumType:      latest.ChecksumType,
			BlockCount:        len(latest.Blocks),
			Version:           latest.ObjVersion,
			Parts:             store.PartBounds(latest.Parts),
		},
		blocks:  latest.Blocks,
		sources: sources,
		ecr:     ecr,
		repair: func(t target, b store.BlockDesc) {
			c.repair.enqueue(t.id, repairBlocks, bucket, key, latest.ObjVersion)
		},
	}, nil
}

// enqueueReadRepairs 把本次探测发现的副本缺陷登记给修复队列。
//
// 只针对"不是最新版本"的副本：与最新版本一致的副本无需修复。
// 不可达的副本同样入队 —— 它恢复后正需要一份 hint。
func (c *Cluster) enqueueReadRepairs(bucket, key string, results []metaResult, latest *store.ObjectMeta) {
	for _, r := range results {
		if r.err == nil && r.found && sameVersion(r.meta, latest) {
			continue
		}
		c.repair.enqueue(r.id, repairMeta, bucket, key, latest.ObjVersion)
	}
}

// clusterReader 是一个由远端块拼成的只读对象视图。
//
// 它不持有任何打开的远端连接：每次 WriteRangeTo 按需向副本拉取
// 覆盖范围内的块。这样对象读取的生命周期不受 HTTP 连接池的
// 空闲回收影响，也不会因为一次大范围读取占住连接。
type clusterReader struct {
	info    store.ObjectInfo
	blocks  []store.BlockDesc
	sources []target

	// ecr 非 nil 表示对象是 EC 布局，块由分片重建而不是从副本整块读取。
	ecr *ecReader

	// repair 在某次块读取因"副本侧原因"失败时上报给协调者。
	// 为 nil 表示不修复（测试与只读视图）。
	repair func(t target, b store.BlockDesc)
}

// Info 返回打开时的元数据快照。
func (r *clusterReader) Info() store.ObjectInfo {
	return r.info
}

// WriteRangeTo 把 [offset, offset+length) 写入 w。
//
// 块表必须连续覆盖请求范围；任何缺口都说明 meta 与块不一致，
// 按 ErrCorrupt 上报，绝不返回一段"看起来正常"的短内容。
func (r *clusterReader) WriteRangeTo(w io.Writer, offset, length int64) error {
	if offset < 0 || length < 0 || offset > r.info.Size || length > r.info.Size-offset {
		return fmt.Errorf("cluster: 读取范围 [%d,%d) 超出对象大小 %d",
			offset, offset+length, r.info.Size)
	}
	if length == 0 {
		return nil
	}

	end := offset + length
	pos := offset
	for _, b := range r.blocks {
		if pos >= end {
			break
		}
		blockEnd := b.Offset + b.Length
		if blockEnd <= pos {
			continue
		}
		if b.Offset > pos {
			return fmt.Errorf("%w: 块表在偏移 %d 处存在缺口", store.ErrCorrupt, pos)
		}
		n := min(blockEnd, end) - pos
		if err := r.readSegment(w, b, pos-b.Offset, n); err != nil {
			return err
		}
		pos += n
	}
	if pos < end {
		return fmt.Errorf("%w: 块表未覆盖到偏移 %d（对象大小 %d）",
			store.ErrCorrupt, end, r.info.Size)
	}
	return nil
}

// readSegment 从第一个可用源读取块内的一段，写入 w。
//
// 传入完整块表描述而不只是哈希：块长度与 CRC32C 是"应该是多少"，
// 每个副本都要拿它们校验自己盘上的内容，否则 verifyOnRead 在远端
// 只能与零比较，必然误报损坏。
//
// 只有"一个字节都还没写"时才允许换源：一旦部分内容已交给调用方，
// 换源会从段首重读并重复写入，产出的内容比失败更糟。
func (r *clusterReader) readSegment(w io.Writer, b store.BlockDesc, offset, length int64) error {
	if r.ecr != nil {
		return r.readECSegment(w, b, offset, length)
	}
	var lastErr error
	for _, src := range r.sources {
		cw := &countingWriter{w: w}
		err := src.getBlock(context.Background(), b, offset, length, cw)
		if err == nil {
			return nil
		}
		// 只有副本侧失败才值得修复。调用方的 writer 出错（连接被取消、
		// 磁盘满）与副本是否缺块无关，上报它只会给修复队列制造噪声。
		if r.repair != nil && isBlockReadFailure(err) {
			r.repair(src, b)
		}
		if cw.n > 0 {
			// 部分字节已经交给调用方，换源会重复写入，只能中断。
			return fmt.Errorf("cluster: 读取块 %s 时传输中断（已写 %d/%d 字节）: %w",
				b.Hash, cw.n, length, err)
		}
		lastErr = err
	}
	return fmt.Errorf("%w: 块 %s 在所有副本上均不可读: %v", store.ErrCorrupt, b.Hash, lastErr)
}

// isBlockReadFailure 判断错误是否来自副本一侧（缺块/损坏/不可达）。
//
// 本地读的失败会被 store 包成 fs 错误或 ErrCorrupt/ErrInvalidBlock，
// 远端失败是 *PeerError。其它错误一律视为调用方侧。
func isBlockReadFailure(err error) bool {
	var pe *PeerError
	if errors.As(err, &pe) {
		return true
	}
	return errors.Is(err, fs.ErrNotExist) ||
		errors.Is(err, store.ErrCorrupt) ||
		errors.Is(err, store.ErrInvalidBlock)
}

// countingWriter 记录成功写出的字节数。
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
