package cluster

import (
	"context"
	"fmt"
	"io"

	"github.com/binbinpeng123/Blink/internal/checksum"
	"github.com/binbinpeng123/Blink/internal/store"
)

// OpenObject 打开环上最新版本的副本。
//
// 读路径不做"读修复"（切片 6 实现），但已经做两件必须现在就做的事：
//   - 至少 R 个副本响应且至少 R 份持有该对象，否则回 503 而不是 404：
//     副本不够时"不存在"是不可知，不是事实；
//   - 多份 meta 里选 (ObjVersion, NodeID) 最大的一份，保证写到 W 之后
//     从任意节点都能读到最新版本。
func (c *Cluster) OpenObject(bucket, key string) (store.Reader, error) {
	if err := store.ValidateObjectPath(bucket, key); err != nil {
		return nil, err
	}

	ts := c.targetsFor(bucket, key)
	results := c.probeMetas(bucket, key, ts)

	responded, found := countResponses(results)
	if responded < c.cfg.ReadQuorum {
		return nil, fmt.Errorf("%w: 仅 %d/%d 个副本响应，需要 %d（%v）",
			store.ErrUnavailable, responded, c.cfg.ReplicaCount, c.cfg.ReadQuorum, firstError(results))
	}
	if found == 0 {
		return nil, store.ErrNoSuchKey
	}
	if found < c.cfg.ReadQuorum {
		return nil, fmt.Errorf("%w: 仅 %d 份副本可用，需要 %d",
			store.ErrUnavailable, found, c.cfg.ReadQuorum)
	}

	latest := latestMeta(results)
	if latest == nil {
		// countResponses 已保证 found>0；走到这里说明响应格式有误。
		return nil, fmt.Errorf("%w: 副本元数据缺失", store.ErrCorrupt)
	}

	// 源顺序：本地优先（命中现有 sendfile 零拷贝路径），其次是持有
	// 该版本的远端副本；最后才是"响应了但说没有"的节点 —— 它们可能
	// 因为 meta 提交失败而只有块没有 meta，兜底使用无害（块按内容寻址）。
	hasLatest := make(map[string]bool, len(results))
	for _, r := range results {
		if r.err == nil && r.found && r.meta != nil {
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

	return &clusterReader{
		info: store.ObjectInfo{
			Key:               latest.Key,
			Size:              latest.Size,
			ETag:              latest.ETag,
			Modified:          latest.Modified,
			ContentType:       latest.ContentType,
			UserMeta:          latest.UserMeta,
			Checksum:          latest.Checksum,
			ChecksumAlgorithm: checksum.Algorithm(latest.ChecksumAlgorithm),
			ChecksumType:      latest.ChecksumType,
			BlockCount:        len(latest.Blocks),
			Version:           latest.ObjVersion,
		},
		blocks:  latest.Blocks,
		sources: sources,
	}, nil
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
	var lastErr error
	for _, src := range r.sources {
		cw := &countingWriter{w: w}
		err := src.getBlock(context.Background(), b, offset, length, cw)
		if err == nil {
			return nil
		}
		if cw.n > 0 {
			return fmt.Errorf("cluster: 读取块 %s 时传输中断（已写 %d/%d 字节）: %w",
				b.Hash, cw.n, length, err)
		}
		lastErr = err
	}
	return fmt.Errorf("%w: 块 %s 在所有副本上均不可读: %v", store.ErrCorrupt, b.Hash, lastErr)
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
