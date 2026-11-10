package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"time"

	"github.com/binbinpeng123/Blink/internal/checksum"
)

// 本文件实现数据完整性检查的两种入口：
//
//  1. 读取时校验（Options.VerifyOnRead，默认关闭）：读取路径每块先重算
//     CRC32C 再写出，发现位翻转立即返回 ErrCorrupt，绝不把错误数据发给客户端。
//  2. 后台巡检（ScrubObject / ScrubAll）：按块重算 CRC32C，检出"写完之后
//     才发生"的静默损坏（位翻转、坏扇区），并记录指标与元数据。
//
// 为什么默认关闭：校验必须把数据读进用户态，与 sendfile 零拷贝互斥
// （03 文档"真实张力：校验读 vs 零拷贝读"）。默认保障模型加载吞吐，
// 对可靠性要求更高的部署用 --verify-read 打开。
//
// 两种入口都只校验**块级 CRC32C**，不重算 SHA256：CRC32C 有硬件指令、
// 每秒可校验数 GB，足以发现"盘上的位翻转"；SHA256 用于防篡改，
// 在写入时已经算过并体现在 CAS 路径里，没有必要在读路径上重复付出代价。

// readBlockVerified 读满一个块并核对 CRC32C。
//
// 必须先读完再返回：块级校验和覆盖整块，只读其中一段无法验证。
// 调用方拿到已校验的字节后才可以写向客户端 —— 边读边写会让损坏的
// 数据先一步到达网络上，而 HTTP 响应发出后无法撤回。
func (s *Store) readBlockVerified(f io.Reader, b BlockDesc) ([]byte, error) {
	data := make([]byte, b.Length)
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, fmt.Errorf("%w: 读取块 %s 失败: %v", ErrCorrupt, b.Hash, err)
	}
	if got := checksum.BlockCRC32C(data); got != b.CRC32C {
		return nil, fmt.Errorf("%w: 块 %s 的 CRC32C 为 %08x，块表记录 %08x",
			ErrCorrupt, b.Hash, got, b.CRC32C)
	}
	return data, nil
}

// ScrubObject 重算对象全部块的 CRC32C 并与块表比对。
//
// 返回 ErrCorrupt 时错误信息里带块哈希：运维据此定位具体是哪个 CAS
// 文件坏了，而不是只知道"这个对象有问题"。
//
// 只读 meta.json 与 CAS 块，不修改任何数据，因此可以安全地在服务运行中调用。
func (s *Store) ScrubObject(bucket, key string) error {
	return s.scrubObject(context.Background(), bucket, key)
}

// scrubObject 是带取消的 ScrubObject。
//
// 显式巡检传入 context.Background()；后台全量巡检传入服务生命周期
// context，让关闭时最多等当前块读完之后就能退出，而不是卡在一个
// 数十 GiB 的对象上。
func (s *Store) scrubObject(ctx context.Context, bucket, key string) error {
	if err := validateBucketName(bucket); err != nil {
		return err
	}
	if err := validateKey(key); err != nil {
		return err
	}
	// 与读取路径一致：先确认桶存在，避免桶缺失被报成"对象不存在"。
	if _, err := s.loadBucketMeta(bucket); err != nil {
		return err
	}
	// scrub 刻意**绕过** metaCache 读盘：它的职责是检查盘上现状，
	// 用缓存里的旧快照会把"meta.json 损坏"和"缓存里还留着旧块表"
	// 一起掩盖掉，巡检结果就失去了意义。点查路径仍走缓存。
	m, err := s.readObjectMetaAt(s.objectMetaPath(bucket, key))
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return ErrNoSuchKey
	case errors.Is(err, ErrCorrupt):
		return err
	case err != nil:
		return fsErr("读取对象元数据", key, err)
	}
	// 内联对象没有 CAS 块可查：数据就在 meta.json 里，校验内容哈希与
	// CRC32C 即可。必须在下面的块循环之前分流，否则会去 CAS 找那个
	// 只存在于元数据中的块，把每一个健康的内联对象误报成损坏。
	if m.Layout == LayoutInline {
		return s.scrubInline(m)
	}
	for i := range m.Blocks {
		if err := ctx.Err(); err != nil {
			return err
		}
		b := m.Blocks[i]
		if m.Layout == LayoutEC {
			// EC 对象的 Blocks[].Hash 是逻辑整块哈希，本地 CAS 不一定
			// 存在：分片分散在环上的组员，本节点只持有其中一部分。
			// 因此只校验**本地存在**的分片；缺失分片由集群反熵修复，
			// 不属于本节点完整性巡检的职责（否则每个非组员节点都会
			// 把同一批健康对象误报成损坏）。
			if len(b.Shards) == 0 {
				return fmt.Errorf("%w: EC 块 %s 缺少分片表", ErrCorrupt, b.Hash)
			}
			for _, sh := range b.Shards {
				if err := s.scrubShardIfPresent(sh); err != nil {
					return err
				}
			}
			continue
		}
		if err := s.scrubBlock(b); err != nil {
			return err
		}
	}
	return nil
}

// scrubShardIfPresent 校验一个本地存在的 EC 分片。
//
// 分片文件不存在时返回 nil 而不是 ErrCorrupt：这个节点可能恰好不在
// 分片组里，也可能只是尚未被反熵补齐。完整性巡检只对"本地已有字节"
// 负责，与 03 文档"EC 解码按本地存在的分片计数"的边界一致。
func (s *Store) scrubShardIfPresent(sh ShardDesc) error {
	if _, err := s.fs.Stat(s.casPath(sh.Hash)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fsErr("检查分片", sh.Hash, err)
	}
	return s.scrubBlock(BlockDesc{
		Hash:   sh.Hash,
		Length: sh.Length,
		CRC32C: sh.CRC32C,
	})
}

// scrubBlock 校验单个块。
func (s *Store) scrubBlock(b BlockDesc) error {
	path := s.casPath(b.Hash)
	f, err := s.fs.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: 块 %s 缺失（对象元数据仍引用它）", ErrCorrupt, b.Hash)
		}
		return fsErr("打开块", b.Hash, err)
	}
	defer func() { _ = f.Close() }()

	st, err := s.fs.Stat(path)
	if err != nil {
		return fsErr("检查块", b.Hash, err)
	}
	if st.Size() != b.Length {
		return fmt.Errorf("%w: 块 %s 长度 %d，块表记录 %d",
			ErrCorrupt, b.Hash, st.Size(), b.Length)
	}
	// 复用同一个"读满并校验"实现：读取时校验与 scrub 若各写一套，
	// 迟早会出现"一种路径检出了、另一种漏掉了"的分叉。
	if _, err := s.readBlockVerified(f, b); err != nil {
		return err
	}
	return nil
}

// ScrubCorruption 是一处巡检发现，供日志与运维定位。
type ScrubCorruption struct {
	Bucket string
	Key    string
	// Err 是 ScrubObject 返回的错误文本，包含出问题块的哈希或
	// 元数据路径，足以定位到具体文件。
	Err string
}

// ScrubStats 是一轮全量巡检的结果。
//
// 巡检**不中断**：单个对象损坏只计数并记录明细，其余对象继续检查。
// 只有"无法列举/无法读取元数据以外的系统错误"才让整轮失败 —— 那说明
// 巡检本身不可信，而不是某一份数据坏了。
type ScrubStats struct {
	// Buckets 是参与巡检的桶数量。
	Buckets int
	// ScannedObjects 是实际完成校验的对象数（含损坏对象）。
	ScannedObjects int
	// CorruptObjects 是检出损坏的对象数（ScannedObjects 的子集）。
	CorruptObjects int
	// SkippedObjects 是巡检过程中已被并发删除、无需报警的对象数。
	SkippedObjects int
	// Corruptions 只保留前 maxScrubCorruptionDetails 条明细；
	// CorruptObjects 的计数不受这个上限影响。
	Corruptions []ScrubCorruption
	Duration    time.Duration
	// FinishedAt 是本轮结束时刻；为零值表示从未成功完成过巡检。
	FinishedAt time.Time
}

const (
	// maxScrubCorruptionDetails 限制明细条数：巡检的用途是告警与定位，
	// 大面积损坏时保留前若干条示例即可，避免一次性把内存与日志打满。
	maxScrubCorruptionDetails = 100
	// scrubMaxKeys 让 ListObjects 一次返回桶内全部对象。
	//
	// ListObjects 的每一页都要重新扫描整个桶（目录名是 key 的 sha256，
	// 无法按目录定位），按 max-keys 翻页会让全量巡检退化为 O(n²)。
	// 巡检本来就只关心"全部对象各一遍"，一次拿全最省。
	scrubMaxKeys = int(^uint(0) >> 1)
)

// ScrubAll 巡检全部桶的全部对象，返回汇总统计。
//
// 供后台完整性调度使用（见 cmd/blink 的 runIntegrityScrubber）。对象
// 逐个顺序校验，天然把磁盘读带宽限制在单流水平，避免与前台读竞争。
//
// 错误语义：
//   - 损坏（ErrCorrupt）只记录，不中断本轮；
//   - 巡检期间对象/桶被删除（ErrNoSuchKey / ErrBucketNotFound）计入跳过；
//   - 列举或读取元数据发生系统错误时立即返回，因为此时"没发现问题"
//     并不代表"没有问题"；
//   - ctx 取消时返回已完成的统计与 ctx.Err()，且**不更新** LastScrub
//     （未跑完的一轮不应被当成巡检结论）。
func (s *Store) ScrubAll(ctx context.Context) (ScrubStats, error) {
	start := s.now()
	stats := ScrubStats{}

	buckets, err := s.ListBuckets()
	if err != nil {
		return stats, err
	}
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	stats.Buckets = len(buckets)

	for _, bucket := range buckets {
		res, err := s.ListObjects(bucket, ListOptions{MaxKeys: scrubMaxKeys})
		if err != nil {
			// 桶在列举与扫描之间被删掉：跳过，不把运维动作当成故障。
			if errors.Is(err, ErrBucketNotFound) {
				continue
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return stats, ctxErr
			}
			return stats, fmt.Errorf("store: 巡检列举桶 %s 失败: %w", bucket, err)
		}
		for _, obj := range res.Objects {
			if err := ctx.Err(); err != nil {
				return stats, err
			}
			err := s.scrubObject(ctx, bucket, obj.Key)
			switch {
			case err == nil:
				stats.ScannedObjects++
			case errors.Is(err, ErrCorrupt):
				stats.ScannedObjects++
				stats.CorruptObjects++
				if len(stats.Corruptions) < maxScrubCorruptionDetails {
					stats.Corruptions = append(stats.Corruptions, ScrubCorruption{
						Bucket: bucket,
						Key:    obj.Key,
						Err:    err.Error(),
					})
				}
			case errors.Is(err, ErrNoSuchKey), errors.Is(err, ErrBucketNotFound):
				// 并发删除：本轮无需报警。
				stats.SkippedObjects++
			default:
				if ctxErr := ctx.Err(); ctxErr != nil {
					return stats, ctxErr
				}
				return stats, fmt.Errorf("store: 巡检对象 %s/%s 失败: %w", bucket, obj.Key, err)
			}
		}
	}

	finished := s.now()
	stats.Duration = finished.Sub(start)
	stats.FinishedAt = finished
	s.lastScrub.Store(&stats)
	return stats, nil
}

// LastScrub 返回最近一轮成功完成的巡检摘要；从未跑过时返回零值。
//
// 返回值是快照：Corruptions 明细已复制，调用方可以自由读取。
func (s *Store) LastScrub() ScrubStats {
	p := s.lastScrub.Load()
	if p == nil {
		return ScrubStats{}
	}
	stats := *p
	stats.Corruptions = append([]ScrubCorruption(nil), p.Corruptions...)
	return stats
}
