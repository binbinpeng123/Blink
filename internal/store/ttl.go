package store

// 本文件实现桶级 TTL（中间结果自动过期，03 文档创新 F'）。
//
// 语义分两层：
//
//  1. **惰性过期**：读取、LIST、COPY 源检查、删桶判空都把 ExpiresAt
//     已到的对象视为不存在。即使后台清扫从未运行，客户端也不会读到
//     过期对象 —— 正确性不依赖后台任务。
//  2. **后台清扫**：SweepExpired 定期删除过期对象的元数据目录，把
//     磁盘空间真正释放出来。它只删 meta.json 所在的目录，**不碰 CAS
//     块**：块可能被其他 key 去重引用，只能由 GC 在确认无引用后回收。
//
// 为什么需要清扫：惰性过期只改变"看不看得见"，不回收空间。边端的
// 中间结果（模型分片、推理缓存、临时样本）写入量大、生命周期短，
// 仅靠惰性过期会让磁盘被看不见的数据逐渐填满。
//
// 为什么清扫必须复检：LIST 扫描到一条过期条目与真正删除之间，同一个
// key 可能已被覆盖写入（新对象通常不再过期）。清扫在 key 锁下绕过
// metaCache 重新读盘，只有磁盘上的当前版本仍然过期才删除；否则跳过。
// 这一步与 DeleteObject 的锁顺序一致，不会误删刚写入的新版本。

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"time"
)

// maxTTLSweepFailureDetails 限制明细条数：清扫失败通常意味着权限或
// 磁盘故障，大面积失败时保留前若干条示例即可，避免把内存与日志打满。
// FailedObjects 的计数不受这个上限影响。
const maxTTLSweepFailureDetails = 100

// TTLSweepFailure 是一条清扫失败明细，供日志与排障使用。
type TTLSweepFailure struct {
	Bucket string
	Key    string
	Err    string
}

// TTLSweepStats 是一轮过期对象清扫的结果。
//
// 与 ScrubStats 一样，清扫**不中断**：单个对象删除失败只计数并记录
// 明细，其余对象继续处理。只有"无法列举桶"这类系统错误才让整轮失败。
type TTLSweepStats struct {
	// Buckets 是实际参与清扫的桶数量（已配置 TTL 且当前存在）。
	Buckets int
	// ExpiredObjects 是扫描时观察到的过期对象数，包含随后被并发覆盖、
	// 因而实际未删除的对象。
	ExpiredObjects int
	// DeletedObjects 是真正删除成功的对象数。
	DeletedObjects int
	// SkippedObjects 是复检时已不再过期的候选数（并发覆盖或并发删除）。
	SkippedObjects int
	// FailedObjects 是复检或删除失败的对象数。
	FailedObjects int
	// Failures 只保留前 maxTTLSweepFailureDetails 条明细。
	Failures []TTLSweepFailure
	Duration time.Duration
	// FinishedAt 是本轮结束时刻；为零值表示从未成功完成过清扫。
	FinishedAt time.Time
}

// expiredAt 报告一个绝对过期时刻是否已到。
//
// Unix 秒是存储格式的精度，比较也按秒进行：对象在 ExpiresAt 这一秒
// 开始时即视为过期，与 S3 对 "expires at" 的直觉一致。
func (s *Store) expiredAt(expiresAt int64) bool {
	return expiresAt > 0 && s.now().Unix() >= expiresAt
}

// expiresAtFor 按桶级 TTL 计算对象的绝对过期时刻；未配置时返回 0。
func (s *Store) expiresAtFor(bucket string, modified time.Time) int64 {
	ttl, ok := s.bucketTTL[bucket]
	if !ok || ttl <= 0 {
		return 0
	}
	return modified.Add(ttl).Unix()
}

// BucketTTLCount 返回已配置 TTL 的桶数量，供启动日志与调度判断。
func (s *Store) BucketTTLCount() int { return len(s.bucketTTL) }

// SweepExpired 清扫一遍已配置 TTL 的桶，删除其中已过期对象的元数据目录。
//
// 调用方通常是 cmd/blink 的后台 goroutine。未配置任何 TTL 时立即返回
// 零值统计，不触碰文件系统。
//
// 错误语义：
//   - 单个对象复检/删除失败只记录，不中断本轮；
//   - 桶在列举与扫描之间被删除（ErrBucketNotFound）视为跳过；
//   - ctx 取消时返回已完成的统计与 ctx.Err()，且**不更新** LastTTLSweep
//     （未跑完的一轮不应被当成清扫结论）。
//
// 只删除元数据目录，不回收 CAS 块：与 DeleteObject 完全一致，块由 GC
// 在确认无引用后回收（03 文档 GC 章节）。
func (s *Store) SweepExpired(ctx context.Context) (TTLSweepStats, error) {
	start := s.now()
	stats := TTLSweepStats{}
	if len(s.bucketTTL) == 0 {
		finished := s.now()
		stats.Duration = finished.Sub(start)
		stats.FinishedAt = finished
		s.lastTTLSweep.Store(&stats)
		return stats, nil
	}

	// 排序让日志与测试有稳定顺序；桶数量是运维配置规模，不是热路径。
	buckets := make([]string, 0, len(s.bucketTTL))
	for bucket := range s.bucketTTL {
		buckets = append(buckets, bucket)
	}
	sort.Strings(buckets)

	for _, bucket := range buckets {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if _, err := s.loadBucketMeta(bucket); err != nil {
			if errors.Is(err, ErrBucketNotFound) {
				// 配置先于建桶存在是合法用法；桶还没建就无需清扫。
				continue
			}
			return stats, fmt.Errorf("store: TTL 清扫加载桶 %s 失败: %w", bucket, err)
		}
		stats.Buckets++

		entries, err := s.ScanReplicaEntries(bucket)
		if err != nil {
			if errors.Is(err, ErrBucketNotFound) {
				continue
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return stats, ctxErr
			}
			return stats, fmt.Errorf("store: TTL 清扫列举桶 %s 失败: %w", bucket, err)
		}

		for _, e := range entries {
			if err := ctx.Err(); err != nil {
				return stats, err
			}
			// 墓碑没有内容，也不需要按 TTL 回收。
			if e.Deleted || !s.expiredAt(e.ExpiresAt) {
				continue
			}
			stats.ExpiredObjects++

			deleted, err := s.sweepExpiredObject(bucket, e.Key)
			switch {
			case err != nil:
				stats.FailedObjects++
				if len(stats.Failures) < maxTTLSweepFailureDetails {
					stats.Failures = append(stats.Failures, TTLSweepFailure{
						Bucket: bucket,
						Key:    e.Key,
						Err:    err.Error(),
					})
				}
			case deleted:
				stats.DeletedObjects++
			default:
				stats.SkippedObjects++
			}
		}
	}

	finished := s.now()
	stats.Duration = finished.Sub(start)
	stats.FinishedAt = finished
	s.lastTTLSweep.Store(&stats)
	return stats, nil
}

// sweepExpiredObject 在 key 锁下复检并删除一个过期对象。
//
// 返回 (true, nil) 表示已删除，(false, nil) 表示复检发现对象已不存在
// 或已不再过期（并发覆盖），error 表示复检或删除失败。失败时**不删除**
// 任何东西：宁可留下一份过期数据，也不要把一条无法解析的元数据当成
// 过期对象误删。
func (s *Store) sweepExpiredObject(bucket, key string) (bool, error) {
	unlock := s.locks.Lock(bucket + "\x00" + key)
	defer unlock()

	// 刻意绕过 metaCache：缓存里可能是扫描时的旧快照，而这里的职责
	// 正是确认"磁盘上的当前版本是否仍然过期"。
	m, err := s.readObjectMetaAt(s.objectMetaPath(bucket, key))
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return false, nil
	case err != nil:
		return false, err
	}
	if m.Key != key {
		// 目录名由 key 的 SHA-256 前两位与哈希决定；读到别的 key 说明
		// 数据目录被手工改动或发生哈希碰撞。不删除，明确报损坏。
		return false, fmt.Errorf("%w: 过期对象目录中的 key 为 %q，期望 %q",
			ErrCorrupt, m.Key, key)
	}
	if m.Deleted || !s.expiredAt(m.ExpiresAt) {
		// 扫描之后被覆盖写成了新版本（新版本通常不再过期）。
		return false, nil
	}

	// 先失效缓存再删盘：即使 RemoveAll 失败，后续读取也必须回到盘上
	// 确认，而不是继续返回一份内存里的旧元数据。
	s.metaCache.delete(metaCacheKey{bucket: bucket, key: key})
	if err := s.fs.RemoveAll(s.objectDir(bucket, key)); err != nil {
		return false, fsErr("清扫过期对象", key, err)
	}
	return true, nil
}

// LastTTLSweep 返回最近一轮成功完成的过期对象清扫摘要；从未跑过时返回零值。
//
// 返回值是快照：Failures 明细已复制，调用方可以自由读取。
func (s *Store) LastTTLSweep() TTLSweepStats {
	p := s.lastTTLSweep.Load()
	if p == nil {
		return TTLSweepStats{}
	}
	stats := *p
	stats.Failures = append([]TTLSweepFailure(nil), p.Failures...)
	return stats
}
