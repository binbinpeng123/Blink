package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
	"sync"
	"time"
)

// CAS 块回收（GC）。
//
// 删除对象只删 meta.json，不碰块：同一个块可能被其他 key、其他桶去重引用，
// 只有扫描全量元数据才知道它是否仍然被需要。本文件实现两段式回收：
//
//	标记（无锁）  扫描全部对象元数据与分片状态，得到引用集合
//	清扫（逐块写锁）重新核对 mtime 与 pin，再删除候选块
//
// 并发安全依赖三件事，缺一不可：
//
//  1. 宽限期：只回收 mtime 早于 markStart-grace 的块。边写边标记时，
//     刚提交的块 mtime 必然晚于 markStart，因此不会被误判为孤儿。
//  2. pin：写路径在块落盘前登记、元数据提交后解除。清扫删除前检查 pin，
//     避免删掉"已落盘但 meta 尚未提交"的块。
//  3. 提交时 touch：commitMeta 在写元数据之前刷新所有被引用块的 mtime，
//     让"引用已存在但标记快照还没看到"的窗口也落在宽限期内。
//
// 标记阶段解析失败一律 fail-closed：宁可这一轮不回收，也不能因为一份
// 损坏的元数据读不出块表，就把仍然被引用的块删掉。

const (
	// defaultGCGrace 是默认宽限期。必须显著大于"块落盘到 meta 提交"的
	// 正常耗时，也要覆盖集群模式下远端节点提交元数据的延迟。
	defaultGCGrace = time.Hour
	// gcTmpMaxAge 是临时文件的最长保留时间。
	//
	// 正常写入的临时文件只存在一个块的写入时长（毫秒级）；超过 1 小时
	// 仍存在的必然是进程被杀后留下的残骸。
	gcTmpMaxAge = time.Hour
)

// GCStats 是一轮回收的统计。
//
// Scanned/Referenced/Reclaimable 描述**标记快照**，Reclaimed 描述本轮
// 实际删除的量。两者不相等是正常的：宽限期内的孤儿块要等到 mtime
// 足够旧才会被删。
type GCStats struct {
	// ScannedBlocks 是 CAS 中实际检查的块文件数。
	ScannedBlocks int
	// ReferencedBlocks 是被对象元数据或分片状态引用的块数（去重）。
	ReferencedBlocks int
	// ReclaimableBlocks / ReclaimableBytes 是本次快照中未被引用的块。
	// 包含仍在宽限期或 pin 中的块 —— 它们最终会被回收，只是不是现在。
	ReclaimableBlocks int
	ReclaimableBytes  int64
	// ReclaimedBlocks / ReclaimedBytes 是本次真正删除的块。
	ReclaimedBlocks int
	ReclaimedBytes  int64
	Duration        time.Duration
}

// GCGrace 返回生效的宽限期。
func (s *Store) GCGrace() time.Duration { return s.gcGrace }

// ReclaimableBytes 返回最近一轮 GC 结束后仍未被引用的块字节数。
//
// 该值包含宽限期内的块（以及删除失败的块），因此是"上限"而不是
// "立即可释放"。本轮成功删除的字节不计入。还没有跑过 GC 时为 0。
func (s *Store) ReclaimableBytes() int64 { return s.reclaimable.Load() }

// pinBlock 登记一个"本进程正在使用、尚未被元数据引用"的块。
func (s *Store) pinBlock(hash string) {
	s.pendingMu.Lock()
	s.pending[hash]++
	s.pendingMu.Unlock()
}

// unpinBlock 解除一次登记。重复解除不会把计数减成负数。
func (s *Store) unpinBlock(hash string) {
	s.pendingMu.Lock()
	if n := s.pending[hash]; n > 1 {
		s.pending[hash] = n - 1
	} else {
		delete(s.pending, hash)
	}
	s.pendingMu.Unlock()
}

// unpinBlocks 批量解除登记。
func (s *Store) unpinBlocks(hashes []string) {
	for _, h := range hashes {
		s.unpinBlock(h)
	}
}

// isPinned 报告块是否仍被本进程持有。调用方必须持有 gcMu（读或写）时
// 使用才有并发意义：写路径在 gcMu 读锁下 pin，清扫在写锁下检查。
func (s *Store) isPinned(hash string) bool {
	s.pendingMu.Lock()
	n := s.pending[hash]
	s.pendingMu.Unlock()
	return n > 0
}

// acquireBlock 检查块是否存在，并在检查期间持有 pin。
//
// 返回 exists=true 时 pin 保留，调用方在元数据提交后必须 unpin；
// 返回 exists=false 时 pin 同样保留，调用方写入成功后继续持有、
// 失败则自行 unpin（见 putBlock）。只有在 Stat 返回非 ENOENT 错误时
// 本函数才自行解除 pin 并返回错误。
func (s *Store) acquireBlock(hash string) (bool, error) {
	s.gcMu.RLock()
	s.pinBlock(hash)
	_, err := s.fs.Stat(s.casPath(hash))
	s.gcMu.RUnlock()
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, fs.ErrNotExist):
		return false, nil
	default:
		s.unpinBlock(hash)
		return false, fsErr("检查块", hash, err)
	}
}

// pinBlocks 在 gcMu 读锁下 pin 一组已存在的块，并返回去重后的列表。
//
// 用于"元数据引用已有块、但块不是本次新写"的路径（CopyObject、
// PutReplicaMeta 的调用方）。pin 让清扫无法在"读源元数据 → 写目标
// 元数据"之间回收这些块；同时 Stat 确认它们真的还在，避免写出一个
// 指向缺失块的对象。
func (s *Store) pinBlocks(hashes []string) ([]string, error) {
	pinned := make([]string, 0, len(hashes))
	seen := make(map[string]struct{}, len(hashes))
	s.gcMu.RLock()
	for _, h := range hashes {
		if h == "" {
			continue
		}
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		s.pinBlock(h)
		pinned = append(pinned, h)
		if _, err := s.fs.Stat(s.casPath(h)); err != nil {
			s.gcMu.RUnlock()
			s.unpinBlocks(pinned)
			if errors.Is(err, fs.ErrNotExist) {
				return nil, fmt.Errorf("%w: 块 %s 不存在", ErrCorrupt, h)
			}
			return nil, fsErr("检查块", h, err)
		}
	}
	s.gcMu.RUnlock()
	return pinned, nil
}

// commitMeta 在写元数据之前刷新所引用块的 mtime，然后原子写入元数据。
//
// hashes 可以包含重复项与空串，函数内部会去重；nil 表示这份元数据
// 不引用任何块（例如分片上传的会话状态）。
//
// 刷新失败（文件不存在）直接忽略：块可能在并发删除中被回收，而这次
// 提交如果确实引用它，后续读取会得到 ErrCorrupt，这是正确的报错。
// 其他错误向上返回 —— 拿不到 mtime 就提交元数据，会让一个"引用已存在
// 但 mtime 很旧"的块暴露在下一轮清扫中。
func (s *Store) commitMeta(path string, data []byte, hashes []string) error {
	// 持有读锁期间清扫无法进入删除阶段，因此"touch → 写 meta →
	// 解除 pin"整段不会与删除交错。
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()

	now := s.now()
	seen := make(map[string]struct{}, len(hashes))
	for _, h := range hashes {
		if h == "" {
			continue
		}
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		if err := s.fs.Chtimes(s.casPath(h), now, now); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("store: 刷新块 %s 的 mtime: %w", h, err)
		}
	}
	return s.writeFileAtomic(path, data)
}

// localBlockHashes 返回一份对象元数据引用的本地 CAS 块。
//
// EC 对象只在本地保存分片（Shards），整块 Hash 不一定存在；多副本
// 对象则直接引用 Blocks[].Hash。内联对象的内容在 meta.json 里，块表
// 只为兼容旧版本保留一条逻辑描述，CAS 中并没有对应文件，因此不能
// pin 或刷新它。墓碑没有内容，不引用任何块。
func localBlockHashes(m *objectMeta) []string {
	if m == nil || m.Deleted || m.Layout == LayoutInline {
		return nil
	}
	var out []string
	for _, b := range m.Blocks {
		if m.Layout == LayoutEC && len(b.Shards) > 0 {
			for _, sh := range b.Shards {
				out = append(out, sh.Hash)
			}
			continue
		}
		out = append(out, b.Hash)
	}
	return out
}

// partHashes 返回一份分片状态引用的本地 CAS 块。
func partHashes(ps *PartState) []string {
	if ps == nil {
		return nil
	}
	out := make([]string, 0, len(ps.Blocks))
	for _, b := range ps.Blocks {
		out = append(out, b.Hash)
	}
	return out
}

// gcCandidate 是一个通过 mtime 初筛、等待清扫阶段复核的块。
type gcCandidate struct {
	hash string
	path string
	size int64
}

// SweepGarbage 执行一轮 CAS 垃圾回收。
//
// 同一时刻只允许一轮（gcRunMu）。调用方可以是后台定时任务，也可以是
// 测试/运维的手工触发。返回的错误不会让已经完成的删除回滚。
func (s *Store) SweepGarbage() (GCStats, error) {
	s.gcRunMu.Lock()
	defer s.gcRunMu.Unlock()

	started := time.Now()
	// markStart 是本轮标记的时间快照。mtime 晚于 markStart-grace 的块
	// 一律不碰：它们要么刚写完、要么刚被提交路径 touch 过。
	markStart := s.now()
	cutoff := markStart.Add(-s.gcGrace)
	tmpCutoff := markStart.Add(-gcTmpMaxAge)

	s.cleanTmpRoot(tmpCutoff)

	refs, err := s.collectGarbageRefs(tmpCutoff)
	if err != nil {
		// fail-closed：标记不完整时绝不进入删除阶段。
		return GCStats{Duration: time.Since(started)}, err
	}

	stats, candidates, err := s.scanCAS(cutoff, refs, tmpCutoff)
	if err != nil {
		return GCStats{Duration: time.Since(started)}, err
	}
	stats.ReferencedBlocks = len(refs)

	// 清扫：每个候选块单独持写锁，重新检查 pin 与 mtime。
	// 锁粒度刻意小到"一个块"，否则一轮 GC 会让所有写入停顿数秒。
	touchedDirs := make(map[string]struct{})
	var firstErr error
	for _, c := range candidates {
		s.gcMu.Lock()
		if s.isPinned(c.hash) {
			s.gcMu.Unlock()
			continue
		}
		fi, statErr := s.fs.Stat(c.path)
		if statErr != nil {
			s.gcMu.Unlock()
			if !errors.Is(statErr, fs.ErrNotExist) && firstErr == nil {
				firstErr = fsErr("检查待回收块", c.hash, statErr)
			}
			continue
		}
		// 标记之后有人 touch 过它：说明新的引用已经提交或正在提交。
		if !fi.ModTime().Before(cutoff) {
			s.gcMu.Unlock()
			continue
		}
		removeErr := s.fs.Remove(c.path)
		s.gcMu.Unlock()
		if removeErr != nil {
			if !errors.Is(removeErr, fs.ErrNotExist) && firstErr == nil {
				firstErr = fsErr("回收块", c.hash, removeErr)
			}
			continue
		}
		touchedDirs[joinPath(s.root, casDirName, c.hash[:2])] = struct{}{}
		stats.ReclaimedBlocks++
		stats.ReclaimedBytes += c.size
	}
	for dir := range touchedDirs {
		_ = s.fs.SyncDir(dir)
	}

	// 指标描述的是**本轮结束之后**仍未释放的未引用字节。
	// 快照里包含本轮删掉的块，因此减掉成功删除的字节；删除失败的
	// 块仍然占着空间，不减。宽限期内的块自然留在结果里。
	if remaining := stats.ReclaimableBytes - stats.ReclaimedBytes; remaining >= 0 {
		s.reclaimable.Store(remaining)
	}

	// 过期分片上传的清理放在删除之后：它只会让更多块失去引用，
	// 那些块要到下一轮才会被回收（宽限期本来也覆盖这一点）。
	if err := s.cleanExpiredUploads(); err != nil && firstErr == nil {
		firstErr = err
	}
	stats.Duration = time.Since(started)
	return stats, firstErr
}

// collectGarbageRefs 扫描全部桶的对象元数据与全部分片状态，返回引用集合。
//
// 任何一份元数据/分片状态解析失败都返回错误（fail-closed）；
// "文件在遍历途中消失"是并发删除，跳过即可。
func (s *Store) collectGarbageRefs(tmpCutoff time.Time) (map[string]struct{}, error) {
	refs := make(map[string]struct{})
	buckets, err := s.ListBuckets()
	if err != nil {
		return nil, err
	}
	for _, bucket := range buckets {
		if err := s.scanGarbageBucket(bucket, refs, tmpCutoff); err != nil {
			return nil, err
		}
	}
	if err := s.scanGarbageUploads(refs, tmpCutoff); err != nil {
		return nil, err
	}
	return refs, nil
}

// scanGarbageBucket 扫描一个桶的全部对象分片。
//
// 与 readAllEntries 一样按分片并行：GC 每小时才跑一次，但对象可能有
// 十万级，串行读 JSON 会把一次回收拖到几十秒。
func (s *Store) scanGarbageBucket(bucket string, refs map[string]struct{}, tmpCutoff time.Time) error {
	root := s.objectRoot(bucket)
	entries, err := s.fs.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil // 桶创建中途失败，视同空桶
	}
	if err != nil {
		return fsErr("读取 objects 目录", bucket, err)
	}
	shards := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			shards = append(shards, e.Name())
		}
	}
	if len(shards) == 0 {
		return nil
	}

	partial := make([]map[string]struct{}, len(shards))
	errs := make([]error, len(shards))
	workers := listScanWorkers(len(shards))
	if workers <= 1 {
		for i, shard := range shards {
			partial[i], errs[i] = s.scanGarbageShard(root, shard, tmpCutoff)
		}
	} else {
		var wg sync.WaitGroup
		next := make(chan int)
		wg.Add(workers)
		for w := 0; w < workers; w++ {
			go func() {
				defer wg.Done()
				for i := range next {
					partial[i], errs[i] = s.scanGarbageShard(root, shards[i], tmpCutoff)
				}
			}()
		}
		for i := range shards {
			next <- i
		}
		close(next)
		wg.Wait()
	}
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	for _, m := range partial {
		for h := range m {
			refs[h] = struct{}{}
		}
	}
	return nil
}

// scanGarbageShard 扫描 objects/<shard>/ 下的全部对象目录。
func (s *Store) scanGarbageShard(root, shardName string, tmpCutoff time.Time) (map[string]struct{}, error) {
	shardDir := joinPath(root, shardName)
	entries, err := s.fs.ReadDir(shardDir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fsErr("读取对象分片目录", shardName, err)
	}
	out := make(map[string]struct{})
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := joinPath(shardDir, e.Name())
		s.cleanTmpResidue(dir, tmpCutoff)
		m, err := s.readObjectMetaAt(joinPath(dir, objectMetaFileName))
		if errors.Is(err, fs.ErrNotExist) {
			continue // 并发删除
		}
		if err != nil {
			return nil, err
		}
		want := hashKey(m.Key)
		if m.Key == "" || want != e.Name() || want[:2] != shardName {
			return nil, fmt.Errorf("%w: 目录 %s/%s 与元数据 key %q 不一致",
				ErrCorrupt, shardName, e.Name(), m.Key)
		}
		for _, h := range localBlockHashes(m) {
			out[h] = struct{}{}
		}
	}
	return out, nil
}

// scanGarbageUploads 扫描全部进行中的分片上传，把分片状态引用的块计入引用集合。
//
// 分片状态与对象元数据一样是原子写入的，读到一半的状态不可能出现；
// 但手工改盘或磁盘损坏仍可能留下非法 JSON，此时必须 fail-closed。
func (s *Store) scanGarbageUploads(refs map[string]struct{}, tmpCutoff time.Time) error {
	entries, err := s.fs.ReadDir(s.multipartRoot())
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fsErr("读取分片上传根目录", multipartDirName, err)
	}
	for _, e := range entries {
		if !e.IsDir() || !validUploadID(e.Name()) {
			continue
		}
		uploadDir := s.multipartUploadDir(e.Name())
		s.cleanTmpResidue(uploadDir, tmpCutoff)
		partsDir := s.multipartPartsDir(e.Name())
		s.cleanTmpResidue(partsDir, tmpCutoff)
		parts, err := s.fs.ReadDir(partsDir)
		if errors.Is(err, fs.ErrNotExist) {
			continue // 会话目录残缺或并发 Abort/Complete
		}
		if err != nil {
			return fsErr("读取分片状态目录", e.Name(), err)
		}
		for _, p := range parts {
			if p.IsDir() || !strings.HasSuffix(p.Name(), multipartPartSuffixName) {
				continue
			}
			data, err := readFile(s.fs, joinPath(partsDir, p.Name()))
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return fsErr("读取分片状态", p.Name(), err)
			}
			var ps PartState
			if err := json.Unmarshal(data, &ps); err != nil {
				return fmt.Errorf("%w: 分片状态 %s 不是合法 JSON: %v", ErrCorrupt, p.Name(), err)
			}
			if err := validatePartState(&ps); err != nil {
				return err
			}
			// 文件名必须与状态内的编号一致，否则目录被手工改动过。
			if p.Name() != strconv.Itoa(ps.PartNumber)+multipartPartSuffixName {
				return fmt.Errorf("%w: 分片状态文件名 %s 与编号 %d 不一致",
					ErrCorrupt, p.Name(), ps.PartNumber)
			}
			for _, h := range partHashes(&ps) {
				refs[h] = struct{}{}
			}
		}
	}
	return nil
}

// scanCAS 遍历整个 CAS，按引用集合与 cutoff 分出候选块。
//
// 顺带做两件事：清理 CAS 分片目录里崩溃残留的 .tmp-*，以及统计
// "未被引用"的总字节数（含宽限期内的块，供指标展示）。
func (s *Store) scanCAS(cutoff time.Time, refs map[string]struct{}, tmpCutoff time.Time) (GCStats, []gcCandidate, error) {
	var stats GCStats
	var candidates []gcCandidate

	root := joinPath(s.root, casDirName)
	shards, err := s.fs.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return stats, nil, nil
	}
	if err != nil {
		return stats, nil, fsErr("读取 CAS 目录", casDirName, err)
	}
	for _, shard := range shards {
		if !shard.IsDir() {
			continue
		}
		shardDir := joinPath(root, shard.Name())
		s.cleanTmpResidue(shardDir, tmpCutoff)
		entries, err := s.fs.ReadDir(shardDir)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return stats, nil, fsErr("读取 CAS 分片目录", shard.Name(), err)
		}
		for _, e := range entries {
			if e.IsDir() || !validBlockHashName(e.Name()) {
				continue
			}
			info, err := e.Info()
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					continue
				}
				return stats, nil, fsErr("读取块信息", e.Name(), err)
			}
			stats.ScannedBlocks++
			if _, ok := refs[e.Name()]; ok {
				continue
			}
			stats.ReclaimableBlocks++
			stats.ReclaimableBytes += info.Size()
			if info.ModTime().Before(cutoff) {
				candidates = append(candidates, gcCandidate{
					hash: e.Name(),
					path: joinPath(shardDir, e.Name()),
					size: info.Size(),
				})
			}
		}
	}
	return stats, candidates, nil
}

// validBlockHashName 报告文件名是否是 64 位小写十六进制的块哈希。
// 与 validateBlockHash 的区别：这里不分配错误，只用于遍历时过滤。
func validBlockHashName(name string) bool {
	if len(name) != 64 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// cleanTmpResidue 删除目录中超过保留期的 .tmp-* 文件。
//
// 只清理明确的临时文件名，绝不动正式文件。失败一律忽略：
// 残留临时文件不威胁正确性，不值得让一轮 GC 失败。
func (s *Store) cleanTmpResidue(dir string, cutoff time.Time) {
	entries, err := s.fs.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() || !strings.Contains(e.Name(), ".tmp-") {
			continue
		}
		info, err := e.Info()
		if err != nil || !info.ModTime().Before(cutoff) {
			continue
		}
		_ = s.fs.Remove(joinPath(dir, e.Name()))
	}
}

// cleanTmpRoot 清理 tmp/ 下超过保留期的文件。
//
// 与 cleanTmpResidue 不同，这里的文件名没有统一后缀（tmpFile 的前缀是
// 调用方给的 tag），因此按"整目录只放临时文件"的约定整体按年龄清理。
// 当前写入路径没有使用 tmp/，这里主要回收历史版本与异常路径的残留。
func (s *Store) cleanTmpRoot(cutoff time.Time) {
	dir := joinPath(s.root, tmpDirName)
	entries, err := s.fs.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil || !info.ModTime().Before(cutoff) {
			continue
		}
		_ = s.fs.Remove(joinPath(dir, e.Name()))
	}
}
