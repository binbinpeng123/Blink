package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// gcBucket 是 GC 测试统一使用的桶名。
const gcBucket = "gcbkt"

func newGCTestStore(t *testing.T, grace time.Duration) *Store {
	t.Helper()
	s, err := Open(Options{
		Root:      filepath.Join(t.TempDir(), "data"),
		GCGrace:   grace,
		BlockSize: 64 << 10,
	})
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	if err := s.CreateBucket(gcBucket); err != nil {
		t.Fatalf("CreateBucket 失败: %v", err)
	}
	return s
}

// gcPut 写入一个对象，返回它唯一块的哈希。
//
// 测试数据都小于 BlockSize，因此每个对象恰好一个块，便于把
// "对象是否被引用"直接映射到"某个块文件是否存在"。
func gcPut(t *testing.T, s *Store, key string, data []byte) string {
	t.Helper()
	if _, err := s.PutObject(gcBucket, key, bytes.NewReader(data), PutOptions{
		ExpectedSize: int64(len(data)),
	}); err != nil {
		t.Fatalf("PutObject(%q) 失败: %v", key, err)
	}
	return gcHash(data)
}

func gcHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// gcBackdate 把块文件的时间戳改到 age 之前，模拟"早已失去引用"。
func gcBackdate(t *testing.T, s *Store, hash string, age time.Duration) {
	t.Helper()
	ts := time.Now().Add(-age)
	if err := os.Chtimes(s.casPath(hash), ts, ts); err != nil {
		t.Fatalf("回拨块 %s 的 mtime 失败: %v", hash, err)
	}
}

func gcExists(t *testing.T, s *Store, hash string) bool {
	t.Helper()
	_, err := os.Stat(s.casPath(hash))
	if err == nil {
		return true
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false
	}
	t.Fatalf("Stat 块 %s 失败: %v", hash, err)
	return false
}

// TestSweepGarbageHonorsGracePeriod 验证宽限期与回收的基本时序：
// 引用消失的块先进入"可回收"统计，只有 mtime 足够旧才会真正被删除。
func TestSweepGarbageHonorsGracePeriod(t *testing.T) {
	s := newGCTestStore(t, time.Hour)

	data := []byte("alpha-block-content")
	hash := gcPut(t, s, "alpha", data)

	// 仍被引用：既不是可回收，也不会被删。
	stats, err := s.SweepGarbage()
	if err != nil {
		t.Fatalf("SweepGarbage 失败: %v", err)
	}
	if stats.ReclaimableBlocks != 0 || stats.ReclaimedBlocks != 0 {
		t.Fatalf("有对象引用时不应回收: %+v", stats)
	}
	if s.ReclaimableBytes() != 0 {
		t.Fatalf("ReclaimableBytes = %d，期望 0", s.ReclaimableBytes())
	}

	if err := s.DeleteObject(gcBucket, "alpha"); err != nil {
		t.Fatalf("DeleteObject 失败: %v", err)
	}

	// 宽限期内：计入可回收字节，但不删除。
	stats, err = s.SweepGarbage()
	if err != nil {
		t.Fatalf("SweepGarbage 失败: %v", err)
	}
	if stats.ReclaimableBlocks != 1 || stats.ReclaimableBytes != int64(len(data)) {
		t.Fatalf("宽限期内的块统计不对: %+v", stats)
	}
	if stats.ReclaimedBlocks != 0 {
		t.Fatalf("宽限期内不应删除: %+v", stats)
	}
	if !gcExists(t, s, hash) {
		t.Fatal("宽限期内的块被删除")
	}
	// 指标与 /readyz 暴露的正是这份"上限"值。
	if got := s.ReclaimableBytes(); got != int64(len(data)) {
		t.Fatalf("ReclaimableBytes = %d，期望 %d", got, len(data))
	}

	// 超过宽限期：真正删除。
	gcBackdate(t, s, hash, 2*time.Hour)
	stats, err = s.SweepGarbage()
	if err != nil {
		t.Fatalf("SweepGarbage 失败: %v", err)
	}
	if stats.ReclaimedBlocks != 1 || stats.ReclaimedBytes != int64(len(data)) {
		t.Fatalf("应回收 1 个块: %+v", stats)
	}
	if gcExists(t, s, hash) {
		t.Fatal("过期孤儿块未被回收")
	}
	if got := s.ReclaimableBytes(); got != 0 {
		t.Fatalf("回收后 ReclaimableBytes = %d，期望 0", got)
	}
}

// TestSweepGarbageKeepsDedupedBlocks 验证去重语义：
// 只要还有一个对象引用该块，就不能因为另一个对象被删而回收它。
func TestSweepGarbageKeepsDedupedBlocks(t *testing.T) {
	s := newGCTestStore(t, time.Hour)

	data := []byte("shared-dedup-content")
	hash := gcPut(t, s, "one", data)
	if got := gcPut(t, s, "two", data); got != hash {
		t.Fatalf("相同内容应得到相同块哈希: %s vs %s", got, hash)
	}

	if err := s.DeleteObject(gcBucket, "one"); err != nil {
		t.Fatalf("DeleteObject 失败: %v", err)
	}
	gcBackdate(t, s, hash, 2*time.Hour)

	if _, err := s.SweepGarbage(); err != nil {
		t.Fatalf("SweepGarbage 失败: %v", err)
	}
	if !gcExists(t, s, hash) {
		t.Fatal("仍被 two 引用的块被回收")
	}

	if err := s.DeleteObject(gcBucket, "two"); err != nil {
		t.Fatalf("DeleteObject 失败: %v", err)
	}
	if _, err := s.SweepGarbage(); err != nil {
		t.Fatalf("SweepGarbage 失败: %v", err)
	}
	if gcExists(t, s, hash) {
		t.Fatal("最后一个引用删除后块仍未被回收")
	}
}

// TestSweepGarbageRespectsPin 验证 pin 保护：写入路径登记过的块，
// 即使 mtime 很旧也不会被清扫阶段删除。
func TestSweepGarbageRespectsPin(t *testing.T) {
	s := newGCTestStore(t, time.Hour)

	data := []byte("in-flight-block")
	hash := gcHash(data)
	// putBlock 返回时块处于 pinned 状态，模拟"已落盘、meta 尚未提交"。
	if _, err := s.putBlock(hash, data); err != nil {
		t.Fatalf("putBlock 失败: %v", err)
	}
	gcBackdate(t, s, hash, 2*time.Hour)

	if _, err := s.SweepGarbage(); err != nil {
		t.Fatalf("SweepGarbage 失败: %v", err)
	}
	if !gcExists(t, s, hash) {
		t.Fatal("pin 中的块被回收")
	}

	s.unpinBlock(hash)
	if _, err := s.SweepGarbage(); err != nil {
		t.Fatalf("SweepGarbage 失败: %v", err)
	}
	if gcExists(t, s, hash) {
		t.Fatal("解除 pin 后块仍未被回收")
	}
}

// TestSweepGarbageKeepsECShardsOnly 验证 EC 对象只按 Shards 计引用：
// Blocks[].Hash 是逻辑整块，本地不一定存在，不能把它当成引用。
func TestSweepGarbageKeepsECShardsOnly(t *testing.T) {
	s := newGCTestStore(t, time.Hour)

	// 两个真实存在的块文件：一个被分片引用，一个是孤儿。
	shardData := []byte("ec-shard-bytes")
	orphanData := []byte("ec-orphan-bytes")
	shardHash, orphanHash := gcHash(shardData), gcHash(orphanData)
	for _, b := range []struct {
		hash string
		data []byte
	}{{shardHash, shardData}, {orphanHash, orphanData}} {
		dir := filepath.Dir(s.casPath(b.hash))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("创建 CAS 目录失败: %v", err)
		}
		if err := os.WriteFile(s.casPath(b.hash), b.data, 0o644); err != nil {
			t.Fatalf("写块失败: %v", err)
		}
		gcBackdate(t, s, b.hash, 2*time.Hour)
	}

	meta := objectMeta{
		Version:    formatVersion,
		Key:        "ec-object",
		Size:       int64(len(shardData)),
		ETag:       "etag",
		Modified:   time.Now().UTC(),
		Layout:     LayoutEC,
		ObjVersion: 1,
		Blocks: []BlockDesc{{
			// 逻辑整块哈希在本地不存在，GC 不得据此要求文件存在。
			Hash:   gcHash([]byte("logical-full-block")),
			Length: int64(len(shardData)),
			Shards: []ShardDesc{{Hash: shardHash, Length: int64(len(shardData))}},
		}},
	}
	path := s.objectMetaPath(gcBucket, meta.Key)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("创建对象目录失败: %v", err)
	}
	if err := os.WriteFile(path, mustJSON(meta), 0o644); err != nil {
		t.Fatalf("写对象元数据失败: %v", err)
	}

	stats, err := s.SweepGarbage()
	if err != nil {
		t.Fatalf("SweepGarbage 失败: %v", err)
	}
	if !gcExists(t, s, shardHash) {
		t.Fatal("EC 分片被回收")
	}
	if gcExists(t, s, orphanHash) {
		t.Fatal("孤儿块未被回收")
	}
	if stats.ReclaimedBytes != int64(len(orphanData)) {
		t.Fatalf("ReclaimedBytes = %d，期望 %d", stats.ReclaimedBytes, len(orphanData))
	}
}

// TestSweepGarbageKeepsMultipartPartBlocks 验证进行中的分片上传
// 也构成引用：分片状态引用的块不能被回收。
func TestSweepGarbageKeepsMultipartPartBlocks(t *testing.T) {
	s := newGCTestStore(t, time.Hour)

	up, err := s.CreateMultipartUpload(gcBucket, "mpu-object", CreateMultipartOptions{})
	if err != nil {
		t.Fatalf("CreateMultipartUpload 失败: %v", err)
	}
	data := []byte("multipart-part-bytes")
	if _, err := s.UploadPart(gcBucket, "mpu-object", up.UploadID, 1, bytes.NewReader(data),
		UploadPartOptions{ExpectedSize: int64(len(data))}); err != nil {
		t.Fatalf("UploadPart 失败: %v", err)
	}
	hash := gcHash(data)
	gcBackdate(t, s, hash, 2*time.Hour)

	if _, err := s.SweepGarbage(); err != nil {
		t.Fatalf("SweepGarbage 失败: %v", err)
	}
	if !gcExists(t, s, hash) {
		t.Fatal("分片上传引用的块被回收")
	}

	if err := s.AbortMultipartUpload(gcBucket, "mpu-object", up.UploadID); err != nil {
		t.Fatalf("AbortMultipartUpload 失败: %v", err)
	}
	if _, err := s.SweepGarbage(); err != nil {
		t.Fatalf("SweepGarbage 失败: %v", err)
	}
	if gcExists(t, s, hash) {
		t.Fatal("分片上传取消后块仍未被回收")
	}
}

// TestSweepGarbageFailsClosedOnCorruptMeta 验证标记阶段 fail-closed：
// 一份读不懂的元数据可能引用任意块，因此本轮一个块都不能删。
func TestSweepGarbageFailsClosedOnCorruptMeta(t *testing.T) {
	s := newGCTestStore(t, time.Hour)

	orphan := []byte("orphan-under-corrupt-meta")
	hash := gcHash(orphan)
	dir := filepath.Dir(s.casPath(hash))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("创建 CAS 目录失败: %v", err)
	}
	if err := os.WriteFile(s.casPath(hash), orphan, 0o644); err != nil {
		t.Fatalf("写块失败: %v", err)
	}
	gcBackdate(t, s, hash, 2*time.Hour)

	metaPath := s.objectMetaPath(gcBucket, "broken")
	if err := os.MkdirAll(filepath.Dir(metaPath), 0o755); err != nil {
		t.Fatalf("创建对象目录失败: %v", err)
	}
	if err := os.WriteFile(metaPath, []byte("{not-json"), 0o644); err != nil {
		t.Fatalf("写损坏元数据失败: %v", err)
	}

	stats, err := s.SweepGarbage()
	if err == nil {
		t.Fatalf("损坏元数据应让本轮 GC 失败，实际 stats=%+v", stats)
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("错误应包装 ErrCorrupt，实际 %v", err)
	}
	if stats.ReclaimedBlocks != 0 {
		t.Fatalf("fail-closed 时不应删除任何块: %+v", stats)
	}
	if !gcExists(t, s, hash) {
		t.Fatal("标记失败后块被删除")
	}
}

// TestOpenRejectsInvalidGCGrace 验证宽限期配置的启动期校验。
func TestOpenRejectsInvalidGCGrace(t *testing.T) {
	if _, err := Open(Options{Root: filepath.Join(t.TempDir(), "data"), GCGrace: -time.Second}); err == nil {
		t.Fatal("负的 GCGrace 应被拒绝")
	}
	// 零值取默认 1 小时，而不是"无宽限期"。
	s, err := Open(Options{Root: filepath.Join(t.TempDir(), "data")})
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	if got := s.GCGrace(); got != defaultGCGrace {
		t.Fatalf("GCGrace = %v，期望 %v", got, defaultGCGrace)
	}
}

// TestSweepGarbageCleansTmpResidue 验证崩溃残留的临时文件会被清理，
// 而"新"临时文件与正式块不受影响。
func TestSweepGarbageCleansTmpResidue(t *testing.T) {
	s := newGCTestStore(t, time.Hour)

	data := []byte("referenced-block")
	hash := gcPut(t, s, "alive", data)

	// 正式块旁的陈旧临时文件（模拟 SIGKILL 留下的 atomicWriter 残骸）。
	dir := filepath.Dir(s.casPath(hash))
	oldTmp := filepath.Join(dir, hash+".tmp-deadbeef")
	freshTmp := filepath.Join(dir, hash+".tmp-cafebabe")
	for _, p := range []string{oldTmp, freshTmp} {
		if err := os.WriteFile(p, []byte("partial"), 0o644); err != nil {
			t.Fatalf("写临时文件失败: %v", err)
		}
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(oldTmp, old, old); err != nil {
		t.Fatalf("回拨临时文件 mtime 失败: %v", err)
	}

	if _, err := s.SweepGarbage(); err != nil {
		t.Fatalf("SweepGarbage 失败: %v", err)
	}
	if _, err := os.Stat(oldTmp); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("陈旧临时文件未被清理: %v", err)
	}
	if _, err := os.Stat(freshTmp); err != nil {
		t.Fatalf("新临时文件不应被清理: %v", err)
	}
	if !gcExists(t, s, hash) {
		t.Fatal("被引用的正式块被清理")
	}
}
