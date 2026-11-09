package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/binbinpeng123/Blink/internal/checksum"
)

// scrubTestHash 返回内容的 CAS 块哈希（sha256 小写 hex）。
func scrubTestHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// writeScrubCAS 直接写入一个 CAS 块文件，绕过写入路径。
//
// EC 分片的分布由环决定，测试无法通过 PutObject 在本地凑出
// "部分分片存在、部分缺失"的形态，只能像真实节点那样手动摆好文件。
func writeScrubCAS(t *testing.T, s *Store, hash string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(s.casPath(hash)), 0o755); err != nil {
		t.Fatalf("创建 CAS 目录失败: %v", err)
	}
	if err := os.WriteFile(s.casPath(hash), data, 0o644); err != nil {
		t.Fatalf("写 CAS 块失败: %v", err)
	}
}

// writeScrubECMeta 写入一份 EC 布局的对象元数据。
func writeScrubECMeta(t *testing.T, s *Store, key string, logicalHash string, shards []ShardDesc) {
	t.Helper()
	meta := objectMeta{
		Version:    formatVersion,
		Key:        key,
		Size:       shards[0].Length,
		ETag:       "etag",
		Modified:   time.Now().UTC(),
		Layout:     LayoutEC,
		ObjVersion: 1,
		Blocks: []BlockDesc{{
			// 逻辑整块哈希在本地不存在：EC 只落分片。
			Hash:   logicalHash,
			Length: shards[0].Length,
			Shards: shards,
		}},
	}
	path := s.objectMetaPath("bkt", key)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("创建对象目录失败: %v", err)
	}
	if err := os.WriteFile(path, mustJSON(meta), 0o644); err != nil {
		t.Fatalf("写对象元数据失败: %v", err)
	}
}

// TestScrubECTreatsMissingLocalShardsAsHealthy 验证 EC 巡检不把
// "本地不存在的分片"当成损坏。
//
// 真实集群里本节点可能只持有同一对象的 1 个分片，其余分片在别的
// 组员上；缺分片是反熵修复的职责。若这里当成损坏，每个非组员节点都
// 会持续误报，告警立刻失去意义。
func TestScrubECTreatsMissingLocalShardsAsHealthy(t *testing.T) {
	s := openConcurrencyStore(t)

	present := []byte("local-shard-bytes")
	missing := []byte("elsewhere-shard-bytes")
	presentHash := scrubTestHash(present)
	missingHash := scrubTestHash(missing)
	writeScrubCAS(t, s, presentHash, present)

	writeScrubECMeta(t, s, "ec-object", scrubTestHash([]byte("logical-block")), []ShardDesc{
		{Hash: presentHash, Length: int64(len(present)), CRC32C: checksum.BlockCRC32C(present)},
		{Hash: missingHash, Length: int64(len(missing)), CRC32C: checksum.BlockCRC32C(missing)},
	})

	if err := s.ScrubObject("bkt", "ec-object"); err != nil {
		t.Fatalf("缺失本地分片不应报损坏: %v", err)
	}

	stats, err := s.ScrubAll(context.Background())
	if err != nil {
		t.Fatalf("ScrubAll 失败: %v", err)
	}
	if stats.ScannedObjects != 1 || stats.CorruptObjects != 0 {
		t.Fatalf("EC 健康对象统计不对: %+v", stats)
	}
}

// TestScrubECDetectsCorruptLocalShard 验证本地存在的分片仍然逐片校验，
// 错误信息必须指向**分片**哈希而不是逻辑整块哈希。
func TestScrubECDetectsCorruptLocalShard(t *testing.T) {
	s := openConcurrencyStore(t)

	shard := []byte("local-shard-to-corrupt")
	shardHash := scrubTestHash(shard)
	writeScrubCAS(t, s, shardHash, shard)
	writeScrubECMeta(t, s, "ec-broken", scrubTestHash([]byte("logical-block")), []ShardDesc{
		{Hash: shardHash, Length: int64(len(shard)), CRC32C: checksum.BlockCRC32C(shard)},
	})

	flipByte(t, s.casPath(shardHash), 0)

	err := s.ScrubObject("bkt", "ec-broken")
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("损坏的本地分片未报 ErrCorrupt，实际: %v", err)
	}
	if !strings.Contains(err.Error(), shardHash) {
		t.Errorf("错误信息应指向分片哈希 %s: %v", shardHash, err)
	}

	stats, err := s.ScrubAll(context.Background())
	if err != nil {
		t.Fatalf("ScrubAll 失败: %v", err)
	}
	if stats.CorruptObjects != 1 || len(stats.Corruptions) != 1 {
		t.Fatalf("损坏分片应计入一轮统计: %+v", stats)
	}
	if stats.Corruptions[0].Key != "ec-broken" {
		t.Fatalf("损坏明细指向错误对象: %+v", stats.Corruptions[0])
	}
}

// TestScrubAllReportsAllObjectsAndKeepsDetailCopy 验证：
//   - 损坏对象不中断整轮，健康对象仍被计入；
//   - LastScrub 返回可安全修改的副本。
func TestScrubAllReportsAllObjectsAndKeepsDetailCopy(t *testing.T) {
	s := openConcurrencyStore(t)

	// seed 的选择要让不同对象的块**不可能**去重成同一个 CAS 文件：
	// patternBytes 的 i>>16 项在跨块边界恰好翻转最低位，seed 只差 1
	// 时会互相抵消（0x32 的块 0 与 0x33 偏移 64 KiB 的块内容相同）。
	// 0x31/0x32/0x35 之间的 XOR 都不是单个最低位，不会出现这种碰撞。
	mustPutBytes(t, s, "healthy-a", patternBytes(150<<10, 0x31))
	mustPutBytes(t, s, "healthy-b", patternBytes(90<<10, 0x32))
	mustPutBytes(t, s, "victim", patternBytes(200<<10, 0x35))

	m, err := s.loadObjectMeta("bkt", "victim")
	if err != nil {
		t.Fatalf("读取元数据失败: %v", err)
	}
	bad := m.Blocks[1]
	flipByte(t, s.casPath(bad.Hash), 0)

	stats, err := s.ScrubAll(context.Background())
	if err != nil {
		t.Fatalf("ScrubAll 失败: %v", err)
	}
	if stats.ScannedObjects != 3 {
		t.Fatalf("ScannedObjects = %d，期望 3: %+v", stats.ScannedObjects, stats)
	}
	if stats.CorruptObjects != 1 {
		t.Fatalf("CorruptObjects = %d，期望 1: %+v", stats.CorruptObjects, stats)
	}
	if len(stats.Corruptions) != 1 || stats.Corruptions[0].Key != "victim" {
		t.Fatalf("损坏明细不对: %+v", stats.Corruptions)
	}
	if !strings.Contains(stats.Corruptions[0].Err, bad.Hash) {
		t.Errorf("损坏明细未指出块哈希 %s: %s", bad.Hash, stats.Corruptions[0].Err)
	}
	if stats.FinishedAt.IsZero() {
		t.Error("FinishedAt 不应为零值")
	}

	last := s.LastScrub()
	if last.ScannedObjects != 3 || last.CorruptObjects != 1 {
		t.Fatalf("LastScrub 未记录本轮结果: %+v", last)
	}
	// 返回的是副本：改它不应污染 Store 内的快照。
	last.Corruptions[0].Key = "mutated"
	if got := s.LastScrub().Corruptions[0].Key; got != "victim" {
		t.Errorf("LastScrub 返回了可被外部修改的明细，Key 变成了 %q", got)
	}
}

// TestScrubAllHonorsContextCancellation 验证取消语义：
// 已取消的 context 不扫描任何对象，也不把半轮结果发布为巡检结论。
func TestScrubAllHonorsContextCancellation(t *testing.T) {
	s := openConcurrencyStore(t)
	mustPutBytes(t, s, "obj", patternBytes(10<<10, 0x41))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stats, err := s.ScrubAll(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ctx 取消应返回 context.Canceled，实际 %v（stats=%+v）", err, stats)
	}
	if stats.ScannedObjects != 0 {
		t.Fatalf("取消后不应扫描对象: %+v", stats)
	}
	if last := s.LastScrub(); !last.FinishedAt.IsZero() {
		t.Fatalf("未完成的一轮不应发布 LastScrub: %+v", last)
	}
}
