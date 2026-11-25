package store

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// openAt 用固定时钟打开存储，复现"重启后自愈"的场景。
func openAt(t *testing.T, root string, now time.Time) *Store {
	t.Helper()
	s, err := Open(Options{Root: root, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("Open(%s) 失败: %v", root, err)
	}
	return s
}

func readObjectBytes(t *testing.T, s *Store, bucket, key string) []byte {
	t.Helper()
	r, err := s.OpenObject(bucket, key)
	if err != nil {
		t.Fatalf("OpenObject(%q) 失败: %v", key, err)
	}
	var buf bytes.Buffer
	if err := r.WriteRangeTo(&buf, 0, r.Info().Size); err != nil {
		t.Fatalf("读取 %q 失败: %v", key, err)
	}
	return buf.Bytes()
}

// TestOpenHealsMissingBucketMeta 锁住 03 文档崩溃恢复第 4 步：
// bucket.json 缺失时按最小元数据重建，桶内对象仍可读。
func TestOpenHealsMissingBucketMeta(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")
	fixed := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	s := openAt(t, root, fixed)
	if err := s.CreateBucket("heal-missing"); err != nil {
		t.Fatalf("CreateBucket 失败: %v", err)
	}
	want := []byte("object survives bucket meta loss")
	if _, err := s.PutObject("heal-missing", "obj", bytes.NewReader(want), PutOptions{ExpectedSize: int64(len(want))}); err != nil {
		t.Fatalf("PutObject 失败: %v", err)
	}
	if err := os.Remove(s.bucketMetaPath("heal-missing")); err != nil {
		t.Fatalf("删除 bucket.json 失败: %v", err)
	}

	s = openAt(t, root, fixed)
	names, err := s.ListBuckets()
	if err != nil {
		t.Fatalf("ListBuckets 失败: %v", err)
	}
	if len(names) != 1 || names[0] != "heal-missing" {
		t.Fatalf("自愈后桶列表 = %v，期望 [heal-missing]", names)
	}
	if got := readObjectBytes(t, s, "heal-missing", "obj"); !bytes.Equal(got, want) {
		t.Fatalf("对象内容 = %q，期望 %q", got, want)
	}
	assertHealedBucketMeta(t, s, "heal-missing", fixed)
}

// TestOpenHealsCorruptBucketMeta 覆盖 bucket.json 内容损坏（JSON 读不懂）。
func TestOpenHealsCorruptBucketMeta(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")
	fixed := time.Date(2026, 11, 25, 11, 0, 0, 0, time.UTC)
	s := openAt(t, root, fixed)
	if err := s.CreateBucket("heal-corrupt"); err != nil {
		t.Fatalf("CreateBucket 失败: %v", err)
	}
	want := []byte("object survives corrupt bucket meta")
	if _, err := s.PutObject("heal-corrupt", "obj", bytes.NewReader(want), PutOptions{ExpectedSize: int64(len(want))}); err != nil {
		t.Fatalf("PutObject 失败: %v", err)
	}
	if err := os.WriteFile(s.bucketMetaPath("heal-corrupt"), []byte("{not-json"), 0o640); err != nil {
		t.Fatalf("写坏 bucket.json 失败: %v", err)
	}

	s = openAt(t, root, fixed)
	if got := readObjectBytes(t, s, "heal-corrupt", "obj"); !bytes.Equal(got, want) {
		t.Fatalf("对象内容 = %q，期望 %q", got, want)
	}
	assertHealedBucketMeta(t, s, "heal-corrupt", fixed)
}

func assertHealedBucketMeta(t *testing.T, s *Store, bucket string, wantCreated time.Time) {
	t.Helper()
	data, err := os.ReadFile(s.bucketMetaPath(bucket))
	if err != nil {
		t.Fatalf("读取重建后的 bucket.json 失败: %v", err)
	}
	var m bucketMeta
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("重建后的 bucket.json 不是合法 JSON: %v", err)
	}
	if m.Name != bucket {
		t.Errorf("重建后的 name = %q，期望 %q", m.Name, bucket)
	}
	if !m.CreatedAt.Equal(wantCreated) {
		t.Errorf("重建后的 created_at = %v，期望 %v", m.CreatedAt, wantCreated)
	}
}

// TestOpenHealsOnlyBucketShapedDirs 验证自愈范围：合法桶名目录被重建，
// buckets/ 下的其它目录保持原样，不会变成可访问的桶。
func TestOpenHealsOnlyBucketShapedDirs(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")
	fixed := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	s := openAt(t, root, fixed)
	for _, name := range []string{"half-created", "Bad_Name"} {
		if err := os.MkdirAll(s.bucketDir(name), 0o750); err != nil {
			t.Fatalf("创建目录 %q 失败: %v", name, err)
		}
	}

	s = openAt(t, root, fixed)
	names, err := s.ListBuckets()
	if err != nil {
		t.Fatalf("ListBuckets 失败: %v", err)
	}
	if len(names) != 1 || names[0] != "half-created" {
		t.Fatalf("桶列表 = %v，期望 [half-created]", names)
	}
	if _, err := os.Stat(s.bucketMetaPath("Bad_Name")); !os.IsNotExist(err) {
		t.Errorf("非法桶名目录不应被重建，Stat 错误 = %v", err)
	}
}

// TestSweepGarbageProtectsObjectsUnderCorruptBucketMeta 是数据安全回归：
// GC 的标记阶段不能复用 ListBuckets 的口径。后者会跳过 bucket.json
// 损坏的桶，若 GC 也跳过，该桶对象引用的块就会被当孤儿删除。
func TestSweepGarbageProtectsObjectsUnderCorruptBucketMeta(t *testing.T) {
	s := newGCTestStore(t, time.Hour)

	data := []byte("protected-by-unreadable-bucket-meta")
	hash := gcPut(t, s, "protected", data)
	gcBackdate(t, s, hash, 2*time.Hour)

	orphan := []byte("orphan-while-bucket-meta-broken")
	orphanHash := gcHash(orphan)
	if err := os.MkdirAll(filepath.Dir(s.casPath(orphanHash)), 0o755); err != nil {
		t.Fatalf("创建 CAS 目录失败: %v", err)
	}
	if err := os.WriteFile(s.casPath(orphanHash), orphan, 0o644); err != nil {
		t.Fatalf("写孤儿块失败: %v", err)
	}
	gcBackdate(t, s, orphanHash, 2*time.Hour)

	// 模拟运行期损坏：丢掉缓存再写坏 bucket.json，让 ListBuckets 跳过该桶。
	s.dropBucketMeta(gcBucket)
	if err := os.WriteFile(s.bucketMetaPath(gcBucket), []byte("{not-json"), 0o640); err != nil {
		t.Fatalf("写坏 bucket.json 失败: %v", err)
	}

	stats, err := s.SweepGarbage()
	if err != nil {
		t.Fatalf("SweepGarbage 失败: %v", err)
	}
	if !gcExists(t, s, hash) {
		t.Fatal("bucket.json 损坏时，对象引用的块被当孤儿删除了")
	}
	if gcExists(t, s, orphanHash) {
		t.Fatal("真正的孤儿块没有被回收")
	}
	if stats.ReclaimedBlocks != 1 {
		t.Errorf("回收块数 = %d，期望 1", stats.ReclaimedBlocks)
	}
}
