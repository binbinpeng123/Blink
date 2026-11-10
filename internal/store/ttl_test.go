package store

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// testClock 是可控时钟，让"等一小时过期"在单测里变成一次赋值。
type testClock struct{ now time.Time }

func (c *testClock) Now() time.Time          { return c.now }
func (c *testClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

// newTTLTestStore 打开一个注入了时钟与桶级 TTL 的存储。
func newTTLTestStore(t *testing.T, clock *testClock, ttls map[string]time.Duration) *Store {
	t.Helper()
	s, err := Open(Options{
		Root:      filepath.Join(t.TempDir(), "data"),
		Now:       clock.Now,
		BucketTTL: ttls,
	})
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	return s
}

// mustCreateBucket 建桶，容忍"已存在"以便复开同一目录的场景。
func mustCreateBucket(t *testing.T, s *Store, bucket string) {
	t.Helper()
	if err := s.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket(%q) 失败: %v", bucket, err)
	}
}

// TestBucketTTLExpiresLazily 验证过期对象在读路径上"逻辑不存在"，
// 且惰性过期不依赖后台清扫。
func TestBucketTTLExpiresLazily(t *testing.T) {
	clock := &testClock{now: time.Unix(1_700_000_000, 0).UTC()}
	s := newTTLTestStore(t, clock, map[string]time.Duration{"tmp": time.Hour})
	mustCreateBucket(t, s, "tmp")
	mustCreateBucket(t, s, "keep")

	putString(t, s, "tmp", "a.txt", "hello")
	putString(t, s, "keep", "a.txt", "hello")

	m, err := s.loadObjectMeta("tmp", "a.txt")
	if err != nil {
		t.Fatalf("loadObjectMeta 失败: %v", err)
	}
	if want := clock.now.Add(time.Hour).Unix(); m.ExpiresAt != want {
		t.Fatalf("ExpiresAt = %d，期望 %d", m.ExpiresAt, want)
	}
	// 未配置 TTL 的桶不得写过期时间。
	km, err := s.loadObjectMeta("keep", "a.txt")
	if err != nil {
		t.Fatalf("loadObjectMeta(keep) 失败: %v", err)
	}
	if km.ExpiresAt != 0 {
		t.Fatalf("未配置 TTL 的桶 ExpiresAt = %d，期望 0", km.ExpiresAt)
	}

	// 到期前一秒仍可见。
	clock.Advance(time.Hour - time.Second)
	if _, err := s.OpenObject("tmp", "a.txt"); err != nil {
		t.Fatalf("到期前 OpenObject 应成功: %v", err)
	}

	// 恰好到期即视为过期：边界必须与文档一致。
	clock.Advance(time.Second)
	if _, err := s.OpenObject("tmp", "a.txt"); !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("到期后 OpenObject 错误 = %v，期望 ErrNoSuchKey", err)
	}
	res, err := s.ListObjects("tmp", ListOptions{MaxKeys: 100})
	if err != nil {
		t.Fatalf("ListObjects 失败: %v", err)
	}
	if res.KeyCount != 0 {
		t.Fatalf("过期对象不应出现在 LIST 中，实际 %d 条", res.KeyCount)
	}
	// 无 TTL 的桶不受影响。
	res, err = s.ListObjects("keep", ListOptions{MaxKeys: 100})
	if err != nil {
		t.Fatalf("ListObjects(keep) 失败: %v", err)
	}
	if res.KeyCount != 1 {
		t.Fatalf("无 TTL 桶的对象应仍可见，实际 %d 条", res.KeyCount)
	}
	// 惰性过期只改变可见性：元数据目录还在，等清扫回收。
	if _, err := s.loadObjectMeta("tmp", "a.txt"); err != nil {
		t.Fatalf("惰性过期不应删除元数据: %v", err)
	}
}

// TestBucketTTLDeleteBucketAfterExpiry 验证只剩过期对象的桶可以删除。
func TestBucketTTLDeleteBucketAfterExpiry(t *testing.T) {
	clock := &testClock{now: time.Unix(1_700_000_000, 0).UTC()}
	s := newTTLTestStore(t, clock, map[string]time.Duration{"tmp": time.Minute})
	mustCreateBucket(t, s, "tmp")
	putString(t, s, "tmp", "a.txt", "hello")

	if err := s.DeleteBucket("tmp"); !errors.Is(err, ErrBucketNotEmpty) {
		t.Fatalf("未过期时 DeleteBucket 错误 = %v，期望 ErrBucketNotEmpty", err)
	}
	clock.Advance(time.Minute)
	if err := s.DeleteBucket("tmp"); err != nil {
		t.Fatalf("只剩过期对象时 DeleteBucket 应成功: %v", err)
	}
}

// TestSweepExpiredDeletesOnlyExpired 验证清扫删除过期元数据、保留非 TTL
// 桶的对象与 CAS 块，并正确填充统计。
func TestSweepExpiredDeletesOnlyExpired(t *testing.T) {
	clock := &testClock{now: time.Unix(1_700_000_000, 0).UTC()}
	s := newTTLTestStore(t, clock, map[string]time.Duration{"tmp": time.Hour})
	mustCreateBucket(t, s, "tmp")
	mustCreateBucket(t, s, "keep")

	// 用大于内联阈值的对象确保确实产生 CAS 块，才能验证"清扫不回收块"。
	// 默认内联阈值是 64KiB，取 128KiB 留足余量。
	body := bytes.Repeat([]byte("x"), 128<<10)
	if _, err := s.PutObject("tmp", "a.bin", bytes.NewReader(body), PutOptions{ExpectedSize: int64(len(body))}); err != nil {
		t.Fatalf("PutObject(a.bin) 失败: %v", err)
	}
	putString(t, s, "tmp", "b.txt", "b")
	putString(t, s, "keep", "a.txt", "keep")

	m, err := s.loadObjectMeta("tmp", "a.bin")
	if err != nil {
		t.Fatalf("loadObjectMeta 失败: %v", err)
	}
	if len(m.Blocks) == 0 {
		t.Fatal("测试前提不成立：128KiB 对象没有产生 CAS 块")
	}
	blockPath := s.casPath(m.Blocks[0].Hash)

	clock.Advance(2 * time.Hour)
	stats, err := s.SweepExpired(context.Background())
	if err != nil {
		t.Fatalf("SweepExpired 失败: %v", err)
	}
	if stats.Buckets != 1 {
		t.Errorf("Buckets = %d，期望 1（只有已建桶的 tmp 配置了 TTL）", stats.Buckets)
	}
	if stats.ExpiredObjects != 2 || stats.DeletedObjects != 2 {
		t.Errorf("expired/deleted = %d/%d，期望 2/2", stats.ExpiredObjects, stats.DeletedObjects)
	}
	if stats.SkippedObjects != 0 || stats.FailedObjects != 0 {
		t.Errorf("skipped/failed = %d/%d，期望 0/0", stats.SkippedObjects, stats.FailedObjects)
	}
	if !stats.FinishedAt.Equal(clock.now) {
		t.Errorf("FinishedAt = %v，期望 %v", stats.FinishedAt, clock.now)
	}

	for _, key := range []string{"a.bin", "b.txt"} {
		if _, err := s.loadObjectMeta("tmp", key); !errors.Is(err, ErrNoSuchKey) {
			t.Errorf("清扫后 %q 元数据错误 = %v，期望 ErrNoSuchKey", key, err)
		}
	}
	// 非 TTL 桶不受影响。
	if _, err := s.loadObjectMeta("keep", "a.txt"); err != nil {
		t.Errorf("清扫误删了非 TTL 桶对象: %v", err)
	}
	// CAS 块必须保留：它可能被其他 key 去重引用，只能由 GC 回收。
	f, err := s.fs.Open(blockPath)
	if err != nil {
		t.Errorf("清扫不应删除 CAS 块 %s: %v", blockPath, err)
	} else {
		f.Close()
	}

	// 第二轮没有候选，统计归零；LastTTLSweep 反映最近一轮。
	stats, err = s.SweepExpired(context.Background())
	if err != nil {
		t.Fatalf("第二轮 SweepExpired 失败: %v", err)
	}
	if stats.ExpiredObjects != 0 || stats.DeletedObjects != 0 {
		t.Errorf("第二轮 expired/deleted = %d/%d，期望 0/0", stats.ExpiredObjects, stats.DeletedObjects)
	}
	last := s.LastTTLSweep()
	if last.FinishedAt != stats.FinishedAt || last.DeletedObjects != 0 {
		t.Errorf("LastTTLSweep = %+v，期望等于第二轮统计 %+v", last, stats)
	}
}

// TestSweepExpiredRechecksUnderLock 验证清扫复检：扫描之后被覆盖写成的
// 新版本不能被误删。
func TestSweepExpiredRechecksUnderLock(t *testing.T) {
	clock := &testClock{now: time.Unix(1_700_000_000, 0).UTC()}
	s := newTTLTestStore(t, clock, map[string]time.Duration{"tmp": time.Hour})
	mustCreateBucket(t, s, "tmp")

	putString(t, s, "tmp", "k", "old")
	clock.Advance(2 * time.Hour) // 旧版本已过期

	// 扫描到过期条目与真正删除之间，写者覆盖了同一个 key。
	putString(t, s, "tmp", "k", "new")

	deleted, err := s.sweepExpiredObject("tmp", "k")
	if err != nil {
		t.Fatalf("sweepExpiredObject 失败: %v", err)
	}
	if deleted {
		t.Fatal("复检发现新版本未过期，不应删除")
	}
	if _, err := s.OpenObject("tmp", "k"); err != nil {
		t.Fatalf("新版本应仍可读: %v", err)
	}

	// 对象已经不存在时同样只是跳过，不是错误。
	deleted, err = s.sweepExpiredObject("tmp", "gone")
	if err != nil || deleted {
		t.Fatalf("不存在对象 = (%v, %v)，期望 (false, nil)", deleted, err)
	}
}

// TestSweepExpiredWithoutTTL 验证未配置 TTL 时不触碰文件系统，也不报错。
func TestSweepExpiredWithoutTTL(t *testing.T) {
	clock := &testClock{now: time.Unix(1_700_000_000, 0).UTC()}
	s := newTTLTestStore(t, clock, nil)
	mustCreateBucket(t, s, "bkt")
	putString(t, s, "bkt", "a.txt", "hello")

	if got := s.BucketTTLCount(); got != 0 {
		t.Fatalf("BucketTTLCount = %d，期望 0", got)
	}
	stats, err := s.SweepExpired(context.Background())
	if err != nil {
		t.Fatalf("SweepExpired 失败: %v", err)
	}
	if stats.FinishedAt.IsZero() {
		t.Error("无 TTL 的一轮也应记录完成时刻，供指标表达\"清扫在运行\"")
	}
	if stats.Buckets != 0 || stats.ExpiredObjects != 0 {
		t.Errorf("无 TTL 时统计应为零，实际 %+v", stats)
	}
	if _, err := s.loadObjectMeta("bkt", "a.txt"); err != nil {
		t.Fatalf("无 TTL 时对象不应被清理: %v", err)
	}
}

// TestSweepExpiredCanceled 验证 ctx 取消时返回已完成统计与错误，
// 且不把未跑完的一轮写成 LastTTLSweep。
func TestSweepExpiredCanceled(t *testing.T) {
	clock := &testClock{now: time.Unix(1_700_000_000, 0).UTC()}
	s := newTTLTestStore(t, clock, map[string]time.Duration{"tmp": time.Hour})
	mustCreateBucket(t, s, "tmp")
	putString(t, s, "tmp", "a.txt", "hello")
	clock.Advance(2 * time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.SweepExpired(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("SweepExpired 错误 = %v，期望 context.Canceled", err)
	}
	if !s.LastTTLSweep().FinishedAt.IsZero() {
		t.Error("被取消的一轮不应写入 LastTTLSweep")
	}
	if _, err := s.loadObjectMeta("tmp", "a.txt"); err != nil {
		t.Fatalf("取消后对象不应被删除: %v", err)
	}
}

// TestOpenRejectsInvalidBucketTTL 验证 Open 拒绝非法的桶级 TTL 配置。
func TestOpenRejectsInvalidBucketTTL(t *testing.T) {
	cases := []struct {
		name string
		ttl  map[string]time.Duration
	}{
		{"非法桶名", map[string]time.Duration{"Bad_Bucket": time.Hour}},
		{"零时长", map[string]time.Duration{"tmp": 0}},
		{"负时长", map[string]time.Duration{"tmp": -time.Second}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := Open(Options{
				Root:      filepath.Join(t.TempDir(), "data"),
				BucketTTL: c.ttl,
			}); err == nil {
				t.Fatal("Open 应拒绝非法 TTL 配置")
			}
		})
	}
}

// TestCopyObjectUsesDestinationTTL 验证 COPY 的过期时间按目标桶重新计算，
// 而不是从源对象复制；源对象已过期时不能作为 COPY 源。
func TestCopyObjectUsesDestinationTTL(t *testing.T) {
	clock := &testClock{now: time.Unix(1_700_000_000, 0).UTC()}
	s := newTTLTestStore(t, clock, map[string]time.Duration{
		"dst": time.Hour,
		"tmp": time.Minute,
	})
	for _, b := range []string{"src", "dst", "tmp"} {
		mustCreateBucket(t, s, b)
	}

	putString(t, s, "src", "a.txt", "hello")
	clock.Advance(30 * time.Minute)
	if _, err := s.CopyObject("dst", "a.txt", "src", "a.txt", CopyOptions{}); err != nil {
		t.Fatalf("CopyObject 失败: %v", err)
	}
	m, err := s.loadObjectMeta("dst", "a.txt")
	if err != nil {
		t.Fatalf("loadObjectMeta 失败: %v", err)
	}
	// 从**复制时刻**起算一小时，不是从源对象的写入时刻起算。
	if want := clock.now.Add(time.Hour).Unix(); m.ExpiresAt != want {
		t.Fatalf("COPY 目标 ExpiresAt = %d，期望 %d（复制时刻 + 目标桶 TTL）", m.ExpiresAt, want)
	}

	// 复制到无 TTL 的桶不继承源的过期时间。
	if _, err := s.CopyObject("src", "b.txt", "dst", "a.txt", CopyOptions{}); err != nil {
		t.Fatalf("CopyObject(dst->src) 失败: %v", err)
	}
	m, err = s.loadObjectMeta("src", "b.txt")
	if err != nil {
		t.Fatalf("loadObjectMeta(src/b.txt) 失败: %v", err)
	}
	if m.ExpiresAt != 0 {
		t.Fatalf("COPY 不应把源的过期时间带到无 TTL 桶，实际 %d", m.ExpiresAt)
	}

	// 过期源不能再被复制出去。
	putString(t, s, "tmp", "old.txt", "old")
	clock.Advance(time.Minute)
	if _, err := s.CopyObject("dst", "old.txt", "tmp", "old.txt", CopyOptions{}); !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("过期源 CopyObject 错误 = %v，期望 ErrNoSuchKey", err)
	}
}

// TestPutIfNoneMatchIgnoresExpired 验证条件写把已过期对象当成"没有对象"。
func TestPutIfNoneMatchIgnoresExpired(t *testing.T) {
	clock := &testClock{now: time.Unix(1_700_000_000, 0).UTC()}
	s := newTTLTestStore(t, clock, map[string]time.Duration{"tmp": time.Minute})
	mustCreateBucket(t, s, "tmp")

	putString(t, s, "tmp", "k", "old")
	clock.Advance(time.Minute)

	body := "new"
	if _, err := s.PutObject("tmp", "k", bytes.NewReader([]byte(body)), PutOptions{
		ExpectedSize:   int64(len(body)),
		IfNoneMatchAny: true,
	}); err != nil {
		t.Fatalf("过期对象不应触发 412: %v", err)
	}

	// 新版本未过期，此时条件写必须恢复 412 语义。
	if _, err := s.PutObject("tmp", "k", bytes.NewReader([]byte(body)), PutOptions{
		ExpectedSize:   int64(len(body)),
		IfNoneMatchAny: true,
	}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("未过期对象条件写错误 = %v，期望 ErrPreconditionFailed", err)
	}
}

// TestCopyIfNoneMatchIgnoresExpired 验证 COPY 目标上的条件写同样忽略过期对象。
func TestCopyIfNoneMatchIgnoresExpired(t *testing.T) {
	clock := &testClock{now: time.Unix(1_700_000_000, 0).UTC()}
	s := newTTLTestStore(t, clock, map[string]time.Duration{"dst": time.Minute})
	mustCreateBucket(t, s, "src")
	mustCreateBucket(t, s, "dst")

	putString(t, s, "src", "a.txt", "hello")
	putString(t, s, "dst", "a.txt", "old")
	clock.Advance(time.Minute)

	if _, err := s.CopyObject("dst", "a.txt", "src", "a.txt", CopyOptions{IfNoneMatchAny: true}); err != nil {
		t.Fatalf("过期目标不应触发 412: %v", err)
	}
}

// TestMultipartCompleteAppliesBucketTTL 验证分片上传的 TTL 从 Complete
// 时刻起算，而不是从 Initiate 或最后一片上传起算。
func TestMultipartCompleteAppliesBucketTTL(t *testing.T) {
	clock := &testClock{now: time.Unix(1_700_000_000, 0).UTC()}
	s := newTTLTestStore(t, clock, map[string]time.Duration{"models": time.Hour})
	mustCreateBucket(t, s, "models")

	up, err := s.CreateMultipartUpload("models", "w.bin", CreateMultipartOptions{})
	if err != nil {
		t.Fatalf("CreateMultipartUpload 失败: %v", err)
	}
	// 客户端拖了 50 分钟才传完：保留期不应被这段等待吃掉。
	clock.Advance(50 * time.Minute)
	data := []byte("weights")
	info, err := s.UploadPart("models", "w.bin", up.UploadID, 1, bytes.NewReader(data), UploadPartOptions{
		ExpectedSize: int64(len(data)),
	})
	if err != nil {
		t.Fatalf("UploadPart 失败: %v", err)
	}
	if _, err := s.CompleteMultipartUpload("models", "w.bin", up.UploadID,
		[]CompletePart{{PartNumber: 1, ETag: info.ETag}}, "", false); err != nil {
		t.Fatalf("CompleteMultipartUpload 失败: %v", err)
	}

	m, err := s.loadObjectMeta("models", "w.bin")
	if err != nil {
		t.Fatalf("loadObjectMeta 失败: %v", err)
	}
	if want := clock.now.Add(time.Hour).Unix(); m.ExpiresAt != want {
		t.Fatalf("MPU 对象 ExpiresAt = %d，期望 %d（Complete 时刻 + TTL）", m.ExpiresAt, want)
	}
	// Complete 后 59 分钟仍可读，满一小时后过期。
	clock.Advance(59 * time.Minute)
	if _, err := s.OpenObject("models", "w.bin"); err != nil {
		t.Fatalf("Complete 后 59 分钟应可读: %v", err)
	}
	clock.Advance(time.Minute)
	if _, err := s.OpenObject("models", "w.bin"); !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("MPU 对象到期后错误 = %v，期望 ErrNoSuchKey", err)
	}
}

// TestMultipartCompleteIfNoneMatchIgnoresExpired 验证 Complete 的条件写
// 也把过期对象视为不存在。
func TestMultipartCompleteIfNoneMatchIgnoresExpired(t *testing.T) {
	clock := &testClock{now: time.Unix(1_700_000_000, 0).UTC()}
	s := newTTLTestStore(t, clock, map[string]time.Duration{"tmp": time.Minute})
	mustCreateBucket(t, s, "tmp")

	putString(t, s, "tmp", "k", "old")
	clock.Advance(time.Minute)

	up, err := s.CreateMultipartUpload("tmp", "k", CreateMultipartOptions{})
	if err != nil {
		t.Fatalf("CreateMultipartUpload 失败: %v", err)
	}
	data := []byte("new")
	info, err := s.UploadPart("tmp", "k", up.UploadID, 1, bytes.NewReader(data), UploadPartOptions{
		ExpectedSize: int64(len(data)),
	})
	if err != nil {
		t.Fatalf("UploadPart 失败: %v", err)
	}
	if _, err := s.CompleteMultipartUpload("tmp", "k", up.UploadID,
		[]CompletePart{{PartNumber: 1, ETag: info.ETag}}, "", true); err != nil {
		t.Fatalf("过期对象不应触发 412: %v", err)
	}
}
