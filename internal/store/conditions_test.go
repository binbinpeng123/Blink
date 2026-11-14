package store

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mustPutConditional 以给定条件写入 body，返回错误供调用方断言。
func mustPutConditional(t *testing.T, s *Store, bucket, key, body string, opt PutOptions) error {
	t.Helper()
	opt.ExpectedSize = int64(len(body))
	_, err := s.PutObject(bucket, key, bytes.NewReader([]byte(body)), opt)
	return err
}

// TestPutIfMatchETag 验证 If-Match: "etag" 的基本语义：
// 匹配才覆盖，不匹配或对象不存在都回 412，且失败不得改动已有内容。
func TestPutIfMatchETag(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)
	first := putString(t, s, "bkt", "k", "v1")

	if err := mustPutConditional(t, s, "bkt", "k", "v2", PutOptions{
		IfMatchETag: first.ETag,
	}); err != nil {
		t.Fatalf("ETag 匹配时 If-Match 应成功，实际 %v", err)
	}

	if err := mustPutConditional(t, s, "bkt", "k", "v3", PutOptions{
		IfMatchETag: first.ETag,
	}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("ETag 已被覆盖后应 412，实际 %v", err)
	}
	meta, err := s.loadObjectMeta("bkt", "k")
	if err != nil {
		t.Fatalf("loadObjectMeta 失败: %v", err)
	}
	if got := string(readBlocks(t, s, meta)); got != "v2" {
		t.Errorf("失败的 If-Match 不应改动内容，实际 %q", got)
	}

	if err := mustPutConditional(t, s, "bkt", "missing", "x", PutOptions{
		IfMatchETag: first.ETag,
	}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("对象不存在时 If-Match 应 412，实际 %v", err)
	}
	if _, err := s.loadObjectMeta("bkt", "missing"); !errors.Is(err, ErrNoSuchKey) {
		t.Errorf("失败的 If-Match 不应创建对象，loadObjectMeta = %v", err)
	}
}

// TestPutIfMatchAny 验证 `If-Match: *`：只要求对象存在。
func TestPutIfMatchAny(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)
	putString(t, s, "bkt", "k", "v1")

	if err := mustPutConditional(t, s, "bkt", "k", "v2", PutOptions{IfMatchAny: true}); err != nil {
		t.Fatalf("对象存在时 If-Match:* 应成功，实际 %v", err)
	}
	if err := mustPutConditional(t, s, "bkt", "fresh", "x", PutOptions{IfMatchAny: true}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("对象不存在时 If-Match:* 应 412，实际 %v", err)
	}
}

// TestPutIfMatchIgnoresTombstone 验证墓碑在条件写里等于"没有对象"。
//
// 把墓碑当成存在会让客户端因为一个自己已经看不见的对象拿到 412，
// 而这个 412 没有任何可恢复的手段 —— 客户端拿不到墓碑的 ETag。
func TestPutIfMatchIgnoresTombstone(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)
	first := putString(t, s, "bkt", "k", "v1")
	if err := s.DeleteObject("bkt", "k"); err != nil {
		t.Fatalf("DeleteObject 失败: %v", err)
	}

	if err := mustPutConditional(t, s, "bkt", "k", "v2", PutOptions{IfMatchAny: true}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("删除后 If-Match:* 应 412，实际 %v", err)
	}
	if err := mustPutConditional(t, s, "bkt", "k", "v2", PutOptions{IfMatchETag: first.ETag}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("删除后旧 ETag 的 If-Match 应 412，实际 %v", err)
	}
	if err := mustPutConditional(t, s, "bkt", "k", "v2", PutOptions{IfNoneMatchAny: true}); err != nil {
		t.Fatalf("删除后 If-None-Match:* 应成功，实际 %v", err)
	}
}

// TestPutIfMatchIgnoresExpired 验证过期对象同样等于"没有对象"。
func TestPutIfMatchIgnoresExpired(t *testing.T) {
	clock := &testClock{now: time.Unix(1_700_000_000, 0).UTC()}
	s := newTTLTestStore(t, clock, map[string]time.Duration{"tmp": time.Minute})
	mustCreateBucket(t, s, "tmp")

	first := putString(t, s, "tmp", "k", "old")
	clock.Advance(time.Minute)

	if err := mustPutConditional(t, s, "tmp", "k", "new", PutOptions{IfMatchAny: true}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("过期对象上 If-Match:* 应 412，实际 %v", err)
	}
	if err := mustPutConditional(t, s, "tmp", "k", "new", PutOptions{IfMatchETag: first.ETag}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("过期对象上旧 ETag 的 If-Match 应 412，实际 %v", err)
	}
}

// TestCopyIfMatchTarget 验证 COPY 目标上的 If-Match 与 PUT 同语义。
func TestCopyIfMatchTarget(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)
	if err := s.CreateBucket("dst"); err != nil {
		t.Fatalf("CreateBucket(dst) 失败: %v", err)
	}
	putString(t, s, "bkt", "src", "payload")
	dst := putString(t, s, "dst", "k", "old")

	if _, err := s.CopyObject("dst", "k", "bkt", "src", CopyOptions{IfMatchETag: dst.ETag}); err != nil {
		t.Fatalf("目标 ETag 匹配时 COPY 应成功，实际 %v", err)
	}
	if _, err := s.CopyObject("dst", "k", "bkt", "src", CopyOptions{IfMatchAny: true}); err != nil {
		t.Fatalf("目标存在时 COPY If-Match:* 应成功，实际 %v", err)
	}

	if _, err := s.CopyObject("dst", "k", "bkt", "src", CopyOptions{IfMatchETag: dst.ETag}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("目标 ETag 已变化后 COPY 应 412，实际 %v", err)
	}
	if _, err := s.CopyObject("dst", "missing", "bkt", "src", CopyOptions{IfMatchAny: true}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("目标不存在时 COPY If-Match:* 应 412，实际 %v", err)
	}
}

// TestPutIfNoneMatchETag 验证 `If-None-Match: "etag"`：
// 当前 ETag 等于给定值才 412；不等或对象不存在都放行。
func TestPutIfNoneMatchETag(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)
	first := putString(t, s, "bkt", "k", "v1")

	if err := mustPutConditional(t, s, "bkt", "k", "v2", PutOptions{
		IfNoneMatchETag: first.ETag,
	}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("ETag 相等时 If-None-Match 应 412，实际 %v", err)
	}
	meta, err := s.loadObjectMeta("bkt", "k")
	if err != nil {
		t.Fatalf("loadObjectMeta 失败: %v", err)
	}
	if got := string(readBlocks(t, s, meta)); got != "v1" {
		t.Errorf("失败的 If-None-Match 不应改动内容，实际 %q", got)
	}

	// 客户端手里的 ETag 不是当前版本 → 覆盖是安全的。
	if err := mustPutConditional(t, s, "bkt", "k", "v2", PutOptions{
		IfNoneMatchETag: "deadbeef",
	}); err != nil {
		t.Fatalf("ETag 不等时 If-None-Match 应成功，实际 %v", err)
	}

	// 对象不存在时不匹配任何 ETag，条件成立。
	if err := mustPutConditional(t, s, "bkt", "missing", "x", PutOptions{
		IfNoneMatchETag: first.ETag,
	}); err != nil {
		t.Fatalf("对象不存在时 If-None-Match 应成功，实际 %v", err)
	}
}

// TestPutIfNoneMatchETagIgnoresTombstoneAndExpiry 验证墓碑与过期对象
// 在 If-None-Match 的具体 ETag 形式上同样视为不存在。
func TestPutIfNoneMatchETagIgnoresTombstoneAndExpiry(t *testing.T) {
	t.Run("墓碑", func(t *testing.T) {
		s := newSmallBlockStore(t, 64, 16)
		first := putString(t, s, "bkt", "k", "v1")
		if err := s.DeleteObject("bkt", "k"); err != nil {
			t.Fatalf("DeleteObject 失败: %v", err)
		}
		if err := mustPutConditional(t, s, "bkt", "k", "v2", PutOptions{
			IfNoneMatchETag: first.ETag,
		}); err != nil {
			t.Fatalf("删除后旧 ETag 的 If-None-Match 应成立，实际 %v", err)
		}
	})

	t.Run("过期", func(t *testing.T) {
		clock := &testClock{now: time.Unix(1_700_000_000, 0).UTC()}
		s := newTTLTestStore(t, clock, map[string]time.Duration{"tmp": time.Minute})
		mustCreateBucket(t, s, "tmp")

		first := putString(t, s, "tmp", "k", "old")
		clock.Advance(time.Minute)

		if err := mustPutConditional(t, s, "tmp", "k", "new", PutOptions{
			IfNoneMatchETag: first.ETag,
		}); err != nil {
			t.Fatalf("过期对象上旧 ETag 的 If-None-Match 应成立，实际 %v", err)
		}
	})
}

// TestCopyIfNoneMatchETagTarget 验证 COPY 目标上的具体 ETag 条件。
func TestCopyIfNoneMatchETagTarget(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)
	if err := s.CreateBucket("dst"); err != nil {
		t.Fatalf("CreateBucket(dst) 失败: %v", err)
	}
	putString(t, s, "bkt", "src", "payload")
	dst := putString(t, s, "dst", "k", "old")

	if _, err := s.CopyObject("dst", "k", "bkt", "src", CopyOptions{
		IfNoneMatchETag: dst.ETag,
	}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("目标 ETag 相等时 COPY 应 412，实际 %v", err)
	}

	if _, err := s.CopyObject("dst", "k", "bkt", "src", CopyOptions{
		IfNoneMatchETag: "deadbeef",
	}); err != nil {
		t.Fatalf("目标 ETag 不等时 COPY 应成功，实际 %v", err)
	}

	if _, err := s.CopyObject("dst", "fresh", "bkt", "src", CopyOptions{
		IfNoneMatchETag: dst.ETag,
	}); err != nil {
		t.Fatalf("目标不存在时 COPY 应成功，实际 %v", err)
	}
}

// TestPutIfMatchConcurrent 验证条件判定与写入在同一把 key 锁内：
// N 个并发写者拿着同一个 ETag，只能有一个成功。
//
// 若判定被挪到锁外（例如在 handler 里先读一次再调用 PutObject），
// 多个写者会同时看到"匹配"并全部成功 —— 那就是丢失更新。
func TestPutIfMatchConcurrent(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)
	first := putString(t, s, "bkt", "k", "v1")

	const writers = 8
	var wg sync.WaitGroup
	var okCount, failCount atomic.Int32
	start := make(chan struct{})
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			body := []byte{byte('a' + i)}
			_, err := s.PutObject("bkt", "k", bytes.NewReader(body), PutOptions{
				ExpectedSize: 1,
				IfMatchETag:  first.ETag,
			})
			switch {
			case err == nil:
				okCount.Add(1)
			case errors.Is(err, ErrPreconditionFailed):
				failCount.Add(1)
			default:
				t.Errorf("写入 %d 出现意外错误: %v", i, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if okCount.Load() != 1 || failCount.Load() != writers-1 {
		t.Fatalf("成功 %d / 412 %d，期望 1 / %d", okCount.Load(), failCount.Load(), writers-1)
	}
}
