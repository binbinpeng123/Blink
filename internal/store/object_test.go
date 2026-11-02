package store

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/binbinpeng123/Blink/internal/checksum"
)

// newSmallBlockStore 打开一个块很小的存储，让"多块"在几十字节内就能触发。
//
// 真实默认是 4 MiB 块，用默认值测多块要写 8 MiB 数据，单测跑不起；
// 但块切分逻辑对块大小不敏感，因此缩小块大小等价。
func newSmallBlockStore(t *testing.T, blockSize, inline int64) *Store {
	t.Helper()
	s, err := Open(Options{
		Root:            filepath.Join(t.TempDir(), "data"),
		BlockSize:       blockSize,
		InlineThreshold: inline,
	})
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	if err := s.CreateBucket("bkt"); err != nil {
		t.Fatalf("CreateBucket 失败: %v", err)
	}
	return s
}

// putString 是用字符串写入对象的快捷方式。
func putString(t *testing.T, s *Store, bucket, key, body string) *ObjectInfo {
	t.Helper()
	info, err := s.PutObject(bucket, key, bytes.NewReader([]byte(body)), PutOptions{
		ExpectedSize: int64(len(body)),
	})
	if err != nil {
		t.Fatalf("PutObject(%q) 失败: %v", key, err)
	}
	return info
}

// readBlocks 按块表从 CAS 拼回对象内容，顺带断言块描述自洽。
func readBlocks(t *testing.T, s *Store, meta *objectMeta) []byte {
	t.Helper()
	var out []byte
	var wantOffset int64
	for i, b := range meta.Blocks {
		if b.Offset != wantOffset {
			t.Fatalf("第 %d 块 offset = %d，期望 %d", i, b.Offset, wantOffset)
		}
		if b.Length <= 0 || b.Length > s.blockSize {
			t.Fatalf("第 %d 块 length = %d，超出 (0, %d]", i, b.Length, s.blockSize)
		}
		data, err := os.ReadFile(s.casPath(b.Hash))
		if err != nil {
			t.Fatalf("读取块 %s 失败: %v", b.Hash, err)
		}
		if int64(len(data)) != b.Length {
			t.Fatalf("块 %s 实际 %d 字节，块表声明 %d", b.Hash, len(data), b.Length)
		}
		// 内容哈希必须与块名一致，否则"内容寻址"这个前提就失效了。
		if got := hashKey(string(data)); got != b.Hash {
			t.Fatalf("块 %s 的内容哈希为 %s", b.Hash, got)
		}
		out = append(out, data...)
		wantOffset += b.Length
	}
	return out
}

func TestPutObjectSmallInline(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)
	const body = "hello"

	info := putString(t, s, "bkt", "greeting.txt", body)

	if info.Size != int64(len(body)) {
		t.Errorf("Size = %d，期望 %d", info.Size, len(body))
	}
	wantETag := fmt.Sprintf("%x", md5.Sum([]byte(body)))
	if info.ETag != wantETag {
		t.Errorf("ETag = %s，期望 %s", info.ETag, wantETag)
	}
	if info.BlockCount != 1 {
		t.Errorf("BlockCount = %d，期望 1", info.BlockCount)
	}
	if info.ContentType != defaultContentType {
		t.Errorf("ContentType = %q，期望 %q", info.ContentType, defaultContentType)
	}

	meta, err := s.loadObjectMeta("bkt", "greeting.txt")
	if err != nil {
		t.Fatalf("loadObjectMeta 失败: %v", err)
	}
	if meta.Key != "greeting.txt" || meta.Size != int64(len(body)) {
		t.Errorf("meta 内容不符: key=%q size=%d", meta.Key, meta.Size)
	}
	if meta.NodeID != s.nodeID || meta.NodeID == "" {
		t.Errorf("meta.NodeID = %q，期望 %q", meta.NodeID, s.nodeID)
	}
	if got := string(readBlocks(t, s, meta)); got != body {
		t.Errorf("回读内容 = %q，期望 %q", got, body)
	}

	// 03 文档的验收标准之一：meta.json 必须是人能读懂的 JSON。
	raw, err := os.ReadFile(s.objectMetaPath("bkt", "greeting.txt"))
	if err != nil {
		t.Fatalf("读取 meta.json 失败: %v", err)
	}
	if !bytes.Contains(raw, []byte(`"key":"greeting.txt"`)) {
		t.Errorf("meta.json 里找不到可读的 key: %s", raw)
	}
	assertNoTmpResidue(t, s.Root())
}

func TestPutObjectEmpty(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)
	info := putString(t, s, "bkt", "empty", "")

	if info.Size != 0 || info.BlockCount != 0 {
		t.Errorf("空对象 size=%d blocks=%d，期望 0/0", info.Size, info.BlockCount)
	}
	// 空内容的 MD5 是定值，ETag 必须与真实 S3 一致，否则客户端比对会失败。
	if info.ETag != "d41d8cd98f00b204e9800998ecf8427e" {
		t.Errorf("空对象 ETag = %s", info.ETag)
	}
	meta, err := s.loadObjectMeta("bkt", "empty")
	if err != nil {
		t.Fatalf("loadObjectMeta 失败: %v", err)
	}
	if len(meta.Blocks) != 0 {
		t.Errorf("空对象不应有块，实际 %d 块", len(meta.Blocks))
	}
}

func TestPutObjectMultiBlock(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	body := "0123456789abcdefghij" // 20 字节 → 8 + 8 + 4

	info := putString(t, s, "bkt", "multi", body)
	if got, want := info.BlockCount, 3; got != want {
		t.Fatalf("BlockCount = %d，期望 %d", got, want)
	}
	meta, err := s.loadObjectMeta("bkt", "multi")
	if err != nil {
		t.Fatalf("loadObjectMeta 失败: %v", err)
	}
	if got, want := len(meta.Blocks), 3; got != want {
		t.Fatalf("块数 = %d，期望 %d", got, want)
	}
	for i, want := range []int64{8, 8, 4} {
		if meta.Blocks[i].Length != want {
			t.Errorf("第 %d 块长度 = %d，期望 %d", i, meta.Blocks[i].Length, want)
		}
	}
	if got := string(readBlocks(t, s, meta)); got != body {
		t.Errorf("回读内容 = %q，期望 %q", got, body)
	}
}

// 恰好是块大小整数倍的对象最容易写错：最后读完时 ReadFull 返回 (0, EOF)，
// 若把"零字节 + EOF"当成"没有数据"就会丢掉最后一块。
func TestPutObjectExactBlockMultiple(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	body := "0123456789abcdef" // 16 字节 = 2 块

	info := putString(t, s, "bkt", "exact", body)
	if info.BlockCount != 2 {
		t.Fatalf("BlockCount = %d，期望 2", info.BlockCount)
	}
	meta, _ := s.loadObjectMeta("bkt", "exact")
	if got := string(readBlocks(t, s, meta)); got != body {
		t.Errorf("回读内容 = %q，期望 %q", got, body)
	}
}

// 长度恰好等于内联阈值时，peek 一次就读完了整个对象：
// 走的是"prefill 后立即 EOF"这条分支。
func TestPutObjectExactInlineThreshold(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	body := "abcd"

	info := putString(t, s, "bkt", "inline-exact", body)
	if info.BlockCount != 1 {
		t.Fatalf("BlockCount = %d，期望 1", info.BlockCount)
	}
	meta, _ := s.loadObjectMeta("bkt", "inline-exact")
	if got := string(readBlocks(t, s, meta)); got != body {
		t.Errorf("回读内容 = %q，期望 %q", got, body)
	}
}

// 关闭内联快路径后，同一个小对象也必须得到同样的结果。
func TestPutObjectWithInlineDisabled(t *testing.T) {
	s := newSmallBlockStore(t, 8, -1)
	body := "abcd"

	putString(t, s, "bkt", "no-inline", body)
	meta, _ := s.loadObjectMeta("bkt", "no-inline")
	if got := string(readBlocks(t, s, meta)); got != body {
		t.Errorf("回读内容 = %q，期望 %q", got, body)
	}
}

// 内容寻址的核心承诺：同一份内容写两次，第二次不占额外空间。
func TestPutObjectDeduplicatesIdenticalContent(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	body := "0123456789abcdefghij"

	first := putString(t, s, "bkt", "a", body)
	second := putString(t, s, "bkt", "b", body)

	if first.DedupedBlocks != 0 {
		t.Errorf("首次写入 DedupedBlocks = %d，期望 0", first.DedupedBlocks)
	}
	if second.DedupedBlocks != second.BlockCount {
		t.Errorf("第二次写入 DedupedBlocks = %d，期望等于块数 %d",
			second.DedupedBlocks, second.BlockCount)
	}

	// 两个 key 必须指向同一组块文件。
	m1, _ := s.loadObjectMeta("bkt", "a")
	m2, _ := s.loadObjectMeta("bkt", "b")
	for i := range m1.Blocks {
		if m1.Blocks[i].Hash != m2.Blocks[i].Hash {
			t.Errorf("第 %d 块哈希不同: %s vs %s", i, m1.Blocks[i].Hash, m2.Blocks[i].Hash)
		}
	}
}

func TestPutObjectOverwriteBumpsObjVersion(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)

	first := putString(t, s, "bkt", "k", "v1")
	second := putString(t, s, "bkt", "k", "v2")

	if first.Version != 1 || second.Version != 2 {
		t.Fatalf("obj_version = %d, %d，期望 1, 2", first.Version, second.Version)
	}
	meta, _ := s.loadObjectMeta("bkt", "k")
	if meta.Size != 2 {
		t.Errorf("覆盖后 size = %d，期望 2", meta.Size)
	}
	if got := string(readBlocks(t, s, meta)); got != "v2" {
		t.Errorf("覆盖后内容 = %q", got)
	}
}

func TestPutObjectIfNoneMatch(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)
	putString(t, s, "bkt", "k", "original")

	_, err := s.PutObject("bkt", "k", bytes.NewReader([]byte("new")), PutOptions{
		ExpectedSize:   3,
		IfNoneMatchAny: true,
	})
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("err = %v，期望 ErrPreconditionFailed", err)
	}
	// 失败的条件写绝不能改动已有内容。
	meta, _ := s.loadObjectMeta("bkt", "k")
	if got := string(readBlocks(t, s, meta)); got != "original" {
		t.Errorf("对象被条件写改动了: %q", got)
	}
}

func TestPutObjectIfNoneMatchOnMissingKeySucceeds(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)
	if _, err := s.PutObject("bkt", "fresh", bytes.NewReader([]byte("x")), PutOptions{
		ExpectedSize:   1,
		IfNoneMatchAny: true,
	}); err != nil {
		t.Fatalf("对象不存在时 If-None-Match 应成功，实际 %v", err)
	}
}

func TestPutObjectContentMD5(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)
	body := []byte("payload")
	good := hex.EncodeToString(func() []byte { h := md5.Sum(body); return h[:] }())

	info, err := s.PutObject("bkt", "k", bytes.NewReader(body), PutOptions{
		ExpectedSize: int64(len(body)),
		ContentMD5:   good,
	})
	if err != nil {
		t.Fatalf("Content-MD5 正确时不应失败: %v", err)
	}

	_, err = s.PutObject("bkt", "k2", bytes.NewReader(body), PutOptions{
		ExpectedSize: int64(len(body)),
		ContentMD5:   "00000000000000000000000000000000",
	})
	if !errors.Is(err, ErrBadDigest) {
		t.Fatalf("err = %v，期望 ErrBadDigest", err)
	}
	// 校验失败的对象绝不能变得可见。
	if _, err := s.loadObjectMeta("bkt", "k2"); !errors.Is(err, ErrNoSuchKey) {
		t.Errorf("校验失败后对象仍然存在: %v", err)
	}
	_ = info
}

func TestPutObjectChecksumRecorded(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)
	body := []byte("123456789")

	sum, _ := checksum.New(checksum.AlgCRC64NVME)
	_, _ = sum.Write(body)

	info, err := s.PutObject("bkt", "k", bytes.NewReader(body), PutOptions{
		ExpectedSize:      int64(len(body)),
		ChecksumAlgorithm: checksum.AlgCRC64NVME,
		Checksum:          sum.Encoded(),
	})
	if err != nil {
		t.Fatalf("PutObject 失败: %v", err)
	}
	if info.Checksum != sum.Encoded() {
		t.Errorf("回传的校验和 = %s，期望 %s", info.Checksum, sum.Encoded())
	}

	meta, _ := s.loadObjectMeta("bkt", "k")
	if meta.ChecksumAlgorithm != string(checksum.AlgCRC64NVME) || meta.Checksum != sum.Encoded() {
		t.Errorf("meta 未记录校验和: %+v", meta)
	}

	_, err = s.PutObject("bkt", "k2", bytes.NewReader(body), PutOptions{
		ExpectedSize:      int64(len(body)),
		ChecksumAlgorithm: checksum.AlgCRC64NVME,
		Checksum:          "AAAAAAAAAAA=",
	})
	if !errors.Is(err, ErrBadDigest) {
		t.Fatalf("err = %v，期望 ErrBadDigest", err)
	}
}

// 未给值时要求调用方算出并返回，同时回调必须发生在请求体读完之后。
func TestPutObjectLateChecksum(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)
	body := []byte("123456789")

	called := false
	info, err := s.PutObject("bkt", "k", bytes.NewReader(body), PutOptions{
		ExpectedSize:      int64(len(body)),
		ChecksumAlgorithm: checksum.AlgCRC32,
		LateChecksum: func() (string, bool) {
			called = true
			return "", false // 客户端没通过 trailer 声明，只要求服务端算
		},
	})
	if err != nil {
		t.Fatalf("PutObject 失败: %v", err)
	}
	if !called {
		t.Error("LateChecksum 未被调用")
	}
	if info.Checksum == "" {
		t.Error("即使客户端未声明也必须算出校验和")
	}

	_, err = s.PutObject("bkt", "k2", bytes.NewReader(body), PutOptions{
		ExpectedSize:      int64(len(body)),
		ChecksumAlgorithm: checksum.AlgCRC32,
		LateChecksum:      func() (string, bool) { return "AAAAAA==", true },
	})
	if !errors.Is(err, ErrBadDigest) {
		t.Fatalf("err = %v，期望 ErrBadDigest", err)
	}
}

func TestPutObjectUnknownChecksumAlgorithm(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)
	_, err := s.PutObject("bkt", "k", bytes.NewReader([]byte("x")), PutOptions{
		ExpectedSize:      1,
		ChecksumAlgorithm: checksum.Algorithm("MD5"),
	})
	if !errors.Is(err, ErrInvalidChecksum) {
		t.Fatalf("err = %v，期望 ErrInvalidChecksum", err)
	}
}

func TestPutObjectIncompleteBody(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)
	_, err := s.PutObject("bkt", "k", bytes.NewReader([]byte("short")), PutOptions{
		ExpectedSize: 100,
	})
	if !errors.Is(err, ErrIncompleteBody) {
		t.Fatalf("err = %v，期望 ErrIncompleteBody", err)
	}
	if _, err := s.loadObjectMeta("bkt", "k"); !errors.Is(err, ErrNoSuchKey) {
		t.Errorf("长度不符的对象不该可见: %v", err)
	}
}

func TestPutObjectRejectsNegativeLength(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)
	_, err := s.PutObject("bkt", "k", bytes.NewReader(nil), PutOptions{ExpectedSize: -1})
	if !errors.Is(err, ErrIncompleteBody) {
		t.Fatalf("err = %v，期望 ErrIncompleteBody", err)
	}
}

func TestPutObjectValidation(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)

	if _, err := s.PutObject("missing", "k", bytes.NewReader(nil),
		PutOptions{ExpectedSize: 0}); !errors.Is(err, ErrBucketNotFound) {
		t.Errorf("桶不存在时 err = %v，期望 ErrBucketNotFound", err)
	}
	if _, err := s.PutObject("bkt", "", bytes.NewReader(nil),
		PutOptions{ExpectedSize: 0}); !errors.Is(err, ErrInvalidKey) {
		t.Errorf("空 key 时 err = %v，期望 ErrInvalidKey", err)
	}
}

func TestPutObjectUserMetaAndContentType(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)
	info, err := s.PutObject("bkt", "k", bytes.NewReader([]byte("x")), PutOptions{
		ExpectedSize: 1,
		ContentType:  "text/plain; charset=utf-8",
		UserMeta:     map[string]string{"Owner": "edge-01"},
	})
	if err != nil {
		t.Fatalf("PutObject 失败: %v", err)
	}
	meta, _ := s.loadObjectMeta("bkt", "k")
	if meta.ContentType != info.ContentType {
		t.Errorf("meta.ContentType = %q", meta.ContentType)
	}
	if meta.UserMeta["Owner"] != "edge-01" {
		t.Errorf("用户元数据丢失: %+v", meta.UserMeta)
	}
}

// 不同 key 的并发写必须互不干扰，且共享块不会因并发而写坏。
func TestPutObjectConcurrentDistinctKeys(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	const n = 16

	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 相同内容 + 相同前缀：故意制造 CAS 上的并发写竞争。
			body := fmt.Sprintf("common-prefix-%02d", i)
			_, err := s.PutObject("bkt", fmt.Sprintf("k%02d", i),
				bytes.NewReader([]byte(body)), PutOptions{ExpectedSize: int64(len(body))})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("并发写入失败: %v", err)
		}
	}

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%02d", i)
		meta, err := s.loadObjectMeta("bkt", key)
		if err != nil {
			t.Fatalf("读取 %s 失败: %v", key, err)
		}
		want := fmt.Sprintf("common-prefix-%02d", i)
		if got := string(readBlocks(t, s, meta)); got != want {
			t.Errorf("%s 内容 = %q，期望 %q", key, got, want)
		}
	}
	assertNoTmpResidue(t, s.Root())
}
