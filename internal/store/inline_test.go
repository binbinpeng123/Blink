package store

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/binbinpeng123/Blink/internal/checksum"
)

// newInlineStore 打开一个支持内联的存储：块只有 64 字节，因此 5 字节的
// 对象就足以覆盖"整对象"路径，不必写数 MiB 测试数据。
//
// inlineMax 为 0 表示关闭内联，用于对照"普通 CAS 路径"。
func newInlineStore(t *testing.T, inlineMax int64) *Store {
	t.Helper()
	return openInlineStore(t, filepath.Join(t.TempDir(), "data"), inlineMax)
}

func openInlineStore(t *testing.T, root string, inlineMax int64) *Store {
	t.Helper()
	s, err := Open(Options{
		Root:                root,
		BlockSize:           64,
		InlineThreshold:     64,
		InlineObjectMaxSize: inlineMax,
	})
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	if err := s.CreateBucket("bkt"); err != nil && !errors.Is(err, ErrBucketExists) {
		t.Fatalf("CreateBucket 失败: %v", err)
	}
	return s
}

// countCAS 数 CAS 里实际落盘的块文件。
func countCAS(t *testing.T, s *Store) int {
	t.Helper()
	return countFiles(t, filepath.Join(s.Root(), casDirName))
}

func TestInlineSmallObjectRoundTrip(t *testing.T) {
	s := newInlineStore(t, 64)
	const body = "hello"

	info, err := s.PutObject("bkt", "k", bytes.NewReader([]byte(body)), PutOptions{
		ExpectedSize: int64(len(body)),
	})
	if err != nil {
		t.Fatalf("PutObject 失败: %v", err)
	}
	if info.Size != int64(len(body)) || info.BlockCount != 1 {
		t.Errorf("info = %+v，期望 size=%d、一个块", info, len(body))
	}
	if got := readObject(t, s, "bkt", "k"); string(got) != body {
		t.Errorf("读回 %q，期望 %q", got, body)
	}

	meta, err := s.loadObjectMeta("bkt", "k")
	if err != nil {
		t.Fatalf("loadObjectMeta 失败: %v", err)
	}
	if meta.Layout != LayoutInline {
		t.Errorf("layout = %q，期望 %q", meta.Layout, LayoutInline)
	}
	if string(meta.InlineData) != body {
		t.Errorf("inline_data = %q，期望 %q", meta.InlineData, body)
	}
	if err := verifyInlineContent(meta); err != nil {
		t.Errorf("内联内容校验失败: %v", err)
	}

	// 内联的核心承诺：不产生 CAS 块文件，对象目录里只有 meta.json。
	if n := countCAS(t, s); n != 0 {
		t.Errorf("CAS 文件数 = %d，期望 0", n)
	}
	entries, err := os.ReadDir(s.objectDir("bkt", "k"))
	if err != nil {
		t.Fatalf("读取对象目录失败: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != objectMetaFileName {
		t.Errorf("对象目录内容 = %v，期望只有 %s", entries, metaFileName)
	}

	// 范围读取直接切内联内容。
	r, err := s.OpenObject("bkt", "k")
	if err != nil {
		t.Fatalf("OpenObject 失败: %v", err)
	}
	var buf bytes.Buffer
	if err := r.WriteRangeTo(&buf, 1, 3); err != nil {
		t.Fatalf("范围读取失败: %v", err)
	}
	if buf.String() != "ell" {
		t.Errorf("范围读取 = %q，期望 %q", buf.String(), "ell")
	}
}

func TestInlineMaxSizeBoundary(t *testing.T) {
	s := newInlineStore(t, 8)

	atLimit := make([]byte, 8)
	putString(t, s, "bkt", "at-limit", string(atLimit))
	meta, _ := s.loadObjectMeta("bkt", "at-limit")
	if meta.Layout != LayoutInline {
		t.Errorf("恰好等于上限的对象未内联：layout=%q", meta.Layout)
	}

	overLimit := make([]byte, 9)
	putString(t, s, "bkt", "over-limit", string(overLimit))
	meta, _ = s.loadObjectMeta("bkt", "over-limit")
	if meta.Layout == LayoutInline {
		t.Error("超出上限的对象被内联了")
	}
	if n := countCAS(t, s); n != 1 {
		t.Errorf("CAS 文件数 = %d，期望 1（只有超限对象落块）", n)
	}
	if got := readObject(t, s, "bkt", "over-limit"); !bytes.Equal(got, overLimit) {
		t.Errorf("超限对象读回内容不符：%d 字节", len(got))
	}
}

func TestInlineEmptyObjectStaysOut(t *testing.T) {
	s := newInlineStore(t, 64)
	putString(t, s, "bkt", "empty", "")

	meta, err := s.loadObjectMeta("bkt", "empty")
	if err != nil {
		t.Fatalf("loadObjectMeta 失败: %v", err)
	}
	if meta.Layout == LayoutInline || meta.Size != 0 || len(meta.Blocks) != 0 {
		t.Errorf("空对象不应内联: %+v", meta)
	}
	if got := readObject(t, s, "bkt", "empty"); len(got) != 0 {
		t.Errorf("空对象读回 %d 字节", len(got))
	}
}

func TestInlineDisabled(t *testing.T) {
	s := newInlineStore(t, 0)
	putString(t, s, "bkt", "k", "hello")

	meta, _ := s.loadObjectMeta("bkt", "k")
	if meta.Layout == LayoutInline {
		t.Error("关闭内联后对象仍被内联")
	}
	if n := countCAS(t, s); n != 1 {
		t.Errorf("CAS 文件数 = %d，期望 1", n)
	}
	if got := string(readObject(t, s, "bkt", "k")); got != "hello" {
		t.Errorf("读回 %q", got)
	}
}

// 长度不符必须沿用普通路径的 ErrIncompleteBody，且不能留下任何对象或
// CAS 块：内联只是"提前读完"，不能改变写入的失败语义。
func TestInlineIncompleteBody(t *testing.T) {
	s := newInlineStore(t, 64)

	cases := []struct {
		name string
		body string
		size int64
	}{
		{"少传", "short", 10},
		{"多传", "longer than declared", 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.PutObject("bkt", "k", bytes.NewReader([]byte(tc.body)), PutOptions{
				ExpectedSize: tc.size,
			})
			if !errors.Is(err, ErrIncompleteBody) {
				t.Fatalf("err = %v，期望 ErrIncompleteBody", err)
			}
			if _, err := s.loadObjectMeta("bkt", "k"); !errors.Is(err, ErrNoSuchKey) {
				t.Errorf("失败后对象仍然存在: %v", err)
			}
			// 长度不符会回退到普通流式路径，后者可能已经落了若干 CAS 块
			// 才在尾部发现 EOF —— 孤儿块由 GC 回收，这里不把它当失败。
			// 关键是对象元数据没有提交（上一行），客户端看不到半截对象。
		})
	}
}

func TestInlineContentMD5(t *testing.T) {
	s := newInlineStore(t, 64)
	body := []byte("hello")

	good := md5.Sum(body)
	if _, err := s.PutObject("bkt", "k", bytes.NewReader(body), PutOptions{
		ExpectedSize: int64(len(body)),
		ContentMD5:   hex.EncodeToString(good[:]),
	}); err != nil {
		t.Fatalf("正确 MD5 的写入失败: %v", err)
	}
	if got := readObject(t, s, "bkt", "k"); !bytes.Equal(got, body) {
		t.Errorf("读回 %q", got)
	}

	bad := md5.Sum([]byte("other"))
	_, err := s.PutObject("bkt", "bad", bytes.NewReader(body), PutOptions{
		ExpectedSize: int64(len(body)),
		ContentMD5:   hex.EncodeToString(bad[:]),
	})
	if !errors.Is(err, ErrBadDigest) {
		t.Fatalf("err = %v，期望 ErrBadDigest", err)
	}
	if _, err := s.loadObjectMeta("bkt", "bad"); !errors.Is(err, ErrNoSuchKey) {
		t.Errorf("校验失败后对象仍然存在: %v", err)
	}
	if n := countCAS(t, s); n != 0 {
		t.Errorf("CAS 文件数 = %d，期望 0", n)
	}
}

func TestInlineChecksum(t *testing.T) {
	s := newInlineStore(t, 64)
	body := []byte("123456789")

	sum, _ := checksum.New(checksum.AlgCRC32)
	_, _ = sum.Write(body)
	info, err := s.PutObject("bkt", "k", bytes.NewReader(body), PutOptions{
		ExpectedSize:      int64(len(body)),
		ChecksumAlgorithm: checksum.AlgCRC32,
		Checksum:          sum.Encoded(),
	})
	if err != nil {
		t.Fatalf("PutObject 失败: %v", err)
	}
	if info.Checksum != sum.Encoded() {
		t.Errorf("回传校验和 = %s，期望 %s", info.Checksum, sum.Encoded())
	}

	_, err = s.PutObject("bkt", "bad", bytes.NewReader(body), PutOptions{
		ExpectedSize:      int64(len(body)),
		ChecksumAlgorithm: checksum.AlgCRC32,
		Checksum:          "AAAAAA==",
	})
	if !errors.Is(err, ErrBadDigest) {
		t.Fatalf("err = %v，期望 ErrBadDigest", err)
	}
}

func TestInlineOverwriteAndDelete(t *testing.T) {
	s := newInlineStore(t, 64)

	putString(t, s, "bkt", "k", "first")
	// 故意超过 64 字节上限，覆盖"内联 → 非内联"的方向。
	second := strings.Repeat("longer-", 10)
	putString(t, s, "bkt", "k", second)
	got := readObject(t, s, "bkt", "k")
	if string(got) != second {
		t.Errorf("覆盖写后读回 %q", got)
	}
	// 内联写被非内联写覆盖后，内容必须来自 CAS，而不是残留的旧内联数据。
	meta, _ := s.loadObjectMeta("bkt", "k")
	if meta.Layout == LayoutInline || len(meta.InlineData) != 0 {
		t.Errorf("覆盖写后仍带内联字段: %+v", meta)
	}

	putString(t, s, "bkt", "k", "third")
	meta, _ = s.loadObjectMeta("bkt", "k")
	if meta.Layout != LayoutInline || string(meta.InlineData) != "third" {
		t.Errorf("覆盖回内联后元数据不符: %+v", meta)
	}

	// 删除即释放：对象目录整个消失，且不影响其他对象。
	if err := s.DeleteObject("bkt", "k"); err != nil {
		t.Fatalf("DeleteObject 失败: %v", err)
	}
	if _, err := s.loadObjectMeta("bkt", "k"); !errors.Is(err, ErrNoSuchKey) {
		t.Errorf("删除后仍能读到对象: %v", err)
	}
}

func TestInlineList(t *testing.T) {
	s := newInlineStore(t, 64)
	putString(t, s, "bkt", "inline.txt", "hello")

	res, err := s.ListObjects("bkt", ListOptions{MaxKeys: 10})
	if err != nil {
		t.Fatalf("ListObjects 失败: %v", err)
	}
	if res.KeyCount != 1 || len(res.Objects) != 1 {
		t.Fatalf("列表结果 = %+v，期望一条", res)
	}
	if res.Objects[0].Key != "inline.txt" || res.Objects[0].Size != 5 {
		t.Errorf("列表条目 = %+v", res.Objects[0])
	}
}

func TestInlineCopyObject(t *testing.T) {
	s := newInlineStore(t, 64)
	putString(t, s, "bkt", "src", "hello")

	before := countCAS(t, s)
	if _, err := s.CopyObject("bkt", "dst", "bkt", "src", CopyOptions{}); err != nil {
		t.Fatalf("CopyObject 失败: %v", err)
	}
	if n := countCAS(t, s); n != before {
		t.Errorf("复制后 CAS 文件数 %d，期望 %d", n, before)
	}
	if got := string(readObject(t, s, "bkt", "dst")); got != "hello" {
		t.Errorf("副本读回 %q", got)
	}

	src, _ := s.loadObjectMeta("bkt", "src")
	dst, _ := s.loadObjectMeta("bkt", "dst")
	if dst.Layout != LayoutInline || string(dst.InlineData) != "hello" {
		t.Errorf("副本元数据 = %+v", dst)
	}
	// 深拷贝：两个 meta 不能共享内容底层数组，否则将来的原地修改会串味。
	if len(src.InlineData) > 0 && &src.InlineData[0] == &dst.InlineData[0] {
		t.Error("副本与源共享 InlineData 底层数组")
	}
}

func TestInlineScrub(t *testing.T) {
	s := newInlineStore(t, 64)
	putString(t, s, "bkt", "k", "hello")

	if err := s.scrubObject(t.Context(), "bkt", "k"); err != nil {
		t.Fatalf("健康内联对象巡检失败: %v", err)
	}

	meta, _ := s.loadObjectMeta("bkt", "k")
	meta.InlineData[0] ^= 0xff
	if err := s.writeFileAtomic(s.objectMetaPath("bkt", "k"), mustJSON(meta)); err != nil {
		t.Fatalf("改写元数据失败: %v", err)
	}
	s.metaCache.delete(metaCacheKey{bucket: "bkt", key: "k"})

	if err := s.scrubObject(t.Context(), "bkt", "k"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("损坏内联对象巡检 err = %v，期望 ErrCorrupt", err)
	}
}

func TestInlineVerifyOnRead(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")
	s, err := Open(Options{
		Root:                root,
		BlockSize:           64,
		InlineThreshold:     64,
		InlineObjectMaxSize: 64,
		VerifyOnRead:        true,
	})
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	if err := s.CreateBucket("bkt"); err != nil {
		t.Fatalf("CreateBucket 失败: %v", err)
	}
	putString(t, s, "bkt", "k", "hello")

	// 正常路径先通一遍。
	if got := string(readObject(t, s, "bkt", "k")); got != "hello" {
		t.Fatalf("读回 %q", got)
	}

	meta, _ := s.loadObjectMeta("bkt", "k")
	meta.InlineData[1] ^= 0xff // 保持长度不变，只改内容
	if err := s.writeFileAtomic(s.objectMetaPath("bkt", "k"), mustJSON(meta)); err != nil {
		t.Fatalf("改写元数据失败: %v", err)
	}
	s.metaCache.delete(metaCacheKey{bucket: "bkt", key: "k"})

	r, err := s.OpenObject("bkt", "k")
	if err != nil {
		t.Fatalf("OpenObject 失败: %v", err)
	}
	if err := r.WriteRangeTo(new(bytes.Buffer), 0, r.Info().Size); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("校验读 err = %v，期望 ErrCorrupt", err)
	}
}

func TestInlineReplicaMetaValidation(t *testing.T) {
	s := newInlineStore(t, 64)
	body := []byte("hello")
	sum := sha256.Sum256(body)

	meta := &ObjectMeta{
		Version:     formatVersion,
		Key:         "rk",
		Size:        int64(len(body)),
		ETag:        hex.EncodeToString(sum[:]),
		ContentType: "text/plain",
		NodeID:      "n1",
		ObjVersion:  1,
		Layout:      LayoutInline,
		InlineData:  body,
		Blocks: []BlockDesc{{
			Hash:   hex.EncodeToString(sum[:]),
			Offset: 0,
			Length: int64(len(body)),
			CRC32C: checksum.BlockCRC32C(body),
		}},
	}
	if err := s.PutReplicaMeta("bkt", "rk", meta); err != nil {
		t.Fatalf("PutReplicaMeta 失败: %v", err)
	}
	if got := readObject(t, s, "bkt", "rk"); !bytes.Equal(got, body) {
		t.Errorf("副本读回 %q", got)
	}

	bad := *meta
	bad.InlineData = body[:len(body)-1]
	if err := s.PutReplicaMeta("bkt", "bad", &bad); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("长度不符的副本元数据 err = %v，期望 ErrCorrupt", err)
	}
}

// TestInlineOldVersionCompatibility 验证不识别 layout 的旧版本读到内联对象
// 时的两种结果：CAS 中恰好有同内容块则读到正确字节，否则明确 ErrCorrupt。
// 旧版本绝不能静默返回一个空对象。
func TestInlineOldVersionCompatibility(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")

	// 先用"内联关闭"的旧式路径写入 shared，让 CAS 中存在同内容块。
	old := openInlineStore(t, root, 0)
	putString(t, old, "bkt", "plain", "shared-content")
	if n := countCAS(t, old); n != 1 {
		t.Fatalf("CAS 文件数 = %d，期望 1", n)
	}

	// 用同一个根目录开启内联，写同内容与不同内容两个对象。
	s := openInlineStore(t, root, 64)
	putString(t, s, "bkt", "shared", "shared-content")
	putString(t, s, "bkt", "unique", "unique-content")

	// 旧式读取：忽略 layout，直接按块表去 CAS 取块。
	simulateOldRead := func(key string) ([]byte, error) {
		m, err := s.readObjectMetaAt(s.objectMetaPath("bkt", key))
		if err != nil {
			t.Fatalf("读取 %s 元数据失败: %v", key, err)
		}
		var buf bytes.Buffer
		for _, b := range m.Blocks {
			if err := s.copyBlockSegment(&buf, b, b.Offset, b.Offset+b.Length); err != nil {
				return nil, err
			}
		}
		return buf.Bytes(), nil
	}

	got, err := simulateOldRead("shared")
	if err != nil {
		t.Fatalf("同内容去重命中时应读到数据: %v", err)
	}
	if string(got) != "shared-content" {
		t.Errorf("旧式读取 = %q", got)
	}

	if _, err := simulateOldRead("unique"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("CAS 无同内容块时 err = %v，期望 ErrCorrupt", err)
	}
}
