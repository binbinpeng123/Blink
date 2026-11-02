package store

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// readRange 读取对象的一段并返回字符串。
func readRange(t *testing.T, r *ObjectReader, offset, length int64) string {
	t.Helper()
	var buf bytes.Buffer
	if err := r.WriteRangeTo(&buf, offset, length); err != nil {
		t.Fatalf("WriteRangeTo(%d, %d) 失败: %v", offset, length, err)
	}
	return buf.String()
}

func TestOpenObjectInfo(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	info := putString(t, s, "bkt", "greeting.txt", "hello world")

	r, err := s.OpenObject("bkt", "greeting.txt")
	if err != nil {
		t.Fatalf("OpenObject 失败: %v", err)
	}
	got := r.Info()
	if got.Key != "greeting.txt" || got.Size != 11 || got.ETag != info.ETag {
		t.Errorf("Info() = %+v，与写入结果 %+v 不符", got, info)
	}
	if got.ContentType != defaultContentType {
		t.Errorf("ContentType = %q，期望默认值 %q", got.ContentType, defaultContentType)
	}
	if got.BlockCount != 2 {
		t.Errorf("BlockCount = %d，期望 2", got.BlockCount)
	}
	if got.Version != 1 {
		t.Errorf("Version = %d，期望 1", got.Version)
	}
}

func TestOpenObjectErrors(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	putString(t, s, "bkt", "exists", "x")

	if _, err := s.OpenObject("bkt", "missing"); !errors.Is(err, ErrNoSuchKey) {
		t.Errorf("不存在的 key 错误 = %v，期望 ErrNoSuchKey", err)
	}
	if _, err := s.OpenObject("nope", "exists"); !errors.Is(err, ErrBucketNotFound) {
		// 桶不存在时绝不能报 NoSuchKey：两者的排障方向完全不同。
		t.Errorf("不存在的桶错误 = %v，期望 ErrBucketNotFound", err)
	}
	if _, err := s.OpenObject("bkt", ""); !errors.Is(err, ErrInvalidKey) {
		t.Errorf("空 key 错误 = %v，期望 ErrInvalidKey", err)
	}
}

// TestWriteRangeTo 覆盖整块、跨块、块内偏移、末块与零长度五类区间。
func TestWriteRangeTo(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	const body = "0123456789abcdefghij" // 20 字节 → 块 8/8/4
	putString(t, s, "bkt", "range", body)

	r, err := s.OpenObject("bkt", "range")
	if err != nil {
		t.Fatalf("OpenObject 失败: %v", err)
	}

	tests := []struct {
		name   string
		offset int64
		length int64
		want   string
	}{
		{"整个对象", 0, 20, body},
		{"首块整块", 0, 8, "01234567"},
		{"首块内偏移", 1, 6, "123456"},
		{"跨块", 7, 10, "789abcdefg"},
		{"中间整块", 8, 8, "89abcdef"},
		{"末块整块", 16, 4, "ghij"},
		{"末块内偏移", 17, 3, "hij"},
		{"最后一字节", 19, 1, "j"},
		{"零长度", 5, 0, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := readRange(t, r, tt.offset, tt.length); got != tt.want {
				t.Errorf("读 [%d,%d) = %q，期望 %q", tt.offset, tt.offset+tt.length, got, tt.want)
			}
		})
	}
}

// 越界范围是调用方的编程错误：必须报 ErrCorrupt 而不是悄悄截断，
// 否则会发出与 Content-Length 不符的响应。
func TestWriteRangeToRejectsOutOfBounds(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	putString(t, s, "bkt", "oob", "0123456789abcdefghij")
	r, _ := s.OpenObject("bkt", "oob")

	for _, tt := range []struct{ off, n int64 }{
		{-1, 1}, {0, 21}, {20, 1}, {21, 0}, {10, -1},
	} {
		if err := r.WriteRangeTo(&bytes.Buffer{}, tt.off, tt.n); !errors.Is(err, ErrCorrupt) {
			t.Errorf("WriteRangeTo(%d,%d) 错误 = %v，期望 ErrCorrupt", tt.off, tt.n, err)
		}
	}
}

// 块文件缺失或长度不符时必须报 ErrCorrupt：静默少读会让客户端收到
// 一个"看起来成功"的短响应，是最难排查的损坏形式。
func TestWriteRangeToDetectsCorruptBlocks(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	putString(t, s, "bkt", "corrupt", "0123456789abcdefghij")

	meta, err := s.loadObjectMeta("bkt", "corrupt")
	if err != nil {
		t.Fatalf("读取元数据失败: %v", err)
	}
	last := meta.Blocks[2]

	t.Run("缺失", func(t *testing.T) {
		path := s.casPath(last.Hash)
		if err := os.Remove(path); err != nil {
			t.Fatalf("删除块失败: %v", err)
		}
		r, _ := s.OpenObject("bkt", "corrupt")

		// 不涉及损坏块的区间仍应正常读出。
		if got := readRange(t, r, 0, 8); got != "01234567" {
			t.Errorf("健康区间 = %q", got)
		}
		if err := r.WriteRangeTo(&bytes.Buffer{}, 16, 4); !errors.Is(err, ErrCorrupt) {
			t.Errorf("读取缺失块错误 = %v，期望 ErrCorrupt", err)
		}
	})

	t.Run("长度不符", func(t *testing.T) {
		// 用正常内容重建块文件，再截断它。
		putString(t, s, "bkt", "corrupt2", "ij")
		meta2, _ := s.loadObjectMeta("bkt", "corrupt2")
		path := s.casPath(meta2.Blocks[0].Hash)
		if err := os.Truncate(path, 1); err != nil {
			t.Fatalf("截断块失败: %v", err)
		}
		r, _ := s.OpenObject("bkt", "corrupt2")
		if err := r.WriteRangeTo(&bytes.Buffer{}, 0, 2); !errors.Is(err, ErrCorrupt) {
			t.Errorf("截断块错误 = %v，期望 ErrCorrupt", err)
		}
	})
}

// 读取器是元数据快照：打开之后的覆盖写不应改变它看到的内容。
// 这是"并发读者要么看到完整旧版本、要么看到完整新版本"的实现依据。
func TestReaderSeesSnapshotAcrossOverwrite(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	putString(t, s, "bkt", "snap", "old-old-old") // 11 字节

	r, err := s.OpenObject("bkt", "snap")
	if err != nil {
		t.Fatalf("OpenObject 失败: %v", err)
	}

	putString(t, s, "bkt", "snap", "new-new-new") // 同样 11 字节，块全不同

	if got := readRange(t, r, 0, 11); got != "old-old-old" {
		t.Errorf("覆盖写后旧读取器读到 %q，期望旧内容", got)
	}
	// 新打开则应看到新内容。
	r2, _ := s.OpenObject("bkt", "snap")
	if got := readRange(t, r2, 0, 11); got != "new-new-new" {
		t.Errorf("新读取器读到 %q，期望新内容", got)
	}
}

func TestDeleteObject(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	putString(t, s, "bkt", "gone", "0123456789")

	if err := s.DeleteObject("bkt", "gone"); err != nil {
		t.Fatalf("DeleteObject 失败: %v", err)
	}
	if _, err := s.OpenObject("bkt", "gone"); !errors.Is(err, ErrNoSuchKey) {
		t.Errorf("删除后 OpenObject 错误 = %v，期望 ErrNoSuchKey", err)
	}
	// 幂等：再删一次仍是成功。
	if err := s.DeleteObject("bkt", "gone"); err != nil {
		t.Errorf("重复删除错误 = %v，期望 nil", err)
	}
}

// 删除对象后 CAS 块必须原样保留：它们可能被其他 key 去重引用，
// 且回收只属于 GC 的职责。
func TestDeleteObjectKeepsBlocksForGC(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	body := "0123456789abcdefghij"
	putString(t, s, "bkt", "a", body)
	// 第二个 key 与第一个共享全部块（去重）。
	putString(t, s, "bkt", "b", body)

	before := countFiles(t, filepath.Join(s.root, casDirName))
	if err := s.DeleteObject("bkt", "a"); err != nil {
		t.Fatalf("DeleteObject 失败: %v", err)
	}
	after := countFiles(t, filepath.Join(s.root, casDirName))
	if after != before {
		t.Errorf("删除对象后 CAS 文件数 %d → %d，不应该变化", before, after)
	}

	// 另一个 key 仍可完整读出。
	r, err := s.OpenObject("bkt", "b")
	if err != nil {
		t.Fatalf("另一个 key 打开失败: %v", err)
	}
	if got := readRange(t, r, 0, int64(len(body))); got != body {
		t.Errorf("去重 key 内容 = %q，期望 %q", got, body)
	}
}

func TestDeleteObjectErrors(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	if err := s.DeleteObject("nope", "k"); !errors.Is(err, ErrBucketNotFound) {
		t.Errorf("不存在的桶错误 = %v，期望 ErrBucketNotFound", err)
	}
	if err := s.DeleteObject("bkt", ""); !errors.Is(err, ErrInvalidKey) {
		t.Errorf("空 key 错误 = %v，期望 ErrInvalidKey", err)
	}
}

// countFiles 统计目录下的普通文件数。
func countFiles(t *testing.T, root string) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("遍历 %s 失败: %v", root, err)
	}
	return n
}
