package bench

import (
	"bytes"
	"io"
	"io/fs"
	"sync/atomic"
	"testing"

	"github.com/binbinpeng123/Blink/internal/store"
)

// countingFS 包在真实 FileSystem 外层，累计经写入路径落盘的字节数。
//
// 只覆写 OpenFile 与 WriteFile：store 的数据块、对象 meta、分片状态都经
// newAtomicWriter 走 OpenFile，读取走 Open，因此包装不影响读路径的零拷贝，
// 也不改变 fsync 行为（countingFile 转发 Sync，syncFile 的类型断言仍成立）。
// Truncate 不计：它只预分配空间，不产生数据写入。
type countingFS struct {
	store.FileSystem
	written atomic.Int64
}

func newCountingFS(inner store.FileSystem) *countingFS {
	return &countingFS{FileSystem: inner}
}

func (c *countingFS) OpenFile(path string, flag int, perm fs.FileMode) (io.ReadWriteCloser, error) {
	f, err := c.FileSystem.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	return &countingFile{ReadWriteCloser: f, written: &c.written}, nil
}

func (c *countingFS) WriteFile(path string, data []byte, perm fs.FileMode) error {
	c.written.Add(int64(len(data)))
	return c.FileSystem.WriteFile(path, data, perm)
}

type countingFile struct {
	io.ReadWriteCloser
	written *atomic.Int64
}

func (f *countingFile) Write(p []byte) (int, error) {
	n, err := f.ReadWriteCloser.Write(p)
	f.written.Add(int64(n))
	return n, err
}

// Sync 转发内核 fsync。store 用类型断言探测 Sync 方法，包装后必须继续
// 实现它，否则写放大基准会悄悄把持久化变成 no-op。
func (f *countingFile) Sync() error {
	if sy, ok := f.ReadWriteCloser.(interface{ Sync() error }); ok {
		return sy.Sync()
	}
	return nil
}

// openCountedBenchStore 打开一个写入量可观测的基准存储。
func openCountedBenchStore(b *testing.B) (*store.Store, *countingFS) {
	b.Helper()
	cf := newCountingFS(store.OSFS())
	s, _ := openBenchStoreRootOpts(b, store.Options{FS: cf})
	return s, cf
}

// TestDedupSecondCopyWritesOnlyMetadata 是计数包装的接线测试：
// 若包装失效（不再计数），两次写入的字节数都会是 0，本用例必须失败。
func TestDedupSecondCopyWritesOnlyMetadata(t *testing.T) {
	const size = 1 << 20

	cf := newCountingFS(store.OSFS())
	s, err := store.Open(store.Options{Root: t.TempDir(), FS: cf})
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	if err := s.CreateBucket("bench"); err != nil {
		t.Fatalf("创建桶失败: %v", err)
	}

	body := benchBody(size)
	if _, err := s.PutObject("bench", "first", bytes.NewReader(body), store.PutOptions{
		ExpectedSize: size,
	}); err != nil {
		t.Fatalf("写入第一份失败: %v", err)
	}
	if got := cf.written.Load(); got < size {
		t.Fatalf("第一份写入量 = %d，应至少包含 %d 字节数据", got, size)
	}

	cf.written.Store(0)
	if _, err := s.PutObject("bench", "second", bytes.NewReader(body), store.PutOptions{
		ExpectedSize: size,
	}); err != nil {
		t.Fatalf("写入第二份失败: %v", err)
	}
	// 只有对象元数据（数百字节），任一数据块重新落盘都会远超此阈值。
	if got := cf.written.Load(); got > 8<<10 {
		t.Fatalf("去重第二份写入量 = %d 字节，应仅写元数据", got)
	}
}
