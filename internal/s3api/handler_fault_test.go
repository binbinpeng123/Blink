package s3api

import (
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/binbinpeng123/Blink/internal/store"
)

// enospcFS 包装真实文件系统，在 armed 之后让所有写入返回 ENOSPC。
//
// 存储层的磁盘满行为已由 store.TestDiskFullKeepsReadsAndDeletesWorking
// 覆盖；这里验证的是**协议层**：ENOSPC 必须翻译成 507 InsufficientStorage，
// 而不是泛化成 500 InternalError，也不能误用表示限流的 503 SlowDown。
//
// 只包装 OpenFile：读取路径没有副作用，满盘时能否读取与包装无关，
// 保留原样还能减少一层类型断言的干扰。
type enospcFS struct {
	inner store.FileSystem
	armed atomic.Bool
}

func (f *enospcFS) MkdirAll(path string, perm fs.FileMode) error {
	return f.inner.MkdirAll(path, perm)
}

func (f *enospcFS) Stat(path string) (fs.FileInfo, error) { return f.inner.Stat(path) }

func (f *enospcFS) Open(path string) (io.ReadWriteCloser, error) { return f.inner.Open(path) }

func (f *enospcFS) OpenFile(path string, flag int, perm fs.FileMode) (io.ReadWriteCloser, error) {
	rc, err := f.inner.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	return &enospcFile{ReadWriteCloser: rc, fsys: f}, nil
}

func (f *enospcFS) ReadDir(path string) ([]fs.DirEntry, error) { return f.inner.ReadDir(path) }
func (f *enospcFS) Remove(path string) error                   { return f.inner.Remove(path) }
func (f *enospcFS) RemoveAll(path string) error                { return f.inner.RemoveAll(path) }
func (f *enospcFS) Rename(oldpath, newpath string) error {
	return f.inner.Rename(oldpath, newpath)
}
func (f *enospcFS) SyncDir(path string) error { return f.inner.SyncDir(path) }
func (f *enospcFS) WriteFile(path string, data []byte, perm fs.FileMode) error {
	return f.inner.WriteFile(path, data, perm)
}
func (f *enospcFS) Truncate(path string, size int64) error {
	return f.inner.Truncate(path, size)
}

// enospcFile 在文件句柄上拦截写。
//
// 必须显式转发 WriteAt/Truncate/Seek：写入路径会用类型断言判断底层是否
// 支持随机写与预分配，只嵌一个 ReadWriteCloser 会让断言失败，
// 测试就会因为"不支持随机写"这种无关错误而变红。
type enospcFile struct {
	io.ReadWriteCloser
	fsys *enospcFS
}

func (f *enospcFile) Write(p []byte) (int, error) {
	if f.fsys.armed.Load() {
		return 0, syscall.ENOSPC
	}
	return f.ReadWriteCloser.Write(p)
}

func (f *enospcFile) WriteAt(p []byte, off int64) (int, error) {
	wa, ok := f.ReadWriteCloser.(io.WriterAt)
	if !ok {
		return 0, syscall.EINVAL
	}
	if f.fsys.armed.Load() {
		return 0, syscall.ENOSPC
	}
	return wa.WriteAt(p, off)
}

func (f *enospcFile) Truncate(size int64) error {
	type truncater interface{ Truncate(int64) error }
	tr, ok := f.ReadWriteCloser.(truncater)
	if !ok {
		return syscall.EINVAL
	}
	if f.fsys.armed.Load() {
		return syscall.ENOSPC
	}
	return tr.Truncate(size)
}

func (f *enospcFile) Seek(offset int64, whence int) (int64, error) {
	seeker, ok := f.ReadWriteCloser.(io.Seeker)
	if !ok {
		return 0, syscall.EINVAL
	}
	return seeker.Seek(offset, whence)
}

// TestDiskFullReturns507InsufficientStorage 验证磁盘满在 HTTP 层的完整契约：
// 写入 507 InsufficientStorage、已有对象照常可读可删、腾出空间后自动恢复。
func TestDiskFullReturns507InsufficientStorage(t *testing.T) {
	fsys := &enospcFS{inner: store.OSFS()}
	st, err := store.Open(store.Options{
		Root:            filepath.Join(t.TempDir(), "data"),
		FS:              fsys,
		BlockSize:       8,
		InlineThreshold: 4,
	})
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	// 建桶发生在 arm 之前：桶已经存在，满盘时请求不会卡在"没有地方写桶元数据"。
	if err := st.CreateBucket("bkt"); err != nil {
		t.Fatalf("建桶失败: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := New(Options{
		Logger:      logger,
		Region:      "us-east-1",
		DisableAuth: true,
		Store:       st,
	}).Handler()

	const keepBody = "keep me around"
	if rec := putObject(t, h, "/bkt/keep", keepBody, nil); rec.Code != http.StatusOK {
		t.Fatalf("准备对象失败: %d %s", rec.Code, rec.Body.String())
	}

	fsys.armed.Store(true)
	const newBody = "0123456789abcdefghijklmnopqrstuvwxyz"
	rec := putObject(t, h, "/bkt/full", newBody, nil)
	if rec.Code != http.StatusInsufficientStorage {
		t.Fatalf("满盘写入状态码 = %d，期望 507 (%s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "InsufficientStorage") {
		t.Errorf("满盘响应体未包含 InsufficientStorage: %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "SlowDown") {
		t.Errorf("满盘响应体不应包含表示限流的 SlowDown: %s", rec.Body.String())
	}

	// 满盘时读取必须照常工作 —— 否则运维连"先备份再清理"都做不到。
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/bkt/keep", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("满盘时 GET 状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != keepBody {
		t.Errorf("满盘时 GET 内容 = %q，期望 %q", got, keepBody)
	}

	// 写入失败的对象不得可见。
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/bkt/full", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("写入失败的对象应返回 404，实际 %d", rec.Code)
	}

	// 删除同样必须可用：这是腾空间的主要途径。
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/bkt/keep", nil))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("满盘时 DELETE 状态码 = %d，期望 204 (%s)", rec.Code, rec.Body.String())
	}

	// 空间腾出（此处等价于故障消失）后写入自动恢复。
	fsys.armed.Store(false)
	if rec := putObject(t, h, "/bkt/full", newBody, nil); rec.Code != http.StatusOK {
		t.Fatalf("恢复后写入状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/bkt/full", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != newBody {
		t.Fatalf("恢复后 GET = %d %q，期望 200 %q", rec.Code, rec.Body.String(), newBody)
	}
}
