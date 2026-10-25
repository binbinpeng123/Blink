package store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
)

// 故障注入的切入点。命名与 FileSystem 的方法对应，
// 但 "sync" 发生在包装后的文件句柄上，而不是 FileSystem 方法上。
const (
	opOpenFile = "openfile"
	opWrite    = "write"
	opSync     = "sync"
	opRename   = "rename"
	opSyncDir  = "syncdir"
	opTruncate = "truncate"
)

// errInjected 是注入的通用故障。
var errInjected = errors.New("faultFS: 注入的故障")

// faultFS 包装真实文件系统，在指定调用点注入错误。
//
// 它是 05 文档"覆盖 fsync 后、rename 前这类关键窗口"的唯一实现手段：
// 真杀进程只能随机命中窗口，而测试需要**每个**窗口都被命中一次。
//
// 两种注入模式：
//   - failNth：第 n 次匹配操作失败一次，之后自动恢复（用于"每个步骤都可能失败"的矩阵）；
//   - fail：每次匹配操作都失败，直到 disarm（用于模拟磁盘满）。
type faultFS struct {
	inner FileSystem

	mu    sync.Mutex
	op    string
	nth   int // 0 表示持续失败
	seen  int
	fired bool
	err   error
}

func newFaultFS(inner FileSystem) *faultFS {
	return &faultFS{inner: inner}
}

// failNth 让第 n 次 op 操作失败一次。
func (f *faultFS) failNth(op string, n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.op, f.nth, f.seen, f.fired, f.err = op, n, 0, false, err
}

// fail 让 op 操作从下一次开始持续失败，直到 disarm。
func (f *faultFS) fail(op string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.op, f.nth, f.seen, f.fired, f.err = op, 0, 0, false, err
}

// disarm 撤销注入。必须在重试之前调用：测试要验证的是
// "故障消失后系统能自行恢复"，而不是"故障永远存在"。
func (f *faultFS) disarm() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.op, f.nth, f.seen, f.fired, f.err = "", 0, 0, false, nil
}

// step 记录一次 op 并决定是否失败。
func (f *faultFS) step(op string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.op == "" || op != f.op {
		return nil
	}
	f.seen++
	if f.nth == 0 {
		return f.err
	}
	if !f.fired && f.seen == f.nth {
		f.fired = true
		return f.err
	}
	return nil
}

func (f *faultFS) MkdirAll(path string, perm fs.FileMode) error {
	return f.inner.MkdirAll(path, perm)
}

func (f *faultFS) Stat(path string) (fs.FileInfo, error) { return f.inner.Stat(path) }

func (f *faultFS) Open(path string) (io.ReadWriteCloser, error) {
	rc, err := f.inner.Open(path)
	if err != nil {
		return nil, err
	}
	return &faultFile{ReadWriteCloser: rc, fsys: f}, nil
}

func (f *faultFS) OpenFile(path string, flag int, perm fs.FileMode) (io.ReadWriteCloser, error) {
	if err := f.step(opOpenFile); err != nil {
		return nil, err
	}
	rc, err := f.inner.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	return &faultFile{ReadWriteCloser: rc, fsys: f}, nil
}

func (f *faultFS) ReadDir(path string) ([]fs.DirEntry, error) { return f.inner.ReadDir(path) }
func (f *faultFS) Remove(path string) error                   { return f.inner.Remove(path) }
func (f *faultFS) RemoveAll(path string) error                { return f.inner.RemoveAll(path) }

func (f *faultFS) Rename(oldpath, newpath string) error {
	if err := f.step(opRename); err != nil {
		return err
	}
	return f.inner.Rename(oldpath, newpath)
}

func (f *faultFS) SyncDir(path string) error {
	if err := f.step(opSyncDir); err != nil {
		return err
	}
	return f.inner.SyncDir(path)
}

func (f *faultFS) WriteFile(path string, data []byte, perm fs.FileMode) error {
	return f.inner.WriteFile(path, data, perm)
}

func (f *faultFS) Truncate(path string, size int64) error {
	if err := f.step(opTruncate); err != nil {
		return err
	}
	return f.inner.Truncate(path, size)
}

// faultFile 在文件句柄上拦截写与 fsync。
//
// 必须显式转发 WriteAt/Truncate/Seek：atomicWriter 用类型断言判断底层文件
// 是否支持随机写与预分配，嵌入一个只有 Read/Write/Close 的接口会让断言失败，
// 错误地报告"不支持随机写"。这正是包装层的经典坑。
type faultFile struct {
	io.ReadWriteCloser
	fsys *faultFS
}

func (f *faultFile) Write(p []byte) (int, error) {
	if err := f.fsys.step(opWrite); err != nil {
		return 0, err
	}
	return f.ReadWriteCloser.Write(p)
}

func (f *faultFile) Sync() error {
	if err := f.fsys.step(opSync); err != nil {
		return err
	}
	type syncer interface{ Sync() error }
	if sy, ok := f.ReadWriteCloser.(syncer); ok {
		return sy.Sync()
	}
	return nil
}

func (f *faultFile) WriteAt(p []byte, off int64) (int, error) {
	wa, ok := f.ReadWriteCloser.(io.WriterAt)
	if !ok {
		return 0, errors.New("faultFS: 底层文件不支持随机写")
	}
	if err := f.fsys.step(opWrite); err != nil {
		return 0, err
	}
	return wa.WriteAt(p, off)
}

func (f *faultFile) Truncate(size int64) error {
	type truncater interface{ Truncate(int64) error }
	tr, ok := f.ReadWriteCloser.(truncater)
	if !ok {
		return errors.New("faultFS: 底层文件不支持截断")
	}
	if err := f.fsys.step(opTruncate); err != nil {
		return err
	}
	return tr.Truncate(size)
}

func (f *faultFile) Seek(offset int64, whence int) (int64, error) {
	seeker, ok := f.ReadWriteCloser.(io.Seeker)
	if !ok {
		return 0, errors.New("faultFS: 底层文件不支持定位")
	}
	return seeker.Seek(offset, whence)
}

// newFaultStore 打开一个挂在 faultFS 上的小存储。
//
// 块大小 64 字节、内联阈值 16 字节：200 字节的测试数据会切成 4 个块，
// 一次 PutObject 因此包含多次 sync / rename / syncdir，
// "第 N 次操作"才有足够多的 N 可注入。
func newFaultStore(t *testing.T) (*Store, *faultFS) {
	t.Helper()
	fsys := newFaultFS(OSFS())
	s, err := Open(Options{
		Root:            filepath.Join(t.TempDir(), "data"),
		FS:              fsys,
		BlockSize:       64,
		InlineThreshold: 16,
	})
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	if err := s.CreateBucket("bkt"); err != nil {
		t.Fatalf("CreateBucket 失败: %v", err)
	}
	return s, fsys
}

// putBytes 写一个对象，把错误留给调用方判断。
func putBytes(s *Store, key string, data []byte) error {
	_, err := s.PutObject("bkt", key, bytes.NewReader(data), PutOptions{
		ExpectedSize: int64(len(data)),
	})
	return err
}

// mustPutBytes 写一个对象，失败即中止测试。
func mustPutBytes(t *testing.T, s *Store, key string, data []byte) {
	t.Helper()
	if err := putBytes(s, key, data); err != nil {
		t.Fatalf("PutObject(%q) 失败: %v", key, err)
	}
}

// readIfExists 读取对象；不存在时返回 (nil, false)。
//
// 与 readObjectAll 的区别：这里把"不存在"当作一种**合法**结果 ——
// 故障测试的断言正是"只可能不存在，或逐字节相等"。
func readIfExists(t *testing.T, s *Store, key string) ([]byte, bool) {
	t.Helper()
	r, err := s.OpenObject("bkt", key)
	if errors.Is(err, ErrNoSuchKey) {
		return nil, false
	}
	if err != nil {
		t.Fatalf("OpenObject(%q) 失败: %v", key, err)
	}
	var buf bytes.Buffer
	if err := r.WriteRangeTo(&buf, 0, r.Info().Size); err != nil {
		t.Fatalf("读取对象 %q 失败: %v", key, err)
	}
	return buf.Bytes(), true
}

// TestFaultInjectionNeverLeavesPartialObject 是本切片的核心断言：
// 在每一个可注入步骤上失败，对象要么不存在，要么是**某一份完整内容**。
//
// 覆盖两类写入：新建（不存在 → 完整）与覆盖（旧完整 → 新完整）。
// 覆盖写是更严格的场景：任何"就地改写"的实现都会在这里露出半个对象。
func TestFaultInjectionNeverLeavesPartialObject(t *testing.T) {
	ops := []string{opOpenFile, opWrite, opSync, opRename, opSyncDir}
	const key = "victim"

	for _, op := range ops {
		// 自校验：如果某个 op 在所有 N 上都没真正触发失败，
		// 说明注入点写错了（比如包装层没有转发该方法），
		// 这一组子测试就是"全绿但什么也没测"。
		injected := 0
		for _, n := range []int{1, 2, 3, 4, 5, 6, 8, 10} {
			for _, overwrite := range []bool{false, true} {
				name := fmt.Sprintf("%s/n=%d/overwrite=%v", op, n, overwrite)
				t.Run(name, func(t *testing.T) {
					s, fsys := newFaultStore(t)

					oldData := mpuBytes(200, 0x11)
					newData := mpuBytes(200, 0x22)
					if overwrite {
						mustPutBytes(t, s, key, oldData)
					}

					fsys.failNth(op, n, errInjected)
					putErr := putBytes(s, key, newData)
					if putErr != nil {
						injected++
					}

					got, exists := readIfExists(t, s, key)
					switch {
					case exists && bytes.Equal(got, oldData):
						// 元数据未替换：旧版本完好。
					case exists && bytes.Equal(got, newData):
						// 元数据已替换（例如 syncDir 失败发生在 rename 之后）。
					case !exists:
						if overwrite {
							t.Errorf("覆盖写失败后旧对象消失了（%s 第 %d 次失败）", op, n)
						}
					default:
						t.Fatalf("读到残缺对象：%d 字节，既不是旧内容也不是新内容", len(got))
					}

					if putErr != nil && !errors.Is(putErr, errInjected) {
						t.Errorf("注入的错误未被保留（无法 errors.Is），实际: %v", putErr)
					}
					// 失败路径必须自己收拾临时文件，不能把账单留给运维。
					assertNoTmpResidue(t, s.Root())

					// 故障消失后必须能自行恢复。
					fsys.disarm()
					if err := putBytes(s, key, newData); err != nil {
						t.Fatalf("故障解除后重写失败: %v", err)
					}
					if got := readObjectAll(t, s, key); !bytes.Equal(got, newData) {
						t.Fatalf("重写后内容不符：%d 字节", len(got))
					}
					assertNoTmpResidue(t, s.Root())
				})
			}
		}
		if injected == 0 {
			t.Errorf("%s 注入从未生效，测试实际上没有覆盖任何故障窗口", op)
		}
	}
}

// TestDiskFullKeepsReadsAndDeletesWorking 对应 05 文档的磁盘满三层次：
// 写入失败且错误可识别、已有数据仍可读可删、腾出空间后自动恢复写入。
func TestDiskFullKeepsReadsAndDeletesWorking(t *testing.T) {
	for _, op := range []string{opWrite, opRename} {
		t.Run(op, func(t *testing.T) {
			s, fsys := newFaultStore(t)

			aData := mpuBytes(200, 0xA1)
			mustPutBytes(t, s, "a", aData)
			mustPutBytes(t, s, "b", mpuBytes(200, 0xB2))

			fsys.fail(op, syscall.ENOSPC)
			err := putBytes(s, "c", mpuBytes(200, 0xC3))
			if err == nil {
				t.Fatal("磁盘满时写入应失败")
			}
			// 错误必须能被识别成 ENOSPC：HTTP 层据此映射 503 SlowDown，
			// 而不是泛化成 500。
			if !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("错误未保留 ENOSPC，实际: %v", err)
			}
			assertNoTmpResidue(t, s.Root())
			if _, exists := readIfExists(t, s, "c"); exists {
				t.Error("写入失败的对象却可见")
			}

			// 满盘时读取与删除必须照常工作 —— 否则运维连"删点东西腾空间"
			// 这条自救路径都没有。
			if got := readObjectAll(t, s, "a"); !bytes.Equal(got, aData) {
				t.Error("满盘时读取已有对象失败或内容不符")
			}
			if err := s.DeleteObject("bkt", "b"); err != nil {
				t.Fatalf("满盘时删除对象失败: %v", err)
			}
			if _, exists := readIfExists(t, s, "b"); exists {
				t.Error("删除后的对象仍然可见")
			}
			if err := s.CreateBucket("other"); err == nil {
				t.Error("满盘时建桶应失败")
			}

			// 腾出空间（此处等价于故障消失）后写入应当自动恢复。
			fsys.disarm()
			mustPutBytes(t, s, "c", mpuBytes(200, 0xC3))
			if got := readObjectAll(t, s, "c"); !bytes.Equal(got, mpuBytes(200, 0xC3)) {
				t.Error("恢复写入后内容不符")
			}
			assertNoTmpResidue(t, s.Root())
		})
	}
}
