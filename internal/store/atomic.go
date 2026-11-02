package store

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// 文件权限。
//
// 目录 0750、文件 0640：同组可读即可满足"运维账号看盘"的常见需求，
// 同时不给 others 任何权限。凭据不在数据目录里，因此这里不需要更严的 0600。
const (
	dirPerm  fs.FileMode = 0o750
	filePerm fs.FileMode = 0o640
)

// randHex 返回 n 字节随机数的十六进制表示（长度 2n）。
func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand 失败意味着系统熵源不可用，继续运行会生成可预测的文件名。
		// 这在实践中只可能发生在极度异常的容器环境，直接 panic 比静默降级安全。
		panic("store: 无法获取随机数: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// fsyncPolicy 决定哪些写入需要 fsync。
//
// always：文件与父目录都 fsync，掉电后保证"要么没有、要么完整"。
// meta：只 fsync 元数据，数据块靠文件系统自身保证。吞吐更高，
// 但掉电后可能丢失刚写入的数据块（对象表现为不存在），不会出现残缺对象。
//
// 默认 always —— 边端设备掉电概率高于数据中心，正确性优先于吞吐。
type fsyncPolicy int

const (
	fsyncAlways fsyncPolicy = iota
	fsyncMeta
)

// syncFile 按策略 fsync 一个已写入的文件。
//
// isMeta 由调用方显式传入，而不是用 Store 上的一个标志位表示：
// 并发写两个对象时，两个 goroutine 会同时读改写那个标志，导致其中一个
// 的数据 fsync 被误判成元数据 fsync 而跳过 —— 这是静默的数据丢失，
// 且在 -race 下才看得见。参数传值没有共享状态，天然安全。
func (s *Store) syncFile(f io.ReadWriteCloser, isMeta bool) error {
	if s.fsync == fsyncMeta && !isMeta {
		return nil
	}
	type syncer interface{ Sync() error }
	if sy, ok := f.(syncer); ok {
		return sy.Sync()
	}
	return nil
}

// writeFileAtomic 原子地把 data 写入 path。
//
// 步骤：写临时文件 → fsync 临时文件 → rename 覆盖 → fsync 父目录。
//
// 缺一不可：只 rename 不 fsync 文件，掉电后可能留下"名字对了但内容为空"
// 的文件；只 fsync 文件不 fsync 父目录，掉电后目录项本身可能丢失。
// 由于 rsync 语义的目标是**整体替换**，中途失败绝不破坏原有文件 ——
// 这也是元数据更新一律走这里的原因。
func (s *Store) writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := s.fs.MkdirAll(dir, dirPerm); err != nil {
		return err
	}
	tmp := path + ".tmp-" + randHex(8)

	f, err := s.fs.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, filePerm)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		_ = s.fs.Remove(tmp)
		return err
	}
	syncErr := s.syncFile(f, true)
	closeErr := f.Close()
	if syncErr != nil {
		_ = s.fs.Remove(tmp)
		return syncErr
	}
	if closeErr != nil {
		_ = s.fs.Remove(tmp)
		return closeErr
	}
	if err := s.fs.Rename(tmp, path); err != nil {
		_ = s.fs.Remove(tmp)
		return err
	}
	return s.fs.SyncDir(dir)
}

// atomicWriter 是一次原子写入的句柄：数据先落到临时文件，
// Close 时才 rename 到位。中途出错则临时文件被丢弃，目标文件保持原样。
type atomicWriter struct {
	store    *Store
	tmp      string
	final    string
	f        io.ReadWriteCloser
	written  int64
	closed   bool
	replaced bool
	// isMeta 表示这次写入是元数据，meta 策略下仍需 fsync。
	isMeta bool
	// aborted 记录临时文件已被丢弃。
	//
	// 没有它 Abort 就不幂等：第二次调用会对已删除的文件再 Remove，
	// 得到一个毫无意义的 ENOENT。而"显式 Abort + defer Abort"是调用方的
	// 自然写法，Commit 失败后的清理路径也会落到同一个临时文件上。
	aborted bool
}

// newAtomicWriter 在 final 所在目录创建临时文件。
//
// 临时文件与目标**必须同目录**：rename 只能在同一文件系统内保证原子，
// 放到 tmp/ 再 rename 会退化成"复制 + 删除"，中途失败会留下半个文件。
//
// isMeta 决定 meta 策略下是否 fsync（见 syncFile）。
func (s *Store) newAtomicWriter(final string, isMeta bool) (*atomicWriter, error) {
	dir := filepath.Dir(final)
	if err := s.fs.MkdirAll(dir, dirPerm); err != nil {
		return nil, err
	}
	tmp := final + ".tmp-" + randHex(8)
	f, err := s.fs.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, filePerm)
	if err != nil {
		return nil, err
	}
	return &atomicWriter{store: s, tmp: tmp, final: final, f: f, isMeta: isMeta}, nil
}

// Write 实现 io.Writer。
func (w *atomicWriter) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	w.written += int64(n)
	return n, err
}

// WriteAt 实现 io.WriterAt，供随机写入（预分配后回填）使用。
func (w *atomicWriter) WriteAt(p []byte, off int64) (int, error) {
	type writerAt interface {
		WriteAt([]byte, int64) (int, error)
	}
	wa, ok := w.f.(writerAt)
	if !ok {
		return 0, errors.New("store: 底层文件不支持随机写")
	}
	return wa.WriteAt(p, off)
}

// Truncate 把临时文件预分配到指定长度。
func (w *atomicWriter) Truncate(size int64) error {
	type truncater interface{ Truncate(int64) error }
	t, ok := w.f.(truncater)
	if !ok {
		return errors.New("store: 底层文件不支持截断")
	}
	// 预分配会改变"已写入字节数"的语义 —— 对固定长度块文件而言，
	// 长度就是最终长度，因此这里同步更新 written。
	err := t.Truncate(size)
	if err == nil && size > w.written {
		w.written = size
	}
	return err
}

// Size 返回已写入的字节数。
func (w *atomicWriter) Size() int64 { return w.written }

// Commit 落盘并 rename 到位；之后调用方再也不能写。
func (w *atomicWriter) Commit() error {
	if w.closed {
		return errors.New("store: 原子写入已关闭")
	}
	s := w.store
	if err := s.syncFile(w.f, w.isMeta); err != nil {
		_ = w.Abort()
		return err
	}
	if err := w.f.Close(); err != nil {
		w.closed = true
		_ = s.fs.Remove(w.tmp)
		return err
	}
	w.closed = true
	if err := s.fs.Rename(w.tmp, w.final); err != nil {
		_ = s.fs.Remove(w.tmp)
		return err
	}
	w.replaced = true
	return s.fs.SyncDir(filepath.Dir(w.final))
}

// Abort 丢弃临时文件。可重复调用。
//
// 已提交的写入不会被撤销 —— 否则 Commit 返回成功就成了谎言。
func (w *atomicWriter) Abort() error {
	if w.replaced || w.aborted {
		return nil
	}
	w.aborted = true
	if !w.closed {
		w.closed = true
		_ = w.f.Close()
	}
	return w.store.fs.Remove(w.tmp)
}

// cleanTmp 清空临时目录。
//
// 启动时调用：任何残留的临时文件都属于"上一次运行中途失败"，
// 其内容要么已通过 rename 正式收录（那它就是重复品），
// 要么从未提交（那就该丢弃）。两种情况都可以安全删除。
func (s *Store) cleanTmp() error {
	tmpDir := joinPath(s.root, tmpDirName)
	if err := s.fs.RemoveAll(tmpDir); err != nil {
		return err
	}
	return s.fs.MkdirAll(tmpDir, dirPerm)
}
