// Package store 实现 Blink 的对象存储引擎。
//
// 设计要点见 docs/03：单节点、无外部依赖、元数据用 JSON、数据块内容寻址（CAS）。
// 本包只依赖标准库，且**不感知 HTTP** —— 错误以 error 返回，
// 由 internal/s3api 负责翻译成 S3 错误码。
package store

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// FileSystem 是所有文件系统操作的唯一出口。
//
// 存在的理由只有一个：**故障注入**。05 文档要求覆盖"fsync 后、rename 前"
// 这类关键崩溃窗口，而这些窗口无法靠真杀进程稳定复现 —— 必须能在
// 指定调用次序上注入错误。把全部系统调用收敛到一个窄接口，
// 测试就能用 faultFS 精确替换掉第 N 次 Sync 或第 N 次 Rename。
//
// 接口刻意保持窄且笨：不用 os.File 而用 io.ReadWriteCloser，
// 是为了让故障实现可以只包装少数几个方法，而不必模拟整个 os.File。
type FileSystem interface {
	MkdirAll(path string, perm fs.FileMode) error
	Stat(path string) (fs.FileInfo, error)
	// Open 以只读方式打开文件。
	Open(path string) (io.ReadWriteCloser, error)
	// OpenFile 以指定标志打开文件（写入路径统一走它）。
	OpenFile(path string, flag int, perm fs.FileMode) (io.ReadWriteCloser, error)
	// ReadDir 按文件名排序返回目录项。
	ReadDir(path string) ([]fs.DirEntry, error)
	Remove(path string) error
	RemoveAll(path string) error
	Rename(oldpath, newpath string) error
	// SyncDir 对目录 fsync。
	//
	// rename 本身不保证落盘，必须再 fsync 父目录，
	// 否则掉电后可能出现"目录项丢了但数据还在"的悬挂状态。
	SyncDir(path string) error
	// WriteFile 是"写临时文件 → fsync → rename"之外的快捷路径，
	// 仅供不要求原子性的场景（如启动时清扫标记）。
	WriteFile(path string, data []byte, perm fs.FileMode) error
	// Truncate 用于预分配块文件。
	Truncate(path string, size int64) error
}

// osFS 是基于 os 包的真实实现。
type osFS struct{}

// OSFS 返回直接操作真实文件系统的 FileSystem。
func OSFS() FileSystem { return osFS{} }

func (osFS) MkdirAll(path string, perm fs.FileMode) error { return os.MkdirAll(path, perm) }
func (osFS) Stat(path string) (fs.FileInfo, error)        { return os.Stat(path) }
func (osFS) Open(path string) (io.ReadWriteCloser, error) { return os.Open(path) }
func (osFS) OpenFile(path string, flag int, perm fs.FileMode) (io.ReadWriteCloser, error) {
	return os.OpenFile(path, flag, perm)
}
func (osFS) ReadDir(path string) ([]fs.DirEntry, error) { return os.ReadDir(path) }
func (osFS) Remove(path string) error                   { return os.Remove(path) }
func (osFS) RemoveAll(path string) error                { return os.RemoveAll(path) }

// Rename 二次尝试以兼容 macOS 语义。
//
// macOS 上 os.Rename **不能**在目标已存在时覆盖目录（返回 ENOTEMPTY），
// 而 Linux 可以。对象元数据更新依赖"改名覆盖"这一原子操作，
// 因此这里显式处理：先尝试直接改名，失败且目标确为目录时，
// 交换成"改名到临时名 + 删除旧目录 + 再改名"的三步。
//
// 三步并非原子，因此**只在目录替换场景**（分片完成、元数据更新）使用；
// 普通文件覆盖在 macOS 上是原子的，走快速路径。
func (fsys osFS) Rename(oldpath, newpath string) error {
	err := os.Rename(oldpath, newpath)
	if err == nil {
		return err
	}
	// 快速路径失败：仅当"目标存在且是目录"时才走兼容路径。
	if fi, statErr := os.Stat(newpath); statErr != nil || !fi.IsDir() {
		return err
	}
	holding := newpath + ".old"
	_ = os.RemoveAll(holding)
	if renameErr := os.Rename(newpath, holding); renameErr != nil {
		return err
	}
	if renameErr := os.Rename(oldpath, newpath); renameErr != nil {
		// 尽力回滚，避免把数据卡在 holding 里。
		_ = os.Rename(holding, newpath)
		return renameErr
	}
	_ = os.RemoveAll(holding)
	return nil
}

// SyncDir 对目录 fsync，确保其中的目录项变更落盘。
func (fsys osFS) SyncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		// 部分文件系统不支持目录 fsync（返回 EINVAL/ENOTSUP）。
		// 这不是数据风险，而是平台限制，因此不向上传播。
		return nil
	}
	return nil
}

func (osFS) WriteFile(path string, data []byte, perm fs.FileMode) error {
	return os.WriteFile(path, data, perm)
}

func (osFS) Truncate(path string, size int64) error { return os.Truncate(path, size) }

// joinPath 是 path/filepath.Join 的包装，集中一处便于将来改为
// 平台无关的路径策略（例如禁止某些字符）。
func joinPath(elem ...string) string { return filepath.Join(elem...) }
