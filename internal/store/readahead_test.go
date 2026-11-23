package store

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// plainFile 隐藏底层文件的 SyscallConn 方法。
//
// 模拟"包装层不透传 fd"的 FileSystem 实现：setReadahead 必须静默走
// 空实现分支，而不是报错或 panic。
type plainFile struct{ io.ReadWriteCloser }

func TestSetReadaheadIsBestEffort(t *testing.T) {
	path := filepath.Join(t.TempDir(), "block")
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), 4096), 0o600); err != nil {
		t.Fatalf("准备文件失败: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("打开文件失败: %v", err)
	}
	defer func() { _ = f.Close() }()

	// 真实 os.File：走 syscall.Conn 分支。
	setReadahead(f, 0, 4096)
	// 包装层不透传 fd：必须静默跳过。
	setReadahead(plainFile{f}, 0, 4096)
	// 非法参数：必须直接返回，不能把负偏移传给内核。
	setReadahead(f, -1, 4096)
	setReadahead(f, 0, 0)
	setReadahead(f, 0, -1)

	// 预读只是提示：调用之后文件句柄仍可正常读取。
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if len(got) != 4096 {
		t.Fatalf("读到 %d 字节，期望 4096", len(got))
	}
}

// TestFaultFileForwardsSyscallConn 固定 faultFS 的包装层对预读透明：
// 不转发 fd 时，故障注入路径覆盖的就只是空实现。
func TestFaultFileForwardsSyscallConn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "block")
	if err := os.WriteFile(path, []byte("data"), 0o600); err != nil {
		t.Fatalf("准备文件失败: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("打开文件失败: %v", err)
	}
	defer func() { _ = f.Close() }()

	ff := &faultFile{ReadWriteCloser: f, fsys: &faultFS{}}
	var conn syscall.Conn = ff
	rc, err := conn.SyscallConn()
	if err != nil {
		t.Fatalf("faultFile 应转发 SyscallConn: %v", err)
	}
	if err := rc.Control(func(fd uintptr) {
		if fd == 0 {
			t.Error("fd 不应为 0")
		}
	}); err != nil {
		t.Fatalf("Control 失败: %v", err)
	}
}
