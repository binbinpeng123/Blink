//go:build unix

package store

import (
	"fmt"
	"syscall"
)

// defaultDiskSpace 用 statfs 读取 path 所在文件系统的容量。
//
// 用 Bavail 而不是 Bfree：前者是**非特权用户**可用的块数，
// Blink 以普通用户运行时它才是真实可写量（root 保留的 5% 不计入）。
func defaultDiskSpace(path string) (total, free uint64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, fmt.Errorf("statfs %s: %w", path, err)
	}
	bsize := uint64(st.Bsize)
	// 各平台字段类型不同（darwin 的 Bsize 是 uint32，linux 是 int64），
	// 统一转 uint64 后再乘；Free/Total 都可能为负值的平台不存在。
	return uint64(st.Blocks) * bsize, uint64(st.Bavail) * bsize, nil
}
