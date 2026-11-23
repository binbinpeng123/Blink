//go:build linux && !arm

package store

import (
	"io"
	"syscall"
)

// posixFadvWillNeed 是 POSIX_FADV_WILLNEED。
//
// Go 的 syscall 包只导出了 fadvise 的系统调用号，没有导出 FADV 常量，
// 因此在这里按 POSIX 取值。该值在所有 Linux 架构上一致。
const posixFadvWillNeed = 3

// setReadahead 提示内核把 [offset, offset+length) 读入页缓存。
//
// 这是**提示**而非契约：任何失败（包括调用方传进来的是包装层、
// 不实现 syscall.Conn）都静默忽略。预读只影响速度，绝不能因为
// 提示失败让一次正常读取返回错误。
//
// 别指望它现在有收益：冷缓存实测显示内核的顺序 readahead 已经覆盖
// 了逐块读，顺序下载时调用它反而更慢（docs/reviews/09）。它默认
// 关闭，保留是为了在慢盘上重新验证。选带区间的 WILLNEED 而非
// FADV_SEQUENTIAL 的理由是后者作用于整个文件、读完即丢页，会伤害
// 多个对象共享同一 CAS 块时的并发读。
func setReadahead(f io.ReadWriteCloser, offset, length int64) {
	if offset < 0 || length <= 0 {
		return
	}
	sc, ok := f.(syscall.Conn)
	if !ok {
		return
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return
	}
	// fd 只在 Control 回调期间有效；回调返回后不得再使用。
	_ = rc.Control(func(fd uintptr) {
		// fadvise64(fd, offset, len, advice)：各架构参数排列一致
		// （只有 32 位 ARM 是例外，见 readahead_linux_arm.go）。
		_, _, _ = syscall.Syscall6(syscall.SYS_FADVISE64,
			fd, uintptr(offset), uintptr(length), posixFadvWillNeed, 0, 0)
	})
}
