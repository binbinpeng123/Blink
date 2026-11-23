//go:build darwin

package store

import (
	"io"
	"math"
	"syscall"
	"unsafe"
)

// setReadahead 在 macOS 上用 fcntl(F_RDADVISE) 提示内核预读。
//
// macOS 没有 fadvise：对应能力是 F_RDADVISE，它只支持"预读一段区间"，
// 没有 sequential 模式。参数结构是 Radvisory_t。
//
// 与 Linux 版一样默认关闭、不宣称收益：本机 A/B 完全重叠，冷缓存
// 顺序下载反而更慢（docs/reviews/09）。
//
// 这是**提示**而非契约：拿不到 fd（包装层）或 fcntl 失败都静默忽略，
// 预读失败绝不能让一次正常读取返回错误。
func setReadahead(f io.ReadWriteCloser, offset, length int64) {
	if offset < 0 || length <= 0 {
		return
	}
	// F_RDADVISE 的 Count 是 int32，超过 MaxInt32 会变成负数。
	// 块最大 4 MiB（--block-size），不会接近上限，这里仍做一次防御
	// 截断而不是依赖调用方保证。
	if length > math.MaxInt32 {
		length = math.MaxInt32
	}
	ra := syscall.Radvisory_t{Offset: offset, Count: int32(length)}

	sc, ok := f.(syscall.Conn)
	if !ok {
		return
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return
	}
	// fd 只在 Control 回调期间有效。
	_ = rc.Control(func(fd uintptr) {
		_, _, _ = syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_RDADVISE,
			uintptr(unsafe.Pointer(&ra)))
	})
}
