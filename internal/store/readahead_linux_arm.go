//go:build linux && arm

package store

import "io"

// setReadahead 在 32 位 ARM 上是空操作。
//
// 该架构内核使用 SYS_ARM_FADVISE64_64，参数排列与其它架构不同
// （64 位偏移与长度被拆成高低两个 32 位寄存器，advice 的位置也提前），
// 直接套用 fadvise64 的排列会传出一个错误的范围。边端部署目标是
// amd64 与 arm64，这里保持空实现而不是猜测参数顺序：预读是纯提示，
// 空实现只损失收益，不会影响正确性。
func setReadahead(io.ReadWriteCloser, int64, int64) {}
