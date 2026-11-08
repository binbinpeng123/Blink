//go:build !unix

package store

import "errors"

// errDiskSpaceUnsupported 表示当前平台没有 statfs 实现。
//
// 水位检查 fail-open：量不出容量时不阻断写入，真正写满时 ENOSPC
// 仍会映射为 507。这样 Windows 开发环境不会因为缺少 statfs 而不可写。
var errDiskSpaceUnsupported = errors.New("store: 当前平台不支持磁盘容量统计")

func defaultDiskSpace(string) (total, free uint64, err error) {
	return 0, 0, errDiskSpaceUnsupported
}
