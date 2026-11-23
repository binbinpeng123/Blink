//go:build !linux && !darwin

package store

import "io"

// setReadahead 在不支持预读提示的平台上为空操作（Windows、*BSD 等）。
//
// 预读只影响速度：缺失时读路径语义完全不变，只是少了内核侧的
// 一次预取机会。
func setReadahead(io.ReadWriteCloser, int64, int64) {}
