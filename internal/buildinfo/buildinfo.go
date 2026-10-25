// Package buildinfo 提供构建期注入的版本信息。
//
// 版本信息通过 -ldflags 注入，未注入时回落到开发值，便于本地调试：
//
//	go build -ldflags "-X github.com/binbinpeng123/Blink/internal/buildinfo.Version=v0.1.0"
package buildinfo

import "runtime"

// 以下变量由构建脚本通过 -ldflags -X 覆盖，不应在代码中直接赋值。
var (
	// Version 是语义化版本号，未注入时为 dev。
	Version = "dev"
	// Commit 是构建所基于的 Git 提交短哈希。
	Commit = "unknown"
	// BuildTime 是构建时间戳（UTC，RFC3339）。
	BuildTime = "unknown"
)

// GoVersion 返回构建所用的 Go 版本。
func GoVersion() string {
	return runtime.Version()
}

// Info 汇总版本信息，供 /version 端点与日志使用。
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildTime string `json:"build_time"`
	Go        string `json:"go"`
}

// Get 返回当前二进制的版本信息快照。
func Get() Info {
	return Info{
		Version:   Version,
		Commit:    Commit,
		BuildTime: BuildTime,
		Go:        GoVersion(),
	}
}
