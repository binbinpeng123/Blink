package s3api

import (
	"net/http"

	"github.com/binbinpeng123/Blink/internal/buildinfo"
)

// TODO(slice-3): 磁盘水位接入后，readyz 返回真实 usage/watermark 字段。
// 在此之前 ready 恒为 true（除数据目录不可写外无其它不可用条件），
// 而"满盘时 /readyz 是否保持 200"属已记录待确认项（docs/design/known-limitations.md），
// 当前实现取"保持 200，仅暴露水位字段"。

// healthzResponse 是 /healthz 的响应体。
type healthzResponse struct {
	Status string `json:"status"`
}

// readyzResponse 是 /readyz 的响应体。
//
// 字段为前向兼容预留：切片 3 接入磁盘水位后会新增 disk 子对象，
// 客户端应忽略未知字段。
type readyzResponse struct {
	Status string `json:"status"`
	Ready  bool   `json:"ready"`
}

// handleHealthz 是存活探针：只要进程还能响应就返回 200。
//
// 该端点**不检查**磁盘水位与后端状态，故意保持极简，
// 以免外部依赖抖动导致容器被反复重启。
func handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		s3errMethodNotAllowed(w, r)
		return
	}
	writeJSON(w, r, http.StatusOK, healthzResponse{Status: "ok"})
}

// handleReadyz 是就绪探针：返回当前是否可接受写入。
//
// 按已记录的设计决策，满盘时仍返回 200，只通过字段反映状态，
// 避免 Kubernetes 摘除流量后用户无法通过删除对象自救。
func handleReadyz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		s3errMethodNotAllowed(w, r)
		return
	}
	writeJSON(w, r, http.StatusOK, readyzResponse{Status: "ok", Ready: true})
}

// handleVersion 返回构建期注入的版本信息，便于排障与灰度核对。
func handleVersion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		s3errMethodNotAllowed(w, r)
		return
	}
	writeJSON(w, r, http.StatusOK, buildinfo.Get())
}
