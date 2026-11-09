package s3api

import (
	"log/slog"
	"net/http"

	"github.com/binbinpeng123/Blink/internal/buildinfo"
)

// "满盘时 /readyz 保持 200"是已确认决策：主流对象存储的健康检查只反映
// 进程/后端可用性，容量作为独立指标监控。若满盘返回 503，Kubernetes 会
// 摘除唯一实例，连读取与删除都无法进行，用户就失去了"删数据自救"的途径。

// healthzResponse 是 /healthz 的响应体。
type healthzResponse struct {
	Status string `json:"status"`
}

// readyzResponse 是 /readyz 的响应体。
//
// disk 反映数据卷容量与写入阻断状态，但**不参与** ready 的判定。
// 客户端应忽略未知字段，以便后续切片继续扩展。
type readyzResponse struct {
	Status string     `json:"status"`
	Ready  bool       `json:"ready"`
	Disk   readyzDisk `json:"disk"`
}

// readyzDisk 是 /readyz 的容量子对象。
//
// available=false 表示 statfs 失败或未接入容量统计，此时其余数值为 0。
// 高/低水位原样回显配置值，让运维不必查启动参数就能核对滞回区间。
type readyzDisk struct {
	Available     bool    `json:"available"`
	TotalBytes    uint64  `json:"total_bytes"`
	FreeBytes     uint64  `json:"free_bytes"`
	UsedRatio     float64 `json:"used_ratio"`
	HighWatermark float64 `json:"high_watermark"`
	LowWatermark  float64 `json:"low_watermark"`
	WriteBlocked  bool    `json:"write_blocked"`
	// ReclaimableBytes 是上一轮 GC 统计出的未引用块字节数。
	//
	// 该数值包含宽限期内与 pin 中的块，因此会大于"下一轮实际删除的
	// 字节"；它的用途是解释"删除对象后空间为何没有立即释放"。
	ReclaimableBytes int64 `json:"reclaimable_bytes"`
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
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		s3errMethodNotAllowed(w, r)
		return
	}

	disk := readyzDisk{
		HighWatermark: s.highWatermark,
		LowWatermark:  s.lowWatermark,
	}
	if s.capacity != nil {
		usage := s.capacity.DiskUsage()
		disk.Available = usage.Available
		disk.TotalBytes = usage.Total
		disk.FreeBytes = usage.Free
		disk.UsedRatio = usage.UsedRatio
		disk.WriteBlocked = s.capacity.WriteBlocked()
		disk.ReclaimableBytes = s.capacity.ReclaimableBytes()
		if !usage.Available && usage.Err != nil {
			// 错误只进日志：探针响应不该把底层文件系统的错误细节
			// 暴露给任何能访问该端口的人，接口本身仍保持 200。
			loggerFrom(r.Context(), s.logger).Warn("readyz_disk_usage_unavailable",
				slog.String("error", usage.Err.Error()),
			)
		}
	}

	writeJSON(w, r, http.StatusOK, readyzResponse{Status: "ok", Ready: true, Disk: disk})
}

// handleVersion 返回构建期注入的版本信息，便于排障与灰度核对。
func handleVersion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		s3errMethodNotAllowed(w, r)
		return
	}
	writeJSON(w, r, http.StatusOK, buildinfo.Get())
}
