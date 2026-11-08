package s3api

import "net/http"

// handleMetrics 暴露 Prometheus 文本格式指标。
//
// 容量与运行时长在**抓取时**刷新，而不是每个请求都算：statfs 有系统
// 调用成本，而这些量变化很慢，抓取间隔（通常 15s）足够覆盖。
//
// 端点免签（见 unauthenticatedPaths）：Prometheus 抓取器通常不带 SigV4。
// 这意味着任何能连上端口的人都能读到负载与错误率，部署必须用
// NetworkPolicy / 反向代理限制来源。指标本身不含 bucket、key 与凭据。
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		s3errMethodNotAllowed(w, r)
		return
	}

	// 未配置指标注册表时仍返回 200 与合法 Content-Type：
	// 抓取器看到空体只会得到零样本，而不是把"没开指标"当成端点故障。
	if s.metrics == nil {
		w.Header().Set("Content-Type", metricsContentType)
		w.WriteHeader(http.StatusOK)
		return
	}

	if s.capacity != nil {
		s.metrics.refreshDisk(s.capacity.DiskUsage(), s.capacity.WriteBlocked())
	}
	s.metrics.refreshUptime(s.startedAt)

	w.Header().Set("Content-Type", metricsContentType)
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	s.metrics.registry.Write(w)
}
