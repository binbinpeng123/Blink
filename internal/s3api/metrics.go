package s3api

import (
	"strconv"
	"time"

	"github.com/binbinpeng123/Blink/internal/buildinfo"
	"github.com/binbinpeng123/Blink/internal/metrics"
	"github.com/binbinpeng123/Blink/internal/store"
)

// metricsContentType 是 Prometheus 文本格式的版本化 Content-Type。
//
// 必须带 version 参数：抓取器按它判定格式版本，缺省时部分实现
// 会当作非 Prometheus 文本处理。
const metricsContentType = "text/plain; version=0.0.4; charset=utf-8"

// serverMetrics 汇总本服务暴露的指标句柄。
//
// 为什么持有句柄而不是每次向 Registry 查询：Registry 的查询语义是
// "创建或复用"，运行期反复调用会不断追加同一指标，最终输出重复样本。
// 句柄只在 New 中创建一次。
//
// 所有方法对 nil 接收者安全：Metrics 未配置（例如只为探针启动的
// 嵌入式用法）时指标调用不应让请求失败。
type serverMetrics struct {
	registry *metrics.Registry

	httpRequests *metrics.CounterVec
	httpBytes    *metrics.CounterVec
	httpDuration *metrics.HistogramVec
	authFailures *metrics.CounterVec
	authzDenied  *metrics.CounterVec

	diskBlocked *metrics.GaugeVec
	diskTotal   *metrics.GaugeVec
	diskFree    *metrics.GaugeVec
	diskRatio   *metrics.GaugeVec
	credentials *metrics.GaugeVec
	uptime      *metrics.GaugeVec
	buildInfo   *metrics.GaugeVec
}

// newServerMetrics 在给定注册表上创建全部指标；reg 为 nil 时返回 nil。
func newServerMetrics(reg *metrics.Registry, credentialCount int) *serverMetrics {
	if reg == nil {
		return nil
	}
	m := &serverMetrics{
		registry: reg,
		httpRequests: reg.Counter("blink_http_requests_total",
			"HTTP requests by method and status code.", "method", "status"),
		httpBytes: reg.Counter("blink_http_response_bytes_total",
			"Response body bytes written, by HTTP method.", "method"),
		httpDuration: reg.Histogram("blink_http_request_duration_seconds",
			"HTTP request latency in seconds.", "method"),
		authFailures: reg.Counter("blink_auth_failures_total",
			"Signature verification failures by S3 error code.", "code"),
		authzDenied: reg.Counter("blink_authz_denied_total",
			"Authorization denials by action.", "action"),
		diskBlocked: reg.Gauge("blink_disk_write_blocked",
			"1 when the data volume is above the high watermark and writes are blocked."),
		diskTotal: reg.Gauge("blink_disk_total_bytes",
			"Total bytes of the data volume filesystem."),
		diskFree: reg.Gauge("blink_disk_free_bytes",
			"Free bytes available to the blink process."),
		diskRatio: reg.Gauge("blink_disk_used_ratio",
			"Used ratio of the data volume filesystem, in [0,1]."),
		credentials: reg.Gauge("blink_credentials_total",
			"Number of configured credentials."),
		uptime: reg.Gauge("blink_uptime_seconds",
			"Process uptime in seconds."),
		buildInfo: reg.Gauge("blink_build_info",
			"Build information; value is always 1.", "version", "commit"),
	}
	m.credentials.Set(float64(credentialCount))
	m.buildInfo.Set(1, buildinfo.Version, buildinfo.Commit)
	return m
}

// observeRequest 记录一次请求的方法、状态码、字节数与耗时。
//
// 标签只取有限集合（方法/状态码），不含 bucket、key 或 request_id：
// 高基数标签会让 Prometheus 的内存随对象数量无限增长。
func (m *serverMetrics) observeRequest(method string, status int, bytes int64, elapsed time.Duration) {
	if m == nil {
		return
	}
	m.httpRequests.Inc(method, strconv.Itoa(status))
	if bytes > 0 {
		m.httpBytes.Add(uint64(bytes), method)
	}
	m.httpDuration.Observe(elapsed.Seconds(), method)
}

// authFailed 记录一次签名校验失败。
func (m *serverMetrics) authFailed(code string) {
	if m == nil {
		return
	}
	m.authFailures.Inc(code)
}

// authorizationDenied 记录一次授权拒绝。
func (m *serverMetrics) authorizationDenied(action string) {
	if m == nil {
		return
	}
	m.authzDenied.Inc(action)
}

// refreshDisk 把容量快照写入磁盘 gauge。
//
// 只在抓取时更新而不是每个请求更新：statfs 本身有成本，而容量是
// 慢变量，抓取间隔（通常 15s）远小于填满磁盘所需时间。
func (m *serverMetrics) refreshDisk(u store.DiskUsage, blocked bool) {
	if m == nil {
		return
	}
	m.diskBlocked.SetBool(blocked)
	if !u.Available {
		// 统计不可用时把数值置 0：保留上一次的真实值会让面板在
		// statfs 失败后继续显示过时容量，比显示 0 更容易误导。
		m.diskTotal.Set(0)
		m.diskFree.Set(0)
		m.diskRatio.Set(0)
		return
	}
	m.diskTotal.Set(float64(u.Total))
	m.diskFree.Set(float64(u.Free))
	m.diskRatio.Set(u.UsedRatio)
}

// refreshUptime 写入进程运行时长。
func (m *serverMetrics) refreshUptime(start time.Time) {
	if m == nil || start.IsZero() {
		return
	}
	m.uptime.Set(time.Since(start).Seconds())
}
