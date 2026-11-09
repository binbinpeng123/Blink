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
	// diskReclaimable 是 CAS 中当前未被引用、超过宽限期即可删除的字节数。
	diskReclaimable *metrics.GaugeVec
	credentials     *metrics.GaugeVec
	uptime          *metrics.GaugeVec
	buildInfo       *metrics.GaugeVec

	scrubScanned   *metrics.GaugeVec
	scrubCorrupt   *metrics.GaugeVec
	scrubDuration  *metrics.GaugeVec
	scrubCompleted *metrics.GaugeVec

	ttlExpired   *metrics.GaugeVec
	ttlDeleted   *metrics.GaugeVec
	ttlSkipped   *metrics.GaugeVec
	ttlFailed    *metrics.GaugeVec
	ttlDuration  *metrics.GaugeVec
	ttlCompleted *metrics.GaugeVec
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
		diskReclaimable: reg.Gauge("blink_disk_reclaimable_bytes",
			"CAS bytes no longer referenced by any metadata; includes blocks still inside the GC grace period."),
		credentials: reg.Gauge("blink_credentials_total",
			"Number of configured credentials."),
		uptime: reg.Gauge("blink_uptime_seconds",
			"Process uptime in seconds."),
		buildInfo: reg.Gauge("blink_build_info",
			"Build information; value is always 1.", "version", "commit"),
		// 巡检指标描述的是**最近一轮**，不是累计值：运维关心的是
		// "现在盘上还有没有坏数据"，单调计数器无法表达这一点。
		scrubScanned: reg.Gauge("blink_scrub_last_scanned_objects",
			"Objects inspected by the most recent background integrity scrub."),
		scrubCorrupt: reg.Gauge("blink_scrub_last_corrupt_objects",
			"Objects with detected corruption in the most recent integrity scrub."),
		scrubDuration: reg.Gauge("blink_scrub_last_duration_seconds",
			"Wall-clock duration of the most recent integrity scrub."),
		scrubCompleted: reg.Gauge("blink_scrub_last_completed_timestamp_seconds",
			"Unix timestamp when the most recent integrity scrub finished; 0 means none has completed."),
		// TTL 清扫指标同样描述"最近一轮"。expired 包含扫描后又被并发
		// 覆盖、因而实际未删除的对象，deleted + skipped 才等于本次
		// 实际处理的候选数；两者分开暴露是为了区分"清扫正常"
		// 与"清扫一直在和写者竞争"。
		ttlExpired: reg.Gauge("blink_ttl_last_expired_objects",
			"Expired objects observed by the most recent TTL sweep."),
		ttlDeleted: reg.Gauge("blink_ttl_last_deleted_objects",
			"Expired objects deleted by the most recent TTL sweep."),
		ttlSkipped: reg.Gauge("blink_ttl_last_skipped_objects",
			"Candidates skipped by the most recent TTL sweep because they were overwritten or already gone."),
		ttlFailed: reg.Gauge("blink_ttl_last_failed_objects",
			"Objects the most recent TTL sweep failed to inspect or delete."),
		ttlDuration: reg.Gauge("blink_ttl_last_duration_seconds",
			"Wall-clock duration of the most recent TTL sweep."),
		ttlCompleted: reg.Gauge("blink_ttl_last_completed_timestamp_seconds",
			"Unix timestamp when the most recent TTL sweep finished; 0 means none has completed."),
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
func (m *serverMetrics) refreshDisk(u store.DiskUsage, blocked bool, reclaimable int64) {
	if m == nil {
		return
	}
	m.diskBlocked.SetBool(blocked)
	// 可回收字节来自上一轮 GC 的扫描结果，与 statfs 是否可用无关，
	// 因此排在 available 判断之前写入。
	m.diskReclaimable.Set(float64(reclaimable))
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

// refreshScrub 写入最近一轮完整性巡检的摘要。
//
// 从未完成过巡检时，除完成时间外的数值都保持 0：这比隐藏整族指标
// 更好——"scanned=0 且 completed=0"在面板上明确表达"还没跑过"，
// 而指标族消失会让告警规则因缺样本触发（或更糟，静默不触发）。
func (m *serverMetrics) refreshScrub(st store.ScrubStats) {
	if m == nil {
		return
	}
	m.scrubScanned.Set(float64(st.ScannedObjects))
	m.scrubCorrupt.Set(float64(st.CorruptObjects))
	m.scrubDuration.Set(st.Duration.Seconds())
	if st.FinishedAt.IsZero() {
		m.scrubCompleted.Set(0)
		return
	}
	m.scrubCompleted.Set(float64(st.FinishedAt.Unix()))
}

// refreshTTLSweep 写入最近一轮过期对象清扫的摘要。
//
// 与 refreshScrub 同一约定：从未跑过时数值为 0，但指标族始终存在。
func (m *serverMetrics) refreshTTLSweep(st store.TTLSweepStats) {
	if m == nil {
		return
	}
	m.ttlExpired.Set(float64(st.ExpiredObjects))
	m.ttlDeleted.Set(float64(st.DeletedObjects))
	m.ttlSkipped.Set(float64(st.SkippedObjects))
	m.ttlFailed.Set(float64(st.FailedObjects))
	m.ttlDuration.Set(st.Duration.Seconds())
	if st.FinishedAt.IsZero() {
		m.ttlCompleted.Set(0)
		return
	}
	m.ttlCompleted.Set(float64(st.FinishedAt.Unix()))
}

// refreshUptime 写入进程运行时长。
func (m *serverMetrics) refreshUptime(start time.Time) {
	if m == nil || start.IsZero() {
		return
	}
	m.uptime.Set(time.Since(start).Seconds())
}
