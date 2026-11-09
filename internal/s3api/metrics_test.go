package s3api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/binbinpeng123/Blink/internal/metrics"
	"github.com/binbinpeng123/Blink/internal/store"
)

// fakeCapacity 是可写死读数的 Capacity 实现。
type fakeCapacity struct {
	usage       store.DiskUsage
	blocked     bool
	reclaimable int64
	scrub       store.ScrubStats
	ttl         store.TTLSweepStats
}

func (f fakeCapacity) DiskUsage() store.DiskUsage        { return f.usage }
func (f fakeCapacity) WriteBlocked() bool                { return f.blocked }
func (f fakeCapacity) ReclaimableBytes() int64           { return f.reclaimable }
func (f fakeCapacity) LastScrub() store.ScrubStats       { return f.scrub }
func (f fakeCapacity) LastTTLSweep() store.TTLSweepStats { return f.ttl }

func newMetricsServer(t *testing.T, capacity Capacity) http.Handler {
	t.Helper()
	return New(Options{
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		Region:          testRegion,
		Provider:        testProvider(),
		Metrics:         metrics.NewRegistry(),
		Capacity:        capacity,
		HighWatermark:   0.90,
		LowWatermark:    0.85,
		CredentialCount: 2,
	}).Handler()
}

// TestMetricsEndpoint 验证 /metrics 的协议细节与关键样本。
//
// 先打一次 /healthz 是必须的：指标在请求处理**之后**才记录，
// 首个请求就是 /metrics 时，requests_total 这一族还不存在。
func TestMetricsEndpoint(t *testing.T) {
	h := newMetricsServer(t, fakeCapacity{
		usage:       store.DiskUsage{Total: 1000, Free: 200, UsedRatio: 0.8, Available: true},
		reclaimable: 640,
		scrub: store.ScrubStats{
			ScannedObjects: 7,
			CorruptObjects: 1,
			Duration:       2 * time.Second,
			FinishedAt:     time.Unix(1_700_000_000, 0).UTC(),
		},
		ttl: store.TTLSweepStats{
			Buckets:        1,
			ExpiredObjects: 5,
			DeletedObjects: 3,
			SkippedObjects: 1,
			FailedObjects:  1,
			Duration:       500 * time.Millisecond,
			FinishedAt:     time.Unix(1_700_000_010, 0).UTC(),
		},
	})

	rec := serve(h, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz 应 200，实际 %d", rec.Code)
	}

	rec = serve(h, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics 应 200，实际 %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != metricsContentType {
		t.Errorf("Content-Type = %q，期望 %q", got, metricsContentType)
	}

	body := rec.Body.String()
	for _, want := range []string{
		`blink_http_requests_total{method="GET",status="200"}`,
		"blink_http_response_bytes_total",
		"blink_http_request_duration_seconds",
		"blink_credentials_total 2",
		"blink_disk_total_bytes 1000",
		"blink_disk_free_bytes 200",
		"blink_disk_write_blocked 0",
		"blink_disk_reclaimable_bytes 640",
		"blink_uptime_seconds",
		"blink_build_info",
		"blink_scrub_last_scanned_objects 7",
		"blink_scrub_last_corrupt_objects 1",
		"blink_scrub_last_duration_seconds 2",
		"blink_scrub_last_completed_timestamp_seconds 1.7e+09",
		"blink_ttl_last_expired_objects 5",
		"blink_ttl_last_deleted_objects 3",
		"blink_ttl_last_skipped_objects 1",
		"blink_ttl_last_failed_objects 1",
		"blink_ttl_last_duration_seconds 0.5",
		"blink_ttl_last_completed_timestamp_seconds 1.70000001e+09",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("指标输出缺少 %q\n%s", want, body)
		}
	}

	// HEAD 只回头，不带体。
	head := serve(h, httptest.NewRequest(http.MethodHead, "/metrics", nil))
	if head.Code != http.StatusOK {
		t.Fatalf("HEAD /metrics 应 200，实际 %d", head.Code)
	}
	if head.Body.Len() != 0 {
		t.Errorf("HEAD /metrics 不应有响应体，实际 %d 字节", head.Body.Len())
	}

	// 非 GET/HEAD 拒绝。
	rec = serve(h, httptest.NewRequest(http.MethodPost, "/metrics", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /metrics 应 405，实际 %d", rec.Code)
	}
}

// TestMetricsScrubNeverRan 验证从未完成巡检时指标仍存在且明确表达"没跑过"。
//
// 不能让指标族消失：缺失样本会让基于它的告警规则失去依据。
func TestMetricsScrubNeverRan(t *testing.T) {
	h := newMetricsServer(t, fakeCapacity{})
	rec := serve(h, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics 应 200，实际 %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"blink_scrub_last_scanned_objects 0",
		"blink_scrub_last_corrupt_objects 0",
		"blink_scrub_last_duration_seconds 0",
		"blink_scrub_last_completed_timestamp_seconds 0",
		"blink_ttl_last_expired_objects 0",
		"blink_ttl_last_deleted_objects 0",
		"blink_ttl_last_skipped_objects 0",
		"blink_ttl_last_failed_objects 0",
		"blink_ttl_last_duration_seconds 0",
		"blink_ttl_last_completed_timestamp_seconds 0",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("指标输出缺少 %q\n%s", want, body)
		}
	}
}

// TestMetricsAnonymous 验证 /metrics 免签名。
//
// 抓取器通常不带 SigV4；若这里改成需要签名，Prometheus 会静默
// 把目标标记为 down，而存储本身看起来一切正常。
func TestMetricsAnonymous(t *testing.T) {
	h := newMetricsServer(t, nil)
	rec := serve(h, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("匿名 GET /metrics 应 200，实际 %d: %s", rec.Code, rec.Body.String())
	}
}

// TestMetricsNilRegistry 验证未配置指标时仍返回合法空响应。
func TestMetricsNilRegistry(t *testing.T) {
	h := New(Options{
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Region:   testRegion,
		Provider: testProvider(),
	}).Handler()
	rec := serve(h, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("未配置指标时 /metrics 应 200，实际 %d", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != metricsContentType {
		t.Errorf("Content-Type = %q，期望 %q", got, metricsContentType)
	}
}

// TestReadyzDiskFields 验证 /readyz 忠实回显容量与水位。
func TestReadyzDiskFields(t *testing.T) {
	h := newMetricsServer(t, fakeCapacity{
		usage:       store.DiskUsage{Total: 1000, Free: 100, UsedRatio: 0.9, Available: true},
		blocked:     true,
		reclaimable: 4096,
	})

	rec := serve(h, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("满盘时 /readyz 仍应 200，实际 %d", rec.Code)
	}
	var out readyzResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("响应不是合法 JSON: %v", err)
	}
	if !out.Ready {
		t.Error("满盘不应影响 ready")
	}
	want := readyzDisk{
		Available:        true,
		TotalBytes:       1000,
		FreeBytes:        100,
		UsedRatio:        0.9,
		HighWatermark:    0.90,
		LowWatermark:     0.85,
		WriteBlocked:     true,
		ReclaimableBytes: 4096,
	}
	if out.Disk != want {
		t.Errorf("disk = %+v\n期望 %+v", out.Disk, want)
	}
}

// TestReadyzDiskUnavailable 验证无容量信息时不编造数值。
func TestReadyzDiskUnavailable(t *testing.T) {
	cases := []struct {
		name     string
		capacity Capacity
	}{
		{"未注入容量", nil},
		{"statfs 失败", fakeCapacity{usage: store.DiskUsage{Available: false}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newMetricsServer(t, c.capacity)
			rec := serve(h, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("/readyz 应 200，实际 %d", rec.Code)
			}
			var out readyzResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
				t.Fatalf("响应不是合法 JSON: %v", err)
			}
			if out.Disk.Available {
				t.Error("无容量信息时 available 必须为 false")
			}
			if out.Disk.TotalBytes != 0 || out.Disk.FreeBytes != 0 || out.Disk.UsedRatio != 0 {
				t.Errorf("不可用时应回 0，实际 %+v", out.Disk)
			}
			// 水位是配置值，与容量是否可用无关，必须照常回显。
			if out.Disk.HighWatermark != 0.90 || out.Disk.LowWatermark != 0.85 {
				t.Errorf("水位未回显: %+v", out.Disk)
			}
		})
	}
}
