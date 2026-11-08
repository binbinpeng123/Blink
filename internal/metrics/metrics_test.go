package metrics

import (
	"strings"
	"sync"
	"testing"
)

func TestWriteTextFormat(t *testing.T) {
	r := NewRegistry()
	reqs := r.Counter("blink_http_requests_total", "HTTP 请求总数", "method", "status")
	reqs.Inc("GET", "200")
	reqs.Inc("GET", "200")
	reqs.Inc("PUT", "403")

	blocked := r.Gauge("blink_disk_write_blocked", "是否因水位阻断写入")
	blocked.SetBool(true)

	h := r.Histogram("blink_http_request_duration_seconds", "请求耗时", "method")
	h.Observe(0.0005, "GET")
	h.Observe(0.75, "GET")

	var b strings.Builder
	r.Write(&b)
	out := b.String()

	for _, want := range []string{
		"# TYPE blink_http_requests_total counter",
		`blink_http_requests_total{method="GET",status="200"} 2`,
		`blink_http_requests_total{method="PUT",status="403"} 1`,
		"# TYPE blink_disk_write_blocked gauge",
		"blink_disk_write_blocked 1",
		`blink_http_request_duration_seconds_bucket{method="GET",le="0.001"} 1`,
		`blink_http_request_duration_seconds_bucket{method="GET",le="1"} 2`,
		`blink_http_request_duration_seconds_bucket{method="GET",le="+Inf"} 2`,
		`blink_http_request_duration_seconds_count{method="GET"} 2`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("输出缺少 %q:\n%s", want, out)
		}
	}
}

func TestConcurrentUpdates(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("c", "help", "x")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				c.Inc("x")
			}
		}()
	}
	wg.Wait()

	var b strings.Builder
	r.Write(&b)
	if !strings.Contains(b.String(), `c{x="x"} 8000`) {
		t.Fatalf("并发计数丢失:\n%s", b.String())
	}
}

func TestLabelEscaping(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("c", "line1\nline2", "a")
	c.Inc(`a"b`)
	var b strings.Builder
	r.Write(&b)
	out := b.String()
	if !strings.Contains(out, `c{a="a\"b"} 1`) {
		t.Fatalf("标签转义不正确:\n%s", out)
	}
	if !strings.Contains(out, "# HELP c line1\\nline2") {
		t.Fatalf("帮助文本转义不正确:\n%s", out)
	}
}

func TestWrongLabelCountIgnored(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("c", "help", "a")
	c.Inc()
	c.Inc("x", "y")
	var b strings.Builder
	r.Write(&b)
	if strings.Contains(b.String(), `c{`) {
		t.Fatalf("标签数不匹配的更新应被忽略:\n%s", b.String())
	}
}
