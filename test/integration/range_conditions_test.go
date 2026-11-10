//go:build integration

package integration

import (
	"net/http"
	"testing"
	"time"
)

// TestRangeRequests 验证单区间 Range 的三种形式、越界语义与多区间拒绝。
func TestRangeRequests(t *testing.T) {
	srv := startServer(t, serverOptions{})
	c := newClient(t, srv)

	bucket := uniqueBucket("range")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)

	body := bodyOf(1000, 7)
	requireStatus(t, c.putObject(bucket, "r.bin", body, nil), http.StatusOK)

	cases := []struct {
		name        string
		rangeHeader string
		wantStatus  int
		wantStart   int
		wantEnd     int // 含端点，-1 表示不检查响应体
		wantCR      string
	}{
		{
			name:        "前 N 字节",
			rangeHeader: "bytes=0-9",
			wantStatus:  http.StatusPartialContent,
			wantStart:   0,
			wantEnd:     9,
			wantCR:      "bytes 0-9/1000",
		},
		{
			name:        "开放区间",
			rangeHeader: "bytes=990-",
			wantStatus:  http.StatusPartialContent,
			wantStart:   990,
			wantEnd:     999,
			wantCR:      "bytes 990-999/1000",
		},
		{
			name:        "后缀区间",
			rangeHeader: "bytes=-10",
			wantStatus:  http.StatusPartialContent,
			wantStart:   990,
			wantEnd:     999,
			wantCR:      "bytes 990-999/1000",
		},
		{
			name:        "终点越界按 RFC 截断",
			rangeHeader: "bytes=995-2000",
			wantStatus:  http.StatusPartialContent,
			wantStart:   995,
			wantEnd:     999,
			wantCR:      "bytes 995-999/1000",
		},
		{
			name:        "起点越界返回 416",
			rangeHeader: "bytes=1000-",
			wantStatus:  http.StatusRequestedRangeNotSatisfiable,
			wantEnd:     -1,
			wantCR:      "bytes */1000",
		},
		{
			name:        "多区间暂不支持",
			rangeHeader: "bytes=0-1,5-6",
			wantStatus:  http.StatusNotImplemented,
			wantEnd:     -1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := c.getObject(bucket, "r.bin", map[string]string{"Range": tc.rangeHeader})
			requireStatus(t, res, tc.wantStatus)
			if tc.wantCR != "" {
				if got := res.Header.Get("Content-Range"); got != tc.wantCR {
					t.Fatalf("Content-Range = %q，期望 %q", got, tc.wantCR)
				}
			}
			if tc.wantEnd >= 0 {
				want := body[tc.wantStart : tc.wantEnd+1]
				if string(res.Body) != string(want) {
					t.Fatalf("区间内容不一致：长度 %d，期望 %d", len(res.Body), len(want))
				}
			}
		})
	}
}

// TestConditionalRequests 验证 RFC 9110 的条件请求优先级与 304/412 语义。
func TestConditionalRequests(t *testing.T) {
	srv := startServer(t, serverOptions{})
	c := newClient(t, srv)

	bucket := uniqueBucket("cond")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)

	body := bodyOf(4096, 3)
	requireStatus(t, c.putObject(bucket, "c.bin", body, nil), http.StatusOK)

	base := c.headObject(bucket, "c.bin", nil)
	requireStatus(t, base, http.StatusOK)
	etag := unquoteETag(base.Header.Get("ETag"))
	lastModified := base.Header.Get("Last-Modified")
	if etag == "" || lastModified == "" {
		t.Fatalf("HEAD 缺少 ETag/Last-Modified: %v", base.Header)
	}
	lm, err := http.ParseTime(lastModified)
	if err != nil {
		t.Fatalf("解析 Last-Modified 失败: %v", err)
	}

	t.Run("If-None-Match 命中返回 304", func(t *testing.T) {
		res := c.getObject(bucket, "c.bin", map[string]string{
			"If-None-Match": `"` + etag + `"`,
		})
		requireStatus(t, res, http.StatusNotModified)
		if len(res.Body) != 0 {
			t.Fatalf("304 不应带响应体，得到 %d 字节", len(res.Body))
		}
		if unquoteETag(res.Header.Get("ETag")) != etag {
			t.Fatalf("304 缺少相同 ETag: %q", res.Header.Get("ETag"))
		}
		if res.Header.Get("Last-Modified") == "" {
			t.Fatalf("304 缺少 Last-Modified")
		}
	})

	t.Run("If-None-Match 未命中返回 200", func(t *testing.T) {
		res := c.getObject(bucket, "c.bin", map[string]string{
			"If-None-Match": `"00000000000000000000000000000000"`,
		})
		requireStatus(t, res, http.StatusOK)
		if string(res.Body) != string(body) {
			t.Fatalf("内容不一致")
		}
	})

	t.Run("If-Match 不匹配返回 412", func(t *testing.T) {
		res := c.getObject(bucket, "c.bin", map[string]string{
			"If-Match": `"deadbeef"`,
		})
		requireS3Error(t, res, http.StatusPreconditionFailed, "PreconditionFailed")
	})

	t.Run("If-Match 匹配返回 200", func(t *testing.T) {
		res := c.getObject(bucket, "c.bin", map[string]string{
			"If-Match": `"` + etag + `"`,
		})
		requireStatus(t, res, http.StatusOK)
	})

	t.Run("If-Modified-Since 为未来时间返回 304", func(t *testing.T) {
		future := lm.Add(time.Hour).UTC().Format(http.TimeFormat)
		res := c.getObject(bucket, "c.bin", map[string]string{
			"If-Modified-Since": future,
		})
		requireStatus(t, res, http.StatusNotModified)
	})

	t.Run("If-Unmodified-Since 为过去时间返回 412", func(t *testing.T) {
		past := lm.Add(-time.Hour).UTC().Format(http.TimeFormat)
		res := c.getObject(bucket, "c.bin", map[string]string{
			"If-Unmodified-Since": past,
		})
		requireS3Error(t, res, http.StatusPreconditionFailed, "PreconditionFailed")
	})

	t.Run("If-Match 存在时忽略 If-Unmodified-Since", func(t *testing.T) {
		past := lm.Add(-time.Hour).UTC().Format(http.TimeFormat)
		res := c.getObject(bucket, "c.bin", map[string]string{
			"If-Match":            `"` + etag + `"`,
			"If-Unmodified-Since": past,
		})
		requireStatus(t, res, http.StatusOK)
	})

	t.Run("If-Range 匹配时返回 206", func(t *testing.T) {
		res := c.getObject(bucket, "c.bin", map[string]string{
			"Range":    "bytes=0-3",
			"If-Range": `"` + etag + `"`,
		})
		requireStatus(t, res, http.StatusPartialContent)
		if len(res.Body) != 4 {
			t.Fatalf("206 响应体长度 = %d，期望 4", len(res.Body))
		}
	})

	t.Run("If-Range 不匹配时回退 200 全量", func(t *testing.T) {
		res := c.getObject(bucket, "c.bin", map[string]string{
			"Range":    "bytes=0-3",
			"If-Range": `"stale-etag"`,
		})
		requireStatus(t, res, http.StatusOK)
		if len(res.Body) != len(body) {
			t.Fatalf("回退响应长度 = %d，期望 %d", len(res.Body), len(body))
		}
	})
}
