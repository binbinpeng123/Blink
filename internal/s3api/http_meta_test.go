package s3api

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// standardHTTPMeta 是五个标准 HTTP 元数据头的一组非空取值。
func standardHTTPMeta() map[string]string {
	return map[string]string{
		"Cache-Control":       "public, max-age=3600",
		"Content-Disposition": `attachment; filename="weights.bin"`,
		"Content-Encoding":    "gzip",
		"Content-Language":    "zh-CN",
		"Expires":             "Wed, 21 Oct 2026 07:28:00 GMT",
	}
}

// requireHeaders 断言响应里回传了期望的头。
func requireHeaders(t *testing.T, h http.Header, want map[string]string) {
	t.Helper()
	for k, v := range want {
		if got := h.Get(k); got != v {
			t.Errorf("%s = %q，期望 %q", k, got, v)
		}
	}
}

// 标准 HTTP 元数据必须在 PUT → GET/HEAD 之间原样往返。
func TestPutObjectHTTPMetaOverHTTP(t *testing.T) {
	h := objectTestServer(t)
	want := standardHTTPMeta()

	rec := putObject(t, h, "/bkt/meta.bin", "hello", func(r *http.Request) {
		for k, v := range want {
			r.Header.Set(k, v)
		}
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT 状态码 = %d (%s)", rec.Code, rec.Body.String())
	}
	etag := rec.Header().Get("ETag")

	get := httptest.NewRecorder()
	h.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/bkt/meta.bin", nil))
	if get.Code != http.StatusOK {
		t.Fatalf("GET 状态码 = %d", get.Code)
	}
	requireHeaders(t, get.Header(), want)

	head := httptest.NewRecorder()
	h.ServeHTTP(head, httptest.NewRequest(http.MethodHead, "/bkt/meta.bin", nil))
	if head.Code != http.StatusOK {
		t.Fatalf("HEAD 状态码 = %d", head.Code)
	}
	requireHeaders(t, head.Header(), want)

	// 304 至少要带影响缓存判定的两个头（RFC 7232）。
	notModified := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/bkt/meta.bin", nil)
	req.Header.Set("If-None-Match", etag)
	h.ServeHTTP(notModified, req)
	if notModified.Code != http.StatusNotModified {
		t.Fatalf("条件 GET 状态码 = %d，期望 304", notModified.Code)
	}
	if got := notModified.Header().Get("Cache-Control"); got != want["Cache-Control"] {
		t.Errorf("304 Cache-Control = %q，期望 %q", got, want["Cache-Control"])
	}
	if got := notModified.Header().Get("Expires"); got != want["Expires"] {
		t.Errorf("304 Expires = %q，期望 %q", got, want["Expires"])
	}
}

// aws-chunked 是传输层框架，解帧后不得作为对象的 Content-Encoding 持久化；
// 同一头里的其它编码（gzip）必须保留。
func TestPutObjectHTTPMetaStripsAWSChunked(t *testing.T) {
	h := objectTestServer(t)
	rec := putObject(t, h, "/bkt/chunked-gzip", "5\r\nhello\r\n0\r\n\r\n", func(r *http.Request) {
		r.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD")
		r.Header.Set("Content-Encoding", "aws-chunked, gzip")
		r.Header.Set("x-amz-decoded-content-length", "5")
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT 状态码 = %d (%s)", rec.Code, rec.Body.String())
	}

	get := httptest.NewRecorder()
	h.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/bkt/chunked-gzip", nil))
	if got := get.Header().Get("Content-Encoding"); got != "gzip" {
		t.Errorf("Content-Encoding = %q，期望 gzip", got)
	}
	if get.Body.String() != "hello" {
		t.Errorf("对象内容 = %q，期望 hello", get.Body.String())
	}
}

// COPY 保留、REPLACE 替换、REPLACE 缺省清空 —— 三条规则在 HTTP 层一致。
func TestCopyObjectHTTPMetaOverHTTP(t *testing.T) {
	h := objectTestServer(t)
	want := standardHTTPMeta()
	rec := putObject(t, h, "/bkt/src-meta", "payload", func(r *http.Request) {
		for k, v := range want {
			r.Header.Set(k, v)
		}
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("写源对象状态码 = %d (%s)", rec.Code, rec.Body.String())
	}

	t.Run("COPY 保留", func(t *testing.T) {
		mustCopy(t, h, "/bkt/copied-meta", "/bkt/src-meta", nil)
		get := httptest.NewRecorder()
		h.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/bkt/copied-meta", nil))
		requireHeaders(t, get.Header(), want)
	})

	t.Run("REPLACE 替换", func(t *testing.T) {
		replaced := map[string]string{
			"Cache-Control":    "no-store",
			"Content-Encoding": "br",
		}
		mustCopy(t, h, "/bkt/replaced-meta", "/bkt/src-meta", func(r *http.Request) {
			r.Header.Set("x-amz-metadata-directive", "REPLACE")
			for k, v := range replaced {
				r.Header.Set(k, v)
			}
		})
		get := httptest.NewRecorder()
		h.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/bkt/replaced-meta", nil))
		requireHeaders(t, get.Header(), replaced)
		// 源对象有、请求没给的头必须清空。
		for _, k := range []string{"Content-Disposition", "Content-Language", "Expires"} {
			if got := get.Header().Get(k); got != "" {
				t.Errorf("%s = %q，REPLACE 未提供时应清空", k, got)
			}
		}
	})
}

// Initiate 声明的标准 HTTP 元数据在 Complete 之后成为对象元数据。
func TestMultipartHTTPMetaOverHTTP(t *testing.T) {
	h := mpuTestServer(t)
	const key = "models/meta.bin"
	want := map[string]string{
		"Cache-Control":       "public, max-age=600",
		"Content-Disposition": `attachment; filename="meta.bin"`,
	}
	uploadID := createUploadHTTP(t, h, key, func(r *http.Request) {
		for k, v := range want {
			r.Header.Set(k, v)
		}
	})

	part := mpuPayload(5<<20, 0x44)
	rec := mpuRequest(t, h, http.MethodPut, "/bkt/"+key+"?partNumber=1&uploadId="+uploadID,
		bytes.NewReader(part), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("UploadPart = %d：%s", rec.Code, rec.Body.String())
	}
	etag := rec.Header().Get("ETag")

	completeXML := fmt.Sprintf(
		`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`,
		etag)
	rec = mpuRequest(t, h, http.MethodPost, "/bkt/"+key+"?uploadId="+uploadID,
		strings.NewReader(completeXML), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("Complete = %d：%s", rec.Code, rec.Body.String())
	}

	get := httptest.NewRecorder()
	h.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/bkt/"+key, nil))
	if get.Code != http.StatusOK {
		t.Fatalf("GET 状态码 = %d", get.Code)
	}
	requireHeaders(t, get.Header(), want)
}
