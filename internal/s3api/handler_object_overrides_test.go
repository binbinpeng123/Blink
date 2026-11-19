package s3api

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// objectWithHTTPMeta 写入一个带完整标准 HTTP 元数据的对象，返回 PUT 响应。
func objectWithHTTPMeta(t *testing.T, h http.Handler, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := putObject(t, h, target, body, func(r *http.Request) {
		r.Header.Set("Content-Type", "text/plain")
		r.Header.Set("Cache-Control", "max-age=60")
		r.Header.Set("Content-Disposition", "inline")
		r.Header.Set("Content-Encoding", "identity")
		r.Header.Set("Content-Language", "en")
		r.Header.Set("Expires", "Wed, 21 Oct 2015 07:28:00 GMT")
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("写入失败: %d (%s)", rec.Code, rec.Body.String())
	}
	return rec
}

// TestGetObjectResponseOverrides 覆盖 GET 的六个 response-* 参数：
// 它们只改变本次响应头，不改变响应体、Content-Length、ETag 或存储元数据。
func TestGetObjectResponseOverrides(t *testing.T) {
	h := objectTestServer(t)
	const body = "alpha,beta\n1,2\n"
	put := objectWithHTTPMeta(t, h, "/bkt/report.csv", body)

	q := url.Values{
		"response-cache-control":       {"no-store"},
		"response-content-disposition": {`attachment; filename="report.csv"`},
		"response-content-encoding":    {"gzip"},
		"response-content-language":    {"zh-CN"},
		"response-content-type":        {"text/csv; charset=utf-8"},
		"response-expires":             {"Thu, 01 Jan 2026 00:00:00 GMT"},
	}
	rec := objectRequest(t, h, http.MethodGet, "/bkt/report.csv?"+q.Encode(), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d (%s)", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != body {
		t.Errorf("响应体 = %q，期望 %q", got, body)
	}

	want := map[string]string{
		"Cache-Control":       "no-store",
		"Content-Disposition": `attachment; filename="report.csv"`,
		"Content-Encoding":    "gzip",
		"Content-Language":    "zh-CN",
		"Content-Type":        "text/csv; charset=utf-8",
		"Expires":             "Thu, 01 Jan 2026 00:00:00 GMT",
	}
	for header, value := range want {
		if got := rec.Header().Get(header); got != value {
			t.Errorf("%s = %q，期望 %q", header, got, value)
		}
	}
	if got := rec.Header().Get("Content-Length"); got != "15" {
		t.Errorf("Content-Length = %q，期望 15", got)
	}
	// 覆盖不能影响这些由对象内容/版本决定的头。
	if got, wantETag := rec.Header().Get("ETag"), put.Header().Get("ETag"); got != wantETag {
		t.Errorf("ETag = %q，期望 %q", got, wantETag)
	}
	if got := rec.Header().Get("x-amz-storage-class"); got != storageClassStandard {
		t.Errorf("x-amz-storage-class = %q", got)
	}

	// 覆盖是请求级的：不带参数再读一次必须回到对象自身元数据。
	rec = objectRequest(t, h, http.MethodGet, "/bkt/report.csv", nil)
	if got := rec.Header().Get("Content-Type"); got != "text/plain" {
		t.Errorf("覆盖污染了持久化元数据: Content-Type = %q", got)
	}
	if got := rec.Header().Get("Cache-Control"); got != "max-age=60" {
		t.Errorf("覆盖污染了持久化元数据: Cache-Control = %q", got)
	}
}

// TestGetObjectResponseOverridesPartial 固定 206 同样生效：
// 浏览器直链下载经常带 Range。
func TestGetObjectResponseOverridesPartial(t *testing.T) {
	h := objectTestServer(t)
	objectWithHTTPMeta(t, h, "/bkt/range.bin", "0123456789")

	rec := objectRequest(t, h, http.MethodGet,
		"/bkt/range.bin?response-content-type=application%2Foctet-stream&response-content-disposition=attachment",
		func(r *http.Request) { r.Header.Set("Range", "bytes=0-3") })
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("状态码 = %d (%s)", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != "0123" {
		t.Errorf("响应体 = %q", got)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Errorf("Content-Type = %q", got)
	}
	if got := rec.Header().Get("Content-Disposition"); got != "attachment" {
		t.Errorf("Content-Disposition = %q", got)
	}
}

// TestGetObjectResponseOverridesHeadIgnored HEAD 不应用覆盖（S3 只对 GET 定义）。
func TestGetObjectResponseOverridesHeadIgnored(t *testing.T) {
	h := objectTestServer(t)
	objectWithHTTPMeta(t, h, "/bkt/head.bin", "abc")

	rec := objectRequest(t, h, http.MethodHead,
		"/bkt/head.bin?response-content-type=application%2Fjson", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "text/plain" {
		t.Errorf("HEAD 不应应用 response-content-type：Content-Type = %q", got)
	}
}

// TestGetObjectResponseOverridesEmptyValue 空值视为未提供。
func TestGetObjectResponseOverridesEmptyValue(t *testing.T) {
	h := objectTestServer(t)
	objectWithHTTPMeta(t, h, "/bkt/empty.bin", "abc")

	rec := objectRequest(t, h, http.MethodGet,
		"/bkt/empty.bin?response-content-type=&response-cache-control=", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d (%s)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "text/plain" {
		t.Errorf("空值不应覆盖 Content-Type：%q", got)
	}
	if got := rec.Header().Get("Cache-Control"); got != "max-age=60" {
		t.Errorf("空值不应覆盖 Cache-Control：%q", got)
	}
}

// TestGetObjectResponseOverridesInvalidValue 控制字符必须 400：
// net/http 会静默把 CR/LF 换成空格，客户端会误以为覆盖按原值生效。
func TestGetObjectResponseOverridesInvalidValue(t *testing.T) {
	h := objectTestServer(t)
	objectWithHTTPMeta(t, h, "/bkt/evil.bin", "abc")

	evil := url.QueryEscape("text/plain\r\nX-Injected: 1")
	rec := objectRequest(t, h, http.MethodGet,
		"/bkt/evil.bin?response-content-type="+evil, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，期望 400（%s）", rec.Code, rec.Body.String())
	}
	if code := decodeError(t, rec).Code; code != "InvalidArgument" {
		t.Errorf("Code = %q，期望 InvalidArgument", code)
	}
	if got := rec.Header().Get("X-Injected"); got != "" {
		t.Errorf("注入头漏进了响应: %q", got)
	}

	// 参数错误优先于对象存在性：对象不存在也应是 400，而不是 404。
	rec = objectRequest(t, h, http.MethodGet,
		"/bkt/missing?response-cache-control="+url.QueryEscape("a\x00b"), nil)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("对象不存在时状态码 = %d，期望 400", rec.Code)
	}
}

// TestGetObjectResponseOverridesPartNumber 分片下载（partNumber → 206）
// 与应用覆盖走的是同一条响应头路径，这里钉住不能漏。
func TestGetObjectResponseOverridesPartNumber(t *testing.T) {
	h := mpuTestServer(t)
	const key = "big/part.bin"

	uploadID := createUploadHTTP(t, h, key, nil)
	p1 := mpuPayload(5<<20, 0x11)
	p2 := mpuPayload(1024, 0x22)
	rec := mpuRequest(t, h, http.MethodPut, "/bkt/"+key+"?partNumber=1&uploadId="+uploadID,
		bytes.NewReader(p1), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("UploadPart(1) = %d：%s", rec.Code, rec.Body.String())
	}
	etag1 := rec.Header().Get("ETag")
	rec = mpuRequest(t, h, http.MethodPut, "/bkt/"+key+"?partNumber=2&uploadId="+uploadID,
		bytes.NewReader(p2), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("UploadPart(2) = %d：%s", rec.Code, rec.Body.String())
	}
	etag2 := rec.Header().Get("ETag")
	completeXML := fmt.Sprintf(
		`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part>`+
			`<Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`,
		etag1, etag2)
	rec = mpuRequest(t, h, http.MethodPost, "/bkt/"+key+"?uploadId="+uploadID,
		strings.NewReader(completeXML), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("Complete = %d：%s", rec.Code, rec.Body.String())
	}

	rec = mpuRequest(t, h, http.MethodGet,
		"/bkt/"+key+"?partNumber=2&response-content-type=text%2Fplain", nil, nil)
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("分片下载 = %d，期望 206（%s）", rec.Code, rec.Body.String())
	}
	if !bytes.Equal(rec.Body.Bytes(), p2) {
		t.Error("分片下载内容与分片 2 不一致")
	}
	if got := rec.Header().Get("Content-Type"); got != "text/plain" {
		t.Errorf("Content-Type = %q，期望被覆盖", got)
	}
	if got := rec.Header().Get("x-amz-mp-parts-count"); got != "2" {
		t.Errorf("x-amz-mp-parts-count = %q，期望 2", got)
	}
}

// TestGetObjectResponseOverridesNotModified 304 只覆盖缓存判定头，
// 不覆盖 Content-Type 等实体头（304 没有实体）。
func TestGetObjectResponseOverridesNotModified(t *testing.T) {
	h := objectTestServer(t)
	put := objectWithHTTPMeta(t, h, "/bkt/cached.bin", "abc")
	etag := put.Header().Get("ETag")

	q := url.Values{
		"response-cache-control": {"no-store"},
		"response-expires":       {"Thu, 01 Jan 2026 00:00:00 GMT"},
		"response-content-type":  {"application/json"},
	}
	rec := objectRequest(t, h, http.MethodGet, "/bkt/cached.bin?"+q.Encode(),
		func(r *http.Request) { r.Header.Set("If-None-Match", etag) })
	if rec.Code != http.StatusNotModified {
		t.Fatalf("状态码 = %d，期望 304（%s）", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q，期望 no-store", got)
	}
	if got := rec.Header().Get("Expires"); !strings.Contains(got, "2026") {
		t.Errorf("Expires = %q，期望被覆盖", got)
	}
	if got := rec.Header().Get("ETag"); got != etag {
		t.Errorf("ETag = %q，期望 %q", got, etag)
	}
	if got := rec.Header().Get("Content-Type"); got != "" {
		t.Errorf("304 不应包含实体头 Content-Type：%q", got)
	}
}
