package s3api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/binbinpeng123/Blink/internal/checksum"
)

// objectRequest 发一个带可选头改写的对象请求。
func objectRequest(t *testing.T, h http.Handler, method, target string, mutate func(*http.Request)) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	if mutate != nil {
		mutate(req)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestGetObject(t *testing.T) {
	h := objectTestServer(t)
	const body = "hello world"

	put := putObject(t, h, "/bkt/greeting.txt", body, func(r *http.Request) {
		r.Header.Set("Content-Type", "text/plain; charset=utf-8")
		r.Header.Set("x-amz-meta-Owner", "edge-01")
	})
	if put.Code != http.StatusOK {
		t.Fatalf("写入失败: %d (%s)", put.Code, put.Body.String())
	}

	rec := objectRequest(t, h, http.MethodGet, "/bkt/greeting.txt", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != body {
		t.Errorf("响应体 = %q，期望 %q", got, body)
	}

	hd := rec.Header()
	if got, want := hd.Get("Content-Length"), "11"; got != want {
		t.Errorf("Content-Length = %q，期望 %q", got, want)
	}
	if got := hd.Get("Content-Type"); got != "text/plain; charset=utf-8" {
		t.Errorf("Content-Type = %q", got)
	}
	if got, want := hd.Get("ETag"), put.Header().Get("ETag"); got != want {
		t.Errorf("ETag = %q，期望 %q", got, want)
	}
	if got, want := hd.Get("Last-Modified"), put.Header().Get("Last-Modified"); got != want {
		t.Errorf("Last-Modified = %q，期望 %q", got, want)
	}
	if got := hd.Get("Accept-Ranges"); got != "bytes" {
		t.Errorf("Accept-Ranges = %q", got)
	}
	if got := hd.Get("x-amz-storage-class"); got != "STANDARD" {
		t.Errorf("x-amz-storage-class = %q", got)
	}
	// 元数据头必须以全小写键写进 map：aws-cli 按原始大小写匹配
	// `x-amz-meta-` 前缀，规范化后的 X-Amz-Meta-Owner 会被它漏掉
	// （实测 --query Metadata.owner 返回 None）。因此这里直接查 map，
	// 并且 hd.Get 的规范化查找**不应**命中。
	if got := hd["x-amz-meta-owner"]; len(got) != 1 || got[0] != "edge-01" {
		t.Errorf("用户元数据未以小写头名回传: %q", got)
	}
	if got := hd.Get("x-amz-meta-Owner"); got != "" {
		t.Errorf("元数据头不应是规范化大写形式: %q", got)
	}
}

// 空对象必须显式 Content-Length: 0，而不是让 Go 改用 chunked。
func TestGetEmptyObject(t *testing.T) {
	h := objectTestServer(t)
	if rec := putObject(t, h, "/bkt/empty", "", nil); rec.Code != http.StatusOK {
		t.Fatalf("写入空对象失败: %d", rec.Code)
	}

	rec := objectRequest(t, h, http.MethodGet, "/bkt/empty", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("空对象响应体长度 = %d", rec.Body.Len())
	}
	if got := rec.Header().Get("Content-Length"); got != "0" {
		t.Errorf("Content-Length = %q，期望 0", got)
	}
	if te := rec.Header().Get("Transfer-Encoding"); te != "" {
		t.Errorf("不应发送 Transfer-Encoding，实际 %q", te)
	}
}

func TestHeadObject(t *testing.T) {
	h := objectTestServer(t)
	put := putObject(t, h, "/bkt/head", "hello world", nil)

	rec := objectRequest(t, h, http.MethodHead, "/bkt/head", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("HEAD 不应有响应体，实际 %q", rec.Body.String())
	}
	if got := rec.Header().Get("Content-Length"); got != "11" {
		t.Errorf("Content-Length = %q，期望 11", got)
	}
	if got := rec.Header().Get("ETag"); got != put.Header().Get("ETag") {
		t.Errorf("ETag = %q", got)
	}
	if got := rec.Header().Get("Accept-Ranges"); got != "bytes" {
		t.Errorf("Accept-Ranges = %q", got)
	}
}

func TestObjectNotFound(t *testing.T) {
	h := objectTestServer(t)

	// GET 带 XML 错误体。
	rec := objectRequest(t, h, http.MethodGet, "/bkt/missing", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET 状态码 = %d，期望 404", rec.Code)
	}
	if got := decodeError(t, rec).Code; got != "NoSuchKey" {
		t.Errorf("错误码 = %q，期望 NoSuchKey", got)
	}

	// HEAD 必须无响应体（02 文档）。
	rec = objectRequest(t, h, http.MethodHead, "/bkt/missing", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("HEAD 状态码 = %d，期望 404", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("HEAD 404 不应有响应体，实际 %q", rec.Body.String())
	}

	// 桶不存在时是 NoSuchBucket，不是 NoSuchKey。
	rec = objectRequest(t, h, http.MethodGet, "/nope/key", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("状态码 = %d，期望 404", rec.Code)
	}
	if got := decodeError(t, rec).Code; got != "NoSuchBucket" {
		t.Errorf("错误码 = %q，期望 NoSuchBucket", got)
	}
}

func TestGetObjectRange(t *testing.T) {
	h := objectTestServer(t)
	const body = "hello world"
	if rec := putObject(t, h, "/bkt/ranged", body, nil); rec.Code != http.StatusOK {
		t.Fatalf("写入失败: %d", rec.Code)
	}

	tests := []struct {
		name             string
		rangeHeader      string
		wantStatus       int
		wantBody         string
		wantContentRange string
	}{
		{"前五字节", "bytes=0-4", http.StatusPartialContent, "hello", "bytes 0-4/11"},
		{"从某位到末尾", "bytes=6-", http.StatusPartialContent, "world", "bytes 6-10/11"},
		{"后缀", "bytes=-5", http.StatusPartialContent, "world", "bytes 6-10/11"},
		{"终点越界被截断", "bytes=0-999", http.StatusPartialContent, body, "bytes 0-10/11"},
		{"后缀超长取整个对象", "bytes=-999", http.StatusPartialContent, body, "bytes 0-10/11"},
		{"起点越界", "bytes=100-200", http.StatusRequestedRangeNotSatisfiable, "", "bytes */11"},
		{"起点大于终点", "bytes=5-2", http.StatusRequestedRangeNotSatisfiable, "", "bytes */11"},
		{"非法格式", "bytes=abc", http.StatusRequestedRangeNotSatisfiable, "", "bytes */11"},
		{"后缀为零", "bytes=-0", http.StatusRequestedRangeNotSatisfiable, "", "bytes */11"},
		{"多区间", "bytes=0-1,4-5", http.StatusNotImplemented, "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := objectRequest(t, h, http.MethodGet, "/bkt/ranged", func(r *http.Request) {
				r.Header.Set("Range", tt.rangeHeader)
			})
			if rec.Code != tt.wantStatus {
				t.Fatalf("状态码 = %d，期望 %d (%s)", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if tt.wantStatus == http.StatusOK || tt.wantStatus == http.StatusPartialContent {
				if got := rec.Body.String(); got != tt.wantBody {
					t.Errorf("响应体 = %q，期望 %q", got, tt.wantBody)
				}
				if got, want := rec.Header().Get("Content-Length"), int64String(int64(len(tt.wantBody))); got != want {
					t.Errorf("Content-Length = %q，期望 %q", got, want)
				}
			} else if tt.wantStatus == http.StatusRequestedRangeNotSatisfiable {
				if got := decodeError(t, rec).Code; got != "InvalidRange" {
					t.Errorf("错误码 = %q，期望 InvalidRange", got)
				}
			} else if got := decodeError(t, rec).Code; got != "NotImplemented" {
				t.Errorf("错误码 = %q，期望 NotImplemented", got)
			}
			if got := rec.Header().Get("Content-Range"); got != tt.wantContentRange {
				t.Errorf("Content-Range = %q，期望 %q", got, tt.wantContentRange)
			}
		})
	}
}

// 空对象的任何 Range 都不可满足，且 Content-Range 必须给出 */0。
func TestRangeOnEmptyObject(t *testing.T) {
	h := objectTestServer(t)
	if rec := putObject(t, h, "/bkt/empty-range", "", nil); rec.Code != http.StatusOK {
		t.Fatalf("写入失败: %d", rec.Code)
	}
	rec := objectRequest(t, h, http.MethodGet, "/bkt/empty-range", func(r *http.Request) {
		r.Header.Set("Range", "bytes=0-0")
	})
	if rec.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("状态码 = %d，期望 416", rec.Code)
	}
	if got := rec.Header().Get("Content-Range"); got != "bytes */0" {
		t.Errorf("Content-Range = %q，期望 bytes */0", got)
	}
}

func TestConditionalGet(t *testing.T) {
	h := objectTestServer(t)
	put := putObject(t, h, "/bkt/cond", "hello world", nil)
	etag := put.Header().Get("ETag")
	lastModified := put.Header().Get("Last-Modified")
	modified, err := time.Parse(http.TimeFormat, lastModified)
	if err != nil {
		t.Fatalf("Last-Modified 不合法: %v", err)
	}
	older := modified.Add(-time.Hour).Format(http.TimeFormat)

	tests := []struct {
		name       string
		header     string
		value      string
		wantStatus int
	}{
		{"If-Match 命中", "If-Match", etag, http.StatusOK},
		{"If-Match 未命中", "If-Match", `"` + strings.Repeat("0", 32) + `"`, http.StatusPreconditionFailed},
		{"If-Match 星号", "If-Match", "*", http.StatusOK},
		{"If-None-Match 命中", "If-None-Match", etag, http.StatusNotModified},
		{"If-None-Match 星号", "If-None-Match", "*", http.StatusNotModified},
		{"If-None-Match 未命中", "If-None-Match", `"` + strings.Repeat("0", 32) + `"`, http.StatusOK},
		{"If-Modified-Since 未修改", "If-Modified-Since", lastModified, http.StatusNotModified},
		{"If-Modified-Since 已修改", "If-Modified-Since", older, http.StatusOK},
		{"If-Unmodified-Since 已修改", "If-Unmodified-Since", older, http.StatusPreconditionFailed},
		{"If-Unmodified-Since 未修改", "If-Unmodified-Since", lastModified, http.StatusOK},
		{"非法日期被忽略", "If-Modified-Since", "not-a-date", http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := objectRequest(t, h, http.MethodGet, "/bkt/cond", func(r *http.Request) {
				r.Header.Set(tt.header, tt.value)
			})
			if rec.Code != tt.wantStatus {
				t.Fatalf("状态码 = %d，期望 %d (%s)", rec.Code, tt.wantStatus, rec.Body.String())
			}
			switch tt.wantStatus {
			case http.StatusNotModified:
				if rec.Body.Len() != 0 {
					t.Errorf("304 不应有响应体，实际 %q", rec.Body.String())
				}
				if got := rec.Header().Get("ETag"); got != etag {
					t.Errorf("304 的 ETag = %q，期望 %q", got, etag)
				}
				if got := rec.Header().Get("Last-Modified"); got != lastModified {
					t.Errorf("304 的 Last-Modified = %q", got)
				}
			case http.StatusPreconditionFailed:
				if got := decodeError(t, rec).Code; got != "PreconditionFailed" {
					t.Errorf("错误码 = %q", got)
				}
			case http.StatusOK:
				if got := rec.Body.String(); got != "hello world" {
					t.Errorf("响应体 = %q", got)
				}
			}
		})
	}
}

// If-None-Match 的优先级高于 If-Modified-Since：前者未命中时，
// 即使后者说"未修改"也必须返回完整内容（RFC 9110）。
func TestIfNoneMatchOverridesIfModifiedSince(t *testing.T) {
	h := objectTestServer(t)
	put := putObject(t, h, "/bkt/prec", "body", nil)
	lastModified := put.Header().Get("Last-Modified")

	rec := objectRequest(t, h, http.MethodGet, "/bkt/prec", func(r *http.Request) {
		r.Header.Set("If-None-Match", `"`+strings.Repeat("a", 32)+`"`)
		r.Header.Set("If-Modified-Since", lastModified)
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200（If-None-Match 未命中应忽略日期条件）", rec.Code)
	}
}

func TestIfRangeFallsBackToFullObject(t *testing.T) {
	h := objectTestServer(t)
	put := putObject(t, h, "/bkt/ifrange", "hello world", nil)
	etag := put.Header().Get("ETag")

	// 标签不匹配：对象可能已变化，必须回退为完整响应。
	rec := objectRequest(t, h, http.MethodGet, "/bkt/ifrange", func(r *http.Request) {
		r.Header.Set("Range", "bytes=0-4")
		r.Header.Set("If-Range", `"`+strings.Repeat("b", 32)+`"`)
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", rec.Code)
	}
	if got := rec.Body.String(); got != "hello world" {
		t.Errorf("响应体 = %q，期望完整内容", got)
	}

	// 标签匹配：正常返回部分内容。
	rec = objectRequest(t, h, http.MethodGet, "/bkt/ifrange", func(r *http.Request) {
		r.Header.Set("Range", "bytes=0-4")
		r.Header.Set("If-Range", etag)
	})
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("状态码 = %d，期望 206", rec.Code)
	}
	if got := rec.Body.String(); got != "hello" {
		t.Errorf("响应体 = %q", got)
	}
}

func TestChecksumMode(t *testing.T) {
	h := objectTestServer(t)
	const body = "checksum me"

	put := putObject(t, h, "/bkt/ck", body, func(r *http.Request) {
		r.Header.Set("x-amz-sdk-checksum-algorithm", "CRC32C")
	})
	if put.Code != http.StatusOK {
		t.Fatalf("写入失败: %d", put.Code)
	}
	want := put.Header().Get("x-amz-checksum-crc32c")
	if want == "" {
		t.Fatal("写入响应缺少校验和")
	}

	// 未开启 ChecksumMode：不回传。
	rec := objectRequest(t, h, http.MethodGet, "/bkt/ck", nil)
	if got := rec.Header().Get("x-amz-checksum-crc32c"); got != "" {
		t.Errorf("未请求 ChecksumMode 却回传了校验和 %q", got)
	}

	// 开启后必须回传与写入时一致的算法和值。
	rec = objectRequest(t, h, http.MethodGet, "/bkt/ck", func(r *http.Request) {
		r.Header.Set("x-amz-checksum-mode", "ENABLED")
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d", rec.Code)
	}
	if got := rec.Header().Get("x-amz-checksum-crc32c"); got != want {
		t.Errorf("ChecksumMode 回传 %q，期望 %q", got, want)
	}

	// 部分响应不得回传整对象校验和：aws-cli 会拿它校验部分内容并报错。
	rec = objectRequest(t, h, http.MethodGet, "/bkt/ck", func(r *http.Request) {
		r.Header.Set("x-amz-checksum-mode", "ENABLED")
		r.Header.Set("Range", "bytes=0-3")
	})
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("状态码 = %d，期望 206", rec.Code)
	}
	if got := rec.Header().Get("x-amz-checksum-crc32c"); got != "" {
		t.Errorf("206 不应回传整对象校验和，实际 %q", got)
	}

	// 与独立计算的结果交叉验证，防止"存进去的就是错的"。
	sum, _ := checksum.New(checksum.AlgCRC32C)
	_, _ = sum.Write([]byte(body))
	if want != sum.Encoded() {
		t.Errorf("存储的校验和 %q 与独立计算 %q 不一致", want, sum.Encoded())
	}
}

func TestDeleteObjectOverHTTP(t *testing.T) {
	h := objectTestServer(t)
	if rec := putObject(t, h, "/bkt/del", "bye", nil); rec.Code != http.StatusOK {
		t.Fatalf("写入失败: %d", rec.Code)
	}

	rec := objectRequest(t, h, http.MethodDelete, "/bkt/del", nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("状态码 = %d，期望 204", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("204 不应有响应体，实际 %q", rec.Body.String())
	}

	// 删除后立即 404（read-after-delete）。
	rec = objectRequest(t, h, http.MethodGet, "/bkt/del", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("删除后 GET 状态码 = %d，期望 404", rec.Code)
	}

	// 幂等：再删一次仍 204。
	if rec := objectRequest(t, h, http.MethodDelete, "/bkt/del", nil); rec.Code != http.StatusNoContent {
		t.Errorf("重复删除状态码 = %d，期望 204", rec.Code)
	}

	// 桶不存在不幂等：404 NoSuchBucket。
	rec = objectRequest(t, h, http.MethodDelete, "/nope/del", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("状态码 = %d，期望 404", rec.Code)
	}
	if got := decodeError(t, rec).Code; got != "NoSuchBucket" {
		t.Errorf("错误码 = %q，期望 NoSuchBucket", got)
	}
}

// 对象级 GET 上的 multipart 子资源仍要明确 501，不能被读路径吞掉。
func TestGetObjectPartNumberStillNotImplemented(t *testing.T) {
	h := objectTestServer(t)
	rec := objectRequest(t, h, http.MethodGet, "/bkt/key?partNumber=1", nil)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("状态码 = %d，期望 501", rec.Code)
	}
}

// int64String 避免为几处断言引入 strconv。
func int64String(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
