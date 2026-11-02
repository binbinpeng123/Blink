package s3api

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// copyResult 是 CopyObject 成功响应的公共字段。
type copyResult struct {
	ETag         string `xml:"ETag"`
	LastModified string `xml:"LastModified"`
}

func decodeCopyResult(t *testing.T, rec *httptest.ResponseRecorder) copyResult {
	t.Helper()
	var body copyResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("响应体不是合法 XML: %v (%s)", err, rec.Body.String())
	}
	return body
}

// copyObject 发一个 CopyObject 请求；请求体必须为空 ——
// 这正是它最容易被误当成普通 PUT 的地方。
func copyObject(t *testing.T, h http.Handler, target, source string, mutate func(*http.Request)) *httptest.ResponseRecorder {
	t.Helper()
	return putObject(t, h, target, "", func(r *http.Request) {
		r.Header.Set("x-amz-copy-source", source)
		if mutate != nil {
			mutate(r)
		}
	})
}

func TestCopyObject(t *testing.T) {
	h := objectTestServer(t)
	const body = "hello world"

	put := putObject(t, h, "/bkt/src.txt", body, func(r *http.Request) {
		r.Header.Set("Content-Type", "text/plain; charset=utf-8")
		r.Header.Set("x-amz-meta-Owner", "edge-01")
	})
	if put.Code != http.StatusOK {
		t.Fatalf("写入源对象失败: %d (%s)", put.Code, put.Body.String())
	}

	rec := copyObject(t, h, "/bkt/dst.txt", "/bkt/src.txt", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}

	res := decodeCopyResult(t, rec)
	// 同内容复制必须得到同一个 ETag：客户端（如 minio-go）依赖它判断
	// 复制是否真的发生了。
	if got, want := res.ETag, put.Header().Get("ETag"); got != want {
		t.Errorf("CopyObjectResult.ETag = %q，期望 %q", got, want)
	}
	if res.LastModified == "" {
		t.Error("CopyObjectResult.LastModified 为空")
	}

	// 目标对象的内容与元数据都必须与源一致（默认 COPY 语义）。
	got := objectRequest(t, h, http.MethodGet, "/bkt/dst.txt", nil)
	if got.Code != http.StatusOK {
		t.Fatalf("GET 目标对象状态码 = %d", got.Code)
	}
	if got.Body.String() != body {
		t.Errorf("目标内容 = %q，期望 %q", got.Body.String(), body)
	}
	if ct := got.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Errorf("目标 Content-Type = %q，期望沿用源", ct)
	}
	if meta := got.Header()["x-amz-meta-owner"]; len(meta) != 1 || meta[0] != "edge-01" {
		t.Errorf("目标用户元数据 = %q，期望沿用源", meta)
	}

	// 源对象必须原样保留：CopyObject 不是 MoveObject。
	src := objectRequest(t, h, http.MethodGet, "/bkt/src.txt", nil)
	if src.Code != http.StatusOK || src.Body.String() != body {
		t.Errorf("源对象被改动: 状态 %d 内容 %q", src.Code, src.Body.String())
	}
}

// 复制到已存在的键是覆盖语义，不能因为目标存在而失败或静默保留旧内容。
func TestCopyObjectOverwritesDestination(t *testing.T) {
	h := objectTestServer(t)
	if rec := putObject(t, h, "/bkt/src", "new-content", nil); rec.Code != http.StatusOK {
		t.Fatalf("写入源对象失败: %d", rec.Code)
	}
	if rec := putObject(t, h, "/bkt/dst", "old-content-that-is-longer", nil); rec.Code != http.StatusOK {
		t.Fatalf("写入目标对象失败: %d", rec.Code)
	}

	rec := copyObject(t, h, "/bkt/dst", "/bkt/src", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}

	got := objectRequest(t, h, http.MethodGet, "/bkt/dst", nil)
	if got.Body.String() != "new-content" {
		t.Errorf("目标内容 = %q，期望被覆盖为 %q", got.Body.String(), "new-content")
	}
}

func TestCopyObjectAcrossBuckets(t *testing.T) {
	h := objectTestServer(t)
	if rec := do(t, h, http.MethodPut, "/dst-bucket"); rec.Code != http.StatusOK {
		t.Fatalf("建目标桶失败: %d (%s)", rec.Code, rec.Body.String())
	}
	if rec := putObject(t, h, "/bkt/src", "cross-bucket", nil); rec.Code != http.StatusOK {
		t.Fatalf("写入源对象失败: %d", rec.Code)
	}

	rec := copyObject(t, h, "/dst-bucket/copied", "/bkt/src", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}

	got := objectRequest(t, h, http.MethodGet, "/dst-bucket/copied", nil)
	if got.Code != http.StatusOK || got.Body.String() != "cross-bucket" {
		t.Errorf("跨桶复制结果: 状态 %d 内容 %q", got.Code, got.Body.String())
	}
}

// x-amz-copy-source 的 key 是 URL 编码的；解码必须整体进行，
// 否则 key 里的 %2F 会被误当成层级分隔符。
func TestCopyObjectEncodedSourceKey(t *testing.T) {
	h := objectTestServer(t)
	// 请求行里的空格必须编码，否则 httptest 会把它当成 HTTP 版本分隔符。
	const srcKey = "/bkt/dir/hello%20world.txt"
	if rec := putObject(t, h, srcKey, "encoded", nil); rec.Code != http.StatusOK {
		t.Fatalf("写入源对象失败: %d (%s)", rec.Code, rec.Body.String())
	}

	cases := []struct {
		name   string
		source string
	}{
		// minio-go 的写法：前导斜杠 + 路径段编码。
		{"带前导斜杠", "/bkt/" + url.PathEscape("dir/hello world.txt")},
		// 部分客户端省略前导斜杠。
		{"无前导斜杠", "bkt/" + url.PathEscape("dir/hello world.txt")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			target := "/bkt/" + url.PathEscape("copied "+c.name)
			rec := copyObject(t, h, target, c.source, nil)
			if rec.Code != http.StatusOK {
				t.Fatalf("状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
			}
			got := objectRequest(t, h, http.MethodGet, target, nil)
			if got.Body.String() != "encoded" {
				t.Errorf("目标内容 = %q，期望 %q", got.Body.String(), "encoded")
			}
		})
	}
}

// REPLACE 语义要求用请求头里的 Content-Type 与用户元数据覆盖源对象的，
// 而不是二者合并。
func TestCopyObjectMetadataDirectiveReplace(t *testing.T) {
	h := objectTestServer(t)
	if rec := putObject(t, h, "/bkt/src", "payload", func(r *http.Request) {
		r.Header.Set("Content-Type", "text/plain")
		r.Header.Set("x-amz-meta-Owner", "edge-01")
	}); rec.Code != http.StatusOK {
		t.Fatalf("写入源对象失败: %d", rec.Code)
	}

	rec := copyObject(t, h, "/bkt/dst", "/bkt/src", func(r *http.Request) {
		r.Header.Set("x-amz-metadata-directive", "REPLACE")
		r.Header.Set("Content-Type", "application/octet-stream")
		r.Header.Set("x-amz-meta-Stage", "final")
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}

	got := objectRequest(t, h, http.MethodGet, "/bkt/dst", nil)
	if got.Body.String() != "payload" {
		t.Errorf("目标内容 = %q，期望 %q", got.Body.String(), "payload")
	}
	if ct := got.Header().Get("Content-Type"); ct != "application/octet-stream" {
		t.Errorf("Content-Type = %q，期望被 REPLACE 覆盖", ct)
	}
	hd := got.Header()
	if v := hd["x-amz-meta-stage"]; len(v) != 1 || v[0] != "final" {
		t.Errorf("新元数据缺失: %q", v)
	}
	// REPLACE 是替换而不是合并：源对象的 Owner 不应残留。
	if v := hd["x-amz-meta-owner"]; len(v) != 0 {
		t.Errorf("REPLACE 后源元数据残留: %q", v)
	}
}

// 未实现的条件复制与标签复制必须明确 501：
// 静默忽略会让调用方误以为条件生效。
func TestCopyObjectRejectsUnimplementedDirectives(t *testing.T) {
	h := objectTestServer(t)
	if rec := putObject(t, h, "/bkt/src", "x", nil); rec.Code != http.StatusOK {
		t.Fatalf("写入源对象失败: %d", rec.Code)
	}

	cases := []struct {
		header string
		value  string
	}{
		{"x-amz-copy-source-if-match", `"some-etag"`},
		{"x-amz-copy-source-if-none-match", `"some-etag"`},
		{"x-amz-copy-source-if-modified-since", "Wed, 21 Oct 2015 07:28:00 GMT"},
		{"x-amz-copy-source-if-unmodified-since", "Wed, 21 Oct 2015 07:28:00 GMT"},
		{"x-amz-tagging-directive", "REPLACE"},
	}
	for _, c := range cases {
		t.Run(c.header, func(t *testing.T) {
			rec := copyObject(t, h, "/bkt/dst-"+c.header, "/bkt/src", func(r *http.Request) {
				r.Header.Set(c.header, c.value)
			})
			if rec.Code != http.StatusNotImplemented {
				t.Fatalf("状态码 = %d，期望 501 (%s)", rec.Code, rec.Body.String())
			}
			if got := decodeError(t, rec).Code; got != "NotImplemented" {
				t.Errorf("错误码 = %q，期望 NotImplemented", got)
			}
		})
	}

	// COPY 是默认值，显式发送也必须接受。
	rec := copyObject(t, h, "/bkt/dst-tag-copy", "/bkt/src", func(r *http.Request) {
		r.Header.Set("x-amz-tagging-directive", "COPY")
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("x-amz-tagging-directive: COPY 被拒: %d (%s)", rec.Code, rec.Body.String())
	}
}

func TestCopyObjectInvalidMetadataDirective(t *testing.T) {
	h := objectTestServer(t)
	if rec := putObject(t, h, "/bkt/src", "x", nil); rec.Code != http.StatusOK {
		t.Fatalf("写入源对象失败: %d", rec.Code)
	}

	rec := copyObject(t, h, "/bkt/dst", "/bkt/src", func(r *http.Request) {
		r.Header.Set("x-amz-metadata-directive", "MERGE")
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，期望 400 (%s)", rec.Code, rec.Body.String())
	}
	if got := decodeError(t, rec).Code; got != "InvalidArgument" {
		t.Errorf("错误码 = %q，期望 InvalidArgument", got)
	}
}

func TestCopyObjectMalformedSource(t *testing.T) {
	h := objectTestServer(t)
	cases := map[string]string{
		"只有桶名":  "/bkt",
		"只有斜杠":  "/",
		"空 key": "/bkt/",
		"空桶名":   "/key",
		"非法转义":  "/bkt/%zz",
		"非法百分号": "/bkt/a%2",
	}
	for name, source := range cases {
		t.Run(name, func(t *testing.T) {
			rec := copyObject(t, h, "/bkt/dst", source, nil)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("状态码 = %d，期望 400 (%s)", rec.Code, rec.Body.String())
			}
			if got := decodeError(t, rec).Code; got != "InvalidArgument" {
				t.Errorf("错误码 = %q，期望 InvalidArgument", got)
			}
		})
	}
}

func TestCopyObjectNoSuchSource(t *testing.T) {
	h := objectTestServer(t)
	rec := copyObject(t, h, "/bkt/dst", "/bkt/missing", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("状态码 = %d，期望 404 (%s)", rec.Code, rec.Body.String())
	}
	if got := decodeError(t, rec).Code; got != "NoSuchKey" {
		t.Errorf("错误码 = %q，期望 NoSuchKey", got)
	}

	// 目标键不能用空对象在失败路径上被创建出来。
	if got := objectRequest(t, h, http.MethodHead, "/bkt/dst", nil); got.Code != http.StatusNotFound {
		t.Errorf("失败的复制留下了目标对象: 状态 %d", got.Code)
	}
}

func TestCopyObjectNoSuchDestinationBucket(t *testing.T) {
	h := objectTestServer(t)
	if rec := putObject(t, h, "/bkt/src", "x", nil); rec.Code != http.StatusOK {
		t.Fatalf("写入源对象失败: %d", rec.Code)
	}

	rec := copyObject(t, h, "/missing-bucket/dst", "/bkt/src", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("状态码 = %d，期望 404 (%s)", rec.Code, rec.Body.String())
	}
	if got := decodeError(t, rec).Code; got != "NoSuchBucket" {
		t.Errorf("错误码 = %q，期望 NoSuchBucket", got)
	}
}
