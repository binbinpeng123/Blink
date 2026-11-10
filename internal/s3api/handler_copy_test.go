package s3api

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/binbinpeng123/Blink/internal/s3err"
)

// TestParseCopySource 覆盖头的解析边界。
//
// 重点有两个：`%2F` 必须在切分之后才解码（否则 key 里的斜杠会被误当成
// bucket 分隔符），以及带 versionId 的请求必须明确拒绝而不是静默忽略。
func TestParseCopySource(t *testing.T) {
	cases := []struct {
		name       string
		in         string
		wantBucket string
		wantKey    string
		wantCode   s3err.Code
	}{
		{name: "标准形式", in: "/bkt/key.txt", wantBucket: "bkt", wantKey: "key.txt"},
		{name: "不带前导斜杠", in: "bkt/key.txt", wantBucket: "bkt", wantKey: "key.txt"},
		{name: "key 含多级路径", in: "/bkt/a/b/c.txt", wantBucket: "bkt", wantKey: "a/b/c.txt"},
		{name: "编码的斜杠保留在 key 内", in: "/bkt/a%2Fb.txt", wantBucket: "bkt", wantKey: "a/b.txt"},
		{name: "空格与中文", in: "/bkt/%E6%A8%A1%E5%9E%8B%20v1.bin", wantBucket: "bkt", wantKey: "模型 v1.bin"},
		{name: "首尾空白被忽略", in: "  /bkt/key  ", wantBucket: "bkt", wantKey: "key"},
		{name: "空值", in: "", wantCode: s3err.CodeInvalidArgument},
		{name: "只有桶", in: "/bkt", wantCode: s3err.CodeInvalidArgument},
		{name: "桶为空", in: "//key", wantCode: s3err.CodeInvalidArgument},
		{name: "key 为空", in: "/bkt/", wantCode: s3err.CodeInvalidArgument},
		{name: "非法百分号编码", in: "/bkt/%zz", wantCode: s3err.CodeInvalidArgument},
		{name: "versionId 不支持", in: "/bkt/key?versionId=abc", wantCode: s3err.CodeNotImplemented},
		{name: "查询串非法", in: "/bkt/key?%zz=1", wantCode: s3err.CodeInvalidArgument},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, serr := parseCopySource(c.in)
			if c.wantCode != "" {
				if serr == nil {
					t.Fatalf("期望错误 %s，实际解析成功: %+v", c.wantCode, got)
				}
				if serr.Code != c.wantCode {
					t.Errorf("Code = %q，期望 %q", serr.Code, c.wantCode)
				}
				return
			}
			if serr != nil {
				t.Fatalf("解析失败: %v", serr)
			}
			if got.Bucket != c.wantBucket || got.Key != c.wantKey {
				t.Errorf("解析结果 = (%q, %q)，期望 (%q, %q)",
					got.Bucket, got.Key, c.wantBucket, c.wantKey)
			}
		})
	}
}

// doCopy 发一个带 x-amz-copy-source 的 PUT。
func doCopy(t *testing.T, h http.Handler, dst, source string, mutate func(*http.Request)) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, dst, nil)
	req.Header.Set("x-amz-copy-source", source)
	if mutate != nil {
		mutate(req)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func mustCopy(t *testing.T, h http.Handler, dst, source string, mutate func(*http.Request)) {
	t.Helper()
	rec := doCopy(t, h, dst, source, mutate)
	if rec.Code != http.StatusOK {
		t.Fatalf("复制状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}
}

func TestCopyObjectOverHTTP(t *testing.T) {
	h := objectTestServer(t)
	const body = "copy me, please"
	rec := putObject(t, h, "/bkt/source.txt", body, func(r *http.Request) {
		r.Header.Set("Content-Type", "text/plain")
		r.Header.Set("x-amz-meta-owner", "edge-01")
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("写源对象状态码 = %d (%s)", rec.Code, rec.Body.String())
	}

	rec = doCopy(t, h, "/bkt/copied.txt", "/bkt/source.txt", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("复制状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}
	var result copyObjectResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("响应不是合法 XML: %v (%s)", err, rec.Body.String())
	}
	if want := `"` + md5Hex(body) + `"`; result.ETag != want {
		t.Errorf("响应 ETag = %s，期望 %s", result.ETag, want)
	}
	if result.LastModified == "" {
		t.Error("LastModified 不应为空")
	}
	if _, err := time.Parse("2006-01-02T15:04:05.000Z", result.LastModified); err != nil {
		t.Errorf("LastModified 格式不合规: %q", result.LastModified)
	}

	// 内容是逐字节一致的副本，元数据按 COPY 指令原样保留。
	rec = do(t, h, http.MethodGet, "/bkt/copied.txt")
	if rec.Code != http.StatusOK {
		t.Fatalf("读取副本状态码 = %d", rec.Code)
	}
	if rec.Body.String() != body {
		t.Errorf("副本内容 = %q，期望 %q", rec.Body.String(), body)
	}
	if got := rec.Header().Get("Content-Type"); got != "text/plain" {
		t.Errorf("副本 Content-Type = %q，期望 text/plain", got)
	}
	// 读取路径按 S3 要求以小写头名回传用户元数据，直接查 map 才能看到。
	if got := rec.Header()["x-amz-meta-owner"]; len(got) != 1 || got[0] != "edge-01" {
		t.Errorf("副本 x-amz-meta-owner = %v，期望 [edge-01]", got)
	}
}

// TestCopyObjectReplaceMetadata 验证 REPLACE 指令下的元数据覆盖。
func TestCopyObjectReplaceMetadata(t *testing.T) {
	h := objectTestServer(t)
	putObject(t, h, "/bkt/src", "payload", func(r *http.Request) {
		r.Header.Set("Content-Type", "image/png")
		r.Header.Set("x-amz-meta-owner", "edge-01")
	})

	rec := doCopy(t, h, "/bkt/replaced", "/bkt/src", func(r *http.Request) {
		r.Header.Set("x-amz-metadata-directive", "REPLACE")
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("x-amz-meta-owner", "edge-02")
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("REPLACE 状态码 = %d (%s)", rec.Code, rec.Body.String())
	}

	rec = do(t, h, http.MethodGet, "/bkt/replaced")
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q，期望 application/json", got)
	}
	if got := rec.Header()["x-amz-meta-owner"]; len(got) != 1 || got[0] != "edge-02" {
		t.Errorf("x-amz-meta-owner = %v，期望 [edge-02]", got)
	}
}

// TestCopyObjectSelfCopy 验证"复制到自身"：COPY 拒绝，REPLACE 允许。
func TestCopyObjectSelfCopy(t *testing.T) {
	h := objectTestServer(t)
	putObject(t, h, "/bkt/self", "payload", nil)

	rec := doCopy(t, h, "/bkt/self", "/bkt/self", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("自复制 COPY 状态码 = %d，期望 400 (%s)", rec.Code, rec.Body.String())
	}
	e := decodeError(t, rec)
	if e.Code != string(s3err.CodeInvalidRequest) {
		t.Errorf("Code = %q，期望 InvalidRequest", e.Code)
	}
	// 消息里不能带 store 层的内部前缀（"store: 请求语义不合法:"）。
	if strings.Contains(e.Message, "store:") {
		t.Errorf("Message = %q，泄露了内部错误文案", e.Message)
	}
	if !strings.Contains(e.Message, "源与目标相同") {
		t.Errorf("Message = %q，缺少具体原因", e.Message)
	}

	rec = doCopy(t, h, "/bkt/self", "/bkt/self", func(r *http.Request) {
		r.Header.Set("x-amz-metadata-directive", "REPLACE")
		r.Header.Set("Content-Type", "text/csv")
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("自复制 REPLACE 状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}
	rec = do(t, h, http.MethodGet, "/bkt/self")
	if rec.Body.String() != "payload" {
		t.Errorf("自复制 REPLACE 后内容 = %q，期望 payload", rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "text/csv" {
		t.Errorf("Content-Type = %q，期望 text/csv", got)
	}
}

// TestCopyObjectSourceConditions 覆盖四个 x-amz-copy-source-if-* 条件。
func TestCopyObjectSourceConditions(t *testing.T) {
	h := objectTestServer(t)
	const body = "conditional source"
	putObject(t, h, "/bkt/src", body, nil)
	etag := `"` + md5Hex(body) + `"`
	past := time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)
	future := time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)

	cases := []struct {
		name   string
		header string
		value  string
		want   int
	}{
		{"If-Match 命中", "x-amz-copy-source-if-match", etag, http.StatusOK},
		{"If-Match 未命中", "x-amz-copy-source-if-match", `"deadbeef"`, http.StatusPreconditionFailed},
		{"If-None-Match 命中", "x-amz-copy-source-if-none-match", etag, http.StatusPreconditionFailed},
		{"If-None-Match 未命中", "x-amz-copy-source-if-none-match", `"deadbeef"`, http.StatusOK},
		{"If-Unmodified-Since 之后修改过", "x-amz-copy-source-if-unmodified-since", past, http.StatusPreconditionFailed},
		{"If-Unmodified-Since 之后未修改", "x-amz-copy-source-if-unmodified-since", future, http.StatusOK},
		{"If-Modified-Since 之后修改过", "x-amz-copy-source-if-modified-since", past, http.StatusOK},
		{"If-Modified-Since 之后未修改", "x-amz-copy-source-if-modified-since", future, http.StatusPreconditionFailed},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dst := "/bkt/cond-" + string(rune('a'+i))
			rec := doCopy(t, h, dst, "/bkt/src", func(r *http.Request) {
				r.Header.Set(c.header, c.value)
			})
			if rec.Code != c.want {
				t.Fatalf("状态码 = %d，期望 %d (%s)", rec.Code, c.want, rec.Body.String())
			}
			if rec.Code == http.StatusPreconditionFailed {
				if got := decodeError(t, rec).Code; got != string(s3err.CodePreconditionFailed) {
					t.Errorf("Code = %q，期望 PreconditionFailed", got)
				}
			}
		})
	}
}

// TestCopyObjectSourceConditionOnMissingKey 验证带条件的复制在源不存在时
// 仍然先报 404：条件是对"已观察到的源版本"求值，没有源就无从谈起。
func TestCopyObjectSourceConditionOnMissingKey(t *testing.T) {
	h := objectTestServer(t)

	rec := doCopy(t, h, "/bkt/dst", "/bkt/missing", func(r *http.Request) {
		r.Header.Set("x-amz-copy-source-if-match", `"deadbeef"`)
	})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("状态码 = %d，期望 404 (%s)", rec.Code, rec.Body.String())
	}
	if got := decodeError(t, rec).Code; got != string(s3err.CodeNoSuchKey) {
		t.Errorf("Code = %q，期望 NoSuchKey", got)
	}
}

// TestCopyObjectDestinationCondition 验证目标上的 If-None-Match: *。
func TestCopyObjectDestinationCondition(t *testing.T) {
	h := objectTestServer(t)
	putObject(t, h, "/bkt/src", "payload", nil)
	putObject(t, h, "/bkt/dst", "already here", nil)

	rec := doCopy(t, h, "/bkt/dst", "/bkt/src", func(r *http.Request) {
		r.Header.Set("If-None-Match", "*")
	})
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("状态码 = %d，期望 412 (%s)", rec.Code, rec.Body.String())
	}
	if got := decodeError(t, rec).Code; got != string(s3err.CodePreconditionFailed) {
		t.Errorf("Code = %q，期望 PreconditionFailed", got)
	}

	// 目标不存在时必须放行。
	mustCopy(t, h, "/bkt/fresh", "/bkt/src", func(r *http.Request) {
		r.Header.Set("If-None-Match", "*")
	})
}

// TestCopyObjectRejectsUnsupportedOptions 验证本版本明确不支持的复制选项。
//
// 每个都必须是 501/400 而不是"忽略参数照常复制"：静默忽略会让客户端
// 以为加密、标签或部分复制生效了，而对象里根本没有这些属性。
func TestCopyObjectRejectsUnsupportedOptions(t *testing.T) {
	h := objectTestServer(t)
	putObject(t, h, "/bkt/src", "payload", nil)

	cases := []struct {
		name   string
		header string
		value  string
		want   int
		code   s3err.Code
	}{
		{"分片复制", "x-amz-copy-source-range", "bytes=0-2", http.StatusNotImplemented, s3err.CodeNotImplemented},
		{"非 STANDARD 存储类型", "x-amz-storage-class", "GLACIER", http.StatusNotImplemented, s3err.CodeNotImplemented},
		{"服务端加密", "x-amz-server-side-encryption", "AES256", http.StatusNotImplemented, s3err.CodeNotImplemented},
		{"客户密钥加密", "x-amz-server-side-encryption-customer-algorithm", "AES256", http.StatusNotImplemented, s3err.CodeNotImplemented},
		{"KMS 密钥", "x-amz-server-side-encryption-aws-kms-key-id", "key-id", http.StatusNotImplemented, s3err.CodeNotImplemented},
		{"对象标签", "x-amz-tagging", "a=b", http.StatusNotImplemented, s3err.CodeNotImplemented},
		{"非法元数据指令", "x-amz-metadata-directive", "MERGE", http.StatusBadRequest, s3err.CodeInvalidArgument},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dst := "/bkt/reject-" + string(rune('a'+i))
			rec := doCopy(t, h, dst, "/bkt/src", func(r *http.Request) {
				r.Header.Set(c.header, c.value)
			})
			if rec.Code != c.want {
				t.Fatalf("状态码 = %d，期望 %d (%s)", rec.Code, c.want, rec.Body.String())
			}
			if got := decodeError(t, rec).Code; got != string(c.code) {
				t.Errorf("Code = %q，期望 %q", got, c.code)
			}
		})
	}

	// STANDARD 是唯一被接受的存储类型。
	mustCopy(t, h, "/bkt/standard", "/bkt/src", func(r *http.Request) {
		r.Header.Set("x-amz-storage-class", "STANDARD")
	})
}

// TestCopyObjectMissingBucket 验证源桶不存在时的错误码。
func TestCopyObjectMissingBucket(t *testing.T) {
	h := objectTestServer(t)

	rec := doCopy(t, h, "/bkt/dst", "/nope/key", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("状态码 = %d，期望 404 (%s)", rec.Code, rec.Body.String())
	}
	if got := decodeError(t, rec).Code; got != string(s3err.CodeNoSuchBucket) {
		t.Errorf("Code = %q，期望 NoSuchBucket", got)
	}
}

// TestCopyObjectDestinationIfMatch 验证 COPY 目标上的 If-Match 与 PUT 同语义：
// 目标 ETag 匹配才覆盖，否则 412；`*` 只要求目标存在。
func TestCopyObjectDestinationIfMatch(t *testing.T) {
	h := objectTestServer(t)
	putObject(t, h, "/bkt/src", "payload", nil)
	dst := putObject(t, h, "/bkt/dst", "old", nil)
	if dst.Code != http.StatusOK {
		t.Fatalf("目标初始化失败: %d (%s)", dst.Code, dst.Body.String())
	}
	etag := dst.Header().Get("ETag")

	rec := doCopy(t, h, "/bkt/dst", "/bkt/src", func(r *http.Request) {
		r.Header.Set("If-Match", etag)
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("ETag 匹配时状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}

	// 覆盖之后旧 ETag 不再成立。
	rec = doCopy(t, h, "/bkt/dst", "/bkt/src", func(r *http.Request) {
		r.Header.Set("If-Match", etag)
	})
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("旧 ETag 状态码 = %d，期望 412 (%s)", rec.Code, rec.Body.String())
	}
	if got := decodeError(t, rec).Code; got != string(s3err.CodePreconditionFailed) {
		t.Errorf("Code = %q，期望 PreconditionFailed", got)
	}

	if rec := doCopy(t, h, "/bkt/dst", "/bkt/src", func(r *http.Request) {
		r.Header.Set("If-Match", "*")
	}); rec.Code != http.StatusOK {
		t.Fatalf("If-Match:* 状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}

	rec = doCopy(t, h, "/bkt/fresh", "/bkt/src", func(r *http.Request) {
		r.Header.Set("If-Match", "*")
	})
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("目标不存在时状态码 = %d，期望 412 (%s)", rec.Code, rec.Body.String())
	}
}
