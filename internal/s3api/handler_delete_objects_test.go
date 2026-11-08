package s3api

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/binbinpeng123/Blink/internal/s3err"
)

// deleteObjectsBody 构造 Delete 请求体。
func deleteObjectsBody(keys ...string) string {
	var b strings.Builder
	b.WriteString("<Delete>")
	for _, k := range keys {
		b.WriteString("<Object><Key>" + k + "</Key></Object>")
	}
	b.WriteString("</Delete>")
	return b.String()
}

// quietDeleteObjectsBody 构造只含 Quiet=true 的请求体。
func quietDeleteObjectsBody(keys ...string) string {
	return strings.Replace(deleteObjectsBody(keys...), "</Delete>", "<Quiet>true</Quiet></Delete>", 1)
}

func doDeleteObjects(t *testing.T, h http.Handler, bucket, body string, mutate func(*http.Request)) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/"+bucket+"?delete", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/xml")
	if mutate != nil {
		mutate(req)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeDeleteResult(t *testing.T, rec *httptest.ResponseRecorder) deleteObjectsResult {
	t.Helper()
	var out deleteObjectsResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("响应不是合法 XML: %v (%s)", err, rec.Body.String())
	}
	return out
}

// TestDeleteObjectsOverHTTP 验证一次删除多条 key，且删除后不可读。
func TestDeleteObjectsOverHTTP(t *testing.T) {
	h := objectTestServer(t)
	keys := []string{"a.txt", "dir/b.txt", "模型.bin"}
	for _, k := range keys {
		putObject(t, h, "/bkt/"+escapedPath(k), "payload", nil)
	}

	rec := doDeleteObjects(t, h, "bkt", deleteObjectsBody(keys...), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}
	out := decodeDeleteResult(t, rec)
	if len(out.Deleted) != len(keys) {
		t.Fatalf("Deleted 条数 = %d，期望 %d (%+v)", len(out.Deleted), len(keys), out)
	}
	for i, k := range keys {
		if out.Deleted[i].Key != k {
			t.Errorf("第 %d 条 Deleted.Key = %q，期望 %q（必须保持请求顺序）", i, out.Deleted[i].Key, k)
		}
	}
	for _, k := range keys {
		rec = do(t, h, http.MethodGet, "/bkt/"+escapedPath(k))
		if rec.Code != http.StatusNotFound {
			t.Errorf("删除后 GET %q 状态码 = %d，期望 404", k, rec.Code)
		}
	}
}

// TestDeleteObjectsIdempotent 验证删除不存在的 key 也算成功。
//
// S3 的 DELETE 是幂等的：客户端重试一次批量删除不应该看到错误。
func TestDeleteObjectsIdempotent(t *testing.T) {
	h := objectTestServer(t)
	rec := doDeleteObjects(t, h, "bkt", deleteObjectsBody("never-existed"), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", rec.Code)
	}
	out := decodeDeleteResult(t, rec)
	if len(out.Deleted) != 1 || len(out.Errors) != 0 {
		t.Errorf("结果 = %+v，期望 1 条 Deleted 且无 Error", out)
	}
}

// TestDeleteObjectsQuiet 验证 Quiet 只回错误项。
func TestDeleteObjectsQuiet(t *testing.T) {
	h := objectTestServer(t)
	putObject(t, h, "/bkt/q1", "payload", nil)

	rec := doDeleteObjects(t, h, "bkt", quietDeleteObjectsBody("q1", "gone"), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", rec.Code)
	}
	out := decodeDeleteResult(t, rec)
	if len(out.Deleted) != 0 {
		t.Errorf("Quiet 模式仍返回了 Deleted: %+v", out.Deleted)
	}
	if len(out.Errors) != 0 {
		t.Errorf("Quiet 模式不应有错误: %+v", out.Errors)
	}
}

// TestDeleteObjectsPartialFailure 验证单条失败只出现在 Error 表里。
//
// 用超长 key 触发存储层校验失败：key 本身来自请求体，属于客户端可控输入，
// 且其他条目必须照常删除。
func TestDeleteObjectsPartialFailure(t *testing.T) {
	h := objectTestServer(t)
	putObject(t, h, "/bkt/good", "payload", nil)
	tooLong := strings.Repeat("k", 2048)

	rec := doDeleteObjects(t, h, "bkt", deleteObjectsBody("good", tooLong), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200 —— 部分成功不是请求级错误 (%s)", rec.Code, rec.Body.String())
	}
	out := decodeDeleteResult(t, rec)
	if len(out.Deleted) != 1 || out.Deleted[0].Key != "good" {
		t.Errorf("Deleted = %+v，期望只含 good", out.Deleted)
	}
	if len(out.Errors) != 1 || out.Errors[0].Key != tooLong {
		t.Fatalf("Errors = %+v，期望只含超长 key", out.Errors)
	}
	if out.Errors[0].Code != string(s3err.CodeInvalidArgument) {
		t.Errorf("单条错误码 = %q，期望 InvalidArgument", out.Errors[0].Code)
	}
}

// TestDeleteObjectsRequestValidation 覆盖请求级 4xx。
func TestDeleteObjectsRequestValidation(t *testing.T) {
	h := objectTestServer(t)

	keys := make([]string, 0, maxDeleteObjects+1)
	for i := 0; i <= maxDeleteObjects; i++ {
		keys = append(keys, fmt.Sprintf("k-%04d", i))
	}

	cases := []struct {
		name   string
		bucket string
		body   string
		want   int
		code   s3err.Code
	}{
		{"坏 XML", "bkt", "<not-xml", http.StatusBadRequest, s3err.CodeMalformedXML},
		{"没有 Object", "bkt", "<Delete></Delete>", http.StatusBadRequest, s3err.CodeMalformedXML},
		{"超过 1000 条", "bkt", deleteObjectsBody(keys...), http.StatusBadRequest, s3err.CodeMalformedXML},
		{"空 Key", "bkt", "<Delete><Object><Key></Key></Object></Delete>", http.StatusBadRequest, s3err.CodeMalformedXML},
		{"带 VersionId", "bkt", `<Delete><Object><Key>a</Key><VersionId>v1</VersionId></Object></Delete>`, http.StatusNotImplemented, s3err.CodeNotImplemented},
		{"桶不存在", "nope", deleteObjectsBody("a"), http.StatusNotFound, s3err.CodeNoSuchBucket},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := doDeleteObjects(t, h, c.bucket, c.body, nil)
			if rec.Code != c.want {
				t.Fatalf("状态码 = %d，期望 %d (%s)", rec.Code, c.want, rec.Body.String())
			}
			if got := decodeError(t, rec).Code; got != string(c.code) {
				t.Errorf("Code = %q，期望 %q", got, c.code)
			}
		})
	}
}

// TestDeleteObjectsContentMD5 验证可选的 Content-MD5 校验。
func TestDeleteObjectsContentMD5(t *testing.T) {
	h := objectTestServer(t)
	body := deleteObjectsBody("md5-key")
	sum := md5.Sum([]byte(body))
	valid := base64.StdEncoding.EncodeToString(sum[:])

	t.Run("正确", func(t *testing.T) {
		rec := doDeleteObjects(t, h, "bkt", body, func(r *http.Request) {
			r.Header.Set("Content-MD5", valid)
		})
		if rec.Code != http.StatusOK {
			t.Fatalf("状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
		}
	})

	t.Run("不匹配", func(t *testing.T) {
		wrong := md5.Sum([]byte("其它内容"))
		rec := doDeleteObjects(t, h, "bkt", body, func(r *http.Request) {
			r.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(wrong[:]))
		})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("状态码 = %d，期望 400", rec.Code)
		}
		if got := decodeError(t, rec).Code; got != string(s3err.CodeBadDigest) {
			t.Errorf("Code = %q，期望 BadDigest", got)
		}
	})

	t.Run("不是合法 base64", func(t *testing.T) {
		rec := doDeleteObjects(t, h, "bkt", body, func(r *http.Request) {
			r.Header.Set("Content-MD5", "!!!not-base64!!!")
		})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("状态码 = %d，期望 400", rec.Code)
		}
		if got := decodeError(t, rec).Code; got != string(s3err.CodeInvalidDigest) {
			t.Errorf("Code = %q，期望 InvalidDigest", got)
		}
	})
}

// TestDeleteObjectsBodyLimit 验证 1 MiB 请求体上限。
func TestDeleteObjectsBodyLimit(t *testing.T) {
	h := objectTestServer(t)
	// 用空白填充到上限之外：它既不是合法 XML，也超过体积限制，
	// 实现必须先按体积拒绝，避免把内存交给一个明显不成立的请求。
	body := strings.Repeat(" ", maxDeleteBody+1)

	rec := doDeleteObjects(t, h, "bkt", body, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，期望 400", rec.Code)
	}
	if got := decodeError(t, rec).Code; got != string(s3err.CodeMalformedXML) {
		t.Errorf("Code = %q，期望 MalformedXML", got)
	}
}
