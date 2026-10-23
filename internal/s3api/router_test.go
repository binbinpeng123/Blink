package s3api

import (
	"encoding/json"
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/binbinpeng123/Blink/internal/s3err"
)

// newTestServer 构造使用静默 logger 的测试服务器，避免测试输出被日志淹没。
//
// 这里**显式关闭鉴权**：本文件测的是路由分发与免签端点自身的行为，
// 与签名校验正交。鉴权行为由 auth_test.go 专门覆盖，两处互不干扰。
// 若不关闭，切片 2 之后所有 S3 路由都会先被 403 拦下，
// 这些测试将测不到 dispatch —— 那正是"把两个关注点混在一起"的后果。
func newTestServer() http.Handler {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(Options{Logger: logger, Region: "us-east-1", DisableAuth: true}).Handler()
}

func do(t *testing.T, h http.Handler, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHealthz(t *testing.T) {
	h := newTestServer()
	rec := do(t, h, http.MethodGet, "/healthz")

	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d, 期望 200", rec.Code)
	}
	var body healthzResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("响应体不是合法 JSON: %v", err)
	}
	if body.Status != "ok" {
		t.Errorf("status = %q, 期望 ok", body.Status)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type = %q", ct)
	}
}

func TestReadyz(t *testing.T) {
	h := newTestServer()
	rec := do(t, h, http.MethodGet, "/readyz")

	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d, 期望 200", rec.Code)
	}
	var body readyzResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("响应体不是合法 JSON: %v", err)
	}
	if !body.Ready {
		t.Error("ready 应为 true")
	}
}

func TestVersion(t *testing.T) {
	h := newTestServer()
	rec := do(t, h, http.MethodGet, "/version")

	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d, 期望 200", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("响应体不是合法 JSON: %v", err)
	}
	for _, k := range []string{"version", "commit", "build_time", "go"} {
		if body[k] == "" {
			t.Errorf("缺少字段 %s", k)
		}
	}
}

// TestHealthEndpointsRejectPost 验证非 S3 端点的方法校验生效。
func TestHealthEndpointsRejectPost(t *testing.T) {
	h := newTestServer()
	for _, path := range []string{"/healthz", "/readyz", "/version"} {
		rec := do(t, h, http.MethodPost, path)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("POST %s 状态码 = %d, 期望 405", path, rec.Code)
		}
	}
}

// TestHeadHasNoBody 验证 HEAD 语义：状态码与头正确，但不写响应体。
func TestHeadHasNoBody(t *testing.T) {
	h := newTestServer()
	rec := do(t, h, http.MethodHead, "/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d, 期望 200", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("HEAD 响应体应为空，实际有 %d 字节", rec.Body.Len())
	}
}

func TestRequestIDHeaderPresent(t *testing.T) {
	h := newTestServer()
	rec := do(t, h, http.MethodGet, "/healthz")

	reqID := rec.Header().Get(headerRequestID)
	if len(reqID) != 16 {
		t.Fatalf("请求 ID 长度 = %d (%q), 期望 16", len(reqID), reqID)
	}
	if strings.ToUpper(reqID) != reqID {
		t.Errorf("请求 ID 应为大写十六进制: %q", reqID)
	}
}

func TestRequestIDIsUnique(t *testing.T) {
	h := newTestServer()
	seen := make(map[string]bool, 100)
	for i := 0; i < 100; i++ {
		rec := do(t, h, http.MethodGet, "/healthz")
		id := rec.Header().Get(headerRequestID)
		if seen[id] {
			t.Fatalf("请求 ID 重复: %s", id)
		}
		seen[id] = true
	}
}

// TestS3RoutesReturnNotImplemented 是本切片的契约：尚未实现的 S3 操作
// 必须返回规范的 501 NotImplemented 错误体，而不是泛化 404。
//
// 这个清单会随切片推进而**缩短**，这是刻意的：既有的 501 断言会变成
// 新功能的回归测试（"它确实不再返回 501 了"），删掉它们反而丢失覆盖。
// 切片 3 已实现 GET / 与桶级 PUT/DELETE/HEAD，因此它们移出本表。
func TestS3RoutesReturnNotImplemented(t *testing.T) {
	h := newTestServer()
	cases := []struct {
		method string
		target string
	}{
		{http.MethodGet, "/my-bucket"},
		{http.MethodGet, "/my-bucket/object.txt"},
		{http.MethodGet, "/my-bucket/?list-type=2"},
		{http.MethodPost, "/my-bucket/key?uploads"},
	}
	for _, c := range cases {
		t.Run(c.method+" "+c.target, func(t *testing.T) {
			rec := do(t, h, c.method, c.target)
			if rec.Code != http.StatusNotImplemented {
				t.Fatalf("状态码 = %d, 期望 501", rec.Code)
			}

			var body struct {
				Code      string `xml:"Code"`
				RequestID string `xml:"RequestId"`
				HostID    string `xml:"HostId"`
			}
			if err := xml.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("错误体不是合法 XML: %v (%s)", err, rec.Body.String())
			}
			if body.Code != string(s3err.CodeNotImplemented) {
				t.Errorf("Code = %q, 期望 %q", body.Code, s3err.CodeNotImplemented)
			}
			if body.RequestID == "" {
				t.Error("错误体缺少 RequestId")
			}
			if body.RequestID != rec.Header().Get(headerRequestID) {
				t.Error("错误体中的 RequestId 与响应头不一致")
			}
			if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/xml") {
				t.Errorf("Content-Type = %q", ct)
			}
		})
	}
}

func TestSplitPath(t *testing.T) {
	cases := []struct {
		path       string
		wantBucket string
		wantKey    string
	}{
		{"/", "", ""},
		{"", "", ""},
		{"/bucket", "bucket", ""},
		{"/bucket/", "bucket", ""},
		{"/bucket/key", "bucket", "key"},
		{"/bucket/a/b/c.txt", "bucket", "a/b/c.txt"},
		{"/bucket//leading-slash-key", "bucket", "/leading-slash-key"},
	}
	for _, c := range cases {
		bucket, key := splitPath(c.path)
		if bucket != c.wantBucket || key != c.wantKey {
			t.Errorf("splitPath(%q) = (%q, %q), 期望 (%q, %q)",
				c.path, bucket, key, c.wantBucket, c.wantKey)
		}
	}
}

// TestPanicRecovered 验证 handler panic 不会导致连接断开，而是返回 500。
func TestPanicRecovered(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := recoverPanic(logger, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("状态码 = %d, 期望 500", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "InternalError") {
		t.Errorf("响应体应包含 InternalError: %s", rec.Body.String())
	}
}
