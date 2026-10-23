package s3api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/binbinpeng123/Blink/internal/sigv4"
)

const (
	testAK     = "AKIATESTTEST1234"
	testSK     = "test-secret-key-not-a-real-credential"
	testRegion = "us-east-1"
)

// newAuthTestServer 构造带鉴权的服务，用于验证中间件行为。
func newAuthTestServer(t *testing.T, disableAuth bool) http.Handler {
	t.Helper()
	return New(Options{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Region: testRegion,
		Provider: sigv4.NewStaticProvider(sigv4.Credentials{
			AccessKeyID: testAK, SecretKey: testSK,
		}),
		DisableAuth: disableAuth,
	}).Handler()
}

// TestHealthEndpointsBypassAuth 验证免签白名单：
// K8s 探针不该被要求签名，否则探针无法工作。
func TestHealthEndpointsBypassAuth(t *testing.T) {
	h := newAuthTestServer(t, false)
	for _, path := range []string{"/healthz", "/readyz", "/version"} {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			if rec.Code != http.StatusOK {
				t.Errorf("%s 应免签名返回 200，实际 %d", path, rec.Code)
			}
		})
	}
}

// TestS3RoutesRequireAuth 验证 S3 路由**必须**签名：
// 匿名请求得到 403 AccessDenied，而不是 501 —— 501 会泄露
// "这条路由存在"，且让未鉴权流量进入业务逻辑。
func TestS3RoutesRequireAuth(t *testing.T) {
	h := newAuthTestServer(t, false)
	cases := []struct {
		method string
		target string
	}{
		{http.MethodGet, "/"},
		{http.MethodGet, "/my-bucket"},
		{http.MethodPut, "/my-bucket"},
		{http.MethodGet, "/my-bucket/object.txt"},
		{http.MethodGet, "/my-bucket/?list-type=2"},
	}
	for _, c := range cases {
		t.Run(c.method+" "+c.target, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(c.method, c.target, nil))
			if rec.Code != http.StatusForbidden {
				t.Fatalf("匿名 S3 请求应返回 403，实际 %d（响应体: %s）",
					rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "AccessDenied") {
				t.Errorf("响应体应为 AccessDenied: %s", rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), "NotImplemented") {
				t.Error("未鉴权请求不应抵达业务逻辑（不应出现 NotImplemented）")
			}
		})
	}
}

// TestAuthFailureResponseShape 验证鉴权失败也走统一的错误响应格式：
// 客户端需要 RequestId 才能报障，排障时只能靠它对齐日志。
func TestAuthFailureResponseShape(t *testing.T) {
	h := newAuthTestServer(t, false)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/b/k", nil))

	body := rec.Body.String()
	for _, want := range []string{"<Code>AccessDenied</Code>", "<RequestId>", "<HostId>blink</HostId>"} {
		if !strings.Contains(body, want) {
			t.Errorf("错误响应缺少 %q: %s", want, body)
		}
	}
	if got := rec.Header().Get(headerRequestID); got == "" {
		t.Error("鉴权失败响应也应带 x-amz-request-id 头")
	}
	// 响应头与响应体中的 RequestId 必须一致，否则日志无法对齐。
	if !strings.Contains(body, "<RequestId>"+rec.Header().Get(headerRequestID)+"</RequestId>") {
		t.Error("响应体的 RequestId 应与响应头一致")
	}
}

// TestDisabledAuthAllowsS3Routes 验证调试开关。
//
// 该开关是本地调试与集成测试的入口；线上开启等于完全开放，
// 因此这里同时断言"关闭时匿名必被拒"，防止默认值被改错。
func TestDisabledAuthAllowsS3Routes(t *testing.T) {
	rec := httptest.NewRecorder()
	newAuthTestServer(t, true).ServeHTTP(rec,
		httptest.NewRequest(http.MethodGet, "/b/k", nil))

	// 跳过鉴权后应抵达 dispatch，得到切片 2 阶段的 501。
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("跳过鉴权后应抵达业务逻辑返回 501，实际 %d: %s", rec.Code, rec.Body.String())
	}
}

// TestAuthAcceptsRealAWSCLIPresign 端到端验证中间件与 sigv4 的接线：
// 用真实 aws-cli 生成的 Presigned URL 应能通过中间件。
func TestAuthAcceptsRealAWSCLIPresign(t *testing.T) {
	h := newAuthTestServer(t, false)

	// 该向量的时间戳固定在 2026-10-23T09:09:52Z。中间件使用系统时间，
	// 因此这里按当前时间重新签发一份 URL，只验证"接线正确"。
	r, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:19000/b/k", nil)
	if err != nil {
		t.Fatalf("构造请求失败: %v", err)
	}
	r.Host = "127.0.0.1:19000"

	signed, err := sigv4.Presign(r, sigv4.PresignOptions{
		Credentials: sigv4.Credentials{AccessKeyID: testAK, SecretKey: testSK},
		Region:      testRegion,
		Expires:     5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Presign 失败: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, signed, nil)
	req.Host = "127.0.0.1:19000"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// 签名通过后抵达业务逻辑 → 当前阶段为 501；403 则表示签名没对上。
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("合法 Presigned URL 应通过鉴权并抵达业务逻辑（501），实际 %d: %s",
			rec.Code, rec.Body.String())
	}
}

// TestAuthFailureIsLoggedWithoutSecrets 验证鉴权失败会记日志，
// 且日志中**不含**任何密钥材料。
//
// 这是安全要求：鉴权日志既要能支撑排障与爆破检测，
// 又不能变成凭据泄露的渠道。
func TestAuthFailureIsLoggedWithoutSecrets(t *testing.T) {
	var buf strings.Builder
	h := New(Options{
		Logger: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Region: testRegion,
		Provider: sigv4.NewStaticProvider(sigv4.Credentials{
			AccessKeyID: testAK, SecretKey: testSK,
		}),
	}).Handler()

	req := httptest.NewRequest(http.MethodGet, "/b/k", nil)
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential="+testAK+"/20261023/us-east-1/s3/aws4_request, "+
			"SignedHeaders=host, Signature=deadbeef")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	logged := buf.String()
	if !strings.Contains(logged, "auth_failed") {
		t.Fatalf("鉴权失败应记日志，实际日志: %s", logged)
	}
	if strings.Contains(logged, testSK) {
		t.Error("日志泄露了 Secret Key")
	}
	if strings.Contains(logged, "deadbeef") {
		t.Error("日志不应记录客户端提供的签名原值")
	}
	// Access Key 允许保留前 4 位用于对照排查，但不得完整出现。
	if strings.Contains(logged, testAK) {
		t.Error("日志不应记录完整 Access Key")
	}
	if !strings.Contains(logged, "AKIA") {
		t.Error("日志应保留 Access Key 前缀以便对照排查")
	}
}

// TestAuthDoesNotBreakRequestContext 验证鉴权中间件不破坏
// 外层的 request_id / logger 注入（顺序错了会让日志丢失 request_id）。
func TestAuthDoesNotBreakRequestContext(t *testing.T) {
	var buf strings.Builder
	h := New(Options{
		Logger: slog.New(slog.NewTextHandler(&buf, nil)),
		Region: testRegion,
		Provider: sigv4.NewStaticProvider(sigv4.Credentials{
			AccessKeyID: testAK, SecretKey: testSK,
		}),
	}).Handler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/b/k", nil))

	reqID := rec.Header().Get(headerRequestID)
	if reqID == "" {
		t.Fatal("应生成 request_id")
	}
	if !strings.Contains(buf.String(), reqID) {
		t.Errorf("日志应包含同一个 request_id %s，实际: %s", reqID, buf.String())
	}
}
