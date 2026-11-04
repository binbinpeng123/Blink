package cluster

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newTestServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	srv, err := NewServer(validConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("构造内部服务失败: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return srv, ts
}

// doSigned 发送一个签名正确的请求；body 必须与实际发送的字节一致。
func doSigned(t *testing.T, method, url string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	SignRequest(req, testSecret, "n2", body, time.Now())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestHealthEndpoint(t *testing.T) {
	_, ts := newTestServer(t)
	resp := doSigned(t, http.MethodGet, ts.URL+"/internal/v1/health", nil)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", resp.StatusCode)
	}
	if got := resp.Header.Get(HeaderNode); got != "n1" {
		t.Errorf("响应头 %s = %q，期望 n1", HeaderNode, got)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}

	var health Health
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("解析响应失败: %v", err)
	}
	if health.NodeID != "n1" || health.PeerCount != 3 {
		t.Errorf("health = %+v", health)
	}
	if len(health.RingNodes) != 3 {
		t.Errorf("环节点 = %v，期望 3 个", health.RingNodes)
	}
}

func TestHealthRejectsNonGET(t *testing.T) {
	_, ts := newTestServer(t)
	resp := doSigned(t, http.MethodPost, ts.URL+"/internal/v1/health", []byte("{}"))
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("状态码 = %d，期望 405", resp.StatusCode)
	}
	if allow := resp.Header.Get("Allow"); allow != http.MethodGet {
		t.Errorf("Allow = %q", allow)
	}
}

func TestUnknownPathReturns404(t *testing.T) {
	_, ts := newTestServer(t)
	resp := doSigned(t, http.MethodGet, ts.URL+"/internal/v1/nope", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("状态码 = %d，期望 404", resp.StatusCode)
	}
	// 404 也必须在认证之后：未签名请求拿到的应是 401 而不是路径是否存在的信息。
	unsigned, err := http.Get(ts.URL + "/internal/v1/nope")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unsigned.Body.Close() }()
	if unsigned.StatusCode != http.StatusUnauthorized {
		t.Errorf("未签名请求状态码 = %d，期望 401", unsigned.StatusCode)
	}
}

func TestUnauthenticatedResponseHidesReason(t *testing.T) {
	_, ts := newTestServer(t)
	resp, err := http.Get(ts.URL + "/internal/v1/health")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("状态码 = %d，期望 401", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if bytes.Contains(body, []byte("timestamp")) || bytes.Contains(body, []byte("signature")) {
		t.Errorf("响应泄露了失败原因: %s", body)
	}
	if resp.Header.Get(HeaderNode) != "" {
		t.Error("未认证响应不应带节点标识")
	}
}

// 认证中间件必须把验签通过的节点 ID 与已读 body 交给业务处理器。
func TestAuthMiddlewarePassesNodeAndBody(t *testing.T) {
	srv, _ := newTestServer(t)

	var gotNode string
	var gotBody []byte
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotNode = NodeIDFromContext(r.Context())
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	})
	ts := httptest.NewServer(srv.withAuth(inner))
	defer ts.Close()

	payload := []byte("payload-bytes")
	resp := doSigned(t, http.MethodPost, ts.URL+"/internal/v1/echo", payload)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("状态码 = %d", resp.StatusCode)
	}
	if gotNode != "n2" {
		t.Errorf("context 中的节点 = %q，期望 n2", gotNode)
	}
	if !bytes.Equal(gotBody, payload) {
		t.Errorf("业务处理器读到的 body = %q，期望 %q", gotBody, payload)
	}
	if NodeIDFromContext(t.Context()) != "" {
		t.Error("未认证的 context 应返回空节点 ID")
	}
}

func TestNewServerRejectsInvalidConfig(t *testing.T) {
	cfg := validConfig()
	cfg.Secret = []byte("short")
	if _, err := NewServer(cfg, nil); err == nil {
		t.Fatal("非法配置应被拒绝")
	}
}
