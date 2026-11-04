package cluster

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

var testSecret = bytes.Repeat([]byte("k"), MinSecretLen)

func signedRequest(t *testing.T, method, path string, body []byte, node string, secret []byte, now time.Time) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	SignRequest(req, secret, node, body, now)
	return req
}

func TestSignAndVerifyRoundTrip(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	body := []byte(`{"hello":"world"}`)
	req := signedRequest(t, http.MethodPost, "/internal/v1/block?x=1", body, "n1", testSecret, now)

	// VerifyRequest 会消费 body，先记下原始内容用于比对回填语义。
	node, got, err := VerifyRequest(req, testSecret, func(id string) bool { return id == "n1" }, now)
	if err != nil {
		t.Fatalf("合法请求验签失败: %v", err)
	}
	if node != "n1" {
		t.Errorf("节点 = %q，期望 n1", node)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("返回的 body = %q，期望 %q", got, body)
	}
}

func TestVerifyRejectsTampering(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	body := []byte(`{"a":1}`)
	member := func(id string) bool { return id == "n1" }

	cases := []struct {
		name    string
		build   func() *http.Request
		members func(string) bool
		now     time.Time
	}{
		{
			name: "未签名",
			build: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/internal/v1/health", nil)
			},
			members: member, now: now,
		},
		{
			name: "错误密钥",
			build: func() *http.Request {
				return signedRequest(t, http.MethodPost, "/internal/v1/x", body, "n1", []byte(strings.Repeat("w", 32)), now)
			},
			members: member, now: now,
		},
		{
			name: "body 被篡改",
			build: func() *http.Request {
				req := signedRequest(t, http.MethodPost, "/internal/v1/x", body, "n1", testSecret, now)
				req.Body = io.NopCloser(bytes.NewReader([]byte(`{"a":2}`)))
				req.ContentLength = int64(len(`{"a":2}`))
				return req
			},
			members: member, now: now,
		},
		{
			name: "路径被篡改",
			build: func() *http.Request {
				req := signedRequest(t, http.MethodPost, "/internal/v1/x", body, "n1", testSecret, now)
				req.URL.Path = "/internal/v1/y"
				return req
			},
			members: member, now: now,
		},
		{
			name: "方法被篡改",
			build: func() *http.Request {
				req := signedRequest(t, http.MethodPost, "/internal/v1/x", body, "n1", testSecret, now)
				req.Method = http.MethodPut
				return req
			},
			members: member, now: now,
		},
		{
			name: "未知节点",
			build: func() *http.Request {
				return signedRequest(t, http.MethodPost, "/internal/v1/x", body, "intruder", testSecret, now)
			},
			members: member, now: now,
		},
		{
			name: "过期时间戳",
			build: func() *http.Request {
				return signedRequest(t, http.MethodPost, "/internal/v1/x", body, "n1", testSecret, now.Add(-AuthSkew-time.Second))
			},
			members: member, now: now,
		},
		{
			name: "未来时间戳",
			build: func() *http.Request {
				return signedRequest(t, http.MethodPost, "/internal/v1/x", body, "n1", testSecret, now.Add(AuthSkew+time.Second))
			},
			members: member, now: now,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := VerifyRequest(tc.build(), testSecret, tc.members, tc.now); err == nil {
				t.Fatal("期望验签失败，实际通过")
			} else if !errors.Is(err, ErrUnauthenticated) {
				t.Fatalf("错误类型 = %v，期望 ErrUnauthenticated", err)
			}
		})
	}
}

// 时间窗口边界必须包含端点：刚好 5 分钟的时钟偏移应被接受。
func TestVerifyAcceptsClockSkewBoundary(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	for _, delta := range []time.Duration{-AuthSkew, AuthSkew} {
		req := signedRequest(t, http.MethodGet, "/internal/v1/health", nil, "n1", testSecret, now.Add(delta))
		if _, _, err := VerifyRequest(req, testSecret, func(string) bool { return true }, now); err != nil {
			t.Errorf("偏移 %v 应被接受: %v", delta, err)
		}
	}
}

func TestVerifyRejectsOversizedBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/block", strings.NewReader("x"))
	// 直接构造一个超限的 Content-Length，避免测试真的分配 16 MiB。
	req.ContentLength = MaxRequestBody + 1
	req.Header.Set(HeaderNode, "n1")
	req.Header.Set(HeaderTime, "1700000000")
	req.Header.Set(HeaderSign, "deadbeef")

	_, _, err := VerifyRequest(req, testSecret, func(string) bool { return true }, time.Unix(1_700_000_000, 0))
	if !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("错误 = %v，期望 ErrBodyTooLarge", err)
	}
}

func TestVerifyRejectsBadNodeHeader(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/internal/v1/health", nil)
	req.Header.Set(HeaderNode, strings.Repeat("x", maxNodeIDLen+1))
	req.Header.Set(HeaderTime, "1700000000")
	req.Header.Set(HeaderSign, "deadbeef")
	if _, _, err := VerifyRequest(req, testSecret, func(string) bool { return true }, time.Unix(1_700_000_000, 0)); err == nil {
		t.Fatal("超长节点 ID 应被拒绝")
	}
}

// VerifyRequest 消费 r.Body 后把它作为返回值交出；中间件负责回填，
// 保证业务处理器看到的字节与验签时完全一致。
func TestVerifyReturnsConsumedBody(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	payload := []byte("payload")
	req := signedRequest(t, http.MethodPost, "/internal/v1/x", payload, "n1", testSecret, now)
	_, got, err := VerifyRequest(req, testSecret, func(string) bool { return true }, now)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("返回的 body = %q，期望 %q", got, payload)
	}
}
