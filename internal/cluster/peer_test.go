package cluster

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testPeer(ts *httptest.Server) Peer {
	return Peer{ID: "n1", Addr: strings.TrimPrefix(ts.URL, "http://")}
}

func TestPeerClientHealth(t *testing.T) {
	_, ts := newTestServer(t)
	client := NewPeerClient(testPeer(ts), "n2", testSecret, time.Second)

	health, err := client.Health(context.Background())
	if err != nil {
		t.Fatalf("健康检查失败: %v", err)
	}
	if health.NodeID != "n1" || health.PeerCount != 3 {
		t.Errorf("health = %+v", health)
	}
	if got := client.Peer().ID; got != "n1" {
		t.Errorf("Peer() = %q", got)
	}
}

func TestPeerClientRejectsWrongSecret(t *testing.T) {
	_, ts := newTestServer(t)
	client := NewPeerClient(testPeer(ts), "n2", []byte(strings.Repeat("x", 32)), time.Second)

	_, err := client.Health(context.Background())
	if err == nil {
		t.Fatal("错误密钥应被拒绝")
	}
	var peerErr *PeerError
	if !errors.As(err, &peerErr) {
		t.Fatalf("错误类型 = %T，期望 *PeerError", err)
	}
	if peerErr.Status != http.StatusUnauthorized {
		t.Errorf("状态码 = %d，期望 401", peerErr.Status)
	}
	if strings.Contains(err.Error(), string(testSecret)) {
		t.Error("错误信息泄露了集群密钥")
	}
}

func TestPeerClientUnknownPathReturnsPeerError(t *testing.T) {
	_, ts := newTestServer(t)
	client := NewPeerClient(testPeer(ts), "n2", testSecret, time.Second)

	err := client.do(context.Background(), http.MethodGet, "/internal/v1/nope", nil, nil)
	var peerErr *PeerError
	if !errors.As(err, &peerErr) {
		t.Fatalf("错误类型 = %T，期望 *PeerError", err)
	}
	if peerErr.Status != http.StatusNotFound || peerErr.Peer != "n1" {
		t.Errorf("PeerError = %+v", peerErr)
	}
	if !strings.Contains(peerErr.Error(), "404") {
		t.Errorf("Error() = %q 未包含状态码", peerErr.Error())
	}
}

func TestPeerClientNetworkError(t *testing.T) {
	// 先占一个端口再释放，得到一个几乎可以确定无人监听的地址。
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	client := NewPeerClient(Peer{ID: "n9", Addr: addr}, "n1", testSecret, 500*time.Millisecond)
	_, err = client.Health(context.Background())
	if err == nil {
		t.Fatal("不可达节点应返回错误")
	}
	var peerErr *PeerError
	if errors.As(err, &peerErr) {
		t.Fatalf("网络错误不应被包装成 PeerError: %v", err)
	}
	if !strings.Contains(err.Error(), "n9") {
		t.Errorf("错误信息应包含节点 ID: %v", err)
	}
}

func TestPeerClientTimeout(t *testing.T) {
	block := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
	}))
	defer func() {
		close(block)
		ts.Close()
	}()

	client := NewPeerClient(testPeer(ts), "n2", testSecret, 50*time.Millisecond)
	start := time.Now()
	_, err := client.Health(context.Background())
	if err == nil {
		t.Fatal("超时应返回错误")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("超时耗时 %v，期望接近 50ms", elapsed)
	}
}

// 内部协议没有重定向语义：3xx 必须原样作为错误返回，而不是跟随。
func TestPeerClientDoesNotFollowRedirect(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 不签名，避免把重定向目标也变成一次有意为之的调用。
		http.Redirect(w, r, "/elsewhere", http.StatusTemporaryRedirect)
	}))
	defer ts.Close()

	client := NewPeerClient(testPeer(ts), "n2", testSecret, time.Second)
	err := client.do(context.Background(), http.MethodGet, "/internal/v1/health", nil, nil)
	var peerErr *PeerError
	if !errors.As(err, &peerErr) {
		t.Fatalf("错误类型 = %T，期望 *PeerError", err)
	}
	if peerErr.Status != http.StatusTemporaryRedirect {
		t.Errorf("状态码 = %d，期望 307（不跟随重定向）", peerErr.Status)
	}
}
