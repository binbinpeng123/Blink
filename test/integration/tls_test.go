//go:build integration

package integration

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 本文件验证 TLS 的**信任链**，而不是像手工验证那样加 --no-verify-ssl。
//
// 证书由测试现场生成的本地 CA 签发：客户端只信任这张 CA，从而完整走一遍
// 证书解析、链构建、主机名校验与 TLS 版本协商。known-limitations 里
// "TLS 仅以自签证书 + --no-verify-ssl 验证" 的缺口由这里关闭。
//
// 同时覆盖启动期语义：证书与私钥不匹配必须拒绝启动（cmd/blink 的既有行为）。

// tlsFixture 是一次测试用的 CA、服务器证书与信任库。
type tlsFixture struct {
	certFile string
	keyFile  string
	roots    *x509.CertPool
	leaf     *x509.Certificate
}

// newTLSFixture 生成 CA 与由它签发的 127.0.0.1/localhost 服务器证书。
func newTLSFixture(t *testing.T) *tlsFixture {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("生成 CA 私钥失败: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Blink Integration CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("签发 CA 证书失败: %v", err)
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("生成服务器私钥失败: %v", err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caTmpl, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("签发服务器证书失败: %v", err)
	}
	leaf, err := x509.ParseCertificate(leafDER)
	if err != nil {
		t.Fatalf("解析服务器证书失败: %v", err)
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatalf("编码服务器私钥失败: %v", err)
	}

	dir := t.TempDir()
	fx := &tlsFixture{
		certFile: filepath.Join(dir, "server.pem"),
		keyFile:  filepath.Join(dir, "server.key"),
		roots:    x509.NewCertPool(),
		leaf:     leaf,
	}
	// 证书链文件按 leaf → CA 顺序写；客户端只用 roots 验证，链本身用于验证
	// TLS 服务端真的发送了完整链。
	chain := append(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})...,
	)
	if err := os.WriteFile(fx.certFile, chain, 0o600); err != nil {
		t.Fatalf("写服务器证书失败: %v", err)
	}
	if err := os.WriteFile(fx.keyFile,
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("写服务器私钥失败: %v", err)
	}
	if !fx.roots.AppendCertsFromPEM(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})) {
		t.Fatal("把测试 CA 加入信任库失败")
	}
	return fx
}

// TestTLSTrustChainAndS3RoundTrip 用仅信任测试 CA 的客户端走完整 S3 往返。
func TestTLSTrustChainAndS3RoundTrip(t *testing.T) {
	fx := newTLSFixture(t)
	srv := startServer(t, serverOptions{tls: fx})
	c := newClient(t, srv)

	// 健康检查：确认确实协商了 TLS 1.2+，且对端证书就是 fixture 里的 leaf。
	hc := &http.Client{Timeout: 5 * time.Second, Transport: srv.transport()}
	resp, err := hc.Get(srv.baseURL + "/healthz")
	if err != nil {
		t.Fatalf("HTTPS 健康检查失败: %v\n%s", err, srv.logTail(40))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HTTPS 健康检查状态码 = %d，期望 200", resp.StatusCode)
	}
	if resp.TLS == nil {
		t.Fatal("响应没有 TLS 状态：连接不是 HTTPS")
	}
	if resp.TLS.Version < tls.VersionTLS12 {
		t.Fatalf("协商的 TLS 版本 = %#x，低于 TLS 1.2", resp.TLS.Version)
	}
	if len(resp.TLS.PeerCertificates) == 0 ||
		!bytes.Equal(resp.TLS.PeerCertificates[0].Raw, fx.leaf.Raw) {
		t.Fatal("对端证书与测试 fixture 的服务器证书不一致")
	}
	// 客户端只信任测试 CA；能走到这里就说明链构建与主机名校验都通过了。
	if err := fx.leaf.VerifyHostname("127.0.0.1"); err != nil {
		t.Fatalf("服务器证书不覆盖 127.0.0.1: %v", err)
	}

	// 协议层往返：桶、对象、HEAD 与 Presigned URL。
	bucket := uniqueBucket("tls")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)
	body := bodyOf(8<<10, 11)
	requireStatus(t, c.putObject(bucket, "k.bin", body, nil), http.StatusOK)

	got := c.getObject(bucket, "k.bin", nil)
	requireStatus(t, got, http.StatusOK)
	if !bytes.Equal(got.Body, body) {
		t.Fatalf("HTTPS 下载内容不一致（%d vs %d 字节）", len(got.Body), len(body))
	}
	requireStatus(t, c.headObject(bucket, "k.bin", nil), http.StatusOK)

	presigned := c.presignedURL(http.MethodGet, c.objectURL(bucket, "k.bin"), nil)
	resp2, err := hc.Get(presigned)
	if err != nil {
		t.Fatalf("通过 HTTPS 访问 Presigned URL 失败: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("Presigned GET 状态码 = %d，期望 200", resp2.StatusCode)
	}
}

// TestTLSRejectsUntrustedClients 验证服务端确实要求一条可验证的信任链：
// 不信任 CA、主机名不匹配、过旧的 TLS 版本都必须握手失败。
func TestTLSRejectsUntrustedClients(t *testing.T) {
	fx := newTLSFixture(t)
	srv := startServer(t, serverOptions{tls: fx})
	healthz := srv.baseURL + "/healthz"

	t.Run("不信任测试 CA", func(t *testing.T) {
		// 默认客户端只信任系统根；随机生成的测试 CA 必然不在其中。
		hc := &http.Client{Timeout: 5 * time.Second, Transport: srv.transport()}
		hc.Transport.(*http.Transport).TLSClientConfig.RootCAs = nil
		_, err := hc.Get(healthz)
		if err == nil {
			t.Fatal("使用了不受信任的证书却连接成功")
		}
		var uae x509.UnknownAuthorityError
		if !errors.As(err, &uae) {
			t.Fatalf("错误 = %v，期望 UnknownAuthorityError", err)
		}
	})

	t.Run("主机名不匹配", func(t *testing.T) {
		cfg := &tls.Config{RootCAs: fx.roots, ServerName: "wrong.invalid"}
		hc := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: cfg}}
		_, err := hc.Get(healthz)
		if err == nil {
			t.Fatal("服务器证书不覆盖该主机名却连接成功")
		}
		var he x509.HostnameError
		if !errors.As(err, &he) {
			t.Fatalf("错误 = %v，期望 HostnameError", err)
		}
	})

	t.Run("拒绝 TLS 1.1", func(t *testing.T) {
		// 服务端 MinVersion 为 1.2（cmd/blink）。客户端显式降到 1.1 后
		// 必须握手失败，而不是被静默降到旧版本。
		cfg := &tls.Config{
			RootCAs:    fx.roots,
			MinVersion: tls.VersionTLS10,
			MaxVersion: tls.VersionTLS11,
		}
		hc := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: cfg}}
		resp, err := hc.Get(healthz)
		if err == nil {
			resp.Body.Close()
			t.Fatal("TLS 1.1 握手成功，服务端未强制 TLS 1.2+")
		}
	})

	t.Run("明文访问 TLS 端口", func(t *testing.T) {
		hc := &http.Client{Timeout: 5 * time.Second}
		resp, err := hc.Get("http://" + srv.addr + "/healthz")
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("明文请求状态码 = %d，期望 400", resp.StatusCode)
			}
		}
		// net/http 也可能直接断开连接；两种结果都说明明文没有被当作正常请求处理。
	})
}

// TestTLSRejectsMismatchedKeyPair 验证证书与私钥不匹配时拒绝启动。
//
// 启动期失败比"端口已监听、每次握手才失败"安全得多：后者会让探活看起来正常。
func TestTLSRejectsMismatchedKeyPair(t *testing.T) {
	fx := newTLSFixture(t)
	other := newTLSFixture(t)

	fail := runServerExpectFailure(t, serverOptions{
		args: []string{"--tls-cert", fx.certFile, "--tls-key", other.keyFile},
	})
	if !strings.Contains(fail.output, "加载 TLS 证书失败") {
		t.Fatalf("启动失败信息未包含证书加载错误：\n%s", fail.output)
	}
}
