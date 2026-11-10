//go:build integration

// Package integration 是 Blink 的黑盒集成测试套件。
//
// 这些测试刻意**不**直接调用 internal 包，而是：
//  1. 用 go build 构建真实二进制；
//  2. 以子进程方式启动，使用临时数据目录与随机端口；
//  3. 通过 HTTP + SigV4 Presigned URL 走完整协议栈。
//
// 05 文档（测试与性能基准）把「集成测试」定义为对真实二进制的端到端验证，
// 因此重启、SIGKILL、格式版本拒绝启动这类行为必须由子进程证明，
// 在进程内构造 handler 无法覆盖。
package integration

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/binbinpeng123/Blink/internal/sigv4"
)

const (
	testRegion    = "us-east-1"
	testAccessKey = "AKIATESTTEST1234"
	// 明确不是真实凭据：测试专用，仅注入子进程环境。
	testSecretKey = "test-secret-key-not-a-real-credential"
)

// blinkBinary 由 TestMain 构建；设置 BLINK_TEST_BIN 可复用已有二进制。
var blinkBinary string

func TestMain(m *testing.M) {
	// testing 包在 TestMain 中不会自动解析命令行参数。
	flag.Parse()

	if bin := os.Getenv("BLINK_TEST_BIN"); bin != "" {
		blinkBinary = bin
		os.Exit(m.Run())
	}

	root, err := repoRoot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "定位仓库根目录失败: %v\n", err)
		os.Exit(1)
	}

	dir, err := os.MkdirTemp("", "blink-integration-bin-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建临时目录失败: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)

	bin := filepath.Join(dir, "blink")
	cmd := exec.Command("go", "build", "-trimpath", "-o", bin, "./cmd/blink")
	cmd.Dir = root
	cmd.Env = os.Environ()
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "构建 blink 失败: %v\n%s", err, out)
		os.Exit(1)
	}

	blinkBinary = bin
	os.Exit(m.Run())
}

// repoRoot 通过本文件的路径定位仓库根（test/integration 的上两级）。
func repoRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("runtime.Caller 失败")
	}
	return filepath.Dir(filepath.Dir(filepath.Dir(file))), nil
}

// serverOptions 描述一个测试服务进程。
type serverOptions struct {
	// dataDir 为空时在 t.TempDir() 下创建新目录；重启测试复用同一目录。
	dataDir string
	// args 追加到默认启动参数之后。
	args []string
	// env 追加/覆盖环境变量；BLINK_ACCESS_KEY 与 BLINK_SECRET_KEY 默认注入。
	env map[string]string
}

type testServer struct {
	t       *testing.T
	addr    string
	baseURL string
	dataDir string
	logPath string

	cmd     *exec.Cmd
	done    chan struct{}
	waitErr error

	stopOnce sync.Once
	killOnce sync.Once
}

func defaultServerArgs(addr, dataDir string) []string {
	return []string{
		"--addr", addr,
		"--data-dir", dataDir,
		"--region", testRegion,
		"--log-level", "debug",
	}
}

// startServer 启动真实二进制并等待 /healthz 就绪。
//
// 子进程环境会先清除宿主上的全部 BLINK_*，再注入固定测试凭据，
// 避免开发机的环境变量让测试结果随机器漂移。
func startServer(t *testing.T, opt serverOptions) *testServer {
	t.Helper()

	dataDir := opt.dataDir
	if dataDir == "" {
		dataDir = filepath.Join(t.TempDir(), "data")
	}
	addr := freeAddr(t)
	logPath := filepath.Join(t.TempDir(), "server.log")

	args := append(defaultServerArgs(addr, dataDir), opt.args...)
	cmd := exec.Command(blinkBinary, args...)
	cmd.Env = cleanEnv(map[string]string{
		"BLINK_ACCESS_KEY": testAccessKey,
		"BLINK_SECRET_KEY": testSecretKey,
	}, opt.env)

	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("创建日志文件失败: %v", err)
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("启动 blink 失败: %v", err)
	}

	srv := &testServer{
		t:       t,
		addr:    addr,
		baseURL: "http://" + addr,
		dataDir: dataDir,
		logPath: logPath,
		cmd:     cmd,
		done:    make(chan struct{}),
	}
	go func() {
		srv.waitErr = cmd.Wait()
		_ = logFile.Close()
		close(srv.done)
	}()

	t.Cleanup(srv.stop)
	srv.waitReady()
	return srv
}

// waitReady 轮询 /healthz，直到进程可服务；进程提前退出则直接失败并打印日志尾部。
func (s *testServer) waitReady() {
	s.t.Helper()
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(20 * time.Second)

	for time.Now().Before(deadline) {
		select {
		case <-s.done:
			s.t.Fatalf("blink 在就绪前退出（wait=%v）：\n%s", s.waitErr, s.logTail(80))
		default:
		}

		resp, err := client.Get(s.baseURL + "/healthz")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	s.t.Fatalf("blink 在 20s 内未就绪：\n%s", s.logTail(80))
}

// stop 先发 SIGTERM 走优雅关闭，超时后 SIGKILL。可重复调用。
func (s *testServer) stop() {
	s.stopOnce.Do(func() {
		if s.exited() {
			return
		}
		_ = s.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-s.done:
		case <-time.After(10 * time.Second):
			_ = s.cmd.Process.Kill()
			<-s.done
		}
	})
}

// kill 模拟进程崩溃（SIGKILL，无优雅关闭）。
func (s *testServer) kill() {
	s.killOnce.Do(func() {
		if s.exited() {
			return
		}
		_ = s.cmd.Process.Kill()
		<-s.done
	})
}

func (s *testServer) exited() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// logTail 返回日志最后 n 行，失败信息里带上它便于定位。
func (s *testServer) logTail(n int) string {
	data, err := os.ReadFile(s.logPath)
	if err != nil {
		return fmt.Sprintf("（读取日志失败: %v）", err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// freeAddr 申请一个空闲端口。
//
// 先 Listen 再 Close 存在理论上的竞态（端口可能被别的进程抢走）；
// 单机测试环境下概率极低，启动失败会以明确日志暴露，而不是静默错连。
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("申请空闲端口失败: %v", err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// cleanEnv 过滤宿主上的全部 BLINK_*，再写入显式提供的变量。
func cleanEnv(base, extra map[string]string) []string {
	env := make([]string, 0, len(os.Environ())+len(base)+len(extra))
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "BLINK_") {
			continue
		}
		env = append(env, kv)
	}
	for k, v := range base {
		env = append(env, k+"="+v)
	}
	for k, v := range extra {
		env = append(env, k+"="+v)
	}
	return env
}

// startupFailure 是一次预期的启动失败。
type startupFailure struct {
	err    error
	output string
}

// runServerExpectFailure 启动二进制并断言它自行退出且返回非零。
//
// 用于验证"拒绝启动"语义：格式版本过高、元数据损坏、非空目录缺 meta。
// 若进程意外长时间运行，会判定测试失败而不是挂死。
func runServerExpectFailure(t *testing.T, opt serverOptions) startupFailure {
	t.Helper()

	dataDir := opt.dataDir
	if dataDir == "" {
		dataDir = filepath.Join(t.TempDir(), "data")
	}
	addr := freeAddr(t)
	args := append(defaultServerArgs(addr, dataDir), opt.args...)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, blinkBinary, args...)
	cmd.Env = cleanEnv(map[string]string{
		"BLINK_ACCESS_KEY": testAccessKey,
		"BLINK_SECRET_KEY": testSecretKey,
	}, opt.env)

	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("进程未在 20s 内退出（预期拒绝启动）：\n%s", out)
	}
	if err == nil {
		t.Fatalf("预期启动失败，但进程正常退出（或仍在运行）：\n%s", out)
	}
	return startupFailure{err: err, output: string(out)}
}

// s3Error 是 S3 错误响应体（只取断言需要的字段）。
type s3Error struct {
	Code      string `xml:"Code"`
	Message   string `xml:"Message"`
	RequestID string `xml:"RequestId"`
}

// httpResult 是一次完整的 HTTP 响应快照。
type httpResult struct {
	Status int
	Header http.Header
	Body   []byte
}

// s3Client 是走 Presigned URL 的测试客户端。
//
// 之所以用 Presign 而不是在测试里重写 SigV4：internal/sigv4 已导出
// Presign，且与服务的验签路径共用规范化代码，签名正确性由单元测试保证。
// Presigned URL 只签名 host，因此 Range、If-*、Content-MD5 等头可以自由附加。
type s3Client struct {
	t    *testing.T
	base string
	host string
	ak   string
	sk   string
	hc   *http.Client
}

func newClient(t *testing.T, srv *testServer) *s3Client {
	t.Helper()
	u, err := url.Parse(srv.baseURL)
	if err != nil {
		t.Fatalf("解析服务地址失败: %v", err)
	}
	return &s3Client{
		t:    t,
		base: srv.baseURL,
		host: u.Host,
		ak:   testAccessKey,
		sk:   testSecretKey,
		hc:   &http.Client{Timeout: 60 * time.Second},
	}
}

// withCredentials 返回使用另一组凭据的客户端，用于鉴权失败断言。
func (c *s3Client) withCredentials(ak, sk string) *s3Client {
	cp := *c
	cp.ak = ak
	cp.sk = sk
	return &cp
}

// bucketURL 返回 /{bucket}。
func (c *s3Client) bucketURL(bucket string) string {
	return "/" + url.PathEscape(bucket)
}

// objectURL 返回 /{bucket}/{key}，key 按 / 分段转义。
func (c *s3Client) objectURL(bucket, key string) string {
	return c.bucketURL(bucket) + "/" + escapeKey(key)
}

// escapeKey 逐段转义对象 key，保留 / 作为分隔符。
func escapeKey(key string) string {
	segs := strings.Split(key, "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	return strings.Join(segs, "/")
}

// request 构造并签名一次请求。
func (c *s3Client) request(method, path string, query url.Values, body []byte, headers map[string]string) *httpResult {
	c.t.Helper()
	res, err := c.tryRequest(method, path, query, body, headers)
	if err != nil {
		c.t.Fatalf("%s %s 请求失败: %v", method, path, err)
	}
	return res
}

// tryRequest 与 request 相同，但把错误返回给调用方。
//
// 供并发与崩溃测试在 goroutine 中使用：t.Fatalf 只能从测试 goroutine
// 调用，后台 goroutine 里失败必须回传给主测试统一报告。
func (c *s3Client) tryRequest(method, path string, query url.Values, body []byte, headers map[string]string) (*httpResult, error) {
	target := c.base + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	req, err := http.NewRequest(method, target, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("构造请求: %w", err)
	}
	req.Host = c.host
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	signed, err := sigv4.Presign(req, sigv4.PresignOptions{
		Credentials: sigv4.Credentials{AccessKeyID: c.ak, SecretKey: c.sk},
		Region:      testRegion,
		Expires:     10 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("签发 Presigned URL: %w", err)
	}
	u, err := url.Parse(signed)
	if err != nil {
		return nil, fmt.Errorf("解析 Presigned URL: %w", err)
	}
	req.URL = u

	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &httpResult{Status: resp.StatusCode, Header: resp.Header, Body: data}, nil
}

// unsigned 发送不签名的请求，用于验证免签端点与匿名拒绝。
func (c *s3Client) unsigned(method, path string, query url.Values, body []byte, headers map[string]string) *httpResult {
	c.t.Helper()

	target := c.base + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	req, err := http.NewRequest(method, target, bytes.NewReader(body))
	if err != nil {
		c.t.Fatalf("构造请求失败: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s 请求失败: %v", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		c.t.Fatalf("读取 %s %s 响应失败: %v", method, path, err)
	}
	return &httpResult{Status: resp.StatusCode, Header: resp.Header, Body: data}
}

func (c *s3Client) do(method, path string, query url.Values, body []byte, headers map[string]string) *httpResult {
	return c.request(method, path, query, body, headers)
}

// presignedURL 生成带签名的 URL，供需要绕过本客户端的场景（例如 Range 下载）使用。
func (c *s3Client) presignedURL(method, path string, query url.Values) string {
	c.t.Helper()
	target := c.base + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	req, err := http.NewRequest(method, target, nil)
	if err != nil {
		c.t.Fatalf("构造请求失败: %v", err)
	}
	req.Host = c.host
	signed, err := sigv4.Presign(req, sigv4.PresignOptions{
		Credentials: sigv4.Credentials{AccessKeyID: c.ak, SecretKey: c.sk},
		Region:      testRegion,
		Expires:     10 * time.Minute,
	})
	if err != nil {
		c.t.Fatalf("签发 Presigned URL 失败: %v", err)
	}
	return signed
}

// Convenience helpers ------------------------------------------------------

func (c *s3Client) createBucket(bucket string) *httpResult {
	c.t.Helper()
	return c.do(http.MethodPut, c.bucketURL(bucket), nil, nil, nil)
}

func (c *s3Client) deleteBucket(bucket string) *httpResult {
	c.t.Helper()
	return c.do(http.MethodDelete, c.bucketURL(bucket), nil, nil, nil)
}

func (c *s3Client) headBucket(bucket string) *httpResult {
	c.t.Helper()
	return c.do(http.MethodHead, c.bucketURL(bucket), nil, nil, nil)
}

func (c *s3Client) putObject(bucket, key string, body []byte, headers map[string]string) *httpResult {
	c.t.Helper()
	return c.do(http.MethodPut, c.objectURL(bucket, key), nil, body, headers)
}

func (c *s3Client) getObject(bucket, key string, headers map[string]string) *httpResult {
	c.t.Helper()
	return c.do(http.MethodGet, c.objectURL(bucket, key), nil, nil, headers)
}

func (c *s3Client) headObject(bucket, key string, headers map[string]string) *httpResult {
	c.t.Helper()
	return c.do(http.MethodHead, c.objectURL(bucket, key), nil, nil, headers)
}

func (c *s3Client) deleteObject(bucket, key string) *httpResult {
	c.t.Helper()
	return c.do(http.MethodDelete, c.objectURL(bucket, key), nil, nil, nil)
}

// Assertion helpers -------------------------------------------------------

func requireStatus(t *testing.T, res *httpResult, want int) {
	t.Helper()
	if res.Status != want {
		t.Fatalf("状态码 = %d，期望 %d；响应=%s", res.Status, want, truncate(res.Body))
	}
}

func decodeS3Error(t *testing.T, res *httpResult) s3Error {
	t.Helper()
	var e s3Error
	if err := xml.Unmarshal(res.Body, &e); err != nil {
		t.Fatalf("解析 S3 错误响应失败: %v；响应=%s", err, truncate(res.Body))
	}
	return e
}

func requireS3Error(t *testing.T, res *httpResult, status int, code string) s3Error {
	t.Helper()
	requireStatus(t, res, status)
	e := decodeS3Error(t, res)
	if e.Code != code {
		t.Fatalf("错误码 = %q，期望 %q；响应=%s", e.Code, code, truncate(res.Body))
	}
	return e
}

// mustXML 反序列化 XML 响应体。
func mustXML[T any](t *testing.T, res *httpResult) T {
	t.Helper()
	var v T
	if err := xml.Unmarshal(res.Body, &v); err != nil {
		t.Fatalf("解析 XML 失败: %v；响应=%s", err, truncate(res.Body))
	}
	return v
}

// mustJSON 反序列化 JSON 响应体。
func mustJSON[T any](t *testing.T, res *httpResult) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(res.Body, &v); err != nil {
		t.Fatalf("解析 JSON 失败: %v；响应=%s", err, truncate(res.Body))
	}
	return v
}

func truncate(b []byte) string {
	const max = 2048
	if len(b) > max {
		return string(b[:max]) + "…"
	}
	return string(b)
}

// uniqueBucket 生成满足 S3 命名规则的桶名。
func uniqueBucket(prefix string) string {
	return prefix + "-" + randHex(6)
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand 失败: " + err.Error())
	}
	return hex.EncodeToString(b)
}

func contentMD5(body []byte) string {
	sum := md5.Sum(body)
	return base64.StdEncoding.EncodeToString(sum[:])
}

func unquoteETag(etag string) string {
	return strings.Trim(strings.TrimSpace(etag), `"`)
}

// bodyOf 生成确定性的测试负载（按 seed 变化），避免随机读带来的不可复现。
func bodyOf(size int, seed byte) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte((i*31 + int(seed)*17) % 251)
	}
	return b
}
