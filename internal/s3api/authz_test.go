package s3api

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/binbinpeng123/Blink/internal/authz"
	"github.com/binbinpeng123/Blink/internal/s3err"
	"github.com/binbinpeng123/Blink/internal/sigv4"
	"github.com/binbinpeng123/Blink/internal/store"
)

const (
	// limitedAK / limitedSK 是带显式策略的凭据；testAK / testSK
	// 在 auth_test.go 中定义，这里复用为全权限凭据。
	limitedAK = "AKIALIMITEDTEST0001"
	limitedSK = "limited-secret-key-for-tests"

	authzSignHost = "blink.test"
)

// presignedRequest 用指定凭据签发 URL 并构造请求。
//
// 走 Presign 而不是手工拼 Authorization：测试要验证的是"策略生效"，
// 若签名由测试自己伪造，403 就分不清是签名失败还是授权失败。
func presignedRequest(t *testing.T, method, target string, body []byte, ak, sk string) *http.Request {
	t.Helper()

	var first io.Reader
	if body != nil {
		first = bytes.NewReader(body)
	}
	base, err := http.NewRequest(method, "http://"+authzSignHost+target, first)
	if err != nil {
		t.Fatalf("构造请求失败: %v", err)
	}
	base.Host = authzSignHost

	signed, err := sigv4.Presign(base, sigv4.PresignOptions{
		Credentials: sigv4.Credentials{AccessKeyID: ak, SecretKey: sk},
		Region:      testRegion,
		Expires:     5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Presign 失败: %v", err)
	}

	var second io.Reader
	if body != nil {
		second = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, signed, second)
	req.Host = authzSignHost
	return req
}

func serve(h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// authzFixture 是接了真实 store 与策略表的测试服务器。
type authzFixture struct {
	h    http.Handler
	st   *store.Store
	logs *strings.Builder
}

// newAuthzFixture 构造带两份凭据的服务：testAK 全权限、limitedAK 使用给定策略。
func newAuthzFixture(t *testing.T, policy *authz.Policy) *authzFixture {
	t.Helper()
	st, err := store.Open(store.Options{Root: filepath.Join(t.TempDir(), "data")})
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	var buf strings.Builder
	handler := New(Options{
		Logger:   slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Region:   testRegion,
		Provider: testProvider(),
		Store:    st,
		Policies: map[string]*authz.Policy{
			testAK:    authz.AllowAll(),
			limitedAK: policy,
		},
	}).Handler()
	return &authzFixture{h: handler, st: st, logs: &buf}
}

func testProvider() *sigv4.StaticProvider {
	return sigv4.NewStaticProvider(
		sigv4.Credentials{AccessKeyID: testAK, SecretKey: testSK},
		sigv4.Credentials{AccessKeyID: limitedAK, SecretKey: limitedSK},
	)
}

// mustCreateBucket / mustPutObject 用存储层直接准备数据，绕开授权，
// 使测试的关注点保持在"请求期策略"上。
func mustCreateBucket(t *testing.T, st *store.Store, name string) {
	t.Helper()
	if err := st.CreateBucket(name); err != nil {
		t.Fatalf("CreateBucket(%s): %v", name, err)
	}
}

func mustPutObject(t *testing.T, st *store.Store, bucket, key string) {
	t.Helper()
	body := []byte("hello")
	_, err := st.PutObject(bucket, key, bytes.NewReader(body), store.PutOptions{ExpectedSize: int64(len(body))})
	if err != nil {
		t.Fatalf("PutObject(%s/%s): %v", bucket, key, err)
	}
}

// TestClassifyAction 固定请求到动作的映射（06 文档的表格）。
//
// 这张表是授权行为的规格：改这里之前先改文档，否则实现与文档会
// 在"某个 query 参数算 read 还是 write"这种细节上悄悄分叉。
func TestClassifyAction(t *testing.T) {
	cases := []struct {
		name   string
		method string
		target string
		bucket string
		key    string
		want   authz.Action
		ok     bool
	}{
		{"列桶", http.MethodGet, "/bkt", "bkt", "", authz.ActionList, true},
		{"HEAD 桶", http.MethodHead, "/bkt", "bkt", "", authz.ActionList, true},
		{"建桶", http.MethodPut, "/bkt", "bkt", "", authz.ActionAdmin, true},
		{"删桶", http.MethodDelete, "/bkt", "bkt", "", authz.ActionAdmin, true},
		{"CORS 配置", http.MethodGet, "/bkt?cors", "bkt", "", authz.ActionAdmin, true},
		{"列分片", http.MethodGet, "/bkt?uploads", "bkt", "", authz.ActionList, true},
		{"初始化分片", http.MethodPost, "/bkt/k?uploads", "bkt", "k", authz.ActionWrite, true},
		{"查分片", http.MethodGet, "/bkt/k?uploadId=u", "bkt", "k", authz.ActionRead, true},
		{"上传分片", http.MethodPut, "/bkt/k?uploadId=u&partNumber=1", "bkt", "k", authz.ActionWrite, true},
		{"完成分片", http.MethodPost, "/bkt/k?uploadId=u", "bkt", "k", authz.ActionWrite, true},
		{"放弃分片", http.MethodDelete, "/bkt/k?uploadId=u", "bkt", "k", authz.ActionWrite, true},
		{"批量删除", http.MethodPost, "/bkt?delete", "bkt", "", authz.ActionDelete, true},
		{"读对象", http.MethodGet, "/bkt/a/b.txt", "bkt", "a/b.txt", authz.ActionRead, true},
		{"HEAD 对象", http.MethodHead, "/bkt/a.txt", "bkt", "a.txt", authz.ActionRead, true},
		{"写对象", http.MethodPut, "/bkt/a.txt", "bkt", "a.txt", authz.ActionWrite, true},
		{"删对象", http.MethodDelete, "/bkt/a.txt", "bkt", "a.txt", authz.ActionDelete, true},
		{"未知方法", http.MethodPatch, "/bkt/a.txt", "bkt", "a.txt", "", false},
		{"缺桶", http.MethodGet, "/", "", "", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest(c.method, c.target, nil)
			got, ok := classifyAction(r, c.bucket, c.key)
			if ok != c.ok || got != c.want {
				t.Errorf("classifyAction(%s %s) = (%q, %v)，期望 (%q, %v)",
					c.method, c.target, got, ok, c.want, c.ok)
			}
		})
	}
}

// TestPolicyDeniesReadOutsidePrefix 验证 read 策略的前缀边界。
func TestPolicyDeniesReadOutsidePrefix(t *testing.T) {
	policy, err := authz.New([]authz.Rule{
		{Bucket: "bkt", Prefix: "allowed/", Actions: []authz.Action{authz.ActionRead}},
	})
	if err != nil {
		t.Fatalf("构造策略失败: %v", err)
	}
	f := newAuthzFixture(t, policy)
	mustCreateBucket(t, f.st, "bkt")
	mustPutObject(t, f.st, "bkt", "allowed/ok.txt")
	mustPutObject(t, f.st, "bkt", "denied/secret.txt")

	rec := serve(f.h, presignedRequest(t, http.MethodGet, "/bkt/allowed/ok.txt", nil, limitedAK, limitedSK))
	if rec.Code != http.StatusOK {
		t.Fatalf("前缀内 GET 应成功，实际 %d: %s", rec.Code, rec.Body.String())
	}

	rec = serve(f.h, presignedRequest(t, http.MethodGet, "/bkt/denied/secret.txt", nil, limitedAK, limitedSK))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("前缀外 GET 应 403，实际 %d: %s", rec.Code, rec.Body.String())
	}
	if code := decodeError(t, rec).Code; code != string(s3err.CodeAccessDenied) {
		t.Errorf("错误码 = %q，期望 AccessDenied", code)
	}
	if !strings.Contains(f.logs.String(), "s3_authz_denied") {
		t.Error("授权拒绝应记日志")
	}
}

// TestPolicyDeniesWriteAndDelete 验证写/删两个独立动作。
func TestPolicyDeniesWriteAndDelete(t *testing.T) {
	policy, err := authz.New([]authz.Rule{
		{Bucket: "bkt", Prefix: "allowed/", Actions: []authz.Action{authz.ActionWrite}},
	})
	if err != nil {
		t.Fatalf("构造策略失败: %v", err)
	}
	f := newAuthzFixture(t, policy)
	mustCreateBucket(t, f.st, "bkt")
	mustPutObject(t, f.st, "bkt", "allowed/existing.txt")

	// 前缀外写入被拒，且对象不应落盘。
	rec := serve(f.h, presignedRequest(t, http.MethodPut, "/bkt/denied/new.txt", []byte("x"), limitedAK, limitedSK))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("前缀外 PUT 应 403，实际 %d: %s", rec.Code, rec.Body.String())
	}
	if _, err := f.st.OpenObject("bkt", "denied/new.txt"); !errors.Is(err, store.ErrNoSuchKey) {
		t.Errorf("被拒绝的写入不应产生对象，实际 err=%v", err)
	}

	// 只有 write 没有 delete：删除同样被拒。
	rec = serve(f.h, presignedRequest(t, http.MethodDelete, "/bkt/allowed/existing.txt", nil, limitedAK, limitedSK))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("无 delete 权限应 403，实际 %d: %s", rec.Code, rec.Body.String())
	}
	if _, err := f.st.OpenObject("bkt", "allowed/existing.txt"); err != nil {
		t.Errorf("被拒绝的删除不应改变对象: %v", err)
	}

	// 前缀内写入成功。
	rec = serve(f.h, presignedRequest(t, http.MethodPut, "/bkt/allowed/new.txt", []byte("x"), limitedAK, limitedSK))
	if rec.Code != http.StatusOK {
		t.Fatalf("前缀内 PUT 应成功，实际 %d: %s", rec.Code, rec.Body.String())
	}
}

// TestListBucketsFiltersInvisible 验证 ListBuckets 按策略过滤。
func TestListBucketsFiltersInvisible(t *testing.T) {
	policy, err := authz.New([]authz.Rule{
		{Bucket: "visible", Actions: []authz.Action{authz.ActionRead}},
	})
	if err != nil {
		t.Fatalf("构造策略失败: %v", err)
	}
	f := newAuthzFixture(t, policy)
	mustCreateBucket(t, f.st, "visible")
	mustCreateBucket(t, f.st, "hidden")

	rec := serve(f.h, presignedRequest(t, http.MethodGet, "/", nil, limitedAK, limitedSK))
	if rec.Code != http.StatusOK {
		t.Fatalf("ListBuckets 应成功，实际 %d: %s", rec.Code, rec.Body.String())
	}
	var out listAllMyBucketsResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("响应不是合法 XML: %v", err)
	}
	names := make([]string, 0, len(out.Buckets.Bucket))
	for _, b := range out.Buckets.Bucket {
		names = append(names, b.Name)
	}
	if len(names) != 1 || names[0] != "visible" {
		t.Errorf("ListBuckets = %v，期望仅 [visible]", names)
	}
}

// TestDeleteObjectsMixedPermissions 验证批量删除按 key 逐条授权。
//
// 整批拒绝是错的：一条策略通常是"某前缀可删"，批量请求混入前缀外
// 的 key 属于正常用法，应只拒该条并保持其余条目成功。
func TestDeleteObjectsMixedPermissions(t *testing.T) {
	policy, err := authz.New([]authz.Rule{
		{Bucket: "bkt", Prefix: "allowed/", Actions: []authz.Action{authz.ActionDelete}},
	})
	if err != nil {
		t.Fatalf("构造策略失败: %v", err)
	}
	f := newAuthzFixture(t, policy)
	mustCreateBucket(t, f.st, "bkt")
	mustPutObject(t, f.st, "bkt", "allowed/1.txt")
	mustPutObject(t, f.st, "bkt", "denied/1.txt")

	body := `<Delete>
		<Object><Key>allowed/1.txt</Key></Object>
		<Object><Key>denied/1.txt</Key></Object>
	</Delete>`
	rec := serve(f.h, presignedRequest(t, http.MethodPost, "/bkt?delete", []byte(body), limitedAK, limitedSK))
	if rec.Code != http.StatusOK {
		t.Fatalf("批量删除整体应为 200，实际 %d: %s", rec.Code, rec.Body.String())
	}

	var out deleteObjectsResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("响应不是合法 XML: %v", err)
	}
	if len(out.Deleted) != 1 || out.Deleted[0].Key != "allowed/1.txt" {
		t.Errorf("Deleted = %+v，期望仅 allowed/1.txt", out.Deleted)
	}
	if len(out.Errors) != 1 || out.Errors[0].Key != "denied/1.txt" ||
		out.Errors[0].Code != string(s3err.CodeAccessDenied) {
		t.Errorf("Errors = %+v，期望 denied/1.txt 的 AccessDenied", out.Errors)
	}

	if _, err := f.st.OpenObject("bkt", "allowed/1.txt"); !errors.Is(err, store.ErrNoSuchKey) {
		t.Errorf("有权限的 key 应已删除，实际 err=%v", err)
	}
	if _, err := f.st.OpenObject("bkt", "denied/1.txt"); err != nil {
		t.Errorf("无权限的 key 应保留，实际 err=%v", err)
	}
}

// TestCopySourceRequiresRead 验证 CopyObject 除了目标 write 还要求源 read。
//
// 只校验目标会让"能写自己的桶"升级成"能读整个集群"：
// 复制结果会出现在攻击者自己的桶里。
func TestCopySourceRequiresRead(t *testing.T) {
	policy, err := authz.New([]authz.Rule{
		{Bucket: "src", Prefix: "ok/", Actions: []authz.Action{authz.ActionRead}},
		{Bucket: "dest", Actions: []authz.Action{authz.ActionWrite}},
	})
	if err != nil {
		t.Fatalf("构造策略失败: %v", err)
	}
	f := newAuthzFixture(t, policy)
	mustCreateBucket(t, f.st, "src")
	mustCreateBucket(t, f.st, "dest")
	mustPutObject(t, f.st, "src", "ok/readable.txt")
	mustPutObject(t, f.st, "src", "secret.txt")

	req := presignedRequest(t, http.MethodPut, "/dest/copied.txt", nil, limitedAK, limitedSK)
	req.Header.Set("x-amz-copy-source", "/src/secret.txt")
	rec := serve(f.h, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("前缀外源对象复制应 403，实际 %d: %s", rec.Code, rec.Body.String())
	}
	if _, err := f.st.OpenObject("dest", "copied.txt"); !errors.Is(err, store.ErrNoSuchKey) {
		t.Errorf("被拒绝的复制不应产生目标对象，实际 err=%v", err)
	}

	req = presignedRequest(t, http.MethodPut, "/dest/copied.txt", nil, limitedAK, limitedSK)
	req.Header.Set("x-amz-copy-source", "/src/ok/readable.txt")
	rec = serve(f.h, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("有权限的源对象复制应成功，实际 %d: %s", rec.Code, rec.Body.String())
	}
}

// TestUploadPartCopyRequiresRead 验证分片复制的源授权与 CopyObject 一致。
func TestUploadPartCopyRequiresRead(t *testing.T) {
	policy, err := authz.New([]authz.Rule{
		{Bucket: "src", Prefix: "ok/", Actions: []authz.Action{authz.ActionRead}},
		{Bucket: "dest", Actions: []authz.Action{authz.ActionWrite}},
	})
	if err != nil {
		t.Fatalf("构造策略失败: %v", err)
	}
	f := newAuthzFixture(t, policy)
	mustCreateBucket(t, f.st, "src")
	mustCreateBucket(t, f.st, "dest")
	srcBody := bytes.Repeat([]byte("a"), 1024)
	if _, err := f.st.PutObject("src", "secret.bin", bytes.NewReader(srcBody),
		store.PutOptions{ExpectedSize: int64(len(srcBody))}); err != nil {
		t.Fatalf("准备源对象失败: %v", err)
	}

	rec := serve(f.h, presignedRequest(t, http.MethodPost, "/dest/big.bin?uploads", nil, limitedAK, limitedSK))
	if rec.Code != http.StatusOK {
		t.Fatalf("初始化分片应成功，实际 %d: %s", rec.Code, rec.Body.String())
	}
	var initOut initiateMultipartUploadResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &initOut); err != nil || initOut.UploadID == "" {
		t.Fatalf("解析 UploadId 失败: %v (%s)", err, rec.Body.String())
	}

	target := fmt.Sprintf("/dest/big.bin?uploadId=%s&partNumber=1", initOut.UploadID)
	req := presignedRequest(t, http.MethodPut, target, nil, limitedAK, limitedSK)
	req.Header.Set("x-amz-copy-source", "/src/secret.bin")
	rec = serve(f.h, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("前缀外源对象的分片复制应 403，实际 %d: %s", rec.Code, rec.Body.String())
	}
}

// TestUnknownCredentialIsDenied 验证"签名有效但策略表查不到"时拒绝。
//
// 这是最容易漏的一类配置错误：Provider 里有凭据，策略表里没有，
// 若默认放行就变成了绕过授权。
func TestUnknownCredentialIsDenied(t *testing.T) {
	h := New(Options{
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Region:   testRegion,
		Provider: testProvider(),
		Policies: map[string]*authz.Policy{testAK: authz.AllowAll()},
	}).Handler()

	rec := serve(h, presignedRequest(t, http.MethodGet, "/bkt/k", nil, limitedAK, limitedSK))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("无策略凭据应 403，实际 %d: %s", rec.Code, rec.Body.String())
	}
}

// TestAuditLogHidesSecrets 验证审计日志的形状与脱敏。
func TestAuditLogHidesSecrets(t *testing.T) {
	var buf strings.Builder
	st, err := store.Open(store.Options{Root: filepath.Join(t.TempDir(), "data")})
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	mustCreateBucket(t, st, "auditb")
	mustPutObject(t, st, "auditb", "hello.txt")

	h := New(Options{
		Logger:   slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Region:   testRegion,
		Provider: testProvider(),
		Store:    st,
		Policies: map[string]*authz.Policy{testAK: authz.AllowAll()},
	}).Handler()

	rec := serve(h, presignedRequest(t, http.MethodGet, "/auditb/hello.txt", nil, testAK, testSK))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET 应成功，实际 %d: %s", rec.Code, rec.Body.String())
	}

	logged := buf.String()
	for _, want := range []string{"s3_audit", "action=read", "bucket=auditb", "key=hello.txt", "status=200"} {
		if !strings.Contains(logged, want) {
			t.Errorf("审计日志缺少 %q: %s", want, logged)
		}
	}
	for _, forbidden := range []string{testSK, testAK, "X-Amz-Signature", "X-Amz-Credential"} {
		if strings.Contains(logged, forbidden) {
			t.Errorf("审计日志泄露了 %q: %s", forbidden, logged)
		}
	}
	// Access Key 允许保留前 4 位用于对照排查。
	if !strings.Contains(logged, testAK[:4]) {
		t.Error("审计日志应保留 Access Key 前缀")
	}

	// 同一行里每个键只能出现一次：slog 的 JSON handler 不去重，
	// 重复键会被不同日志解析器取成不同值。
	for _, line := range strings.Split(strings.TrimSpace(logged), "\n") {
		if !strings.Contains(line, "s3_audit") {
			continue
		}
		for _, field := range []string{"remote", "status", "action", "access_key"} {
			// TextHandler 用 "key=value" 输出；同名键出现两次同样说明
			// 基础 logger 与审计属性重复。
			if n := strings.Count(line, field+"="); n != 1 {
				t.Errorf("审计日志字段 %q 出现 %d 次: %s", field, n, line)
			}
		}
	}

	// ListBuckets 不按单一动作授权，审计里仍应记为 list。
	rec = serve(h, presignedRequest(t, http.MethodGet, "/", nil, testAK, testSK))
	if rec.Code != http.StatusOK {
		t.Fatalf("ListBuckets 应成功，实际 %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(buf.String(), `action=list`) {
		t.Error("ListBuckets 审计应记 action=list")
	}
}

// TestStorageFullMapsTo507 验证水位阻断在协议层映射为 507。
func TestStorageFullMapsTo507(t *testing.T) {
	st, err := store.Open(store.Options{
		Root:          filepath.Join(t.TempDir(), "data"),
		HighWatermark: 0.90,
		LowWatermark:  0.85,
		DiskSpace: func(string) (uint64, uint64, error) {
			return 100, 1, nil // 99% 已用
		},
	})
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	mustCreateBucket(t, st, "bkt")

	h := New(Options{
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Region:      testRegion,
		DisableAuth: true,
		Store:       st,
	}).Handler()

	req := httptest.NewRequest(http.MethodPut, "/bkt/k.txt", strings.NewReader("x"))
	req.ContentLength = 1
	rec := serve(h, req)
	if rec.Code != http.StatusInsufficientStorage {
		t.Fatalf("满盘写入应 507，实际 %d: %s", rec.Code, rec.Body.String())
	}
	if code := decodeError(t, rec).Code; code != string(s3err.CodeInsufficientStorage) {
		t.Errorf("错误码 = %q，期望 InsufficientStorage", code)
	}
}
