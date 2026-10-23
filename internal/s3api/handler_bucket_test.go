package s3api

import (
	"encoding/xml"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/binbinpeng123/Blink/internal/s3err"
	"github.com/binbinpeng123/Blink/internal/store"
)

// bucketTestServer 构造一个接了真实 store 的测试服务器。
//
// 与 router_test.go 的 newTestServer 的差别只有 store：那边测的是路由分发，
// 这边测的是"存储错误如何变成 S3 响应"，因此必须有真 store 才能造出
// 真实的 ErrBucketNotFound 之类。
func bucketTestServer(t *testing.T) http.Handler {
	t.Helper()
	st, err := store.Open(store.Options{Root: filepath.Join(t.TempDir(), "data")})
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(Options{
		Logger:      logger,
		Region:      "us-east-1",
		DisableAuth: true,
		Store:       st,
	}).Handler()
}

// s3ErrorBody 是错误响应的公共字段。
type s3ErrorBody struct {
	Code      string `xml:"Code"`
	Message   string `xml:"Message"`
	RequestID string `xml:"RequestId"`
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder) s3ErrorBody {
	t.Helper()
	var body s3ErrorBody
	if err := xml.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("错误体不是合法 XML: %v (%s)", err, rec.Body.String())
	}
	return body
}

func TestBucketLifecycleOverHTTP(t *testing.T) {
	h := bucketTestServer(t)

	// 建桶。
	rec := do(t, h, http.MethodPut, "/lifecycle")
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT 桶状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}
	if loc := rec.Header().Get("Location"); loc != "/lifecycle" {
		t.Errorf("Location = %q，期望 /lifecycle", loc)
	}

	// HEAD 确认存在，且给出区域 —— 部分 SDK 靠它决定后续请求去哪。
	rec = do(t, h, http.MethodHead, "/lifecycle")
	if rec.Code != http.StatusOK {
		t.Fatalf("HEAD 桶状态码 = %d，期望 200", rec.Code)
	}
	if region := rec.Header().Get("x-amz-bucket-region"); region != "us-east-1" {
		t.Errorf("x-amz-bucket-region = %q，期望 us-east-1", region)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("HEAD 响应体应为空，实际 %d 字节", rec.Body.Len())
	}

	// LIST 应包含它。
	rec = do(t, h, http.MethodGet, "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("ListBuckets 状态码 = %d，期望 200", rec.Code)
	}
	var listed listAllMyBucketsResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &listed); err != nil {
		t.Fatalf("ListBuckets 响应不是合法 XML: %v", err)
	}
	if len(listed.Buckets.Bucket) != 1 || listed.Buckets.Bucket[0].Name != "lifecycle" {
		t.Fatalf("ListBuckets 结果 = %+v，期望只含 lifecycle", listed.Buckets.Bucket)
	}
	if listed.Buckets.Bucket[0].CreationDate == "" {
		t.Error("CreationDate 不应为空")
	}
	// 不少客户端会解析 Owner，缺失会导致解码报错。
	if listed.Owner.ID == "" || listed.Owner.DisplayName == "" {
		t.Error("Owner 字段不应为空")
	}

	// 删桶。
	rec = do(t, h, http.MethodDelete, "/lifecycle")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE 桶状态码 = %d，期望 204", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("DeleteBucket 响应体应为空，实际 %d 字节", rec.Body.Len())
	}

	// 删完再 HEAD 应为 404。
	rec = do(t, h, http.MethodHead, "/lifecycle")
	if rec.Code != http.StatusNotFound {
		t.Errorf("删除后 HEAD 状态码 = %d，期望 404", rec.Code)
	}
}

func TestCreateBucketWithLocationConstraint(t *testing.T) {
	h := bucketTestServer(t)

	// aws-cli 与非 us-east-1 的 SDK 都会带请求体，拒绝它会让 `aws s3 mb` 不可用。
	body := `<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		`<LocationConstraint>us-west-2</LocationConstraint></CreateBucketConfiguration>`
	req := httptest.NewRequest(http.MethodPut, "/with-location", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}
}

func TestCreateBucketMalformedXML(t *testing.T) {
	h := bucketTestServer(t)

	req := httptest.NewRequest(http.MethodPut, "/bad-xml", strings.NewReader("<not-xml"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，期望 400", rec.Code)
	}
	if got := decodeError(t, rec).Code; got != string(s3err.CodeMalformedXML) {
		t.Errorf("Code = %q，期望 MalformedXML", got)
	}
}

func TestCreateBucketDuplicateReturnsOwnedByYou(t *testing.T) {
	h := bucketTestServer(t)
	do(t, h, http.MethodPut, "/dup")

	rec := do(t, h, http.MethodPut, "/dup")
	if rec.Code != http.StatusConflict {
		t.Fatalf("状态码 = %d，期望 409", rec.Code)
	}
	// 单节点单租户没有"他人拥有"的情形，因此必须是 AlreadyOwnedByYou ——
	// 这正是 `aws s3 mb` 能正确呈现的那个错误码。
	if got := decodeError(t, rec).Code; got != string(s3err.CodeBucketAlreadyOwnedByYou) {
		t.Errorf("Code = %q，期望 BucketAlreadyOwnedByYou", got)
	}
}

func TestCreateBucketInvalidNameOverHTTP(t *testing.T) {
	h := bucketTestServer(t)

	rec := do(t, h, http.MethodPut, "/Bad_Name")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，期望 400", rec.Code)
	}
	if got := decodeError(t, rec).Code; got != string(s3err.CodeInvalidBucketName) {
		t.Errorf("Code = %q，期望 InvalidBucketName", got)
	}
}

func TestDeleteBucketNotFoundOverHTTP(t *testing.T) {
	h := bucketTestServer(t)

	rec := do(t, h, http.MethodDelete, "/nope")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("状态码 = %d，期望 404", rec.Code)
	}
	if got := decodeError(t, rec).Code; got != string(s3err.CodeNoSuchBucket) {
		t.Errorf("Code = %q，期望 NoSuchBucket", got)
	}
}

// TestS3ErrorBodyHasRequestID 验证错误体里的 RequestId 与响应头一致。
//
// 这一致性是排障的前提：用户拿着错误体里的 ID 来找运维，
// 运维要能在日志里按同一个 ID 检索到这次请求。
func TestS3ErrorBodyHasRequestID(t *testing.T) {
	h := bucketTestServer(t)

	rec := do(t, h, http.MethodDelete, "/nope")
	body := decodeError(t, rec)
	if body.RequestID == "" {
		t.Fatal("错误体缺少 RequestId")
	}
	if body.RequestID != rec.Header().Get(headerRequestID) {
		t.Errorf("错误体 RequestId = %q，响应头 = %q",
			body.RequestID, rec.Header().Get(headerRequestID))
	}
}

// TestNoStoreReturns500 验证未接入存储时数据操作失败得**明确**。
//
// 关键是绝不能"静默返回空结果"：客户端会把"服务没配好"
// 误读成"桶里没东西"，进而以为数据丢了。
func TestNoStoreReturns500(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := New(Options{Logger: logger, Region: "us-east-1", DisableAuth: true}).Handler()

	for _, tc := range []struct{ method, target string }{
		{http.MethodGet, "/"},
		{http.MethodPut, "/b"},
		{http.MethodDelete, "/b"},
	} {
		t.Run(tc.method+" "+tc.target, func(t *testing.T) {
			rec := do(t, h, tc.method, tc.target)
			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("状态码 = %d，期望 500", rec.Code)
			}
			if got := decodeError(t, rec).Code; got != string(s3err.CodeInternalError) {
				t.Errorf("Code = %q，期望 InternalError", got)
			}
		})
	}

	// HEAD 单独处理：按 HTTP 语义它不写响应体，因此只能断言状态码。
	// 代价是客户端看不到错误码 —— 这是 HEAD 固有的限制，不是本实现的疏忽。
	t.Run("HEAD /b", func(t *testing.T) {
		rec := do(t, h, http.MethodHead, "/b")
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("状态码 = %d，期望 500", rec.Code)
		}
		if rec.Body.Len() != 0 {
			t.Errorf("HEAD 响应体应为空，实际 %d 字节", rec.Body.Len())
		}
	})
}

// TestNoStoreStillReturns501ForUnimplemented 验证"功能不支持"与"存储没配好"
// 是两件独立的事：前者是这个二进制的永久事实，不该因一次配置失误变成 500。
func TestNoStoreStillReturns501ForUnimplemented(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := New(Options{Logger: logger, Region: "us-east-1", DisableAuth: true}).Handler()

	rec := do(t, h, http.MethodGet, "/my-bucket/object.txt")
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("状态码 = %d，期望 501（不应受 store 缺失影响）", rec.Code)
	}
}

func TestUnsupportedSubresource(t *testing.T) {
	h := bucketTestServer(t)
	do(t, h, http.MethodPut, "/sub")

	cases := []struct{ target, why string }{
		{"/sub?acl", "裸键形式，SDK 常这么发"},
		{"/sub?acl=", "带等号但无值，语义与裸键相同"},
		{"/sub?versioning&x=1", "与其他参数混用"},
		{"/sub?tagging", "桶标签未实现"},
	}
	for _, c := range cases {
		t.Run(c.target, func(t *testing.T) {
			rec := do(t, h, http.MethodGet, c.target)
			if rec.Code != http.StatusNotImplemented {
				t.Fatalf("状态码 = %d，期望 501（%s）", rec.Code, c.why)
			}
			if got := decodeError(t, rec).Code; got != string(s3err.CodeNotImplemented) {
				t.Errorf("Code = %q，期望 NotImplemented", got)
			}
		})
	}
}

// TestToS3ErrorMapping 直接表驱动测映射表。
//
// 单独测这一层是因为它是"存储领域概念 → 协议错误码"的**唯一**翻译点，
// 散在 handler 里就再也说不清"哪些存储错误会让客户端看到什么"。
func TestToS3ErrorMapping(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantCode   s3err.Code
		wantStatus int
	}{
		{"桶不存在", store.ErrBucketNotFound, s3err.CodeNoSuchBucket, http.StatusNotFound},
		{"桶已存在", store.ErrBucketExists, s3err.CodeBucketAlreadyOwnedByYou, http.StatusConflict},
		{"桶非空", store.ErrBucketNotEmpty, s3err.CodeBucketNotEmpty, http.StatusConflict},
		{"桶名非法", store.ErrInvalidBucketName, s3err.CodeInvalidBucketName, http.StatusBadRequest},
		{"key 非法", store.ErrInvalidKey, s3err.CodeInvalidArgument, http.StatusBadRequest},
		{"数据损坏", store.ErrCorrupt, s3err.CodeInternalError, http.StatusInternalServerError},
		{"磁盘满", syscall.ENOSPC, s3err.CodeSlowDown, http.StatusServiceUnavailable},
		{"权限不足", fs.ErrPermission, s3err.CodeAccessDenied, http.StatusForbidden},
		{"未知错误", errors.New("谁也想不到的错"), s3err.CodeInternalError, http.StatusInternalServerError},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := toS3Error(c.err)
			if got == nil {
				t.Fatal("toS3Error 返回 nil")
			}
			if got.Code != c.wantCode {
				t.Errorf("Code = %q，期望 %q", got.Code, c.wantCode)
			}
			if got.HTTPStatus != c.wantStatus {
				t.Errorf("HTTPStatus = %d，期望 %d", got.HTTPStatus, c.wantStatus)
			}
		})
	}

	if toS3Error(nil) != nil {
		t.Error("toS3Error(nil) 应返回 nil")
	}
}

// TestCorruptDoesNotMasqueradeAs404 验证损坏绝不被翻译成"不存在"。
//
// 这是整个错误映射里最要紧的一条：把损坏报成 404，用户会以为数据本来
// 就没有而放弃排查，真实的数据损坏就此被掩盖。
func TestCorruptDoesNotMasqueradeAs404(t *testing.T) {
	serr := toS3Error(store.ErrCorrupt)
	if serr.HTTPStatus == http.StatusNotFound {
		t.Fatal("数据损坏被翻译成了 404 —— 这正是必须避免的")
	}
	if !strings.Contains(serr.Message, "损坏") {
		t.Errorf("错误消息应说明是损坏: %q", serr.Message)
	}
}

// TestWrappedStoreErrorStillMaps 验证 fsErr 包裹后仍能正确映射。
//
// store 的所有错误都经过 pathError 包装，若映射依赖裸比较就会全线失效 ——
// 这类"包了一层就认不出来"的 bug 只能靠测试守住。
func TestWrappedStoreErrorStillMaps(t *testing.T) {
	wrapped := &wrappedErr{store.ErrBucketNotFound}
	if got := toS3Error(wrapped).Code; got != s3err.CodeNoSuchBucket {
		t.Errorf("包裹后的 ErrBucketNotFound 映射为 %q，期望 NoSuchBucket", got)
	}
}

type wrappedErr struct{ err error }

func (e *wrappedErr) Error() string { return "上下文: " + e.err.Error() }
func (e *wrappedErr) Unwrap() error { return e.err }

func TestISO8601Format(t *testing.T) {
	// 毫秒精度是必须的：部分 SDK 严格按这个格式解析，省略毫秒会失败。
	got := iso8601(time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC))
	if !strings.HasSuffix(got, "Z") {
		t.Errorf("时间为 UTC 且以 Z 结尾: %q", got)
	}
	if len(got) != len("2006-01-02T15:04:05.000Z") {
		t.Errorf("时间格式长度 = %d，期望 %d: %q",
			len(got), len("2006-01-02T15:04:05.000Z"), got)
	}
	if got != "2026-10-23T12:00:00.000Z" {
		t.Errorf("iso8601 = %q，期望 2026-10-23T12:00:00.000Z", got)
	}
}
