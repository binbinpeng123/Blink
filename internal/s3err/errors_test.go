package s3err

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestTableMatchesContract 把文档 02 的错误码表固化成测试。
// 任何状态码改动都会在这里失败，强制同步评审文档。
func TestTableMatchesContract(t *testing.T) {
	want := map[Code]int{
		CodeNoSuchBucket:                 http.StatusNotFound,
		CodeNoSuchKey:                    http.StatusNotFound,
		CodeBucketAlreadyOwnedByYou:      http.StatusConflict,
		CodeBucketAlreadyExists:          http.StatusConflict,
		CodeBucketNotEmpty:               http.StatusConflict,
		CodeInvalidBucketName:            http.StatusBadRequest,
		CodeInvalidArgument:              http.StatusBadRequest,
		CodeInvalidRange:                 http.StatusRequestedRangeNotSatisfiable,
		CodeNoSuchUpload:                 http.StatusNotFound,
		CodeEntityTooSmall:               http.StatusBadRequest,
		CodeInvalidPart:                  http.StatusBadRequest,
		CodeInvalidPartOrder:             http.StatusBadRequest,
		CodeBadDigest:                    http.StatusBadRequest,
		CodeInvalidDigest:                http.StatusBadRequest,
		CodeMalformedXML:                 http.StatusBadRequest,
		CodeMethodNotAllowed:             http.StatusMethodNotAllowed,
		CodePreconditionFailed:           http.StatusPreconditionFailed,
		CodeNotImplemented:               http.StatusNotImplemented,
		CodeAccessDenied:                 http.StatusForbidden,
		CodeInvalidAccessKeyID:           http.StatusForbidden,
		CodeSignatureDoesNotMatch:        http.StatusForbidden,
		CodeRequestTimeTooSkewed:         http.StatusForbidden,
		CodeMissingSecurityHeader:        http.StatusBadRequest,
		CodeAuthorizationHeaderMalformed: http.StatusBadRequest,
		CodeSlowDown:                     http.StatusServiceUnavailable,
		CodeInternalError:                http.StatusInternalServerError,
		CodeLengthRequired:               http.StatusLengthRequired,
	}

	if len(table) != len(want) {
		t.Errorf("错误码数量 = %d, 契约要求 %d", len(table), len(want))
	}
	for code, status := range want {
		e, ok := table[code]
		if !ok {
			t.Errorf("缺少错误码 %s", code)
			continue
		}
		if e.HTTPStatus != status {
			t.Errorf("%s 状态码 = %d, 契约要求 %d", code, e.HTTPStatus, status)
		}
		if e.Message == "" {
			t.Errorf("%s 缺少消息", code)
		}
		if e.Code != code {
			t.Errorf("表中 %s 的 Code 字段 = %s", code, e.Code)
		}
	}
}

func TestGetUnknownFallsBackToInternalError(t *testing.T) {
	e := Get(Code("NoSuchErrorCodeAtAll"))
	if e.Code != CodeInternalError {
		t.Errorf("未知错误码应回落到 InternalError，实际 %s", e.Code)
	}
	// 必须返回表中的实例，而不是调用方可改写的副本。
	if e != table[CodeInternalError] {
		t.Error("应返回表中的规范实例")
	}
}

func TestWithMessageKeepsCodeAndStatus(t *testing.T) {
	orig := Get(CodeInvalidArgument)
	cp := orig.WithMessage("bucket name is empty")

	if cp.Code != orig.Code || cp.HTTPStatus != orig.HTTPStatus {
		t.Error("WithMessage 不应改变 Code 与状态码")
	}
	if cp.Message == orig.Message {
		t.Error("WithMessage 应替换消息")
	}
	if orig.Message == "bucket name is empty" {
		t.Error("WithMessage 修改了共享的规范实例")
	}
}

func TestWriteErrorBody(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/my-bucket/missing.txt", nil)

	Get(CodeNoSuchKey).Write(rec, req, "17A0F3C2B8D4E5F6")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("状态码 = %d, 期望 404", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/xml") {
		t.Errorf("Content-Type = %q", ct)
	}

	var got struct {
		Code      string `xml:"Code"`
		Message   string `xml:"Message"`
		Resource  string `xml:"Resource"`
		RequestID string `xml:"RequestId"`
		HostID    string `xml:"HostId"`
	}
	if err := xml.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("响应体不是合法 XML: %v (%s)", err, rec.Body.String())
	}
	if got.Code != string(CodeNoSuchKey) {
		t.Errorf("Code = %q", got.Code)
	}
	if got.Resource != "/my-bucket/missing.txt" {
		t.Errorf("Resource = %q", got.Resource)
	}
	if got.RequestID != "17A0F3C2B8D4E5F6" {
		t.Errorf("RequestId = %q", got.RequestID)
	}
	if got.HostID != HostID {
		t.Errorf("HostId = %q, 期望 %q", got.HostID, HostID)
	}
}

// TestWriteHeadNoBody 验证 HEAD 请求按 HTTP 语义不返回响应体。
func TestWriteHeadNoBody(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodHead, "/my-bucket/missing.txt", nil)

	Get(CodeNoSuchKey).Write(rec, req, "ID")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("状态码 = %d, 期望 404", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("HEAD 不应有响应体，实际 %d 字节", rec.Body.Len())
	}
}

func TestErrorImplementsErrorInterface(t *testing.T) {
	var err error = Get(CodeNoSuchBucket)
	if !strings.Contains(err.Error(), string(CodeNoSuchBucket)) {
		t.Errorf("Error() = %q", err.Error())
	}
}
