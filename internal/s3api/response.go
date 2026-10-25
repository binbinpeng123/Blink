package s3api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"runtime"
	"strings"

	"github.com/binbinpeng123/Blink/internal/s3err"
)

// 服务端通用响应头。
const (
	headerRequestID  = "x-amz-request-id"
	headerServerTime = "Date"
	headerServer     = "Server"

	// serverName 出现在 Server 头与日志中，便于识别部署实例。
	serverName = "blink"
)

// newRequestID 生成 16 位大写十六进制的请求 ID，形如 17A0F3C2B8D4E5F6。
//
// 该 ID 会回写给客户端、写入日志，并在错误响应体中作为 RequestId，
// 因此必须足够随机以避免被用于关联或推断内部状态。
func newRequestID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 失败极为罕见；此时退化为全零 ID 而非中断请求，
		// 因为请求 ID 只用于排障，不影响数据正确性。
		return "0000000000000000"
	}
	return strings.ToUpper(hex.EncodeToString(b[:]))
}

// writeJSON 以 JSON 形式返回响应，非 S3 端点（/readyz、/version）使用。
func writeJSON(w http.ResponseWriter, r *http.Request, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if r != nil && r.Method == http.MethodHead {
		return
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(body)
}

// stackTrace 返回当前 goroutine 的调用栈，仅在 panic 兜底路径使用。
func stackTrace() string {
	buf := make([]byte, 8192)
	n := runtime.Stack(buf, false)
	return string(buf[:n])
}

// writeErrorInternal 是 panic 兜底路径的错误写出：此时上下文可能已不可用，
// 直接使用最保守的固定响应。
func writeErrorInternal(w http.ResponseWriter, r *http.Request) {
	s3err.New(s3err.CodeInternalError).Write(w, r, requestIDFrom(r.Context()))
}

// writeXMLBody 以 XML 形式返回响应，S3 端点使用。
func writeXMLBody(w http.ResponseWriter, r *http.Request, status int, body any) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(status)
	if r != nil && r.Method == http.MethodHead {
		return
	}
	if _, err := w.Write([]byte(xml.Header)); err != nil {
		return
	}
	_ = xml.NewEncoder(w).Encode(body)
}
