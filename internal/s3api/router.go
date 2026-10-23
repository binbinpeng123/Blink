// Package s3api 实现 S3 兼容的 HTTP 接口层。
//
// 路由、中间件与错误响应统一在本包内，业务逻辑委托给 internal/store。
// 本包不直接操作文件系统，也不感知数据布局，以便存储后端可替换。
package s3api

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/binbinpeng123/Blink/internal/s3err"
)

// Options 是构造 HTTP 处理器所需的依赖。
type Options struct {
	// Logger 是基础 logger，每个请求会派生带 request_id 的子 logger。
	Logger *slog.Logger
	// Region 用于响应中的 x-amz-bucket-region 与签名校验。
	Region string
}

// Server 承载路由与共享依赖。
type Server struct {
	logger *slog.Logger
	region string
}

// New 构造 Server。logger 为 nil 时使用 slog 默认 logger，避免调用方崩溃。
func New(opt Options) *Server {
	logger := opt.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{logger: logger, region: opt.Region}
}

// Handler 返回装好全部中间件的 http.Handler。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// 非 S3 端点，免签名。放在鉴权中间件之前注册的白名单里。
	mux.HandleFunc("/healthz", handleHealthz)
	mux.HandleFunc("/readyz", handleReadyz)
	mux.HandleFunc("/version", handleVersion)

	// S3 端点统一入口，路径解析在 dispatch 中完成。
	mux.HandleFunc("/", s.dispatch)

	var h http.Handler = mux
	h = withRequestContext(s.logger, h)
	h = recoverPanic(s.logger, h)
	return h
}

// dispatch 是 S3 请求的统一入口。
//
// 切片 1 只完成路径解析与"未实现"兜底：任何 S3 请求都返回
// 501 NotImplemented，而不是泛化的 404。这样 `aws s3 ls` 会得到
// 明确的 NotImplemented（证明路由与错误体格式正确），
// 切片 2 接入签名校验后，改为"签名失败 403 / 签名通过再 501"。
func (s *Server) dispatch(w http.ResponseWriter, r *http.Request) {
	reqID := requestIDFrom(r.Context())
	logger := loggerFrom(r.Context(), s.logger)

	bucket, key := splitPath(r.URL.Path)

	if bucket == "" {
		// 根路径：ListBuckets（切片 6）。当前显式声明未实现。
		s3err.New(s3err.CodeNotImplemented).Write(w, r, reqID)
		return
	}

	logger.Debug("s3_request_not_implemented",
		slog.String("bucket", bucket),
		slog.String("key", key),
		slog.String("query", r.URL.RawQuery),
	)
	s3err.New(s3err.CodeNotImplemented).Write(w, r, reqID)
}

// splitPath 将 URL 路径拆成 bucket 与 key。
//
// 路径语义遵循 S3：路径分三段 /{bucket}/{key...}，其中 key 允许包含 "/"。
// 注意此处**不做 URL 解码** —— 解码后的 key 交给对象层处理，
// 而签名校验必须使用原始字节（见文档 02 的 SigV4 易错点 ①）。
func splitPath(p string) (bucket, key string) {
	p = strings.TrimPrefix(p, "/")
	if p == "" {
		return "", ""
	}
	if i := strings.Index(p, "/"); i >= 0 {
		return p[:i], p[i+1:]
	}
	return p, ""
}

// s3errMethodNotAllowed 返回 405，用于非 S3 端点上的非法方法。
func s3errMethodNotAllowed(w http.ResponseWriter, r *http.Request) {
	s3err.New(s3err.CodeMethodNotAllowed).Write(w, r, requestIDFrom(r.Context()))
}
