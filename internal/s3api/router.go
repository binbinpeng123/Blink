// Package s3api 实现 S3 兼容的 HTTP 接口层。
//
// 路由、中间件与错误响应统一在本包内，业务逻辑委托给 internal/store。
// 本包不直接操作文件系统，也不感知数据布局，以便存储后端可替换。
package s3api

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/binbinpeng123/Blink/internal/s3err"
	"github.com/binbinpeng123/Blink/internal/sigv4"
	"github.com/binbinpeng123/Blink/internal/store"
)

// Options 是构造 HTTP 处理器所需的依赖。
type Options struct {
	// Logger 是基础 logger，每个请求会派生带 request_id 的子 logger。
	Logger *slog.Logger
	// Region 用于响应中的 x-amz-bucket-region 与签名校验。
	Region string
	// Provider 按 Access Key 查凭据。为 nil 时**拒绝启动**而非放行 ——
	// 让"忘配凭据"表现为明确的启动失败，而不是静默的鉴权绕过。
	Provider sigv4.Provider
	// MaxClockSkew 是签名时间容差，为 0 时用 15 分钟。
	MaxClockSkew time.Duration
	// DisableAuth 跳过签名校验，仅供本地调试与集成测试。
	DisableAuth bool
	// Store 是存储引擎。为 nil 时所有 S3 数据操作返回
	// 500 InternalError 而非 panic —— 见 requireStore。
	Store *store.Store
}

// Server 承载路由与共享依赖。
type Server struct {
	logger *slog.Logger
	region string
	auth   AuthOptions
	store  *store.Store
}

// New 构造 Server。logger 为 nil 时使用 slog 默认 logger，避免调用方崩溃。
func New(opt Options) *Server {
	logger := opt.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		logger: logger,
		region: opt.Region,
		store:  opt.Store,
		auth: AuthOptions{
			Provider:     opt.Provider,
			Region:       opt.Region,
			MaxClockSkew: opt.MaxClockSkew,
			Disabled:     opt.DisableAuth,
		},
	}
}

// requireStore 在存储未接入时回 500，并明确说明原因。
//
// 之所以不 panic：本包同时被 /healthz 之类的免存储端点使用，
// 缺少 store 不该让进程在启动时就崩 —— 那会让"只想跑个探针"的场景无法调试。
// 但数据操作必须失败，绝不能"静默返回空结果"，否则客户端会把
// "服务没配好"误读成"桶里没东西"。
func (s *Server) requireStore(w http.ResponseWriter, r *http.Request, reqID string) bool {
	if s.store != nil {
		return true
	}
	s3err.New(s3err.CodeInternalError).
		WithMessage("存储层未初始化。").
		Write(w, r, reqID)
	return false
}

// Handler 返回装好全部中间件的 http.Handler。
//
// 中间件顺序（由外到内）：
//
//	withRequestContext → recoverPanic → withAuth → mux
//
// request_id 必须在最外层生成，否则鉴权失败的响应里没有可对照的 ID；
// recoverPanic 必须在鉴权之前，否则签名代码里的 panic 会变成连接中断而非 500。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// 非 S3 端点，免签名，白名单见 unauthenticatedPaths。
	mux.HandleFunc("/healthz", handleHealthz)
	mux.HandleFunc("/readyz", handleReadyz)
	mux.HandleFunc("/version", handleVersion)

	// S3 端点统一入口，路径解析在 dispatch 中完成。
	mux.HandleFunc("/", s.dispatch)

	var h http.Handler = mux
	h = withAuth(s.auth, h)
	h = recoverPanic(s.logger, h)
	h = withRequestContext(s.logger, h)
	return h
}

// dispatch 是 S3 请求的统一入口。
//
// 能走到这里的请求要么签名有效，要么命中了免签白名单（见 withAuth）。
//
// 分发顺序刻意是"先判不支持项、再判实现项"：`?acl`、`?tagging` 这类
// 子资源必须显式回 501 NotImplemented，绝不能落到泛化 404 ——
// 后者会让用户以为"这个桶不存在"，而不是"这个功能没实现"。
//
// 切片 3 实现 bucket 级操作；对象级与 LIST 仍回 501，将在后续切片替换。
func (s *Server) dispatch(w http.ResponseWriter, r *http.Request) {
	reqID := requestIDFrom(r.Context())
	logger := loggerFrom(r.Context(), s.logger)

	bucket, key := splitPath(r.URL.Path)

	// 未实现的子资源先拦下，避免被后续分支误吞。
	// 完整清单见 docs/02「不支持项」。
	if sub := unsupportedSubresource(r.URL.RawQuery); sub != "" {
		logger.Debug("unsupported_subresource",
			slog.String("subresource", sub),
			slog.String("bucket", bucket),
			slog.String("key", key),
		)
		s3err.New(s3err.CodeNotImplemented).Write(w, r, reqID)
		return
	}

	// requireStore 只加在**真正会碰存储**的分支上，而不是 dispatch 入口。
	// 这样 501 的答复与存储是否就绪无关 —— "本版本不支持该功能"是关于
	// 这个二进制的永久事实，不该因为一次配置失误而变成 500。
	switch {
	case bucket == "":
		// 根路径：ListBuckets。
		if r.Method == http.MethodGet {
			if !s.requireStore(w, r, reqID) {
				return
			}
			s.handleListBuckets(w, r, reqID)
			return
		}
	case key == "":
		// 桶级操作。
		switch r.Method {
		case http.MethodPut:
			if !s.requireStore(w, r, reqID) {
				return
			}
			s.handleCreateBucket(w, r, bucket, reqID)
			return
		case http.MethodDelete:
			if !s.requireStore(w, r, reqID) {
				return
			}
			s.handleDeleteBucket(w, r, bucket, reqID)
			return
		case http.MethodHead:
			if !s.requireStore(w, r, reqID) {
				return
			}
			s.handleHeadBucket(w, r, bucket, reqID)
			return
		}
	}

	logger.Debug("s3_request_not_implemented",
		slog.String("method", r.Method),
		slog.String("bucket", bucket),
		slog.String("key", key),
		slog.String("query", r.URL.RawQuery),
	)
	s3err.New(s3err.CodeNotImplemented).Write(w, r, reqID)
}

// unsupportedSubresources 是本版本**明确不支持**的查询子资源。
//
// 之所以要显式列出而不是一律忽略：S3 客户端会在自己需要某项能力时
// 才带上这些参数（例如 `--acl public-read` 触发 `?acl`），
// 静默忽略等于"假装设置成功"，用户会在很久以后才发现权限没生效。
// 见 docs/02。
var unsupportedSubresources = map[string]bool{
	"acl": true, "tagging": true, "cors": true, "lifecycle": true,
	"versioning": true, "policy": true, "replication": true, "versions": true,
	"restore": true, "torrent": true, "encryption": true, "logging": true,
	"notification": true, "website": true, "accelerate": true,
	"requestPayment": true, "object-lock": true, "legal-hold": true,
	"retention": true, "inventory": true, "metrics": true, "analytics": true,
	"publicAccessBlock": true, "ownershipControls": true, "intelligent-tiering": true,
}

// unsupportedSubresource 返回查询串中命中的不支持子资源名；无则返回空串。
//
// 只看键、不看值：`?acl` 与 `?acl=` 语义相同，都应被拒绝。
// 不解析成 url.Values 是为了与签名校验保持同一套"原始查询串"处理方式。
func unsupportedSubresource(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}
	for _, pair := range strings.Split(rawQuery, "&") {
		name, _, _ := strings.Cut(pair, "=")
		if unsupportedSubresources[name] {
			return name
		}
	}
	return ""
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
