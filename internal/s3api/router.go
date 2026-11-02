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
//	withRequestContext → recoverPanic → withCORS → withAuth → mux
//
// request_id 必须在最外层生成，否则鉴权失败的响应里没有可对照的 ID；
// recoverPanic 必须在鉴权之前，否则签名代码里的 panic 会变成连接中断而非 500；
// withCORS 必须在 withAuth 之前，因为浏览器的预检请求不携带 Authorization。
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
	// CORS 在鉴权之外侧：预检请求不带 Authorization，必须先于签名校验处理。
	h = s.withCORS(h)
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
// 当前已实现桶生命周期、对象 CRUD、ListObjectsV2 与分片上传；
// ListObjects V1 等仍未实现的组合回 501，将在后续切片替换。
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

	// 桶级 CORS 子资源（?cors）同样在通用方法分派之前处理。
	if s.dispatchCORS(w, r, bucket, key, reqID) {
		return
	}

	// 分片上传的四个子资源在通用方法分派**之前**处理：它们的语义由
	// 子资源决定（同一路径上的 POST 既可能是 Complete 也可能是 Create），
	// 先按方法分派会立刻分错。
	if s.dispatchMultipart(w, r, bucket, key, reqID) {
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
		case http.MethodGet:
			// GET /{bucket} 只在 list-type=2 时是合法操作（ListObjectsV2）。
			// list-type=1（ListObjects V1）不在 02 文档的契约内，回 501；
			// 其他值不是合法参数，回 400 而不是 501 —— 前者是"这个版本
			// 没实现"，后者是"你写错了"，两者对客户端的意义不同。
			switch r.URL.Query().Get("list-type") {
			case "2":
				if !s.requireStore(w, r, reqID) {
					return
				}
				s.handleListObjectsV2(w, r, bucket, reqID)
				return
			case "", "1":
				// 落到兜底 501。
			default:
				invalidArg("list-type 只支持 \"2\"。").
					Write(w, r, reqID)
				return
			}
		}
	case key != "":
		// 对象级操作。
		switch r.Method {
		case http.MethodPut:
			if !s.requireStore(w, r, reqID) {
				return
			}
			s.handlePutObject(w, r, bucket, key, reqID)
			return
		case http.MethodGet:
			if !s.requireStore(w, r, reqID) {
				return
			}
			s.handleGetObject(w, r, bucket, key, reqID)
			return
		case http.MethodHead:
			if !s.requireStore(w, r, reqID) {
				return
			}
			s.handleHeadObject(w, r, bucket, key, reqID)
			return
		case http.MethodDelete:
			if !s.requireStore(w, r, reqID) {
				return
			}
			s.handleDeleteObject(w, r, bucket, key, reqID)
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
	"acl": true, "tagging": true, "lifecycle": true,
	"versioning": true, "policy": true, "replication": true, "versions": true,
	"restore": true, "torrent": true, "encryption": true, "logging": true,
	"notification": true, "website": true, "accelerate": true,
	"requestPayment": true, "object-lock": true, "legal-hold": true,
	"retention": true, "inventory": true, "metrics": true, "analytics": true,
	"publicAccessBlock": true, "ownershipControls": true, "intelligent-tiering": true,
}

// dispatchMultipart 处理分片上传的四个子资源。
//
// 返回 true 表示请求已被处理（成功或已写出错误响应），调用方必须立即返回。
// 返回 false 表示这个请求与分片上传无关，交回通用分派。
//
// 子资源与方法的三条硬约束（02 文档）：
//
//	POST /{bucket}/{key}?uploads          CreateMultipartUpload
//	PUT  /{bucket}/{key}?partNumber&uploadId  UploadPart
//	POST /{bucket}/{key}?uploadId         CompleteMultipartUpload
//	DELETE /{bucket}/{key}?uploadId       AbortMultipartUpload
//	GET  /{bucket}/{key}?uploadId         ListParts
//	GET  /{bucket}?uploads                ListMultipartUploads
//
// `?partNumber` 单独出现是 GetObject 的分片下载子资源，本版本仍回 501；
// 明确拒绝而不是让请求落到普通 GET —— 否则客户端会拿到整个对象，
// 却以为那是第 N 片，拼出重复数据。
func (s *Server) dispatchMultipart(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) bool {
	q := r.URL.Query()

	if q.Has("uploads") {
		switch {
		case key != "" && r.Method == http.MethodPost:
			if !s.requireStore(w, r, reqID) {
				return true
			}
			s.handleCreateMultipartUpload(w, r, bucket, key, reqID)
		case key == "" && r.Method == http.MethodGet:
			if !s.requireStore(w, r, reqID) {
				return true
			}
			s.handleListMultipartUploads(w, r, bucket, reqID)
		default:
			// 例如 PUT /{bucket}?uploads：S3 没有这个操作。
			notImplemented("不支持的子资源组合：?uploads 只接受 POST /{bucket}/{key} 与 GET /{bucket}。").
				Write(w, r, reqID)
		}
		return true
	}

	// 桶级没有 uploadId 子资源；交回通用分派（最终是 501）。
	if key == "" {
		return false
	}

	if !q.Has("uploadId") {
		if q.Has("partNumber") {
			// 分片下载（GetObject partNumber）不在本版本范围内。
			notImplemented("暂不支持按 partNumber 下载对象分片。").
				Write(w, r, reqID)
			return true
		}
		return false
	}

	switch r.Method {
	case http.MethodPut:
		if !q.Has("partNumber") {
			// 带 uploadId 的 PUT 只能是 UploadPart；缺 partNumber 时
			// 静默当成覆盖整个对象，会用一个分片的内容毁掉已有对象。
			invalidArg("UploadPart 请求必须同时提供 partNumber。").
				Write(w, r, reqID)
			return true
		}
		if !s.requireStore(w, r, reqID) {
			return true
		}
		s.handleUploadPart(w, r, bucket, key, reqID)
	case http.MethodPost:
		if !s.requireStore(w, r, reqID) {
			return true
		}
		s.handleCompleteMultipartUpload(w, r, bucket, key, reqID)
	case http.MethodDelete:
		if !s.requireStore(w, r, reqID) {
			return true
		}
		s.handleAbortMultipartUpload(w, r, bucket, key, reqID)
	case http.MethodGet:
		if q.Has("partNumber") {
			notImplemented("暂不支持按 partNumber 下载对象分片。").
				Write(w, r, reqID)
			return true
		}
		if !s.requireStore(w, r, reqID) {
			return true
		}
		s.handleListParts(w, r, bucket, key, reqID)
	default:
		// HEAD /{bucket}/{key}?uploadId 之类：没有对应语义。
		// 必须在这里明确回 501，不能交回通用分派 —— 那会让 HEAD 带着
		// uploadId 落到普通 GET/HEAD 对象，客户端会以为拿到的元数据
		// 是在描述这次分片上传。
		notImplemented("该 uploadId 子资源不支持此 HTTP 方法。").
			Write(w, r, reqID)
	}
	return true
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
//
// 入参**必须**是 r.URL.Path：net/http 在解析请求行时已经对它做过一次
// 百分号解码，因此这里的返回值就是对象层要用的 key。
// handler 绝不能再调 url.PathUnescape —— 那是对已解码的值再解一次，
// 名为 `a%252Fb` 的 key 会被读成 `a/b`，静默命中另一个对象。
//
// 签名校验走的是另一条路径：它用未解码的 r.URL.EscapedPath() 与
// r.URL.RawQuery 逐字节计算（文档 02 的 SigV4 易错点 ①），两者互不影响。
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

// notImplemented 构造 501 NotImplemented，并附上"为什么不做"的说明。
func notImplemented(msg string) *s3err.Error {
	return s3err.New(s3err.CodeNotImplemented).WithMessage(msg)
}

// s3errMethodNotAllowed 返回 405，用于非 S3 端点上的非法方法。
func s3errMethodNotAllowed(w http.ResponseWriter, r *http.Request) {
	s3err.New(s3err.CodeMethodNotAllowed).Write(w, r, requestIDFrom(r.Context()))
}
