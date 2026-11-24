package s3api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"time"
)

// ctxKey 是本包私有的 context 键类型，避免与外部包的键冲突。
type ctxKey int

const (
	ctxKeyRequestID ctxKey = iota
	ctxKeyLogger
)

// requestIDFrom 从 context 中取出请求 ID；缺失时返回空串。
func requestIDFrom(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyRequestID).(string); ok {
		return v
	}
	return ""
}

// loggerFrom 从 context 中取出带请求上下文的 logger；缺失时返回传入的兜底 logger。
func loggerFrom(ctx context.Context, fallback *slog.Logger) *slog.Logger {
	if v, ok := ctx.Value(ctxKeyLogger).(*slog.Logger); ok {
		return v
	}
	return fallback
}

// statusRecorder 包装 ResponseWriter 以记录实际写出的状态码与字节数。
//
// 同时透传 http.Flusher 与 io.ReaderFrom，使大对象下载的流式写出与
// 零拷贝路径不被中间件挡住。
type statusRecorder struct {
	http.ResponseWriter
	status  int
	written int64
}

// WriteHeader 记录状态码并转发。
func (s *statusRecorder) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

// Write 记录写入字节数，并在首次写入时补记隐式 200。
func (s *statusRecorder) Write(p []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	n, err := s.ResponseWriter.Write(p)
	s.written += int64(n)
	return n, err
}

// Flush 透传 Flush，保证流式响应可用。
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// writerOnly 隐藏 WriteTo/ReadFrom，避免 io.Copy 在包装层之间递归。
//
// 它存在的理由是 ReadFrom 的实现：把 src 交给底层 ResponseWriter 的
// ReadFrom 之前，必须保证底层不会反过来调用本类型的 ReadFrom。
type writerOnly struct{ io.Writer }

// ReadFrom 透传底层 ResponseWriter 的 ReadFrom，同时累计字节数。
//
// 这一层不是可选的性能调优：io.Copy 的选择顺序是"先看 src 是否
// 实现 WriterTo，再看 dst 是否实现 ReaderFrom"。`os.File` 有
// WriteTo，但它内部只对 *net.TCPConn 之类带网络信息的 writer 走
// sendfile；面对包装类型会退回用户态拷贝。真正能触发零拷贝的是
// net/http 的 *response.ReadFrom：它先窥探 SniffLen 字节补齐响应头，
// 再把剩余数据交给底层 *TCPConn.ReadFrom → sendfile(2)。
//
// 若不透传，本类型只有 Write，net/http 的零拷贝分支永远不可达，
// 大对象下载全部退化为一趟用户态 read/write —— 16 MiB 端到端实测
// 吞吐约差 2.2 倍（docs/reviews/09）。
func (s *statusRecorder) ReadFrom(src io.Reader) (int64, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	rf, ok := s.ResponseWriter.(io.ReaderFrom)
	if !ok {
		// 底层不支持 ReadFrom：退回普通拷贝。目标必须是底层 writer 而不是
		// 本类型，否则 io.Copy 会再次选中本方法形成无穷递归；而记数由这里
		// 统一完成，不能靠 s.Write 累加，否则会与下面的 += n 重复计数。
		n, err := io.Copy(writerOnly{s.ResponseWriter}, src)
		s.written += n
		return n, err
	}
	n, err := rf.ReadFrom(src)
	s.written += n
	return n, err
}

// status 返回记录到的状态码，未写出时为 200。
func (s *statusRecorder) statusCode() int {
	if s.status == 0 {
		return http.StatusOK
	}
	return s.status
}

// withRequestContext 为每个请求生成请求 ID、注入 logger，并在结束时记录访问日志与指标。
//
// 请求 ID 同时写入响应头，客户端报障时可直接提供该 ID 定位日志。
// 指标在这里统一更新：所有请求（含鉴权失败与探针）都要经过本中间件，
// 且 statusRecorder 已经拿到最终状态码与字节数。
func withRequestContext(logger *slog.Logger, m *serverMetrics, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := newRequestID()

		w.Header().Set(headerRequestID, reqID)
		w.Header().Set(headerServer, serverName)

		reqLogger := logger.With(
			slog.String("request_id", reqID),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.String("remote", r.RemoteAddr),
		)

		ctx := context.WithValue(r.Context(), ctxKeyRequestID, reqID)
		ctx = context.WithValue(ctx, ctxKeyLogger, reqLogger)

		rec := &statusRecorder{ResponseWriter: w}
		start := time.Now()

		next.ServeHTTP(rec, r.WithContext(ctx))

		elapsed := time.Since(start)
		reqLogger.Info("http_request",
			slog.Int("status", rec.statusCode()),
			slog.Int64("bytes", rec.written),
			slog.Duration("elapsed", elapsed),
		)
		m.observeRequest(r.Method, rec.statusCode(), rec.written, elapsed)
	})
}

// recoverPanic 捕获 handler 中的 panic，返回 500 InternalError 而不是断开连接。
//
// 由于响应可能已部分写出，此处只做尽力而为的兜底；真正的问题是 panic 本身，
// 因此必须把堆栈写进日志。
func recoverPanic(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			rec := recover()
			if rec == nil {
				return
			}
			// http.ErrAbortHandler 是服务器主动中止响应的约定信号，需原样抛出。
			if rec == http.ErrAbortHandler {
				panic(rec)
			}
			loggerFrom(r.Context(), logger).Error("panic_recovered",
				slog.Any("panic", rec),
				slog.String("stack", stackTrace()),
			)
			writeErrorInternal(w, r)
		}()
		next.ServeHTTP(w, r)
	})
}
