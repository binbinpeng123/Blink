package s3api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/binbinpeng123/Blink/internal/authz"
)

// withAudit 输出单行审计日志 `s3_audit`。
//
// 位置：在 withAuth 之内、业务 handler 之外。只有校验通过的请求
// 才会走到这里，因此日志里的 access_key 是**已验证**的身份，
// 而不是从 Authorization 头里猜出来的字符串；鉴权失败的请求由
// `auth_failed` 记录，两者不重叠。
//
// 刻意不记录的内容：请求体、Authorization 头原文、query string
// （预签名 URL 的 X-Amz-Signature 与临时凭据就在 query 里）、Secret Key。
// key 按原样记录：审计需要知道"动了哪个对象"，这与"不回显签名"不冲突。
func (s *Server) withAudit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 探针与指标抓取不属于 S3 数据访问，记进来只会淹没真正的审计事件。
		// OPTIONS 预检不携带身份，也已在 CORS 中间件里应答。
		if unauthenticatedPaths[r.URL.Path] || r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}

		rec := &statusRecorder{ResponseWriter: w}
		start := time.Now()

		next.ServeHTTP(rec, r)

		bucket, key := splitPath(r.URL.Path)
		action, ok := classifyAction(r, bucket, key)
		if !ok && bucket == "" && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
			// ListBuckets 不按单一动作授权（结果按可见桶过滤），
			// 但审计里仍应记成 list 而不是空串。
			action = authz.ActionList
		}

		var (
			ak        string
			presigned bool
		)
		if ar := authFrom(r); ar != nil {
			ak = redactKeyID(ar.AccessKeyID)
			presigned = ar.Presigned
		}

		loggerFrom(r.Context(), s.logger).Info("s3_audit",
			slog.String("action", string(action)),
			slog.String("access_key", ak),
			slog.String("bucket", bucket),
			slog.String("key", key),
			slog.Int("status", rec.statusCode()),
			slog.Int64("bytes", rec.written),
			slog.Duration("elapsed", time.Since(start)),
			// remote 已由 withRequestContext 注入请求 logger，这里不再重复：
			// slog 的 JSON handler 不去重，重复键会让解析器只保留其中一个。
			slog.Bool("presigned", presigned),
		)
	})
}
