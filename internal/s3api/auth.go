package s3api

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/binbinpeng123/Blink/internal/logx"
	"github.com/binbinpeng123/Blink/internal/sigv4"
)

// AuthOptions 是鉴权中间件的依赖。
type AuthOptions struct {
	// Provider 按 Access Key 查凭据。
	Provider sigv4.Provider
	// Region 是本服务区域，用于校验凭据作用域。
	Region string
	// MaxClockSkew 为 0 时用 15 分钟。
	MaxClockSkew time.Duration
	// Disabled 为真时跳过全部签名校验。
	//
	// 该开关只为本地调试与集成测试提供，线上不得开启 ——
	// 开启意味着任何能连上端口的人都能读写全部数据。
	Disabled bool
}

// authResult 是校验通过后注入上下文的产物。
type authResult struct {
	AccessKeyID   string
	ContentSHA256 string
	Presigned     bool
	// ChunkSigner 仅签名分块载荷（STREAMING-AWS4-HMAC-SHA256-PAYLOAD*）
	// 非 nil；它是有状态的签名链，只能用于本次请求。
	ChunkSigner *sigv4.ChunkSigner
}

type authCtxKey int

const authCtxKeyResult authCtxKey = iota

// authFrom 从上下文取鉴权结果；未鉴权（白名单路径或 Disabled）时返回 nil。
func authFrom(r *http.Request) *authResult {
	v, _ := r.Context().Value(authCtxKeyResult).(*authResult)
	return v
}

// unauthenticatedPaths 是免签路径。
//
// 只放健康检查与版本信息：它们要么被 K8s 探针高频调用（不该要求签名），
// 要么本来就不含敏感信息。S3 路由一律不在此列。
var unauthenticatedPaths = map[string]bool{
	"/healthz": true,
	"/readyz":  true,
	"/version": true,
}

func (o AuthOptions) maxSkew() time.Duration {
	if o.MaxClockSkew <= 0 {
		return sigv4.DefaultMaxClockSkew
	}
	return o.MaxClockSkew
}

// withAuth 是鉴权中间件。
//
// 放在路由**之前**：这样 `/healthz` 与 S3 路由的分流只发生一次，
// 且任何新加的 S3 handler 都自动获得保护 —— 不存在"忘了加鉴权"的路径。
func withAuth(opt AuthOptions, next http.Handler) http.Handler {
	verifyOpt := sigv4.Options{
		Provider:     opt.Provider,
		Region:       opt.Region,
		MaxClockSkew: opt.maxSkew(),
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if opt.Disabled || unauthenticatedPaths[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}

		res, serr := sigv4.Verify(r, verifyOpt)
		if serr != nil {
			// 鉴权失败必须记日志：它是唯一能区分"客户端配错"与"有人在爆破"的信号。
			// 注意不记录请求体、Authorization 头原文或任何密钥片段。
			loggerFrom(r.Context(), nil).Warn("auth_failed",
				slog.String("code", string(serr.Code)),
				slog.String("message", serr.Message),
				slog.String("access_key", redactKeyID(accessKeyOf(r))),
			)
			serr.Write(w, r, requestIDFrom(r.Context()))
			return
		}

		loggerFrom(r.Context(), nil).Debug("auth_ok",
			slog.String("access_key", redactKeyID(res.AccessKeyID)),
			slog.Bool("presigned", res.Presigned),
			slog.Bool("payload_signed", res.PayloadSigned()),
		)

		ctx := context.WithValue(r.Context(), authCtxKeyResult, &authResult{
			AccessKeyID:   res.AccessKeyID,
			ContentSHA256: res.ContentSHA256,
			Presigned:     res.Presigned,
			ChunkSigner:   res.ChunkSigner,
		})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// accessKeyOf 尽力从请求中取出 Access Key，仅用于日志脱敏展示。
//
// 这里刻意只做"够用即可"的解析：日志字段不值得为畸形输入付出完整解析的代价，
// 取不到就返回空串，redactKeyID("") 会返回空串。
func accessKeyOf(r *http.Request) string {
	if v := r.Header.Get("Authorization"); v != "" {
		if _, rest, ok := strings.Cut(v, "Credential="); ok {
			cred, _, _ := strings.Cut(rest, ",")
			ak, _, _ := strings.Cut(strings.TrimSpace(cred), "/")
			return ak
		}
		return ""
	}
	if cred, ok := rawQueryValue(r.URL.RawQuery, "X-Amz-Credential"); ok {
		ak, _, _ := strings.Cut(cred, "/")
		return ak
	}
	return ""
}

// redactKeyID 对 Access Key 做脱敏，保留前 4 位便于对照排查。
func redactKeyID(id string) string { return logx.RedactKeyID(id) }

// rawQueryValue 在原始查询串中查找键并返回**已解码**的值。
//
// 这里可以从宽解码：它只服务于日志字段，不参与任何签名计算。
func rawQueryValue(raw, name string) (string, bool) {
	for _, seg := range strings.Split(raw, "&") {
		k, v, _ := strings.Cut(seg, "=")
		if dec, err := url.QueryUnescape(k); err == nil {
			k = dec
		}
		if k != name {
			continue
		}
		if dec, err := url.QueryUnescape(v); err == nil {
			return dec, true
		}
		return v, true
	}
	return "", false
}
