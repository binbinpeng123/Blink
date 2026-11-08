package s3api

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/binbinpeng123/Blink/internal/authz"
	"github.com/binbinpeng123/Blink/internal/s3err"
)

// errAccessDenied 是批量操作中单条 key 被拒绝时的哨兵。
//
// DeleteObjects 的每条结果都按 key 独立返回，不能因为其中一条无权限
// 就让整个请求写 403；用一个可识别的错误让它落到 Error 表里。
var errAccessDenied = errors.New("s3api: 无权限")

// classifyAction 把请求归类为授权动作。
//
// 返回 false 表示这条请求不触达任何数据（例如没有 ?delete 的桶级 POST），
// 随后会被 501 兜底。分类只看方法与子资源，不解析请求体。
func classifyAction(r *http.Request, bucket, key string) (authz.Action, bool) {
	// 根路径是 ListBuckets：不按单一动作过滤，由结果可见性过滤。
	if bucket == "" {
		return "", false
	}

	q := r.URL.Query()

	// ?cors 的读写删都属于桶级管理。
	if q.Has("cors") {
		return authz.ActionAdmin, true
	}

	// POST /{bucket}?delete：请求级按 delete 检查桶，handler 内逐 key 再查。
	if q.Has("delete") {
		return authz.ActionDelete, true
	}

	if q.Has("uploads") {
		// GET /{bucket}?uploads 是列举进行中的分片上传。
		if key == "" {
			if r.Method == http.MethodGet {
				return authz.ActionList, true
			}
			return "", false
		}
		// POST /{bucket}/{key}?uploads 是 CreateMultipartUpload。
		if r.Method == http.MethodPost {
			return authz.ActionWrite, true
		}
		return "", false
	}

	if q.Has("uploadId") {
		switch r.Method {
		case http.MethodGet:
			// ListParts：只读。
			return authz.ActionRead, true
		case http.MethodPut, http.MethodPost, http.MethodDelete:
			// UploadPart/UploadPartCopy、Complete、Abort。
			return authz.ActionWrite, true
		}
		return "", false
	}

	if key != "" {
		switch r.Method {
		case http.MethodGet, http.MethodHead:
			return authz.ActionRead, true
		case http.MethodPut:
			// 普通上传与复制目标都要求 write；复制源的 read 在 handler 内补查。
			return authz.ActionWrite, true
		case http.MethodDelete:
			return authz.ActionDelete, true
		}
		return "", false
	}

	switch r.Method {
	case http.MethodGet, http.MethodHead:
		// ListObjects（V1/V2）与 HeadBucket。
		return authz.ActionList, true
	case http.MethodPut, http.MethodDelete:
		// CreateBucket / DeleteBucket。
		return authz.ActionAdmin, true
	}
	return "", false
}

// authorizeRequest 是 dispatch 入口的统一授权检查。
//
// 拒绝时写出 403 AccessDenied 并返回 false。不区分"桶不存在"、
// "key 不存在"与"无权限"：可区分的答复会变成一次免费的资源探测。
func (s *Server) authorizeRequest(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) bool {
	action, ok := classifyAction(r, bucket, key)
	if !ok {
		// 不触达数据的请求（最终 501）不需要授权。
		return true
	}
	if s.accessAllowed(r, action, bucket, key) {
		return true
	}
	s3err.New(s3err.CodeAccessDenied).Write(w, r, reqID)
	return false
}

// authorizeSource 检查复制请求的源对象读权限。
//
// CopyObject 与 UploadPartCopy 的源可能是另一个桶、另一个前缀，
// 只检查目标上的 write 会让"有写权限"变成"能读全库"。
func (s *Server) authorizeSource(w http.ResponseWriter, r *http.Request, src copySource, reqID string) bool {
	if s.accessAllowed(r, authz.ActionRead, src.Bucket, src.Key) {
		return true
	}
	s3err.New(s3err.CodeAccessDenied).Write(w, r, reqID)
	return false
}

// accessAllowed 判断当前请求的凭据是否允许某个动作。
//
// 未鉴权（DisableAuth 或免签路径）时返回 true：此时没有策略可比，
// 该开关本身已经意味着"不做访问控制"。
func (s *Server) accessAllowed(r *http.Request, action authz.Action, bucket, key string) bool {
	ar := authFrom(r)
	if ar == nil {
		return true
	}
	if ar.Policy.Allow(action, bucket, key) {
		return true
	}
	s.recordDenied(r, action, bucket, key)
	return false
}

// bucketVisible 报告 ListBuckets 是否应包含该桶。
func (s *Server) bucketVisible(r *http.Request, bucket string) bool {
	ar := authFrom(r)
	if ar == nil {
		return true
	}
	return ar.Policy.VisibleBucket(bucket)
}

// recordDenied 记录一次授权拒绝。
//
// 日志与指标是发现"凭据配错"与"越权探测"的唯一信号；响应本身
// 刻意保持统一，不泄露任何资源是否存在。
func (s *Server) recordDenied(r *http.Request, action authz.Action, bucket, key string) {
	ak := ""
	if ar := authFrom(r); ar != nil {
		ak = redactKeyID(ar.AccessKeyID)
	}
	loggerFrom(r.Context(), s.logger).Warn("s3_authz_denied",
		slog.String("action", string(action)),
		slog.String("access_key", ak),
		slog.String("bucket", bucket),
		slog.String("key", key),
	)
	s.metrics.authorizationDenied(string(action))
}
