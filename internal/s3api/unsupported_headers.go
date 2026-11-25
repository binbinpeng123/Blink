package s3api

import (
	"net/http"

	"github.com/binbinpeng123/Blink/internal/s3err"
)

// 本文件集中定义「客户端显式要求了本版本未实现的能力」的请求头。
//
// 01/02 契约要求 ACL、Tagging、Lifecycle、Object Lock 等特性显式 501、
// 不得静默忽略。子资源（?acl、?tagging）已由路由层统一拦截；但同一批
// 能力还有纯请求头形态（x-amz-acl、x-amz-tagging、SSE 等），它们不改变
// 路由，只在写请求的头部出现。静默忽略的后果是语义降级而非"少个功能"：
// 带 SSE 的写入以为落盘是密文（实际明文），带 ACL 的建桶以为桶是私有的
// （实际是服务端默认权限）——客户端收到的 200 与真实状态不符，且无法
// 从响应里察觉。
//
// 分组按"能力"而不是"请求"：新增写路径时按组引用，避免某条路径漏掉
// 一组头造成契约漂移。

// unsupportedSSEHeaders 是 SSE 全组：SSE-S3、SSE-KMS 与 SSE-C。
var unsupportedSSEHeaders = []string{
	"x-amz-server-side-encryption",
	"x-amz-server-side-encryption-customer-algorithm",
	"x-amz-server-side-encryption-customer-key",
	"x-amz-server-side-encryption-customer-key-md5",
	"x-amz-server-side-encryption-aws-kms-key-id",
	"x-amz-server-side-encryption-context",
	"x-amz-server-side-encryption-bucket-key-enabled",
}

// unsupportedACLHeaders 是 ACL 组：canned ACL 与五个显式 grant 头。
var unsupportedACLHeaders = []string{
	"x-amz-acl",
	"x-amz-grant-read",
	"x-amz-grant-write",
	"x-amz-grant-read-acp",
	"x-amz-grant-write-acp",
	"x-amz-grant-full-control",
}

// unsupportedObjectLockHeaders 是 Object Lock 组：保留期与法律保留。
var unsupportedObjectLockHeaders = []string{
	"x-amz-object-lock-mode",
	"x-amz-object-lock-retain-until-date",
	"x-amz-object-lock-legal-hold",
}

// unsupportedObjectWriteHeaders 是对象写入类请求（PutObject、
// CreateMultipartUpload、CopyObject、UploadPartCopy）共用的拒绝集合。
var unsupportedObjectWriteHeaders = concatHeaders(
	unsupportedSSEHeaders,
	unsupportedACLHeaders,
	unsupportedObjectLockHeaders,
	[]string{
		"x-amz-tagging",
		"x-amz-website-redirect-location",
	},
)

// unsupportedBucketWriteHeaders 是建桶请求的拒绝集合。
//
// 与对象写入的差异：Object Lock 在桶级是「启用」开关，ACL 之外还有
// 对象所有权；SSE 头在建桶上表示「桶默认加密」，同样属于落盘语义。
var unsupportedBucketWriteHeaders = concatHeaders(
	unsupportedSSEHeaders,
	unsupportedACLHeaders,
	[]string{
		"x-amz-bucket-object-lock-enabled",
		"x-amz-object-ownership",
	},
)

// unsupportedPartHeaders 是分片级请求（UploadPart、
// CompleteMultipartUpload）的拒绝集合：只有 SSE 能在分片请求上出现
// （延续 Initiate 时声明的加密），而本版本没有加密。
var unsupportedPartHeaders = concatHeaders(unsupportedSSEHeaders)

// unsupportedCopyHeaders 是复制类请求（CopyObject、UploadPartCopy）的
// 拒绝集合：对象写入组全部，另加标签指令与复制源 SSE-C 组。
//
// 复制源 SSE-C 头容易漏：源对象若真是 SSE-C 加密，缺这三个头就无法
// 解密；本版本没有加密，同样按「未实现能力」拒绝而不是当作普通头忽略。
var unsupportedCopyHeaders = concatHeaders(
	unsupportedObjectWriteHeaders,
	[]string{
		"x-amz-tagging-directive",
		"x-amz-copy-source-server-side-encryption-customer-algorithm",
		"x-amz-copy-source-server-side-encryption-customer-key",
		"x-amz-copy-source-server-side-encryption-customer-key-md5",
	},
)

// concatHeaders 按顺序拼接若干头名分组，返回独立切片。
//
// 必须复制而非直接 append：各分组是包级变量，共享底层数组会让一次
// 拼接覆盖另一组的内容。
func concatHeaders(groups ...[]string) []string {
	var out []string
	for _, g := range groups {
		out = append(out, g...)
	}
	return out
}

// rejectUnsupportedHeaders 在请求带有 headers 中任一头时写 501 并返回 true。
//
// subject 用于错误消息前缀（"上传请求"/"建桶请求"等），让客户端一眼看出
// 是哪类操作被拒绝，而不是只拿到一个裸头名。
func rejectUnsupportedHeaders(w http.ResponseWriter, r *http.Request, reqID, subject string, headers []string) bool {
	for _, h := range headers {
		if r.Header.Get(h) != "" {
			s3err.New(s3err.CodeNotImplemented).
				WithMessage(subject+"暂不支持 "+h+"。").
				Write(w, r, reqID)
			return true
		}
	}
	return false
}

// rejectUnsupportedCopyHeaders 是复制路径的薄封装：两条复制路径共用
// unsupportedCopyHeaders，调用点只关心 subject。
func rejectUnsupportedCopyHeaders(w http.ResponseWriter, r *http.Request, reqID, subject string) bool {
	return rejectUnsupportedHeaders(w, r, reqID, subject, unsupportedCopyHeaders)
}
