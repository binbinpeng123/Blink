// Package sigv4 实现 AWS Signature Version 4 的校验（含 Presigned URL）。
//
// 设计目标是**无状态**：Verify 只依赖 *http.Request、凭据表与选项，
// 不触碰存储层，因此可以直接用官方向量做单元测试。
//
// 协议规范不在此复述，但评审文档 02 列出的四个易错点在实现中逐条对应：
//
//  1. CanonicalURI 使用未解码的原始路径，且不做 path.Clean（见 canonical.go）
//  2. CanonicalQueryString 自行解析原始查询串并按字节序排序（见 canonical.go）
//  3. CanonicalHeaders 严格按客户端声明的 SignedHeaders 取（见 canonical.go）
//  4. HashedPayload 取自 X-Amz-Content-SHA256，缺失即拒绝（见 verify.go）
package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
)

// 协议常量。
const (
	// algorithm 是签名的算法标识，出现在 Authorization 头与 X-Amz-Algorithm。
	algorithm = "AWS4-HMAC-SHA256"
	// service 是 S3 的签名服务名。
	service = "s3"
	// terminator 是派生签名密钥时最后一层 HMAC 的字面量。
	terminator = "aws4_request"
	// unsignedPayload 表示不校验请求体哈希（S3 over TLS 的常见取值）。
	unsignedPayload = "UNSIGNED-PAYLOAD"

	// amzDateFormat 是 x-amz-date 的时间格式（ISO8601 基本格式）。
	amzDateFormat = "20060102T150405Z"
	// dateStampFormat 是凭据作用域中的日期格式。
	dateStampFormat = "20060102"

	// headerAuthorization 是签名所在的请求头。
	headerAuthorization = "Authorization"
	// headerAmzDate 是签名使用的时间戳头。
	headerAmzDate = "X-Amz-Date"
	// headerContentSHA256 是请求体哈希头。
	headerContentSHA256 = "X-Amz-Content-SHA256"

	// headerAmzAlgorithm 等为 Presigned URL 的查询参数名。
	// 其中 X-Amz-Date 与头部同名，共用 headerAmzDate。
	headerAmzAlgorithm     = "X-Amz-Algorithm"
	headerAmzCredential    = "X-Amz-Credential"
	headerAmzExpires       = "X-Amz-Expires"
	headerAmzSignedHeaders = "X-Amz-SignedHeaders"
	headerAmzSignature     = "X-Amz-Signature"
	headerAmzSecurityToken = "X-Amz-Security-Token"

	// MaxPresignExpiry 是 Presigned URL 允许的最长有效期（7 天），与 AWS 一致。
	MaxPresignExpiry = 7 * 24 * 60 * 60
)

// Credentials 是一组访问凭据。
type Credentials struct {
	// AccessKeyID 是访问密钥 ID。
	AccessKeyID string
	// SecretKey 是签名密钥，任何日志中都不得出现。
	SecretKey string
}

// Provider 按 Access Key 查询凭据。
//
// 单节点版本只有一组静态凭据，但接口按"可查表"设计，
// 以便后续接入多租户或外部凭据源时不必改动校验逻辑。
type Provider interface {
	Lookup(accessKeyID string) (Credentials, bool)
}

// StaticProvider 是 Provider 的静态实现，持有若干组固定凭据。
//
// 用 map 而非遍历比较：Access Key 是公开标识符，无需常数时间比较，
// 而 Secret Key 的比较发生在签名比对处（见 verify.go，使用 hmac.Equal）。
type StaticProvider struct {
	byID map[string]Credentials
}

// NewStaticProvider 构造静态凭据表；AccessKeyID 为空的条目被忽略。
func NewStaticProvider(creds ...Credentials) *StaticProvider {
	m := make(map[string]Credentials, len(creds))
	for _, c := range creds {
		if c.AccessKeyID == "" {
			continue
		}
		m[c.AccessKeyID] = c
	}
	return &StaticProvider{byID: m}
}

// Lookup 实现 Provider。
func (p *StaticProvider) Lookup(accessKeyID string) (Credentials, bool) {
	c, ok := p.byID[accessKeyID]
	return c, ok
}

// sha256Hex 返回数据的 SHA256 十六进制小写表示。
func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// hmacSHA256 计算 HMAC-SHA256。
func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

// signingKey 按 AWS 规范派生签名密钥：四层 HMAC，末层字面量为 aws4_request。
func signingKey(secretKey, dateStamp, region, svc string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, svc)
	return hmacSHA256(kService, terminator)
}

// stringToSign 组装待签名字符串。
func stringToSign(amzDate, scope, canonicalRequestHash string) string {
	return algorithm + "\n" + amzDate + "\n" + scope + "\n" + canonicalRequestHash
}

// hexEncode 返回字节切片的十六进制小写表示。
func hexEncode(b []byte) string { return hex.EncodeToString(b) }

// hmacEqual 以常数时间比较两个签名，避免通过耗时差异逐字节猜测签名。
//
// 直接复用 hmac.Equal：它内部已做长度检查与逐字节异或累加，
// 不会因提前返回而泄露前缀匹配长度。
func hmacEqual(a, b string) bool {
	return hmac.Equal([]byte(a), []byte(b))
}

// unescape 对查询参数值做百分号解码。
//
// 仅用于 Presigned URL 的**参数取值**（如 Credential 里的 `/` 被编码为 %2F）；
// 签名基里的查询串仍按原始编码字面量参与计算（见 canonicalQueryString）。
func unescape(v string) (string, error) { return url.QueryUnescape(v) }
