package sigv4

import (
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// PresignOptions 是生成 Presigned URL 的参数。
type PresignOptions struct {
	// Credentials 是签发凭据；空 AccessKeyID 或 SecretKey 会返回错误。
	Credentials Credentials
	// Region 是本服务所属区域。
	Region string
	// Expires 是链接有效期，必须在 1 秒到 7 天之间。
	Expires time.Duration
	// Now 可注入时间源，为零值时用 time.Now。
	Now func() time.Time
	// PayloadHash 是请求体哈希；为空时用 UNSIGNED-PAYLOAD（上传场景）。
	PayloadHash string
}

// Presign 为请求生成 Presigned URL。
//
// 与 Verify 共用同一套规范化代码（buildCanonical / signingKey），
// 因此"签发"与"校验"不可能出现两套实现漂移 —— 这也是把它们放在同一
// 包内的主要原因。
//
// 返回的 URL 已含全部 X-Amz-* 查询参数；对同一 r 调用两次会得到相同结果
// （时间源固定时），便于测试断言。
func Presign(r *http.Request, opt PresignOptions) (string, error) {
	if opt.Credentials.AccessKeyID == "" || opt.Credentials.SecretKey == "" {
		return "", errPresign("签发 Presigned URL 需要 Access Key 与 Secret Key。")
	}
	if opt.Region == "" {
		return "", errPresign("签发 Presigned URL 需要指定区域。")
	}
	secs := int(opt.Expires / time.Second)
	if secs < 1 || secs > MaxPresignExpiry {
		return "", errPresign("X-Amz-Expires 必须是 1 到 " + strconv.Itoa(MaxPresignExpiry) + " 之间的整数秒。")
	}

	now := time.Now
	if opt.Now != nil {
		now = opt.Now
	}
	amzDate := now().UTC().Format(amzDateFormat)
	dateStamp := now().UTC().Format(dateStampFormat)

	payloadHash := opt.PayloadHash
	if payloadHash == "" {
		payloadHash = unsignedPayload
	}

	// 签名头列表只含 host：Presigned URL 的隐含语义是把身份放进查询串，
	// 因此不强制客户端携带任何额外头。这也是 AWS 的推荐做法。
	signedHeaders := []string{"host"}

	scope := strings.Join([]string{dateStamp, opt.Region, service, terminator}, "/")
	credential := opt.Credentials.AccessKeyID + "/" + scope

	// 签名要基于"含全部 X-Amz-* 参数、但**不含** X-Amz-Signature"的查询串 ——
	// 把签名值算进签名会形成自指。base 即待签名的查询串。
	params := url.Values{
		headerAmzAlgorithm:     {algorithm},
		headerAmzCredential:    {credential},
		headerAmzDate:          {amzDate},
		headerAmzExpires:       {strconv.Itoa(secs)},
		headerAmzSignedHeaders: {strings.Join(signedHeaders, ";")},
	}
	base := presignQuery(r.URL.RawQuery, params)

	// 借用 buildCanonical 的规范化逻辑，只把查询串换成 base。
	// stripSignature 传 true 是双保险：与 Verify 路径对同一 URL 的规范化完全一致。
	withBase := *r
	u := *r.URL
	u.RawQuery = base
	withBase.URL = &u
	canonical := buildCanonical(&withBase, signedHeaders, payloadHash, true)

	toSign := stringToSign(amzDate, scope, canonical.Hash())
	key := signingKey(opt.Credentials.SecretKey, dateStamp, opt.Region, service)
	sig := hexEncode(hmacSHA256(key, toSign))

	// 最终 URL 在待签名查询串后追加 X-Amz-Signature。
	// 签名放在末尾是 aws-cli 的习惯做法，这样产物与 `aws s3 presign` 逐字节一致
	// （见 TestPresignMatchesAWSCLI）；对校验而言顺序无关，
	// 因为 canonicalQueryString 会重新按键排序。
	final := *r.URL
	final.RawQuery = base + "&" + headerAmzSignature + "=" + awsEncode(sig)
	return final.String(), nil
}

// presignQuery 把签名参数合并进原始查询串。
//
// extra 中的值按 AWS 规则编码（`/` 编码为 %2F），原有参数保持**原始编码**
// 不动 —— 上游客户端可能已经带了 `?versionId=%E4%B8%AD` 这类编码，
// 二次解码再编码会改变字面量，导致签名与客户端不一致。
//
// 剔除参数的需求（计算签名时排除 X-Amz-Signature）由 dropRawParam 承担 ——
// 剔除与"拼装待签名查询串"是两件事，分开更不易出错。
func presignQuery(rawQuery string, extra url.Values) string {
	pairs := parseRawQuery(rawQuery)
	out := make([]queryPair, 0, len(pairs)+len(extra))
	out = append(out, pairs...)

	// extra 用 url.Values 传入，转成 queryPair 时统一走 AWS 编码规则：
	// 空格→%20（不是 +），`/`→%2F，其余未保留字符原样。
	for k, vs := range extra {
		for _, v := range vs {
			out = append(out, queryPair{key: awsEncode(k), val: awsEncode(v)})
		}
	}

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].key != out[j].key {
			return out[i].key < out[j].key
		}
		return out[i].val < out[j].val
	})

	parts := make([]string, len(out))
	for i, p := range out {
		parts[i] = p.key + "=" + p.val
	}
	return strings.Join(parts, "&")
}

// awsEncode 按 AWS 的 URI 编码规则编码参数。
//
// 与 url.QueryEscape 的差别有二：空格编码为 %20 而非 `+`；
// 且 `/` 必须编码为 %2F（QueryEscape 在 Go 中确实会编码 `/`，
// 但显式写出以避免依赖标准库实现细节）。
func awsEncode(s string) string {
	esc := url.QueryEscape(s)
	esc = strings.ReplaceAll(esc, "+", "%20")
	return esc
}

// errPresign 构造签发阶段的错误。
func errPresign(msg string) error { return presignError(msg) }

// errPresign 是签发阶段的错误类型。
//
// 签发失败属于调用方的参数问题（缺凭据、有效期越界），
// 不该走 S3 错误码 —— 它不会出现在 HTTP 响应里，而是直接返回给内部调用者。
type presignError string

func (e presignError) Error() string { return string(e) }
