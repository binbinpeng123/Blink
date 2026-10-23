package sigv4

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/binbinpeng123/Blink/internal/s3err"
)

// Result 是一次成功校验的产物。
//
// 调用方最需要的是 ContentSHA256：把它直接交给存储层边写边算，
// 就不必为了验签先把请求体读进内存或落盘再重读一遍。
type Result struct {
	// AccessKeyID 是校验通过的访问密钥 ID。
	AccessKeyID string
	// Region 是客户端声明的凭据作用域区域。
	Region string
	// SignedHeaders 是客户端声明的签名头列表（小写、按序）。
	SignedHeaders []string
	// ContentSHA256 是请求体哈希，可能为 "UNSIGNED-PAYLOAD"。
	ContentSHA256 string
	// Presigned 标识本次请求走的是查询参数签名。
	Presigned bool
	// RequestTime 是客户端在请求中声明的时间。
	RequestTime time.Time
	// ExpiresAt 仅对 Presigned 有效，为链接失效时刻。
	ExpiresAt time.Time
}

// PayloadSigned 报告请求体哈希是否真实参与签名。
//
// 返回 false 时（UNSIGNED-PAYLOAD），存储层仍应计算哈希用于 ETag 与校验，
// 只是不能拿它来断言"内容未被篡改"。
func (res *Result) PayloadSigned() bool {
	return res.ContentSHA256 != unsignedPayload && res.ContentSHA256 != ""
}

// Options 是校验所需的全部外部依赖。
type Options struct {
	// Provider 按 Access Key 查凭据，必填。
	Provider Provider
	// Region 是本服务所属区域，必填；与客户端声明不一致即拒绝。
	Region string
	// MaxClockSkew 是与客户端的时间容差，为 0 时使用 15 分钟。
	MaxClockSkew time.Duration
	// Now 可注入时间源，便于用固定时间的官方向量做测试；为零值时用 time.Now。
	Now func() time.Time
}

// DefaultMaxClockSkew 是默认时间容差。
const DefaultMaxClockSkew = 15 * time.Minute

func (o Options) now() time.Time {
	if o.Now != nil {
		return o.Now()
	}
	return time.Now()
}

func (o Options) maxClockSkew() time.Duration {
	if o.MaxClockSkew <= 0 {
		return DefaultMaxClockSkew
	}
	return o.MaxClockSkew
}

// Verify 校验请求签名，成功返回校验结果，失败返回可直接写给客户端的 S3 错误。
//
// 两种签名方式自动识别：查询串含 X-Amz-Signature 即走 Presigned URL，
// 否则看 Authorization 头；两者皆无按匿名请求处理（403 AccessDenied）。
func Verify(r *http.Request, opt Options) (*Result, *s3err.Error) {
	if opt.Provider == nil {
		return nil, s3err.New(s3err.CodeInternalError).WithMessage("sigv4: 未配置凭据提供者")
	}
	if opt.Region == "" {
		return nil, s3err.New(s3err.CodeInternalError).WithMessage("sigv4: 未配置区域")
	}

	switch {
	case hasRawParam(r.URL.RawQuery, headerAmzSignature):
		return verifyPresigned(r, opt)
	case r.Header.Get(headerAuthorization) != "":
		return verifyHeader(r, opt)
	default:
		// 匿名访问：本版本只支持一组静态凭据，不接受匿名读写。
		return nil, s3err.New(s3err.CodeAccessDenied).WithMessage("缺少签名信息：请使用签名请求或 Presigned URL。")
	}
}

// authSpec 是从请求中解析出的签名参数，两种签名方式共用。
type authSpec struct {
	accessKeyID   string
	dateStamp     string
	region        string
	signedHeaders []string
	signature     string
	amzDate       string
	payloadHash   string
	presigned     bool
	expiresAt     time.Time
}

// verifyHeader 校验 Authorization 头签名。
func verifyHeader(r *http.Request, opt Options) (*Result, *s3err.Error) {
	spec, serr := parseAuthorization(r.Header.Get(headerAuthorization))
	if serr != nil {
		return nil, serr
	}
	spec.presigned = false

	// 易错点 ④：请求体哈希取自 X-Amz-Content-SHA256。
	// 明文 HTTP 下缺少该头就无法把请求体纳入签名，必须拒绝而不是"当作空体"。
	//
	// TODO(切片 10)：接入 TLS + aws-chunked 后，此处还需支持
	// STREAMING-AWS4-HMAC-SHA256-PAYLOAD 的分块签名链。
	hash := strings.TrimSpace(r.Header.Get(headerContentSHA256))
	if hash == "" {
		return nil, s3err.New(s3err.CodeMissingSecurityHeader).
			WithMessage("缺少 " + headerContentSHA256 + " 头：HTTP 下必须显式声明请求体哈希。")
	}
	spec.payloadHash = hash

	amzDate := strings.TrimSpace(r.Header.Get(headerAmzDate))
	if amzDate == "" {
		// 兼容仅带 Date 头的客户端；AWS 允许二者之一。
		if d := r.Header.Get("Date"); d != "" {
			t, err := http.ParseTime(d)
			if err != nil {
				return nil, s3err.New(s3err.CodeAccessDenied).WithMessage("Date 头格式无法解析。")
			}
			amzDate = t.UTC().Format(amzDateFormat)
		} else {
			return nil, s3err.New(s3err.CodeMissingSecurityHeader).
				WithMessage("缺少 " + headerAmzDate + " 头（或可解析的 Date 头）。")
		}
	}
	spec.amzDate = amzDate

	return finishVerify(r, opt, spec)
}

// verifyPresigned 校验 Presigned URL 签名。
func verifyPresigned(r *http.Request, opt Options) (*Result, *s3err.Error) {
	raw := r.URL.RawQuery

	get := func(name string) (string, bool) {
		v, ok := rawParamValue(raw, name)
		if !ok {
			return "", false
		}
		dec, err := unescape(v)
		if err != nil {
			return "", false
		}
		return dec, true
	}

	algo, ok := get(headerAmzAlgorithm)
	if !ok || algo != algorithm {
		return nil, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("Presigned URL 的 " + headerAmzAlgorithm + " 必须为 " + algorithm + "。")
	}

	cred, ok := get(headerAmzCredential)
	if !ok || cred == "" {
		return nil, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("Presigned URL 缺少 " + headerAmzCredential + "。")
	}
	spec, serr := parseCredential(cred)
	if serr != nil {
		return nil, serr
	}
	spec.presigned = true

	sh, ok := get(headerAmzSignedHeaders)
	if !ok || sh == "" {
		return nil, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("Presigned URL 缺少 " + headerAmzSignedHeaders + "。")
	}
	spec.signedHeaders = signedHeaderNames(sh)

	sig, ok := get(headerAmzSignature)
	if !ok || sig == "" {
		return nil, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("Presigned URL 缺少 " + headerAmzSignature + "。")
	}
	spec.signature = sig

	amzDate, ok := get(headerAmzDate)
	if !ok || amzDate == "" {
		return nil, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("Presigned URL 缺少 " + headerAmzDate + "。")
	}
	spec.amzDate = amzDate

	// Presigned URL 不覆盖请求体，按协议固定为 UNSIGNED-PAYLOAD。
	spec.payloadHash = unsignedPayload

	// 有效期先于签名比对检查，这样"链接过期"不会被误报成"签名不匹配"。
	if serr := checkPresignExpiry(spec, opt, get); serr != nil {
		return nil, serr
	}

	return finishVerify(r, opt, spec)
}

// checkPresignExpiry 校验 X-Amz-Expires 的取值范围与是否过期。
func checkPresignExpiry(spec authSpec, opt Options, get func(string) (string, bool)) *s3err.Error {
	rawExpires, ok := get(headerAmzExpires)
	if !ok || rawExpires == "" {
		return s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("Presigned URL 缺少 " + headerAmzExpires + "。")
	}
	secs, err := strconv.Atoi(rawExpires)
	if err != nil || secs < 1 || secs > MaxPresignExpiry {
		return s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage(headerAmzExpires + " 必须是 1 到 " + strconv.Itoa(MaxPresignExpiry) + " 之间的整数秒。")
	}

	t, serr := parseAmzDate(spec.amzDate)
	if serr != nil {
		return serr
	}
	expiresAt := t.Add(time.Duration(secs) * time.Second)
	if opt.now().After(expiresAt) {
		return s3err.New(s3err.CodeAccessDenied).WithMessage("Presigned URL 已过期。")
	}
	spec.expiresAt = expiresAt
	return nil
}

// finishVerify 是两种签名方式共用的后段：查凭据 → 校时间与区域 → 比对签名。
func finishVerify(r *http.Request, opt Options, spec authSpec) (*Result, *s3err.Error) {
	creds, found := opt.Provider.Lookup(spec.accessKeyID)
	if !found {
		return nil, s3err.New(s3err.CodeInvalidAccessKeyID)
	}

	// 区域必须与服务一致：签名作用域里的区域是签名文本的一部分，
	// 放行会让"用错区域的客户端"得到难以排查的签名不匹配。
	if spec.region != opt.Region {
		return nil, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("凭据作用域区域 " + spec.region + " 与本服务区域 " + opt.Region + " 不一致。")
	}

	reqTime, serr := parseAmzDate(spec.amzDate)
	if serr != nil {
		return nil, serr
	}
	// 作用域中的日期必须与 x-amz-date 同日，否则签名文本本身就自相矛盾。
	if dateStampOf(reqTime) != spec.dateStamp {
		return nil, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("凭据作用域日期与 " + headerAmzDate + " 不一致。")
	}
	// 时间容差**只对头部签名生效**。
	//
	// Presigned URL 的合法性由 X-Amz-Expires 决定，而非 15 分钟窗口：
	// `aws s3 presign` 默认就签发 3600 秒有效期，若再叠加 15 分钟容差，
	// 超过 15 分钟的链接将永远无法校验 —— 这是把"防重放"错误地
	// 套用到"预授权链接"上。过期检查已在 checkPresignExpiry 完成。
	if !spec.presigned {
		if serr := checkClockSkew(reqTime, opt); serr != nil {
			return nil, serr
		}
	}

	canonical := buildCanonical(r, spec.signedHeaders, spec.payloadHash, spec.presigned)
	scope := strings.Join([]string{spec.dateStamp, spec.region, service, terminator}, "/")
	toSign := stringToSign(spec.amzDate, scope, canonical.Hash())
	key := signingKey(creds.SecretKey, spec.dateStamp, spec.region, service)
	want := hexEncode(hmacSHA256(key, toSign))

	if !hmacEqual(want, spec.signature) {
		return nil, s3err.New(s3err.CodeSignatureDoesNotMatch)
	}

	return &Result{
		AccessKeyID:   spec.accessKeyID,
		Region:        spec.region,
		SignedHeaders: spec.signedHeaders,
		ContentSHA256: spec.payloadHash,
		Presigned:     spec.presigned,
		RequestTime:   reqTime,
		ExpiresAt:     spec.expiresAt,
	}, nil
}

// checkClockSkew 校验请求时间与本地时间之差是否在容差内。
//
// 容差存在的意义是容忍两端时钟漂移；超出即 403 RequestTimeTooSkewed，
// 并按 AWS 惯例在响应体中给出 RequestTime / ServerTime / 允许偏差，
// 让用户能直接对比出时钟差了多少。
func checkClockSkew(reqTime time.Time, opt Options) *s3err.Error {
	skew := opt.maxClockSkew()
	now := opt.now()
	diff := now.Sub(reqTime)
	if diff < 0 {
		diff = -diff
	}
	if diff <= skew {
		return nil
	}
	e := s3err.New(s3err.CodeRequestTimeTooSkewed)
	return e.WithDetails(
		s3err.D("RequestTime", reqTime.UTC().Format(amzDateFormat)),
		s3err.D("ServerTime", now.UTC().Format(amzDateFormat)),
		s3err.D("MaxAllowedSkewMilliseconds", strconv.FormatInt(skew.Milliseconds(), 10)),
	)
}

// parseAmzDate 解析 x-amz-date 时间戳。
func parseAmzDate(v string) (time.Time, *s3err.Error) {
	t, err := time.Parse(amzDateFormat, v)
	if err != nil {
		return time.Time{}, s3err.New(s3err.CodeAccessDenied).
			WithMessage(headerAmzDate + " 不是合法的 ISO8601 基本格式时间戳。")
	}
	return t.UTC(), nil
}

// dateStampOf 取时间的 8 位日期部分。
func dateStampOf(t time.Time) string { return t.UTC().Format(dateStampFormat) }

// parseAuthorization 解析 Authorization 头。
//
// 目标格式：
//
//	AWS4-HMAC-SHA256 Credential=<AKID>/<date>/<region>/<service>/aws4_request, SignedHeaders=a;b, Signature=<hex>
//
// 解析刻意保持"字面严格"：签名文本由客户端生成，
// 任何"善意修正"（补空、改大小写、重排）都会让计算结果与客户端不一致。
func parseAuthorization(v string) (authSpec, *s3err.Error) {
	var spec authSpec

	rest, ok := strings.CutPrefix(strings.TrimSpace(v), algorithm)
	if !ok {
		return spec, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("Authorization 头必须以 " + algorithm + " 开头。")
	}
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return spec, s3err.New(s3err.CodeAuthorizationHeaderMalformed)
	}

	var credRaw string
	seen := map[string]bool{}
	for _, field := range strings.Split(rest, ",") {
		k, val, found := strings.Cut(strings.TrimSpace(field), "=")
		if !found {
			return spec, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
				WithMessage("Authorization 头中存在缺少 '=' 的片段。")
		}
		k, val = strings.TrimSpace(k), strings.TrimSpace(val)
		if seen[k] {
			return spec, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
				WithMessage("Authorization 头中 " + k + " 重复出现。")
		}
		seen[k] = true

		switch k {
		case "Credential":
			credRaw = val
		case "SignedHeaders":
			spec.signedHeaders = signedHeaderNames(val)
		case "Signature":
			spec.signature = val
		default:
			return spec, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
				WithMessage("Authorization 头中存在未知字段 " + k + "。")
		}
	}

	if credRaw == "" || len(spec.signedHeaders) == 0 || spec.signature == "" {
		return spec, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("Authorization 头必须同时包含 Credential、SignedHeaders 与 Signature。")
	}

	fromCred, serr := parseCredential(credRaw)
	if serr != nil {
		return spec, serr
	}
	spec.accessKeyID = fromCred.accessKeyID
	spec.dateStamp = fromCred.dateStamp
	spec.region = fromCred.region
	return spec, nil
}

// parseCredential 拆分凭据作用域 `<AKID>/<date>/<region>/<service>/aws4_request`。
func parseCredential(v string) (authSpec, *s3err.Error) {
	var spec authSpec
	parts := strings.Split(v, "/")
	if len(parts) != 5 {
		return spec, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("Credential 必须形如 <AccessKeyId>/<date>/<region>/<service>/aws4_request。")
	}
	if parts[0] == "" {
		return spec, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("Credential 中缺少 Access Key ID。")
	}
	if len(parts[1]) != 8 {
		return spec, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("Credential 中日期必须为 YYYYMMDD。")
	}
	// 服务名与终止符是签名文本的一部分，必须逐字匹配，不接受别名。
	if parts[3] != service {
		return spec, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("Credential 中服务名必须为 " + service + "。")
	}
	if parts[4] != terminator {
		return spec, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("Credential 必须以 " + terminator + " 结尾。")
	}
	spec.accessKeyID, spec.dateStamp, spec.region = parts[0], parts[1], parts[2]
	return spec, nil
}

// hasRawParam 判断原始查询串中是否存在指定键（不解码）。
func hasRawParam(raw, name string) bool {
	_, ok := rawParamValue(raw, name)
	return ok
}

// rawParamValue 在原始查询串中查找键，返回其**原始编码**的值。
//
// 刻意不用 url.Values：签名必须基于字面值计算，
// 提前解码会让形如 `+` 与 `%20` 的差异丢失。
func rawParamValue(raw, name string) (string, bool) {
	for _, p := range parseRawQuery(raw) {
		if p.key == name {
			return p.val, true
		}
	}
	return "", false
}
