package sigv4

import (
	"github.com/binbinpeng123/Blink/internal/s3err"
)

// PostPolicyFields 是 POST Object 表单中参与验签的五个字段。
//
// 表单字段名大小写不敏感，调用方传入前应已统一为小写并去掉前缀歧义。
type PostPolicyFields struct {
	Algorithm  string // x-amz-algorithm
	Credential string // x-amz-credential
	Date       string // x-amz-date
	Policy     string // policy（base64 原样，不解码）
	Signature  string // x-amz-signature
}

// VerifyPostPolicy 校验 POST Object 的 SigV4 表单签名。
//
// 签名对象是 **base64 编码后的 policy 字符串本身**，而不是解码后的 JSON：
// 先解码再签名是这类实现最常见的错误，且错误只在跨实现互操作时才暴露。
//
// 与头部签名不同，这里**不做时钟偏移检查**：POST policy 自带 expiration，
// 请求是否仍在有效期内由 policy 决定；x-amz-date 只参与派生签名密钥，
// 若再叠加 15 分钟窗口，离线签发、稍后上传的正常流程会被误杀。
func VerifyPostPolicy(f PostPolicyFields, opt Options) (*Result, *s3err.Error) {
	if opt.Provider == nil {
		return nil, s3err.New(s3err.CodeInternalError).WithMessage("sigv4: 未配置凭据提供者")
	}
	if opt.Region == "" {
		return nil, s3err.New(s3err.CodeInternalError).WithMessage("sigv4: 未配置区域")
	}

	if f.Algorithm != algorithm {
		return nil, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("POST 表单 " + headerAmzAlgorithm + " 必须为 " + algorithm + "。")
	}
	if f.Credential == "" {
		return nil, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("POST 表单缺少 " + headerAmzCredential + "。")
	}
	spec, serr := parseCredential(f.Credential)
	if serr != nil {
		return nil, serr
	}
	if f.Date == "" {
		return nil, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("POST 表单缺少 " + headerAmzDate + "。")
	}
	spec.amzDate = f.Date
	if f.Policy == "" {
		return nil, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("POST 表单缺少 policy。")
	}
	if f.Signature == "" {
		return nil, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("POST 表单缺少 " + headerAmzSignature + "。")
	}

	creds, found := opt.Provider.Lookup(spec.accessKeyID)
	if !found {
		return nil, s3err.New(s3err.CodeInvalidAccessKeyID)
	}
	if spec.region != opt.Region {
		return nil, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("凭据作用域区域 " + spec.region + " 与本服务区域 " + opt.Region + " 不一致。")
	}
	reqTime, serr := parseAmzDate(spec.amzDate)
	if serr != nil {
		return nil, serr
	}
	if dateStampOf(reqTime) != spec.dateStamp {
		return nil, s3err.New(s3err.CodeAuthorizationHeaderMalformed).
			WithMessage("凭据作用域日期与 " + headerAmzDate + " 不一致。")
	}

	key := signingKey(creds.SecretKey, spec.dateStamp, spec.region, service)
	want := hexEncode(hmacSHA256(key, f.Policy))
	if !hmacEqual(want, f.Signature) {
		return nil, s3err.New(s3err.CodeSignatureDoesNotMatch)
	}

	return &Result{
		AccessKeyID:   spec.accessKeyID,
		Region:        spec.region,
		ContentSHA256: unsignedPayload,
		RequestTime:   reqTime,
		PostPolicy:    true,
	}, nil
}

// SignPostPolicy 计算 POST policy 签名，供客户端与服务端测试使用。
func SignPostPolicy(secretKey, dateStamp, region, policyBase64 string) string {
	key := signingKey(secretKey, dateStamp, region, service)
	return hexEncode(hmacSHA256(key, policyBase64))
}
