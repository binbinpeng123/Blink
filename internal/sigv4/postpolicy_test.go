package sigv4

import (
	"testing"
	"time"
)

// postPolicyVector 是一组固定向量。签名由一段独立的 Python 实现算出
// （base64 编码 policy → HMAC-SHA256 派生密钥 → 对 base64 字符串签名），
// 用来防止"实现与测试互相印证"：两边都由同一份 Go 代码生成，写错也一起错。
const (
	postPolicyBase64 = "eyJleHBpcmF0aW9uIjogIjIwMjYtMTAtMjRUMTI6MDA6MDAuMDAwWiIsICJjb25kaXRpb25zIjogW3siYnVja2V0IjogImV4YW1wbGVidWNrZXQifSwgWyJzdGFydHMtd2l0aCIsICIka2V5IiwgInVzZXIvdXNlcjEvIl0sIHsic3VjY2Vzc19hY3Rpb25fcmVkaXJlY3QiOiAiaHR0cDovL2V4YW1wbGVidWNrZXQuczMuYW1hem9uYXdzLmNvbS9zdWNjZXNzZnVsX3VwbG9hZC5odG1sIn0sIFsic3RhcnRzLXdpdGgiLCAiJENvbnRlbnQtVHlwZSIsICJpbWFnZS8iXSwgWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsIDAsIDEwNDg1NzZdXX0="
	postPolicySig    = "43d42428af05f30f4846e17301a55c7cca79bf2b06a9796f83f35c094af58b83"
	postPolicyDate   = "20261023T090952Z"
)

func postPolicyFields() PostPolicyFields {
	return PostPolicyFields{
		Algorithm:  algorithm,
		Credential: vecAK + "/20261023/us-east-1/s3/aws4_request",
		Date:       postPolicyDate,
		Policy:     postPolicyBase64,
		Signature:  postPolicySig,
	}
}

func TestSignPostPolicyMatchesVector(t *testing.T) {
	got := SignPostPolicy(vecSK, "20261023", vecRegion, postPolicyBase64)
	if got != postPolicySig {
		t.Fatalf("签名 = %s，期望 %s", got, postPolicySig)
	}
}

func TestVerifyPostPolicyAcceptsVector(t *testing.T) {
	res, serr := VerifyPostPolicy(postPolicyFields(), vecOptions(postPolicyDate))
	if serr != nil {
		t.Fatalf("意外失败: %v", serr)
	}
	if res.AccessKeyID != vecAK {
		t.Fatalf("AccessKeyID = %q", res.AccessKeyID)
	}
	if !res.PostPolicy {
		t.Fatal("结果未标记为 PostPolicy")
	}
	if res.ContentSHA256 != unsignedPayload {
		t.Fatalf("ContentSHA256 = %q，期望 %q", res.ContentSHA256, unsignedPayload)
	}
}

// TestVerifyPostPolicyIgnoresClockSkew 固化一条与头部签名不同的语义：
// POST policy 的有效期由 expiration 决定，x-amz-date 只用于派生密钥。
func TestVerifyPostPolicyIgnoresClockSkew(t *testing.T) {
	opt := vecOptions(postPolicyDate)
	// 时间源拉到一年后：头部签名会 RequestTimeTooSkewed，POST policy 不应受影响。
	opt.Now = func() time.Time { return time.Date(2027, 10, 23, 9, 0, 0, 0, time.UTC) }
	if _, serr := VerifyPostPolicy(postPolicyFields(), opt); serr != nil {
		t.Fatalf("POST policy 不应校验时钟偏移，却失败: %v", serr)
	}
}

func TestVerifyPostPolicyRejectsTamperedPolicy(t *testing.T) {
	f := postPolicyFields()
	f.Policy = postPolicyBase64 + "x"
	if _, serr := VerifyPostPolicy(f, vecOptions(postPolicyDate)); serr == nil ||
		serr.Code != "SignatureDoesNotMatch" {
		t.Fatalf("篡改 policy 后错误 = %v，期望 SignatureDoesNotMatch", serr)
	}
}

func TestVerifyPostPolicyRejectsWrongRegion(t *testing.T) {
	f := postPolicyFields()
	f.Credential = vecAK + "/20261023/eu-west-1/s3/aws4_request"
	if _, serr := VerifyPostPolicy(f, vecOptions(postPolicyDate)); serr == nil ||
		serr.Code != "AuthorizationHeaderMalformed" {
		t.Fatalf("错误 = %v，期望 AuthorizationHeaderMalformed", serr)
	}
}

func TestVerifyPostPolicyRejectsUnknownKey(t *testing.T) {
	f := postPolicyFields()
	f.Credential = "AKIAUNKNOWN/20261023/us-east-1/s3/aws4_request"
	if _, serr := VerifyPostPolicy(f, vecOptions(postPolicyDate)); serr == nil ||
		serr.Code != "InvalidAccessKeyId" {
		t.Fatalf("错误 = %v，期望 InvalidAccessKeyId", serr)
	}
}

func TestVerifyPostPolicyRejectsMissingFields(t *testing.T) {
	base := postPolicyFields()
	cases := map[string]func(*PostPolicyFields){
		"algorithm":  func(f *PostPolicyFields) { f.Algorithm = "" },
		"credential": func(f *PostPolicyFields) { f.Credential = "" },
		"date":       func(f *PostPolicyFields) { f.Date = "" },
		"policy":     func(f *PostPolicyFields) { f.Policy = "" },
		"signature":  func(f *PostPolicyFields) { f.Signature = "" },
	}
	for name, mutate := range cases {
		f := base
		mutate(&f)
		if _, serr := VerifyPostPolicy(f, vecOptions(postPolicyDate)); serr == nil ||
			serr.Code != "AuthorizationHeaderMalformed" {
			t.Errorf("%s 缺失时错误 = %v，期望 AuthorizationHeaderMalformed", name, serr)
		}
	}
}

func TestVerifyPostPolicyRejectsDateMismatch(t *testing.T) {
	f := postPolicyFields()
	f.Date = "20261024T090952Z"
	if _, serr := VerifyPostPolicy(f, vecOptions(postPolicyDate)); serr == nil ||
		serr.Code != "AuthorizationHeaderMalformed" {
		t.Fatalf("错误 = %v，期望 AuthorizationHeaderMalformed", serr)
	}
}
