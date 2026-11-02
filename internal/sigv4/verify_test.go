package sigv4

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// fixedNow 返回固定时间源，便于构造确定性的过期/时钟偏移场景。
func fixedNow(t time.Time) func() time.Time { return func() time.Time { return t } }

func mustParseAmz(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(amzDateFormat, s)
	if err != nil {
		t.Fatalf("时间解析失败 %q: %v", s, err)
	}
	return ts
}

// signedReq 构造一个经签名（头部签名）的请求，返回请求本身与所用时间。
func signedReq(t *testing.T, method, rawURL string) (*http.Request, string) {
	t.Helper()
	const amzDate = "20261023T120000Z"
	r := mustRequest(t, method, rawURL)
	r.Header.Set(headerAmzDate, amzDate)
	r.Header.Set(headerContentSHA256, vecEmptySHA256)

	signed := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	canonical := buildCanonical(r, signed, vecEmptySHA256, false)
	key := signingKey(vecSK, "20261023", vecRegion, service)
	sig := hexEncode(hmacSHA256(key, stringToSign(amzDate, vecScope, canonical.Hash())))
	r.Header.Set(headerAuthorization,
		algorithm+" Credential="+vecAK+"/"+vecScope+
			", SignedHeaders="+strings.Join(signed, ";")+
			", Signature="+sig)
	return r, amzDate
}

func baseOptions(amzDate string) Options {
	return Options{
		Provider: NewStaticProvider(Credentials{AccessKeyID: vecAK, SecretKey: vecSK}),
		Region:   vecRegion,
		Now:      fixedNow(mustParseAmzForTest(amzDate)),
	}
}

// mustParseAmzForTest 是 baseOptions 用的解析，失败即 panic（仅测试内使用）。
func mustParseAmzForTest(s string) time.Time {
	ts, err := time.Parse(amzDateFormat, s)
	if err != nil {
		panic(err)
	}
	return ts
}

func TestVerifyRejectsTamperedSignature(t *testing.T) {
	r, amzDate := signedReq(t, http.MethodGet, "http://"+vecHost+"/b/k")

	// 篡改最后一个十六进制字符。
	auth := r.Header.Get(headerAuthorization)
	tampered := auth[:len(auth)-1] + "0"
	if strings.HasSuffix(auth, "0") {
		tampered = auth[:len(auth)-1] + "1"
	}
	r.Header.Set(headerAuthorization, tampered)

	_, serr := Verify(r, baseOptions(amzDate))
	if serr == nil || serr.Code != "SignatureDoesNotMatch" {
		t.Fatalf("篡改签名应返回 SignatureDoesNotMatch，实际 %v", serr)
	}
}

// TestVerifyRejectsTamperedPath 验证路径参与签名：
// 改掉 key 后签名必须失效（否则可以把签名"借"给别的对象）。
func TestVerifyRejectsTamperedPath(t *testing.T) {
	r, amzDate := signedReq(t, http.MethodGet, "http://"+vecHost+"/b/k")
	r.URL = &url.URL{Path: "/b/other-key"}

	_, serr := Verify(r, baseOptions(amzDate))
	if serr == nil || serr.Code != "SignatureDoesNotMatch" {
		t.Fatalf("篡改路径应返回 SignatureDoesNotMatch，实际 %v", serr)
	}
}

// TestVerifyRejectsTamperedQuery 验证查询串参与签名。
func TestVerifyRejectsTamperedQuery(t *testing.T) {
	r, amzDate := signedReq(t, http.MethodGet, "http://"+vecHost+"/b/k?list-type=2")
	u := *r.URL
	u.RawQuery = "list-type=3"
	r.URL = &u

	_, serr := Verify(r, baseOptions(amzDate))
	if serr == nil || serr.Code != "SignatureDoesNotMatch" {
		t.Fatalf("篡改查询串应返回 SignatureDoesNotMatch，实际 %v", serr)
	}
}

func TestVerifyRejectsUnknownAccessKey(t *testing.T) {
	r, amzDate := signedReq(t, http.MethodGet, "http://"+vecHost+"/b/k")
	auth := strings.Replace(r.Header.Get(headerAuthorization), vecAK, "AKIANOTEXIST0000", 1)
	r.Header.Set(headerAuthorization, auth)

	_, serr := Verify(r, baseOptions(amzDate))
	if serr == nil || serr.Code != "InvalidAccessKeyId" {
		t.Fatalf("未知 Access Key 应返回 InvalidAccessKeyId，实际 %v", serr)
	}
}

func TestVerifyRejectsWrongRegion(t *testing.T) {
	r, amzDate := signedReq(t, http.MethodGet, "http://"+vecHost+"/b/k")
	// 把作用域里的区域改成别的值（签名会一并失效，但区域检查先命中）。
	auth := strings.Replace(r.Header.Get(headerAuthorization), "/us-east-1/", "/eu-west-1/", 1)
	r.Header.Set(headerAuthorization, auth)

	_, serr := Verify(r, baseOptions(amzDate))
	if serr == nil || serr.Code != "AuthorizationHeaderMalformed" {
		t.Fatalf("区域不一致应返回 AuthorizationHeaderMalformed，实际 %v", serr)
	}
	if !strings.Contains(serr.Message, "区域") {
		t.Errorf("错误消息应说明区域不一致: %s", serr.Message)
	}
}

// TestVerifyRequiresContentSHA256 锁定易错点 ④：
// HTTP 下缺少 X-Amz-Content-SHA256 必须拒绝，而不是"当作空体"。
func TestVerifyRequiresContentSHA256(t *testing.T) {
	r, amzDate := signedReq(t, http.MethodGet, "http://"+vecHost+"/b/k")
	r.Header.Del(headerContentSHA256)

	_, serr := Verify(r, baseOptions(amzDate))
	if serr == nil || serr.Code != "MissingSecurityHeader" {
		t.Fatalf("缺少内容哈希头应返回 MissingSecurityHeader，实际 %v", serr)
	}
}

// TestVerifyRejectsClockSkew 验证 15 分钟容差与附加字段。
func TestVerifyRejectsClockSkew(t *testing.T) {
	r, amzDate := signedReq(t, http.MethodGet, "http://"+vecHost+"/b/k")
	reqTime := mustParseAmz(t, amzDate)

	opt := baseOptions(amzDate)
	// 服务端时间比请求时间晚 1 小时，超出默认 15 分钟容差。
	opt.Now = fixedNow(reqTime.Add(time.Hour))

	_, serr := Verify(r, opt)
	if serr == nil || serr.Code != "RequestTimeTooSkewed" {
		t.Fatalf("时钟偏移过大应返回 RequestTimeTooSkewed，实际 %v", serr)
	}
	// AWS 会回传可比对的字段，让用户一眼看出差了多少。
	names := map[string]bool{}
	for _, d := range serr.Details {
		names[d.XMLName.Local] = true
	}
	for _, want := range []string{"RequestTime", "ServerTime", "MaxAllowedSkewMilliseconds"} {
		if !names[want] {
			t.Errorf("错误响应缺少 %s 字段", want)
		}
	}
}

func TestVerifyAcceptsBoundarySkew(t *testing.T) {
	r, amzDate := signedReq(t, http.MethodGet, "http://"+vecHost+"/b/k")
	reqTime := mustParseAmz(t, amzDate)

	opt := baseOptions(amzDate)
	// 恰好等于容差应放行（判据是 <= ，不是 < ）。
	opt.Now = fixedNow(reqTime.Add(DefaultMaxClockSkew))

	if _, serr := Verify(r, opt); serr != nil {
		t.Fatalf("恰好等于容差应放行，实际 %s (%s)", serr.Code, serr.Message)
	}
}

func TestVerifyRejectsAnonymous(t *testing.T) {
	r := mustRequest(t, http.MethodGet, "http://"+vecHost+"/b/k")
	_, serr := Verify(r, baseOptions("20261023T120000Z"))
	if serr == nil || serr.Code != "AccessDenied" {
		t.Fatalf("匿名请求应返回 AccessDenied，实际 %v", serr)
	}
}

func TestVerifyRejectsMalformedAuthorization(t *testing.T) {
	cases := []struct {
		name string
		auth string
	}{
		{"前缀错误", "AWS4-HMAC-SHA256X Credential=a/1/2/s3/aws4_request, SignedHeaders=host, Signature=x"},
		{"凭据段数不对", algorithm + " Credential=a/1/2/s3, SignedHeaders=host, Signature=x"},
		{"服务名错误", algorithm + " Credential=a/20261023/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=x"},
		{"终止符错误", algorithm + " Credential=a/20261023/us-east-1/s3/aws4_requestX, SignedHeaders=host, Signature=x"},
		{"缺 Signature", algorithm + " Credential=a/" + vecScope + ", SignedHeaders=host"},
		{"缺 SignedHeaders", algorithm + " Credential=a/" + vecScope + ", Signature=x"},
		{"重复字段", algorithm + " Credential=a/" + vecScope + ", Credential=b/" + vecScope + ", Signature=x"},
		{"未知字段", algorithm + " Credential=a/" + vecScope + ", Bogus=1, SignedHeaders=host, Signature=x"},
		{"日期长度错误", algorithm + " Credential=a/2026/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=x"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := mustRequest(t, http.MethodGet, "http://"+vecHost+"/b/k")
			r.Header.Set(headerAmzDate, "20261023T120000Z")
			r.Header.Set(headerContentSHA256, vecEmptySHA256)
			r.Header.Set(headerAuthorization, c.auth)

			_, serr := Verify(r, baseOptions("20261023T120000Z"))
			if serr == nil {
				t.Fatal("畸形 Authorization 应被拒绝")
			}
			if serr.Code != "AuthorizationHeaderMalformed" && serr.Code != "MissingSecurityHeader" {
				t.Errorf("应返回 AuthorizationHeaderMalformed，实际 %s", serr.Code)
			}
		})
	}
}

// TestPresignMatchesAWSCLI 是**交叉验证**：我的 Presign 输出必须与
// `aws s3 presign` 逐字节一致 —— 这同时验证了签发方向的正确性，
// 而不只是"我签我验能对上"。
func TestPresignMatchesAWSCLI(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "http://"+vecHost+"/my-bucket/obj.bin", nil)
	if err != nil {
		t.Fatalf("构造请求失败: %v", err)
	}
	r.Host = vecHost

	at := mustParseAmz(t, "20261023T090952Z")
	got, err := Presign(r, PresignOptions{
		Credentials: Credentials{AccessKeyID: vecAK, SecretKey: vecSK},
		Region:      vecRegion,
		Expires:     300 * time.Second,
		Now:         fixedNow(at),
	})
	if err != nil {
		t.Fatalf("Presign 失败: %v", err)
	}

	const want = "http://127.0.0.1:19000/my-bucket/obj.bin" +
		"?X-Amz-Algorithm=AWS4-HMAC-SHA256" +
		"&X-Amz-Credential=AKIATESTTEST1234%2F20261023%2Fus-east-1%2Fs3%2Faws4_request" +
		"&X-Amz-Date=20261023T090952Z" +
		"&X-Amz-Expires=300" +
		"&X-Amz-SignedHeaders=host" +
		"&X-Amz-Signature=0150d8994207354edaf6f3dd935cecb6fc5bbafe5ca5591c13c2786a36a6eb09"

	if got != want {
		t.Fatalf("Presign 结果与 aws-cli 不一致\n实际:\n%s\n期望:\n%s", got, want)
	}
}

// TestPresignRoundTrip 验证签发→校验闭环（含附加查询参数）。
func TestPresignRoundTrip(t *testing.T) {
	cases := []string{
		"http://" + vecHost + "/b/k",
		"http://" + vecHost + "/b/weird%20name%2Bplus.txt",
		"http://" + vecHost + "/b/deep/nested/k?versionId=abc123",
		"http://" + vecHost + "/b/k?list-type=2&prefix=mod",
	}
	at := mustParseAmz(t, "20261023T120000Z")

	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodGet, raw, nil)
			if err != nil {
				t.Fatalf("构造请求失败: %v", err)
			}
			r.Host = vecHost

			signed, err := Presign(r, PresignOptions{
				Credentials: Credentials{AccessKeyID: vecAK, SecretKey: vecSK},
				Region:      vecRegion,
				Expires:     time.Hour,
				Now:         fixedNow(at),
			})
			if err != nil {
				t.Fatalf("Presign 失败: %v", err)
			}

			vr := mustRequest(t, http.MethodGet, signed)
			res, serr := Verify(vr, Options{
				Provider: NewStaticProvider(Credentials{AccessKeyID: vecAK, SecretKey: vecSK}),
				Region:   vecRegion,
				Now:      fixedNow(at.Add(30 * time.Minute)),
			})
			if serr != nil {
				t.Fatalf("往返校验应通过，实际 %s (%s)\nURL: %s", serr.Code, serr.Message, signed)
			}
			if !res.Presigned {
				t.Error("应识别为 Presigned")
			}
		})
	}
}

func TestPresignRejectsBadExpiry(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "http://"+vecHost+"/b/k", nil)
	if err != nil {
		t.Fatalf("构造请求失败: %v", err)
	}
	cases := []struct {
		name    string
		expires time.Duration
	}{
		{"零秒", 0},
		{"负数", -time.Second},
		{"超过 7 天", (MaxPresignExpiry + 1) * time.Second},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Presign(r, PresignOptions{
				Credentials: Credentials{AccessKeyID: vecAK, SecretKey: vecSK},
				Region:      vecRegion,
				Expires:     c.expires,
			})
			if err == nil {
				t.Fatal("有效期越界应报错")
			}
		})
	}
}

func TestPresignRejectsMissingCredentials(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "http://"+vecHost+"/b/k", nil)
	if err != nil {
		t.Fatalf("构造请求失败: %v", err)
	}
	cases := []struct {
		name string
		cred Credentials
		reg  string
	}{
		{"缺 Access Key", Credentials{SecretKey: vecSK}, vecRegion},
		{"缺 Secret Key", Credentials{AccessKeyID: vecAK}, vecRegion},
		{"缺区域", Credentials{AccessKeyID: vecAK, SecretKey: vecSK}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := Presign(r, PresignOptions{
				Credentials: c.cred, Region: c.reg, Expires: time.Hour,
			}); err == nil {
				t.Fatal("缺少必要参数应报错")
			}
		})
	}
}

// TestVerifyRejectsExpiredPresign 验证过期判定先于签名比对
// （否则"链接过期"会被误报成"签名不匹配"，让人白查半天）。
func TestVerifyRejectsExpiredPresign(t *testing.T) {
	at := mustParseAmz(t, "20261023T120000Z")
	r, err := http.NewRequest(http.MethodGet, "http://"+vecHost+"/b/k", nil)
	if err != nil {
		t.Fatalf("构造请求失败: %v", err)
	}
	r.Host = vecHost

	signed, err := Presign(r, PresignOptions{
		Credentials: Credentials{AccessKeyID: vecAK, SecretKey: vecSK},
		Region:      vecRegion,
		Expires:     5 * time.Minute,
		Now:         fixedNow(at),
	})
	if err != nil {
		t.Fatalf("Presign 失败: %v", err)
	}

	_, serr := Verify(mustRequest(t, http.MethodGet, signed), Options{
		Provider: NewStaticProvider(Credentials{AccessKeyID: vecAK, SecretKey: vecSK}),
		Region:   vecRegion,
		Now:      fixedNow(at.Add(time.Hour)), // 远超 5 分钟有效期
	})
	if serr == nil || serr.Code != "AccessDenied" {
		t.Fatalf("过期链接应返回 AccessDenied，实际 %v", serr)
	}
	if !strings.Contains(serr.Message, "过期") {
		t.Errorf("错误消息应说明已过期: %s", serr.Message)
	}
}

// TestStaticProviderSkipsEmptyKey 验证空 AccessKeyID 的条目被忽略 ——
// 否则一个"空凭据"会意外匹配上缺少凭据声明的请求。
func TestStaticProviderSkipsEmptyKey(t *testing.T) {
	p := NewStaticProvider(
		Credentials{AccessKeyID: "", SecretKey: "x"},
		Credentials{AccessKeyID: vecAK, SecretKey: vecSK},
	)
	if _, ok := p.Lookup(""); ok {
		t.Error("空 AccessKeyID 不应可查")
	}
	if _, ok := p.Lookup(vecAK); !ok {
		t.Error("有效凭据应可查")
	}
}
