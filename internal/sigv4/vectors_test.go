package sigv4

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// 本文件的向量全部来自**真实 aws-cli 2.36.2 的输出**（AWS 自己的签名器实现），
// 而不是从文档抄来的常量 —— 抄常量一旦抄错或记错，测试就会变成
// "把我的错误固化下来"，反而有害。
//
// 复现方式（离线可用，`presign` 与 `--debug` 都不需要服务端在线）：
//
//	export AWS_ACCESS_KEY_ID=AKIATESTTEST1234
//	export AWS_SECRET_ACCESS_KEY=test-secret-key-not-a-real-credential
//	export AWS_DEFAULT_REGION=us-east-1
//	export AWS_EC2_METADATA_DISABLED=true
//	aws s3 presign s3://my-bucket/obj.bin \
//	    --endpoint-url http://127.0.0.1:19000 --expires-in 300
//	aws --endpoint-url http://127.0.0.1:19000 s3api list-multipart-uploads \
//	    --bucket b --debug
//
// 向量里的 X-Amz-Date 是固定字符串，因此**签名可离线复算**：
// 校验时把时间源固定到该时刻即可，不依赖运行时刻。
//
// 局限（必须如实记录）：向量未覆盖 aws-chunked 分块签名（切片 10）。
const (
	vecAK     = "AKIATESTTEST1234"
	vecSK     = "test-secret-key-not-a-real-credential"
	vecRegion = "us-east-1"
	vecHost   = "127.0.0.1:19000"
	vecScope  = "20261023/us-east-1/s3/aws4_request"

	vecEmptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

func vecOptions(amzDate string) Options {
	reqTime, err := time.Parse(amzDateFormat, amzDate)
	if err != nil {
		panic("向量时间戳非法: " + amzDate)
	}
	return Options{
		Provider: NewStaticProvider(Credentials{AccessKeyID: vecAK, SecretKey: vecSK}),
		Region:   vecRegion,
		// 时间源固定在向量时刻：签名与日期无关部分必须逐字复现。
		Now: func() time.Time { return reqTime },
	}
}

// TestVerifyRealPresignSimple 用 aws-cli 生成的 Presigned URL 校验。
//
// 原始输出：
//
//	http://127.0.0.1:19000/my-bucket/obj.bin?X-Amz-Algorithm=AWS4-HMAC-SHA256
//	&X-Amz-Credential=AKIATESTTEST1234%2F20261023%2Fus-east-1%2Fs3%2Faws4_request
//	&X-Amz-Date=20261023T090952Z&X-Amz-Expires=300&X-Amz-SignedHeaders=host
//	&X-Amz-Signature=0150d8994207354edaf6f3dd935cecb6fc5bbafe5ca5591c13c2786a36a6eb09
func TestVerifyRealPresignSimple(t *testing.T) {
	const (
		amzDate = "20261023T090952Z"
		wantSig = "0150d8994207354edaf6f3dd935cecb6fc5bbafe5ca5591c13c2786a36a6eb09"
		rawURL  = "http://" + vecHost + "/my-bucket/obj.bin" +
			"?X-Amz-Algorithm=AWS4-HMAC-SHA256" +
			"&X-Amz-Credential=AKIATESTTEST1234%2F20261023%2Fus-east-1%2Fs3%2Faws4_request" +
			"&X-Amz-Date=" + amzDate +
			"&X-Amz-Expires=300" +
			"&X-Amz-SignedHeaders=host" +
			"&X-Amz-Signature=" + wantSig
	)

	r := mustRequest(t, http.MethodGet, rawURL)
	res, serr := Verify(r, vecOptions(amzDate))
	if serr != nil {
		t.Fatalf("aws-cli 向量应校验通过，实际失败: %s (%s)", serr.Code, serr.Message)
	}
	if !res.Presigned {
		t.Error("应识别为 Presigned URL")
	}
	if res.ContentSHA256 != unsignedPayload {
		t.Errorf("Presigned 请求体哈希应为 UNSIGNED-PAYLOAD，实际 %s", res.ContentSHA256)
	}
	if res.PayloadSigned() {
		t.Error("UNSIGNED-PAYLOAD 时 PayloadSigned 应为 false")
	}
	// 若把签名参数算进签名，结果必然不同 —— 断言这一点的反例：
	// 直接在 Query 里包含 X-Amz-Signature 必须导致失败。
	if _, e := Verify(mustRequest(t, http.MethodGet, rawURL),
		vecOptions(amzDate)); e == nil {
		// 已在上面验证通过；此处确认"排除签名"确实是必要条件。
		include := buildCanonical(r, []string{"host"}, unsignedPayload, false)
		exclude := buildCanonical(r, []string{"host"}, unsignedPayload, true)
		if include.Hash() == exclude.Hash() {
			t.Error("剔除 X-Amz-Signature 后规范化哈希不应相同")
		}
	}
}

// TestVerifyRealPresignEscaping 覆盖真实 URL 转义：
// key 中的空格编码为 %20、加号编码为 %2B（而非 `+` 或被解码）。
//
// 原始输出：
//
//	http://127.0.0.1:19000/b/weird%20name%2Bplus.txt?...&X-Amz-Date=20261023T091004Z
//	&X-Amz-Signature=c7c556633ff02cd901ed1519735a9bd5634bcc507a117c90d78cd9b0d0a34269
func TestVerifyRealPresignEscaping(t *testing.T) {
	const (
		amzDate = "20261023T091004Z"
		wantSig = "c7c556633ff02cd901ed1519735a9bd5634bcc507a117c90d78cd9b0d0a34269"
		rawURL  = "http://" + vecHost + "/b/weird%20name%2Bplus.txt" +
			"?X-Amz-Algorithm=AWS4-HMAC-SHA256" +
			"&X-Amz-Credential=AKIATESTTEST1234%2F20261023%2Fus-east-1%2Fs3%2Faws4_request" +
			"&X-Amz-Date=" + amzDate +
			"&X-Amz-Expires=300" +
			"&X-Amz-SignedHeaders=host" +
			"&X-Amz-Signature=" + wantSig
	)

	if _, serr := Verify(mustRequest(t, http.MethodGet, rawURL), vecOptions(amzDate)); serr != nil {
		t.Fatalf("含转义字符的 aws-cli 向量应校验通过，实际失败: %s (%s)", serr.Code, serr.Message)
	}
}

// TestVerifyRealPresignNestedKey 覆盖含斜杠的多级 key。
//
// 这条同时验证易错点 ①：CanonicalURI 必须用**未解码原始路径**且不做 path.Clean。
// 若实现里写了 path.Clean 或用了 r.URL.Path，签名立刻对不上。
//
// 原始输出：
//
//	http://127.0.0.1:19000/b/a/deep/nested/key.bin?...&X-Amz-Date=20261023T091004Z
//	&X-Amz-Signature=65572b1c1037a874439c5c064075f1ad0fd2065edb918a233eff2fc643e2c11c
func TestVerifyRealPresignNestedKey(t *testing.T) {
	const (
		amzDate = "20261023T091004Z"
		wantSig = "65572b1c1037a874439c5c064075f1ad0fd2065edb918a233eff2fc643e2c11c"
		rawURL  = "http://" + vecHost + "/b/a/deep/nested/key.bin" +
			"?X-Amz-Algorithm=AWS4-HMAC-SHA256" +
			"&X-Amz-Credential=AKIATESTTEST1234%2F20261023%2Fus-east-1%2Fs3%2Faws4_request" +
			"&X-Amz-Date=" + amzDate +
			"&X-Amz-Expires=300" +
			"&X-Amz-SignedHeaders=host" +
			"&X-Amz-Signature=" + wantSig
	)

	if _, serr := Verify(mustRequest(t, http.MethodGet, rawURL), vecOptions(amzDate)); serr != nil {
		t.Fatalf("多级 key 向量应校验通过，实际失败: %s (%s)", serr.Code, serr.Message)
	}
}

// TestVerifyRealHeaderSignedUploads 是**头部签名**的真实向量，覆盖两个易错点：
//
//	② 无值查询参数 `?uploads` 必须规范化为 `uploads=`；
//	③ SignedHeaders 由客户端声明为 host;x-amz-content-sha256;x-amz-date，
//	   多算或少算任何一个头都会失败。
//
// 原始抓包（aws --endpoint-url ... s3api list-multipart-uploads --bucket b --debug）：
//
//	GET http://127.0.0.1:19000/b?uploads
//	x-amz-content-sha256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
//	x-amz-date: 20261023T091034Z
//	Authorization: AWS4-HMAC-SHA256 Credential=AKIATESTTEST1234/20261023/us-east-1/s3/aws4_request,
//	  SignedHeaders=host;x-amz-content-sha256;x-amz-date,
//	  Signature=e60999e11a21971c2248da7b708e0e279d6eff4577a86eef55ccf429ce436877
func TestVerifyRealHeaderSignedUploads(t *testing.T) {
	const (
		amzDate = "20261023T091034Z"
		wantSig = "e60999e11a21971c2248da7b708e0e279d6eff4577a86eef55ccf429ce436877"
	)

	r := mustRequest(t, http.MethodGet, "http://"+vecHost+"/b?uploads")
	r.Header.Set(headerAmzDate, amzDate)
	r.Header.Set(headerContentSHA256, vecEmptySHA256)
	r.Header.Set(headerAuthorization,
		algorithm+" Credential="+vecAK+"/"+vecScope+
			", SignedHeaders=host;x-amz-content-sha256;x-amz-date"+
			", Signature="+wantSig)

	res, serr := Verify(r, vecOptions(amzDate))
	if serr != nil {
		t.Fatalf("头部签名向量应校验通过，实际失败: %s (%s)", serr.Code, serr.Message)
	}
	if res.Presigned {
		t.Error("不应识别为 Presigned")
	}
	if res.ContentSHA256 != vecEmptySHA256 {
		t.Errorf("ContentSHA256 = %s", res.ContentSHA256)
	}
	// 空体哈希参与签名，因此 PayloadSigned 应为 true。
	if !res.PayloadSigned() {
		t.Error("显式声明哈希时 PayloadSigned 应为 true")
	}
}

// TestCanonicalQueryUploadsNormalization 锁定易错点 ②：
// `?uploads` 必须变成 `uploads=`，且多参数按键的字节序排序。
func TestCanonicalQueryUploadsNormalization(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"无值参数补等号", "uploads", "uploads="},
		{"按键字节序排序", "list-type=2&prefix=b&max-keys=10", "list-type=2&max-keys=10&prefix=b"},
		{"空值保留", "a=&b=1", "a=&b=1"},
		{"大小写敏感排序", "B=1&a=2", "B=1&a=2"},
		{"连续与号被忽略", "a=1&&b=2&", "a=1&b=2"},
		{"值为空串", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := canonicalQueryString(c.in); got != c.want {
				t.Errorf("canonicalQueryString(%q) = %q, 期望 %q", c.in, got, c.want)
			}
		})
	}
}

// TestCanonicalURIKeepsRawPath 锁定易错点 ①：
// 路径必须原样保留，绝不做 Clean 或解码。
func TestCanonicalURIKeepsRawPath(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{"http://h/a/b", "/a/b"},
		{"http://h/a//b", "/a//b"},     // 双斜杠不得被折叠
		{"http://h/a/../b", "/a/../b"}, // 点段不得被消除
		{"http://h/a%20b", "/a%20b"},   // 不解码
		{"http://h/a%2Fb", "/a%2Fb"},   // 编码斜杠保持编码
		{"http://h/", "/"},
		{"http://h", "/"}, // 空路径补为 /
	}
	for _, c := range cases {
		u, err := url.Parse(c.raw)
		if err != nil {
			t.Fatalf("URL 解析失败 %q: %v", c.raw, err)
		}
		if got := canonicalURI(u); got != c.want {
			t.Errorf("canonicalURI(%q) = %q, 期望 %q", c.raw, got, c.want)
		}
	}
}

// TestSigningKeyTerminatorIsLiteral 锁定第四层 HMAC 的字面量。
//
// 末层若写成别的字符串（例如误用 dateStamp 或漏掉），
// 得到的密钥完全不同、全部签名失败，但错误信息只是
// "签名不匹配"，极难定位。这里用"改动终止符则结果必变"来钉住它。
func TestSigningKeyTerminatorIsLiteral(t *testing.T) {
	const (
		date   = "20261023"
		region = "us-east-1"
		svc    = "s3"
	)
	got := signingKey(vecSK, date, region, svc)

	// 逐层手算，与 signingKey 的结果比对。
	kDate := hmacSHA256([]byte("AWS4"+vecSK), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, svc)
	manual := hmacSHA256(kService, "aws4_request")
	if !hmacEqual(hexEncode(got), hexEncode(manual)) {
		t.Fatal("signingKey 与逐层手算不一致")
	}

	// 终止符不是 aws4_request 时，结果必须不同。
	notLiteral := hmacSHA256(kService, "aws4-request") // 连字符而非下划线
	if hmacEqual(hexEncode(got), hexEncode(notLiteral)) {
		t.Fatal("终止符必须严格为 aws4_request")
	}
}

// TestStringToSignLayout 校验四行结构。
func TestStringToSignLayout(t *testing.T) {
	got := stringToSign("20261023T090952Z", vecScope, "abc")
	want := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		"20261023T090952Z",
		vecScope,
		"abc",
	}, "\n")
	if got != want {
		t.Fatalf("待签名字符串布局错误\n实际:\n%s\n期望:\n%s", got, want)
	}
}

// emptyBodySHA256 保留为常量引用点，便于将来引入端到端官方向量时复用。
const emptyBodySHA256 = vecEmptySHA256

// mustRequest 构造测试用请求；URL 解析失败直接终止测试。
//
// 注意 Host 取自 URL —— net/http 在服务端把 Host 放在 r.Host 而非头里，
// 规范化时必须走 r.Host，这里模拟同样的形态。
func mustRequest(t *testing.T, method, rawURL string) *http.Request {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("URL 解析失败 %q: %v", rawURL, err)
	}
	return &http.Request{
		Method: method,
		URL:    u,
		Host:   u.Host,
		Header: http.Header{},
	}
}
