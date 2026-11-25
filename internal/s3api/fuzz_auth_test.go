package s3api

import (
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/binbinpeng123/Blink/internal/s3err"
	"github.com/binbinpeng123/Blink/internal/sigv4"
	"github.com/binbinpeng123/Blink/internal/store"
)

// 本文件补 FuzzServeHTTP 显式关闭鉴权留下的缺口：签名解析、凭据查找、
// 鉴权失败响应在随机输入下也要走一遍完整中间件链。
//
// 两种模式：
//   - 有效签名：用测试凭据现场对请求签名，断言请求抵达业务逻辑。
//   - 篡改：签名后破坏一处，断言得到 400/403 的鉴权错误而不是 5xx。
//
// 有界 fuzz（会启动 handler 与临时 store，不联网）：
//
//	go test -run=^$ -fuzz=^FuzzServeHTTPAuth$ -fuzztime=20s ./internal/s3api

const (
	fuzzAuthMutationNone = iota
	fuzzAuthMutationBadSignature
	fuzzAuthMutationDropSignature
	fuzzAuthMutationBadAccessKey
	fuzzAuthMutationPathChanged
	fuzzAuthMutationStaleDate
	fuzzAuthMutationDropAlgorithm
	fuzzAuthMutationCount
)

// fuzzAuthFailureCodes 是鉴权层面的失败码。有效签名的请求不应出现这些码；
// 篡改后的请求只允许出现这些码（或同样属于鉴权失败的 AuthorizationHeaderMalformed）。
var fuzzAuthFailureCodes = map[string]bool{
	string(s3err.CodeAccessDenied):                 true,
	string(s3err.CodeInvalidAccessKeyID):           true,
	string(s3err.CodeSignatureDoesNotMatch):        true,
	string(s3err.CodeRequestTimeTooSkewed):         true,
	string(s3err.CodeMissingSecurityHeader):        true,
	string(s3err.CodeAuthorizationHeaderMalformed): true,
}

// fuzzAuthPresignParams 是 Presign 会写入查询串的参数名（匹配时大小写不敏感）。
var fuzzAuthPresignParams = []string{
	"X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date",
	"X-Amz-Expires", "X-Amz-SignedHeaders", "X-Amz-Signature",
}

// FuzzServeHTTPAuth 校验开启鉴权后全链路对任意凭据输入的三条契约：
// 不 panic、有效签名能抵达业务逻辑、篡改签名只会得到鉴权失败。
func FuzzServeHTTPAuth(f *testing.F) {
	root, err := os.MkdirTemp("", "blink-fuzz-auth-*")
	if err != nil {
		f.Fatalf("创建临时存储目录失败: %v", err)
	}
	defer os.RemoveAll(root)

	// 与 FuzzServeHTTP 相同：请求体 ≤1 KiB 全部走内联，不产生 CAS 块文件。
	st, err := store.Open(store.Options{
		Root:            filepath.Join(root, "data"),
		BlockSize:       64 << 10,
		InlineThreshold: 64 << 10,
	})
	if err != nil {
		f.Fatalf("打开存储失败: %v", err)
	}
	if err := st.CreateBucket("bkt"); err != nil {
		f.Fatalf("建桶失败: %v", err)
	}
	h := New(Options{
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Region:   testRegion,
		Provider: sigv4.NewStaticProvider(sigv4.Credentials{AccessKeyID: testAK, SecretKey: testSK}),
		Store:    st,
	}).Handler()

	// 种子：有效签名覆盖各主链路，篡改覆盖每种破坏方式。
	for _, s := range []struct {
		name    string
		method  string
		path    string
		query   string
		headers string
		body    string
		mutate  int
	}{
		{"列出桶", http.MethodGet, "/", "", "", "", fuzzAuthMutationNone},
		{"列出对象", http.MethodGet, "/bkt", "list-type=2&delimiter=%2F", "", "", fuzzAuthMutationNone},
		{"写对象", http.MethodPut, "/bkt/signed.txt", "", "", "hello", fuzzAuthMutationNone},
		{"读对象", http.MethodGet, "/bkt/signed.txt", "", "", "", fuzzAuthMutationNone},
		{"对象头", http.MethodHead, "/bkt/signed.txt", "", "", "", fuzzAuthMutationNone},
		{"删对象", http.MethodDelete, "/bkt/signed.txt", "", "", "", fuzzAuthMutationNone},
		{"批量删除", http.MethodPost, "/bkt", "delete",
			"", `<Delete><Object><Key>a.txt</Key></Object></Delete>`, fuzzAuthMutationNone},
		{"初始化分片", http.MethodPost, "/bkt/mpu.bin", "uploads", "", "", fuzzAuthMutationNone},
		{"免签健康检查", http.MethodGet, "/healthz", "", "", "", fuzzAuthMutationNone},
		// 原始 '#' 若经 url.Parse 往返会被截成 fragment；保留为切分逻辑的回归。
		{"井号查询串", http.MethodGet, "/bkt", "#", "", "", fuzzAuthMutationNone},
		{"篡改签名", http.MethodGet, "/bkt", "", "", "", fuzzAuthMutationBadSignature},
		{"删除签名", http.MethodGet, "/bkt", "", "Authorization: AWS4-HMAC-SHA256 bad\n", "", fuzzAuthMutationDropSignature},
		{"错误 Access Key", http.MethodGet, "/bkt", "", "", "", fuzzAuthMutationBadAccessKey},
		{"签名后改路径", http.MethodGet, "/bkt", "", "", "", fuzzAuthMutationPathChanged},
		{"过期时间", http.MethodGet, "/bkt", "", "", "", fuzzAuthMutationStaleDate},
		{"缺 Algorithm", http.MethodGet, "/bkt", "", "", "", fuzzAuthMutationDropAlgorithm},
	} {
		f.Add(s.method, s.path, s.query, s.headers, s.body, s.mutate)
	}

	f.Fuzz(func(t *testing.T, method, path, rawQuery, headerBlock, body string, mutation int) {
		if len(body) > fuzzHTTPMaxBody || len(path) > fuzzHTTPMaxTarget ||
			len(rawQuery) > fuzzHTTPMaxTarget || len(headerBlock) > fuzzHTTPMaxHeaderBlock {
			t.Skip()
		}
		if method == "" {
			method = http.MethodGet
		}
		if !isHTTPToken(method) {
			t.Skip()
		}
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		if mutation < 0 {
			mutation = -mutation
		}
		mutation %= fuzzAuthMutationCount

		// 现场签发要求查询串里没有同名参数，否则会与签发结果重复。
		req := newFuzzRequest(method, path, stripPresignParams(rawQuery), headerBlock, body)
		signed, err := sigv4.Presign(req, sigv4.PresignOptions{
			Credentials: sigv4.Credentials{AccessKeyID: testAK, SecretKey: testSK},
			Region:      testRegion,
			Expires:     time.Hour,
		})
		if err != nil {
			t.Fatalf("签发 Presigned URL 失败（输入已过护栏，不应失败）：%v", err)
		}
		// 不经 url.Parse(signed) 往返：查询串里的 '#' 会被当成 fragment 截断，
		// 而控制字符会让 url.Parse 直接报错。Presign 只追加查询串、不改路径，
		// 因此按第一个 '?' 切出路径与查询串即可（路径内的 '?' 已由 URL 转义）。
		_, signedQuery, ok := strings.Cut(signed, "?")
		if !ok {
			t.Fatalf("签发结果缺少查询串：%q", signed)
		}
		u := &url.URL{Path: path, RawQuery: signedQuery}
		mutatePresignedURL(u, mutation)
		req.URL = u

		defer func() {
			if p := recover(); p != nil {
				t.Errorf("请求逃逸出未捕获的 panic：%v\nmethod=%q path=%q query=%q headers=%q body=%q mutation=%d",
					p, method, u.Path, u.RawQuery, headerBlock, body, mutation)
			}
		}()

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if strings.Contains(rec.Body.String(), testSK) {
			t.Fatalf("响应泄露了 Secret Key：method=%q path=%q mutation=%d body=%q",
				method, u.Path, mutation, rec.Body.String())
		}

		code := checkFuzzResponse(t, rec, method, u.Path, u.RawQuery, headerBlock, body)

		// POST Object 的签名材料在表单里，现场签发的查询串签名不参与校验；
		// 免签路径直接越过鉴权。两者都不适用下面的模式断言。
		if isPostObjectRequest(req) || unauthenticatedPaths[u.Path] {
			return
		}

		if mutation == fuzzAuthMutationNone {
			// HEAD 的错误体是空的，取不到 code；只有带错误体时才能判断。
			if code != "" && fuzzAuthFailureCodes[code] {
				t.Fatalf("有效签名被拒为鉴权失败：code=%s method=%q path=%q query=%q",
					code, method, u.Path, u.RawQuery)
			}
			return
		}

		if rec.Code != http.StatusBadRequest && rec.Code != http.StatusForbidden {
			t.Fatalf("篡改签名后状态码 = %d，期望 400 或 403：method=%q path=%q mutation=%d",
				rec.Code, method, u.Path, mutation)
		}
		if method != http.MethodHead && !fuzzAuthFailureCodes[code] {
			t.Fatalf("篡改签名后错误码 = %q，不属于鉴权失败集合：method=%q path=%q mutation=%d body=%q",
				code, method, u.Path, mutation, rec.Body.String())
		}
	})
}

// stripPresignParams 剔除输入查询串里与 Presign 同名的参数，其余字节原样保留。
func stripPresignParams(raw string) string {
	if raw == "" {
		return ""
	}
	segs := strings.Split(raw, "&")
	out := segs[:0]
	for _, seg := range segs {
		if isPresignParam(rawQueryKey(seg)) {
			continue
		}
		out = append(out, seg)
	}
	return strings.Join(out, "&")
}

// mutatePresignedURL 按模式破坏已签名的 URL。
//
// 全部在原始查询串上做替换，不经过解码再编码：重新编码会改变规范化结果，
// 让"篡改"变成"换了个合法请求"，断言就失去意义。
func mutatePresignedURL(u *url.URL, mutation int) {
	switch mutation {
	case fuzzAuthMutationBadSignature:
		if v, ok := rawParamValueFold(u.RawQuery, "X-Amz-Signature"); ok {
			u.RawQuery = rewriteRawParam(u.RawQuery, "X-Amz-Signature", v+"0")
		}
	case fuzzAuthMutationDropSignature:
		u.RawQuery = dropRawParamFold(u.RawQuery, "X-Amz-Signature")
	case fuzzAuthMutationBadAccessKey:
		// 只改 X-Amz-Credential 里的 AK 段：直接替换查询串里的字符串可能命中
		// 输入自带的同名片段，签名反而保持有效。
		if v, ok := rawParamValueFold(u.RawQuery, "X-Amz-Credential"); ok {
			u.RawQuery = rewriteRawParam(u.RawQuery, "X-Amz-Credential",
				strings.Replace(v, testAK, "AKIAFUZZINVALID0000", 1))
		}
	case fuzzAuthMutationPathChanged:
		u.Path += "x"
	case fuzzAuthMutationStaleDate:
		u.RawQuery = rewriteRawParam(u.RawQuery, "X-Amz-Date", "20000101T000000Z")
	case fuzzAuthMutationDropAlgorithm:
		u.RawQuery = dropRawParamFold(u.RawQuery, "X-Amz-Algorithm")
	}
}

func isPresignParam(name string) bool {
	return slices.ContainsFunc(fuzzAuthPresignParams, func(p string) bool {
		return strings.EqualFold(p, name)
	})
}

// rawQueryKey 返回单个 "k=v" 段的键（不做百分号解码）。
func rawQueryKey(seg string) string {
	k, _, _ := strings.Cut(seg, "=")
	if dec, err := url.QueryUnescape(k); err == nil {
		return dec
	}
	return k
}

// rawParamValueFold 取原始查询串中首个匹配参数的值（未解码）。
func rawParamValueFold(raw, name string) (string, bool) {
	for _, seg := range strings.Split(raw, "&") {
		if !strings.EqualFold(rawQueryKey(seg), name) {
			continue
		}
		_, v, _ := strings.Cut(seg, "=")
		return v, true
	}
	return "", false
}

// rewriteRawParam 只替换首个匹配参数的**值**，键的原始编码与其它段保持不变。
func rewriteRawParam(raw, name, value string) string {
	segs := strings.Split(raw, "&")
	for i, seg := range segs {
		if !strings.EqualFold(rawQueryKey(seg), name) {
			continue
		}
		k, _, _ := strings.Cut(seg, "=")
		segs[i] = k + "=" + value
		break
	}
	return strings.Join(segs, "&")
}

// dropRawParamFold 删除全部匹配参数（大小写不敏感）。
func dropRawParamFold(raw, name string) string {
	segs := strings.Split(raw, "&")
	out := segs[:0]
	for _, seg := range segs {
		if strings.EqualFold(rawQueryKey(seg), name) {
			continue
		}
		out = append(out, seg)
	}
	return strings.Join(out, "&")
}

// FuzzParsePostPolicy 校验 POST Object policy 文档的解析：成功时条件与过期时刻
// 必须是规范化后的形态，否则下游 evaluatePostPolicy 的覆盖检查会失效。
func FuzzParsePostPolicy(f *testing.F) {
	f.Add(encodePolicy(`{"expiration":"2099-01-01T00:00:00Z","conditions":[` +
		`{"bucket":"bkt"},["eq","$key","a.txt"],["starts-with","$Content-Type","image/"],` +
		`["content-length-range",0,1024]]}`))
	f.Add(encodePolicy(`{"expiration":"2099-01-01T00:00:00Z","conditions":[]}`))
	f.Add(encodePolicy(`{"expiration":"2099-01-01T00:00:00Z"}`))
	f.Add(encodePolicy(`{"conditions":[]}`))
	f.Add(encodePolicy(`{"expiration":"not-a-time","conditions":[]}`))
	f.Add(encodePolicy(`{"expiration":"2099-01-01T00:00:00Z","conditions":[{"a":"1","b":"2"}]}`))
	f.Add(encodePolicy(`{"expiration":"2099-01-01T00:00:00Z","conditions":[["eq","$key"]]}`))
	f.Add(encodePolicy(`{"expiration":"2099-01-01T00:00:00Z","conditions":[["starts-with","$key",1]]}`))
	f.Add(encodePolicy(`{"expiration":"2099-01-01T00:00:00Z","conditions":[["content-length-range",10,1]]}`))
	f.Add(encodePolicy(`{"expiration":"2099-01-01T00:00:00Z","conditions":[["content-length-range",-1,1]]}`))
	f.Add(encodePolicy(`{"expiration":"2099-01-01T00:00:00Z","conditions":[["unknown","$key","v"]]}`))
	f.Add(encodePolicy(`{"expiration":"2099-01-01T00:00:00Z","conditions":[42]}`))
	f.Add(encodePolicy(`{"expiration":"2099-01-01T00:00:00Z","conditions":[{"k":null}]}`))
	f.Add(encodePolicy(`{"expiration":"2099-01-01T00:00:00Z","conditions":[],"extra":1}`))
	f.Add("not-base64!!")
	f.Add("")

	f.Fuzz(func(t *testing.T, encoded string) {
		conds, exp, serr := parsePostPolicy(encoded)
		if serr != nil {
			if conds != nil || !exp.IsZero() {
				t.Fatalf("解析失败却返回了结果：conds=%+v exp=%v err=%s", conds, exp, serr.Message)
			}
			return
		}
		if exp.IsZero() {
			t.Fatalf("解析成功但过期时刻为零：input=%q", encoded)
		}
		for i, c := range conds {
			switch c.op {
			case "eq", "starts-with":
				if norm := normalizePolicyField(c.field); norm != c.field {
					t.Fatalf("条件 %d 字段未规范化：%q → %q", i, c.field, norm)
				}
			case "content-length-range":
				if c.field != "" || c.min < 0 || c.max < c.min {
					t.Fatalf("条件 %d 长度区间非法：%+v", i, c)
				}
			default:
				t.Fatalf("条件 %d 操作未知：%q", i, c.op)
			}
		}

		conds2, exp2, serr2 := parsePostPolicy(encoded)
		if serr2 != nil || !exp2.Equal(exp) || len(conds2) != len(conds) {
			t.Fatalf("重复解析结果不一致：input=%q", encoded)
		}
		for i := range conds {
			if conds2[i] != conds[i] {
				t.Fatalf("条件 %d 重复解析不一致：%+v vs %+v", i, conds[i], conds2[i])
			}
		}
	})
}

// encodePolicy 把 policy JSON 编码成 base64，供种子语料使用。
func encodePolicy(doc string) string {
	return base64.StdEncoding.EncodeToString([]byte(doc))
}
