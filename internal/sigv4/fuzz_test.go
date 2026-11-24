package sigv4

import (
	"slices"
	"strconv"
	"strings"
	"testing"
)

// 本文件是签名层的模糊测试基线：签名解析直接面对攻击者可完全控制的字节，
// 任何一个 panic 都是远程拒绝服务。断言除"不 panic"外还包括
// 规范化幂等性与解析-重写可逆性。
//
// 有界 fuzz：
//
//	go test -run=^$ -fuzz=^FuzzCanonicalQueryString$ -fuzztime=20s ./internal/sigv4
func FuzzCanonicalQueryString(f *testing.F) {
	for _, raw := range []string{
		"",
		"uploads",
		"uploads=",
		"a=1&b=2",
		"b=2&a=1",
		"a=2&a=1",
		"&&a=1&&",
		"a=b=c",
		"=v",
		"a",
		"a=1&",
		"X-Amz-Signature=deadbeef&X-Amz-Credential=a%2Fb",
	} {
		f.Add(raw)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		got := canonicalQueryString(raw)

		// 性质 1：规范化是幂等的 —— 把输出再规范化一次结果不变。
		// 规范化结果会被当作查询串再次进入签名流程（例如重放、代理重写），
		// 不幂等意味着"同一请求签名两次不一致"。
		if again := canonicalQueryString(got); again != got {
			t.Fatalf("规范化不幂等：input=%q first=%q second=%q", raw, got, again)
		}

		// 性质 2：键值对按 (key, value) 的原始编码字节序非降排列，
		// 且不产生空段。
		pairs := parseRawQuery(got)
		for i := 1; i < len(pairs); i++ {
			prev, cur := pairs[i-1], pairs[i]
			if prev.key > cur.key || (prev.key == cur.key && prev.val > cur.val) {
				t.Fatalf("键值对未按规范排序：%q → %q", got, []queryPair{prev, cur})
			}
		}
		for _, p := range pairs {
			if strings.Contains(p.key, "=") {
				t.Fatalf("键中残留 '='：%q → key=%q", got, p.key)
			}
		}
	})
}

// FuzzParseChunkHeader 断言 aws-chunked 块头解析：长度非负、签名要么缺省
// 要么是 64 位十六进制，并且解析结果可重写为等价块头再次解析。
func FuzzParseChunkHeader(f *testing.F) {
	hex64 := strings.Repeat("0123456789abcdef", 4)
	for _, line := range []string{
		"0",
		"1",
		"ff",
		"FFFF",
		"1000000000000000",
		"7fffffffffffffff",
		"8000000000000000",
		"1;chunk-signature=" + hex64,
		"1;" + strings.ToUpper(hex64),
		" 2a ; chunk-signature = " + hex64 + " ",
		"1;chunk-signature=short",
		"1;chunk-signature=" + hex64 + ";chunk-signature=" + hex64,
		"",
		"g",
		"-1",
		"0x10",
		"1;;",
	} {
		f.Add(line)
	}

	f.Fuzz(func(t *testing.T, line string) {
		size, signature, err := parseChunkHeader(line)
		if err != nil {
			return
		}
		if size < 0 {
			t.Fatalf("解析出负的块长度：input=%q → %d", line, size)
		}
		if signature != "" && (len(signature) != 64 || !isHexString(signature)) {
			t.Fatalf("接受非法 chunk-signature：input=%q sig=%q", line, signature)
		}

		rebuilt := strconv.FormatUint(uint64(size), 16)
		if signature != "" {
			rebuilt += ";chunk-signature=" + signature
		}
		gotSize, gotSignature, gotErr := parseChunkHeader(rebuilt)
		if gotErr != nil || gotSize != size || gotSignature != signature {
			t.Fatalf("块头不可逆：input=%q → (%d,%q)，重写为 %q 得到 (%d,%q) err=%v",
				line, size, signature, rebuilt, gotSize, gotSignature, gotErr)
		}
	})
}

// FuzzParseTrailerLine 断言 trailer 行解析：名字非空、已小写、字符集受限，
// 且 name+":"+value 重写后解析结果不变。
func FuzzParseTrailerLine(f *testing.F) {
	for _, line := range []string{
		"x-amz-checksum-crc32:AAAAAA==",
		"X-Amz-Checksum-CRC32: AAAAAA== ",
		"x-amz-checksum-sha256:abc:def",
		"a:b",
		"no-colon",
		":value",
		"name:",
		"bad name:value",
		"中文:值",
	} {
		f.Add(line)
	}

	f.Fuzz(func(t *testing.T, line string) {
		name, value, ok := parseTrailerLine(line)
		if !ok {
			return
		}
		if name == "" || name != strings.ToLower(name) || !validHeaderName(name) {
			t.Fatalf("返回了非法 trailer 名：input=%q name=%q", line, name)
		}

		againName, againValue, againOK := parseTrailerLine(name + ":" + value)
		if !againOK || againName != name || againValue != value {
			t.Fatalf("trailer 行不可逆：input=%q → (%q,%q)，重写得到 (%q,%q) ok=%v",
				line, name, value, againName, againValue, againOK)
		}
	})
}

// FuzzParseAuthorization 断言 Authorization 解析的成功结果满足签名校验
// 所需的最小形状，并且对同一输入幂等。
func FuzzParseAuthorization(f *testing.F) {
	for _, v := range []string{
		"AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=" + strings.Repeat("0", 64),
		"AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260101/us-east-1/s3/aws4_request",
		"AWS4-HMAC-SHA256",
		"",
		"Basic dXNlcjpwYXNz",
		"AWS4-HMAC-SHA256 Credential=a/b/c/s3/aws4_request, SignedHeaders=host, Signature=x",
		"AWS4-HMAC-SHA256 Credential=a/20260101//s3/aws4_request, SignedHeaders=host, Signature=x",
		"AWS4-HMAC-SHA256 Credential=a/abcdefgh/r/s3/aws4_request, SignedHeaders=host, Signature=x",
		"AWS4-HMAC-SHA256 Credential=a/20260101/r/s3/aws4_request, Credential=b/20260101/r/s3/aws4_request, SignedHeaders=host, Signature=x",
		"AWS4-HMAC-SHA256 Credential=a/20260101/r/s3/aws4_request, SignedHeaders=, Signature=x",
		"AWS4-HMAC-SHA256 Credential=a/20260101/r/s3/aws4_request, SignedHeaders=host, Signature=",
		"AWS4-HMAC-SHA256 Credential=a/20260101/r/s3/aws4_request, SignedHeaders=host, Signature=x, Foo=bar",
		"AWS4-HMAC-SHA256 Credential=a/20260101/r/s3/aws4_request, SignedHeaders=host, Signature=x,",
	} {
		f.Add(v)
	}

	f.Fuzz(func(t *testing.T, v string) {
		spec, serr := parseAuthorization(v)
		if serr != nil {
			return
		}
		if spec.accessKeyID == "" || len(spec.dateStamp) != 8 ||
			len(spec.signedHeaders) == 0 || spec.signature == "" {
			t.Fatalf("成功解析出残缺结果：input=%q → %+v", v, spec)
		}

		again, againErr := parseAuthorization(v)
		if againErr != nil || again.accessKeyID != spec.accessKeyID ||
			again.dateStamp != spec.dateStamp || again.region != spec.region ||
			again.signature != spec.signature ||
			!slices.Equal(again.signedHeaders, spec.signedHeaders) {
			t.Fatalf("解析不确定：input=%q first=%+v second=%+v err=%v", v, spec, again, againErr)
		}
	})
}
