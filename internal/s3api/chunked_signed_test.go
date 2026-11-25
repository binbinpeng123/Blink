package s3api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/binbinpeng123/Blink/internal/checksum"
	"github.com/binbinpeng123/Blink/internal/sigv4"
	"github.com/binbinpeng123/Blink/internal/store"
)

// 本文件是签名分块（STREAMING-AWS4-HMAC-SHA256-PAYLOAD*）的端到端测试。
//
// sigv4 包已有解帧与签名的单元测试，但那是"用产品代码验产品代码"：
// 两侧共用 canonicalTrailer / stringToSign 等私有函数，同错同对。
// 这里在 s3api 层**独立实现**一遍 SigV4 头部签名与块签名链，
// 走完整链路（鉴权中间件 → decodeChunkedBody → 存储），
// 让两套实现互为交叉验证 —— 任一侧把协议写错都会在这里暴露。

// testEmptySHA256Hex 是空字符串的 SHA256，签名块与 trailer 的固定取值。
var testEmptySHA256Hex = testSHA256Hex(nil)

// signedChunkedTestServer 装配开启鉴权 + 真实存储的 handler。
// 端到端测试必须经过鉴权中间件：ChunkSigner 正是在那里由
// 请求签名（seed）构造出来的。
func signedChunkedTestServer(t *testing.T) http.Handler {
	t.Helper()
	st, err := store.Open(store.Options{
		Root:            filepath.Join(t.TempDir(), "data"),
		BlockSize:       64 << 10,
		InlineThreshold: 16,
	})
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	if err := st.CreateBucket("bkt"); err != nil {
		t.Fatalf("建桶失败: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(Options{
		Logger:   logger,
		Region:   testRegion,
		Provider: sigv4.NewStaticProvider(sigv4.Credentials{AccessKeyID: testAK, SecretKey: testSK}),
		Store:    st,
	}).Handler()
}

// signV4 用独立实现给请求签名，返回 seed 签名（块签名链的第一环）。
//
// 只支持无查询串的请求：本文件覆盖的是 PUT/GET 对象，签名材料里
// 没有需要排序的查询参数；为它们维护一份 CanonicalQueryString
// 只会增加"测试自己写错"的面积。
func signV4(t *testing.T, r *http.Request, payloadHash string, signed []string) (seed, amzDate, scope string) {
	t.Helper()
	if r.URL.RawQuery != "" {
		t.Fatalf("签名助手只支持无查询串的请求: %q", r.URL.RawQuery)
	}
	amzDate = time.Now().UTC().Format("20060102T150405Z")
	date := amzDate[:8]
	r.Header.Set("X-Amz-Date", amzDate)
	r.Header.Set("X-Amz-Content-SHA256", payloadHash)

	var hb strings.Builder
	for _, name := range signed {
		hb.WriteString(name)
		hb.WriteByte(':')
		hb.WriteString(testCanonicalHeaderValue(t, r, name))
		hb.WriteByte('\n')
	}
	canonical := strings.Join([]string{
		r.Method,
		r.URL.EscapedPath(),
		"",
		hb.String(),
		strings.Join(signed, ";"),
		payloadHash,
	}, "\n")

	scope = date + "/" + testRegion + "/s3/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" +
		testSHA256Hex([]byte(canonical))
	seed = testHMACHex(testSigningKey(testSK, date), stringToSign)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+testAK+"/"+scope+
		", SignedHeaders="+strings.Join(signed, ";")+
		", Signature="+seed)
	return seed, amzDate, scope
}

// testCanonicalHeaderValue 复刻服务端的取值规则：host 取 r.Host，
// 其余取全部值、逗号连接后折叠空白。
func testCanonicalHeaderValue(t *testing.T, r *http.Request, name string) string {
	t.Helper()
	if name == "host" {
		return strings.Join(strings.Fields(r.Host), " ")
	}
	vals := r.Header.Values(http.CanonicalHeaderKey(name))
	if len(vals) == 0 {
		t.Fatalf("签名头 %q 未设置", name)
	}
	return strings.Join(strings.Fields(strings.Join(vals, ",")), " ")
}

func testSHA256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func testHMACSHA256(key []byte, msg string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(msg))
	return m.Sum(nil)
}

func testHMACHex(key []byte, msg string) string {
	return hex.EncodeToString(testHMACSHA256(key, msg))
}

func testSigningKey(secret, date string) []byte {
	k := testHMACSHA256([]byte("AWS4"+secret), date)
	k = testHMACSHA256(k, testRegion)
	k = testHMACSHA256(k, "s3")
	return testHMACSHA256(k, "aws4_request")
}

// testChunkChain 模拟客户端逐块签名。链从请求签名（seed）开始，
// 每签一块就把 prev 推进到该块签名；trailer 签在结束块之后。
type testChunkChain struct {
	key     []byte
	amzDate string
	scope   string
	seed    string
	prev    string
}

// frame 生成一个数据块：`<hex-size>;chunk-signature=<sig>` + 数据 + CRLF。
func (c *testChunkChain) frame(data []byte) string {
	sig := c.sign("AWS4-HMAC-SHA256-PAYLOAD", testSHA256Hex(data))
	var b strings.Builder
	fmt.Fprintf(&b, "%x;chunk-signature=%s\r\n", len(data), sig)
	b.Write(data)
	b.WriteString("\r\n")
	return b.String()
}

// end 生成结束块（size=0），可选地附上 trailer 及其签名。
func (c *testChunkChain) end(trailer [][2]string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "0;chunk-signature=%s\r\n",
		c.sign("AWS4-HMAC-SHA256-PAYLOAD", testEmptySHA256Hex))
	if len(trailer) > 0 {
		sorted := append([][2]string(nil), trailer...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i][0] < sorted[j][0] })
		var canonical strings.Builder
		for _, kv := range sorted {
			canonical.WriteString(kv[0] + ":" + kv[1] + "\n")
		}
		for _, kv := range trailer {
			b.WriteString(kv[0] + ":" + kv[1] + "\r\n")
		}
		b.WriteString("x-amz-trailer-signature:" +
			c.sign("AWS4-HMAC-SHA256-TRAILER", testSHA256Hex([]byte(canonical.String()))) + "\r\n")
	}
	b.WriteString("\r\n")
	return b.String()
}

// body 按 chunkSize 切分内容，生成完整的签名分块请求体。
func (c *testChunkChain) body(content []byte, chunkSize int, trailer [][2]string) string {
	var b strings.Builder
	for rest := content; len(rest) > 0; {
		n := chunkSize
		if n > len(rest) {
			n = len(rest)
		}
		b.WriteString(c.frame(rest[:n]))
		rest = rest[n:]
	}
	b.WriteString(c.end(trailer))
	return b.String()
}

func (c *testChunkChain) sign(prefix, payloadHex string) string {
	fields := []string{prefix, c.amzDate, c.scope, c.prev}
	if prefix != "AWS4-HMAC-SHA256-TRAILER" {
		fields = append(fields, testEmptySHA256Hex)
	}
	fields = append(fields, payloadHex)
	c.prev = testHMACHex(c.key, strings.Join(fields, "\n"))
	return c.prev
}

// newSignedChunkedPut 构造一条头部签名合法的签名分块 PUT。
// 请求体由调用方用返回的 chain 生成后挂到 req.Body 上。
func newSignedChunkedPut(t *testing.T, target string, decodedLen int, alg checksum.Algorithm) (*http.Request, *testChunkChain) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, target, nil)
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("x-amz-decoded-content-length", strconv.Itoa(decodedLen))

	payloadHash := "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	signed := []string{
		"content-encoding", "host", "x-amz-content-sha256",
		"x-amz-date", "x-amz-decoded-content-length",
	}
	if alg != "" {
		payloadHash += "-TRAILER"
		req.Header.Set("x-amz-sdk-checksum-algorithm", string(alg))
		req.Header.Set("x-amz-trailer", alg.Header())
		signed = append(signed, "x-amz-sdk-checksum-algorithm", "x-amz-trailer")
	}
	req.Header.Set("X-Amz-Content-SHA256", payloadHash)

	seed, amzDate, scope := signV4(t, req, payloadHash, signed)
	return req, &testChunkChain{
		key:     testSigningKey(testSK, amzDate[:8]),
		amzDate: amzDate,
		scope:   scope,
		seed:    seed,
		prev:    seed,
	}
}

// serveSigned 发送请求并返回响应记录。
func serveSigned(t *testing.T, h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// signedGet 发送一条头部签名的 GET，用于回读写入结果。
func signedGet(t *testing.T, h http.Handler, target string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	signed := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	signV4(t, req, testEmptySHA256Hex, signed)
	return serveSigned(t, h, req)
}

// tamperSignatureAt 把请求体中第 occurrence 个（0 起）marker 之后的
// 64 位十六进制签名的首字符翻转，用于构造"签名被篡改"的请求。
func tamperSignatureAt(t *testing.T, body, marker string, occurrence int) string {
	t.Helper()
	idx := 0
	for i := 0; i <= occurrence; i++ {
		j := strings.Index(body[idx:], marker)
		if j < 0 {
			t.Fatalf("请求体中找不到第 %d 个 %q", occurrence, marker)
		}
		idx += j + len(marker)
	}
	if idx+64 > len(body) {
		t.Fatalf("%q 之后不足 64 位签名", marker)
	}
	b := []byte(body)
	if b[idx] == '0' {
		b[idx] = '1'
	} else {
		b[idx] = '0'
	}
	return string(b)
}

// TestPutObjectSignedChunked 覆盖最基础的签名分块：多块、无 trailer。
// 链路必须完整走通：请求签名 → 逐块验签 → 解帧落盘 → 回读一致。
func TestPutObjectSignedChunked(t *testing.T) {
	h := signedChunkedTestServer(t)
	const key = "signed/multi-block"
	content := []byte("blink signed chunked payload across three chunks")

	req, chain := newSignedChunkedPut(t, "/bkt/"+key, len(content), "")
	req.Body = io.NopCloser(strings.NewReader(chain.body(content, 11, nil)))

	rec := serveSigned(t, h, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT = %d：%s", rec.Code, rec.Body.String())
	}
	if got, want := rec.Header().Get("ETag"), `"`+md5Hex(string(content))+`"`; got != want {
		t.Errorf("ETag = %s，期望 %s", got, want)
	}

	get := signedGet(t, h, "/bkt/"+key)
	if get.Code != http.StatusOK {
		t.Fatalf("GET = %d：%s", get.Code, get.Body.String())
	}
	if got := get.Body.String(); got != string(content) {
		t.Errorf("回读内容 = %q，期望 %q", got, content)
	}
}

// TestPutObjectSignedChunkedTrailer 覆盖带 trailer 的签名分块：
// 校验和值只在请求体末尾出现，且自身带一条签名。
func TestPutObjectSignedChunkedTrailer(t *testing.T) {
	h := signedChunkedTestServer(t)
	const key = "signed/trailer"
	content := []byte("signed chunked payload with crc32 trailer")

	sum, _ := checksum.New(checksum.AlgCRC32)
	_, _ = sum.Write(content)
	trailer := [][2]string{{checksum.AlgCRC32.Header(), sum.Encoded()}}

	req, chain := newSignedChunkedPut(t, "/bkt/"+key, len(content), checksum.AlgCRC32)
	req.Body = io.NopCloser(strings.NewReader(chain.body(content, 8, trailer)))

	rec := serveSigned(t, h, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT = %d：%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get(checksum.AlgCRC32.Header()); got != sum.Encoded() {
		t.Errorf("回传校验和 = %q，期望 %q", got, sum.Encoded())
	}
	if got := signedGet(t, h, "/bkt/"+key); got.Code != http.StatusOK || got.Body.String() != string(content) {
		t.Fatalf("回读 = %d %q", got.Code, got.Body.String())
	}
}

// TestPutObjectSignedChunkedRejectsBadTrailerChecksum 验证签名与内容完整性
// 是两件事：客户端把 trailer 值写错时，trailer 签名本身是"诚实的"，
// 但解帧后的内容与声明值不符，仍必须 BadDigest。
func TestPutObjectSignedChunkedRejectsBadTrailerChecksum(t *testing.T) {
	h := signedChunkedTestServer(t)
	content := []byte("payload whose checksum trailer lies")

	trailer := [][2]string{{checksum.AlgCRC32.Header(), "AAAAAA=="}}
	req, chain := newSignedChunkedPut(t, "/bkt/signed/bad-checksum", len(content), checksum.AlgCRC32)
	req.Body = io.NopCloser(strings.NewReader(chain.body(content, 9, trailer)))

	rec := serveSigned(t, h, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，期望 400：%s", rec.Code, rec.Body.String())
	}
	if got := decodeError(t, rec).Code; got != "BadDigest" {
		t.Errorf("错误码 = %q，期望 BadDigest", got)
	}
}

// TestPutObjectSignedChunkedRejectsTamperedChunk 篡改任一块的签名：
// 必须 403，且对象不能落盘 —— 部分验证通过的前缀也不能当成功。
func TestPutObjectSignedChunkedRejectsTamperedChunk(t *testing.T) {
	h := signedChunkedTestServer(t)
	content := []byte("first chunk is fine but second chunk is tampered")

	req, chain := newSignedChunkedPut(t, "/bkt/signed/tampered-chunk", len(content), "")
	body := tamperSignatureAt(t, chain.body(content, 10, nil), "chunk-signature=", 1)
	req.Body = io.NopCloser(strings.NewReader(body))

	rec := serveSigned(t, h, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("状态码 = %d，期望 403：%s", rec.Code, rec.Body.String())
	}
	if got := decodeError(t, rec).Code; got != "SignatureDoesNotMatch" {
		t.Errorf("错误码 = %q，期望 SignatureDoesNotMatch", got)
	}
	if got := signedGet(t, h, "/bkt/signed/tampered-chunk"); got.Code != http.StatusNotFound {
		t.Errorf("验签失败的对象不应存在，GET = %d：%s", got.Code, got.Body.String())
	}
}

// TestPutObjectSignedChunkedRejectsTamperedTrailer 篡改 trailer 签名：
// 块数据全部正确也必须在最后一步拒绝。
func TestPutObjectSignedChunkedRejectsTamperedTrailer(t *testing.T) {
	h := signedChunkedTestServer(t)
	content := []byte("chunks are valid, trailer signature is not")

	sum, _ := checksum.New(checksum.AlgCRC32)
	_, _ = sum.Write(content)
	trailer := [][2]string{{checksum.AlgCRC32.Header(), sum.Encoded()}}

	req, chain := newSignedChunkedPut(t, "/bkt/signed/tampered-trailer", len(content), checksum.AlgCRC32)
	body := tamperSignatureAt(t, chain.body(content, 12, trailer), "x-amz-trailer-signature:", 0)
	req.Body = io.NopCloser(strings.NewReader(body))

	rec := serveSigned(t, h, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("状态码 = %d，期望 403：%s", rec.Code, rec.Body.String())
	}
	if got := decodeError(t, rec).Code; got != "SignatureDoesNotMatch" {
		t.Errorf("错误码 = %q，期望 SignatureDoesNotMatch", got)
	}
}

// TestPutObjectSignedChunkedRejectsBrokenChain 每块签名必须绑定前一块：
// 第二块若仍用 seed 作为 prev，签名本身"格式合法"，但链是断的。
func TestPutObjectSignedChunkedRejectsBrokenChain(t *testing.T) {
	h := signedChunkedTestServer(t)
	first := []byte("AAAAAAAAAA")
	second := []byte("BBBBBBBBBB")

	req, chain := newSignedChunkedPut(t, "/bkt/signed/broken-chain", len(first)+len(second), "")
	frame1 := chain.frame(first)
	chain.prev = chain.seed // 回退：模拟客户端没有把第一块签名喂给第二块
	frame2 := chain.frame(second)
	req.Body = io.NopCloser(strings.NewReader(frame1 + frame2 + chain.end(nil)))

	rec := serveSigned(t, h, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("状态码 = %d，期望 403：%s", rec.Code, rec.Body.String())
	}
	if got := decodeError(t, rec).Code; got != "SignatureDoesNotMatch" {
		t.Errorf("错误码 = %q，期望 SignatureDoesNotMatch", got)
	}
}

// TestPutObjectSignedChunkedRejectsBadDecodedLength 声明的解帧长度与
// 实际块内容不符：必须在落盘前按协议错误拒绝，而不是存下截断的对象。
func TestPutObjectSignedChunkedRejectsBadDecodedLength(t *testing.T) {
	h := signedChunkedTestServer(t)
	content := []byte("declared length is one byte too large")

	// 头部声明 +1，请求体仍按真实内容生成 —— 签名链本身完全合法。
	req, chain := newSignedChunkedPut(t, "/bkt/signed/bad-length", len(content)+1, "")
	req.Body = io.NopCloser(strings.NewReader(chain.body(content, 8, nil)))

	rec := serveSigned(t, h, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，期望 400：%s", rec.Code, rec.Body.String())
	}
	if got := decodeError(t, rec).Code; got != "InvalidArgument" {
		t.Errorf("错误码 = %q，期望 InvalidArgument", got)
	}
}

// TestPutObjectSignedChunkedRequiresSigner 服务端没有签名链（鉴权被关闭）时，
// 签名分块请求必须 500 而不是"跳过验签继续落盘"。
func TestPutObjectSignedChunkedRequiresSigner(t *testing.T) {
	h := objectTestServer(t) // DisableAuth=true：authFrom(r) == nil

	rec := putObject(t, h, "/bkt/signed/no-signer", "x", func(r *http.Request) {
		r.Header.Set("Content-Encoding", "aws-chunked")
		r.Header.Set("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
		r.Header.Set("x-amz-decoded-content-length", "1")
	})
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("状态码 = %d，期望 500：%s", rec.Code, rec.Body.String())
	}
	if got := decodeError(t, rec).Code; got != "InternalError" {
		t.Errorf("错误码 = %q，期望 InternalError", got)
	}
}
