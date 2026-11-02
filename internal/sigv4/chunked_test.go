package sigv4

import (
	"crypto/sha256"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/binbinpeng123/Blink/internal/s3err"
)

// 测试向量由一份独立的 Python 实现（hashlib + hmac）生成，
// 而不是由被测代码自己产出 —— 否则签名文本写错时测试会一起错。
const (
	chunkVecKey      = "test-signing-key"
	chunkVecAmzDate  = "20240101T000000Z"
	chunkVecScope    = "20240101/us-east-1/s3/aws4_request"
	chunkVecSeed     = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	chunkVecChunk1   = "4d7fd96f90a573c58f5f7678b538a43c7f0074d0525ec52b023d441be0285b73" // "hello world\n"
	chunkVecChunk2   = "b1e2ba503341747ac144369dc6f0fb3fd4dc5e784b12ee1c6f3ec974b58d319b" // "second block"
	chunkVecFinal    = "2b1c3c7d1b2128f090f394b5aed4b821dfac7ccb54f09b47c2d186823822730c" // 空块
	chunkVecTrailer  = "056aeec3789ae22e74f674273f2633135d87486277b80f8e5f5260f37c6df70c"
	chunkVecTrailerC = "x-amz-checksum-crc32:AAAA\n"
)

func newTestSigner() *ChunkSigner {
	return &ChunkSigner{
		key:     []byte(chunkVecKey),
		amzDate: chunkVecAmzDate,
		scope:   chunkVecScope,
		prev:    chunkVecSeed,
	}
}

// TestChunkSignerKnownVector 用固定向量校验签名文本的逐行构造。
func TestChunkSignerKnownVector(t *testing.T) {
	s := newTestSigner()

	if !s.verifyChunk(sha256Hex([]byte("hello world\n")), chunkVecChunk1) {
		t.Fatal("第一个数据块签名向量不通过")
	}
	if s.prev != chunkVecChunk1 {
		t.Fatalf("签名链未推进：prev=%s", s.prev)
	}
	if !s.verifyChunk(sha256Hex([]byte("second block")), chunkVecChunk2) {
		t.Fatal("第二个数据块签名向量不通过")
	}
	if !s.verifyChunk(emptySHA256Hex, chunkVecFinal) {
		t.Fatal("结束块签名向量不通过")
	}
	if !s.verifyTrailer(sha256Hex([]byte(chunkVecTrailerC)), chunkVecTrailer) {
		t.Fatal("trailer 签名向量不通过")
	}

	// 篡改任何一环都必须失败。
	s2 := newTestSigner()
	bad := chunkVecChunk1[:63] + "0"
	if bad == chunkVecChunk1 {
		bad = chunkVecChunk1[:63] + "1"
	}
	if s2.verifyChunk(sha256Hex([]byte("hello world\n")), bad) {
		t.Fatal("错误的数据块签名被接受")
	}
}

// unsignedBody 构造未签名分块流。
func unsignedBody(chunks []string, trailers ...string) string {
	var b strings.Builder
	for _, c := range chunks {
		b.WriteString(hexLen(len(c)))
		b.WriteString("\r\n")
		b.WriteString(c)
		b.WriteString("\r\n")
	}
	b.WriteString("0\r\n")
	for _, tr := range trailers {
		b.WriteString(tr)
		b.WriteString("\r\n")
	}
	b.WriteString("\r\n")
	return b.String()
}

func hexLen(n int) string {
	const hexdigits = "0123456789abcdef"
	if n == 0 {
		return "0"
	}
	var buf [16]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = hexdigits[n&0xf]
		n >>= 4
	}
	return string(buf[i:])
}

func readAll(t *testing.T, body string, opt ChunkedOptions) (string, error) {
	t.Helper()
	cr := NewChunkedReader(strings.NewReader(body), opt)
	out, err := io.ReadAll(cr)
	return string(out), err
}

func wantCode(t *testing.T, err error, code s3err.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("期望错误码 %s，实际成功", code)
	}
	var se *s3err.Error
	if !errors.As(err, &se) {
		t.Fatalf("期望 *s3err.Error，实际 %T: %v", err, err)
	}
	if se.Code != code {
		t.Fatalf("期望错误码 %s，实际 %s（%s）", code, se.Code, se.Message)
	}
}

func TestChunkedReaderUnsigned(t *testing.T) {
	body := unsignedBody([]string{"hello", " world"})
	got, err := readAll(t, body, ChunkedOptions{DecodedContentLength: 11})
	if err != nil {
		t.Fatalf("解帧失败: %v", err)
	}
	if got != "hello world" {
		t.Fatalf("内容不符: %q", got)
	}
}

func TestChunkedReaderUnsignedTrailer(t *testing.T) {
	body := unsignedBody([]string{"hello"}, "x-amz-checksum-crc32:AAAA")
	cr := NewChunkedReader(strings.NewReader(body), ChunkedOptions{
		DecodedContentLength: 5,
		TrailerNames:         []string{"X-Amz-Checksum-CRC32"},
	})
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("解帧失败: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("内容不符: %q", got)
	}
	v, ok := cr.Trailer("x-amz-checksum-crc32")
	if !ok || v != "AAAA" {
		t.Fatalf("trailer 取值失败: %q %v", v, ok)
	}
}

func TestChunkedReaderSignedKnownVector(t *testing.T) {
	body := "c;chunk-signature=" + chunkVecChunk1 + "\r\n" +
		"hello world\n" + "\r\n" +
		"c;chunk-signature=" + chunkVecChunk2 + "\r\n" +
		"second block" + "\r\n" +
		"0;chunk-signature=" + chunkVecFinal + "\r\n\r\n"
	cr := NewChunkedReader(strings.NewReader(body), ChunkedOptions{
		DecodedContentLength: 24,
		Signer:               newTestSigner(),
	})
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("解帧失败: %v", err)
	}
	if string(got) != "hello world\nsecond block" {
		t.Fatalf("内容不符: %q", got)
	}
}

func TestChunkedReaderSignedTrailerKnownVector(t *testing.T) {
	body := "c;chunk-signature=" + chunkVecChunk1 + "\r\n" +
		"hello world\n" + "\r\n" +
		"c;chunk-signature=" + chunkVecChunk2 + "\r\n" +
		"second block" + "\r\n" +
		"0;chunk-signature=" + chunkVecFinal + "\r\n" +
		strings.TrimSuffix(chunkVecTrailerC, "\n") + "\r\n" +
		"x-amz-trailer-signature:" + chunkVecTrailer + "\r\n\r\n"
	cr := NewChunkedReader(strings.NewReader(body), ChunkedOptions{
		DecodedContentLength: 24,
		Signer:               newTestSigner(),
		TrailerNames:         []string{"x-amz-checksum-crc32"},
	})
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("解帧失败: %v", err)
	}
	if string(got) != "hello world\nsecond block" {
		t.Fatalf("内容不符: %q", got)
	}
	if v, ok := cr.Trailer("x-amz-checksum-crc32"); !ok || v != "AAAA" {
		t.Fatalf("trailer 取值失败: %q %v", v, ok)
	}
}

func TestChunkedReaderErrors(t *testing.T) {
	tests := []struct {
		name string
		body string
		opt  ChunkedOptions
		code s3err.Code
	}{
		{
			name: "数据块签名不符",
			body: "c;chunk-signature=" + strings.Repeat("0", 64) + "\r\nhello world\n\r\n0;chunk-signature=" + chunkVecFinal + "\r\n\r\n",
			opt:  ChunkedOptions{DecodedContentLength: 12, Signer: newTestSigner()},
			code: s3err.CodeSignatureDoesNotMatch,
		},
		{
			name: "trailer 签名不符",
			body: "c;chunk-signature=" + chunkVecChunk1 + "\r\nhello world\n\r\n" +
				"0;chunk-signature=" + chunkVecFinal + "\r\n" + strings.TrimSuffix(chunkVecTrailerC, "\n") + "\r\n" +
				"x-amz-trailer-signature:" + strings.Repeat("0", 64) + "\r\n\r\n",
			opt:  ChunkedOptions{DecodedContentLength: 12, Signer: newTestSigner(), TrailerNames: []string{"x-amz-checksum-crc32"}},
			code: s3err.CodeSignatureDoesNotMatch,
		},
		{
			name: "签名模式缺少 chunk-signature",
			body: "c\r\nhello world\n0;chunk-signature=" + chunkVecFinal + "\r\n\r\n",
			opt:  ChunkedOptions{DecodedContentLength: 12, Signer: newTestSigner()},
			code: s3err.CodeInvalidArgument,
		},
		{
			name: "未签名模式出现 chunk-signature",
			body: "c;chunk-signature=" + chunkVecChunk1 + "\r\nhello world\n0\r\n\r\n",
			opt:  ChunkedOptions{DecodedContentLength: 12},
			code: s3err.CodeInvalidArgument,
		},
		{
			name: "请求体截断",
			body: "5\r\nhel",
			opt:  ChunkedOptions{DecodedContentLength: 5},
			code: s3err.CodeInvalidArgument,
		},
		{
			name: "块头非十六进制",
			body: "zz\r\nhello\r\n0\r\n\r\n",
			opt:  ChunkedOptions{DecodedContentLength: 5},
			code: s3err.CodeInvalidArgument,
		},
		{
			name: "块长超过声明长度",
			body: "6\r\nabcdef\r\n0\r\n\r\n",
			opt:  ChunkedOptions{DecodedContentLength: 5},
			code: s3err.CodeInvalidArgument,
		},
		{
			name: "解帧后长度不足声明",
			body: unsignedBody([]string{"hello"}),
			opt:  ChunkedOptions{DecodedContentLength: 6},
			code: s3err.CodeInvalidArgument,
		},
		{
			name: "trailer 未声明",
			body: unsignedBody([]string{"hello"}, "x-amz-checksum-crc32:AAAA"),
			opt:  ChunkedOptions{DecodedContentLength: 5},
			code: s3err.CodeInvalidArgument,
		},
		{
			name: "缺少已声明的 trailer",
			body: unsignedBody([]string{"hello"}),
			opt:  ChunkedOptions{DecodedContentLength: 5, TrailerNames: []string{"x-amz-checksum-crc32"}},
			code: s3err.CodeInvalidArgument,
		},
		{
			name: "结束块后有多余数据",
			body: unsignedBody([]string{"hello"}) + "junk",
			opt:  ChunkedOptions{DecodedContentLength: 5},
			code: s3err.CodeInvalidArgument,
		},
		{
			name: "数据块后缺 CRLF",
			body: "5\r\nhelloXX0\r\n\r\n",
			opt:  ChunkedOptions{DecodedContentLength: 5},
			code: s3err.CodeInvalidArgument,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := readAll(t, tt.body, tt.opt)
			wantCode(t, err, tt.code)
		})
	}
}

// TestChunkedReaderTrailerBeforeEOF 确认只有读到 EOF 之后才能取 trailer。
func TestChunkedReaderTrailerBeforeEOF(t *testing.T) {
	body := unsignedBody([]string{"hello"}, "x-amz-checksum-crc32:AAAA")
	cr := NewChunkedReader(strings.NewReader(body), ChunkedOptions{
		DecodedContentLength: 5,
		TrailerNames:         []string{"x-amz-checksum-crc32"},
	})
	if _, ok := cr.Trailer("x-amz-checksum-crc32"); ok {
		t.Fatal("EOF 之前不应能取到 trailer")
	}
	if _, err := io.ReadAll(cr); err != nil {
		t.Fatalf("解帧失败: %v", err)
	}
	if v, ok := cr.Trailer("x-amz-checksum-crc32"); !ok || v != "AAAA" {
		t.Fatalf("EOF 之后 trailer 取值失败: %q %v", v, ok)
	}
}

// 保证 sha256 常量与标准库一致（空块签名依赖它）。
func TestEmptySHA256(t *testing.T) {
	sum := sha256.Sum256(nil)
	if hexEncode(sum[:]) != emptySHA256Hex {
		t.Fatalf("emptySHA256Hex 常量错误: %s", hexEncode(sum[:]))
	}
}
