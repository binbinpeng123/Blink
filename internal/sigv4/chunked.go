package sigv4

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/binbinpeng123/Blink/internal/s3err"
)

// aws-chunked 的载荷哈希取值（X-Amz-Content-SHA256）。
//
// 它们表示请求体不是对象原始字节，而是"块头 + 数据 + 尾"的框架：
// 不解帧就落盘，存进去的是框架而不是内容，且 ETag、校验和全部
// "看起来正常" —— 这是最难发现的一类数据损坏。
const (
	streamingUnsignedPayload        = "STREAMING-UNSIGNED-PAYLOAD"
	streamingUnsignedPayloadTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	streamingSignedPayload          = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	streamingSignedPayloadTrailer   = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"

	chunkStringToSignPrefix   = algorithm + "-PAYLOAD"
	trailerStringToSignPrefix = algorithm + "-TRAILER"
	// trailerSignatureHeader 是签名 trailer 模式的最后一行，自身不参与规范化。
	trailerSignatureHeader = "x-amz-trailer-signature"

	// emptySHA256Hex 是空字符串的 SHA256，签名块的 StringToSign 固定使用它。
	emptySHA256Hex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	// 框架行的上限。块头只有十六进制长度与一个签名，2 KiB 已极宽松；
	// trailer 总量限制用于挡住"用超长尾把内存吃光"的请求。
	maxChunkHeaderBytes = 2 << 10
	maxTrailerBytes     = 16 << 10
	// readLine 的默认缓冲；超长行会由 bufio 返回 ErrBufferFull，按格式错误处理。
	chunkReadBuffer = 4 << 10
)

// IsStreamingPayload 报告载荷哈希是否表示 aws-chunked 请求体。
func IsStreamingPayload(hash string) bool {
	switch hash {
	case streamingUnsignedPayload,
		streamingUnsignedPayloadTrailer,
		streamingSignedPayload,
		streamingSignedPayloadTrailer:
		return true
	}
	return false
}

// IsSignedStreamingPayload 报告流式载荷是否要求逐块验签。
func IsSignedStreamingPayload(hash string) bool {
	return hash == streamingSignedPayload || hash == streamingSignedPayloadTrailer
}

// StreamingPayloadHasTrailer 报告该流式载荷是否按协议带 trailer。
func StreamingPayloadHasTrailer(hash string) bool {
	return hash == streamingUnsignedPayloadTrailer || hash == streamingSignedPayloadTrailer
}

// ChunkSigner 是 aws-chunked 的签名链。
//
// 链的第一环是请求签名（seed signature）：头部验签通过后用它构造。
// 每个数据块的签名又把前一个签名作为下一块 StringToSign 的一行，
// 因此**一次请求一个实例**，绝不可跨请求复用。
type ChunkSigner struct {
	key     []byte
	amzDate string
	scope   string
	prev    string
}

// verifyChunk 校验一个数据块的签名，成功时把链推进到该签名。
//
// payloadHex 是该块明文的 SHA256 十六进制；空块（结束块）固定用
// emptySHA256Hex。比较是常数时间的，避免按字节泄露签名前缀。
func (c *ChunkSigner) verifyChunk(payloadHex, got string) bool {
	if c == nil {
		return false
	}
	toSign := strings.Join([]string{
		chunkStringToSignPrefix,
		c.amzDate,
		c.scope,
		c.prev,
		emptySHA256Hex,
		payloadHex,
	}, "\n")
	want := hexEncode(hmacSHA256(c.key, toSign))
	if !hmacEqual(want, got) {
		return false
	}
	c.prev = want
	return true
}

// verifyTrailer 校验签名 trailer。
//
// payloadHex 是规范化 trailer 文本的 SHA256 十六进制。trailer 不推进签名链
// （它已经是最后一段），因此不修改 prev。
func (c *ChunkSigner) verifyTrailer(payloadHex, got string) bool {
	if c == nil {
		return false
	}
	toSign := strings.Join([]string{
		trailerStringToSignPrefix,
		c.amzDate,
		c.scope,
		c.prev,
		payloadHex,
	}, "\n")
	want := hexEncode(hmacSHA256(c.key, toSign))
	return hmacEqual(want, got)
}

// ChunkedOptions 是解帧所需的参数。
type ChunkedOptions struct {
	// DecodedContentLength 是 x-amz-decoded-content-length；负值表示未声明。
	DecodedContentLength int64
	// Signer 非 nil 时按签名分块解析并逐块验签。
	Signer *ChunkSigner
	// TrailerNames 是 x-amz-trailer 声明的 trailer 名（可含大小写）。
	TrailerNames []string
}

// ChunkedReader 把 aws-chunked 请求体还原为对象原始字节流。
//
// 它只负责"框架与签名"，不解释 trailer 中校验和的含义 —— 调用方在
// 读到 EOF 后通过 Trailer 取值，再与落盘内容的校验和比对。
type ChunkedReader struct {
	br     *bufio.Reader
	src    io.Reader
	opt    ChunkedOptions
	signer *ChunkSigner

	remaining int64
	total     int64
	sum       hash.Hash
	chunkSig  string

	finished bool
	err      error
	trailers map[string]string
}

// NewChunkedReader 构造解帧 reader。src 的所有字节都会被解读为分块框架。
func NewChunkedReader(src io.Reader, opt ChunkedOptions) *ChunkedReader {
	names := make([]string, 0, len(opt.TrailerNames))
	for _, n := range opt.TrailerNames {
		names = append(names, strings.ToLower(strings.TrimSpace(n)))
	}
	opt.TrailerNames = names
	return &ChunkedReader{
		br:     bufio.NewReaderSize(src, chunkReadBuffer),
		src:    src,
		opt:    opt,
		signer: opt.Signer,
	}
}

// Close 实现 io.ReadCloser：底层是请求体时，把关闭动作透传给它。
//
// 解帧器自己在错误路径上不会读完请求体；close 让 net/http 能及时
// 释放连接，而不是等超时。
func (c *ChunkedReader) Close() error {
	if rc, ok := c.src.(io.Closer); ok {
		return rc.Close()
	}
	return nil
}

// Signed 报告当前 reader 是否在逐块验签。
func (c *ChunkedReader) Signed() bool { return c.signer != nil }

// Trailer 返回解帧后 trailer 的值。
//
// 只有在返回 io.EOF 之后调用才有完整结果；未声明或未出现的名字返回 false。
func (c *ChunkedReader) Trailer(name string) (string, bool) {
	v, ok := c.trailers[strings.ToLower(strings.TrimSpace(name))]
	return v, ok
}

// Read 实现 io.Reader。
func (c *ChunkedReader) Read(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if c.remaining > 0 {
			n, err := c.readData(p)
			if n > 0 || err != nil {
				return n, err
			}
			continue
		}
		if c.finished {
			return 0, io.EOF
		}
		if err := c.startChunk(); err != nil {
			c.err = err
			return 0, err
		}
	}
}

// readData 读取当前块的数据，并在块结束时立即验签、消费结尾 CRLF。
func (c *ChunkedReader) readData(p []byte) (int, error) {
	if int64(len(p)) > c.remaining {
		p = p[:c.remaining]
	}
	n, err := c.br.Read(p)
	if n > 0 {
		if c.sum != nil {
			c.sum.Write(p[:n])
		}
		c.remaining -= int64(n)
		c.total += int64(n)
		if c.remaining == 0 {
			if ferr := c.finishChunk(); ferr != nil {
				c.err = ferr
				return n, ferr
			}
		}
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			c.err = chunkProtocolError("请求体在数据块中途结束")
		} else {
			c.err = chunkProtocolError("读取数据块失败: " + err.Error())
		}
		return n, c.err
	}
	return n, nil
}

// finishChunk 校验已读完的数据块签名，并消费块尾的 CRLF。
func (c *ChunkedReader) finishChunk() error {
	if c.signer != nil {
		payloadHex := hex.EncodeToString(c.sum.Sum(nil))
		if !c.signer.verifyChunk(payloadHex, c.chunkSig) {
			return s3err.New(s3err.CodeSignatureDoesNotMatch).
				WithMessage("aws-chunked 数据块签名不匹配。")
		}
	}
	var crlf [2]byte
	if _, err := io.ReadFull(c.br, crlf[:]); err != nil {
		return chunkProtocolError("数据块后缺少 CRLF")
	}
	if crlf != [2]byte{'\r', '\n'} {
		return chunkProtocolError("数据块后不是 CRLF")
	}
	c.sum = nil
	c.chunkSig = ""
	return nil
}

// startChunk 解析块头；遇到结束块时继续解析 trailer 并核对总长度。
func (c *ChunkedReader) startChunk() error {
	line, err := c.readLine(maxChunkHeaderBytes)
	if err != nil {
		return err
	}
	size, sig, perr := parseChunkHeader(line)
	if perr != nil {
		return chunkProtocolError(perr.Error())
	}
	if c.signer != nil && sig == "" {
		return chunkProtocolError("签名模式下的块头缺少 chunk-signature")
	}
	if c.signer == nil && sig != "" {
		return chunkProtocolError("未签名模式下出现 chunk-signature")
	}

	if size > 0 && c.opt.DecodedContentLength >= 0 {
		// 先于读数据检查，避免"声明 1 字节却要求 1 GiB"被读到一半才发现。
		if size > c.opt.DecodedContentLength-c.total {
			return chunkProtocolError("块长度超过 x-amz-decoded-content-length 的声明")
		}
	}

	if size == 0 {
		if c.signer != nil && !c.signer.verifyChunk(emptySHA256Hex, sig) {
			return s3err.New(s3err.CodeSignatureDoesNotMatch).
				WithMessage("aws-chunked 结束块签名不匹配。")
		}
		if err := c.readTrailers(); err != nil {
			return err
		}
		if c.opt.DecodedContentLength >= 0 && c.total != c.opt.DecodedContentLength {
			return chunkProtocolError(fmt.Sprintf(
				"解帧后为 %d 字节，与 x-amz-decoded-content-length 声明的 %d 不符",
				c.total, c.opt.DecodedContentLength))
		}
		c.finished = true
		return nil
	}

	c.remaining = size
	c.chunkSig = sig
	if c.signer != nil {
		c.sum = sha256.New()
	}
	return nil
}

type trailerLine struct{ name, value string }

// readTrailers 读取结束块之后的 trailer 行，直到空行为止，并校验签名。
func (c *ChunkedReader) readTrailers() error {
	declared := make(map[string]bool, len(c.opt.TrailerNames))
	for _, n := range c.opt.TrailerNames {
		declared[n] = true
	}

	var (
		lines        []trailerLine
		signature    string
		hasSignature bool
	)
	seen := make(map[string]bool, len(declared))
	total := 0
	for {
		line, err := c.readLine(maxTrailerBytes)
		if err != nil {
			return err
		}
		if line == "" {
			break
		}
		total += len(line) + 2
		if total > maxTrailerBytes {
			return chunkProtocolError("trailer 总长度超过上限")
		}

		name, value, ok := parseTrailerLine(line)
		if !ok {
			return chunkProtocolError("trailer 行格式非法")
		}
		if seen[name] {
			return chunkProtocolError("trailer 重复出现：" + name)
		}
		seen[name] = true

		if name == trailerSignatureHeader {
			signature, hasSignature = value, true
			continue
		}
		if !declared[name] {
			return chunkProtocolError("trailer 未在 x-amz-trailer 中声明：" + name)
		}
		lines = append(lines, trailerLine{name: name, value: value})
	}

	// 只有"签名 + 声明了 trailer"才要求 trailer 签名。
	// STREAMING-AWS4-HMAC-SHA256-PAYLOAD（无 trailer）的结束块之后
	// 只有一个空行，没有可签的内容。
	switch {
	case c.signer != nil && len(declared) > 0:
		if !hasSignature {
			return s3err.New(s3err.CodeSignatureDoesNotMatch).
				WithMessage("签名分块模式下缺少 " + trailerSignatureHeader + "。")
		}
		canonical := canonicalTrailer(lines)
		payloadHex := sha256Hex([]byte(canonical))
		if !c.signer.verifyTrailer(payloadHex, signature) {
			return s3err.New(s3err.CodeSignatureDoesNotMatch).
				WithMessage("aws-chunked trailer 签名不匹配。")
		}
	case hasSignature:
		return chunkProtocolError("未签名模式下出现 " + trailerSignatureHeader)
	}

	// 声明了 trailer 就必须真的收到，否则"客户端说会校验"变成了空话。
	for name := range declared {
		if !seen[name] {
			return chunkProtocolError("缺少已声明的 trailer：" + name)
		}
	}
	if len(lines) > 0 {
		c.trailers = make(map[string]string, len(lines))
		for _, l := range lines {
			c.trailers[l.name] = l.value
		}
	}

	// 结束块之后必须恰好是请求体结尾；多余字节说明客户端与我们在
	// 框架边界上理解不一致，继续处理只会把错位的内容当成合法数据。
	if _, err := c.br.ReadByte(); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return chunkProtocolError("读取请求体结尾失败: " + err.Error())
	}
	return chunkProtocolError("结束块之后仍有多余数据")
}

// canonicalTrailer 按名称排序生成 `name:value\n` 的规范化文本。
//
// 签名 trailer 自身不在 lines 里；名字已是小写。
func canonicalTrailer(lines []trailerLine) string {
	sorted := append([]trailerLine(nil), lines...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].name < sorted[j].name })
	var b strings.Builder
	for _, l := range sorted {
		b.WriteString(l.name)
		b.WriteByte(':')
		b.WriteString(l.value)
		b.WriteByte('\n')
	}
	return b.String()
}

// readLine 读取一行框架文本并去掉 CRLF。
func (c *ChunkedReader) readLine(limit int) (string, error) {
	line, err := c.br.ReadString('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return "", chunkProtocolError("请求体在块框架中途结束")
		}
		return "", chunkProtocolError("读取块框架失败: " + err.Error())
	}
	if len(line) > limit {
		return "", chunkProtocolError("块框架行过长")
	}
	if !strings.HasSuffix(line, "\r\n") {
		return "", chunkProtocolError("块框架行未以 CRLF 结尾")
	}
	return line[:len(line)-2], nil
}

// parseChunkHeader 解析 `<hex>[;chunk-signature=<hex>]`。
func parseChunkHeader(line string) (size int64, signature string, err error) {
	sizeHex, rest, hasExt := strings.Cut(line, ";")
	sizeHex = strings.TrimSpace(sizeHex)
	if sizeHex == "" || len(sizeHex) > 16 || !isHexString(sizeHex) {
		return 0, "", fmt.Errorf("块头长度不是合法的十六进制整数")
	}
	v, perr := strconv.ParseUint(sizeHex, 16, 63)
	if perr != nil {
		return 0, "", fmt.Errorf("块头长度超出范围")
	}
	if hasExt {
		for _, ext := range strings.Split(rest, ";") {
			k, val, ok := strings.Cut(ext, "=")
			if !ok {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(k), "chunk-signature") {
				signature = strings.TrimSpace(val)
			}
		}
	}
	if signature != "" && (len(signature) != 64 || !isHexString(signature)) {
		return 0, "", fmt.Errorf("chunk-signature 不是 64 位十六进制")
	}
	return int64(v), signature, nil
}

// parseTrailerLine 解析 `name:value`。
func parseTrailerLine(line string) (name, value string, ok bool) {
	name, value, ok = strings.Cut(line, ":")
	if !ok {
		return "", "", false
	}
	name = strings.ToLower(strings.TrimSpace(name))
	value = strings.TrimSpace(value)
	if name == "" || !validHeaderName(name) {
		return "", "", false
	}
	return name, value, true
}

// validHeaderName 只接受 S3 trailer 实际会用的字符集，避免把奇怪的
// 名字带进日志或后续比对。
func validHeaderName(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '-':
		default:
			return false
		}
	}
	return true
}

func isHexString(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f', c >= 'A' && c <= 'F':
		default:
			return false
		}
	}
	return true
}

// chunkProtocolError 把框架错误映射为 400 InvalidArgument。
//
// 这类错误与"对象内容不合法"无关，是请求传输格式错误；返回 400 且
// 带明确消息，才能让客户端知道该换一条传输路径而不是重试。
func chunkProtocolError(msg string) *s3err.Error {
	return s3err.New(s3err.CodeInvalidArgument).
		WithMessage("aws-chunked 请求体格式非法：" + msg)
}
