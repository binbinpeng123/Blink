package s3api

import (
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/binbinpeng123/Blink/internal/s3err"
	"github.com/binbinpeng123/Blink/internal/sigv4"
)

// decodedContentLengthHeader 是 aws-chunked 请求声明的"解帧后"字节数。
//
// 分块框架自身没有总长度，缺了它就无法在落盘前知道对象大小，
// 也无法判断请求体是否被截断。S3 对它缺失的响应是 411。
const decodedContentLengthHeader = "x-amz-decoded-content-length"

// decodeChunkedBody 在鉴权之后、读取请求体之前解开 aws-chunked。
//
// 非 aws-chunked 请求原样返回 (nil, nil)。成功时会把 r.Body 换成
// 解帧 reader，并把 r.ContentLength 修正为解码后的长度 —— 这样后续
// handler 与存储层看到的语义与普通 PUT 完全一致。
func decodeChunkedBody(r *http.Request) (*sigv4.ChunkedReader, *s3err.Error) {
	if !isAWSChunked(r) {
		return nil, nil
	}

	// 未签名流式载荷不依赖鉴权结果，只有签名分块需要 ChunkSigner。
	// 因此这里仅在签名的分支上要求上下文存在 —— 鉴权被显式关闭时
	// （本地调试），unsigned 流式上传仍应可用。
	var signer *sigv4.ChunkSigner
	if auth := authFrom(r); auth != nil {
		signer = auth.ChunkSigner
	}

	hash := strings.TrimSpace(r.Header.Get("x-amz-content-sha256"))
	if !sigv4.IsStreamingPayload(hash) {
		return nil, s3err.New(s3err.CodeInvalidArgument).
			WithMessage("Content-Encoding: aws-chunked 与 x-amz-content-sha256=" + hash + " 不一致。")
	}

	decoded, serr := parseDecodedContentLength(r.Header.Get(decodedContentLengthHeader))
	if serr != nil {
		return nil, serr
	}

	names, serr := parseTrailerNames(r.Header)
	if serr != nil {
		return nil, serr
	}
	hasTrailer := sigv4.StreamingPayloadHasTrailer(hash)
	if hasTrailer && len(names) == 0 {
		return nil, s3err.New(s3err.CodeInvalidArgument).
			WithMessage("流式载荷声明带 trailer，但缺少 x-amz-trailer 头。")
	}
	if !hasTrailer && len(names) > 0 {
		return nil, s3err.New(s3err.CodeInvalidArgument).
			WithMessage("x-amz-trailer 只能用于带 trailer 的流式载荷。")
	}

	if sigv4.IsSignedStreamingPayload(hash) {
		if signer == nil {
			// 签名分块必须逐块验签；没有签名链说明请求没经过鉴权中间件
			// 或上下文丢失 —— 是服务端装配错误，绝不能"跳过验签"继续。
			return nil, s3err.New(s3err.CodeInternalError).
				WithMessage("签名分块请求缺少签名链。")
		}
	} else {
		signer = nil
	}

	cr := sigv4.NewChunkedReader(r.Body, sigv4.ChunkedOptions{
		DecodedContentLength: decoded,
		Signer:               signer,
		TrailerNames:         names,
	})
	r.Body = cr
	r.ContentLength = decoded

	// 只记录框架元数据（模式、解码后长度、trailer 名），不记录内容或值。
	// 排障时"这条请求到底有没有走解帧路径"是最先要回答的问题。
	loggerFrom(r.Context(), nil).Debug("aws_chunked_decoded",
		slog.Bool("signed", cr.Signed()),
		slog.Int64("decoded_length", decoded),
		slog.Any("trailer_names", names),
	)
	// 解帧后对象内容本身不再带 aws-chunked 编码；其它编码（如 gzip）保留。
	if enc := stripAWSChunked(r.Header.Values("Content-Encoding")); enc != "" {
		r.Header.Set("Content-Encoding", enc)
	} else {
		r.Header.Del("Content-Encoding")
	}
	return cr, nil
}

// parseDecodedContentLength 解析 x-amz-decoded-content-length。
func parseDecodedContentLength(v string) (int64, *s3err.Error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, s3err.New(s3err.CodeLengthRequired).
			WithMessage("aws-chunked 请求必须带 " + decodedContentLengthHeader + "。")
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, s3err.New(s3err.CodeInvalidArgument).
			WithMessage(decodedContentLengthHeader + " 必须是非负十进制整数。")
	}
	return n, nil
}

// parseTrailerNames 解析 x-amz-trailer 声明的 trailer 名（小写、去重）。
func parseTrailerNames(h http.Header) ([]string, *s3err.Error) {
	var names []string
	seen := make(map[string]bool)
	for _, v := range h.Values("x-amz-trailer") {
		for _, part := range strings.Split(v, ",") {
			name := strings.ToLower(strings.TrimSpace(part))
			if name == "" {
				continue
			}
			if !validTrailerName(name) {
				return nil, s3err.New(s3err.CodeInvalidArgument).
					WithMessage("x-amz-trailer 中存在非法的 trailer 名：" + name)
			}
			if seen[name] {
				continue
			}
			seen[name] = true
			names = append(names, name)
		}
	}
	return names, nil
}

// hasTrailerName 报告 x-amz-trailer 是否声明了指定名字。
func hasTrailerName(h http.Header, name string) bool {
	names, _ := parseTrailerNames(h)
	for _, n := range names {
		if n == strings.ToLower(name) {
			return true
		}
	}
	return false
}

// validTrailerName 限制为 S3 trailer 实际使用的字符集。
func validTrailerName(s string) bool {
	if len(s) > 128 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-':
		default:
			return false
		}
	}
	return true
}

// stripAWSChunked 从 Content-Encoding 中移除 aws-chunked，保留其它编码。
//
// aws-cli 在已有编码后追加 `,aws-chunked`；解帧之后只剩原有编码，
// 若把它原样留给存储层，元数据里就会永久记录一个已经被消除的编码。
func stripAWSChunked(values []string) string {
	var kept []string
	for _, v := range values {
		for _, part := range strings.Split(v, ",") {
			p := strings.TrimSpace(part)
			if p == "" || strings.EqualFold(p, "aws-chunked") {
				continue
			}
			kept = append(kept, p)
		}
	}
	return strings.Join(kept, ", ")
}
