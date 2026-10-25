package s3api

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strings"

	"github.com/binbinpeng123/Blink/internal/checksum"
	"github.com/binbinpeng123/Blink/internal/s3err"
	"github.com/binbinpeng123/Blink/internal/store"
)

// handlePutObject 处理 PUT /{bucket}/{key}。
//
// 所有"能用头部判断出失败"的检查都放在读取请求体之前：
// 一是让 `Expect: 100-continue` 的客户端不必上传完整个大对象才知道被拒，
// 二是这些错误绝不依赖已写入的数据。
func (s *Server) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) {
	// aws-chunked 是 SigV4 的流式签名传输：请求体是"长度头 + 数据 + 尾"的
	// 分块框架，而不是对象的原始字节。不解帧就落盘等于把框架一起存进去 ——
	// 存的内容是错的，但校验和、ETag 全都"看起来正常"，是最难发现的一类损坏。
	// 解帧后 r.ContentLength 会被修正为 x-amz-decoded-content-length。
	chunked, serr := decodeChunkedBody(r)
	if serr != nil {
		serr.Write(w, r, reqID)
		return
	}

	// S3 要求单次 PUT 必须声明长度（02 文档：缺失回 411）。
	// Go 在 chunked 传输或 HTTP/2 无长度时把 ContentLength 置为 -1。
	if r.ContentLength < 0 {
		s3err.New(s3err.CodeLengthRequired).Write(w, r, reqID)
		return
	}

	// 存储类型不在本版本支持范围内：静默忽略会让用户以为数据落到了
	// "低频/归档"层，而实际上什么也没发生。
	if sc := r.Header.Get("x-amz-storage-class"); sc != "" && !strings.EqualFold(sc, "STANDARD") {
		s3err.New(s3err.CodeNotImplemented).
			WithMessage("本版本只支持 STANDARD 存储类型。").
			Write(w, r, reqID)
		return
	}

	contentMD5, ok := parseContentMD5(r.Header.Get("Content-MD5"))
	if !ok {
		s3err.New(s3err.CodeInvalidDigest).Write(w, r, reqID)
		return
	}

	// 条件写：只支持 If-None-Match: *（02 文档）。
	// 其他形式不静默忽略 —— 客户端明确要求了"仅当 ETag 不等于 X 才写"，
	// 忽略它就变成了无条件覆盖，是数据丢失而不是"少个特性"。
	condErr := checkPutPreconditions(r)
	if condErr != nil {
		condErr.Write(w, r, reqID)
		return
	}

	alg, declared, algErr := parseChecksumHeaders(r.Header)
	if algErr != nil {
		algErr.Write(w, r, reqID)
		return
	}
	if alg != "" && declared == "" && chunked != nil && !hasTrailerName(r.Header, alg.Header()) {
		// 声明了算法却既没有值、也没有在 x-amz-trailer 里声明对应 trailer：
		// 若放行，存储层会"算一个校验和但没人可对"，客户端却以为已被校验。
		s3err.New(s3err.CodeInvalidArgument).
			WithMessage("声明了 "+string(alg)+" 校验和，但 x-amz-trailer 未包含 "+alg.Header()+"。").
			Write(w, r, reqID)
		return
	}
	var lateChecksum func() (string, bool)
	if alg != "" && declared == "" {
		// 声明了算法却没给值：值在 trailer 里，只有请求体读到 EOF 之后才存在。
		// aws-chunked 的 trailer 由解帧器收集（Go 的 r.Trailer 只覆盖
		// HTTP 层的 chunked 尾，看不到 aws-chunked 框架内部的尾）。
		lateChecksum = func() (string, bool) {
			if chunked != nil {
				return chunked.Trailer(alg.Header())
			}
			v := r.Trailer.Get(alg.Header())
			return v, v != ""
		}
	}

	info, err := s.store.PutObject(bucket, key, r.Body, store.PutOptions{
		ContentType:       r.Header.Get("Content-Type"),
		UserMeta:          userMetadata(r.Header),
		ChecksumAlgorithm: alg,
		Checksum:          declared,
		LateChecksum:      lateChecksum,
		ContentMD5:        contentMD5,
		IfNoneMatchAny:    strings.TrimSpace(r.Header.Get("If-None-Match")) == "*",
		ExpectedSize:      r.ContentLength,
	})
	if err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}

	h := w.Header()
	// ETag 必须带引号：S3 的 ETag 是一个 quoted-string，部分 SDK 会
	// 原样保存并在下一次请求里带回 If-Match。不带引号会让条件请求失配。
	h.Set("ETag", `"`+info.ETag+`"`)
	h.Set("Last-Modified", info.Modified.UTC().Format(http.TimeFormat))
	if info.ChecksumAlgorithm != "" {
		// 回传的是**服务端实际算出**的值，客户端据此判断传输是否完整。
		h.Set(info.ChecksumAlgorithm.Header(), info.Checksum)
		// 类型必须一起回传：aws-cli 只有在看到 checksum-type 时才敢
		// 按对应规则复算（FULL_OBJECT 直接比对 / COMPOSITE 需带分片数）。
		// 单次 PUT 恒为 FULL_OBJECT，但仍然显式声明 —— 让客户端不必猜。
		if info.ChecksumType != "" {
			h.Set("x-amz-checksum-type", info.ChecksumType)
		}
	}
	w.WriteHeader(http.StatusOK)

	loggerFrom(r.Context(), s.logger).Debug("object_put",
		slog.String("bucket", bucket),
		slog.String("key", key),
		slog.Int64("size", info.Size),
		slog.Int("blocks", info.BlockCount),
		slog.Int("deduped_blocks", info.DedupedBlocks),
	)
}

// checkPutPreconditions 解析写请求上的前置条件。
//
// 只实现 If-None-Match: *。其余形式（带具体 ETag 的 If-None-Match、If-Match）
// 返回 501 而不是忽略：忽略等价于把用户的"仅当…才写"降级成无条件覆盖。
func checkPutPreconditions(r *http.Request) *s3err.Error {
	if v := strings.TrimSpace(r.Header.Get("If-None-Match")); v != "" && v != "*" {
		return s3err.New(s3err.CodeNotImplemented).
			WithMessage("仅支持 `If-None-Match: *`。")
	}
	if v := strings.TrimSpace(r.Header.Get("If-Match")); v != "" {
		return s3err.New(s3err.CodeNotImplemented).
			WithMessage("写请求暂不支持 If-Match。")
	}
	return nil
}

// parseContentMD5 把头部里的 base64 Content-MD5 转成小写十六进制。
//
// 返回 ("", true) 表示客户端没有声明；格式非法时返回 ok=false，
// 由调用方回 400 InvalidDigest（02 文档），而不是 BadDigest ——
// 前者是"你发的值不合法"，后者是"你发的值合法但内容对不上"。
func parseContentMD5(v string) (string, bool) {
	if v == "" {
		return "", true
	}
	raw, err := base64.StdEncoding.DecodeString(v)
	if err != nil || len(raw) != md5.Size {
		return "", false
	}
	return hex.EncodeToString(raw), true
}

// parseChecksumHeaders 解析客户端声明的对象级校验和。
//
// 三种来源：`x-amz-sdk-checksum-algorithm`（SDK 注入）或
// `x-amz-checksum-algorithm`（S3 原生头部，CreateMultipartUpload 用）
// 指明算法 + 对应的 `x-amz-checksum-<alg>` 给出 base64 值；
// 或只给后者，算法由头部名推断（部分 SDK 就是这样发的）。
//
// 返回值 declared 为空表示客户端只要"算一个给我看"，不代表出错 ——
// 这种用法在 ChecksumMode 场景下完全合法。
func parseChecksumHeaders(h http.Header) (alg checksum.Algorithm, declared string, e *s3err.Error) {
	name := h.Get("x-amz-sdk-checksum-algorithm")
	if name == "" {
		// aws-cli 的 CreateMultipartUpload 发的是不带 sdk- 前缀的
		// `x-amz-checksum-algorithm`（实测 2.36.2）。漏掉这个回退，
		// 会话就会被建成"没有校验和"，而客户端之后每片都带 ChecksumAlgorithm，
		// 整个 multipart 上传在 UploadPart 阶段全部 400。
		name = h.Get("x-amz-checksum-algorithm")
	}
	if name != "" {
		parsed, ok := checksum.ParseAlgorithm(name)
		if !ok {
			return "", "", s3err.New(s3err.CodeInvalidArgument).
				WithMessage("不支持的校验和算法：" + name)
		}
		// 指定了算法就只认这一个：其他 x-amz-checksum-* 头一律忽略，
		// 因为响应只能回传一个值，多算法并存会让"回传哪个"变成隐式规则。
		return parsed, h.Get(parsed.Header()), nil
	}

	found := ""
	for _, a := range checksum.AllAlgorithms {
		v := h.Get(a.Header())
		if v == "" {
			continue
		}
		if found != "" {
			return "", "", s3err.New(s3err.CodeInvalidArgument).
				WithMessage("一次请求只能声明一个校验和算法。")
		}
		found, alg, declared = a.Header(), a, v
	}
	if found != "" {
		return alg, declared, nil
	}

	// 最后一种来源：`x-amz-trailer`。aws-chunked 的校验和值在请求体末尾，
	// 请求头里只有 trailer 名。只接受能唯一映射到已知算法的 trailer ——
	// 出现多个校验和 trailer 时无法决定回传哪一个，按 400 拒绝。
	names, _ := parseTrailerNames(h)
	for _, a := range checksum.AllAlgorithms {
		if !hasName(names, a.Header()) {
			continue
		}
		if found != "" {
			return "", "", s3err.New(s3err.CodeInvalidArgument).
				WithMessage("x-amz-trailer 一次只能声明一个校验和算法。")
		}
		found, alg = a.Header(), a
	}
	return alg, "", nil
}

// hasName 报告名字列表（已小写）是否包含指定名字。
func hasName(names []string, name string) bool {
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
}

// userMetadata 提取 x-amz-meta-* 用户元数据，键去掉前缀。
//
// 注意：Go 的 http.Header 会把键规范化成 X-Amz-Meta-Foo，**原始大小写
// 已经无法恢复**，因此返回的键一律是规范化形式的后半段。S3 本身不区分
// 元数据键的大小写，这个损失是可接受的。
func userMetadata(h http.Header) map[string]string {
	const prefix = "X-Amz-Meta-"
	var out map[string]string
	for k, vals := range h {
		if !strings.HasPrefix(k, prefix) || len(vals) == 0 {
			continue
		}
		if out == nil {
			out = make(map[string]string, 4)
		}
		out[k[len(prefix):]] = vals[0]
	}
	return out
}

// isAWSChunked 判断请求体是否为 SigV4 流式签名的分块框架。
//
// 三个特征取其一即可：显式的 aws-chunked 编码、STREAMING- 前缀的
// 载荷哈希声明、以及 aws-chunked 专用的 decoded-content-length 头。
// 多重判断是因为不同 SDK/传输方式组合下出现的特征并不一致。
func isAWSChunked(r *http.Request) bool {
	if r.Header.Get("x-amz-decoded-content-length") != "" {
		return true
	}
	for _, v := range r.Header.Values("Content-Encoding") {
		if strings.Contains(strings.ToLower(v), "aws-chunked") {
			return true
		}
	}
	sha := r.Header.Get("x-amz-content-sha256")
	return strings.HasPrefix(sha, "STREAMING-")
}
