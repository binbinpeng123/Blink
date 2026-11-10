package s3api

import (
	"encoding/xml"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/binbinpeng123/Blink/internal/s3err"
	"github.com/binbinpeng123/Blink/internal/store"
)

// copyObjectResult 是 CopyObject 的成功响应。
//
// 只有两个字段：复制不产生新的数据内容，源对象的 ETag 原样保留，
// 唯一变化的是修改时间与（REPLACE 时的）元数据。
type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

// copySource 是解析后的 x-amz-copy-source。
type copySource struct {
	Bucket string
	Key    string
}

// parseCopySource 解析 `x-amz-copy-source` 头。
//
// 头的形状是 `/bucket/key`（也可以不带前导 `/`），key 按 URL 路径规则
// 编码。解析必须**在解码之前**按第一个 `/` 切成 bucket 与 key：
// key 里合法的 `/` 可能被编码成 %2F，先解码再切会把 `a%2Fb` 误当成
// bucket=a、key=b 的两级路径。
//
// `?versionId=...` 明确回 501：本版本没有版本控制，静默忽略会让客户端
// 以为自己复制的是某个历史版本，而实际拿到的是当前版本 —— 这是"看起来
// 成功但内容不对"的一类错误。
func parseCopySource(v string) (copySource, *s3err.Error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return copySource{}, invalidArg("缺少 x-amz-copy-source。")
	}
	path, query, _ := strings.Cut(v, "?")
	if query != "" {
		q, err := url.ParseQuery(query)
		if err != nil {
			return copySource{}, invalidArg("x-amz-copy-source 的查询串不合法。")
		}
		if q.Has("versionId") {
			return copySource{}, s3err.New(s3err.CodeNotImplemented).
				WithMessage("本版本不支持版本控制，x-amz-copy-source 不能带 versionId。")
		}
	}

	path = strings.TrimPrefix(path, "/")
	bucket, key, ok := strings.Cut(path, "/")
	if !ok || bucket == "" || key == "" {
		return copySource{}, invalidArg("x-amz-copy-source 必须形如 /bucket/key。")
	}
	decodedBucket, err := url.PathUnescape(bucket)
	if err != nil {
		return copySource{}, invalidArg("x-amz-copy-source 的 bucket 编码不合法。")
	}
	decodedKey, err := url.PathUnescape(key)
	if err != nil {
		return copySource{}, invalidArg("x-amz-copy-source 的 key 编码不合法。")
	}
	if decodedBucket == "" || decodedKey == "" {
		return copySource{}, invalidArg("x-amz-copy-source 必须形如 /bucket/key。")
	}
	return copySource{Bucket: decodedBucket, Key: decodedKey}, nil
}

// hasCopySourceConditions 报告请求是否带了任一复制源条件。
func hasCopySourceConditions(r *http.Request) bool {
	for _, h := range []string{
		"x-amz-copy-source-if-match",
		"x-amz-copy-source-if-none-match",
		"x-amz-copy-source-if-modified-since",
		"x-amz-copy-source-if-unmodified-since",
	} {
		if strings.TrimSpace(r.Header.Get(h)) != "" {
			return true
		}
	}
	return false
}

// checkCopySourceConditions 按 RFC 9110 §13.2.2 的优先级求值源对象条件。
//
// 与读请求的区别：CopyObject 对"条件不满足"只有一种答复 —— 412。
// 这里没有 304：复制不是检索，"源没有变化"对客户端意味着"别复制"，
// 而不是"用你的缓存"。
func checkCopySourceConditions(r *http.Request, info store.ObjectInfo) *s3err.Error {
	ifMatch := strings.TrimSpace(r.Header.Get("x-amz-copy-source-if-match"))
	ifNoneMatch := strings.TrimSpace(r.Header.Get("x-amz-copy-source-if-none-match"))

	if ifMatch != "" {
		if !etagListMatches(ifMatch, info.ETag) {
			return s3err.New(s3err.CodePreconditionFailed)
		}
	} else if t, ok := parseHTTPTime(r.Header.Get("x-amz-copy-source-if-unmodified-since")); ok {
		if info.Modified.After(t) {
			return s3err.New(s3err.CodePreconditionFailed)
		}
	}

	if ifNoneMatch != "" {
		if etagListMatches(ifNoneMatch, info.ETag) {
			return s3err.New(s3err.CodePreconditionFailed)
		}
	} else if t, ok := parseHTTPTime(r.Header.Get("x-amz-copy-source-if-modified-since")); ok {
		if !info.Modified.After(t) {
			return s3err.New(s3err.CodePreconditionFailed)
		}
	}
	return nil
}

// handleCopyObject 处理 PUT /{bucket}/{key} + x-amz-copy-source。
//
// 分辨"普通上传"与"服务端复制"的唯一依据就是 x-amz-copy-source：
// 两者的方法与路径完全相同。router 在进入本函数前已经确认该头非空。
func (s *Server) handleCopyObject(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) {
	src, serr := parseCopySource(r.Header.Get("x-amz-copy-source"))
	if serr != nil {
		serr.Write(w, r, reqID)
		return
	}

	// 分片复制需要独立的内部协议（按源区间读、按分片写），本切片不做。
	// 静默忽略会让客户端以为复制了一部分，实际复制了全部 —— 结果更大，
	// 但"以为"和"实际"不一致本身就是错误。
	if r.Header.Get("x-amz-copy-source-range") != "" {
		s3err.New(s3err.CodeNotImplemented).
			WithMessage("暂不支持 x-amz-copy-source-range（分片复制）。").
			Write(w, r, reqID)
		return
	}

	// 源的 read 必须在任何存储访问之前检查：CopyObject 的源可以是
	// 另一个桶，只校验目标上的 write 会让"能写自己的桶"升级成
	// "能读整个集群"。
	if !s.authorizeSource(w, r, src, reqID) {
		return
	}

	if sc := r.Header.Get("x-amz-storage-class"); sc != "" && !strings.EqualFold(sc, storageClassStandard) {
		s3err.New(s3err.CodeNotImplemented).
			WithMessage("本版本只支持 STANDARD 存储类型。").
			Write(w, r, reqID)
		return
	}

	// 加密与标签会改变对象的元数据形状，而本版本两者都没有实现。
	// 拒绝而不是忽略：客户端带上 x-amz-server-side-encryption 就是在
	// 要求"落到盘上必须是密文"，照常写入明文是不可接受的安全降级。
	for _, h := range []string{
		"x-amz-server-side-encryption",
		"x-amz-server-side-encryption-customer-algorithm",
		"x-amz-server-side-encryption-aws-kms-key-id",
		"x-amz-tagging",
	} {
		if r.Header.Get(h) != "" {
			s3err.New(s3err.CodeNotImplemented).
				WithMessage("复制请求暂不支持 "+h+"。").
				Write(w, r, reqID)
			return
		}
	}

	var replace bool
	switch d := strings.ToUpper(strings.TrimSpace(r.Header.Get("x-amz-metadata-directive"))); d {
	case "", "COPY":
		replace = false
	case "REPLACE":
		replace = true
	default:
		invalidArg(`x-amz-metadata-directive 只支持 "COPY" 或 "REPLACE"。`).
			Write(w, r, reqID)
		return
	}

	// 目标对象上的条件写：与普通 PUT 同一套规则（If-None-Match: * 与
	// If-Match），复用同一个解析函数，避免两条写路径对同一个头给出不同答复。
	cond, condErr := parseWriteConditions(r)
	if condErr != nil {
		condErr.Write(w, r, reqID)
		return
	}

	// 源条件需要源对象的 ETag 与修改时间。只在客户端确实带了条件时才
	// 打开源对象：无条件复制由存储层一次性完成，省掉一次元数据读取。
	//
	// 这里的求值与随后的复制之间有一个 TOCTOU 窗口：源对象若在这期间被
	// 覆盖，复制到的是新版本。这与 S3 的语义一致 —— 条件针对"复制请求
	// 观察到的源版本"求值，而不是对复制本身加锁。
	if hasCopySourceConditions(r) {
		obj, err := s.store.OpenObject(src.Bucket, src.Key)
		if err != nil {
			s.writeStoreErr(w, r, reqID, err)
			return
		}
		if e := checkCopySourceConditions(r, obj.Info()); e != nil {
			e.Write(w, r, reqID)
			return
		}
	}

	info, err := s.store.CopyObject(bucket, key, src.Bucket, src.Key, store.CopyOptions{
		ReplaceMetadata: replace,
		ContentType:     r.Header.Get("Content-Type"),
		UserMeta:        userMetadata(r.Header),
		HTTPMeta:        objectHTTPMeta(r.Header),
		IfNoneMatchAny:  cond.ifNoneMatchAny,
		IfMatchETag:     cond.ifMatchETag,
		IfMatchAny:      cond.ifMatchAny,
	})
	if err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}

	writeXMLBody(w, r, http.StatusOK, copyObjectResult{
		LastModified: iso8601(info.Modified),
		ETag:         quoteETag(info.ETag),
	})
	loggerFrom(r.Context(), s.logger).Debug("object_copy",
		slog.String("bucket", bucket),
		slog.String("key", key),
		slog.String("src_bucket", src.Bucket),
		slog.String("src_key", src.Key),
		slog.Bool("replace_metadata", replace),
		slog.Int64("size", info.Size),
	)
}
