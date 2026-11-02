package s3api

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/binbinpeng123/Blink/internal/store"
)

// CopyObject 是 S3 的"服务端复制"：PUT /{bucket}/{key} 带 x-amz-copy-source
// 头，请求体为空，源对象的内容由服务端读取后写到目标键。
//
// 为什么必须显式识别这个头：不识别时请求会落进普通 PutObject，
// 而 copy 请求的 Content-Length 是 0 —— 结果是"成功"地用一个空对象
// 覆盖了目标键。客户端随后解析响应又会失败（CopyObject 期望 XML
// CopyObjectResult，拿到的是空 body），留下一个 0 字节对象和一条
// 互相矛盾的日志。minio-go 的 copy_object 正是这种请求形态，
// edge-flywheel-training 的"先 staging 再覆盖"就依赖它。
//
// 本版本只实现默认的 COPY 元数据语义（沿用源对象的 Content-Type 与
// 用户元数据）。条件复制（x-amz-copy-source-if-*）与 tagging 复制
// 明确返回 501：忽略它们会让调用方以为条件生效了。
type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	ETag         string   `xml:"ETag"`
	LastModified string   `xml:"LastModified"`
}

func (s *Server) handleCopyObject(w http.ResponseWriter, r *http.Request, dstBucket, dstKey, reqID string) {
	srcBucket, srcKey, ok := parseCopySource(r.Header.Get("x-amz-copy-source"))
	if !ok {
		invalidArg("x-amz-copy-source 必须形如 /{bucket}/{key}。").
			Write(w, r, reqID)
		return
	}

	// 未实现的条件请求一律拒绝，而不是"当没看见"。
	if v := r.Header.Get("x-amz-copy-source-if-match"); v != "" {
		notImplemented("暂不支持 x-amz-copy-source-if-match。").Write(w, r, reqID)
		return
	}
	if v := r.Header.Get("x-amz-copy-source-if-none-match"); v != "" {
		notImplemented("暂不支持 x-amz-copy-source-if-none-match。").Write(w, r, reqID)
		return
	}
	if v := r.Header.Get("x-amz-copy-source-if-modified-since"); v != "" {
		notImplemented("暂不支持 x-amz-copy-source-if-modified-since。").Write(w, r, reqID)
		return
	}
	if v := r.Header.Get("x-amz-copy-source-if-unmodified-since"); v != "" {
		notImplemented("暂不支持 x-amz-copy-source-if-unmodified-since。").Write(w, r, reqID)
		return
	}
	if v := r.Header.Get("x-amz-tagging-directive"); v != "" && !strings.EqualFold(v, "COPY") {
		notImplemented("本版本不支持对象标签，x-amz-tagging-directive 只能是 COPY。").Write(w, r, reqID)
		return
	}

	directive := strings.ToUpper(strings.TrimSpace(r.Header.Get("x-amz-metadata-directive")))
	if directive == "" {
		directive = "COPY"
	}
	if directive != "COPY" && directive != "REPLACE" {
		invalidArg("x-amz-metadata-directive 只能是 COPY 或 REPLACE。").Write(w, r, reqID)
		return
	}

	reader, err := s.store.OpenObject(srcBucket, srcKey)
	if err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}
	src := reader.Info()

	opt := store.PutOptions{
		ContentType:  src.ContentType,
		UserMeta:     src.UserMeta,
		ExpectedSize: src.Size,
	}
	if directive == "REPLACE" {
		opt.ContentType = r.Header.Get("Content-Type")
		opt.UserMeta = userMetadata(r.Header)
	}

	// ObjectReader 只提供 WriteRangeTo（按块表顺序输出），不实现 io.Reader；
	// 用管道把它接到 PutObject 的流式写入路径上，无需把整个对象读进内存。
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(reader.WriteRangeTo(pw, 0, src.Size))
	}()

	info, err := s.store.PutObject(dstBucket, dstKey, pr, opt)
	if err != nil {
		// PutObject 可能在读完之前就返回（校验失败、目标桶不存在等）；
		// 关闭读端让上面的 goroutine 的 Write 解除阻塞，避免泄漏。
		_ = pr.CloseWithError(err)
		s.writeStoreErr(w, r, reqID, err)
		return
	}

	w.Header().Set("ETag", `"`+info.ETag+`"`)
	writeXMLBody(w, r, http.StatusOK, copyObjectResult{
		ETag:         `"` + info.ETag + `"`,
		LastModified: iso8601(info.Modified),
	})
}

// parseCopySource 解析 x-amz-copy-source。
//
// 两种写法都要接受：minio-go 发 "/bucket/key"，部分客户端发 "bucket/key"。
// key 里的非 ASCII 与特殊字符由客户端做 URL 编码（路径段编码），
// 因此必须先整体反转义再按第一个 '/' 切分 —— 反过来先切分会把
// 编码后的 %2F 误当成层级分隔符。
func parseCopySource(raw string) (bucket, key string, ok bool) {
	if raw == "" {
		return "", "", false
	}
	decoded, err := url.PathUnescape(strings.TrimPrefix(raw, "/"))
	if err != nil {
		return "", "", false
	}
	i := strings.IndexByte(decoded, '/')
	if i <= 0 || i == len(decoded)-1 {
		return "", "", false
	}
	return decoded[:i], decoded[i+1:], true
}
