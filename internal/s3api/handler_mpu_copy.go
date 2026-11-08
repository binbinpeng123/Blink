package s3api

import (
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/binbinpeng123/Blink/internal/s3err"
	"github.com/binbinpeng123/Blink/internal/store"
)

// maxCopyPartSize 是单个复制分片的字节上限：5 GiB。
//
// 与 CopyObject 的源对象上限相同，但两者约束的对象不同：CopyObject
// 限制"整个源对象"，这里限制"本次复制区间"。超过时不做自动拆分 ——
// 客户端必须自己按 ≤5 GiB 的区间多次调用。
const maxCopyPartSize int64 = 5 << 30

// copyPartResult 是 UploadPartCopy 的成功响应。
type copyPartResult struct {
	XMLName      xml.Name `xml:"CopyPartResult"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

// handleUploadPartCopy 处理 PUT /{bucket}/{key}?partNumber=N&uploadId=U
// 且带 x-amz-copy-source 的请求。
//
// 数据路径刻意只有一条：OpenObject 读出源区间，io.Pipe 交给 UploadPart
// 重新切块写 CAS。不做"复制块表"之类的捷径 —— 目标分片是新的内容边界，
// 块表必然要按新偏移重算；集群模式下目标节点也可能与源不同。
func (s *Server) handleUploadPartCopy(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) {
	q := r.URL.Query()
	uploadID := q.Get("uploadId")

	partNumber, perr := parsePartNumber(q.Get("partNumber"))
	if perr != nil {
		perr.Write(w, r, reqID)
		return
	}

	src, serr := parseCopySource(r.Header.Get("x-amz-copy-source"))
	if serr != nil {
		serr.Write(w, r, reqID)
		return
	}

	// 存储类型、加密与标签头与 CopyObject 同一套规则：分片本身不携带
	// 这些属性，出现即 501，绝不静默忽略。x-amz-metadata-directive 在
	// 分片复制上没有定义，按 S3 行为忽略。
	if sc := r.Header.Get("x-amz-storage-class"); sc != "" && !strings.EqualFold(sc, storageClassStandard) {
		s3err.New(s3err.CodeNotImplemented).
			WithMessage("本版本只支持 STANDARD 存储类型。").
			Write(w, r, reqID)
		return
	}
	for _, h := range []string{
		"x-amz-server-side-encryption",
		"x-amz-server-side-encryption-customer-algorithm",
		"x-amz-server-side-encryption-aws-kms-key-id",
		"x-amz-tagging",
	} {
		if r.Header.Get(h) != "" {
			s3err.New(s3err.CodeNotImplemented).
				WithMessage("分片复制暂不支持 "+h+"。").
				Write(w, r, reqID)
			return
		}
	}

	obj, err := s.store.OpenObject(src.Bucket, src.Key)
	if err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}
	info := obj.Info()

	// 源条件与 CopyObject 共用同一套求值：四个 x-amz-copy-source-if-*，
	// 不满足一律 412。源不存在已在 OpenObject 处先回 404。
	if e := checkCopySourceConditions(r, info); e != nil {
		e.Write(w, r, reqID)
		return
	}

	offset, length, rangeErr := parseCopyPartRange(
		r.Header.Get("x-amz-copy-source-range"), info.Size)
	if rangeErr != nil {
		if rangeErr.Code == s3err.CodeInvalidRange {
			// 与 GetObject 的 416 一致：告诉客户端源对象的真实大小。
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", info.Size))
		}
		rangeErr.Write(w, r, reqID)
		return
	}
	if length > maxCopyPartSize {
		s3err.New(s3err.CodeInvalidRequest).
			WithMessage(fmt.Sprintf("单个复制分片 %d 字节超过 5 GiB 上限，请按区间分片复制。", length)).
			Write(w, r, reqID)
		return
	}

	part, err := s.copyRangeToPart(bucket, key, uploadID, partNumber, obj, offset, length)
	if err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}

	h := w.Header()
	h.Set("ETag", quoteETag(part.ETag))
	if part.ChecksumAlgorithm != "" && part.Checksum != "" {
		h.Set(part.ChecksumAlgorithm.Header(), part.Checksum)
	}
	writeXMLBody(w, r, http.StatusOK, copyPartResult{
		LastModified: iso8601(part.Modified),
		ETag:         quoteETag(part.ETag),
	})

	loggerFrom(r.Context(), s.logger).Debug("multipart_part_copied",
		slog.String("bucket", bucket),
		slog.String("key", key),
		slog.String("upload_id", uploadID),
		slog.Int("part_number", partNumber),
		slog.String("src_bucket", src.Bucket),
		slog.String("src_key", src.Key),
		slog.Int64("offset", offset),
		slog.Int64("length", length),
	)
}

// copyRangeToPart 把源对象区间通过管道喂给 UploadPart。
//
// 管道而不是先读进内存：单个分片上限 5 GiB，任何缓冲区方案都不可行。
// UploadPart 的 ExpectedSize 同时守两道关 —— 写端少写会得到
// ErrIncompleteBody，多写会被长度校验截住。
//
// 失败路径必须先 CloseWithError 唤醒写端再等待它退出：UploadPart 可能
// 在读取 body 之前就返回（如 NoSuchUpload），此时写端还阻塞在管道上，
// 不关闭就会永久泄漏一个 goroutine 并持有源对象 reader。
func (s *Server) copyRangeToPart(bucket, key, uploadID string, partNumber int,
	obj store.Reader, offset, length int64) (*store.PartInfo, error) {

	pr, pw := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		// WriteRangeTo 返回后关闭写端：成功时给 EOF，失败时把错误
		// 传给读端，让 UploadPart 以读取错误结束，而不是一直等。
		pw.CloseWithError(obj.WriteRangeTo(pw, offset, length))
	}()

	part, err := s.store.UploadPart(bucket, key, uploadID, partNumber, pr,
		store.UploadPartOptions{ExpectedSize: length})
	if err != nil {
		// 读端提前放弃：关闭管道，写端从阻塞的 Write 立即返回。
		_ = pr.CloseWithError(err)
		<-done
		return nil, err
	}
	// 正常路径也要关闭读端：UploadPart 已按 ExpectedSize 读满，
	// 但 Close 让写端的收尾提前返回，避免依赖 EOF 传播的时序。
	_ = pr.Close()
	<-done
	return part, nil
}

// parseCopyPartRange 解析 x-amz-copy-source-range。
//
// 只接受 `bytes=first-last`：两端都必须给出。`first-`（到末尾）与
// `-suffix`（末尾 N 字节）在分片复制中没有实际价值，多支持一种语法
// 就多一处与 S3 的语义偏差；多区间同理。
//
// 越界是严格错误（不像 Range 请求可以截断到对象末尾）：复制区间越界
// 说明客户端算错了分片边界，静默截断会让目标对象比预期短，直到
// Complete 后才暴露。
func parseCopyPartRange(spec string, size int64) (offset, length int64, e *s3err.Error) {
	invalid := func(msg string) (int64, int64, *s3err.Error) {
		return 0, 0, s3err.New(s3err.CodeInvalidRange).WithMessage(msg)
	}

	spec = strings.TrimSpace(spec)
	if spec == "" {
		return 0, size, nil
	}
	if !strings.HasPrefix(strings.ToLower(spec), "bytes=") {
		return invalid("x-amz-copy-source-range 只支持 `bytes=first-last`。")
	}
	rest := strings.TrimSpace(spec[len("bytes="):])
	if strings.Contains(rest, ",") {
		return invalid("x-amz-copy-source-range 不支持多区间。")
	}
	first, last, ok := strings.Cut(rest, "-")
	if !ok {
		return invalid("x-amz-copy-source-range 格式应为 bytes=first-last。")
	}
	first, last = strings.TrimSpace(first), strings.TrimSpace(last)
	if first == "" || last == "" {
		return invalid("x-amz-copy-source-range 必须同时给出起点与终点。")
	}

	begin, err := strconv.ParseInt(first, 10, 64)
	if err != nil || begin < 0 {
		return invalid("x-amz-copy-source-range 起点不合法。")
	}
	end, err := strconv.ParseInt(last, 10, 64)
	if err != nil || end < begin {
		return invalid("x-amz-copy-source-range 终点不合法。")
	}
	if begin >= size || end >= size {
		return invalid(fmt.Sprintf("复制区间 %d-%d 超出源对象大小 %d。", begin, end, size))
	}
	return begin, end - begin + 1, nil
}
