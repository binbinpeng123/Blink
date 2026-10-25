package s3api

import (
	"encoding/xml"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/binbinpeng123/Blink/internal/s3err"
	"github.com/binbinpeng123/Blink/internal/store"
)

// 分片上传的协议常量。
const (
	// maxPartNumber 是 S3 规定的分片编号上限（与 store 内部一致）。
	maxPartNumber = 10000
	// defaultMaxParts / maxMaxParts 是 ListParts 的默认值与上限。
	defaultMaxParts = 1000
	maxMaxParts     = 1000
	// defaultMaxUploads / maxMaxUploads 是 ListMultipartUploads 的默认值与上限。
	defaultMaxUploads = 1000
	maxMaxUploads     = 1000
	// maxCompleteBodyBytes 是 Complete 请求体的上限。
	//
	// 10000 个分片（S3 上限）每个约 100 字节，真实体积不到 1 MiB；
	// 4 MiB 留足余量，同时挡住"用超大 XML 把内存吃光"的请求。
	maxCompleteBodyBytes = 4 << 20
)

// handleCreateMultipartUpload 处理 POST /{bucket}/{key}?uploads。
func (s *Server) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) {
	// 会话级的算法与类型在创建时确定，此后每个分片都必须按它计算 ——
	// 中途换算法会让 Complete 无从合并校验和。
	alg, _, algErr := parseChecksumHeaders(r.Header)
	if algErr != nil {
		algErr.Write(w, r, reqID)
		return
	}
	checksumType := strings.TrimSpace(r.Header.Get("x-amz-checksum-type"))

	up, err := s.store.CreateMultipartUpload(bucket, key, store.CreateMultipartOptions{
		ContentType:       r.Header.Get("Content-Type"),
		UserMeta:          userMetadata(r.Header),
		ChecksumAlgorithm: alg,
		ChecksumType:      checksumType,
	})
	if err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}

	h := w.Header()
	if up.ChecksumAlgorithm != "" {
		// 把归一后的算法与类型回告客户端：客户端据此决定每个分片算哪种校验和。
		h.Set("x-amz-checksum-algorithm", string(up.ChecksumAlgorithm))
		h.Set("x-amz-checksum-type", up.ChecksumType)
	}

	writeXMLBody(w, r, http.StatusOK, initiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadID: up.UploadID,
	})
	loggerFrom(r.Context(), s.logger).Debug("multipart_created",
		slog.String("bucket", bucket),
		slog.String("key", key),
		slog.String("upload_id", up.UploadID),
		slog.String("checksum_algorithm", string(up.ChecksumAlgorithm)),
		slog.String("checksum_type", up.ChecksumType),
	)
}

// handleUploadPart 处理 PUT /{bucket}/{key}?partNumber=N&uploadId=X。
func (s *Server) handleUploadPart(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) {
	q := r.URL.Query()
	uploadID := q.Get("uploadId")

	// 与 PutObject 同样的理由：aws-chunked 的请求体是分块框架而非对象字节，
	// 不解帧就落盘会存进错误的内容。解帧后 r.ContentLength 被修正为
	// x-amz-decoded-content-length，下面的长度检查对两种传输方式同样成立。
	chunked, serr := decodeChunkedBody(r)
	if serr != nil {
		serr.Write(w, r, reqID)
		return
	}
	// S3 要求每个分片都必须声明长度；长度未知时无法判断请求体是否被截断。
	if r.ContentLength < 0 {
		s3err.New(s3err.CodeLengthRequired).Write(w, r, reqID)
		return
	}

	partNumber, perr := parsePartNumber(q.Get("partNumber"))
	if perr != nil {
		perr.Write(w, r, reqID)
		return
	}
	contentMD5, ok := parseContentMD5(r.Header.Get("Content-MD5"))
	if !ok {
		s3err.New(s3err.CodeInvalidDigest).Write(w, r, reqID)
		return
	}
	alg, declared, algErr := parseChecksumHeaders(r.Header)
	if algErr != nil {
		algErr.Write(w, r, reqID)
		return
	}
	if alg != "" && declared == "" && chunked != nil && !hasTrailerName(r.Header, alg.Header()) {
		s3err.New(s3err.CodeInvalidArgument).
			WithMessage("声明了 "+string(alg)+" 校验和，但 x-amz-trailer 未包含 "+alg.Header()+"。").
			Write(w, r, reqID)
		return
	}
	var lateChecksum func() (string, bool)
	if alg != "" && declared == "" {
		lateChecksum = func() (string, bool) {
			if chunked != nil {
				return chunked.Trailer(alg.Header())
			}
			v := r.Trailer.Get(alg.Header())
			return v, v != ""
		}
	}

	part, err := s.store.UploadPart(bucket, key, uploadID, partNumber, r.Body, store.UploadPartOptions{
		ExpectedSize:      r.ContentLength,
		ContentMD5:        contentMD5,
		ChecksumAlgorithm: alg,
		Checksum:          declared,
		LateChecksum:      lateChecksum,
	})
	if err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}

	h := w.Header()
	h.Set("ETag", quoteETag(part.ETag))
	// 回传服务端实际算出的分片校验和。客户端（aws-cli）会把它写进
	// Complete 的 XML；Complete 响应里的全对象校验和必须与之一致。
	if part.ChecksumAlgorithm != "" && part.Checksum != "" {
		h.Set(part.ChecksumAlgorithm.Header(), part.Checksum)
	}
	w.WriteHeader(http.StatusOK)

	loggerFrom(r.Context(), s.logger).Debug("multipart_part_uploaded",
		slog.String("bucket", bucket),
		slog.String("key", key),
		slog.String("upload_id", uploadID),
		slog.Int("part_number", partNumber),
		slog.Int64("size", part.Size),
	)
}

// handleCompleteMultipartUpload 处理 POST /{bucket}/{key}?uploadId=X。
func (s *Server) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) {
	uploadID := r.URL.Query().Get("uploadId")

	condErr := checkPutPreconditions(r)
	if condErr != nil {
		condErr.Write(w, r, reqID)
		return
	}

	var req completeMultipartUploadRequest
	if err := decodeXMLBody(w, r, maxCompleteBodyBytes, &req); err != nil {
		s3err.New(s3err.CodeMalformedXML).Write(w, r, reqID)
		return
	}
	if len(req.Parts) == 0 {
		// 空列表在 store 里也有防御性检查（ErrInvalidPartOrder），
		// 但在 HTTP 层先按 MalformedXML 拒绝更贴近 AWS：这是"请求体不合法"，
		// 不是"分片顺序不对"。
		s3err.New(s3err.CodeMalformedXML).
			WithMessage("CompleteMultipartUpload 至少需要一个 Part。").
			Write(w, r, reqID)
		return
	}

	parts := make([]store.CompletePart, 0, len(req.Parts))
	for _, p := range req.Parts {
		parts = append(parts, store.CompletePart{PartNumber: p.PartNumber, ETag: p.ETag})
	}

	// 客户端可能声明全对象的校验和。store 会把它与合并结果比对：
	// 这是客户端侧对"分片拼起来是否就是它上传的内容"的最后一道确认。
	_, declared, algErr := parseChecksumHeaders(r.Header)
	if algErr != nil {
		algErr.Write(w, r, reqID)
		return
	}

	info, err := s.store.CompleteMultipartUpload(bucket, key, uploadID, parts, declared,
		strings.TrimSpace(r.Header.Get("If-None-Match")) == "*")
	if err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}

	h := w.Header()
	h.Set("ETag", quoteETag(info.ETag))
	if info.ChecksumAlgorithm != "" {
		h.Set(info.ChecksumAlgorithm.Header(), info.Checksum)
		if info.ChecksumType != "" {
			h.Set("x-amz-checksum-type", info.ChecksumType)
		}
	}

	out := completeMultipartUploadResult{
		Location: objectLocation(r),
		Bucket:   bucket,
		Key:      key,
		ETag:     quoteETag(info.ETag),
	}
	if info.ChecksumAlgorithm != "" {
		out.Checksum = &checksumXMLElement{
			XMLName: xml.Name{Local: "Checksum" + string(info.ChecksumAlgorithm)},
			Value:   info.Checksum,
		}
		out.ChecksumType = info.ChecksumType
	}
	writeXMLBody(w, r, http.StatusOK, out)

	loggerFrom(r.Context(), s.logger).Debug("multipart_completed",
		slog.String("bucket", bucket),
		slog.String("key", key),
		slog.String("upload_id", uploadID),
		slog.Int("parts", len(parts)),
		slog.Int64("size", info.Size),
		slog.Int("blocks", info.BlockCount),
		slog.Int("deduped_blocks", info.DedupedBlocks),
	)
}

// handleAbortMultipartUpload 处理 DELETE /{bucket}/{key}?uploadId=X。
func (s *Server) handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) {
	uploadID := r.URL.Query().Get("uploadId")
	if err := s.store.AbortMultipartUpload(bucket, key, uploadID); err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}
	// 02 文档：Abort 成功回 204，无响应体。
	w.WriteHeader(http.StatusNoContent)
	loggerFrom(r.Context(), s.logger).Debug("multipart_aborted",
		slog.String("bucket", bucket),
		slog.String("key", key),
		slog.String("upload_id", uploadID),
	)
}

// handleListParts 处理 GET /{bucket}/{key}?uploadId=X。
func (s *Server) handleListParts(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) {
	q := r.URL.Query()
	uploadID := q.Get("uploadId")

	opts := store.ListPartsOptions{MaxParts: defaultMaxParts}
	if q.Has("max-parts") {
		n, err := strconv.Atoi(q.Get("max-parts"))
		if err != nil || n < 0 {
			invalidArg("max-parts 必须是非负整数。").Write(w, r, reqID)
			return
		}
		if n > maxMaxParts {
			n = maxMaxParts
		}
		opts.MaxParts = n
	}
	if q.Has("part-number-marker") {
		n, err := strconv.Atoi(q.Get("part-number-marker"))
		if err != nil || n < 0 {
			invalidArg("part-number-marker 必须是非负整数。").Write(w, r, reqID)
			return
		}
		opts.PartNumberMarker = n
	}
	encodingType, encErr := parseEncodingType(q.Get("encoding-type"))
	if encErr != nil {
		encErr.Write(w, r, reqID)
		return
	}
	enc := func(v string) string { return encodeIfURL(encodingType, v) }

	res, err := s.store.ListParts(bucket, key, uploadID, opts)
	if err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}

	out := listPartsResult{
		Bucket:               bucket,
		Key:                  enc(key),
		UploadID:             res.Upload.UploadID,
		PartNumberMarker:     opts.PartNumberMarker,
		NextPartNumberMarker: res.NextPartNumberMarker,
		MaxParts:             opts.MaxParts,
		IsTruncated:          res.IsTruncated,
		StorageClass:         storageClassStandard,
		EncodingType:         encodingType,
	}
	if res.Upload.ChecksumAlgorithm != "" {
		out.ChecksumAlgorithm = string(res.Upload.ChecksumAlgorithm)
		out.ChecksumType = res.Upload.ChecksumType
	}
	for _, p := range res.Parts {
		e := listPartsPart{
			PartNumber:   p.PartNumber,
			LastModified: iso8601(p.Modified),
			ETag:         quoteETag(p.ETag),
			Size:         p.Size,
		}
		if res.Upload.ChecksumAlgorithm != "" && p.Checksum != "" {
			e.Checksum = &checksumXMLElement{
				XMLName: xml.Name{Local: "Checksum" + string(res.Upload.ChecksumAlgorithm)},
				Value:   p.Checksum,
			}
		}
		out.Parts = append(out.Parts, e)
	}

	writeXMLBody(w, r, http.StatusOK, out)
	loggerFrom(r.Context(), s.logger).Debug("multipart_parts_listed",
		slog.String("bucket", bucket),
		slog.String("key", key),
		slog.String("upload_id", uploadID),
		slog.Int("parts", len(res.Parts)),
		slog.Bool("truncated", res.IsTruncated),
	)
}

// handleListMultipartUploads 处理 GET /{bucket}?uploads。
func (s *Server) handleListMultipartUploads(w http.ResponseWriter, r *http.Request, bucket, reqID string) {
	q := r.URL.Query()

	opts := store.ListMultipartUploadsOptions{
		Prefix:     q.Get("prefix"),
		Delimiter:  q.Get("delimiter"),
		MaxUploads: defaultMaxUploads,
	}
	if q.Has("max-uploads") {
		n, err := strconv.Atoi(q.Get("max-uploads"))
		if err != nil || n < 0 {
			invalidArg("max-uploads 必须是非负整数。").Write(w, r, reqID)
			return
		}
		if n > maxMaxUploads {
			n = maxMaxUploads
		}
		opts.MaxUploads = n
	}
	// upload-id-marker 只在 key-marker 同时给出时才有意义（S3 契约），
	// 单独出现时 store 会忽略它。
	if q.Has("key-marker") {
		opts.KeyMarker = q.Get("key-marker")
		opts.UploadIDMarker = q.Get("upload-id-marker")
	}
	encodingType, encErr := parseEncodingType(q.Get("encoding-type"))
	if encErr != nil {
		encErr.Write(w, r, reqID)
		return
	}
	enc := func(v string) string { return encodeIfURL(encodingType, v) }

	res, err := s.store.ListMultipartUploads(bucket, opts)
	if err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}

	out := listMultipartUploadsResult{
		Bucket:         bucket,
		KeyMarker:      enc(opts.KeyMarker),
		UploadIDMarker: enc(opts.UploadIDMarker),
		NextKeyMarker:  enc(res.NextKeyMarker),
		NextUploadID:   enc(res.NextUploadID),
		Delimiter:      enc(opts.Delimiter),
		Prefix:         enc(opts.Prefix),
		EncodingType:   encodingType,
		MaxUploads:     opts.MaxUploads,
		IsTruncated:    res.IsTruncated,
	}
	for _, u := range res.Uploads {
		out.Uploads = append(out.Uploads, multipartUploadEntry{
			Key:               enc(u.Key),
			UploadID:          u.UploadID,
			StorageClass:      storageClassStandard,
			Initiated:         iso8601(u.Initiated),
			ChecksumAlgorithm: string(u.ChecksumAlgorithm),
			ChecksumType:      u.ChecksumType,
		})
	}
	for _, cp := range res.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, commonPrefix{Prefix: enc(cp)})
	}

	writeXMLBody(w, r, http.StatusOK, out)
	loggerFrom(r.Context(), s.logger).Debug("multipart_uploads_listed",
		slog.String("bucket", bucket),
		slog.Int("uploads", len(res.Uploads)),
		slog.Int("common_prefixes", len(res.CommonPrefixes)),
		slog.Bool("truncated", res.IsTruncated),
	)
}

// parsePartNumber 解析并校验 partNumber。
//
// 先在这里拒绝非法值，避免把一个明显错误的编号带进 store 再以
// NoSuchUpload 之类的间接错误返回 —— 客户端会以为是 uploadId 的问题。
func parsePartNumber(v string) (int, *s3err.Error) {
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, invalidArg("partNumber 必须是整数。")
	}
	if n < 1 || n > maxPartNumber {
		return 0, invalidArg("partNumber 必须在 1–10000 之间。")
	}
	return n, nil
}

// parseEncodingType 校验 encoding-type 参数；只支持 "url"。
func parseEncodingType(v string) (string, *s3err.Error) {
	if v == "" {
		return "", nil
	}
	if v != "url" {
		return "", invalidArg(`encoding-type 只支持 "url"。`)
	}
	return v, nil
}

// encodeIfURL 在 encoding-type=url 时按 S3 规则编码。
func encodeIfURL(encodingType, v string) string {
	if encodingType == "url" {
		return s3URLEncode(v)
	}
	return v
}

// decodeXMLBody 解析 S3 的 XML 请求体，并施加上限。
//
// 上限用 http.MaxBytesReader 而不是先 io.ReadAll 再判长度：
// 后者要先把整个 body 读进内存，攻击者用一个大 body 就能在检查之前
// 吃光内存。MaxBytesReader 在读超的瞬间就返回错误。
func decodeXMLBody(w http.ResponseWriter, r *http.Request, limit int64, v any) error {
	return xml.NewDecoder(http.MaxBytesReader(w, r.Body, limit)).Decode(v)
}

// objectLocation 生成 Complete 响应里的 Location。
//
// 客户端（aws-cli）用它拼出对象 URL；用请求自身的 scheme/host/路径，
// 部署在反向代理后面时也能得到对外可见的地址。
func objectLocation(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host + r.URL.EscapedPath()
}

// checksumXMLElement 是"元素名由算法决定"的校验和节点。
//
// encoding/xml 的标签名必须是编译期常量，而这里需要 ChecksumCRC32、
// ChecksumCRC64NVME 之类的动态名字，因此用 XMLName 字段承载。
type checksumXMLElement struct {
	XMLName xml.Name
	Value   string `xml:",chardata"`
}

// initiateMultipartUploadResult 是 CreateMultipartUpload 的响应体。
type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

// completeMultipartUploadRequest 是 Complete 的请求体。
//
// 每个 <Part> 里可能还带 Checksum<ALG> 子元素；本版本不做逐片复核
// （分片校验和已在 UploadPart 时校验过），encoding/xml 会忽略未知元素。
type completeMultipartUploadRequest struct {
	XMLName xml.Name                   `xml:"CompleteMultipartUpload"`
	Parts   []completeMultipartPartReq `xml:"Part"`
}

type completeMultipartPartReq struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// completeMultipartUploadResult 是 Complete 的响应体。
type completeMultipartUploadResult struct {
	XMLName      xml.Name            `xml:"CompleteMultipartUploadResult"`
	Location     string              `xml:"Location"`
	Bucket       string              `xml:"Bucket"`
	Key          string              `xml:"Key"`
	ETag         string              `xml:"ETag"`
	Checksum     *checksumXMLElement `xml:",omitempty"`
	ChecksumType string              `xml:"ChecksumType,omitempty"`
}

// listPartsResult 是 ListParts 的响应体。
type listPartsResult struct {
	XMLName              xml.Name        `xml:"ListPartsResult"`
	Bucket               string          `xml:"Bucket"`
	Key                  string          `xml:"Key"`
	UploadID             string          `xml:"UploadId"`
	PartNumberMarker     int             `xml:"PartNumberMarker"`
	NextPartNumberMarker int             `xml:"NextPartNumberMarker"`
	MaxParts             int             `xml:"MaxParts"`
	IsTruncated          bool            `xml:"IsTruncated"`
	Parts                []listPartsPart `xml:"Part"`
	StorageClass         string          `xml:"StorageClass"`
	ChecksumAlgorithm    string          `xml:"ChecksumAlgorithm,omitempty"`
	ChecksumType         string          `xml:"ChecksumType,omitempty"`
	EncodingType         string          `xml:"EncodingType,omitempty"`
}

type listPartsPart struct {
	PartNumber   int                 `xml:"PartNumber"`
	LastModified string              `xml:"LastModified"`
	ETag         string              `xml:"ETag"`
	Size         int64               `xml:"Size"`
	Checksum     *checksumXMLElement `xml:",omitempty"`
}

// listMultipartUploadsResult 是 ListMultipartUploads 的响应体。
type listMultipartUploadsResult struct {
	XMLName        xml.Name               `xml:"ListMultipartUploadsResult"`
	Bucket         string                 `xml:"Bucket"`
	KeyMarker      string                 `xml:"KeyMarker"`
	UploadIDMarker string                 `xml:"UploadIdMarker"`
	NextKeyMarker  string                 `xml:"NextKeyMarker"`
	NextUploadID   string                 `xml:"NextUploadIdMarker"`
	Delimiter      string                 `xml:"Delimiter,omitempty"`
	Prefix         string                 `xml:"Prefix"`
	EncodingType   string                 `xml:"EncodingType,omitempty"`
	MaxUploads     int                    `xml:"MaxUploads"`
	IsTruncated    bool                   `xml:"IsTruncated"`
	Uploads        []multipartUploadEntry `xml:"Upload"`
	CommonPrefixes []commonPrefix         `xml:"CommonPrefixes"`
}

type multipartUploadEntry struct {
	Key               string `xml:"Key"`
	UploadID          string `xml:"UploadId"`
	StorageClass      string `xml:"StorageClass"`
	Initiated         string `xml:"Initiated"`
	ChecksumAlgorithm string `xml:"ChecksumAlgorithm,omitempty"`
	ChecksumType      string `xml:"ChecksumType,omitempty"`
}
