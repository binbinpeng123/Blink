package s3api

import (
	"encoding/xml"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/binbinpeng123/Blink/internal/checksum"
	"github.com/binbinpeng123/Blink/internal/s3err"
	"github.com/binbinpeng123/Blink/internal/store"
)

// objectAttributeNames 是 GetObjectAttributes 允许的 x-amz-object-attributes 值。
//
// 用规范大小写做键，解析时大小写不敏感匹配：HTTP 头值本身不区分大小写，
// 不同 SDK 对枚举值的序列化并不完全一致。
var objectAttributeNames = []string{
	"ETag", "Checksum", "ObjectParts", "ObjectSize", "StorageClass",
}

// dispatchObjectAttributes 处理对象级 `?attributes` 子资源。
//
// 返回 true 表示请求已被处理，调用方必须立即返回。
//
// 必须在通用方法分派与 multipart 分派之前处理：`GET /{bucket}/{key}?attributes`
// 与普通对象下载共用同一路径，不拦下就会把属性查询变成一次完整对象下载，
// 客户端拿到 200 却按属性文档解析 —— 错误要到很久之后才暴露。
func (s *Server) dispatchObjectAttributes(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) bool {
	if !r.URL.Query().Has("attributes") {
		return false
	}
	if bucket == "" || key == "" {
		notImplemented("?attributes 只支持对象级请求：GET /{bucket}/{key}?attributes。").
			Write(w, r, reqID)
		return true
	}
	// 只有 GetObjectAttributes 合法。其它方法回 405 而不是 501：
	// 子资源存在，只是方法不对（与 ?location / ?cors 的约定一致）。
	if r.Method != http.MethodGet {
		s3errMethodNotAllowed(w, r)
		return true
	}
	// 本版本没有多版本，也没有按 uploadId 查询属性的语义。这些组合同样
	// 不能"忽略附加参数、返回当前对象属性"：客户端会以为查的是指定版本
	// 或某次上传，结果与预期完全不同。
	q := r.URL.Query()
	if q.Has("versionId") || q.Has("uploadId") || q.Has("partNumber") {
		notImplemented("GetObjectAttributes 不支持 versionId/uploadId/partNumber 组合。").
			Write(w, r, reqID)
		return true
	}
	if !s.requireStore(w, r, reqID) {
		return true
	}
	s.handleGetObjectAttributes(w, r, bucket, key, reqID)
	return true
}

// handleGetObjectAttributes 处理 GET /{bucket}/{key}?attributes。
//
// 响应只包含客户端请求的属性：缺省的属性不出现（不是空元素），
// 这样客户端才能用"元素是否存在"判断这项属性是否可得。
func (s *Server) handleGetObjectAttributes(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) {
	attrs, attrErr := parseObjectAttributes(r.Header.Get("x-amz-object-attributes"))
	if attrErr != nil {
		attrErr.Write(w, r, reqID)
		return
	}

	// 分页参数只在请求 ObjectParts 时校验：它们对其它属性没有意义，
	// 若客户端只是在复用一套查询参数，不该因此拿到 400。
	var maxParts, marker int
	if attrs["ObjectParts"] {
		var parseErr *s3err.Error
		maxParts, marker, parseErr = parseObjectPartsQuery(r)
		if parseErr != nil {
			parseErr.Write(w, r, reqID)
			return
		}
	}

	reader, err := s.store.OpenObject(bucket, key)
	if err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}
	info := reader.Info()

	out := getObjectAttributesResponse{}
	if attrs["ETag"] {
		// GetObjectAttributes 的 ETag **不带引号**（与 GET/HEAD 响应头
		// 不同）。部分客户端直接把这个值当作十六进制串比较，加引号会失配。
		etag := info.ETag
		out.ETag = &etag
	}
	if attrs["Checksum"] && info.Checksum != "" {
		if values, ok := checksumValuesOf(info.ChecksumAlgorithm, info.Checksum); ok {
			out.Checksum = &objectChecksumAttributes{
				checksumValues: values,
				ChecksumType:   info.ChecksumType,
			}
		}
	}
	if attrs["ObjectParts"] {
		out.ObjectParts = objectPartsOf(info.Parts, info.ChecksumAlgorithm, maxParts, marker)
	}
	if attrs["ObjectSize"] {
		size := info.Size
		out.ObjectSize = &size
	}
	if attrs["StorageClass"] {
		class := storageClassStandard
		out.StorageClass = &class
	}

	// Last-Modified 是 GetObjectAttributes 的固定响应头，与是否请求
	// ETag 无关；客户端靠它判断手里缓存的属性是否还新鲜。
	w.Header().Set("Last-Modified", httpTime(info.Modified))
	writeXMLBody(w, r, http.StatusOK, out)
	loggerFrom(r.Context(), s.logger).Debug("object_attributes",
		slog.String("bucket", bucket),
		slog.String("key", key),
		slog.Any("attributes", attrs),
	)
}

// parseObjectAttributes 解析 x-amz-object-attributes 头。
//
// 返回请求的属性集合（规范大小写）。头缺失、为空、含空项或未知属性
// 一律 400：静默忽略未知值会返回一份缺字段的文档，客户端无从判断
// 是"服务端没有这项属性"还是"请求头写错了"。
func parseObjectAttributes(header string) (map[string]bool, *s3err.Error) {
	header = strings.TrimSpace(header)
	if header == "" {
		return nil, invalidArg("GetObjectAttributes 必须提供 x-amz-object-attributes 请求头。")
	}
	out := make(map[string]bool, len(objectAttributeNames))
	for _, raw := range strings.Split(header, ",") {
		name := strings.TrimSpace(raw)
		if name == "" {
			return nil, invalidArg("x-amz-object-attributes 含空项。")
		}
		canonical := ""
		for _, allowed := range objectAttributeNames {
			if strings.EqualFold(name, allowed) {
				canonical = allowed
				break
			}
		}
		if canonical == "" {
			return nil, invalidArg(fmt.Sprintf("无法识别的对象属性 %q；合法值：%s。",
				name, strings.Join(objectAttributeNames, "、")))
		}
		out[canonical] = true
	}
	return out, nil
}

// parseObjectPartsQuery 解析 ObjectParts 的分页参数。
//
// 语义与 ListParts 一致：max-parts 缺省 1000、上限 1000；marker 缺省 0。
// 非整数或负数属于"请求写错了" → 400，而不是静默按缺省值处理。
func parseObjectPartsQuery(r *http.Request) (maxParts, marker int, e *s3err.Error) {
	q := r.URL.Query()
	maxParts = defaultMaxParts
	if q.Has("max-parts") {
		n, err := strconv.Atoi(q.Get("max-parts"))
		if err != nil || n < 0 {
			return 0, 0, invalidArg("max-parts 必须是非负整数。")
		}
		if n > maxMaxParts {
			n = maxMaxParts
		}
		maxParts = n
	}
	if q.Has("part-number-marker") {
		n, err := strconv.Atoi(q.Get("part-number-marker"))
		if err != nil || n < 0 {
			return 0, 0, invalidArg("part-number-marker 必须是非负整数。")
		}
		marker = n
	}
	return maxParts, marker, nil
}

// objectPartsOf 构造 ObjectParts 元素，并按 marker/maxParts 分页。
//
// 非分片对象也会返回该元素（空分片集合、计数为 0）：客户端请求了
// ObjectParts 就应得到明确答复"没有分片"，而不是靠元素缺失去猜。
//
// alg 是对象级校验和算法：分片校验和只在会话声明了算法时才会落盘，
// 元素名（ChecksumCRC32 等）由它决定；没有算法时整份分片校验和省略。
func objectPartsOf(parts []store.ObjectPart, alg checksum.Algorithm, maxParts, marker int) *objectPartsAttributes {
	out := &objectPartsAttributes{
		MaxParts:         maxParts,
		PartNumberMarker: marker,
		TotalPartsCount:  len(parts),
	}
	for _, p := range parts {
		if p.Number <= marker {
			continue
		}
		if len(out.Parts) >= maxParts {
			out.IsTruncated = true
			break
		}
		part := objectPartAttributes{
			PartNumber: p.Number,
			Size:       p.Size,
		}
		if values, ok := checksumValuesOf(alg, p.Checksum); ok {
			part.checksumValues = values
		}
		out.Parts = append(out.Parts, part)
	}
	if out.IsTruncated && len(out.Parts) > 0 {
		next := out.Parts[len(out.Parts)-1].PartNumber
		out.NextPartNumberMarker = &next
	}
	out.PartsCount = len(out.Parts)
	return out
}

// checksumValuesOf 把算法与值映射到对应的 XML 元素字段。
//
// 算法未知或值为空时返回 ok=false：宁可省略整个 Checksum 元素，
// 也不能把一个来历不明的值挂到错误的算法名下发出去。
func checksumValuesOf(alg checksum.Algorithm, value string) (checksumValues, bool) {
	if value == "" || alg == "" {
		return checksumValues{}, false
	}
	out := checksumValues{}
	switch alg {
	case checksum.AlgCRC32:
		out.ChecksumCRC32 = &value
	case checksum.AlgCRC32C:
		out.ChecksumCRC32C = &value
	case checksum.AlgCRC64NVME:
		out.ChecksumCRC64NVME = &value
	case checksum.AlgSHA1:
		out.ChecksumSHA1 = &value
	case checksum.AlgSHA256:
		out.ChecksumSHA256 = &value
	default:
		return checksumValues{}, false
	}
	return out, true
}

// getObjectAttributesResponse 是 GetObjectAttributes 的响应体。
//
// 字段全部带 omitempty：文档只包含客户端请求的属性，未请求的属性
// 既不出现空元素也不出现零值。
type getObjectAttributesResponse struct {
	XMLName      xml.Name                  `xml:"http://s3.amazonaws.com/doc/2006-03-01/ GetObjectAttributesResponse"`
	ETag         *string                   `xml:"ETag,omitempty"`
	Checksum     *objectChecksumAttributes `xml:"Checksum,omitempty"`
	ObjectParts  *objectPartsAttributes    `xml:"ObjectParts,omitempty"`
	ObjectSize   *int64                    `xml:"ObjectSize,omitempty"`
	StorageClass *string                   `xml:"StorageClass,omitempty"`
}

// checksumValues 是五种算法对应的 XML 元素集合。
//
// 用匿名内嵌让对象级与分片级共享字段；encoding/xml 会把匿名结构体的
// 字段提升到外层，与手写五份字段的序列化结果相同。
type checksumValues struct {
	ChecksumCRC32     *string `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C    *string `xml:"ChecksumCRC32C,omitempty"`
	ChecksumCRC64NVME *string `xml:"ChecksumCRC64NVME,omitempty"`
	ChecksumSHA1      *string `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256    *string `xml:"ChecksumSHA256,omitempty"`
}

// objectChecksumAttributes 是对象级 Checksum 元素。
type objectChecksumAttributes struct {
	checksumValues
	ChecksumType string `xml:"ChecksumType,omitempty"`
}

// objectPartsAttributes 是 ObjectParts 元素。
type objectPartsAttributes struct {
	IsTruncated          bool                   `xml:"IsTruncated"`
	MaxParts             int                    `xml:"MaxParts"`
	NextPartNumberMarker *int                   `xml:"NextPartNumberMarker,omitempty"`
	PartNumberMarker     int                    `xml:"PartNumberMarker"`
	Parts                []objectPartAttributes `xml:"Part"`
	PartsCount           int                    `xml:"PartsCount"`
	TotalPartsCount      int                    `xml:"TotalPartsCount"`
}

// objectPartAttributes 是 ObjectParts 中的单个 Part。
type objectPartAttributes struct {
	checksumValues
	PartNumber int   `xml:"PartNumber"`
	Size       int64 `xml:"Size"`
}
