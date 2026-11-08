package s3api

import (
	"encoding/xml"
	"log/slog"
	"net/http"
	"strings"

	"github.com/binbinpeng123/Blink/internal/s3err"
	"github.com/binbinpeng123/Blink/internal/store"
)

// listV1Params 是解析后的 ListObjects V1 请求参数。
//
// 与 V2 的差别只在起点的表示方式：V1 用 `marker`（上一页最后一条的
// 明文），V2 用不透明的 `continuation-token`。
type listV1Params struct {
	Prefix       string
	Delimiter    string
	Marker       string
	MaxKeys      int
	EncodingType string
}

// parseListV1Params 解析并校验 V1 查询参数。
//
// 未知参数同样忽略：V1 是老客户端的主战场，它们附加的标记参数更多
// （部分 SDK 会带 `x-id=ListObjects` 之外的签名辅助参数）。
func parseListV1Params(r *http.Request) (*listV1Params, *s3err.Error) {
	q := r.URL.Query()
	maxKeys, perr := parseMaxKeys(q)
	if perr != nil {
		return nil, perr
	}
	encType, perr := parseEncodingType(q.Get("encoding-type"))
	if perr != nil {
		return nil, perr
	}
	return &listV1Params{
		Prefix:       q.Get("prefix"),
		Delimiter:    q.Get("delimiter"),
		Marker:       q.Get("marker"),
		MaxKeys:      maxKeys,
		EncodingType: encType,
	}, nil
}

// handleListObjectsV1 处理 GET /{bucket}（不带 list-type 或 list-type=1）。
//
// 分页完全复用 store.PageObjects：V1 与 V2 的差别只在"起点怎么表示"
// 与"下一页怎么告诉客户端"，扫描与折叠规则必须是同一份实现 ——
// 两套边界逻辑最容易在 delimiter 与 marker 的交叉情形上出现分歧。
func (s *Server) handleListObjectsV1(w http.ResponseWriter, r *http.Request, bucket, reqID string) {
	p, perr := parseListV1Params(r)
	if perr != nil {
		perr.Write(w, r, reqID)
		return
	}

	opt := store.ListOptions{
		Prefix:    p.Prefix,
		Delimiter: p.Delimiter,
		MaxKeys:   p.MaxKeys,
	}
	if p.Marker != "" {
		// marker 以 delimiter 结尾时视为公共前缀游标：例如上一页返回了
		// 公共前缀 `photos/`，下一页必须跳过 `photos/` 下的全部 key。
		// 这一判断与 V2 continuation-token 里的 IsPrefix 完全同源。
		opt.Cursor = &store.ListCursor{
			After:    p.Marker,
			IsPrefix: p.Delimiter != "" && strings.HasSuffix(p.Marker, p.Delimiter),
		}
	}

	res, err := s.store.ListObjects(bucket, opt)
	if err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}

	enc := func(v string) string {
		if p.EncodingType == "url" {
			return s3URLEncode(v)
		}
		return v
	}

	out := listBucketResultV1{
		Name:         bucket,
		Prefix:       enc(p.Prefix),
		Marker:       enc(p.Marker),
		MaxKeys:      p.MaxKeys,
		Delimiter:    enc(p.Delimiter),
		IsTruncated:  res.IsTruncated,
		EncodingType: p.EncodingType,
	}
	// V1 的 NextMarker 只在 delimiter 非空时返回（S3 契约）：
	// 不带 delimiter 时客户端直接把最后一条 Contents 的 Key 当 marker，
	// 多给一个 NextMarker 反而会让某些老 SDK 用它去请求一个不存在的参数组合。
	if res.IsTruncated && p.Delimiter != "" && res.Next != nil {
		out.NextMarker = enc(res.Next.After)
	}
	for _, o := range res.Objects {
		out.Contents = append(out.Contents, listContent{
			Key:          enc(o.Key),
			LastModified: iso8601(o.Modified),
			ETag:         quoteETag(o.ETag),
			Size:         o.Size,
			StorageClass: storageClassStandard,
		})
	}
	for _, cp := range res.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, commonPrefix{Prefix: enc(cp)})
	}

	writeXMLBody(w, r, http.StatusOK, out)
	loggerFrom(r.Context(), s.logger).Debug("list_objects_v1",
		slog.String("bucket", bucket),
		slog.Int("key_count", res.KeyCount),
		slog.Bool("truncated", res.IsTruncated),
	)
}

// listBucketResultV1 是 ListObjects V1 的响应体。
//
// 与 V2 的差异是契约的一部分：没有 KeyCount / ContinuationToken /
// StartAfter，多了 Marker / NextMarker。字段顺序按 AWS 的响应习惯排列。
type listBucketResultV1 struct {
	XMLName        xml.Name       `xml:"ListBucketResult"`
	Name           string         `xml:"Name"`
	Prefix         string         `xml:"Prefix"`
	Marker         string         `xml:"Marker"`
	NextMarker     string         `xml:"NextMarker,omitempty"`
	MaxKeys        int            `xml:"MaxKeys"`
	Delimiter      string         `xml:"Delimiter,omitempty"`
	IsTruncated    bool           `xml:"IsTruncated"`
	Contents       []listContent  `xml:"Contents"`
	CommonPrefixes []commonPrefix `xml:"CommonPrefixes"`
	EncodingType   string         `xml:"EncodingType,omitempty"`
}
