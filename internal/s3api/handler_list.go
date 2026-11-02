package s3api

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/binbinpeng123/Blink/internal/s3err"
	"github.com/binbinpeng123/Blink/internal/store"
)

// ListObjectsV2 的参数默认值与上限。02 文档规定两者均为 1000。
const (
	defaultMaxKeys = 1000
	maxMaxKeys     = 1000
)

// listParams 是解析后的 ListObjectsV2 请求参数。
type listParams struct {
	Prefix            string
	Delimiter         string
	StartAfter        string
	MaxKeys           int
	EncodingType      string
	FetchOwner        bool
	Cursor            *store.ListCursor
	ContinuationToken string
}

// handleListObjectsV2 处理 GET /{bucket}?list-type=2。
func (s *Server) handleListObjectsV2(w http.ResponseWriter, r *http.Request, bucket, reqID string) {
	p, perr := parseListParams(r)
	if perr != nil {
		perr.Write(w, r, reqID)
		return
	}

	res, err := s.store.ListObjects(bucket, store.ListOptions{
		Prefix:     p.Prefix,
		Delimiter:  p.Delimiter,
		StartAfter: p.StartAfter,
		Cursor:     p.Cursor,
		MaxKeys:    p.MaxKeys,
	})
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

	out := listBucketResult{
		Name:              bucket,
		Prefix:            enc(p.Prefix),
		Delimiter:         enc(p.Delimiter),
		MaxKeys:           p.MaxKeys,
		KeyCount:          res.KeyCount,
		IsTruncated:       res.IsTruncated,
		ContinuationToken: p.ContinuationToken,
		StartAfter:        enc(p.StartAfter),
		EncodingType:      p.EncodingType,
	}
	if res.Next != nil {
		out.NextContinuationToken = encodeListToken(*res.Next, p.Prefix, p.Delimiter)
	}
	for _, o := range res.Objects {
		c := listContent{
			Key:          enc(o.Key),
			LastModified: iso8601(o.Modified),
			ETag:         quoteETag(o.ETag),
			Size:         o.Size,
			StorageClass: storageClassStandard,
		}
		if p.FetchOwner {
			c.Owner = &ownerInfo{ID: ownerID, DisplayName: ownerDisplayName}
		}
		out.Contents = append(out.Contents, c)
	}
	for _, cp := range res.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, commonPrefix{Prefix: enc(cp)})
	}

	writeXMLBody(w, r, http.StatusOK, out)
	loggerFrom(r.Context(), s.logger).Debug("list_objects_v2",
		slog.String("bucket", bucket),
		slog.Int("key_count", res.KeyCount),
		slog.Bool("truncated", res.IsTruncated),
	)
}

// parseListParams 解析并校验查询参数。
//
// 未知参数一律忽略而不是 501：主流 SDK 会附加自己的标记参数
// （aws-sdk-js v3 每次请求都带 `x-id=ListObjectsV2`），
// 把它们当作"不支持的功能"会让整个 SDK 不可用。
// 02 文档"未知查询参数组合 → 501"指的是子资源（?acl、?tagging 之类），
// 那些由 router.go 的 unsupportedSubresources 统一拦截。
func parseListParams(r *http.Request) (*listParams, *s3err.Error) {
	q := r.URL.Query()
	p := &listParams{
		Prefix:     q.Get("prefix"),
		Delimiter:  q.Get("delimiter"),
		StartAfter: q.Get("start-after"),
		MaxKeys:    defaultMaxKeys,
	}

	if q.Has("max-keys") {
		n, err := strconv.Atoi(q.Get("max-keys"))
		if err != nil || n < 0 {
			return nil, invalidArg("max-keys 必须是非负整数。")
		}
		// 超出上限按上限截断，与 S3 行为一致 —— 报错会让
		// `--max-keys 5000` 这样的请求直接失败，而它本意就是"尽量多"。
		if n > maxMaxKeys {
			n = maxMaxKeys
		}
		p.MaxKeys = n
	}

	if q.Has("encoding-type") {
		v := q.Get("encoding-type")
		if v != "url" {
			return nil, invalidArg(`encoding-type 只支持 "url"。`)
		}
		p.EncodingType = v
	}

	if q.Has("fetch-owner") {
		switch strings.ToLower(q.Get("fetch-owner")) {
		case "true":
			p.FetchOwner = true
		case "false":
			p.FetchOwner = false
		default:
			return nil, invalidArg(`fetch-owner 只能是 "true" 或 "false"。`)
		}
	}

	if q.Has("continuation-token") {
		token := q.Get("continuation-token")
		cursor, tokPrefix, tokDelimiter, err := decodeListToken(token)
		if err != nil {
			return nil, invalidArg("continuation-token 无效。")
		}
		// token 里绑定 prefix/delimiter：用 A 前缀拿到的 token 去续 B 前缀，
		// 结果既不是 A 也不是 B 的列表。明确报错比返回一个语义不明的页好。
		if tokPrefix != p.Prefix || tokDelimiter != p.Delimiter {
			return nil, invalidArg("continuation-token 与本次请求的 prefix/delimiter 不一致。")
		}
		p.Cursor = cursor
		p.ContinuationToken = token
	}

	return p, nil
}

// invalidArg 构造 400 InvalidArgument。
func invalidArg(msg string) *s3err.Error {
	return s3err.New(s3err.CodeInvalidArgument).WithMessage(msg)
}

// listToken 是 continuation-token 的明文结构。
//
// 客户端**不得**依赖它的形状（02 文档：不透明值）；这里用 JSON 只是为了
// 调试时能被一眼看懂，而不是给客户端解析的。
type listToken struct {
	After     string `json:"after"`
	IsPrefix  bool   `json:"after_is_prefix"`
	Prefix    string `json:"prefix"`
	Delimiter string `json:"delimiter"`
}

// encodeListToken 生成下一页的 continuation-token。
//
// 用 RawURLEncoding 而不是标准 base64：标准字母表含 `+` 和 `/`，
// 其中 `+` 在查询串里会被解析成空格 —— 客户端只要漏一次百分号编码，
// 续页就永远失败，而且报错信息（"token 无效"）很难让人想到是 `+`。
func encodeListToken(c store.ListCursor, prefix, delimiter string) string {
	raw, err := json.Marshal(listToken{
		After:     c.After,
		IsPrefix:  c.IsPrefix,
		Prefix:    prefix,
		Delimiter: delimiter,
	})
	if err != nil {
		// 只有字符串与布尔字段，序列化不可能失败；失败即是编程错误。
		panic("s3api: continuation-token 序列化失败: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

// decodeListToken 解析 continuation-token。
//
// 同时接受标准与 URL 安全两种字母表（含/不含 padding）：
// token 是我们自己签发的，但客户端、代理、复制粘贴都可能把它转成另一种
// 变体；只要 JSON 能解出来、语义一致，就没有拒绝的理由。
func decodeListToken(s string) (*store.ListCursor, string, string, error) {
	var raw []byte
	decoded := false
	for _, enc := range []*base64.Encoding{
		base64.RawURLEncoding, base64.URLEncoding,
		base64.RawStdEncoding, base64.StdEncoding,
	} {
		if b, err := enc.DecodeString(s); err == nil {
			raw, decoded = b, true
			break
		}
	}
	if !decoded {
		return nil, "", "", errors.New("continuation-token 不是合法 base64")
	}
	var t listToken
	if err := json.Unmarshal(raw, &t); err != nil {
		return nil, "", "", err
	}
	if t.After == "" {
		return nil, "", "", errors.New("continuation-token 缺少续页位置")
	}
	return &store.ListCursor{After: t.After, IsPrefix: t.IsPrefix}, t.Prefix, t.Delimiter, nil
}

// s3URLEncode 按 S3 的 encoding-type=url 规则编码。
//
// url.QueryEscape 把空格编成 `+`，而 XML 里的值需要的是 `%20`；
// 其余（包括 `/` 编成 `%2F`）保持 QueryEscape 的行为，与 MinIO 一致 ——
// 这是被 AWS CLI 与主流 SDK 长期验证过的编码方式。
func s3URLEncode(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}

// listBucketResult 是 ListObjectsV2 的响应体。
//
// 字段顺序按 AWS 的响应习惯排列：头部元信息在前，Contents 与
// CommonPrefixes 在后。encoding/xml 按结构体字段顺序输出。
type listBucketResult struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	MaxKeys               int            `xml:"MaxKeys"`
	KeyCount              int            `xml:"KeyCount"`
	IsTruncated           bool           `xml:"IsTruncated"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
	EncodingType          string         `xml:"EncodingType,omitempty"`
	Contents              []listContent  `xml:"Contents"`
	CommonPrefixes        []commonPrefix `xml:"CommonPrefixes"`
}

// listContent 是单个对象条目。
type listContent struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
	// Owner 只在 fetch-owner=true 时出现；*ownerInfo 为 nil 时整体省略。
	Owner *ownerInfo `xml:"Owner,omitempty"`
}

// commonPrefix 是被 delimiter 折叠出的公共前缀。
type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}
