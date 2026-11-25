package s3api

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/binbinpeng123/Blink/internal/s3err"
)

// createBucketXML 是 PUT /{bucket} 的可选请求体。
//
// 本版本不支持任何桶级配置，因此除 LocationConstraint 外的字段一律忽略，
// 但**不报错** —— 主流客户端（aws-cli、各语言 SDK）都会发送带
// LocationConstraint 的请求体，拒绝它会让 `aws s3 mb` 直接不可用。
type createBucketXML struct {
	LocationConstraint string `xml:"LocationConstraint"`
}

// handleCreateBucket 处理 PUT /{bucket}。
func (s *Server) handleCreateBucket(w http.ResponseWriter, r *http.Request, bucket, reqID string) {
	// ACL、Object Lock 启用、对象所有权与默认加密都只能通过请求头声明，
	// 本版本均未实现：静默忽略会让客户端以为桶已按私有/合规策略创建。
	if rejectUnsupportedHeaders(w, r, reqID, "建桶请求", unsupportedBucketWriteHeaders) {
		return
	}

	// 请求体可有可无。空体（Content-Length: 0）是合法的：
	// 部分客户端不发送 LocationConstraint，视为 us-east-1。
	if r.ContentLength > 0 {
		var body createBucketXML
		if err := xml.NewDecoder(r.Body).Decode(&body); err != nil {
			s3err.New(s3err.CodeMalformedXML).Write(w, r, reqID)
			return
		}
	}

	if err := s.store.CreateBucket(bucket); err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}

	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
	loggerFrom(r.Context(), s.logger).Debug("bucket_created", slog.String("bucket", bucket))
}

// handleDeleteBucket 处理 DELETE /{bucket}。
func (s *Server) handleDeleteBucket(w http.ResponseWriter, r *http.Request, bucket, reqID string) {
	if err := s.store.DeleteBucket(bucket); err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}
	// S3 规定 DeleteBucket 成功返回 204，且无响应体。
	w.WriteHeader(http.StatusNoContent)
	loggerFrom(r.Context(), s.logger).Debug("bucket_deleted", slog.String("bucket", bucket))
}

// handleHeadBucket 处理 HEAD /{bucket}。
//
// HEAD 没有响应体，客户端据此判断桶是否存在与所属区域，
// 因此 x-amz-bucket-region 必须给出 —— 部分 SDK 用它决定后续请求去哪个区域。
func (s *Server) handleHeadBucket(w http.ResponseWriter, r *http.Request, bucket, reqID string) {
	ok, err := s.store.BucketExists(bucket)
	if err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}
	if !ok {
		s3err.New(s3err.CodeNoSuchBucket).Write(w, r, reqID)
		return
	}
	w.Header().Set("x-amz-bucket-region", s.region)
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
}

// dispatchBucketLocation 处理桶级 `?location` 子资源。
//
// 返回 true 表示请求已被处理（成功或已写出错误响应），调用方必须立即返回。
//
// 必须早于按方法分派：`GET /{bucket}?location` 与 ListObjects V1 共用
// 同一路径，`PUT`/`DELETE` 则会命中 CreateBucket/DeleteBucket。
// 不拦截的后果不是"少一个 API"，而是把一次区域查询变成创建或删除桶。
func (s *Server) dispatchBucketLocation(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) bool {
	if !r.URL.Query().Has("location") {
		return false
	}
	// `/{bucket}/{key}?location` 在 S3 里没有定义，明确 501，
	// 不能让它落到对象读写。
	if bucket == "" || key != "" {
		notImplemented("?location 只支持桶级请求：GET /{bucket}?location。").
			Write(w, r, reqID)
		return true
	}
	// 只有 GetBucketLocation 合法。非 GET 回 405 而不是 501：
	// 子资源存在，只是方法不对（与 ?cors 的约定一致）。
	if r.Method != http.MethodGet {
		s3err.New(s3err.CodeMethodNotAllowed).Write(w, r, reqID)
		return true
	}
	if !s.requireStore(w, r, reqID) {
		return true
	}
	s.handleGetBucketLocation(w, r, bucket, reqID)
	return true
}

// handleGetBucketLocation 处理 GET /{bucket}?location。
//
// 本版本不支持桶级独立区域，`CreateBucket` 收到 LocationConstraint
// 也不落盘，因此这里统一返回服务端配置的区域。AWS 的语义是
// us-east-1 对应**空**元素，其余区域返回区域名；照此返回，
// 让 SDK 的 "create bucket in us-east-1" 判定与真实 S3 一致。
func (s *Server) handleGetBucketLocation(w http.ResponseWriter, r *http.Request, bucket, reqID string) {
	ok, err := s.store.BucketExists(bucket)
	if err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}
	if !ok {
		s3err.New(s3err.CodeNoSuchBucket).Write(w, r, reqID)
		return
	}

	region := s.region
	if region == "us-east-1" {
		region = ""
	}
	writeXMLBody(w, r, http.StatusOK, locationConstraint{Value: region})
	loggerFrom(r.Context(), s.logger).Debug("bucket_location",
		slog.String("bucket", bucket),
		slog.String("region", s.region),
	)
}

// locationConstraint 是 GetBucketLocation 的响应体。
//
// 命名空间必须写进标签：客户端（含 aws-sdk-go 的 LocationConstraint
// 解析）按带 xmlns 的根元素解包，缺了它部分 SDK 会判为无法识别。
type locationConstraint struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LocationConstraint"`
	Value   string   `xml:",chardata"`
}

// handleListBuckets 处理 GET /（根路径，无 bucket 名）。
//
// 本版本在切片 3 就实现它，而不是等到切片 6：`aws s3 ls` 不带参数时
// 走的就是 ListBuckets，而它是验收"桶生命周期跑通"最直接的手段 ——
// 没有它就只能靠 `aws s3api head-bucket` 逐个确认。
//
// 分页参数（AWS 2024-11 加入）：
//
//   - prefix：只返回名字以它开头的桶。
//   - bucket-region：只返回该区域的桶。本服务只服务一个区域，指定其它
//     区域得到空结果，而不是忽略参数。
//   - max-buckets：本页最多条数，默认 10000。
//   - continuation-token：上一页返回的续页位置，绑定 prefix 与 region。
//
// 分页边界必须在**可见性过滤之后**：凭证看不到的桶不占额度、也不该
// 出现在续页位置里，否则客户端会为了几条永远拿不到的桶反复翻页。
func (s *Server) handleListBuckets(w http.ResponseWriter, r *http.Request, reqID string) {
	p, perr := parseBucketListParams(r)
	if perr != nil {
		perr.Write(w, r, reqID)
		return
	}

	names, err := s.store.ListBuckets()
	if err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}
	out := listAllMyBucketsResult{Prefix: p.Prefix}
	// 指定了别的区域：本服务只服务一个区域，结果必然为空。
	// 空区域名（未指定）与 s.region 都表示"不按区域过滤"。
	regionMatches := p.Region == "" || p.Region == s.region
	if regionMatches {
		for _, name := range names {
			if p.Prefix != "" && !strings.HasPrefix(name, p.Prefix) {
				continue
			}
			if p.After != "" && name <= p.After {
				continue
			}
			if !s.bucketVisible(r, name) {
				continue
			}
			created, err := s.store.BucketCreatedAt(name)
			if err != nil {
				// 桶在列举与读取之间被删除：跳过即可，不该让整个 LIST 失败。
				continue
			}
			// 额度已满且又遇到一条**可见**桶，才说明还有下一页；
			// 先判可见性再判额度，避免为一个看不见的桶发续页 token。
			if len(out.Buckets.Bucket) >= p.MaxBuckets {
				last := out.Buckets.Bucket[len(out.Buckets.Bucket)-1].Name
				out.ContinuationToken = encodeBucketListToken(last, p.Prefix, p.Region)
				break
			}
			out.Buckets.Bucket = append(out.Buckets.Bucket, bucketEntry{
				Name:         name,
				CreationDate: iso8601(created),
				BucketRegion: s.region,
			})
		}
	}
	out.Owner = ownerInfo{ID: ownerID, DisplayName: ownerDisplayName}
	writeXMLBody(w, r, http.StatusOK, out)
}

// ListBuckets 分页参数的默认值与上限（AWS 契约）。
const (
	defaultMaxBuckets = 10000
	maxMaxBuckets     = 10000
)

// bucketListParams 是解析后的 ListBuckets 请求参数。
type bucketListParams struct {
	Prefix     string
	Region     string
	MaxBuckets int
	// After 是解码后的续页位置（严格大于该名字）。
	After string
}

// parseBucketListParams 解析 ListBuckets 的查询参数。
func parseBucketListParams(r *http.Request) (*bucketListParams, *s3err.Error) {
	q := r.URL.Query()
	p := &bucketListParams{
		Prefix:     q.Get("prefix"),
		Region:     q.Get("bucket-region"),
		MaxBuckets: defaultMaxBuckets,
	}
	if q.Has("max-buckets") {
		n, err := strconv.Atoi(q.Get("max-buckets"))
		// 与 max-keys 不同，0 不是合法值：S3 的 max-buckets 下限是 1，
		// 而 max-keys=0 有"只取 KeyCount"的明确语义。
		if err != nil || n < 1 {
			return nil, invalidArg("max-buckets 必须是 1 到 10000 之间的整数。")
		}
		// 超出上限按上限截断，与 max-keys 一致：客户端写 100000 的
		// 本意是"尽量多"，报错只会让一次列举整体失败。
		if n > maxMaxBuckets {
			n = maxMaxBuckets
		}
		p.MaxBuckets = n
	}
	if q.Has("continuation-token") {
		token := q.Get("continuation-token")
		after, tokPrefix, tokRegion, err := decodeBucketListToken(token)
		if err != nil {
			return nil, invalidArg("continuation-token 无效。")
		}
		// token 绑定 prefix/region：拿 A 的结果去续 B，既不完整也不是 B。
		if tokPrefix != p.Prefix || tokRegion != p.Region {
			return nil, invalidArg("continuation-token 与本次请求的 prefix/bucket-region 不一致。")
		}
		p.After = after
	}
	return p, nil
}

// bucketListToken 是 ListBuckets continuation-token 的明文结构。
//
// 与 listToken 一样，客户端不得依赖形状；用 JSON 只为排障时一眼能看懂。
// 与对象列举不同，这里只记"最后返回的桶名"：桶列表没有公共前缀折叠，
// 位置即可完整描述下一页。
type bucketListToken struct {
	After  string `json:"after"`
	Prefix string `json:"prefix"`
	Region string `json:"region"`
}

// encodeBucketListToken 生成下一页的 continuation-token。
//
// 用 RawURLEncoding 的理由同 encodeListToken：标准字母表里的 `+` 在
// 查询串中会被当成空格，客户端漏一次百分号编码就永远翻不了页。
func encodeBucketListToken(after, prefix, region string) string {
	raw, err := json.Marshal(bucketListToken{After: after, Prefix: prefix, Region: region})
	if err != nil {
		// 只有字符串字段，序列化不可能失败；失败即是编程错误。
		panic("s3api: ListBuckets continuation-token 序列化失败: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

// decodeBucketListToken 解析 ListBuckets 的 continuation-token。
func decodeBucketListToken(s string) (after, prefix, region string, err error) {
	raw, err := decodeBase64Token(s)
	if err != nil {
		return "", "", "", err
	}
	var t bucketListToken
	if err := json.Unmarshal(raw, &t); err != nil {
		return "", "", "", err
	}
	if t.After == "" {
		return "", "", "", errors.New("continuation-token 缺少续页位置")
	}
	return t.After, t.Prefix, t.Region, nil
}

// S3 返回的 Owner 字段。
//
// 单节点单租户没有真实账号体系，但大量客户端会解析该字段，
// 缺失会导致解码报错，因此给出固定占位值。
const (
	ownerID          = "blink"
	ownerDisplayName = "blink"
)

// listAllMyBucketsResult 是 ListBuckets 的响应体。
//
// ContinuationToken 是**下一页**的续页位置：只有结果被截断且后面确实还有
// 可见桶时才出现。它不能用来回显请求里的 token —— 客户端普遍把"响应给了
// token"当作"还有下一页"，在最后一页回显会使其无限翻页。
//
// Prefix 回显请求的过滤前缀（omitempty）：无参请求看到的响应与旧版 S3
// 完全一致。
type listAllMyBucketsResult struct {
	XMLName xml.Name  `xml:"ListAllMyBucketsResult"`
	Owner   ownerInfo `xml:"Owner"`
	Buckets struct {
		Bucket []bucketEntry `xml:"Bucket"`
	} `xml:"Buckets"`
	ContinuationToken string `xml:"ContinuationToken,omitempty"`
	Prefix            string `xml:"Prefix,omitempty"`
}

// ownerInfo 是资源归属信息。
type ownerInfo struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

// bucketEntry 是 ListBuckets 中的单个桶。
//
// BucketRegion 自 AWS 2024-11 起出现在每条记录上：多区域客户端靠它
// 决定后续请求打向哪个端点，缺了它部分 SDK 会回退到默认区域。
type bucketEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
	BucketRegion string `xml:"BucketRegion"`
}
