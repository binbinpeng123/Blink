package s3api

import (
	"encoding/xml"
	"log/slog"
	"net/http"

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
func (s *Server) handleListBuckets(w http.ResponseWriter, r *http.Request, reqID string) {
	names, err := s.store.ListBuckets()
	if err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}
	out := listAllMyBucketsResult{}
	for _, name := range names {
		// ListBuckets 没有单一动作可归类，按"该凭据是否有任何规则
		// 能让这个桶产生可见性"过滤：看到不存在的桶比"看不到有权限的桶"
		// 更糟 —— 前者会让客户端去探测一个它永远访问不了的资源。
		if !s.bucketVisible(r, name) {
			continue
		}
		created, err := s.store.BucketCreatedAt(name)
		if err != nil {
			// 桶在列举与读取之间被删除：跳过即可，不该让整个 LIST 失败。
			continue
		}
		out.Buckets.Bucket = append(out.Buckets.Bucket, bucketEntry{
			Name:         name,
			CreationDate: iso8601(created),
		})
	}
	out.Owner = ownerInfo{ID: ownerID, DisplayName: ownerDisplayName}
	writeXMLBody(w, r, http.StatusOK, out)
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
type listAllMyBucketsResult struct {
	XMLName xml.Name  `xml:"ListAllMyBucketsResult"`
	Owner   ownerInfo `xml:"Owner"`
	Buckets struct {
		Bucket []bucketEntry `xml:"Bucket"`
	} `xml:"Buckets"`
}

// ownerInfo 是资源归属信息。
type ownerInfo struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

// bucketEntry 是 ListBuckets 中的单个桶。
type bucketEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}
