package store

import (
	"io"
	"time"
)

// Reader 是一个已打开对象的只读视图。
//
// 只暴露协议层真正需要的两个动作：读元数据快照、按字节区间写出。
// 之所以是接口而不是 *ObjectReader：集群模式下对象可能由多个节点上的
// 块拼成，读取器由 cluster 包自己实现，s3api 不需要知道数据来自本地
// 还是远端。
type Reader interface {
	// Info 返回打开时的元数据快照。
	Info() ObjectInfo
	// WriteRangeTo 把 [offset, offset+length) 字节写入 w。
	// 调用方必须先按 Info().Size 校验范围。
	WriteRangeTo(w io.Writer, offset, length int64) error
}

// Backend 是 s3api 依赖的全部存储能力。
//
// 接口放在 store 包，与参数、结果类型同处一处：s3api 与 cluster 都只
// 依赖本包，不必各自再声明一份形状相同的接口。方法集与 s3api 的实际
// 调用一一对应，不预留"将来可能用到"的方法 —— 多一个方法，集群实现
// 就多一份必须正确维护的语义。
//
// *Store 直接满足该接口；单机路径只是把具体类型换成接口，不改变任何
// 实现，也不引入额外 goroutine 或监听端口。
type Backend interface {
	// 桶生命周期。
	CreateBucket(name string) error
	DeleteBucket(name string) error
	BucketExists(name string) (bool, error)
	ListBuckets() ([]string, error)
	BucketCreatedAt(name string) (time.Time, error)

	// 桶级 CORS 配置。
	PutBucketCORS(bucket string, cfg *CORSConfiguration) error
	GetBucketCORS(bucket string) (*CORSConfiguration, error)
	DeleteBucketCORS(bucket string) error

	// 对象读写。
	PutObject(bucket, key string, body io.Reader, opt PutOptions) (*ObjectInfo, error)
	OpenObject(bucket, key string) (Reader, error)
	DeleteObject(bucket, key string) error
	ListObjects(bucket string, opt ListOptions) (*ListResult, error)

	// 分片上传。
	CreateMultipartUpload(bucket, key string, opt CreateMultipartOptions) (*MultipartUpload, error)
	UploadPart(bucket, key, uploadID string, partNumber int, body io.Reader, opt UploadPartOptions) (*PartInfo, error)
	CompleteMultipartUpload(bucket, key, uploadID string, parts []CompletePart, declaredChecksum string, ifNoneMatchAny bool) (*ObjectInfo, error)
	AbortMultipartUpload(bucket, key, uploadID string) error
	ListParts(bucket, key, uploadID string, opt ListPartsOptions) (*ListPartsResult, error)
	ListMultipartUploads(bucket string, opt ListMultipartUploadsOptions) (*ListMultipartUploadsResult, error)
}

// 编译期确认单机实现满足接口：签名一旦漂移，构建立即失败，
// 而不是等到 main 里接线时才发现。
var _ Backend = (*Store)(nil)
