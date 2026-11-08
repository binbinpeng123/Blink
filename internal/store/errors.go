package store

import "errors"

// 本包的哨兵错误。
//
// 刻意不复用 s3err：存储层不应该知道 HTTP 或 S3 错误码的存在，
// 否则"换一个协议前端"就要改存储层。由 internal/s3api 负责映射。
var (
	// ErrBucketNotFound 表示 bucket 不存在。
	ErrBucketNotFound = errors.New("store: bucket 不存在")
	// ErrBucketExists 表示 bucket 已存在。
	ErrBucketExists = errors.New("store: bucket 已存在")
	// ErrBucketNotEmpty 表示 bucket 内仍有对象，不能删除。
	ErrBucketNotEmpty = errors.New("store: bucket 非空")
	// ErrInvalidBucketName 表示 bucket 名不符合命名规则。
	ErrInvalidBucketName = errors.New("store: bucket 名不合法")
	// ErrInvalidKey 表示对象 key 不合法（为空、超长等）。
	ErrInvalidKey = errors.New("store: 对象 key 不合法")
	// ErrNoSuchKey 表示对象不存在。
	//
	// 与 ErrBucketNotFound 分开：S3 对这两者有不同错误码
	// （NoSuchKey 与 NoSuchBucket），客户端据此区分"桶没建"与"文件没传"。
	ErrNoSuchKey = errors.New("store: 对象不存在")
	// ErrPreconditionFailed 表示条件请求未满足（如 If-None-Match: *）。
	ErrPreconditionFailed = errors.New("store: 前置条件未满足")
	// ErrBadDigest 表示客户端声明的校验和与实际内容不符。
	//
	// 这是**数据完整性**信号，不是格式问题：出现它意味着传输链路损坏了内容，
	// 因此必须让写入失败，绝不能"照存不误"。
	ErrBadDigest = errors.New("store: 校验和不匹配")
	// ErrIncompleteBody 表示请求体字节数与 Content-Length 不符。
	ErrIncompleteBody = errors.New("store: 请求体不完整")
	// ErrInvalidChecksum 表示调用方指定了本包不认识的校验和算法。
	//
	// 与 ErrBadDigest 区分：这里是"根本没算过"（参数问题），
	// 那里是"算了但对不上"（数据完整性问题）。
	ErrInvalidChecksum = errors.New("store: 校验和算法不受支持")
	// ErrCorrupt 表示磁盘上的元数据或数据块损坏。
	//
	// 与"不存在"严格区分：损坏必须报错，绝不能回落到"对象不存在" ——
	// 那会让用户以为数据本来就没了，从而放弃排查。
	ErrCorrupt = errors.New("store: 数据损坏")
	// ErrNoSuchUpload 表示分片上传不存在（已 Abort、已 Complete 或从未创建）。
	ErrNoSuchUpload = errors.New("store: 分片上传不存在")
	// ErrInvalidPart 表示 Complete 引用了不存在的分片，或 ETag 与之不符。
	//
	// 两者共用一个哨兵是 S3 的契约：错误码都是 InvalidPart，
	// 客户端据此重传对应分片，而区分"没有"与"内容变了"对它没有意义。
	ErrInvalidPart = errors.New("store: 分片不存在或 ETag 不匹配")
	// ErrInvalidPartOrder 表示 Complete 的分片列表未按 partNumber 严格升序。
	ErrInvalidPartOrder = errors.New("store: 分片顺序不合法")
	// ErrEntityTooSmall 表示非末片小于 5 MiB。
	ErrEntityTooSmall = errors.New("store: 非末尾分片过小")
	// ErrInvalidPartNumber 表示 partNumber 不在 1–10000 之间。
	ErrInvalidPartNumber = errors.New("store: 分片编号不合法")
	// ErrNoSuchCORSConfiguration 表示桶存在但没有配置 CORS。
	//
	// 与 ErrBucketNotFound 分开：S3 的 GetBucketCors 对这两种情况
	// 分别返回 NoSuchCORSConfiguration 与 NoSuchBucket，客户端据此
	// 区分"桶没建"与"桶没配跨域"。
	ErrNoSuchCORSConfiguration = errors.New("store: 桶未配置 CORS")
	// ErrStaleMeta 表示副本拒绝了一个不比现有版本更新的元数据。
	//
	// 这是分布式写路径的乱序保护：两个协调者并发写同一个 key 时，
	// 网络延迟可能让旧版本后到；副本按 (obj_version, node_id) 拒绝它，
	// 保证所有副本最终收敛到同一个版本，而不是"最后到达者获胜"。
	ErrStaleMeta = errors.New("store: 副本元数据版本过旧")
	// ErrUnavailable 表示后端暂时无法满足一致性要求（副本不足、法定人数不足）。
	//
	// 与 ErrCorrupt 严格区分：前者可以通过重试恢复，后者必须运维介入。
	// 协议层据此返回 503 而不是 500。
	ErrUnavailable = errors.New("store: 后端暂时不可用")
	// ErrInvalidBlock 表示块哈希格式非法或块内容为空。
	//
	// 与 ErrCorrupt 分开：这是**调用方给错了参数**，不是盘上的数据坏了。
	// CAS 路径直接用哈希的前两位做目录名，因此哈希必须先过格式校验，
	// 否则短字符串会让路径截取 panic。
	ErrInvalidBlock = errors.New("store: 数据块参数不合法")
	// ErrInvalidRequest 表示请求在语义上不成立（例如自复制却要求 COPY
	// 元数据、复制超过 5 GiB 的源对象）。
	//
	// 与 ErrInvalidKey 分开：key 本身合法，被拒绝的是这次操作的组合。
	// 协议层据此返回 400 InvalidRequest。
	ErrInvalidRequest = errors.New("store: 请求语义不合法")
	// ErrStorageFull 表示磁盘使用率已达高水位，写入被主动拒绝。
	//
	// 与 syscall.ENOSPC 区分：这是**预防性**拒绝（高水位），ENOSPC 是
	// 内核已经写不进去。协议层把两者都映射为 507 InsufficientStorage，
	// 但对内可以据此区分"该扩容了"与"已经满了"。
	ErrStorageFull = errors.New("store: 磁盘使用率已达高水位")
	// ErrNotImplemented 表示当前存储后端不具备完成该操作的能力。
	//
	// 目前唯一的用途是单机路径拒绝复制纠删码布局的对象：EC 分片只存在于
	// 集群的多个节点上，单机 meta 直接引用块表会指向不存在的分片。
	// 协议层据此返回 501 NotImplemented，而不是把它伪装成 404/500。
	ErrNotImplemented = errors.New("store: 当前后端不支持该操作")
)

// fsErr 把文件系统错误包装成带上下文的错误。
//
// 保留原始 error 以便 errors.Is/As 判断（尤其 syscall.ENOSPC）。
func fsErr(op, path string, err error) error {
	if err == nil {
		return nil
	}
	return &pathError{op: op, path: path, err: err}
}

type pathError struct {
	op   string
	path string
	err  error
}

func (e *pathError) Error() string { return e.op + " " + e.path + ": " + e.err.Error() }
func (e *pathError) Unwrap() error { return e.err }
