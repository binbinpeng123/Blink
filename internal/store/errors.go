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
	// ErrCorrupt 表示磁盘上的元数据或数据块损坏。
	//
	// 与"不存在"严格区分：损坏必须报错，绝不能回落到"对象不存在" ——
	// 那会让用户以为数据本来就没了，从而放弃排查。
	ErrCorrupt = errors.New("store: 数据损坏")
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
