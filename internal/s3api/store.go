package s3api

import (
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"syscall"
	"time"

	"github.com/binbinpeng123/Blink/internal/s3err"
	"github.com/binbinpeng123/Blink/internal/store"
)

// toS3Error 把存储层错误翻译成 S3 错误码。
//
// 这一层存在的意义：store 只懂"桶不存在""非空"这类领域概念，
// S3 错误码是协议细节。把翻译集中在一个函数里，好处是
// **可以一眼看全"哪些存储错误会让客户端看到什么"**，
// 而不必在几十个 handler 里四处翻找。
func toS3Error(err error) *s3err.Error {
	if err == nil {
		return nil
	}
	var serr *s3err.Error
	if errors.As(err, &serr) {
		return serr
	}

	switch {
	case errors.Is(err, store.ErrBucketNotFound):
		return s3err.New(s3err.CodeNoSuchBucket)
	case errors.Is(err, store.ErrBucketExists):
		// 单节点单租户：没有"他人拥有"的情形，因此一律回
		// BucketAlreadyOwnedByYou —— 这也是 `aws s3 mb` 期望看到的。
		return s3err.New(s3err.CodeBucketAlreadyOwnedByYou)
	case errors.Is(err, store.ErrBucketNotEmpty):
		return s3err.New(s3err.CodeBucketNotEmpty)
	case errors.Is(err, store.ErrInvalidBucketName):
		return s3err.New(s3err.CodeInvalidBucketName)
	case errors.Is(err, store.ErrInvalidKey):
		return s3err.New(s3err.CodeInvalidArgument).WithMessage("对象 key 不合法。")
	case errors.Is(err, store.ErrNoSuchKey):
		return s3err.New(s3err.CodeNoSuchKey)
	case errors.Is(err, store.ErrPreconditionFailed):
		return s3err.New(s3err.CodePreconditionFailed)
	case errors.Is(err, store.ErrBadDigest):
		// BadDigest 是**数据传输损坏**的信号：客户端声明的校验和与实际
		// 收到的内容不符。用 400 而不是 500 —— 错的不是服务端，
		// 但这绝不能被重试逻辑当成"稍后再试"（那只会重复坏数据）。
		return s3err.New(s3err.CodeBadDigest).WithMessage(err.Error())
	case errors.Is(err, store.ErrIncompleteBody):
		// 02 文档的错误码表里没有 IncompleteBody，因此归到 InvalidArgument。
		return s3err.New(s3err.CodeInvalidArgument).WithMessage(err.Error())
	case errors.Is(err, store.ErrInvalidChecksum):
		return s3err.New(s3err.CodeInvalidArgument).WithMessage(err.Error())
	case errors.Is(err, store.ErrNoSuchUpload):
		// 已 Abort、已 Complete 或从未创建，对客户端是同一种状况：
		// 这个 uploadId 已经不能用了。
		return s3err.New(s3err.CodeNoSuchUpload)
	case errors.Is(err, store.ErrInvalidPart):
		return s3err.New(s3err.CodeInvalidPart).WithMessage(err.Error())
	case errors.Is(err, store.ErrInvalidPartOrder):
		return s3err.New(s3err.CodeInvalidPartOrder).WithMessage(err.Error())
	case errors.Is(err, store.ErrEntityTooSmall):
		return s3err.New(s3err.CodeEntityTooSmall).WithMessage(err.Error())
	case errors.Is(err, store.ErrInvalidPartNumber):
		// 02 文档的错误码表里没有专门的编号错误码，归到 InvalidArgument。
		return s3err.New(s3err.CodeInvalidArgument).WithMessage(err.Error())
	case errors.Is(err, store.ErrNoSuchCORSConfiguration):
		// 桶存在但没配跨域：与"桶不存在"是两个可区分的状况。
		return s3err.New(s3err.CodeNoSuchCORSConfiguration)
	case errors.Is(err, store.ErrCorrupt):
		// 损坏绝不伪装成 404：用户必须知道自己该去查盘，而不是以为对象本来就不存在。
		return s3err.New(s3err.CodeInternalError).
			WithMessage("存储层数据损坏：" + err.Error())
	case errors.Is(err, syscall.ENOSPC):
		// 磁盘写满：507 InsufficientStorage。
		//
		// 这是 S3 兼容实现的主流语义：MinIO 返回 507 XMinioStorageFull，
		// Dell ECS/ObjectScale 返回 507 InsufficientStorage。
		// AWS 的 503 SlowDown 表示**请求限流**（"Please reduce your
		// request rate"），不是容量耗尽，继续用它会让客户端误以为
		// "稍后重试就能成功"，从而掩盖需要运维介入的满盘故障。
		//
		// 507 仍是 5xx，SDK 的退避重试依然生效；但错误码明确指向
		// "扩容或删除数据"，而不是"降低请求速率"。
		return s3err.New(s3err.CodeInsufficientStorage).WithMessage("磁盘空间不足。")
	case errors.Is(err, fs.ErrPermission):
		return s3err.New(s3err.CodeAccessDenied).WithMessage("数据目录权限不足。")
	}
	return s3err.New(s3err.CodeInternalError)
}

// noSuchBucketIfMissing 把"读取时桶目录不存在"统一归为 NoSuchBucket。
//
// 存在的理由：桶元数据缺失与桶目录缺失是两个不同的 errno，
// 但对客户端而言都是"这个桶不存在"，不该出现两种答复。
func noSuchBucketIfMissing(err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return store.ErrBucketNotFound
	}
	return err
}

// writeStoreErr 把存储层错误翻译后写给客户端，并记录一条服务端日志。
//
// 日志在 5xx 时用 Error 级别、4xx 用 Debug —— 4xx 是客户端用法问题，
// 让它淹没在 Error 日志里会让真正的故障被噪声掩盖。
func (s *Server) writeStoreErr(w http.ResponseWriter, r *http.Request, reqID string, err error) {
	serr := toS3Error(err)
	level := slog.LevelDebug
	if serr.HTTPStatus >= http.StatusInternalServerError {
		level = slog.LevelError
	}
	loggerFrom(r.Context(), s.logger).Log(r.Context(), level, "store_error",
		slog.String("code", string(serr.Code)),
		slog.Int("http_status", serr.HTTPStatus),
		slog.String("path", r.URL.Path),
		slog.String("err", err.Error()),
	)
	serr.Write(w, r, reqID)
}

// iso8601 按 S3 要求格式化时间：2006-01-02T15:04:05.000Z。
//
// 毫秒精度是必须的：部分 SDK 会按这个格式做严格解析，
// 省略毫秒会得到解析失败而不是被容错。
func iso8601(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}
