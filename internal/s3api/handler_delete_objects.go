package s3api

import (
	"crypto/md5"
	"crypto/subtle"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"

	"github.com/binbinpeng123/Blink/internal/authz"
	"github.com/binbinpeng123/Blink/internal/s3err"
)

// DeleteObjects 的硬性上限（与 S3 一致）。
const (
	// maxDeleteObjects 是单次请求允许的 key 数。
	maxDeleteObjects = 1000
	// maxDeleteBody 是请求体上限：1000 个 key 的 XML 在这种量级，
	// 1 MiB 足够容纳超长 key（S3 单 key 上限 1024 字节），
	// 又能挡住"用请求体把内存吃光"的玩法。
	maxDeleteBody = 1 << 20
	// deleteWorkers 是并行删除的 goroutine 上限。
	//
	// 8 是一个刻意保守的数字：批量删除的瓶颈通常是磁盘的随机元数据操作，
	// 再高的并发只会把 APFS/ext4 的提交队列压满，让单次延迟变长而总吞吐
	// 不变。不同 key 才并行；同一个 key 上的并发布由存储层的 key 锁串行化。
	deleteWorkers = 8
)

type deleteObjectsRequest struct {
	XMLName xml.Name              `xml:"Delete"`
	Objects []deleteObjectRequest `xml:"Object"`
	Quiet   bool                  `xml:"Quiet"`
}

type deleteObjectRequest struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId"`
}

// deleteObjectsResult 是 `POST /{bucket}?delete` 的响应体。
//
// 分组输出（Deleted 与 Error 各自成表）是 S3 的既定形状；组内顺序
// 与请求顺序一致，保证同一请求重放时客户端能对齐结果。
type deleteObjectsResult struct {
	XMLName xml.Name        `xml:"DeleteResult"`
	Deleted []deletedObject `xml:"Deleted"`
	Errors  []deleteError   `xml:"Error"`
}

type deletedObject struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId,omitempty"`
}

type deleteError struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// handleDeleteObjects 处理 POST /{bucket}?delete。
//
// 语义与 S3 对齐：**部分成功是正常结果**，不是错误。单条 key 的失败
// 只出现在响应的 Error 表里，HTTP 状态码仍是 200 —— 客户端按条目
// 决定是否重试。整体 4xx/5xx 只用于"这次请求本身不成立"
// （XML 坏、key 太多、桶不存在）。
func (s *Server) handleDeleteObjects(w http.ResponseWriter, r *http.Request, bucket, reqID string) {
	// 多读 1 字节用于区分"恰好等于上限"与"超过上限"，
	// 否则 1 MiB 整的合法请求会被误判。
	body, err := io.ReadAll(io.LimitReader(r.Body, maxDeleteBody+1))
	if err != nil {
		s3err.New(s3err.CodeMalformedXML).
			WithMessage("读取请求体失败。").
			Write(w, r, reqID)
		return
	}
	if len(body) > maxDeleteBody {
		s3err.New(s3err.CodeMalformedXML).
			WithMessage("请求体超过 1 MiB 上限。").
			Write(w, r, reqID)
		return
	}

	// Content-MD5 是可选的（S3 对 DeleteObjects 也是可选项），但存在时
	// 必须校验：它是客户端唯一能确认"服务端收到的 XML 就是我发的"的手段。
	if v := r.Header.Get("Content-MD5"); v != "" {
		raw, decErr := base64.StdEncoding.DecodeString(v)
		if decErr != nil || len(raw) != md5.Size {
			s3err.New(s3err.CodeInvalidDigest).Write(w, r, reqID)
			return
		}
		sum := md5.Sum(body)
		if subtle.ConstantTimeCompare(raw, sum[:]) != 1 {
			s3err.New(s3err.CodeBadDigest).
				WithMessage("Content-MD5 与请求体不符。").
				Write(w, r, reqID)
			return
		}
	}

	var req deleteObjectsRequest
	if err := xml.Unmarshal(body, &req); err != nil {
		s3err.New(s3err.CodeMalformedXML).Write(w, r, reqID)
		return
	}
	if len(req.Objects) == 0 || len(req.Objects) > maxDeleteObjects {
		s3err.New(s3err.CodeMalformedXML).
			WithMessage("一次请求必须包含 1–1000 个 Object。").
			Write(w, r, reqID)
		return
	}
	for _, o := range req.Objects {
		if o.Key == "" {
			s3err.New(s3err.CodeMalformedXML).
				WithMessage("Object 的 Key 不能为空。").
				Write(w, r, reqID)
			return
		}
		// 没有版本控制时，VersionId 不能"当作没看见"：客户端要求删的是
		// 某个历史版本，而我们会删掉当前版本 —— 结果与请求不符。
		if o.VersionID != "" {
			s3err.New(s3err.CodeNotImplemented).
				WithMessage("本版本不支持版本控制，DeleteObjects 不能带 VersionId。").
				Write(w, r, reqID)
			return
		}
	}

	// 桶不存在是整请求级错误，不能伪装成 1000 条逐项 NoSuchBucket：
	// 后者会让客户端逐条重试，而重试永远不会成功。
	exists, err := s.store.BucketExists(bucket)
	if err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}
	if !exists {
		s3err.New(s3err.CodeNoSuchBucket).Write(w, r, reqID)
		return
	}

	// 结果按下标写回，天然保持请求顺序且没有数据竞争。
	errs := make([]error, len(req.Objects))
	idx := make(chan int)
	var wg sync.WaitGroup
	for i := 0; i < deleteWorkers && i < len(req.Objects); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range idx {
				key := req.Objects[j].Key
				// 请求级只检查了桶，这里逐 key 再查：一条策略通常是
				// "某前缀可删"，批量请求完全可能混入前缀外的 key。
				// 拒绝项作为该 key 的独立结果返回，不影响同批其他 key。
				if !s.accessAllowed(r, authz.ActionDelete, bucket, key) {
					errs[j] = errAccessDenied
					continue
				}
				errs[j] = s.store.DeleteObject(bucket, key)
			}
		}()
	}
	for i := range req.Objects {
		idx <- i
	}
	close(idx)
	wg.Wait()

	out := deleteObjectsResult{}
	for i, o := range req.Objects {
		if errs[i] != nil {
			// 无权限不是存储错误，toS3Error 会把它当成未知错误回 500；
			// 这里单独映射为 AccessDenied，与单对象 DELETE 的答复一致。
			if errors.Is(errs[i], errAccessDenied) {
				se := s3err.New(s3err.CodeAccessDenied)
				out.Errors = append(out.Errors, deleteError{
					Key:     o.Key,
					Code:    string(se.Code),
					Message: se.Message,
				})
				continue
			}
			// 单条错误用与普通 DELETE 相同的翻译，客户端看到的 Code
			// 不会因为走了批量接口而变样。
			se := toS3Error(errs[i])
			out.Errors = append(out.Errors, deleteError{
				Key:     o.Key,
				Code:    string(se.Code),
				Message: se.Message,
			})
			continue
		}
		// Quiet 模式只回错误项：aws s3 rm --quiet 依赖它来减少响应体积。
		if !req.Quiet {
			out.Deleted = append(out.Deleted, deletedObject{Key: o.Key})
		}
	}

	writeXMLBody(w, r, http.StatusOK, out)
	loggerFrom(r.Context(), s.logger).Debug("delete_objects",
		slog.String("bucket", bucket),
		slog.Int("requested", len(req.Objects)),
		slog.Int("deleted", len(out.Deleted)),
		slog.Int("errors", len(out.Errors)),
		slog.Bool("quiet", req.Quiet),
	)
}
