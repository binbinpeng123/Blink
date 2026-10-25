package s3api

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/binbinpeng123/Blink/internal/s3err"
	"github.com/binbinpeng123/Blink/internal/store"
)

// storageClassStandard 是本版本唯一支持的存储类型（02 文档）。
const storageClassStandard = "STANDARD"

// handleGetObject 处理 GET /{bucket}/{key}。
func (s *Server) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) {
	s.serveObject(w, r, bucket, key, reqID)
}

// handleHeadObject 处理 HEAD /{bucket}/{key}。
//
// 与 GET 共用同一套实现：两者的元数据、条件请求和 Range 语义完全一致，
// 唯一区别是 HEAD 不发送响应体。分开写两份必然会在某次修改后漂移。
func (s *Server) handleHeadObject(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) {
	s.serveObject(w, r, bucket, key, reqID)
}

// handleDeleteObject 处理 DELETE /{bucket}/{key}。
func (s *Server) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) {
	if err := s.store.DeleteObject(bucket, key); err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}
	// 02 文档：幂等，对象不存在也回 204，且无响应体。
	w.WriteHeader(http.StatusNoContent)
	loggerFrom(r.Context(), s.logger).Debug("object_deleted",
		slog.String("bucket", bucket),
		slog.String("key", key),
	)
}

// serveObject 是 GET 与 HEAD 的共同实现。
//
// 执行顺序刻意如此：
//
//	打开对象 → 条件请求 → 常规响应头 → Range → Content-Length → 状态码 → 响应体
//
// 条件请求必须在 Range 之前：304/412 的语义是"这次请求整体不成立"，
// 此时再解析 Range 既无意义，也会让 304 响应的头部形状不一致。
// Content-Length 必须在 WriteHeader 之前设置 —— 一旦状态码写出，
// 再设头部就无效了。
func (s *Server) serveObject(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) {
	logger := loggerFrom(r.Context(), s.logger)

	reader, err := s.store.OpenObject(bucket, key)
	if err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}
	info := reader.Info()

	notModified, condErr := evaluateConditions(r, info)
	if condErr != nil {
		condErr.Write(w, r, reqID)
		return
	}
	if notModified {
		// 304 不带响应体，但必须回 ETag 与 Last-Modified：
		// 客户端靠它们确认手里缓存的是哪个版本。
		h := w.Header()
		h.Set("ETag", quoteETag(info.ETag))
		h.Set("Last-Modified", httpTime(info.Modified))
		w.WriteHeader(http.StatusNotModified)
		logger.Debug("object_not_modified",
			slog.String("bucket", bucket),
			slog.String("key", key),
		)
		return
	}

	h := w.Header()
	setObjectHeaders(h, info)

	start, length, partial, rangeErr := objectRange(r, info)
	if rangeErr != nil {
		if rangeErr.Code == s3err.CodeInvalidRange {
			// 02 文档要求 416 带 Content-Range: bytes */<size>，
			// 让客户端知道对象的真实大小、从而修正下一次请求。
			h.Set("Content-Range", fmt.Sprintf("bytes */%d", info.Size))
		}
		rangeErr.Write(w, r, reqID)
		return
	}

	// 只有客户端明确要求时才回传校验和：头部不是免费的，
	// 常规下载不需要它，且 ChecksumMode 是客户端表达"我要核对"的方式。
	//
	// 部分响应（206）**不**回传校验和：存储的是整个对象的校验和，
	// 附在一段字节上会让按规范校验的客户端（如 aws-cli 的 Range 下载）
	// 判定"校验和不匹配"而报错。校验和只对完整对象有意义。
	if !partial && checksumModeEnabled(r) && info.ChecksumAlgorithm != "" {
		h.Set(info.ChecksumAlgorithm.Header(), info.Checksum)
		// checksum-type 必须一起回传：FULL_OBJECT 与 COMPOSITE 的校验值
		// 形状完全不同（后者带 `-N` 后缀），客户端不知道类型就无法复算。
		// aws-cli 的下载路径正是靠它决定"直接比对"还是"按分片复算"。
		if info.ChecksumType != "" {
			h.Set("x-amz-checksum-type", info.ChecksumType)
		}
	}

	// 显式设置 Content-Length，空对象也必须是 0 而不是省略：
	// 省略后 net/http 会改用 chunked，部分老客户端对空对象的 chunked
	// 响应处理有缺陷（02 文档）。
	h.Set("Content-Length", strconv.FormatInt(length, 10))

	status := http.StatusOK
	if partial {
		status = http.StatusPartialContent
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, info.Size))
	}
	w.WriteHeader(status)

	if r.Method == http.MethodHead || length == 0 {
		logger.Debug("object_head_or_empty",
			slog.String("bucket", bucket),
			slog.String("key", key),
			slog.Int64("size", info.Size),
			slog.Int("status", status),
		)
		return
	}

	if err := reader.WriteRangeTo(w, start, length); err != nil {
		// 响应头（含 Content-Length）已经发出，此时无法再改成错误状态码。
		// 只能记录日志：客户端会因收到的字节数与 Content-Length 不符而
		// 判定传输失败，这比静默截断可发现得多。
		logger.Error("object_read_failed",
			slog.String("bucket", bucket),
			slog.String("key", key),
			slog.Int64("offset", start),
			slog.Int64("length", length),
			slog.String("err", err.Error()),
		)
		return
	}

	logger.Debug("object_get",
		slog.String("bucket", bucket),
		slog.String("key", key),
		slog.Int("status", status),
		slog.Int64("offset", start),
		slog.Int64("length", length),
		slog.Int64("size", info.Size),
	)
}

// setObjectHeaders 写入 02 文档规定的对象响应头。
func setObjectHeaders(h http.Header, info store.ObjectInfo) {
	// ETag 是 quoted-string；不带引号会让客户端回传的 If-Match 永远失配。
	h.Set("ETag", quoteETag(info.ETag))
	h.Set("Last-Modified", httpTime(info.Modified))
	h.Set("Accept-Ranges", "bytes")
	h.Set("Content-Type", info.ContentType)
	h.Set("x-amz-storage-class", storageClassStandard)
	// 用户元数据必须用**全小写**的原始头名写进 map，不能走 h.Set：
	// h.Set 会把键规范成 X-Amz-Meta-Owner，而 aws-cli（botocore）是按
	// 原始大小写匹配 `x-amz-meta-` 前缀的，大写头会被解析成
	// Metadata.Owner 而不是 AWS 约定的 Metadata.owner —— 实测
	// `--query Metadata.owner` 直接得到 None。真实 S3 也返回小写头名。
	// 元数据键在存储里是 Go 规范化过的形式（Owner），这里一并转小写。
	for k, v := range info.UserMeta {
		h["x-amz-meta-"+strings.ToLower(k)] = []string{v}
	}
}

// quoteETag 给 ETag 加双引号。
func quoteETag(etag string) string { return `"` + etag + `"` }

// httpTime 按 RFC 1123（GMT）格式化 Last-Modified 等时间头。
func httpTime(t time.Time) string { return t.UTC().Format(http.TimeFormat) }

// checksumModeEnabled 判断客户端是否要求回传对象校验和。
func checksumModeEnabled(r *http.Request) bool {
	return strings.EqualFold(strings.TrimSpace(r.Header.Get("x-amz-checksum-mode")), "ENABLED")
}

// objectRange 计算本次响应要发送的字节区间。
//
// 返回 partial=false 表示发送整个对象（无 Range，或 If-Range 不成立）。
// If-Range 的作用是"只有对象没变才给部分内容"：对象已被覆盖时回退为
// 完整响应，避免客户端把新版本的一段拼到旧版本的缓存里。
func objectRange(r *http.Request, info store.ObjectInfo) (start, length int64, partial bool, e *s3err.Error) {
	spec := strings.TrimSpace(r.Header.Get("Range"))
	if spec == "" {
		return 0, info.Size, false, nil
	}
	if !ifRangeApplies(r.Header.Get("If-Range"), info) {
		return 0, info.Size, false, nil
	}
	start, length, e = parseRange(spec, info.Size)
	if e != nil {
		return 0, 0, false, e
	}
	return start, length, true, nil
}

// ifRangeApplies 判断 If-Range 是否仍然成立。
//
// 两种形式：实体标签或 HTTP 日期。日期比较用"对象修改时间不晚于它"，
// 与 RFC 9110 的强校验语义一致；标签必须是强匹配（弱标签一律视为不匹配）。
func ifRangeApplies(v string, info store.ObjectInfo) bool {
	v = strings.TrimSpace(v)
	if v == "" {
		return true
	}
	if t, ok := parseHTTPTime(v); ok {
		return !info.Modified.After(t)
	}
	if strings.HasPrefix(v, "W/") {
		return false
	}
	return strings.Trim(v, `"`) == info.ETag
}

// parseRange 解析单区间 Range 头。
//
// 支持 02 文档的三种形式：bytes=start-end、bytes=start-、bytes=-suffix。
// 终点越界按 RFC 9110 截断到对象末尾（不是错误）；起点越界才是 416。
// 多区间回 501 而不是"只满足第一个"：后者会让客户端以为拿到了全部区间，
// 却只拼出一部分内容，是比明确失败更糟的静默错误。
func parseRange(spec string, size int64) (start, length int64, e *s3err.Error) {
	invalid := func(msg string) (int64, int64, *s3err.Error) {
		return 0, 0, s3err.New(s3err.CodeInvalidRange).WithMessage(msg)
	}

	spec = strings.TrimSpace(spec)
	if !strings.HasPrefix(strings.ToLower(spec), "bytes=") {
		return invalid("只支持 `Range: bytes=...`。")
	}
	rest := strings.TrimSpace(spec[len("bytes="):])
	if strings.Contains(rest, ",") {
		return 0, 0, s3err.New(s3err.CodeNotImplemented).
			WithMessage("暂不支持多区间 Range 请求。")
	}
	first, last, ok := strings.Cut(rest, "-")
	if !ok {
		return invalid("Range 格式应为 bytes=start-end。")
	}
	first, last = strings.TrimSpace(first), strings.TrimSpace(last)

	if first == "" {
		// 后缀形式 bytes=-N：最后 N 个字节。N 超过对象大小时取整个对象。
		n, err := strconv.ParseInt(last, 10, 64)
		if err != nil || n <= 0 || size == 0 {
			return invalid("Range 后缀长度不合法或对象为空。")
		}
		if n > size {
			n = size
		}
		return size - n, n, nil
	}

	begin, err := strconv.ParseInt(first, 10, 64)
	if err != nil || begin < 0 {
		return invalid("Range 起点不合法。")
	}
	if begin >= size {
		return invalid(fmt.Sprintf("Range 起点 %d 超出对象大小 %d。", begin, size))
	}
	if last == "" {
		return begin, size - begin, nil
	}
	end, err := strconv.ParseInt(last, 10, 64)
	if err != nil || end < begin {
		return invalid("Range 终点不合法。")
	}
	if end >= size {
		end = size - 1
	}
	return begin, end - begin + 1, nil
}

// evaluateConditions 按 RFC 9110 §13.2.2 的优先级计算条件请求结果。
//
// 返回 notModified=true 表示应回 304；返回错误表示应回 412。
//
// 优先级（写错会让"同时带 If-Match 与 If-Unmodified-Since 的请求"给出
// 错误答复）：
//
//	If-Match   存在时，完全忽略 If-Unmodified-Since
//	If-None-Match 存在时，完全忽略 If-Modified-Since
//	日期头无法解析时忽略该条件（RFC 要求）
func evaluateConditions(r *http.Request, info store.ObjectInfo) (notModified bool, e *s3err.Error) {
	ifMatch := strings.TrimSpace(r.Header.Get("If-Match"))
	ifNoneMatch := strings.TrimSpace(r.Header.Get("If-None-Match"))

	if ifMatch != "" {
		if !etagListMatches(ifMatch, info.ETag) {
			return false, s3err.New(s3err.CodePreconditionFailed)
		}
	} else if t, ok := parseHTTPTime(r.Header.Get("If-Unmodified-Since")); ok {
		if info.Modified.After(t) {
			return false, s3err.New(s3err.CodePreconditionFailed)
		}
	}

	if ifNoneMatch != "" {
		if etagListMatches(ifNoneMatch, info.ETag) {
			return true, nil
		}
	} else if t, ok := parseHTTPTime(r.Header.Get("If-Modified-Since")); ok {
		if !info.Modified.After(t) {
			return true, nil
		}
	}
	return false, nil
}

// etagListMatches 判断头部值是否命中给定 ETag。
//
// 支持 `*`、逗号分隔列表、以及 W/ 弱标签（弱标签在 If-None-Match 下
// 是允许的）。比较时去掉引号：不同客户端对引号的处理并不一致，
// 而 ETag 本身是十六进制串，不存在歧义。
func etagListMatches(header, etag string) bool {
	if header == "*" {
		return true
	}
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(part)
		part = strings.TrimPrefix(part, "W/")
		if strings.Trim(part, `"`) == etag {
			return true
		}
	}
	return false
}

// parseHTTPTime 解析 HTTP 日期；不合法时 ok=false，调用方据此忽略该条件。
func parseHTTPTime(v string) (time.Time, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}, false
	}
	t, err := http.ParseTime(v)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}
