package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strconv"

	"github.com/binbinpeng123/Blink/internal/store"
)

// 本文件实现节点间搬运数据块与对象元数据的内部接口。
//
// 协议只搬运**存储格式里的东西**，不搬运命令：块按内容寻址、meta 按版本
// 比较，因此每个接口天然幂等，重试与重放都不会把数据带偏。
//
// 所有定位参数都走 query（不是路径段）：key 可以包含 `/` 与前导 `/`，
// 放进路径会被 chi/net/http 的路径规范化改写；query 由签名原样覆盖，
// 语义最清楚。

const (
	pathBlock = "/internal/v1/block"
	pathMeta  = "/internal/v1/meta"
)

// handleBlock 同时处理块的写入（POST）与读取（GET）。
func (s *Server) handleBlock(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.handleBlockPut(w, r)
	case http.MethodGet:
		s.handleBlockGet(w, r)
	default:
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, errorBody{Error: "method_not_allowed"})
	}
}

// handleBlockPut 接收一个完整的内容块。
//
// 必须重算 SHA-256 并与声明的 hash 比对：PutBlock 为了省掉重复计算
// 不校验内容，本地调用方给的是刚算出的哈希，可以信任；网络来的一侧
// 没有任何理由被信任 —— 一条损坏的链路足以把错误内容写进 CAS，
// 而 CAS 是"内容即身份"，写错了就再没有自愈机会。
func (s *Server) handleBlockPut(w http.ResponseWriter, r *http.Request) {
	hash := r.URL.Query().Get("hash")

	data, err := io.ReadAll(io.LimitReader(r.Body, MaxRequestBody+1))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "read_failed"})
		return
	}
	if len(data) > MaxRequestBody {
		writeJSON(w, http.StatusRequestEntityTooLarge, errorBody{Error: "request_too_large"})
		return
	}

	if got := store.BlockHash(data); got != hash {
		writeJSON(w, http.StatusBadRequest, errorBody{
			Error:   "hash_mismatch",
			Message: fmt.Sprintf("声明 %s，实得 %s", hash, got),
		})
		return
	}

	wrote, err := s.store.PutBlock(hash, data)
	if err != nil {
		s.writeStoreError(w, "写入块", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"deduped": !wrote})
}

// handleBlockGet 流式返回块的 [offset, offset+length) 字节。
//
// 先取块长度再设响应头：客户端据此判断内容是否被截断。范围校验也在
// 写响应体之前完成，因此越界能干净地回 416，而不是发一个半截 200。
func (s *Server) handleBlockGet(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	hash := q.Get("hash")

	// 块表记录的长度与 CRC32C 由协调者带过来：对端据此执行与本地读
	// 相同的完整性校验，而不是拿自己磁盘上的长度自证清白。
	// 两者都是必填：缺省时无法判断"文件短了"是不是损坏，只能当作可信，
	// 而内部协议两端由同一个二进制实现，没有"老版本客户端"需要兼容。
	size, err := s.store.BlockSize(hash)
	if err != nil {
		s.writeStoreError(w, "读取块", err)
		return
	}
	declared, err := parseIntParam(q.Get("blockLength"), -1)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid_block_length", Message: err.Error()})
		return
	}
	if declared <= 0 {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid_block_length", Message: "缺少有效的 blockLength"})
		return
	}
	if declared != size {
		writeJSON(w, http.StatusConflict, errorBody{
			Error:   "block_length_mismatch",
			Message: fmt.Sprintf("块表记录 %d 字节，本地文件 %d 字节", declared, size),
		})
		return
	}
	crc, err := parseCRC32C(q.Get("crc"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid_crc", Message: err.Error()})
		return
	}

	offset, err := parseIntParam(q.Get("offset"), 0)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid_offset", Message: err.Error()})
		return
	}
	length, err := parseIntParam(q.Get("length"), size-offset)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid_length", Message: err.Error()})
		return
	}
	if offset < 0 || length < 0 || offset > size || length > size-offset {
		writeJSON(w, http.StatusRequestedRangeNotSatisfiable, errorBody{
			Error:   "range_not_satisfiable",
			Message: fmt.Sprintf("范围 [%d,%d) 超出块长度 %d", offset, offset+length, size),
		})
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	b := store.BlockDesc{Hash: hash, Length: declared, CRC32C: crc}
	if err := s.store.ReadBlockRange(w, b, offset, length); err != nil {
		// 响应头已经发出，能做的只有记日志：客户端会因 Content-Length
		// 与实收字节不符而发现传输不完整。
		s.logger.Error("internal_block_read_failed",
			"hash", hash, "offset", offset, "length", length, "err", err.Error())
	}
}

// handleMeta 同时处理元数据的读取、提交与删除。
func (s *Server) handleMeta(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleMetaGet(w, r)
	case http.MethodPut:
		s.handleMetaPut(w, r)
	case http.MethodDelete:
		s.handleMetaDelete(w, r)
	default:
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodPut+", "+http.MethodDelete)
		writeJSON(w, http.StatusMethodNotAllowed, errorBody{Error: "method_not_allowed"})
	}
}

// handleMetaGet 返回某对象的副本元数据。
//
// "没有副本"是正常结果（found=false，200），不是 404：读路径要把它与
// "节点不可达"区分开，前者计入"已响应但不含该对象"，后者计入失败。
func (s *Server) handleMetaGet(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	bucket, key := q.Get("bucket"), q.Get("key")

	m, found, err := s.store.LoadReplicaMeta(bucket, key)
	if err != nil {
		s.writeStoreError(w, "读取副本元数据", err)
		return
	}
	writeJSON(w, http.StatusOK, metaResponse{Found: found, Meta: m})
}

// handleMetaPut 提交一份副本元数据。
//
// 版本过旧回 409 而不是错误码模糊的 500：协调者据此知道"另一次写入
// 已提交更新的版本"，这是可以继续的事实，不是故障。
func (s *Server) handleMetaPut(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	bucket, key := q.Get("bucket"), q.Get("key")

	body, err := io.ReadAll(io.LimitReader(r.Body, MaxRequestBody+1))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "read_failed"})
		return
	}
	if len(body) > MaxRequestBody {
		writeJSON(w, http.StatusRequestEntityTooLarge, errorBody{Error: "request_too_large"})
		return
	}

	var m store.ObjectMeta
	if err := json.Unmarshal(body, &m); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid_meta", Message: "不是合法 JSON"})
		return
	}
	if m.Key != key {
		writeJSON(w, http.StatusBadRequest, errorBody{
			Error:   "key_mismatch",
			Message: fmt.Sprintf("meta.key=%q 与请求 key=%q 不符", m.Key, key),
		})
		return
	}

	// 必须传指针：PutReplicaMeta 按 (ObjVersion, NodeID) 比较，值类型
	// 会在这里被复制一次，语义上没有问题但会多一份块表副本。
	if err := s.store.PutReplicaMeta(bucket, key, &m); err != nil {
		s.writeStoreError(w, "提交副本元数据", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleMetaDelete 删除副本，返回删除前是否存在。
func (s *Server) handleMetaDelete(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	bucket, key := q.Get("bucket"), q.Get("key")

	found, err := s.store.DeleteReplicaMeta(bucket, key)
	if err != nil {
		s.writeStoreError(w, "删除副本元数据", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"found": found})
}

// metaResponse 是 GET /internal/v1/meta 的响应。
//
// Found 与 Meta 必须同时传递：meta 为 nil 但 found=true 在协议上不可能
// 出现，写出两个字段让调用方无需猜测。
type metaResponse struct {
	Found bool             `json:"found"`
	Meta  *store.ObjectMeta `json:"meta,omitempty"`
}

// parseIntParam 解析可选的整数 query 参数，缺省时返回 def。
func parseIntParam(raw string, def int64) (int64, error) {
	if raw == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q 不是整数", raw)
	}
	return n, nil
}

// parseCRC32C 解析块表里的十六进制 CRC32C。
//
// 空串不是"未提供"，而是协议错误：CRC 为 0 是合法值，用 0 表示缺省会
// 让"忘记传参"伪装成"校验通过"，在 verifyOnRead 下静默失效。
func parseCRC32C(raw string) (uint32, error) {
	if raw == "" {
		return 0, errors.New("缺少 crc")
	}
	n, err := strconv.ParseUint(raw, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("%q 不是 32 位十六进制", raw)
	}
	return uint32(n), nil
}

// writeStoreError 把存储层错误翻译成内部协议状态码。
//
// 映射规则：
//
//	ErrInvalidBlock/Bucket/Key → 400  调用方参数错
//	ErrStaleMeta              → 409  版本冲突（可继续，不是故障）
//	fs.ErrNotExist            → 404  块/副本不存在
//	ErrCorrupt                → 500  盘上数据坏了，必须运维介入
//	ErrUnavailable            → 503  暂时性失败，可重试
//	其余                      → 500
//
// 内部端口不把 err.Error() 之外的信息回给对端；错误文本只含哈希与 key，
// 不含密钥或凭据。
func (s *Server) writeStoreError(w http.ResponseWriter, op string, err error) {
	switch {
	case errors.Is(err, store.ErrInvalidBlock),
		errors.Is(err, store.ErrInvalidBucketName),
		errors.Is(err, store.ErrInvalidKey):
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid_argument", Message: err.Error()})
	case errors.Is(err, store.ErrStaleMeta):
		writeJSON(w, http.StatusConflict, errorBody{Error: "stale_meta", Message: err.Error()})
	case errors.Is(err, fs.ErrNotExist):
		writeJSON(w, http.StatusNotFound, errorBody{Error: "not_found", Message: err.Error()})
	case errors.Is(err, store.ErrCorrupt):
		s.logger.Error("internal_store_corrupt", "op", op, "err", err.Error())
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "corrupt", Message: err.Error()})
	case errors.Is(err, store.ErrUnavailable):
		writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "unavailable", Message: err.Error()})
	default:
		s.logger.Error("internal_store_error", "op", op, "err", err.Error())
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "internal_error", Message: err.Error()})
	}
}
