package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strconv"
	"time"

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
	pathBlock       = "/internal/v1/block"
	pathMeta        = "/internal/v1/meta"
	pathList        = "/internal/v1/list"
	pathBucket      = "/internal/v1/bucket"
	pathBucketState = "/internal/v1/bucket-status"
	pathBuckets     = "/internal/v1/buckets"
	pathCORS        = "/internal/v1/cors"

	// 分片上传（MPU）会话的四条路由。会话与数据分开搬运：
	// upload/part/parts 只碰 multipart 目录下的小 JSON，
	// 真正的分片数据走 pathBlock。
	pathMPUUpload  = "/internal/v1/mpu/upload"
	pathMPUPart    = "/internal/v1/mpu/part"
	pathMPUParts   = "/internal/v1/mpu/parts"
	pathMPUUploads = "/internal/v1/mpu/uploads"
)

// handleBlock 同时处理块的写入（POST）与读取（GET）。
func (s *Server) handleBlock(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.handleBlockPut(w, r)
	case http.MethodGet:
		s.handleBlockGet(w, r)
	case http.MethodHead:
		s.handleBlockHead(w, r)
	default:
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodHead+", "+http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, errorBody{Error: "method_not_allowed"})
	}
}

// handleBlockHead 只回答"这个块在不在"，不返回内容。
//
// 修复路径用它避免把完整的块搬一遍再判断：块按内容寻址，绝大多数
// 修复只需要确认目标有没有这个文件。HEAD 没有响应体，因此错误只能
// 通过状态码表达 —— 这里不接受请求体里的任何参数。
func (s *Server) handleBlockHead(w http.ResponseWriter, r *http.Request) {
	hash := r.URL.Query().Get("hash")
	ok, err := s.store.BlockExists(hash)
	if err != nil {
		// 哈希非法是 400；其余（含 IO 错误）一律 500，让调用方区分
		// "确定缺失"与"查不了"——前者可以复制，后者必须重试。
		if errors.Is(err, store.ErrInvalidBlock) {
			writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid_argument", Message: err.Error()})
			return
		}
		s.logger.Error("internal_block_stat_failed", "hash", hash, "err", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
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
	Found bool              `json:"found"`
	Meta  *store.ObjectMeta `json:"meta,omitempty"`
}

// listResponse 是 GET /internal/v1/list 的响应。
//
// Bucket 与 Entries 分开：「本节点没有这个桶」是正常响应（200 +
// bucket=false），不是故障；协调者据此判断 NoSuchBucket，而只有真正
// 的 I/O 错误才应让该节点退出法定人数统计。
type listResponse struct {
	Bucket  bool                 `json:"bucket"`
	Entries []store.ReplicaEntry `json:"entries,omitempty"`
}

// bucketInfo 是 GET /internal/v1/bucket 的响应。
type bucketInfo struct {
	Exists    bool      `json:"exists"`
	CreatedAt time.Time `json:"created_at,omitzero"`
}

// bucketState 是 GET /internal/v1/bucket-status 的响应。
//
// Empty 只在 Exists=true 时有意义：不存在的桶没有"空不空"可言。
type bucketState struct {
	Exists bool `json:"exists"`
	Empty  bool `json:"empty"`
}

// bucketsResponse 是 GET /internal/v1/buckets 的响应。
type bucketsResponse struct {
	Buckets []string `json:"buckets"`
}

// corsResponse 是 GET /internal/v1/cors 的响应。
//
// Found=false 表示"桶在但没有配置"，与 404 "桶不存在" 是两件事。
type corsResponse struct {
	Found  bool                     `json:"found"`
	Config *store.CORSConfiguration `json:"config,omitempty"`
}

// handleList 返回本节点持有的、桶内全部副本条目（**含墓碑**）。
//
// 返回墓碑而不是过滤掉：同一个 key 在 A 上已删除、在 B 上还残留旧副本时，
// 协调者必须看到墓碑才能判定最终状态是"不存在"。过滤只能在全集群归并后做。
func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJSON(w, http.StatusMethodNotAllowed, errorBody{Error: "method_not_allowed"})
		return
	}
	bucket := r.URL.Query().Get("bucket")
	if err := store.ValidateBucketName(bucket); err != nil {
		s.writeStoreError(w, "校验桶名", err)
		return
	}
	exists, err := s.store.BucketExists(bucket)
	if err != nil {
		s.writeStoreError(w, "检查桶", err)
		return
	}
	if !exists {
		writeJSON(w, http.StatusOK, listResponse{})
		return
	}
	entries, err := s.store.ScanReplicaEntries(bucket)
	if err != nil {
		s.writeStoreError(w, "扫描桶", err)
		return
	}
	writeJSON(w, http.StatusOK, listResponse{Bucket: true, Entries: entries})
}

// handleBucket 同时处理桶的查询、创建与删除。
//
// PUT/DELETE 都是幂等的：广播会被重试，节点也可能收到重复请求。
// "已经存在"与"本来就没有"在控制面上都是成功 —— 真正需要报错的是
// "桶非空"和 I/O 失败。
func (s *Server) handleBucket(w http.ResponseWriter, r *http.Request) {
	bucket := r.URL.Query().Get("bucket")
	switch r.Method {
	case http.MethodGet:
		if err := store.ValidateBucketName(bucket); err != nil {
			s.writeStoreError(w, "校验桶名", err)
			return
		}
		created, err := s.store.BucketCreatedAt(bucket)
		if errors.Is(err, store.ErrBucketNotFound) {
			writeJSON(w, http.StatusOK, bucketInfo{})
			return
		}
		if err != nil {
			s.writeStoreError(w, "读取桶元数据", err)
			return
		}
		writeJSON(w, http.StatusOK, bucketInfo{Exists: true, CreatedAt: created})
	case http.MethodPut:
		if err := s.store.CreateBucket(bucket); err != nil && !errors.Is(err, store.ErrBucketExists) {
			s.writeStoreError(w, "创建桶", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	case http.MethodDelete:
		if err := s.store.DeleteBucket(bucket); err != nil && !errors.Is(err, store.ErrBucketNotFound) {
			s.writeStoreError(w, "删除桶", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	default:
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodPut+", "+http.MethodDelete)
		writeJSON(w, http.StatusMethodNotAllowed, errorBody{Error: "method_not_allowed"})
	}
}

// handleBucketState 返回桶是否存在于本节点、以及本节点视角下是否为空。
func (s *Server) handleBucketState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJSON(w, http.StatusMethodNotAllowed, errorBody{Error: "method_not_allowed"})
		return
	}
	bucket := r.URL.Query().Get("bucket")
	exists, err := s.store.BucketExists(bucket)
	if err != nil {
		s.writeStoreError(w, "检查桶", err)
		return
	}
	if !exists {
		writeJSON(w, http.StatusOK, bucketState{})
		return
	}
	empty, err := s.store.BucketIsEmpty(bucket)
	if err != nil {
		s.writeStoreError(w, "检查桶是否为空", err)
		return
	}
	writeJSON(w, http.StatusOK, bucketState{Exists: true, Empty: empty})
}

// handleBuckets 返回本节点的桶名列表。
func (s *Server) handleBuckets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJSON(w, http.StatusMethodNotAllowed, errorBody{Error: "method_not_allowed"})
		return
	}
	names, err := s.store.ListBuckets()
	if err != nil {
		s.writeStoreError(w, "列举桶", err)
		return
	}
	writeJSON(w, http.StatusOK, bucketsResponse{Buckets: names})
}

// handleCORS 同时处理桶级 CORS 配置的读取、写入与删除。
func (s *Server) handleCORS(w http.ResponseWriter, r *http.Request) {
	bucket := r.URL.Query().Get("bucket")
	switch r.Method {
	case http.MethodGet:
		cfg, err := s.store.GetBucketCORS(bucket)
		switch {
		case errors.Is(err, store.ErrNoSuchCORSConfiguration):
			writeJSON(w, http.StatusOK, corsResponse{})
		case err != nil:
			s.writeStoreError(w, "读取 CORS 配置", err)
		default:
			writeJSON(w, http.StatusOK, corsResponse{Found: true, Config: cfg})
		}
	case http.MethodPut:
		body, err := io.ReadAll(io.LimitReader(r.Body, MaxRequestBody+1))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, errorBody{Error: "read_failed"})
			return
		}
		if len(body) > MaxRequestBody {
			writeJSON(w, http.StatusRequestEntityTooLarge, errorBody{Error: "request_too_large"})
			return
		}
		var cfg store.CORSConfiguration
		if err := json.Unmarshal(body, &cfg); err != nil {
			writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid_cors", Message: "不是合法 JSON"})
			return
		}
		if err := s.store.PutBucketCORS(bucket, &cfg); err != nil {
			s.writeStoreError(w, "写入 CORS 配置", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	case http.MethodDelete:
		if err := s.store.DeleteBucketCORS(bucket); err != nil {
			s.writeStoreError(w, "删除 CORS 配置", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	default:
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodPut+", "+http.MethodDelete)
		writeJSON(w, http.StatusMethodNotAllowed, errorBody{Error: "method_not_allowed"})
	}
}

// handleMPUUpload 处理会话的读取、提交与删除。
//
// GET 对"没有这个会话"回 200 + found=false，理由与 handleMetaGet 相同：
// 协调者必须把"副本没参与这次会话"与"节点不可达"分开统计，否则一次
// 部分失败的 Create 会让后续每次 UploadPart 都看成 503。
func (s *Server) handleMPUUpload(w http.ResponseWriter, r *http.Request) {
	uploadID := r.URL.Query().Get("uploadId")
	switch r.Method {
	case http.MethodGet:
		st, err := s.store.LoadUploadState(uploadID)
		switch {
		case errors.Is(err, store.ErrNoSuchUpload):
			writeJSON(w, http.StatusOK, uploadStateResponse{})
		case err != nil:
			s.writeStoreError(w, "读取分片上传会话", err)
		default:
			writeJSON(w, http.StatusOK, uploadStateResponse{Found: true, State: st})
		}
	case http.MethodPut:
		body, ok := readInternalBody(w, r, "invalid_upload_state")
		if !ok {
			return
		}
		var st store.UploadState
		if err := json.Unmarshal(body, &st); err != nil {
			writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid_upload_state", Message: "不是合法 JSON"})
			return
		}
		if st.UploadID != uploadID {
			writeJSON(w, http.StatusBadRequest, errorBody{
				Error:   "upload_id_mismatch",
				Message: fmt.Sprintf("state.upload_id=%q 与请求 uploadId=%q 不符", st.UploadID, uploadID),
			})
			return
		}
		if err := s.store.PutUploadState(&st); err != nil {
			s.writeStoreError(w, "提交分片上传会话", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	case http.MethodDelete:
		found, err := s.store.DeleteUploadState(uploadID)
		if err != nil {
			s.writeStoreError(w, "删除分片上传会话", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"found": found})
	default:
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodPut+", "+http.MethodDelete)
		writeJSON(w, http.StatusMethodNotAllowed, errorBody{Error: "method_not_allowed"})
	}
}

// handleMPUPart 提交单个分片的状态（块表），不是分片数据本身。
func (s *Server) handleMPUPart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		w.Header().Set("Allow", http.MethodPut)
		writeJSON(w, http.StatusMethodNotAllowed, errorBody{Error: "method_not_allowed"})
		return
	}
	q := r.URL.Query()
	uploadID := q.Get("uploadId")
	partNumber, err := parseIntParam(q.Get("partNumber"), 0)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid_part_number", Message: err.Error()})
		return
	}

	body, ok := readInternalBody(w, r, "invalid_part_state")
	if !ok {
		return
	}
	var ps store.PartState
	if err := json.Unmarshal(body, &ps); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid_part_state", Message: "不是合法 JSON"})
		return
	}
	// body 与 query 必须一致：两者都来自同一个协调者，对不上说明请求
	// 在传输或拼装时已经错了，此时写盘只会污染状态。
	if int64(ps.PartNumber) != partNumber {
		writeJSON(w, http.StatusBadRequest, errorBody{
			Error:   "part_number_mismatch",
			Message: fmt.Sprintf("state.part_number=%d 与请求 partNumber=%d 不符", ps.PartNumber, partNumber),
		})
		return
	}
	if err := s.store.PutPartState(uploadID, &ps); err != nil {
		s.writeStoreError(w, "提交分片状态", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleMPUParts 返回本节点上某次会话的全部分片状态。
//
// 会话不在本节点时回空列表而不是 404：这不是故障，只是本节点没参与。
// 协调者已从 S 确认会话存在，缺副本由合并逻辑自然处理。
func (s *Server) handleMPUParts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJSON(w, http.StatusMethodNotAllowed, errorBody{Error: "method_not_allowed"})
		return
	}
	parts, err := s.store.ListPartStates(r.URL.Query().Get("uploadId"))
	if errors.Is(err, store.ErrNoSuchUpload) {
		writeJSON(w, http.StatusOK, partStatesResponse{})
		return
	}
	if err != nil {
		s.writeStoreError(w, "列举分片状态", err)
		return
	}
	writeJSON(w, http.StatusOK, partStatesResponse{Parts: parts})
}

// handleMPUUploads 返回本节点上属于某桶的进行中会话。
func (s *Server) handleMPUUploads(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJSON(w, http.StatusMethodNotAllowed, errorBody{Error: "method_not_allowed"})
		return
	}
	uploads, err := s.store.ListUploadStates(r.URL.Query().Get("bucket"))
	if err != nil {
		s.writeStoreError(w, "列举分片上传会话", err)
		return
	}
	writeJSON(w, http.StatusOK, uploadStatesResponse{Uploads: uploads})
}

// readInternalBody 读取一个受限长度的内部请求体。
func readInternalBody(w http.ResponseWriter, r *http.Request, code string) ([]byte, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxRequestBody+1))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "read_failed"})
		return nil, false
	}
	if len(body) > MaxRequestBody {
		writeJSON(w, http.StatusRequestEntityTooLarge, errorBody{Error: "request_too_large", Message: code})
		return nil, false
	}
	return body, true
}

// uploadStateResponse 是 GET /internal/v1/mpu/upload 的响应。
type uploadStateResponse struct {
	Found bool               `json:"found"`
	State *store.UploadState `json:"state,omitempty"`
}

// partStatesResponse 是 GET /internal/v1/mpu/parts 的响应。
type partStatesResponse struct {
	Parts []*store.PartState `json:"parts,omitempty"`
}

// uploadStatesResponse 是 GET /internal/v1/mpu/uploads 的响应。
type uploadStatesResponse struct {
	Uploads []*store.UploadState `json:"uploads,omitempty"`
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
//	ErrNoSuchUpload           → 404  会话不存在（副本没参与，不是故障）
//	ErrInvalidPart 及其一族    → 400  分片参数错
//	ErrPreconditionFailed     → 412  条件写不成立
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
	case errors.Is(err, store.ErrBucketNotFound):
		// 必须排在 fs.ErrNotExist 之前？两类错误互不包装，但桶级错误
		// 需要自己的 code 让协调者区分"桶不存在"与"对象不存在"。
		writeJSON(w, http.StatusNotFound, errorBody{Error: "bucket_not_found", Message: err.Error()})
	case errors.Is(err, store.ErrBucketExists):
		writeJSON(w, http.StatusConflict, errorBody{Error: "bucket_exists", Message: err.Error()})
	case errors.Is(err, store.ErrBucketNotEmpty):
		// 409 而不是 400：桶非空是**当前状态**不允许删除，
		// 协调者清空对象后重试就能成功，不是请求本身写错了。
		writeJSON(w, http.StatusConflict, errorBody{Error: "bucket_not_empty", Message: err.Error()})
	case errors.Is(err, store.ErrNoSuchUpload):
		writeJSON(w, http.StatusNotFound, errorBody{Error: "no_such_upload", Message: err.Error()})
	case errors.Is(err, store.ErrInvalidPart),
		errors.Is(err, store.ErrInvalidPartNumber),
		errors.Is(err, store.ErrInvalidPartOrder),
		errors.Is(err, store.ErrEntityTooSmall),
		errors.Is(err, store.ErrInvalidChecksum),
		errors.Is(err, store.ErrBadDigest),
		errors.Is(err, store.ErrIncompleteBody):
		// 分片参数错误全部落到 400 一族：协调者会把它们原样回给客户端，
		// 不会被误当成"节点故障"而触发重试。
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid_part", Message: err.Error()})
	case errors.Is(err, store.ErrPreconditionFailed):
		writeJSON(w, http.StatusPreconditionFailed, errorBody{Error: "precondition_failed", Message: err.Error()})
	case errors.Is(err, fs.ErrNotExist):
		writeJSON(w, http.StatusNotFound, errorBody{Error: "not_found", Message: err.Error()})
	case errors.Is(err, store.ErrCorrupt):
		s.logger.Error("internal_store_corrupt", "op", op, "err", err.Error())
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "corrupt", Message: err.Error()})
	case errors.Is(err, store.ErrStorageFull):
		// 507 让协调者把"容量不足"与"节点故障"分开：前者重试无用。
		writeJSON(w, http.StatusInsufficientStorage, errorBody{Error: "storage_full", Message: err.Error()})
	case errors.Is(err, store.ErrUnavailable):
		writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "unavailable", Message: err.Error()})
	default:
		s.logger.Error("internal_store_error", "op", op, "err", err.Error())
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "internal_error", Message: err.Error()})
	}
}
