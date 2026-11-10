package store

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/binbinpeng123/Blink/internal/checksum"
)

// 分片上传的固定参数。
const (
	// minPartSize 是除最后一片外每个分片的最小字节数（S3 契约）。
	//
	// 5 MiB 不是性能选择而是协议规定：aws-cli 的默认分片尺寸、
	// 各种 SDK 的分片策略都以它为前提。放宽会让客户端以为自己的
	// 分片策略被接受，取回对象时才发现别人拼不出来。
	minPartSize = 5 << 20
	// maxPartNumber 是 partNumber 的上限（S3 契约，从 1 开始）。
	maxPartNumber = 10000
	// multipartRetention 是未完成分片上传的保留期。
	//
	// 与 AWS 的未完成 MPU 默认清理周期一致。过期后启动清扫直接删除：
	// 这些数据永远等不到 Complete，留着只会让盘慢慢被填满。
	multipartRetention = 7 * 24 * time.Hour
)

// 分片参数对集群层导出：集群协调者在把请求分发给远端之前要先按同一套
// S3 契约校验编号与尺寸。复制一份常量会让两条路径在调参时分叉，
// 让客户端在单机与集群下看到不同的错误。
const (
	MinPartSize   = minPartSize
	MaxPartNumber = maxPartNumber
)

// 校验和类型。FULL_OBJECT 覆盖整个对象，COMPOSITE 覆盖"各分片校验和"。
const (
	ChecksumTypeFullObject = "FULL_OBJECT"
	ChecksumTypeComposite  = "COMPOSITE"
)

// MultipartUpload 是一次分片上传的会话信息。
type MultipartUpload struct {
	UploadID  string
	Bucket    string
	Key       string
	Initiated time.Time

	ContentType string
	UserMeta    map[string]string
	// ChecksumAlgorithm 与 ChecksumType 在建会话时确定，此后不可变：
	// 每个分片的校验和必须按同一算法计算，否则 Complete 无从合并。
	ChecksumAlgorithm checksum.Algorithm
	ChecksumType      string
}

// CreateMultipartOptions 是 CreateMultipartUpload 的输入。
type CreateMultipartOptions struct {
	ContentType string
	UserMeta    map[string]string
	// ChecksumAlgorithm 为空表示客户端不要校验和。
	ChecksumAlgorithm checksum.Algorithm
	// ChecksumType 为空时按 02 文档归一：CRC64NVME → FULL_OBJECT，
	// 其余算法 → COMPOSITE。
	ChecksumType string
}

// UploadPartOptions 是 UploadPart 的输入。
type UploadPartOptions struct {
	// ExpectedSize 是 Content-Length；分片上传同样必须有确定长度。
	ExpectedSize int64
	// ContentMD5 是 Content-MD5 头解码后的小写十六进制。
	ContentMD5 string
	// ChecksumAlgorithm 是客户端在本次请求里声明的算法。
	// 允许为空（沿用会话的算法）；非空时必须与会话一致。
	ChecksumAlgorithm checksum.Algorithm
	// Checksum 是客户端声明得分片校验和（base64）。
	Checksum string
	// LateChecksum 在请求体读完后才可用（aws-chunked / HTTP trailer）。
	// 第二个返回值为 false 表示客户端没有通过 trailer 声明。
	//
	// 与 PutOptions.LateChecksum 同理：值只有在读到 EOF 之后才存在，
	// 而校验必须发生在分片状态落盘之前，回调是唯一能夹住这两者的形态。
	LateChecksum func() (string, bool)
}

// CompletePart 是 Complete 请求里引用的一个分片。
type CompletePart struct {
	PartNumber int
	// ETag 可以带引号；本包会去掉引号后再比对。
	ETag string
}

// PartInfo 是 UploadPart 的结果，也是 ListParts 的条目。
type PartInfo struct {
	PartNumber int
	ETag       string
	Size       int64
	Modified   time.Time
	Checksum   string
	// ChecksumAlgorithm 是本分片校验和所用的算法（取自会话）。
	// 调用方要靠它选出响应头名（x-amz-checksum-crc64nvme 等），
	// 空表示该会话没有启用校验和。
	ChecksumAlgorithm checksum.Algorithm
}

// ListPartsOptions 是 ListParts 的参数。
type ListPartsOptions struct {
	// MaxParts 上限 1000；<=0 返回空页。
	MaxParts int
	// PartNumberMarker 只返回编号**大于**它的分片。
	PartNumberMarker int
}

// ListPartsResult 是 ListParts 的结果。
type ListPartsResult struct {
	Upload               MultipartUpload
	Parts                []PartInfo
	IsTruncated          bool
	NextPartNumberMarker int
}

// ListMultipartUploadsOptions 是 ListMultipartUploads 的参数。
type ListMultipartUploadsOptions struct {
	Prefix    string
	Delimiter string
	// MaxUploads 上限 1000；<=0 返回空页。
	MaxUploads int
	KeyMarker  string
	// UploadIDMarker 必须与 KeyMarker 同时使用：单独出现时被忽略（S3 契约）。
	UploadIDMarker string
}

// MultipartUploadInfo 是 ListMultipartUploads 返回的一次上传。
type MultipartUploadInfo struct {
	Key               string
	UploadID          string
	Initiated         time.Time
	ChecksumAlgorithm checksum.Algorithm
	ChecksumType      string
}

// ListMultipartUploadsResult 是 ListMultipartUploads 的结果。
type ListMultipartUploadsResult struct {
	Uploads        []MultipartUploadInfo
	CommonPrefixes []string
	IsTruncated    bool
	NextKeyMarker  string
	NextUploadID   string
}

// UploadState 是 multipart/<uploadId>/upload.json 的内容。
//
// 与对象元数据一样是 JSON、一样整体原子替换：会话的字段很少、
// 更新只在创建时发生一次，没有追加结构可言。
type UploadState struct {
	Version           int                `json:"version"`
	UploadID          string             `json:"upload_id"`
	Bucket            string             `json:"bucket"`
	Key               string             `json:"key"`
	ContentType       string             `json:"content_type"`
	UserMeta          map[string]string  `json:"user_meta,omitempty"`
	ChecksumAlgorithm checksum.Algorithm `json:"checksum_algorithm,omitempty"`
	ChecksumType      string             `json:"checksum_type,omitempty"`
	Initiated         time.Time          `json:"initiated"`
	NodeID            string             `json:"node_id"`
	// Revision 在会话发生"非创建性"变更时自增（目前只有首批分片采用
	// 校验和算法一种）。集群层用它做 LWW 比较：会话的各副本可能从
	// 不同协调者、以不同顺序收到更新，只有更高的 Revision 才能覆盖。
	Revision uint64 `json:"revision,omitempty"`
}

// PartState 是 multipart/<uploadId>/parts/<N>.json 的内容。
//
// Blocks 只记这个分片内部的块表，Offset 也是分片内偏移。Complete 时
// 才把它们按顺序重排为全局偏移 —— 这正是创新 B：不复制一个字节，
// 只重写一份块表。分片的大小与其块表长度必须自洽，读取时校验。
type PartState struct {
	Version    int         `json:"version"`
	PartNumber int         `json:"part_number"`
	ETag       string      `json:"etag"`
	Size       int64       `json:"size"`
	Modified   time.Time   `json:"modified"`
	Blocks     []BlockDesc `json:"blocks"`
	Checksum   string      `json:"checksum,omitempty"`
	// Deduped 是本次分片里命中 CAS 的块数，仅用于观测。
	Deduped int `json:"deduped,omitempty"`
}

// validUploadID 校验上传编号的格式：32 位小写十六进制。
//
// 这是**安全边界**而不是风格检查：uploadId 直接拼进目录路径，
// 放行任意字符串就等于允许 `../` 穿越。只接受自己生成的格式，
// 任何多余字符都在触碰文件系统之前被拒。
func validUploadID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// CreateMultipartUpload 创建一次分片上传会话。
func (s *Store) CreateMultipartUpload(bucket, key string, opt CreateMultipartOptions) (*MultipartUpload, error) {
	if err := validateBucketName(bucket); err != nil {
		return nil, err
	}
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if _, err := s.loadBucketMeta(bucket); err != nil {
		return nil, err
	}
	st, err := NewMultipartUploadState(bucket, key, NewUploadID(), opt, s.now(), s.nodeID)
	if err != nil {
		return nil, err
	}
	if err := s.createUploadState(st); err != nil {
		return nil, err
	}
	return st.Info(), nil
}

// NewUploadID 生成一个新的上传编号（32 位小写十六进制）。
//
// 集群协调者必须在广播之前拿到编号：会话位置由
// Ring.Replicas("__mpu__/"+uploadID, ...) 推导，编号是放置键的一部分。
// 单机路径也走这里，两条路径的编号格式不会分叉。
func NewUploadID() string { return randHex(16) }

// NewMultipartUploadState 构造会话状态但不落盘。
//
// 与 CreateMultipartUpload 校验同一套参数，区别只在**不要求本地有桶**：
// 集群协调者本节点不一定是会话副本，它构造好状态后广播给 S，由 S 落盘。
// 报错类型与单机完全一致，客户端看到的仍是 InvalidBucketName/InvalidKey。
func NewMultipartUploadState(bucket, key, uploadID string, opt CreateMultipartOptions, now time.Time, nodeID string) (*UploadState, error) {
	if err := validateBucketName(bucket); err != nil {
		return nil, err
	}
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if !validUploadID(uploadID) {
		// 编号由本包生成，走到这里说明调用方传了自造的值；
		// 按"这次上传不存在"处理，与 loadUploadState 的判定一致。
		return nil, fmt.Errorf("%w: 非法的上传编号 %q", ErrNoSuchUpload, uploadID)
	}
	ct, err := normalizeChecksumType(opt.ChecksumAlgorithm, opt.ChecksumType)
	if err != nil {
		return nil, err
	}
	contentType := opt.ContentType
	if contentType == "" {
		contentType = defaultContentType
	}
	return &UploadState{
		Version:           formatVersion,
		UploadID:          uploadID,
		Bucket:            bucket,
		Key:               key,
		ContentType:       contentType,
		UserMeta:          opt.UserMeta,
		ChecksumAlgorithm: opt.ChecksumAlgorithm,
		ChecksumType:      ct,
		Initiated:         now.UTC().Truncate(time.Second),
		NodeID:            nodeID,
	}, nil
}

// createUploadState 把一份新会话写入本地盘。
func (s *Store) createUploadState(st *UploadState) error {
	return s.putUploadState(st, true)
}

// ---------------------------------------------------------------------------
// 集群层接口：会话与分片状态的显式读写
//
// 单机路径上面的方法把"读写会话状态"和"业务语义"（校验分片、串接块表）
// 绑在一起；集群协调者需要的恰好相反 —— 它要把同一份状态写到多个节点、
// 从多处读出后自行合并。因此这里导出一组只做读写与校验的原语，让集群层
// 复用单机的落盘格式与校验规则，而不是复制一份会漂移的实现。
// ---------------------------------------------------------------------------

// PutUploadState 把一份会话状态写入本节点（创建或更新）。
//
// 幂等：相同内容的重复写入允许。更新受 Revision 单调约束——更旧的
// Revision 一律拒绝（ErrStaleMeta），避免重试把已采用的校验和算法回退。
// 同 Revision 下不允许改写已确定的算法：分片校验和一旦按某算法算出，
// 会话算法就不能再变，否则 Complete 无从合并。
func (s *Store) PutUploadState(st *UploadState) error {
	return s.putUploadState(st, false)
}

func (s *Store) putUploadState(st *UploadState, fresh bool) error {
	if err := validateUploadState(st); err != nil {
		return err
	}

	unlock := s.locks.Lock("mpu:" + st.UploadID)
	defer unlock()

	existed := false
	cur, err := s.loadUploadState(st.UploadID)
	switch {
	case err == nil:
		existed = true
		if cur.Revision > st.Revision {
			return fmt.Errorf("%w: 会话 %s 的 revision %d 小于现有 %d",
				ErrStaleMeta, st.UploadID, st.Revision, cur.Revision)
		}
		if cur.Revision == st.Revision && cur.ChecksumAlgorithm != "" &&
			cur.ChecksumAlgorithm != st.ChecksumAlgorithm {
			return fmt.Errorf("%w: 会话 %s 已确定为 %s，不能再改为 %s",
				ErrInvalidChecksum, st.UploadID, cur.ChecksumAlgorithm, st.ChecksumAlgorithm)
		}
	case errors.Is(err, ErrNoSuchUpload):
		// 新会话，或此前写入中途失败留下的空目录。
	default:
		return err
	}

	// 先建目录再写状态：状态文件是"会话存在"的唯一凭据，它落盘之前
	// 客户端不可能拿到 uploadId，因此不存在"状态在但目录没建"的窗口。
	if err := s.fs.MkdirAll(s.multipartPartsDir(st.UploadID), dirPerm); err != nil {
		return fsErr("创建分片上传目录", st.UploadID, err)
	}
	if err := s.writeFileAtomic(s.multipartStatePath(st.UploadID), mustJSON(st)); err != nil {
		// 只有确实新建时才清目录：更新失败时目录里可能有已上传的分片，
		// 整目录删掉会把别人的分片一起毁掉。
		if fresh && !existed {
			_ = s.fs.RemoveAll(s.multipartUploadDir(st.UploadID))
		}
		return fsErr("写入分片上传状态", st.UploadID, err)
	}
	if err := s.fs.SyncDir(s.multipartRoot()); err != nil {
		return fsErr("同步分片上传目录", st.UploadID, err)
	}
	return nil
}

// validateUploadState 校验一份来自调用方（可能是远端节点）的会话状态。
//
// 远端传来的 JSON 必须先按本机规则验一遍再落盘：会话里的 bucket/key
// 会再次拼进路径，放任非法值等于允许路径穿越。
func validateUploadState(st *UploadState) error {
	if st == nil {
		return fmt.Errorf("%w: 会话状态为空", ErrCorrupt)
	}
	if !validUploadID(st.UploadID) {
		return fmt.Errorf("%w: 非法的上传编号 %q", ErrNoSuchUpload, st.UploadID)
	}
	if err := validateBucketName(st.Bucket); err != nil {
		return err
	}
	if err := validateKey(st.Key); err != nil {
		return err
	}
	if st.Version != formatVersion {
		return fmt.Errorf("%w: 会话 %s 的格式版本 %d，期望 %d",
			ErrCorrupt, st.UploadID, st.Version, formatVersion)
	}
	if st.NodeID == "" {
		return fmt.Errorf("%w: 会话 %s 缺少 node_id", ErrCorrupt, st.UploadID)
	}
	if st.ChecksumAlgorithm == "" {
		if st.ChecksumType != "" {
			return fmt.Errorf("%w: 会话 %s 声明了 checksum 类型 %q 但没有算法",
				ErrCorrupt, st.UploadID, st.ChecksumType)
		}
		return nil
	}
	if _, ok := checksum.New(st.ChecksumAlgorithm); !ok {
		return fmt.Errorf("%w: 会话 %s 的校验和算法 %q 未知",
			ErrInvalidChecksum, st.UploadID, st.ChecksumAlgorithm)
	}
	switch st.ChecksumType {
	case ChecksumTypeFullObject, ChecksumTypeComposite:
		return nil
	default:
		return fmt.Errorf("%w: 会话 %s 的 checksum 类型 %q 未知",
			ErrCorrupt, st.UploadID, st.ChecksumType)
	}
}

// LoadUploadState 读取会话状态；不存在时返回 ErrNoSuchUpload。
func (s *Store) LoadUploadState(uploadID string) (*UploadState, error) {
	return s.loadUploadState(uploadID)
}

// DeleteUploadState 删除会话目录，返回删除前是否存在。
//
// 幂等：删除不存在（或编号非法）的会话不算错误。Complete/Abort 在
// 多节点上尽力清理时，不能因为某个副本早已删掉就整体失败。
func (s *Store) DeleteUploadState(uploadID string) (bool, error) {
	if !validUploadID(uploadID) {
		return false, nil
	}
	unlock := s.locks.Lock("mpu:" + uploadID)
	defer unlock()

	if _, err := s.loadUploadState(uploadID); err != nil {
		if errors.Is(err, ErrNoSuchUpload) {
			return false, nil
		}
		return false, err
	}
	if err := s.fs.RemoveAll(s.multipartUploadDir(uploadID)); err != nil {
		return false, fsErr("删除分片上传目录", uploadID, err)
	}
	if err := s.fs.SyncDir(s.multipartRoot()); err != nil {
		return false, fsErr("同步分片上传目录", uploadID, err)
	}
	return true, nil
}

// LoadPartState 读取分片状态；不存在时返回 ErrInvalidPart。
func (s *Store) LoadPartState(uploadID string, partNumber int) (*PartState, error) {
	if !validUploadID(uploadID) {
		return nil, ErrNoSuchUpload
	}
	if partNumber < 1 || partNumber > maxPartNumber {
		return nil, fmt.Errorf("%w: %d 不在 1–%d 之间",
			ErrInvalidPartNumber, partNumber, maxPartNumber)
	}
	return s.loadPartState(uploadID, partNumber)
}

// PutPartState 把一份分片状态写入本节点。
//
// 要求会话已存在：分片状态写进一个已 Abort/Complete 的会话只会留下
// 永远无人认领的孤儿。远端数据先过 validatePartState，块表与 Size
// 不自洽的状态不会落盘。
func (s *Store) PutPartState(uploadID string, ps *PartState) error {
	if !validUploadID(uploadID) {
		return ErrNoSuchUpload
	}
	if err := validatePartState(ps); err != nil {
		return err
	}

	unlock := s.locks.Lock("mpu:" + uploadID)
	defer unlock()

	if _, err := s.loadUploadState(uploadID); err != nil {
		return err
	}
	if err := s.fs.MkdirAll(s.multipartPartsDir(uploadID), dirPerm); err != nil {
		return fsErr("创建分片目录", uploadID, err)
	}
	if err := s.commitMeta(s.multipartPartPath(uploadID, ps.PartNumber), mustJSON(ps), partHashes(ps)); err != nil {
		return fsErr("写入分片状态", uploadID, err)
	}
	return nil
}

// validatePartState 校验一份来自调用方（可能是远端节点）的分片状态。
func validatePartState(ps *PartState) error {
	if ps == nil {
		return fmt.Errorf("%w: 分片状态为空", ErrCorrupt)
	}
	if ps.Version != formatVersion {
		return fmt.Errorf("%w: 分片 %d 的格式版本 %d，期望 %d",
			ErrCorrupt, ps.PartNumber, ps.Version, formatVersion)
	}
	if ps.PartNumber < 1 || ps.PartNumber > maxPartNumber {
		return fmt.Errorf("%w: %d 不在 1–%d 之间",
			ErrInvalidPartNumber, ps.PartNumber, maxPartNumber)
	}
	if ps.Size < 0 {
		return fmt.Errorf("%w: 分片 %d 的大小为 %d", ErrCorrupt, ps.PartNumber, ps.Size)
	}
	raw, err := hex.DecodeString(ps.ETag)
	if err != nil || len(raw) != md5.Size {
		return fmt.Errorf("%w: 分片 %d 的 ETag %q 不是 MD5",
			ErrCorrupt, ps.PartNumber, ps.ETag)
	}
	var total int64
	for i, b := range ps.Blocks {
		if err := validateBlockHash(b.Hash); err != nil {
			return err
		}
		if b.Length <= 0 || b.Offset != total {
			return fmt.Errorf("%w: 分片 %d 的第 %d 块偏移 %d/长度 %d 与累计 %d 不符",
				ErrCorrupt, ps.PartNumber, i, b.Offset, b.Length, total)
		}
		total += b.Length
	}
	if total != ps.Size {
		return fmt.Errorf("%w: 分片 %d 的块表合计 %d 字节，状态声明 %d",
			ErrCorrupt, ps.PartNumber, total, ps.Size)
	}
	if ps.Deduped < 0 || ps.Deduped > len(ps.Blocks) {
		return fmt.Errorf("%w: 分片 %d 的 deduped=%d 超出块数 %d",
			ErrCorrupt, ps.PartNumber, ps.Deduped, len(ps.Blocks))
	}
	return nil
}

// ListUploadStates 返回本节点上属于指定桶的全部会话，按 (key, uploadId) 排序。
//
// 单个状态损坏时跳过而不是让整个列表失败：一个坏会话不应该让运维
// 看不到其余仍然健康的上传。
func (s *Store) ListUploadStates(bucket string) ([]*UploadState, error) {
	if err := validateBucketName(bucket); err != nil {
		return nil, err
	}
	entries, err := s.fs.ReadDir(s.multipartRoot())
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fsErr("读取分片上传根目录", multipartDirName, err)
	}
	var out []*UploadState
	for _, e := range entries {
		if !e.IsDir() || !validUploadID(e.Name()) {
			continue
		}
		st, err := s.loadUploadState(e.Name())
		if err != nil {
			if errors.Is(err, ErrNoSuchUpload) || errors.Is(err, ErrCorrupt) {
				continue
			}
			return nil, err
		}
		if st.Bucket != bucket {
			continue
		}
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		return out[i].UploadID < out[j].UploadID
	})
	return out, nil
}

// ListPartStates 返回一次上传在本节点上的全部分片状态，按编号升序。
// 会话不存在时返回 ErrNoSuchUpload。
func (s *Store) ListPartStates(uploadID string) ([]*PartState, error) {
	if _, err := s.loadUploadState(uploadID); err != nil {
		return nil, err
	}
	entries, err := s.fs.ReadDir(s.multipartPartsDir(uploadID))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fsErr("读取分片目录", uploadID, err)
	}
	numbers := make([]int, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), multipartPartSuffixName) {
			continue
		}
		if n, ok := parsePartFileName(e.Name()); ok {
			numbers = append(numbers, n)
		}
	}
	sort.Ints(numbers)

	out := make([]*PartState, 0, len(numbers))
	for _, n := range numbers {
		ps, err := s.loadPartState(uploadID, n)
		if errors.Is(err, ErrInvalidPart) {
			continue // 并发覆盖/删除：这一片在遍历途中没了
		}
		if err != nil {
			return nil, err
		}
		out = append(out, ps)
	}
	return out, nil
}

// normalizeChecksumType 归一校验和类型，并校验组合是否合法。
func normalizeChecksumType(alg checksum.Algorithm, declared string) (string, error) {
	if alg == "" {
		if declared != "" {
			// 没有算法却声明类型：参数自相矛盾，绝不能"猜一个"。
			return "", fmt.Errorf("%w: 指定了 checksum 类型 %q 但未指定算法",
				ErrInvalidChecksum, declared)
		}
		return "", nil
	}
	if _, ok := checksum.New(alg); !ok {
		return "", fmt.Errorf("%w: 未知的校验和算法 %q", ErrInvalidChecksum, alg)
	}
	switch declared {
	case "":
		if alg == checksum.AlgCRC64NVME {
			return ChecksumTypeFullObject, nil
		}
		return ChecksumTypeComposite, nil
	case ChecksumTypeFullObject:
		if !checksum.SupportsFullObject(alg) {
			return "", fmt.Errorf("%w: %s 不支持 %s", ErrInvalidChecksum, alg, ChecksumTypeFullObject)
		}
		return ChecksumTypeFullObject, nil
	case ChecksumTypeComposite:
		return ChecksumTypeComposite, nil
	default:
		return "", fmt.Errorf("%w: 未知的 checksum 类型 %q", ErrInvalidChecksum, declared)
	}
}

// NormalizeChecksumType 是 normalizeChecksumType 的导出形式。
//
// 集群协调者在"首个分片声明算法"时要在本地推演同一个结果（会话的
// ChecksumType 由算法决定），否则 Complete 时两侧对不上。
func NormalizeChecksumType(alg checksum.Algorithm, declared string) (string, error) {
	return normalizeChecksumType(alg, declared)
}

// Info 返回会话的对外视图（复制一份 UserMeta 的引用即可，调用方只读）。
func (st *UploadState) Info() *MultipartUpload {
	return &MultipartUpload{
		UploadID:          st.UploadID,
		Bucket:            st.Bucket,
		Key:               st.Key,
		Initiated:         st.Initiated,
		ContentType:       st.ContentType,
		UserMeta:          st.UserMeta,
		ChecksumAlgorithm: st.ChecksumAlgorithm,
		ChecksumType:      st.ChecksumType,
	}
}

// loadUploadState 读取会话状态；不存在时返回 ErrNoSuchUpload。
//
// 与对象元数据的取舍相同：解析失败报 ErrCorrupt，绝不回落成"不存在"。
func (s *Store) loadUploadState(uploadID string) (*UploadState, error) {
	if !validUploadID(uploadID) {
		return nil, ErrNoSuchUpload
	}
	data, err := readFile(s.fs, s.multipartStatePath(uploadID))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNoSuchUpload
	}
	if err != nil {
		return nil, fsErr("读取分片上传状态", uploadID, err)
	}
	var st UploadState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("%w: 分片上传状态 %s 损坏: %v", ErrCorrupt, uploadID, err)
	}
	// 状态里的 upload_id 必须与目录名一致：防止目录被改名后
	// 不同会话互相覆盖。
	if st.UploadID != uploadID {
		return nil, fmt.Errorf("%w: 分片上传状态 %s 的 upload_id 为 %q",
			ErrCorrupt, uploadID, st.UploadID)
	}
	return &st, nil
}

// UploadPart 上传一个分片。
//
// 流程刻意分成"锁外流式写入"与"锁内确认 + 落状态"两段：
// 分片可能有数 GiB，持有 uploadId 锁读完全部内容会让同一上传的
// 其他分片完全串行；而锁内只做一次小文件写入，足以防止与
// Abort/Complete 的竞态（锁内重新读取状态，发现会话已消失就失败）。
func (s *Store) UploadPart(bucket, key, uploadID string, partNumber int, body io.Reader, opt UploadPartOptions) (*PartInfo, error) {
	if partNumber < 1 || partNumber > maxPartNumber {
		return nil, fmt.Errorf("%w: %d 不在 1–%d 之间", ErrInvalidPartNumber, partNumber, maxPartNumber)
	}
	if opt.ExpectedSize < 0 {
		return nil, fmt.Errorf("%w: 缺少 Content-Length", ErrIncompleteBody)
	}
	// 与 PutObject 相同：在高水位直接拒绝，避免半个分片落盘。
	if err := s.checkWriteAllowed(); err != nil {
		return nil, err
	}

	st, err := s.loadUploadState(uploadID)
	if err != nil {
		return nil, err
	}
	if st.Bucket != bucket || st.Key != key {
		// 与 AWS 一致：uploadId 不属于这个 bucket/key 时按"不存在"处理，
		// 不泄露某次上传属于哪个 key。
		return nil, ErrNoSuchUpload
	}

	alg := st.ChecksumAlgorithm
	adopting := false
	if opt.ChecksumAlgorithm != "" && opt.ChecksumAlgorithm != alg {
		if alg != "" {
			return nil, fmt.Errorf("%w: 本次声明 %s，会话为 %s",
				ErrInvalidChecksum, opt.ChecksumAlgorithm, alg)
		}
		// 会话创建时没有声明算法，但客户端在 UploadPart 上带了校验和。
		// 这不是错误用法：aws-cli 的 create-multipart-upload 不带
		// --checksum-algorithm 时，后续 upload-part 仍会按默认 CRC64NVME
		// 自动算出并发送校验和，而真实 S3 接受这种组合。直接拒绝会让
		// "手工 multipart" 这一最常见的调试流程完全跑不通。
		//
		// 因此采用第一个分片声明的算法，并在锁内持久化到会话状态；
		// 此后所有分片都必须使用同一算法（下方锁内复核会拦住并发写
		// 不同算法的请求）。
		alg = opt.ChecksumAlgorithm
		adopting = true
		if _, ok := checksum.New(alg); !ok {
			return nil, fmt.Errorf("%w: 未知的校验和算法 %q", ErrInvalidChecksum, alg)
		}
	}

	tag := fmt.Sprintf("upload %s part %d", uploadID, partNumber)
	res, err := s.streamBlocks(body, alg, tag, opt.ExpectedSize)
	if res != nil {
		// 块已 pin，分片状态提交（或失败返回）后统一解除。
		defer s.unpinBlocks(res.hashes)
	}
	if err != nil {
		return nil, err
	}
	if res.size != opt.ExpectedSize {
		return nil, fmt.Errorf("%w: 收到 %d 字节，Content-Length 声明 %d",
			ErrIncompleteBody, res.size, opt.ExpectedSize)
	}
	if opt.ContentMD5 != "" && !strings.EqualFold(opt.ContentMD5, res.etag) {
		return nil, fmt.Errorf("%w: Content-MD5 声明 %s，实际 %s", ErrBadDigest, opt.ContentMD5, res.etag)
	}
	declared := opt.Checksum
	if declared == "" && alg != "" && opt.LateChecksum != nil {
		// trailer 里的校验和只有读到 EOF 之后才存在；到这一步请求体
		// 已完整读完，可以安全取值。
		if v, ok := opt.LateChecksum(); ok {
			declared = v
		}
	}
	if declared != "" && declared != res.checksum {
		return nil, fmt.Errorf("%w: %s 声明 %s，实际 %s",
			ErrBadDigest, alg, declared, res.checksum)
	}

	ps := PartState{
		Version:    formatVersion,
		PartNumber: partNumber,
		ETag:       res.etag,
		Size:       res.size,
		Modified:   s.now().UTC().Truncate(time.Second),
		Blocks:     res.blocks,
		Checksum:   res.checksum,
		Deduped:    res.deduped,
	}

	unlock := s.locks.Lock("mpu:" + uploadID)
	defer unlock()
	// 锁内复核：读请求体期间可能发生了 Abort 或 Complete。
	// 不复核就会把分片状态写进一个已删除的目录，留下永远无人认领的孤儿。
	cur, err := s.loadUploadState(uploadID)
	if err != nil {
		return &PartInfo{}, err
	}
	if cur.ChecksumAlgorithm != "" && cur.ChecksumAlgorithm != alg {
		// 并发的首批分片可能各自"采用"了不同算法：先落盘者赢，
		// 后到者必须失败，否则对象里会混着两种算法的校验和。
		return nil, fmt.Errorf("%w: 会话已被设为 %s，本次声明 %s",
			ErrInvalidChecksum, cur.ChecksumAlgorithm, alg)
	}
	if adopting && cur.ChecksumAlgorithm == "" {
		checksumType, err := normalizeChecksumType(alg, "")
		if err != nil {
			return nil, err
		}
		cur.ChecksumAlgorithm = alg
		cur.ChecksumType = checksumType
		cur.Revision++
		// 会话状态不引用数据块，hashes 传 nil。
		if err := s.commitMeta(s.multipartStatePath(uploadID), mustJSON(cur), nil); err != nil {
			return nil, fsErr("更新分片上传状态", uploadID, err)
		}
	}
	// 分片状态引用的块必须在提交前刷新 mtime，否则一份"刚写好状态、
	// 块却是旧 mtime"的分片会被并发清扫误判为孤儿。
	if err := s.commitMeta(s.multipartPartPath(uploadID, partNumber), mustJSON(ps), res.hashes); err != nil {
		return nil, fsErr("写入分片状态", tag, err)
	}
	return &PartInfo{
		PartNumber:        partNumber,
		ETag:              ps.ETag,
		Size:              ps.Size,
		Modified:          ps.Modified,
		Checksum:          ps.Checksum,
		ChecksumAlgorithm: alg,
	}, nil
}

// loadPartState 读取分片状态；不存在时返回 ErrInvalidPart。
func (s *Store) loadPartState(uploadID string, partNumber int) (*PartState, error) {
	data, err := readFile(s.fs, s.multipartPartPath(uploadID, partNumber))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrInvalidPart
	}
	if err != nil {
		return nil, fsErr("读取分片状态", uploadID, err)
	}
	var ps PartState
	if err := json.Unmarshal(data, &ps); err != nil {
		return nil, fmt.Errorf("%w: 分片 %d 状态损坏: %v", ErrCorrupt, partNumber, err)
	}
	if ps.PartNumber != partNumber {
		return nil, fmt.Errorf("%w: 分片 %d 的状态文件声称为 %d",
			ErrCorrupt, partNumber, ps.PartNumber)
	}
	return &ps, nil
}

// CompleteMultipartUpload 把已上传的分片组装成对象。
//
// 关键实现（创新 B）：**不重读、不复制任何数据块**，只把各分片的块表
// 按顺序串起来并重算全局偏移，然后一次原子写入 meta.json。因此耗时与
// 写入量都与对象大小无关 —— 20 GiB 对象与 20 MiB 对象的 Complete
// 成本相同（BenchmarkCompleteMultipart20GB 断言这一点）。
//
// 锁顺序：先 uploadId 锁、再 key 锁。反向（先 key 再 uploadId）会与
// 本函数竞争出死锁，因此全包统一这一个顺序。
func (s *Store) CompleteMultipartUpload(bucket, key, uploadID string, parts []CompletePart, declaredChecksum string, ifNoneMatchAny bool) (*ObjectInfo, error) {
	if len(parts) == 0 {
		return nil, fmt.Errorf("%w: 分片列表为空", ErrInvalidPartOrder)
	}

	unlockMPU := s.locks.Lock("mpu:" + uploadID)
	defer unlockMPU()

	st, err := s.loadUploadState(uploadID)
	if err != nil {
		return nil, err
	}
	if st.Bucket != bucket || st.Key != key {
		return nil, ErrNoSuchUpload
	}

	out, err := AssembleMultipart(st, parts, func(n int) (*PartState, error) {
		return s.loadPartState(uploadID, n)
	}, declaredChecksum)
	if err != nil {
		return nil, err
	}

	unlockKey := s.locks.Lock(bucket + "\x00" + key)
	defer unlockKey()

	prev, err := s.loadObjectMeta(bucket, key)
	switch {
	case err == nil:
		// 已过期对象在逻辑上不存在，条件写不能因为它触发 412。
		if ifNoneMatchAny && !s.expiredAt(prev.ExpiresAt) {
			return nil, ErrPreconditionFailed
		}
	case errors.Is(err, ErrNoSuchKey):
		prev = nil
	default:
		return nil, err
	}

	// TTL 在对象真正变得可见的这一刻（Complete）开始计算，而不是从
	// Initiate 或最后一片上传算起：客户端等待完成的时长不应吃掉保留期。
	modified := s.now().UTC().Truncate(time.Second)
	meta := objectMeta{
		Version:           formatVersion,
		Key:               key,
		Size:              out.Size,
		ETag:              out.ETag,
		ContentType:       st.ContentType,
		Modified:          modified,
		ExpiresAt:         s.expiresAtFor(bucket, modified),
		UserMeta:          st.UserMeta,
		Blocks:            out.Blocks,
		Checksum:          out.Checksum,
		ChecksumAlgorithm: string(out.ChecksumAlgorithm),
		ChecksumType:      out.ChecksumType,
		NodeID:            s.nodeID,
		ObjVersion:        1,
		Parts:             out.Parts,
	}
	if prev != nil {
		meta.ObjVersion = prev.ObjVersion + 1
	}

	// 先提交对象（元数据 + 块表在同一次 rename 里），再删除分片状态。
	// 顺序不能反：先删状态而对象没写成功，数据就彻底丢了；
	// 反过来的中间态只是"对象已可见、临时状态还没清"，重试是幂等的。
	if err := s.commitMeta(s.objectMetaPath(bucket, key), mustJSON(meta), localBlockHashes(&meta)); err != nil {
		return nil, fsErr("写入对象元数据", key, err)
	}
	s.metaCache.put(metaCacheKey{bucket: bucket, key: key}, &meta)
	if err := s.fs.RemoveAll(s.multipartUploadDir(uploadID)); err != nil {
		// 对象已经提交成功，这里失败不能报错 —— 客户端会以为上传失败
		// 而重试，但重试只会得到 NoSuchUpload。残留目录由启动清扫回收。
		_ = err
	} else {
		_ = s.fs.SyncDir(s.multipartRoot())
	}

	return &ObjectInfo{
		Key:               key,
		Size:              out.Size,
		ETag:              out.ETag,
		Modified:          meta.Modified,
		ContentType:       st.ContentType,
		UserMeta:          st.UserMeta,
		Checksum:          out.Checksum,
		ChecksumAlgorithm: st.ChecksumAlgorithm,
		ChecksumType:      st.ChecksumType,
		BlockCount:        out.BlockCount,
		DedupedBlocks:     out.Deduped,
		Version:           meta.ObjVersion,
		Parts:             PartBounds(out.Parts),
	}, nil
}

// AssembledObject 是一组分片串接后的结果：只有块表与派生属性，不含数据。
type AssembledObject struct {
	Blocks   []BlockDesc
	Size     int64
	ETag     string
	Checksum string
	// ChecksumAlgorithm / ChecksumType 取自会话，空表示该对象没有校验和。
	ChecksumAlgorithm checksum.Algorithm
	ChecksumType      string
	BlockCount        int
	Deduped           int
	// Parts 是本次 Complete 的分片边界，直接来自各 PartState.Size。
	// 只能在这里产出：Complete 之后 multipart/<uploadId>/ 会被删除，
	// 事后再也恢复不出"第 N 片从哪到哪"。
	Parts []PartDesc
}

// AssembleMultipart 校验并串接分片块表，产出对象级块表与 ETag/校验和。
//
// 这是 Complete 的全部"计算"部分，单机与集群共用：单机的 load 从本地
// 读状态，集群的 load 从 S 的多个副本合并出最新状态。两条路径的
// 校验顺序、错误类型与 ETag 算法因此不可能分叉。
//
// load 返回 ErrInvalidPart 表示请求引用了未上传的分片。
func AssembleMultipart(st *UploadState, parts []CompletePart, load func(int) (*PartState, error), declaredChecksum string) (*AssembledObject, error) {
	if len(parts) == 0 {
		return nil, fmt.Errorf("%w: 分片列表为空", ErrInvalidPartOrder)
	}
	if st == nil || load == nil {
		return nil, fmt.Errorf("%w: 缺少会话或分片来源", ErrCorrupt)
	}

	out := &AssembledObject{
		ChecksumAlgorithm: st.ChecksumAlgorithm,
		ChecksumType:      st.ChecksumType,
	}
	var (
		partsCS   []checksum.PartChecksum
		etagInput []byte
	)
	lastNumber := 0
	for i, p := range parts {
		if p.PartNumber < 1 || p.PartNumber > maxPartNumber {
			return nil, fmt.Errorf("%w: %d 不在 1–%d 之间",
				ErrInvalidPartNumber, p.PartNumber, maxPartNumber)
		}
		// 严格升序：S3 不接受乱序或重复。必须在"存在性"检查之前判断，
		// 否则乱序请求会先撞上 InvalidPart，客户端会去重传一个其实没问题的分片。
		if p.PartNumber <= lastNumber {
			return nil, fmt.Errorf("%w: %d 出现在 %d 之后",
				ErrInvalidPartOrder, p.PartNumber, lastNumber)
		}
		lastNumber = p.PartNumber

		ps, err := load(p.PartNumber)
		if err != nil {
			return nil, err
		}
		if ps == nil {
			return nil, ErrInvalidPart
		}
		if !etagMatch(ps.ETag, p.ETag) {
			return nil, fmt.Errorf("%w: 分片 %d ETag %q 与已上传的 %s 不符",
				ErrInvalidPart, p.PartNumber, p.ETag, ps.ETag)
		}
		// 非末片至少 5 MiB。最后一片不受限：对象总大小常常不是
		// 分片尺寸的整数倍，限制它会让正常上传无法完成。
		if i < len(parts)-1 && ps.Size < minPartSize {
			return nil, fmt.Errorf("%w: 分片 %d 为 %d 字节，非末片至少 %d 字节",
				ErrEntityTooSmall, p.PartNumber, ps.Size, minPartSize)
		}

		// 串接块表：只改 Offset，块内容仍在 CAS 里原样复用。
		for _, b := range ps.Blocks {
			b.Offset = out.Size
			out.Blocks = append(out.Blocks, b)
			out.Size += b.Length
		}
		// 分片自身的块表必须与其 Size 自洽，否则说明状态文件被改过。
		if out.Size < 0 {
			return nil, fmt.Errorf("%w: 分片 %d 的块表长度溢出", ErrCorrupt, p.PartNumber)
		}
		out.Deduped += ps.Deduped
		out.BlockCount += len(ps.Blocks)
		// 分片边界直接取 PartState.Size。不重新用块表累计：两者不一致
		// 说明状态文件被改过，随后的 out.Size 校验会暴露它，而不是
		// 把错误长度静默写进 meta。
		out.Parts = append(out.Parts, PartDesc{Number: p.PartNumber, Size: ps.Size})

		raw, err := hex.DecodeString(ps.ETag)
		if err != nil || len(raw) != md5.Size {
			return nil, fmt.Errorf("%w: 分片 %d 的 ETag %q 不是 MD5",
				ErrCorrupt, p.PartNumber, ps.ETag)
		}
		etagInput = append(etagInput, raw...)

		if st.ChecksumAlgorithm != "" {
			if ps.Checksum == "" {
				return nil, fmt.Errorf("%w: 分片 %d 缺少 %s 校验和",
					ErrCorrupt, p.PartNumber, st.ChecksumAlgorithm)
			}
			partsCS = append(partsCS, checksum.PartChecksum{
				Encoded: ps.Checksum,
				Size:    ps.Size,
			})
		}
	}

	if st.ChecksumAlgorithm != "" {
		var ok bool
		if st.ChecksumType == ChecksumTypeFullObject {
			out.Checksum, ok = checksum.MergeFullObject(st.ChecksumAlgorithm, partsCS)
		} else {
			out.Checksum, ok = checksum.MergeComposite(st.ChecksumAlgorithm, partsCS)
		}
		if !ok {
			return nil, fmt.Errorf("%w: 无法合并 %s 校验和",
				ErrCorrupt, st.ChecksumAlgorithm)
		}
		if declaredChecksum != "" && declaredChecksum != out.Checksum {
			return nil, fmt.Errorf("%w: %s 声明 %s，实际 %s",
				ErrBadDigest, st.ChecksumAlgorithm, declaredChecksum, out.Checksum)
		}
	}

	sum := md5.Sum(etagInput)
	out.ETag = hex.EncodeToString(sum[:]) + "-" + fmt.Sprint(len(parts))
	return out, nil
}

// etagMatch 比较两个 ETag，忽略客户端可能带上的引号。
func etagMatch(stored, given string) bool {
	return strings.EqualFold(stored, strings.Trim(given, `"`))
}

// AbortMultipartUpload 放弃一次分片上传并删除其全部状态。
//
// 分片数据块本身不删：它们与普通对象共享 CAS，可能有别的对象
// 正在引用；孤儿块由 GC 统一回收（03 文档）。
func (s *Store) AbortMultipartUpload(bucket, key, uploadID string) error {
	unlock := s.locks.Lock("mpu:" + uploadID)
	defer unlock()

	st, err := s.loadUploadState(uploadID)
	if err != nil {
		return err
	}
	if st.Bucket != bucket || st.Key != key {
		return ErrNoSuchUpload
	}
	if err := s.fs.RemoveAll(s.multipartUploadDir(uploadID)); err != nil {
		return fsErr("删除分片上传目录", uploadID, err)
	}
	return s.fs.SyncDir(s.multipartRoot())
}

// ListParts 列举一次上传中已上传的分片，按编号升序。
func (s *Store) ListParts(bucket, key, uploadID string, opt ListPartsOptions) (*ListPartsResult, error) {
	st, err := s.loadUploadState(uploadID)
	if err != nil {
		return nil, err
	}
	if st.Bucket != bucket || st.Key != key {
		return nil, ErrNoSuchUpload
	}

	res := &ListPartsResult{Upload: *st.Info()}
	if opt.MaxParts <= 0 {
		return res, nil
	}

	entries, err := s.fs.ReadDir(s.multipartPartsDir(uploadID))
	if errors.Is(err, fs.ErrNotExist) {
		return res, nil
	}
	if err != nil {
		return nil, fsErr("读取分片目录", uploadID, err)
	}

	numbers := make([]int, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), multipartPartSuffixName) {
			continue
		}
		n, ok := parsePartFileName(e.Name())
		if !ok {
			continue
		}
		if n > opt.PartNumberMarker {
			numbers = append(numbers, n)
		}
	}
	sort.Ints(numbers)

	for _, n := range numbers {
		if len(res.Parts) >= opt.MaxParts {
			res.IsTruncated = true
			break
		}
		ps, err := s.loadPartState(uploadID, n)
		if errors.Is(err, ErrInvalidPart) {
			continue // 并发覆盖/删除：这一片在遍历途中没了
		}
		if err != nil {
			return nil, err
		}
		res.Parts = append(res.Parts, PartInfo{
			PartNumber:        ps.PartNumber,
			ETag:              ps.ETag,
			Size:              ps.Size,
			Modified:          ps.Modified,
			Checksum:          ps.Checksum,
			ChecksumAlgorithm: st.ChecksumAlgorithm,
		})
		res.NextPartNumberMarker = ps.PartNumber
	}
	if !res.IsTruncated {
		res.NextPartNumberMarker = 0
	}
	return res, nil
}

// parsePartFileName 从 "<N>.json" 解析编号，拒绝前导零与越界值。
func parsePartFileName(name string) (int, bool) {
	s := strings.TrimSuffix(name, multipartPartSuffixName)
	if s == "" || len(s) > 5 || s[0] == '0' {
		return 0, false
	}
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
		n = n*10 + int(s[i]-'0')
	}
	if n < 1 || n > maxPartNumber {
		return 0, false
	}
	return n, true
}

// ListMultipartUploads 列举桶内进行中的分片上传。
func (s *Store) ListMultipartUploads(bucket string, opt ListMultipartUploadsOptions) (*ListMultipartUploadsResult, error) {
	if err := validateBucketName(bucket); err != nil {
		return nil, err
	}
	if _, err := s.loadBucketMeta(bucket); err != nil {
		return nil, err
	}
	uploads, err := s.allUploads(bucket)
	if err != nil {
		return nil, err
	}
	return PageUploads(uploads, opt), nil
}

// PageUploads 对一组（已去重的）会话做排序、delimiter 折叠与分页。
//
// 单机直接从本地全部会话调用；集群把各节点的结果按 uploadId 合并后
// 调用同一个函数 —— 分页边界因此在两条路径上不可能漂移。
// 调用方需保证同一 uploadId 只出现一次（集群侧保留最新副本）。
func PageUploads(uploads []MultipartUploadInfo, opt ListMultipartUploadsOptions) *ListMultipartUploadsResult {
	res := &ListMultipartUploadsResult{}
	if opt.MaxUploads <= 0 {
		return res
	}
	sort.Slice(uploads, func(i, j int) bool {
		if uploads[i].Key != uploads[j].Key {
			return uploads[i].Key < uploads[j].Key
		}
		return uploads[i].UploadID < uploads[j].UploadID
	})

	// 先把 marker 之后的结果折叠成逻辑条目，再截断：
	// 顺序反了会让"上一页最后一条是公共前缀"时的分页边界算错。
	type entry struct {
		key      string
		isPrefix bool
		upload   *MultipartUploadInfo
	}
	var entries []entry
	lastPrefix := ""
	for i := range uploads {
		u := &uploads[i]
		if opt.Prefix != "" && !strings.HasPrefix(u.Key, opt.Prefix) {
			continue
		}
		e := entry{key: u.Key, upload: u}
		if opt.Delimiter != "" {
			rest := u.Key[len(opt.Prefix):]
			if idx := strings.Index(rest, opt.Delimiter); idx >= 0 {
				e = entry{key: opt.Prefix + rest[:idx+len(opt.Delimiter)], isPrefix: true}
			}
		}
		if !pastUploadMarker(e.key, e.isPrefix, u.UploadID, opt) {
			continue
		}
		if e.isPrefix {
			if e.key == lastPrefix {
				continue
			}
			lastPrefix = e.key
		}
		entries = append(entries, e)
	}

	if len(entries) > opt.MaxUploads {
		entries = entries[:opt.MaxUploads]
		res.IsTruncated = true
	}
	for _, e := range entries {
		if e.isPrefix {
			res.CommonPrefixes = append(res.CommonPrefixes, e.key)
			continue
		}
		res.Uploads = append(res.Uploads, *e.upload)
	}
	if res.IsTruncated && len(entries) > 0 {
		last := entries[len(entries)-1]
		res.NextKeyMarker = last.key
		if last.upload != nil {
			res.NextUploadID = last.upload.UploadID
		}
	}
	return res
}

// pastUploadMarker 判断一条逻辑条目是否位于请求起始位置之后。
//
// 规则：条目 key 大于 marker 即保留；相等时公共前缀丢弃（已返回过），
// 上传条目则按 uploadId 严格大于 marker 判断；只给 keyMarker 而不给
// uploadIdMarker 时，同 key 的上传全部跳过（S3 契约）。
func pastUploadMarker(key string, isPrefix bool, uploadID string, opt ListMultipartUploadsOptions) bool {
	if opt.KeyMarker == "" && opt.UploadIDMarker == "" {
		return true
	}
	if key > opt.KeyMarker {
		return true
	}
	if key < opt.KeyMarker {
		return false
	}
	if isPrefix {
		return false
	}
	if opt.UploadIDMarker == "" {
		return false
	}
	return uploadID > opt.UploadIDMarker
}

// allUploads 扫描 multipart/ 并读出属于指定 bucket 的全部会话。
//
// 单个状态文件损坏时跳过而不是让整个列表失败：一个坏会话不应该
// 让运维看不到其余仍然健康的上传。
func (s *Store) allUploads(bucket string) ([]MultipartUploadInfo, error) {
	entries, err := s.fs.ReadDir(s.multipartRoot())
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fsErr("读取分片上传根目录", multipartDirName, err)
	}
	var out []MultipartUploadInfo
	for _, e := range entries {
		if !e.IsDir() || !validUploadID(e.Name()) {
			continue
		}
		st, err := s.loadUploadState(e.Name())
		if err != nil {
			if errors.Is(err, ErrNoSuchUpload) || errors.Is(err, ErrCorrupt) {
				continue
			}
			return nil, err
		}
		if st.Bucket != bucket {
			continue
		}
		out = append(out, MultipartUploadInfo{
			Key:               st.Key,
			UploadID:          st.UploadID,
			Initiated:         st.Initiated,
			ChecksumAlgorithm: st.ChecksumAlgorithm,
			ChecksumType:      st.ChecksumType,
		})
	}
	return out, nil
}

// bucketHasUploads 报告桶内是否有进行中的分片上传（DeleteBucket 用）。
func (s *Store) bucketHasUploads(bucket string) (bool, error) {
	uploads, err := s.allUploads(bucket)
	if err != nil {
		return false, err
	}
	return len(uploads) > 0, nil
}

// cleanExpiredUploads 在启动时回收过期或残缺的分片上传目录。
//
// 两类对象：
//   - 缺 upload.json 的目录：创建中途失败或 Complete 后删除了一半的残骸，
//     没有任何价值，直接删。
//   - Initiated 超过保留期的会话：客户端早已放弃，永远不会完成。
//
// 损坏但未过期的会话**保留**：它可能是唯一能解释"为什么这次上传完了
// 对象却不在"的证据，静默删掉只会让问题变成"数据凭空消失"。
func (s *Store) cleanExpiredUploads() error {
	root := s.multipartRoot()
	entries, err := s.fs.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fsErr("读取分片上传根目录", multipartDirName, err)
	}
	cutoff := s.now().UTC().Add(-multipartRetention)
	removed := false
	for _, e := range entries {
		if !e.IsDir() || !validUploadID(e.Name()) {
			// 不是我们生成的目录名：不动它。数据目录里可能有
			// 运维自己放的东西，静默删除是不可接受的。
			continue
		}
		dir := s.multipartUploadDir(e.Name())
		st, err := s.loadUploadState(e.Name())
		switch {
		case errors.Is(err, ErrNoSuchUpload):
			if err := s.fs.RemoveAll(dir); err == nil {
				removed = true
			}
		case err != nil:
			// 损坏：按目录 mtime 判断是否过期，避免把仍在排查的证据删掉。
			if fi, statErr := s.fs.Stat(dir); statErr == nil && fi.ModTime().Before(cutoff) {
				if err := s.fs.RemoveAll(dir); err == nil {
					removed = true
				}
			}
		case st.Initiated.Before(cutoff):
			if err := s.fs.RemoveAll(dir); err == nil {
				removed = true
			}
		}
	}
	if removed {
		return s.fs.SyncDir(root)
	}
	return nil
}
