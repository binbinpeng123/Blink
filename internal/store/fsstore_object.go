package store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/binbinpeng123/Blink/internal/checksum"
)

// DefaultContentType 是客户端未提供 Content-Type 时的落库值（02 文档）。
//
// 导出给集群协调者：它构造副本 meta 时必须与单机路径落出同一个默认值，
// 否则同一个对象经不同路径写入会有不同的 Content-Type。
const DefaultContentType = "application/octet-stream"

const defaultContentType = DefaultContentType

// PutOptions 是一次 PutObject 的输入。
//
// 这些都是**协议层已验证过**的值：本包只看语义，不解析 HTTP 头。
// 例如 ContentMD5 是解码后的十六进制，而不是头部里的 base64 文本。
type PutOptions struct {
	// ContentType 由客户端提供；为空时落库为 application/octet-stream。
	ContentType string
	// UserMeta 是 x-amz-meta-* 的解码结果（键已去掉前缀）。
	UserMeta map[string]string
	// ChecksumAlgorithm 与 Checksum 是客户端声明的对象级校验和。
	// Checksum 为 base64 编码；两者要么同时为空，要么同时非空。
	ChecksumAlgorithm checksum.Algorithm
	Checksum          string
	// ContentMD5 是 Content-MD5 头解码后的**小写十六进制**，空表示未声明。
	ContentMD5 string
	// IfNoneMatchAny 对应 If-None-Match: *：对象已存在则失败。
	IfNoneMatchAny bool
	// IfMatchETag 对应 If-Match: "etag"：仅当当前对象 ETag 完全匹配才写。
	// 对象不存在（或已过期）同样视为前置条件未满足。
	IfMatchETag string
	// IfMatchAny 对应 If-Match: *：仅当对象存在才写。
	// 与 IfMatchETag 互斥；两者都为零值时不做 If-Match 判断。
	IfMatchAny bool
	// ExpectedSize 是 Content-Length。
	//
	// S3 要求单次 PUT 必须声明长度，负值一律拒绝：边读边写时"长度未知"
	// 既没法提前拒绝超大请求，也没法判断请求体是否被截断 ——
	// 而后者正是 ErrIncompleteBody 要守的那道关。
	ExpectedSize int64
	// LateChecksum 在请求体读完后才可用（HTTP trailer 里的校验和）。
	// 第二个返回值为 false 表示客户端没有通过 trailer 声明。
	//
	// 之所以做成回调而不是字段：trailer 的值只在 body 读到 EOF 之后才存在，
	// 而校验必须发生在 meta.json 落盘之前 —— 两者恰好夹住读循环，
	// 回调是唯一能同时满足这两个时序的接口形态。
	LateChecksum func() (string, bool)
}

// ObjectInfo 是一次成功写入的结果，供协议层直接生成响应头。
type ObjectInfo struct {
	Key      string
	Size     int64
	ETag     string
	Modified time.Time

	ContentType string
	UserMeta    map[string]string

	// Checksum 与 ChecksumAlgorithm 是**实际算出**的值（不是客户端声明的），
	// 用于 ChecksumMode: ENABLED 时回传。
	Checksum          string
	ChecksumAlgorithm checksum.Algorithm
	// ChecksumType 是校验和覆盖范围。单次 PUT 恒为 FULL_OBJECT；
	// 分片上传才可能是 COMPOSITE。回传时要与 Checksum 一起给出，
	// 客户端据此决定用哪种方式复算（见 objectMeta.ChecksumType）。
	ChecksumType string

	// BlockCount 是块数；DedupedBlocks 是其中已在 CAS 中存在、本次未写盘的块数。
	BlockCount    int
	DedupedBlocks int
	// Version 是本次写入后的 obj_version。
	Version uint64

	// Parts 是分片上传产生的分片边界（含推导出的绝对偏移），供
	// GetObject/HeadObject 的 partNumber 子资源使用。
	//
	// 普通 PutObject 与 CopyObject 产生的对象为空：复制不继承源对象的
	// 分片边界，两种复制模式都按"整体对象即第一片"处理。
	Parts []ObjectPart
}

// ObjectPart 是一个分片在对象内的绝对边界。
//
// Offset 由 PartDesc 的 Size 累计推导，不落盘。PartNumber 从 1 开始，
// 编号可以不连续（客户端允许跳过编号），因此不能用下标推算。
type ObjectPart struct {
	Number int
	Offset int64
	Size   int64
}

// PartBounds 把落盘的分片长度描述换算成带绝对偏移的读取视图。
//
// 单机与集群的读路径都要做这一步；集中在这里可以保证两条路径对
// "第 N 片从哪开始"给出完全相同的结果。
func PartBounds(parts []PartDesc) []ObjectPart {
	if len(parts) == 0 {
		return nil
	}
	out := make([]ObjectPart, 0, len(parts))
	var off int64
	for _, p := range parts {
		out = append(out, ObjectPart{Number: p.Number, Offset: off, Size: p.Size})
		off += p.Size
	}
	return out
}

// PutObject 把一个对象流式写入存储。
//
// 写入顺序（与 03 文档一致，只有一处刻意偏离，见下）：
//
//	边读边切块 → 每块直接以 4 MiB 为单位原子写入 CAS → 校验 → 原子替换 meta.json
//
// **刻意偏离**：03 文档的写路径是"先全量写 tmp/，再逐块提交到 CAS"，
// 那样整个对象要落盘两次。这里改为直接把每个块原子写进 CAS：
// 对象仍然只在 meta.json 被 rename 之后才可见（读者找不到 meta 就等于
// 对象不存在），失败的上传只会留下孤儿块 —— 而孤儿块本来就是 GC 的
// 回收对象（03 文档 GC 章节）。边端设备上少写一半字节，值得这处偏离。
//
// 并发：同一 key 的写入被串行化，不同 key 完全并行。CAS 的块写入不加锁，
// 理由见 putBlock。
func (s *Store) PutObject(bucket, key string, body io.Reader, opt PutOptions) (*ObjectInfo, error) {
	// 桶名同样校验一次：bucket 来自 URL 路径，是攻击者可控的片段。
	// 校验让 `..`、`/` 这类值在触碰文件系统之前就被拒掉。
	if err := validateBucketName(bucket); err != nil {
		return nil, err
	}
	if err := validateKey(key); err != nil {
		return nil, err
	}
	// 先确认桶存在：否则"桶没建"会由下面的 loadObjectMeta 报成 ErrNoSuchKey，
	// 客户端拿到 NoSuchKey 而不是 NoSuchBucket —— 两者对排障的意义完全不同。
	if _, err := s.loadBucketMeta(bucket); err != nil {
		return nil, err
	}
	// 高水位在入口拒绝，而不是等写到一半 ENOSPC：半途失败会留下
	// 无法回收的临时对象，且客户端已经上传了部分字节。
	if err := s.checkWriteAllowed(); err != nil {
		return nil, err
	}
	if opt.ExpectedSize < 0 {
		return nil, fmt.Errorf("%w: 缺少 Content-Length", ErrIncompleteBody)
	}

	unlock := s.locks.Lock(bucket + "\x00" + key)
	defer unlock()

	// 读旧元数据有两个用途：条件写判断，以及 obj_version 递增。
	// 持有 key 锁期间读取，因此 prev 与随后的写入之间不可能插入别的写者。
	prev, err := s.loadObjectMeta(bucket, key)
	switch {
	case err == nil:
		// 条件判定统一走 checkWriteConditions：这里刻意不直接把
		// If-None-Match 与 If-Match 写死在分支里，避免 PUT/COPY/MPU
		// 三条写路径对同一组头部给出不同答复。
		if cerr := s.checkWriteConditions(prev, opt.IfNoneMatchAny, opt.IfMatchETag, opt.IfMatchAny); cerr != nil {
			return nil, cerr
		}
	case errors.Is(err, ErrNoSuchKey):
		prev = nil
		if cerr := s.checkWriteConditions(nil, opt.IfNoneMatchAny, opt.IfMatchETag, opt.IfMatchAny); cerr != nil {
			return nil, cerr
		}
	default:
		return nil, err
	}

	// 内联快路径（创新 D0）：声明长度不超过上限时，先把内容读进内存。
	//
	// 只有长度**恰好**等于声明值才走内联；少了或多了都把已读到的前缀
	// 拼回请求体，交给普通流式路径产生与以前完全相同的错误
	// （ErrIncompleteBody），不能因为"试过内联"而改变失败语义。
	//
	// 上限本身不大于 InlineThreshold 与 BlockSize，因此内联对象必然只有
	// 一个块。多读一个字节（+1）专门用来识别"声明 4 KiB、实际更多"。
	var (
		res        *streamResult
		inlineData []byte
	)
	if s.inlineObjectMax > 0 && opt.ExpectedSize > 0 && opt.ExpectedSize <= s.inlineObjectMax {
		prefix, rerr := io.ReadAll(io.LimitReader(body, opt.ExpectedSize+1))
		if rerr != nil {
			return nil, fsErr("读取请求体", key, rerr)
		}
		if int64(len(prefix)) == opt.ExpectedSize {
			res, err = s.hashInline(prefix, opt.ChecksumAlgorithm, key)
			if err != nil {
				return nil, err
			}
			inlineData = prefix
		} else {
			body = io.MultiReader(bytes.NewReader(prefix), body)
		}
	}
	if res == nil {
		res, err = s.streamBlocks(body, opt.ChecksumAlgorithm, key, opt.ExpectedSize)
	}
	if res != nil {
		// 块在 streamBlocks 里已经 pin；元数据提交（或失败返回）后统一解除。
		defer s.unpinBlocks(res.hashes)
	}
	if err != nil {
		return nil, err
	}
	blocks, total, etag := res.blocks, res.size, res.etag
	deduped := res.deduped

	// 长度校验必须在写 meta.json 之前：此刻失败只会留下孤儿块（可回收），
	// 一旦 meta 落盘，客户端就会看到一个"长度不对但确实存在"的对象。
	if total != opt.ExpectedSize {
		return nil, fmt.Errorf("%w: 收到 %d 字节，Content-Length 声明 %d",
			ErrIncompleteBody, total, opt.ExpectedSize)
	}

	if opt.ContentMD5 != "" && !strings.EqualFold(opt.ContentMD5, etag) {
		return nil, fmt.Errorf("%w: Content-MD5 声明 %s，实际 %s", ErrBadDigest, opt.ContentMD5, etag)
	}

	actualChecksum := res.checksum
	// 单次 PUT 只有一个分片，校验和天然覆盖整个对象。
	var checksumType string
	if opt.ChecksumAlgorithm != "" {
		checksumType = ChecksumTypeFullObject
		declared := opt.Checksum
		if declared == "" && opt.LateChecksum != nil {
			if v, ok := opt.LateChecksum(); ok {
				declared = v
			}
		}
		if declared != "" && declared != actualChecksum {
			return nil, fmt.Errorf("%w: %s 声明 %s，实际 %s",
				ErrBadDigest, opt.ChecksumAlgorithm, declared, actualChecksum)
		}
	}

	contentType := opt.ContentType
	if contentType == "" {
		contentType = defaultContentType
	}
	// Modified 与 ExpiresAt 必须来自同一个时间点：若分两次取 now，
	// 过期时刻会比可见的修改时间略晚，测试与排障时对不上账。
	modified := s.now().UTC().Truncate(time.Second)
	meta := objectMeta{
		Version:           formatVersion,
		Key:               key,
		Size:              total,
		ETag:              etag,
		ContentType:       contentType,
		Modified:          modified,
		ExpiresAt:         s.expiresAtFor(bucket, modified),
		UserMeta:          opt.UserMeta,
		Blocks:            blocks,
		Checksum:          actualChecksum,
		ChecksumAlgorithm: string(opt.ChecksumAlgorithm),
		ChecksumType:      checksumType,
		NodeID:            s.nodeID,
		ObjVersion:        1,
	}
	if inlineData != nil {
		// 块描述保留（Hash=内容 SHA-256），让旧二进制能按块表找到
		// 同等内容或明确报损坏；新二进制按 Layout 直接读 InlineData。
		meta.Layout = LayoutInline
		meta.InlineData = inlineData
	}
	if prev != nil {
		meta.ObjVersion = prev.ObjVersion + 1
	}

	// 唯一让对象变得可见的一步：改名是原子的，读者要么看不到对象，
	// 要么看到完整的新版本，绝不会看到"块表指向一半的块"。
	if err := s.commitMeta(s.objectMetaPath(bucket, key), mustJSON(meta), res.hashes); err != nil {
		return nil, fsErr("写入对象元数据", key, err)
	}
	s.metaCache.put(metaCacheKey{bucket: bucket, key: key}, &meta)

	return &ObjectInfo{
		Key:               key,
		Size:              total,
		ETag:              etag,
		Modified:          meta.Modified,
		ContentType:       contentType,
		UserMeta:          opt.UserMeta,
		Checksum:          actualChecksum,
		ChecksumAlgorithm: opt.ChecksumAlgorithm,
		ChecksumType:      checksumType,
		BlockCount:        len(blocks),
		DedupedBlocks:     deduped,
		Version:           meta.ObjVersion,
	}, nil
}

// streamResult 是一次流式切块的产出，额外记下本地去重命中数。
//
// 抽出来给 UploadPart 复用：分片写入与普通对象写入在"切块、算 MD5、
// 算校验和、块落 CAS"上完全一致，差别只在最终如何组装元数据。
// 两处各写一遍读循环是最容易悄悄跑偏的那种重复 —— 一处改了块边界
// 或校验顺序，另一处不会跟着变。
type streamResult struct {
	blocks []BlockDesc
	size   int64
	// etag 是内容 MD5 的小写十六进制（普通对象 ETag 与分片 ETag 同义）。
	etag string
	// checksum 是 alg 对应的校验和（base64）；alg 为空时为空串。
	checksum string
	// deduped 是命中本地 CAS、本次未写盘的块数。
	deduped int
	// hashes 是本次写入过程中成功 pin 的块（含去重命中），按出现顺序。
	//
	// 调用方必须把它们交给 commitMeta 刷新 mtime，并在元数据提交后
	// unpinBlocks；失败路径同样要 unpin —— 块已落盘但无人引用，
	// 交给 GC 在宽限期后回收。
	hashes []string
}

// streamBlocks 从 body 读完全部内容，切块写入本地 CAS。
//
// 切块与校验逻辑在导出的 StreamBlocks 中；这里只提供单机路径的落盘
// 回调，集群路径用同一个 StreamBlocks 把块同时扇出到远端。
//
// expected 是调用方已知的 Content-Length（< 0 表示未知），只用于决定
// 临时缓冲大小，见 StreamBlocksSized。
func (s *Store) streamBlocks(body io.Reader, alg checksum.Algorithm, tag string, expected int64) (*streamResult, error) {
	res := &streamResult{}
	summary, err := s.StreamBlocksSized(body, alg, tag, expected, func(b Block) error {
		wrote, err := s.putBlock(b.Desc.Hash, b.Data)
		if err != nil {
			return err
		}
		// pin 在 putBlock 内登记，无论是否命中已有块都要记下来，
		// 由调用方在元数据提交后统一解除。
		res.hashes = append(res.hashes, b.Desc.Hash)
		if !wrote {
			res.deduped++
		}
		return nil
	})
	if err != nil {
		// 失败时也返回 res：已经 pin 的块必须由调用方 unpin，
		// 否则它们会被永久视为"使用中"，GC 再也回收不掉。
		return res, err
	}
	res.blocks = summary.Blocks
	res.size = summary.Size
	res.etag = summary.ETag
	res.checksum = summary.Checksum
	return res, nil
}

// putBlock 把一块内容写入 CAS，返回是否真的写了盘。
//
// 已存在则跳过 —— 这就是"跨桶、跨 key 天然去重"（创新 A）的全部实现：
// 路径由内容决定，"存在"必然意味着"内容相同"，不需要任何索引或引用计数
// 就完成了两个桶的同一份模型权重只存一遍。
//
// 并发下两个写者可能同时发现"不存在"，各自写临时文件后 rename 到同一路径：
// 两者内容逐字节相同，后到的覆盖先到的，结果依然正确。因此这里不加锁 ——
// 给 CAS 加锁会把所有共享块前缀的写入串行化，正好抵消掉并发写的好处。
//
// 返回时块处于 **pinned** 状态（无论是否命中已有块）：调用方必须在
// 元数据提交后 unpin，或在本函数返回错误时由本函数自行解除。
func (s *Store) putBlock(hash string, data []byte) (bool, error) {
	exists, err := s.acquireBlock(hash)
	if err != nil {
		return false, err
	}
	if exists {
		return false, nil // pin 保留给调用方，用于 commitMeta 的 touch
	}

	// 从这里开始，任何失败路径都必须解除 acquireBlock 留下的 pin。
	success := false
	defer func() {
		if !success {
			s.unpinBlock(hash)
		}
	}()

	// 去重命中不占新空间，因此水位检查放在 Stat 之后：已存在的块
	// 即使在阻断期也允许命中，否则重复上传同一份模型会莫名失败。
	if err := s.checkWriteAllowed(); err != nil {
		return false, err
	}

	w, err := s.newAtomicWriter(s.casPath(hash), false)
	if err != nil {
		return false, fsErr("创建块", hash, err)
	}
	// Commit 成功后 Abort 是 no-op（见 atomicWriter.Abort），
	// 失败路径上它负责把临时文件收拾掉。
	defer func() { _ = w.Abort() }()

	if _, err := w.Write(data); err != nil {
		return false, fsErr("写入块", hash, err)
	}
	if err := w.Commit(); err != nil {
		return false, fsErr("提交块", hash, err)
	}
	success = true
	return true, nil
}
