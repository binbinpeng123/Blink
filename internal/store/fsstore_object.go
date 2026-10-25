package store

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"time"

	"github.com/binbinpeng123/Blink/internal/checksum"
)

// defaultContentType 是客户端未提供 Content-Type 时的落库值（02 文档）。
const defaultContentType = "application/octet-stream"

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
	if opt.ExpectedSize < 0 {
		return nil, fmt.Errorf("%w: 缺少 Content-Length", ErrIncompleteBody)
	}

	unlock := s.locks.Lock(bucket + "\x00" + key)
	defer unlock()

	// 读旧元数据有两个用途：If-None-Match 判断，以及 obj_version 递增。
	// 持有 key 锁期间读取，因此 prev 与随后的写入之间不可能插入别的写者。
	prev, err := s.loadObjectMeta(bucket, key)
	switch {
	case err == nil:
		if opt.IfNoneMatchAny {
			return nil, ErrPreconditionFailed
		}
	case errors.Is(err, ErrNoSuchKey):
		prev = nil
	default:
		return nil, err
	}

	res, err := s.streamBlocks(body, opt.ChecksumAlgorithm, key)
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
	meta := objectMeta{
		Version:           formatVersion,
		Key:               key,
		Size:              total,
		ETag:              etag,
		ContentType:       contentType,
		Modified:          s.now().UTC().Truncate(time.Second),
		UserMeta:          opt.UserMeta,
		Blocks:            blocks,
		Checksum:          actualChecksum,
		ChecksumAlgorithm: string(opt.ChecksumAlgorithm),
		ChecksumType:      checksumType,
		NodeID:            s.nodeID,
		ObjVersion:        1,
	}
	if prev != nil {
		meta.ObjVersion = prev.ObjVersion + 1
	}

	// 唯一让对象变得可见的一步：改名是原子的，读者要么看不到对象，
	// 要么看到完整的新版本，绝不会看到"块表指向一半的块"。
	if err := s.writeFileAtomic(s.objectMetaPath(bucket, key), mustJSON(meta)); err != nil {
		return nil, fsErr("写入对象元数据", key, err)
	}

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

// streamResult 是一次流式切块的产出。
//
// 抽出来给 UploadPart 复用：分片写入与普通对象写入在"切块、算 MD5、
// 算校验和、块落 CAS"上完全一致，差别只在最终如何组装元数据。
// 两处各写一遍读循环是最容易悄悄跑偏的那种重复 —— 一处改了块边界
// 或校验顺序，另一处不会跟着变。
type streamResult struct {
	blocks []blockDesc
	size   int64
	// etag 是内容 MD5 的小写十六进制（普通对象 ETag 与分片 ETag 同义）。
	etag string
	// checksum 是 alg 对应的校验和（base64）；alg 为空时为空串。
	checksum string
	deduped  int
}

// streamBlocks 从 body 读完全部内容，切块写入 CAS，同时算出 MD5 与校验和。
//
// tag 只用于错误信息（对象 key 或分片标识），不参与任何判断。
// 调用方负责长度校验：本函数不知道客户端声明了多少字节。
func (s *Store) streamBlocks(body io.Reader, alg checksum.Algorithm, tag string) (*streamResult, error) {
	var objSum checksum.Sum
	if alg != "" {
		sum, ok := checksum.New(alg)
		if !ok {
			return nil, fmt.Errorf("%w: 未知的校验和算法 %q", ErrInvalidChecksum, alg)
		}
		objSum = sum
	}

	res := &streamResult{blocks: make([]blockDesc, 0, 4)}
	md5h := md5.New()

	buf := make([]byte, s.blockSize)
	// 小对象快路径：先读 inlineThreshold 字节。若流在此结束，对象只有一块，
	// 省下 4 MiB 的缓冲分配 —— 边端常见的是几 KiB 的配置/清单文件，
	// 为每个这样的对象分配一个块大小的缓冲是把内存当垃圾扔。
	prefill := 0
	if s.inlineThreshold > 0 {
		n, err := io.ReadFull(body, buf[:s.inlineThreshold])
		switch {
		case err == nil, errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
			prefill = n
		default:
			return nil, fsErr("读取请求体", tag, err)
		}
	}

	for {
		n, err := readBlock(body, buf, prefill)
		prefill = 0
		if n > 0 {
			chunk := buf[:n]
			// md5/sha 与校验和计算器都不返回错误，但接口要求处理它。
			_, _ = md5h.Write(chunk)
			if objSum != nil {
				_, _ = objSum.Write(chunk)
			}

			sum := sha256.Sum256(chunk)
			hash := hex.EncodeToString(sum[:])
			wrote, err := s.putBlock(hash, chunk)
			if err != nil {
				return nil, err
			}
			if !wrote {
				res.deduped++
			}
			res.blocks = append(res.blocks, blockDesc{
				Hash:   hash,
				Offset: res.size,
				Length: int64(n),
				CRC32C: checksum.BlockCRC32C(chunk),
			})
			res.size += int64(n)
		}
		if err != nil {
			// EOF 与 ErrUnexpectedEOF 都表示"正常读完，只是最后一块不满"；
			// 其余错误意味着链路出了问题，必须让整次写入失败。
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, fsErr("读取请求体", tag, err)
		}
	}

	res.etag = hex.EncodeToString(md5h.Sum(nil))
	if objSum != nil {
		res.checksum = objSum.Encoded()
	}
	return res, nil
}

// readBlock 把一个块读满到 buf，返回块长度。
//
// prefill 表示 buf 前若干字节已在小对象快路径里读入。返回语义：
//
//	err == nil                 读满 buf，可能还有更多数据
//	err == io.EOF              流已结束；n 为最后一块长度（空对象时为 0）
//	err == io.ErrUnexpectedEOF 流在中途结束；n 为最后一块长度
//
// 关键细节：prefill > 0 且流恰好在此结束时，io.ReadFull 返回
// (0, io.EOF)，此时 buf[:prefill] 本身就是最后一个完整块 ——
// 直接丢弃会让每个"长度恰好是阈值整数倍"的对象少一段内容。
func readBlock(r io.Reader, buf []byte, prefill int) (int, error) {
	if prefill == 0 {
		return io.ReadFull(r, buf)
	}
	n, err := io.ReadFull(r, buf[prefill:])
	return prefill + n, err
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
func (s *Store) putBlock(hash string, data []byte) (bool, error) {
	final := s.casPath(hash)
	if _, err := s.fs.Stat(final); err == nil {
		return false, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return false, fsErr("检查块", hash, err)
	}

	w, err := s.newAtomicWriter(final, false)
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
	return true, nil
}
