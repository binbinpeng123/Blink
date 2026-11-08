package store

import (
	"errors"
	"fmt"
	"time"

	"github.com/binbinpeng123/Blink/internal/checksum"
)

// MaxCopyObjectSize 是 CopyObject 允许的源对象大小上限：5 GiB。
//
// 与 S3 一致。更大的对象必须走分片上传（UploadPartCopy 尚未实现），
// 静默放行会让单次复制的元数据写入（块表）远远超出 readFile 的 32 MiB
// 上限，客户端会在复制"成功"之后再也读不出该对象。
const MaxCopyObjectSize int64 = 5 << 30

// CopyOptions 是一次 CopyObject 的输入。
//
// ContentType 与 UserMeta 仅在 ReplaceMetadata=true 时生效：COPY 指令
// 要求完整保留源对象的元数据，此时调用方不应（也无法）覆盖它们。
type CopyOptions struct {
	// ReplaceMetadata 对应 x-amz-metadata-directive: REPLACE。
	// false 表示 COPY：保留源的 Content-Type、x-amz-meta-* 与校验和。
	ReplaceMetadata bool
	// ContentType 是 REPLACE 时的新 Content-Type；为空时落默认值。
	ContentType string
	// UserMeta 是 REPLACE 时的新用户元数据（键已去掉 x-amz-meta- 前缀）。
	UserMeta map[string]string
	// IfNoneMatchAny 对应目标上的 If-None-Match: *。
	IfNoneMatchAny bool
}

// CopyObject 在单机模式下以 O(1) 完成对象复制。
//
// 这是内容寻址布局的直接收益：对象由「meta.json + 一组不可变 CAS 块」
// 组成，复制只需要写一份新的 meta.json 指向**同一组块**，不需要读写
// 任何数据字节。复制 1 KiB 与复制 5 GiB 的代价都是「读一次小文件 +
// 写一次小文件」。
//
// 集群模式下块分布按 key 的环位置决定，不能共享单机块表，因此
// Cluster.CopyObject 显式覆盖本方法，走流式复制。
//
// 并发：只锁目标 key。源对象在读取 meta 后即为不可变快照（覆盖写只会
// 换掉 meta 的指向，不会修改旧块），因此不需要同时持有源 key 的锁，
// 也不会与源上的写入互相阻塞。
func (s *Store) CopyObject(dstBucket, dstKey, srcBucket, srcKey string, opt CopyOptions) (*ObjectInfo, error) {
	if err := validateBucketName(dstBucket); err != nil {
		return nil, err
	}
	if err := validateKey(dstKey); err != nil {
		return nil, err
	}
	if err := validateBucketName(srcBucket); err != nil {
		return nil, err
	}
	if err := validateKey(srcKey); err != nil {
		return nil, err
	}

	if _, err := s.loadBucketMeta(dstBucket); err != nil {
		return nil, err
	}
	if _, err := s.loadBucketMeta(srcBucket); err != nil {
		return nil, err
	}

	if dstBucket == srcBucket && dstKey == srcKey && !opt.ReplaceMetadata {
		// 复制到自身且保留元数据等于"把文件重命名为自己"：S3 明确拒绝。
		// REPLACE 是合法的（原地更新 Content-Type / 用户元数据）。
		return nil, fmt.Errorf("%w: 源与目标相同，COPY 元数据指令没有意义", ErrInvalidRequest)
	}

	unlock := s.locks.Lock(dstBucket + "\x00" + dstKey)
	defer unlock()

	src, err := s.loadObjectMeta(srcBucket, srcKey)
	if err != nil {
		return nil, err
	}
	// 墓碑表示对象已删除：与"从未存在"对客户端是同一种状况。
	if src.Deleted {
		return nil, ErrNoSuchKey
	}
	// EC 分片分散在集群的多个节点上，单机 meta 引用它们会指向不存在的块。
	// 明确回 501，而不是复制出一个读一次就报 ErrCorrupt 的对象。
	if src.Layout == LayoutEC {
		return nil, fmt.Errorf("%w: 纠删码布局的对象不能通过单机路径复制", ErrNotImplemented)
	}
	if src.Size > MaxCopyObjectSize {
		return nil, fmt.Errorf("%w: 源对象 %d 字节超过 %d 字节上限，请使用分片上传",
			ErrInvalidRequest, src.Size, MaxCopyObjectSize)
	}

	prev, err := s.loadObjectMeta(dstBucket, dstKey)
	switch {
	case err == nil:
		// 目标上的旧版本（可能是墓碑）只用于两条判断：条件写与版本递增。
		if opt.IfNoneMatchAny && !prev.Deleted {
			return nil, ErrPreconditionFailed
		}
	case errors.Is(err, ErrNoSuchKey):
		prev = nil
	default:
		return nil, err
	}

	contentType := src.ContentType
	userMeta := src.UserMeta
	if opt.ReplaceMetadata {
		contentType = opt.ContentType
		if contentType == "" {
			contentType = defaultContentType
		}
		userMeta = opt.UserMeta
	}
	if contentType == "" {
		contentType = defaultContentType
	}

	meta := objectMeta{
		Version:     formatVersion,
		Key:         dstKey,
		Size:        src.Size,
		ETag:        src.ETag,
		ContentType: contentType,
		Modified:    s.now().UTC().Truncate(time.Second),
		UserMeta:    userMeta,
		// 块表按值复制一份：meta 是只读快照，但两个 meta 共享同一个
		// 底层数组会让将来的任何原地改动（如修复逻辑）同时改到源对象。
		// 复制 1300 个块描述是微秒级操作，换掉这类隐性耦合值得。
		Blocks:            append([]BlockDesc(nil), src.Blocks...),
		Checksum:          src.Checksum,
		ChecksumAlgorithm: src.ChecksumAlgorithm,
		ChecksumType:      src.ChecksumType,
		NodeID:            s.nodeID,
		ObjVersion:        1,
	}
	if prev != nil {
		meta.ObjVersion = prev.ObjVersion + 1
	}

	if err := s.writeFileAtomic(s.objectMetaPath(dstBucket, dstKey), mustJSON(meta)); err != nil {
		return nil, fsErr("写入对象元数据", dstKey, err)
	}
	s.metaCache.put(metaCacheKey{bucket: dstBucket, key: dstKey}, &meta)

	return &ObjectInfo{
		Key:               dstKey,
		Size:              meta.Size,
		ETag:              meta.ETag,
		Modified:          meta.Modified,
		ContentType:       meta.ContentType,
		UserMeta:          meta.UserMeta,
		Checksum:          meta.Checksum,
		ChecksumAlgorithm: checksum.Algorithm(meta.ChecksumAlgorithm),
		ChecksumType:      meta.ChecksumType,
		BlockCount:        len(meta.Blocks),
		Version:           meta.ObjVersion,
	}, nil
}
