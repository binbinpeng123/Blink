package cluster

import (
	"fmt"
	"io"

	"github.com/binbinpeng123/Blink/internal/store"
)

// CopyObject 在集群模式下以流式方式复制对象。
//
// 必须显式覆盖：Cluster 内嵌 *store.Store，若不覆盖，方法提升会让调用
// 落到单机实现 —— 目标 meta 会直接引用**本节点**上的块表，而副本位置
// 由目标 key 在环上的位置决定，其它节点上根本没有这些块。复制"成功"
// 之后读一次就是 503/数据缺失。
//
// 与单机 O(1) 复制的取舍：
//   - 单机：meta 直接共享同一组 CAS 块，零字节搬运；
//   - 集群：源经 quorum 读出、目标经 quorum 写入，协调者中转一次数据。
//
// 集群也做不到块级共享，因为目标 key 的环位置可能完全不同。数据量小的
// 元数据级优化留在单机（边端单节点是主力场景），集群保证正确性优先。
// 文档与响应中不宣称集群复制为零拷贝。
func (c *Cluster) CopyObject(dstBucket, dstKey, srcBucket, srcKey string, opt store.CopyOptions) (*store.ObjectInfo, error) {
	if err := store.ValidateObjectPath(srcBucket, srcKey); err != nil {
		return nil, err
	}
	if err := store.ValidateObjectPath(dstBucket, dstKey); err != nil {
		return nil, err
	}
	if dstBucket == srcBucket && dstKey == srcKey && !opt.ReplaceMetadata {
		return nil, fmt.Errorf("%w: 源与目标相同，COPY 元数据指令没有意义", store.ErrInvalidRequest)
	}

	// 先确认两个桶都存在。OpenObject 对"桶不存在"和"key 不存在"都回
	// NoSuchKey（环上没有任何副本），而 S3 契约要求桶的问题报
	// NoSuchBucket。复制是低频操作，多一次存在性检查换取与单机模式
	// 一致的错误码。
	for _, bucket := range []string{srcBucket, dstBucket} {
		exists, err := c.BucketExists(bucket)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, store.ErrBucketNotFound
		}
	}

	src, err := c.OpenObject(srcBucket, srcKey)
	if err != nil {
		return nil, err
	}
	info := src.Info()
	if info.Size > store.MaxCopyObjectSize {
		return nil, fmt.Errorf("%w: 源对象 %d 字节超过 %d 字节上限，请使用分片上传",
			store.ErrInvalidRequest, info.Size, store.MaxCopyObjectSize)
	}

	// 管道把"读源"与"写目标"接成一条流：峰值内存是一块（4 MiB），
	// 不随对象大小增长。写入端全部消费完（或报错）后 pw 关闭，读端
	// 才能收到 EOF。
	pr, pw := io.Pipe()
	go func() {
		// 把读侧错误带过管道，而不是让它变成一个无解释的 EOF。
		_ = pw.CloseWithError(src.WriteRangeTo(pw, 0, info.Size))
	}()
	// PutObject 可能在前置条件失败时提前返回、不再读 body：此时读
	// goroutine 必须被唤醒，否则它会永远阻塞在 Write 上（goroutine 泄漏，
	// 并且源 reader 持有的资源也不释放）。
	defer func() { _ = pr.CloseWithError(io.ErrClosedPipe) }()

	putOpt := store.PutOptions{
		ContentType:     info.ContentType,
		UserMeta:        info.UserMeta,
		HTTPMeta:        info.HTTPMeta,
		IfNoneMatchAny:  opt.IfNoneMatchAny,
		IfNoneMatchETag: opt.IfNoneMatchETag,
		IfMatchETag:     opt.IfMatchETag,
		IfMatchAny:      opt.IfMatchAny,
		ExpectedSize:    info.Size,
	}
	if opt.ReplaceMetadata {
		putOpt.ContentType = opt.ContentType
		putOpt.UserMeta = opt.UserMeta
		// REPLACE 与单机路径语义一致：缺省的标准 HTTP 元数据要清空。
		putOpt.HTTPMeta = opt.HTTPMeta
	}
	// 不传 ChecksumAlgorithm：集群路径重新切块、重新计算 ETag，源的
	// FULL_OBJECT 校验和虽然仍然成立，但 COMPOSITE（分片上传产生）的
	// 校验和无法在流式重写中保留，统一按"未声明"处理比"有时保留有时丢弃"
	// 更可预期。
	return c.PutObject(dstBucket, dstKey, pr, putOpt)
}
