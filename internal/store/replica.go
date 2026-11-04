package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
)

// 本文件是单机存储对集群层暴露的"副本引擎"API。
//
// 集群协调者不重新实现存储格式：它用 StreamBlocks 切块，然后通过
// PutBlock/ReadBlockRange 把块写进/读出各节点的 CAS，用
// PutReplicaMeta/DeleteReplicaMeta 维护对象元数据。这样单机与集群
// 共享同一套落盘布局、去重与完整性校验，不存在两份会互相漂移的实现。
//
// 这些方法跳过"桶必须存在"的检查：跨节点读取时，执行读的节点可能
// 从未建过该桶（切片 3 的桶复制尚未实现）。桶存在性是协调者的事，
// 不是副本引擎的事。

// ValidateObjectPath 校验 bucket 与 key 是否合法。
//
// 集群层的对象 key 来自 HTTP 查询参数，必须先过与单机路径相同的校验，
// 再触碰文件系统。
func ValidateObjectPath(bucket, key string) error {
	if err := validateBucketName(bucket); err != nil {
		return err
	}
	return validateKey(key)
}

// LockObject 返回按 (bucket, key) 串行化的解锁函数。
//
// 集群协调者用它在"读旧版本 → 生成新版本 → 写副本"期间排斥同 key 的
// 其他协调者。它与 Store.PutObject 内部的锁是同一把，因此单机路径与
// 集群路径对同一个 key 也互斥。
func (s *Store) LockObject(bucket, key string) func() {
	return s.locks.Lock(bucket + "\x00" + key)
}

// LoadReplicaMeta 读取对象元数据，不要求桶存在。
//
// 返回值 found=false 表示该节点没有这个对象的副本；元数据损坏仍返回
// ErrCorrupt —— "没有副本"与"副本坏了"在分布式读里必须区分：
// 前者可以换节点，后者说明数据需要修复。
func (s *Store) LoadReplicaMeta(bucket, key string) (*ObjectMeta, bool, error) {
	if err := ValidateObjectPath(bucket, key); err != nil {
		return nil, false, err
	}
	m, err := s.readObjectMetaAt(s.objectMetaPath(bucket, key))
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil, false, nil
	case err != nil:
		return nil, false, fsErr("读取副本元数据", key, err)
	}
	return m, true, nil
}

// PutReplicaMeta 写入一个对象的副本元数据。
//
// 写入前按 (ObjVersion, NodeID) 与现有版本比较：旧版本或同版本但节点
// 字典序更小的元数据一律拒绝（ErrStaleMeta）。这让并发覆盖写最终收敛
// 到同一个版本，而不是"谁最后到达谁赢"——后者会让两个副本永久分叉。
//
// 相同版本的重复写入是允许的：重试必须幂等。
func (s *Store) PutReplicaMeta(bucket, key string, m *ObjectMeta) error {
	if err := ValidateObjectPath(bucket, key); err != nil {
		return err
	}
	if err := validateReplicaMeta(key, m); err != nil {
		return err
	}

	unlock := s.LockObject(bucket, key)
	defer unlock()

	cur, found, err := s.LoadReplicaMeta(bucket, key)
	if err != nil {
		return err
	}
	if found && staleMeta(m, cur) {
		return fmt.Errorf("%w: 新版本 (%d,%s) 小于现有 (%d,%s)",
			ErrStaleMeta, m.ObjVersion, m.NodeID, cur.ObjVersion, cur.NodeID)
	}

	if err := s.writeFileAtomic(s.objectMetaPath(bucket, key), mustJSON(m)); err != nil {
		return fsErr("写入副本元数据", key, err)
	}
	return nil
}

// DeleteReplicaMeta 删除对象副本，返回删除前是否存在。
//
// 与 Store.DeleteObject 一样只删元数据目录，不碰 CAS 块：块可能被其他
// key 去重引用，只能由 GC 回收。幂等，但会把"本来是否存在"告诉调用方，
// 供协调者统计删除确认数。
func (s *Store) DeleteReplicaMeta(bucket, key string) (bool, error) {
	if err := ValidateObjectPath(bucket, key); err != nil {
		return false, err
	}

	unlock := s.LockObject(bucket, key)
	defer unlock()

	if _, err := s.fs.Stat(s.objectMetaPath(bucket, key)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fsErr("检查副本元数据", key, err)
	}
	if err := s.fs.RemoveAll(s.objectDir(bucket, key)); err != nil {
		return false, fsErr("删除副本元数据", key, err)
	}
	return true, nil
}

// PutBlock 把一块内容写入 CAS，返回是否真的写了盘。
//
// 调用方必须保证 data 的 SHA-256 就是 hash（StreamBlocks 返回的块天然
// 满足）。本方法**不重算哈希**：单机写路径已经为每块算过一次 SHA-256，
// 再算一次等于把 CPU 花在重复劳动上；远端节点则在内部接口处校验，
// 因为那里的数据来自网络，必须重算。
func (s *Store) PutBlock(hash string, data []byte) (bool, error) {
	if err := validateBlockHash(hash); err != nil {
		return false, err
	}
	if len(data) == 0 {
		return false, fmt.Errorf("%w: 块内容为空", ErrInvalidBlock)
	}
	return s.putBlock(hash, data)
}

// BlockExists 报告 CAS 中是否已有该块。
func (s *Store) BlockExists(hash string) (bool, error) {
	if err := validateBlockHash(hash); err != nil {
		return false, err
	}
	_, err := s.fs.Stat(s.casPath(hash))
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, fs.ErrNotExist):
		return false, nil
	default:
		return false, fsErr("检查块", hash, err)
	}
}

// BlockSize 返回块内容的字节数。
//
// 内部下载接口需要它来设置 Content-Length：先知道长度再写响应头，
// 客户端才能判断传输是否被截断。块不存在时返回的错误满足
// errors.Is(err, fs.ErrNotExist)。
func (s *Store) BlockSize(hash string) (int64, error) {
	if err := validateBlockHash(hash); err != nil {
		return 0, err
	}
	st, err := s.fs.Stat(s.casPath(hash))
	if err != nil {
		return 0, fsErr("检查块", hash, err)
	}
	return st.Size(), nil
}

// ReadBlockRange 把块 b 的 [offset, offset+length) 字节写入 w。
//
// 复用单机读路径的 copyBlockSegment，因此"整块走 sendfile 零拷贝、部分
// 块先 seek 再定量拷贝"的行为与本地 GET 完全一致，没有第二份需要同步
// 维护的拷贝循环。
//
// 之所以接收完整 BlockDesc 而不是只收哈希：拷贝函数在 verifyOnRead 下
// 要拿 CRC32C 校验内容。只传哈希会让校验退化成"与零比较"，把每次读
// 都判成损坏 —— 这正是集群读路径必须与单机共享块表描述的原因。
// 越界或长度与块文件不符返回 ErrCorrupt：内部调用方已按块表校验过范围，
// 走到这里不一致意味着元数据与块文件已经分叉。
func (s *Store) ReadBlockRange(w io.Writer, b BlockDesc, offset, length int64) error {
	if err := validateBlockHash(b.Hash); err != nil {
		return err
	}
	if b.Length <= 0 {
		return fmt.Errorf("%w: 块 %s 长度 %d 非正", ErrInvalidBlock, b.Hash, b.Length)
	}
	if offset < 0 || length < 0 {
		return fmt.Errorf("%w: 块 %s 的读取范围 [%d,%d) 非法",
			ErrCorrupt, b.Hash, offset, offset+length)
	}

	st, err := s.fs.Stat(s.casPath(b.Hash))
	if err != nil {
		// 不翻译 fs.ErrNotExist：调用方需要区分"块不在"（可换源）
		// 与"读失败"（重试同一来源或报 500）。
		return fsErr("检查块", b.Hash, err)
	}
	if st.Size() != b.Length {
		return fmt.Errorf("%w: 块 %s 长度 %d，块表记录 %d",
			ErrCorrupt, b.Hash, st.Size(), b.Length)
	}
	if offset > st.Size() || length > st.Size()-offset {
		return fmt.Errorf("%w: 块 %s 长度 %d，读取范围 [%d,%d) 越界",
			ErrCorrupt, b.Hash, st.Size(), offset, offset+length)
	}
	if length == 0 {
		return nil
	}
	// 拷贝函数按块内偏移定位，块的 Offset 在本调用里恒为 0。
	b.Offset = 0
	return s.copyBlockSegment(w, b, offset, offset+length)
}

// validateBlockHash 校验块哈希是 64 位小写十六进制。
//
// 大小写必须统一：CAS 路径由哈希字符串直接拼出，接受大写会产生
// 同一内容的两个路径，去重就失效了。
func validateBlockHash(hash string) error {
	if len(hash) != 64 {
		return fmt.Errorf("%w: 块哈希长度 %d，期望 64", ErrInvalidBlock, len(hash))
	}
	for i := 0; i < len(hash); i++ {
		c := hash[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return fmt.Errorf("%w: 块哈希含非法字符 %q", ErrInvalidBlock, string(c))
		}
	}
	return nil
}

// staleMeta 判断 incoming 是否比当前版本旧。
//
// 比较顺序是 (ObjVersion, NodeID)：版本号是主序，同版本用节点 ID 字典序
// 决胜。所有副本用同一个比较函数，因此无论写入以什么顺序到达，
// 最终都会收敛到同一个最大值。
func staleMeta(incoming, current *ObjectMeta) bool {
	if incoming.ObjVersion != current.ObjVersion {
		return incoming.ObjVersion < current.ObjVersion
	}
	return incoming.NodeID < current.NodeID
}

// validateReplicaMeta 校验跨节点搬运来的元数据。
//
// 内部端口虽然有 HMAC 认证，但一个过期或有 bug 的节点仍可能送来
// 结构上自相矛盾的元数据。在落盘前拦住它，比让损坏的块表进入读路径
// 再以 ErrCorrupt 暴露要好得多。
func validateReplicaMeta(key string, m *ObjectMeta) error {
	if m == nil {
		return fmt.Errorf("%w: 元数据为空", ErrCorrupt)
	}
	if m.Version < 1 || m.Version > FormatVersion {
		return fmt.Errorf("%w: 元数据格式版本 %d 不受支持", ErrCorrupt, m.Version)
	}
	if m.Key != key {
		return fmt.Errorf("%w: 元数据 key %q 与请求 key %q 不符", ErrCorrupt, m.Key, key)
	}
	if m.Size < 0 {
		return fmt.Errorf("%w: 对象大小 %d 为负", ErrCorrupt, m.Size)
	}
	if m.ETag == "" {
		return fmt.Errorf("%w: 缺少 ETag", ErrCorrupt)
	}
	if m.NodeID == "" {
		return fmt.Errorf("%w: 缺少 node_id", ErrCorrupt)
	}
	if m.ObjVersion == 0 {
		return fmt.Errorf("%w: obj_version 必须从 1 开始", ErrCorrupt)
	}
	if m.ContentType == "" {
		return fmt.Errorf("%w: 缺少 content_type", ErrCorrupt)
	}

	var next int64
	var total int64
	for _, b := range m.Blocks {
		if err := validateBlockHash(b.Hash); err != nil {
			return err
		}
		if b.Length <= 0 {
			return fmt.Errorf("%w: 块 %s 长度 %d 非正", ErrCorrupt, b.Hash, b.Length)
		}
		if b.Offset != next {
			return fmt.Errorf("%w: 块 %s 偏移 %d，期望 %d", ErrCorrupt, b.Hash, b.Offset, next)
		}
		next += b.Length
		total += b.Length
	}
	if total != m.Size {
		return fmt.Errorf("%w: 块总长 %d 与对象大小 %d 不符", ErrCorrupt, total, m.Size)
	}
	if m.Size > 0 && len(m.Blocks) == 0 {
		return fmt.Errorf("%w: 非空对象没有块表", ErrCorrupt)
	}
	return nil
}

// BlockHash 返回内容块 SHA-256 的十六进制摘要，与 StreamBlocks 产出的
// BlockDesc.Hash 同一算法。
//
// 内部上传接口用它把网络收到的字节与声明的哈希做比对：PutBlock 本身
// 不重算（本地调用方已算过），校验必须发生在数据从网络进入进程的那一刻。
func BlockHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
