package store

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sort"

	"github.com/binbinpeng123/Blink/internal/checksum"
)

// ObjectReader 是一个已打开对象的只读视图。
//
// 打开时一次性读入并原子快照 meta.json，因此**后续的覆盖写不会影响**
// 这个读取器：它手里的块表指向的 CAS 块是不可变的，而覆盖写只会改变
// meta.json 的指向。这正是 02 文档"并发读者要么看到完整旧版本、要么看到
// 完整新版本"在实现层面的依据。
//
// 与之对应，读路径**不加 key 锁**：加锁只会让大对象的读取阻塞写入，
// 而上面说的不可变性已经保证了正确性。DeleteObject 也只删除元数据，
// 已打开的读取器仍能读完自己的块（块由 GC 回收）。
type ObjectReader struct {
	s    *Store
	meta *objectMeta
	info ObjectInfo
}

// OpenObject 打开对象并返回读取器；对象不存在时返回 ErrNoSuchKey，
// 桶不存在时返回 ErrBucketNotFound（与 PutObject 的区分方式一致）。
func (s *Store) OpenObject(bucket, key string) (*ObjectReader, error) {
	if err := validateBucketName(bucket); err != nil {
		return nil, err
	}
	if err := validateKey(key); err != nil {
		return nil, err
	}
	// 先确认桶存在：否则桶没建会被下面的 loadObjectMeta 报成 NoSuchKey，
	// 客户端拿到的错误码就与"文件没传"混为一谈了。
	if _, err := s.loadBucketMeta(bucket); err != nil {
		return nil, err
	}
	m, err := s.loadObjectMeta(bucket, key)
	if err != nil {
		return nil, err
	}
	return &ObjectReader{s: s, meta: m, info: objectInfoFromMeta(m)}, nil
}

// Info 返回对象的元数据快照，供协议层生成响应头。
func (r *ObjectReader) Info() ObjectInfo {
	return r.info
}

// objectInfoFromMeta 把磁盘格式转成对外结构。
//
// 两个结构刻意分开：objectMeta 是磁盘契约（字段名进 JSON、改动要兼容旧版本），
// ObjectInfo 是调用方契约（只含"响应需要什么"）。把两者绑在一起，
// 将来加一个纯内部字段就得连带改协议层。
func objectInfoFromMeta(m *objectMeta) ObjectInfo {
	return ObjectInfo{
		Key:               m.Key,
		Size:              m.Size,
		ETag:              m.ETag,
		Modified:          m.Modified,
		ContentType:       m.ContentType,
		UserMeta:          m.UserMeta,
		Checksum:          m.Checksum,
		ChecksumAlgorithm: checksum.Algorithm(m.ChecksumAlgorithm),
		ChecksumType:      m.ChecksumType,
		BlockCount:        len(m.Blocks),
		Version:           m.ObjVersion,
	}
}

// WriteRangeTo 把对象的 [offset, offset+length) 字节写入 w。
//
// 调用方必须先按 Info().Size 校验范围；本函数只做防御性检查，
// 越界返回 ErrCorrupt 而不是截断 —— 越界意味着调用方算错了，
// 静默少写会让 HTTP 层发出与 Content-Length 不符的响应。
//
// 整块读取走 io.Copy，这是"sendfile 零拷贝"（创新 C）的落点：
// CAS 块就是 *os.File，io.Copy 能沿 os.File.WriteTo → sendfile 直达
// 网络连接，不经过用户态缓冲。部分块必须先 seek 再定量拷贝，
// 这一步拿不到 sendfile，但只影响每个 Range 请求的首尾各一块。
func (r *ObjectReader) WriteRangeTo(w io.Writer, offset, length int64) error {
	if offset < 0 || length < 0 || offset > r.info.Size || length > r.info.Size-offset {
		return fmt.Errorf("%w: 读取范围 [%d,%d) 超出对象大小 %d",
			ErrCorrupt, offset, offset+length, r.info.Size)
	}
	if length == 0 {
		return nil
	}

	end := offset + length
	// 块表按 Offset 升序（写路径按顺序追加），用二分找到第一个可能
	// 覆盖 offset 的块。对象可能有上万个块，线性扫描在 Range 请求上
	// 不值得。
	i := sort.Search(len(r.meta.Blocks), func(i int) bool {
		b := r.meta.Blocks[i]
		return b.Offset+b.Length > offset
	})

	for ; i < len(r.meta.Blocks); i++ {
		b := r.meta.Blocks[i]
		if b.Offset >= end {
			break
		}
		start := max(offset, b.Offset)
		stop := min(end, b.Offset+b.Length)
		if start >= stop {
			continue
		}
		if err := r.s.copyBlockSegment(w, b, start, stop); err != nil {
			return err
		}
	}
	return nil
}

// copyBlockSegment 把块 b 的 [start, stop) 字节写入 w。
//
// 每次打开都先 Stat 校验文件长度与块描述一致。这一步很便宜（每 4 MiB
// 一次 fstat），但能把"文件被截断/替换"从"客户端收到半个对象"变成
// 一个明确的 ErrCorrupt —— 前者在 HTTP 层表现为连接中断，几乎无法排查。
func (s *Store) copyBlockSegment(w io.Writer, b blockDesc, start, stop int64) error {
	path := s.casPath(b.Hash)
	st, err := s.fs.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: 块 %s 缺失（对象元数据仍引用它）", ErrCorrupt, b.Hash)
		}
		return fsErr("检查块", b.Hash, err)
	}
	if st.Size() != b.Length {
		return fmt.Errorf("%w: 块 %s 长度 %d，块表记录 %d",
			ErrCorrupt, b.Hash, st.Size(), b.Length)
	}

	f, err := s.fs.Open(path)
	if err != nil {
		return fsErr("打开块", b.Hash, err)
	}
	defer func() { _ = f.Close() }()

	rel := start - b.Offset
	n := stop - start
	if s.verifyOnRead {
		// 校验读：整块读入并核对 CRC32C 后才写出任何字节。
		// 这一步刻意放在所有写操作之前 —— 先写后校验等于把损坏数据
		// 发给了客户端，HTTP 响应无法撤回。
		data, err := s.readBlockVerified(f, b)
		if err != nil {
			return err
		}
		if rel < 0 || n < 0 || rel+n > int64(len(data)) {
			return fmt.Errorf("%w: 块 %s 的区间 [%d,%d) 越界", ErrCorrupt, b.Hash, rel, rel+n)
		}
		_, err = w.Write(data[rel : rel+n])
		return err
	}
	if rel == 0 && n == b.Length {
		// 整块：保留 sendfile 路径。上面的 Stat 已保证文件长度恰好是
		// b.Length，因此 io.Copy 不会多读出一个字节。
		if _, err := io.Copy(w, f); err != nil {
			return err
		}
		return nil
	}

	// 部分块：先定位。FileSystem 接口没有 Seek（对象存储后端可能不支持
	// 随机读），因此按类型断言；不支持的实现退化为"丢弃 rel 字节"，
	// 语义相同，只是慢。
	if rel > 0 {
		if seeker, ok := f.(io.Seeker); ok {
			if _, err := seeker.Seek(rel, io.SeekStart); err != nil {
				return fsErr("定位块", b.Hash, err)
			}
		} else if _, err := io.CopyN(io.Discard, f, rel); err != nil {
			return fsErr("跳过块前缀", b.Hash, err)
		}
	}
	if _, err := io.CopyN(w, f, n); err != nil {
		return err
	}
	return nil
}

// DeleteObject 删除对象当前版本的元数据。
//
// 幂等：对象不存在也返回 nil（02 文档）。桶不存在则返回 ErrBucketNotFound ——
// 这一项刻意不幂等，因为"桶都没了"和"对象不在"对客户端是两种不同的状况，
// 前者通常意味着建桶步骤失败了。
//
// 只删除 meta.json 所在的目录，**不碰 CAS 块**：块可能被其他 key 去重引用
// （创新 A），而且是内容寻址的不可变文件，只能由 GC 在确认无引用后回收。
func (s *Store) DeleteObject(bucket, key string) error {
	if err := validateBucketName(bucket); err != nil {
		return err
	}
	if err := validateKey(key); err != nil {
		return err
	}
	if _, err := s.loadBucketMeta(bucket); err != nil {
		return err
	}

	unlock := s.locks.Lock(bucket + "\x00" + key)
	defer unlock()

	// RemoveAll 对不存在的路径返回 nil，正好就是"幂等删除"需要的语义。
	if err := s.fs.RemoveAll(s.objectDir(bucket, key)); err != nil {
		return fsErr("删除对象", key, err)
	}
	return nil
}
