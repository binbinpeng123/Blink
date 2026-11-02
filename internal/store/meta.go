package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"time"
)

// diskMeta 是 <data-dir>/blink-meta.json 的内容。
//
// 这个文件唯一的作用是回答"这块盘是什么格式、能不能被本二进制安全地读"。
// 它必须存在的另一个原因：用它区分"空目录"和"别人的目录" ——
// 如果启动时发现目录非空却没有 blink-meta.json，说明用户大概率指错了路径，
// 此时**拒绝启动**远比在里面创建 buckets/ 安全。
type diskMeta struct {
	FormatVersion int       `json:"format_version"`
	CreatedAt     time.Time `json:"created_at"`
	// NodeID 是这块盘的稳定标识。
	//
	// 单节点下它不参与任何判断，只是"留边端分布式入口"的最小一步：
	// 将来多节点共享或迁移数据时，对象 meta.json 里的 node_id 能让
	// 读者判断"这条记录出自哪个节点"，从而决定冲突由谁仲裁（04 文档）。
	// 生成一次后持久化；每次启动重新生成等于没写。
	NodeID string `json:"node_id"`
}

// bucketMeta 是 <bucket>/bucket.json 的内容。
//
// 字段刻意少：桶级配置（版本控制、生命周期、访问策略）本版本均不支持，
// 因此现在只有名字与创建时间；将来加字段时靠 json 的向后兼容自然演进。
type bucketMeta struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

// loadDiskMeta 读取磁盘格式版本；文件不存在时返回 (nil, nil)。
func (s *Store) loadDiskMeta() (*diskMeta, error) {
	data, err := readFile(s.fs, joinPath(s.root, metaFileName))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fsErr("读取", metaFileName, err)
	}
	var m diskMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: %s 不是合法 JSON: %v", ErrCorrupt, metaFileName, err)
	}
	return &m, nil
}

// initDiskMeta 首次初始化或校验磁盘格式版本。
func (s *Store) initDiskMeta() error {
	m, err := s.loadDiskMeta()
	if err != nil {
		return err
	}
	if m == nil {
		// 首次初始化：目录必须为空，否则拒绝 —— 见 diskMeta 的说明。
		entries, err := s.fs.ReadDir(s.root)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fsErr("读取目录", s.root, err)
		}
		if len(entries) > 0 {
			return fmt.Errorf("%w: 数据目录 %s 非空但缺少 %s，"+
				"请确认路径是否正确（或清空该目录后重试）",
				ErrCorrupt, s.root, metaFileName)
		}
		id := randHex(16)
		if err := s.writeFileAtomic(joinPath(s.root, metaFileName), mustJSON(diskMeta{
			FormatVersion: formatVersion,
			CreatedAt:     s.now(),
			NodeID:        id,
		})); err != nil {
			return err
		}
		s.nodeID = id
		return nil
	}
	if m.FormatVersion > formatVersion {
		return fmt.Errorf("%w: 磁盘格式版本 %d 高于本二进制支持的 %d，"+
			"请升级 Blink 后再挂载该数据目录",
			ErrCorrupt, m.FormatVersion, formatVersion)
	}
	if m.NodeID == "" {
		// 早期版本写下的数据目录没有 node_id：补一个并落盘。
		// 不能"只放在内存里" —— 那样每次重启 node_id 都会变，
		// 历史对象与新对象的 node_id 分属两套取值，等于没写。
		m.NodeID = randHex(16)
		if err := s.writeFileAtomic(joinPath(s.root, metaFileName), mustJSON(m)); err != nil {
			return err
		}
	}
	s.nodeID = m.NodeID
	return nil
}

// loadBucketMeta 读取桶元数据，优先命中进程内缓存。
//
// 缓存的安全性来自两点：bucket.json 的唯一写入点是 CreateBucket，
// 唯一删除点是 DeleteBucket，两者都会同步更新缓存；且本进程独占数据目录
// （单节点单实例是 01 文档的部署前提），不存在"别的进程改了但缓存不知道"。
// 因此命中缓存的答案与读盘永远一致。
//
// 返回的 *bucketMeta 与缓存共享，调用方只读。
func (s *Store) loadBucketMeta(bucket string) (*bucketMeta, error) {
	if m := s.cachedBucketMeta(bucket); m != nil {
		return m, nil
	}
	data, err := readFile(s.fs, s.bucketMetaPath(bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrBucketNotFound
	}
	if err != nil {
		return nil, fsErr("读取桶元数据", bucket, err)
	}
	var m bucketMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: 桶 %s 的 %s 不是合法 JSON: %v",
			ErrCorrupt, bucket, bucketMetaFileName, err)
	}
	s.cacheBucketMeta(bucket, &m)
	return &m, nil
}

// cachedBucketMeta 返回缓存中的桶元数据；未命中返回 nil。
func (s *Store) cachedBucketMeta(bucket string) *bucketMeta {
	s.bucketsMu.RLock()
	defer s.bucketsMu.RUnlock()
	return s.buckets[bucket]
}

// cacheBucketMeta 记录一条已确认存在的桶元数据。
func (s *Store) cacheBucketMeta(bucket string, m *bucketMeta) {
	s.bucketsMu.Lock()
	defer s.bucketsMu.Unlock()
	s.buckets[bucket] = m
}

// dropBucketMeta 让缓存失效，供 DeleteBucket 调用。
func (s *Store) dropBucketMeta(bucket string) {
	s.bucketsMu.Lock()
	defer s.bucketsMu.Unlock()
	delete(s.buckets, bucket)
}

// objectMeta 是对象目录下 meta.json 的内容，即 03 文档的字段表。
//
// 块描述**内嵌**在这个文件里，而不是单独的 blocks.bin：一次写入只有一个
// 文件被 rename，因此"对象可见"与"块表完整"必然同时成立。拆成两个文件
// 就需要两次 rename，中间任何一次崩溃都会留下"块表存在但指向一半的块"
// 或反之，恰好破坏 03 文档"任何时刻都不会出现对象存在但内容残缺"的保证。
//
// 代价是元数据随对象增大而增大：4 MiB 块下 1 TiB 对象约 26 万个块描述，
// 按本结构约 30 MB JSON —— 超过 readFile 的 32 MiB 上限，属于已知限制
// （见 known-limitations.md）。v1 的单次 PUT 上限是 5 GiB，约 1300 块、
// 约 150 KB，远在安全范围内。
type objectMeta struct {
	Version     int       `json:"version"`
	Key         string    `json:"key"`
	Size        int64     `json:"size"`
	ETag        string    `json:"etag"`
	ContentType string    `json:"content_type"`
	Modified    time.Time `json:"modified"`
	// UserMeta 原样保存 x-amz-meta-* 的值。
	UserMeta map[string]string `json:"user_meta,omitempty"`
	// Blocks 是**有序**块描述列表：顺序即对象内容的顺序。
	Blocks []blockDesc `json:"blocks"`
	// Checksum 是客户端声明的对象级校验和（base64）；未声明时为空。
	// 它是 base64 文本而非二进制，因为回传时要逐字节还原客户端的值。
	Checksum string `json:"checksum,omitempty"`
	// ChecksumAlgorithm 必须与 Checksum 同时记录，否则 ChecksumMode
	// 无法知道这个值是哪种算法算出来的（03 文档）。
	ChecksumAlgorithm string `json:"checksum_algorithm,omitempty"`
	// ChecksumType 记录 FULL_OBJECT 还是 COMPOSITE（分片上传才可能非空）。
	//
	// 必须与 Checksum 一起回传：同名头在两种类型下的值格式不同
	// （COMPOSITE 带 "-N" 后缀），客户端要据此判断该用哪种方式复算。
	ChecksumType string `json:"checksum_type,omitempty"`
	// NodeID 是写入该版本的节点标识（见 diskMeta.NodeID）。
	NodeID string `json:"node_id"`
	// ObjVersion 是同一 key 的覆盖写计数，从 1 开始。
	//
	// 单节点下它只用于观测"这个 key 被覆盖过几次"，但它同时是
	// 分布式场景判断"哪份更新"的依据，因此 v1 就开始记录。
	ObjVersion uint64 `json:"obj_version"`
}

// blockDesc 描述对象中的一个块。
//
// Hash 既是 CAS 的定位键，也是完整性凭据：读取时重算内容哈希与它比对，
// 就能发现"文件还在但内容变了"（03 文档的"完整性自校验"）。
type blockDesc struct {
	Hash   string `json:"hash"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
	// CRC32C 是块内容的 Castagnoli 校验和，供后台 scrub 快速比对。
	//
	// 与 Hash 并存不是冗余：SHA256 用于"内容就是它宣称的东西"（安全性），
	// CRC32C 用于"盘上的位没翻转"（可廉价全量重算，有硬件指令）。
	CRC32C uint32 `json:"crc32c"`
}

// loadObjectMeta 读取对象元数据；对象不存在时返回 ErrNoSuchKey。
//
// 调用方若需要区分"桶不存在"，必须先自己确认桶存在 —— 本函数只看
// 对象路径，桶缺失时同样会得到 fs.ErrNotExist，进而报 ErrNoSuchKey。
func (s *Store) loadObjectMeta(bucket, key string) (*objectMeta, error) {
	m, err := s.readObjectMetaAt(s.objectMetaPath(bucket, key))
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil, ErrNoSuchKey
	case err != nil:
		return nil, fsErr("读取对象元数据", key, err)
	}
	return m, nil
}

// readObjectMetaAt 按路径读取并解析对象元数据，不做"不存在 → ErrNoSuchKey"的
// 翻译：LIST 遍历时"meta 不见了"只是并发删除，不是客户端错误。
//
// 文件不存在时返回的错误满足 errors.Is(err, fs.ErrNotExist)，由调用方决定
// 语义；解析失败一律是 ErrCorrupt —— 损坏绝不回落成"不存在"。
func (s *Store) readObjectMetaAt(path string) (*objectMeta, error) {
	data, err := readFile(s.fs, path)
	if err != nil {
		return nil, err
	}
	var m objectMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: 对象元数据 %s 不是合法 JSON: %v", ErrCorrupt, path, err)
	}
	return &m, nil
}

// mustJSON 序列化元数据。
//
// 元数据都是本包自己构造的简单结构，序列化不可能失败；
// 真要失败（例如含 NaN）也是编程错误，panic 好过返回一个"看起来成功"的空文件。
func mustJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic("store: 元数据序列化失败: " + err.Error())
	}
	return data
}

// readFile 是读取小文件的辅助函数（元数据都是小文件，一次性读入最简单）。
func readFile(fsys FileSystem, path string) ([]byte, error) {
	f, err := fsys.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// 元数据文件理论上很小，但损坏的盘上可能出现超大文件。
	// 设上限避免读进内存后才发现异常 —— 32 MiB 远超任何合法元数据。
	const maxMetaBytes = 32 << 20
	r := &limitedReader{r: f, remaining: maxMetaBytes}
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if r.remaining == 0 {
		return nil, fmt.Errorf("%w: 元数据文件 %s 超过 %d 字节上限", ErrCorrupt, path, maxMetaBytes)
	}
	return out, nil
}

// limitedReader 把读取量限制在 remaining 内；用尽即报错而不是返回 EOF ——
// EOF 会被上层当成"文件正常结束"，从而把截断的元数据读成合法 JSON。
type limitedReader struct {
	r         io.Reader
	remaining int
}

func (l *limitedReader) Read(p []byte) (int, error) {
	if l.remaining <= 0 {
		return 0, io.ErrNoProgress
	}
	if len(p) > l.remaining {
		p = p[:l.remaining]
	}
	n, err := l.r.Read(p)
	l.remaining -= n
	return n, err
}
