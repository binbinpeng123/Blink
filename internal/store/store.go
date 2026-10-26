package store

import (
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Options 是打开存储的配置。
type Options struct {
	// Root 是数据目录。不存在时自动创建；非空但缺格式文件时拒绝打开。
	Root string
	// FsyncPolicy 为 "always"（默认）或 "meta"，见 fsyncPolicy。
	FsyncPolicy string
	// BlockSize 是大对象的切块大小，零值取 4 MiB。
	BlockSize int64
	// InlineThreshold 是小对象免分片阈值，零值取 64 KiB；
	// 显式赋 0 之外的负值视为禁用小对象快路径。
	InlineThreshold int64
	// Now 可注入时间源，便于测试断言 mtime；为零值时用 time.Now。
	Now func() time.Time
	// FS 可注入文件系统实现，用于故障注入测试；为零值时用真实文件系统。
	FS FileSystem
	// VerifyOnRead 开启读取时逐块校验（CRC32C）。
	//
	// 默认关闭：校验要把数据读进用户态，与 sendfile 零拷贝互斥，
	// 关闭时吞吐最高（见 03 文档"校验读 vs 零拷贝读"）。
	// 打开后只读路径多一次 CRC 计算，换"绝不把损坏数据发给客户端"。
	VerifyOnRead bool
}

// Store 是对象存储引擎的入口。
//
// 并发模型：写路径按对象加锁（见 lock.go），元数据读取无锁 ——
// 因为所有元数据更新都是"整体替换"的原子改名，读者要么看到旧版本、
// 要么看到新版本，不可能看到中间态。
type Store struct {
	root  string
	fs    FileSystem
	fsync fsyncPolicy
	now   func() time.Time

	blockSize       int64
	inlineThreshold int64
	// verifyOnRead 见 Options.VerifyOnRead。只读，不需要同步。
	verifyOnRead bool
	// nodeID 是这块盘的稳定标识，写进每个对象的 meta.json。
	//
	// 单节点场景下它看似多余，但留的是分布式入口：多节点共享/迁移数据时，
	// "这条元数据是哪个节点写的"决定了冲突解决由谁仲裁（04 文档）。
	// 生成后必须稳定 —— 每次启动重新生成会让历史对象的 node_id 失去意义。
	nodeID string
	// cleanupErr 记录启动时非致命清扫的失败，供前端日志/诊断读取。
	// 不让它阻塞 Open：见 Open 里的说明。
	cleanupErr error

	// buckets 缓存已确认存在的桶元数据，见 loadBucketMeta。
	//
	// 每个对象请求都要先回答"桶在不在"，而桶元数据只在建桶时写一次。
	// 不缓存的话，一次 Range 读要打开并解析两个 JSON 文件（bucket.json
	// + meta.json）才能碰到数据块 —— 桶那一份纯属重复劳动。
	//
	// 只缓存成功结果、不做负缓存：桶被删掉后每次请求仍会读盘，
	// 但删除桶是运维动作，频率低到不值得为它引入失效竞态。
	bucketsMu sync.RWMutex
	buckets   map[string]*bucketMeta

	// cors 缓存已加载的桶级 CORS 配置。
	//
	// 与 buckets 分开持锁：CORS 配置的读取频率比桶元数据低，但更新
	// （PutBucketCORS）是整份替换，独立缓存让更新不必等待所有桶元数据读者。
	// 缓存只保存成功加载的结果；未配置的桶每次仍读盘（负缓存需要额外的
	// 失效协议，而"桶没配 CORS"是常态，读一次 ENOENT 的代价可以接受）。
	corsMu sync.RWMutex
	cors   map[string]*CORSConfiguration

	locks *keyLocker
}

// CleanupError 返回启动时清扫失败的详情（没有失败则为 nil）。
func (s *Store) CleanupError() error { return s.cleanupErr }

// 默认参数。与 config 的默认值保持一致，但存储层不依赖 config ——
// 它可能被别的协议前端复用，因此自带兜底。
const (
	defaultBlockSize       = 4 << 20 // 4 MiB
	defaultInlineThreshold = 64 << 10
)

// Open 打开或初始化数据目录。
func Open(opt Options) (*Store, error) {
	if strings.TrimSpace(opt.Root) == "" {
		return nil, errors.New("store: 必须指定数据目录")
	}
	fsys := opt.FS
	if fsys == nil {
		fsys = OSFS()
	}
	now := opt.Now
	if now == nil {
		now = time.Now
	}

	policy := fsyncAlways
	switch opt.FsyncPolicy {
	case "", "always":
		policy = fsyncAlways
	case "meta":
		policy = fsyncMeta
	default:
		return nil, fmt.Errorf("store: 未知的 fsync 策略 %q（可选 always / meta）", opt.FsyncPolicy)
	}

	blockSize := opt.BlockSize
	if blockSize <= 0 {
		blockSize = defaultBlockSize
	}
	inline := opt.InlineThreshold
	if inline == 0 {
		inline = defaultInlineThreshold
	}
	// 负值表示显式关闭小对象快路径（测试与极端内存受限场景用）。
	if inline < 0 {
		inline = 0
	}
	if inline > blockSize {
		// 阈值越过块边界没有意义：一个块之内已经没有"再分片"可言。
		// 宁可拒绝启动，也不静默截断成 blockSize —— 那会让运维以为
		// 自己配的值生效了（与 fsync 策略的校验同一个理由）。
		return nil, fmt.Errorf("store: 内联阈值 %d 大于块大小 %d", inline, blockSize)
	}

	s := &Store{
		root:            opt.Root,
		fs:              fsys,
		fsync:           policy,
		now:             now,
		blockSize:       blockSize,
		inlineThreshold: inline,
		verifyOnRead:    opt.VerifyOnRead,
		buckets:         make(map[string]*bucketMeta),
		cors:            make(map[string]*CORSConfiguration),
		locks:           newKeyLocker(),
	}

	if err := s.fs.MkdirAll(s.root, dirPerm); err != nil {
		return nil, fsErr("创建数据目录", s.root, err)
	}
	if err := s.initDiskMeta(); err != nil {
		return nil, err
	}
	for _, d := range []string{bucketsDirName, casDirName, tmpDirName, multipartDirName} {
		if err := s.fs.MkdirAll(joinPath(s.root, d), dirPerm); err != nil {
			return nil, fsErr("创建目录", d, err)
		}
	}
	// 启动清扫：上次运行中途失败的残留临时文件一律丢弃。
	if err := s.cleanTmp(); err != nil {
		return nil, fsErr("清理临时目录", tmpDirName, err)
	}
	// 再回收过期/残缺的分片上传。放在 cleanTmp 之后：清扫本身可能
	// 写临时文件，先收拾干净再动长期状态。
	//
	// 清扫失败只记录不阻塞启动：一块盘上"有些过期分片没删掉"不影响
	// 任何已有对象的正确性，而拒绝启动会让整个服务不可用。
	if err := s.cleanExpiredUploads(); err != nil {
		s.cleanupErr = err
	}
	return s, nil
}

// Root 返回数据目录。
func (s *Store) Root() string { return s.root }

// bucketNameRe 是 S3 bucket 命名规则。
//
// 3–63 字符，仅小写字母、数字、点、连字符，且必须字母或数字开头结尾。
// 点号合法但会让 TLS 通配符证书失效，这里仍接受 —— 与 AWS 一致。
var bucketNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

// maxKeyBytes 是对象 key 的长度上限（UTF-8 字节）。
// 与 AWS 一致：1024 字节。
const maxKeyBytes = 1024

// validateBucketName 校验 bucket 名。
func validateBucketName(name string) error {
	if !bucketNameRe.MatchString(name) {
		return ErrInvalidBucketName
	}
	// 连续的点会与目录遍历语义混淆（".."), 显式拒绝。
	if strings.Contains(name, "..") {
		return ErrInvalidBucketName
	}
	// 形如 192.168.1.1 的名字是合法 DNS 但 AWS 禁止，
	// 因为它会让 bucket 名与主机名歧义。简单近似：全部为数字与点即拒绝。
	if isAllDigitsAndDots(name) {
		return ErrInvalidBucketName
	}
	return nil
}

func isAllDigitsAndDots(s string) bool {
	for _, r := range s {
		if (r < '0' || r > '9') && r != '.' {
			return false
		}
	}
	return true
}

// validateKey 校验对象 key。
func validateKey(key string) error {
	if key == "" {
		return ErrInvalidKey
	}
	if len(key) > maxKeyBytes {
		return ErrInvalidKey
	}
	return nil
}

// CreateBucket 创建 bucket。
//
// 语义与 AWS 的差异：AWS 的 PUT bucket 在**自己已拥有**时返回
// 409 BucketAlreadyOwnedByYou，在**他人拥有**时返回 409 BucketAlreadyExists。
// 单节点单租户下没有"他人"，因此统一返回 ErrBucketExists，
// 由 s3api 映射为 BucketAlreadyOwnedByYou —— 这是 `aws s3 mb` 期望的结果。
func (s *Store) CreateBucket(name string) error {
	if err := validateBucketName(name); err != nil {
		return err
	}
	if _, err := s.loadBucketMeta(name); err == nil {
		return ErrBucketExists
	} else if !errors.Is(err, ErrBucketNotFound) {
		return err
	}

	// 先建桶元数据，最后建目录会让"目录存在但元数据缺失"成为中间态。
	// 反过来则安全：目录不存在时任何对象操作都会得到 NoSuchBucket。
	if err := s.fs.MkdirAll(s.objectRoot(name), dirPerm); err != nil {
		return fsErr("创建桶目录", name, err)
	}
	meta := bucketMeta{Name: name, CreatedAt: s.now().UTC()}
	if err := s.writeFileAtomic(s.bucketMetaPath(name), mustJSON(meta)); err != nil {
		// 回滚目录，避免留下"空目录但无元数据"的残骸。
		_ = s.fs.RemoveAll(s.bucketDir(name))
		return fsErr("写入桶元数据", name, err)
	}
	s.cacheBucketMeta(name, &meta)
	return nil
}

// DeleteBucket 删除空 bucket。非空返回 ErrBucketNotEmpty。
func (s *Store) DeleteBucket(name string) error {
	if _, err := s.loadBucketMeta(name); err != nil {
		return err
	}
	// 检查是否为空：objects/ 下不得有任何对象目录。
	empty, err := s.bucketIsEmpty(name)
	if err != nil {
		return err
	}
	if !empty {
		return ErrBucketNotEmpty
	}
	if err := s.fs.RemoveAll(s.bucketDir(name)); err != nil {
		return fsErr("删除桶", name, err)
	}
	// 目录已经消失，缓存必须立刻失效 —— 即使下面的 SyncDir 失败，
	// 桶的数据也已经不存在了，再让后续请求命中缓存会让"刚删掉的桶"
	// 继续接受写入。
	s.dropBucketMeta(name)
	s.dropBucketCORS(name)
	return s.fs.SyncDir(joinPath(s.root, bucketsDirName))
}

// BucketExists 报告 bucket 是否存在。
func (s *Store) BucketExists(name string) (bool, error) {
	if err := validateBucketName(name); err != nil {
		return false, err
	}
	_, err := s.loadBucketMeta(name)
	if errors.Is(err, ErrBucketNotFound) {
		return false, nil
	}
	return err == nil, err
}

// ListBuckets 返回全部 bucket 名，按字节序排序。
func (s *Store) ListBuckets() ([]string, error) {
	entries, err := s.fs.ReadDir(joinPath(s.root, bucketsDirName))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fsErr("读取 buckets 目录", bucketsDirName, err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		// 目录存在但元数据缺失 = 创建中途失败，跳过而非报错，
		// 否则一个半成品桶会让整个 ListBuckets 失败。
		if _, err := s.loadBucketMeta(e.Name()); err != nil {
			continue
		}
		names = append(names, e.Name())
	}
	sortStrings(names)
	return names, nil
}

// BucketCreatedAt 返回桶的创建时间，用于 ListBuckets 的 CreationDate。
func (s *Store) BucketCreatedAt(name string) (time.Time, error) {
	m, err := s.loadBucketMeta(name)
	if err != nil {
		return time.Time{}, err
	}
	return m.CreatedAt, nil
}

// objectRoot 返回桶内的 objects 目录；bucket 不存在时先建。
func (s *Store) objectRoot(bucket string) string {
	return joinPath(s.bucketDir(bucket), objectsDirName)
}

// bucketIsEmpty 报告桶内是否既没有对象、也没有进行中的分片上传。
//
// 必须同时检查分片上传（02 文档）：只删掉对象就允许删桶，会让仍在
// 上传的客户端在 Complete 时撞上一个不存在的桶，而不是得到
// "桶非空"这个可理解的拒绝。
func (s *Store) bucketIsEmpty(bucket string) (bool, error) {
	root := s.objectRoot(bucket)
	prefixes, err := s.fs.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, fsErr("读取 objects 目录", bucket, err)
	}
	for _, p := range prefixes {
		if !p.IsDir() {
			continue
		}
		objs, err := s.fs.ReadDir(joinPath(root, p.Name()))
		if err != nil {
			return false, fsErr("读取对象目录", bucket, err)
		}
		if len(objs) > 0 {
			return false, nil
		}
	}
	has, err := s.bucketHasUploads(bucket)
	if err != nil {
		return false, err
	}
	if has {
		return false, nil
	}
	return true, nil
}
