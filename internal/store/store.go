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
	// Now 可注入时间源，便于测试断言 mtime；为零值时用 time.Now。
	Now func() time.Time
	// FS 可注入文件系统实现，用于故障注入测试；为零值时用真实文件系统。
	FS FileSystem
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

	// syncingMeta 标记"当前 fsync 属于元数据写入"，
	// 供 syncFile 在 meta 策略下放行。不是并发安全的，
	// 但只在单次写调用内短暂为真 —— 见 writeFileAtomic。
	syncingMeta bool

	mu    sync.RWMutex
	locks *keyLocker
}

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

	s := &Store{
		root:  opt.Root,
		fs:    fsys,
		fsync: policy,
		now:   now,
		locks: newKeyLocker(),
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

// bucketIsEmpty 报告桶内是否没有对象。
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
	return true, nil
}
