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
		return s.writeFileAtomic(joinPath(s.root, metaFileName), mustJSON(diskMeta{
			FormatVersion: formatVersion,
			CreatedAt:     s.now(),
		}))
	}
	if m.FormatVersion > formatVersion {
		return fmt.Errorf("%w: 磁盘格式版本 %d 高于本二进制支持的 %d，"+
			"请升级 Blink 后再挂载该数据目录",
			ErrCorrupt, m.FormatVersion, formatVersion)
	}
	return nil
}

// loadBucketMeta 读取桶元数据。
func (s *Store) loadBucketMeta(bucket string) (*bucketMeta, error) {
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
