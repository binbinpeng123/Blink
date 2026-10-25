package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

// ListOptions 是 ListObjects 的参数。
//
// 游标与 StartAfter 二选一：Cursor 非 nil 时优先，且两者都由 s3api 层
// 决定（S3 规定 continuation-token 优先于 start-after）。
type ListOptions struct {
	// Prefix 只返回 key 以它开头的对象（字节前缀，不做任何归一化）。
	Prefix string
	// Delimiter 非空时，把 key 中它之后的部分折叠为一个公共前缀。
	// 最常见的取值是 "/"，用于模拟目录浏览。
	Delimiter string
	// StartAfter 从**严格大于**该 key 的位置开始返回。
	StartAfter string
	// Cursor 是上一页返回的续页位置，优先于 StartAfter。
	Cursor *ListCursor
	// MaxKeys 是本页最多的条目数（对象 + 公共前缀合计）。
	// <= 0 表示返回空页 —— S3 的 max-keys=0 就是这个语义。
	MaxKeys int
}

// ListCursor 标记"上一页停在哪里"。
//
// IsPrefix 是刻意保留的信息：若上一页最后一条是公共前缀，下一页必须
// 跳过该前缀下的**全部** key。只记 After 会导致每次翻页都把同一个
// 公共前缀再返回一遍，客户端在 `aws s3 ls --recursive` 里会看到重复目录。
type ListCursor struct {
	After    string `json:"after"`
	IsPrefix bool   `json:"after_is_prefix,omitempty"`
}

// ListObject 是 LIST 返回的单个对象摘要。
type ListObject struct {
	Key      string
	Size     int64
	ETag     string
	Modified time.Time
}

// ListResult 是 ListObjects 的结果。
//
// Objects 与 CommonPrefixes 各自有序，但两者的**相对顺序**不保留：
// S3 的响应体天然是"所有 Contents 再所有 CommonPrefixes"，
// 客户端不依赖交错顺序，而 Next 游标记的是按 key 序的最后一条条目。
type ListResult struct {
	Objects        []ListObject
	CommonPrefixes []string
	// KeyCount 是 Objects + CommonPrefixes 的条数（S3 的 KeyCount 语义）。
	KeyCount    int
	IsTruncated bool
	// Next 在 IsTruncated 时非 nil，作为下一次请求的 Cursor。
	Next *ListCursor
}

// ListObjects 列举桶内对象。
//
// 实现方式是全量扫描 objects/ 并读取每个 meta.json —— 目录名是 key 的
// sha256，不读元数据就还原不出原始 key，因此无法借助目录名做范围定位。
// 这是 03 文档标注的最大性能风险（创新 D 小对象打包要解决的问题）。
//
// 切片 9 在不改变"仍要读全部元数据"这一前提下削减了两项成本：
// 分片之间并行扫描（readAllObjectMetas），以及 LIST 只解码它用到的字段
// （listMeta）。前后对照见 docs/05 与 SESSION_MEMORY 中的基准数字。
//
// 一致性：读取的是"某一时刻的目录快照"。并发写入要么以完整 meta.json
// 出现、要么不出现（写入以原子 rename 提交）；并发删除表现为某个
// meta.json 读取时已不存在，此时跳过该对象而不是让整个 LIST 失败。
// 因此结果是强一致的（不会看到半个对象），但不保证是某个单一瞬间的快照。
func (s *Store) ListObjects(bucket string, opt ListOptions) (*ListResult, error) {
	if err := validateBucketName(bucket); err != nil {
		return nil, err
	}
	// 桶不存在必须回 NoSuchBucket —— 空桶是 200 + KeyCount=0，
	// 两者对客户端的意义完全不同（02 文档）。
	if _, err := s.loadBucketMeta(bucket); err != nil {
		return nil, err
	}

	res := &ListResult{}
	if opt.MaxKeys <= 0 {
		return res, nil
	}

	metas, err := s.readAllObjectMetas(bucket)
	if err != nil {
		return nil, err
	}
	// Go 的字符串比较就是逐字节比较，因此这里天然满足 02 文档要求的
	// UTF-8 字节序（不是 locale 序，也不是 rune 序）。
	sort.Slice(metas, func(i, j int) bool { return metas[i].Key < metas[j].Key })

	var last ListCursor
	for _, m := range metas {
		key := m.Key
		if opt.Prefix != "" && !strings.HasPrefix(key, opt.Prefix) {
			continue
		}
		if !pastCursor(key, opt) {
			continue
		}

		if opt.Delimiter != "" {
			rest := key[len(opt.Prefix):]
			if i := strings.Index(rest, opt.Delimiter); i >= 0 {
				prefix := opt.Prefix + rest[:i+len(opt.Delimiter)]
				// 同一公共前缀下的其余 key 已在第一条时折叠完毕。
				// 这个判断必须在 maxKeys 检查**之前**：重复 key 不占额度，
				// 否则 max-keys=2 时第二页会凭空少一条。
				if prefix == last.After && last.IsPrefix {
					continue
				}
				if res.KeyCount >= opt.MaxKeys {
					res.IsTruncated = true
					break
				}
				res.CommonPrefixes = append(res.CommonPrefixes, prefix)
				res.KeyCount++
				last = ListCursor{After: prefix, IsPrefix: true}
				continue
			}
		}

		if res.KeyCount >= opt.MaxKeys {
			res.IsTruncated = true
			break
		}
		res.Objects = append(res.Objects, ListObject{
			Key:      key,
			Size:     m.Size,
			ETag:     m.ETag,
			Modified: m.Modified,
		})
		res.KeyCount++
		last = ListCursor{After: key}
	}

	if res.IsTruncated {
		next := last
		res.Next = &next
	}
	return res, nil
}

// pastCursor 判断 key 是否晚于请求的起始位置。
//
// 返回 false 表示该 key 在起始位置之前（或恰好等于它），必须跳过。
// 游标是前缀时，该前缀下的所有 key 一律跳过 —— 它们已经被折叠成
// 上一页的那一条 CommonPrefixes。
func pastCursor(key string, opt ListOptions) bool {
	after := opt.StartAfter
	isPrefix := false
	if opt.Cursor != nil {
		after, isPrefix = opt.Cursor.After, opt.Cursor.IsPrefix
	}
	if after == "" {
		return true
	}
	if key <= after {
		return false
	}
	if isPrefix && strings.HasPrefix(key, after) {
		return false
	}
	return true
}

// listMeta 是 LIST 需要的全部对象元数据。
//
// 刻意不复用 objectMeta：完整结构里的 blocks 在真实负载下可能很大
// （4 MiB 块、64 MiB 对象就有 16 条块描述，GB 级对象上千条），而清单
// 一条也用不到。encoding/json 对结构体里没有的字段仍然要扫描 token，
// 但不再为每个块分配 blockDesc、也不再解转义哈希字符串 —— 对象越大，
// 省下的分配与解析时间越多。
//
// 这不改变"目录布局是稳定契约"这一事实：字段名与 objectMeta 一致，
// 缺字段/字段类型错误照样在解码时失败并报 ErrCorrupt。
type listMeta struct {
	Key      string    `json:"key"`
	Size     int64     `json:"size"`
	ETag     string    `json:"etag"`
	Modified time.Time `json:"modified"`
}

// readAllObjectMetas 读出桶内全部对象元数据。
//
// 目录布局为 objects/<hash 前 2 位>/<hash>/meta.json（见 paths.go）。
// 目录名与 meta 里的 Key 必须互相印证：不一致意味着有人手工动过数据目录，
// 此时报 ErrCorrupt 而不是"跳过" —— 否则 LIST 会静默少返回对象，
// 而这种缺数据只能靠用户发现对不上账。
//
// 每个分片是一个独立任务，用有界 worker 池并行扫描：单次 LIST 对 10 万
// 对象要做 10 万次"打开→读→解析"，这些操作之间没有任何依赖，
// 串行扫描等于把本地盘的队列深度浪费到 1。结果按分片序拼回，
// worker 之间不共享切片，无需加锁；错误也按分片序挑第一个，
// 使并发下的失败原因可复现。
func (s *Store) readAllObjectMetas(bucket string) ([]listMeta, error) {
	root := s.objectRoot(bucket)
	shards, err := s.fs.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		// 桶元数据在、objects/ 不在：创建中途失败的残骸。
		// 对客户端而言仍是空桶，不该因此让整个 LIST 500。
		return nil, nil
	}
	if err != nil {
		return nil, fsErr("读取 objects 目录", bucket, err)
	}

	names := make([]string, 0, len(shards))
	for _, shard := range shards {
		if shard.IsDir() {
			names = append(names, shard.Name())
		}
	}
	if len(names) == 0 {
		return nil, nil
	}

	parts := make([][]listMeta, len(names))
	errs := make([]error, len(names))
	workers := listScanWorkers(len(names))
	if workers <= 1 {
		for i, name := range names {
			parts[i], errs[i] = s.readShardMetas(root, name)
		}
	} else {
		var wg sync.WaitGroup
		next := make(chan int)
		wg.Add(workers)
		for w := 0; w < workers; w++ {
			go func() {
				defer wg.Done()
				for i := range next {
					parts[i], errs[i] = s.readShardMetas(root, names[i])
				}
			}()
		}
		for i := range names {
			next <- i
		}
		close(next)
		wg.Wait()
	}

	total := 0
	for i, err := range errs {
		if err != nil {
			return nil, err
		}
		total += len(parts[i])
	}
	metas := make([]listMeta, 0, total)
	for _, p := range parts {
		metas = append(metas, p...)
	}
	return metas, nil
}

// listScanWorkers 返回扫描分片时使用的并发度。
//
// 上限 16：分片总共只有 256 个，再高的并发只会让 SSD 队列与内核目录锁
// 互相排队，同时抬高单次 LIST 的内存峰值（每个 worker 至少持有一个
// 分片的全部结果）。
func listScanWorkers(shards int) int {
	n := runtime.GOMAXPROCS(0)
	if n > 16 {
		n = 16
	}
	if n > shards {
		n = shards
	}
	if n < 1 {
		n = 1
	}
	return n
}

// readShardMetas 扫描一个分片目录下的全部对象。
func (s *Store) readShardMetas(root, shardName string) ([]listMeta, error) {
	entries, err := s.fs.ReadDir(joinPath(root, shardName))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil // 并发删除让整个分片目录消失
	}
	if err != nil {
		return nil, fsErr("读取对象分片目录", shardName, err)
	}
	out := make([]listMeta, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		metaPath := joinPath(root, shardName, e.Name(), objectMetaFileName)
		m, err := s.readListMetaAt(metaPath)
		if errors.Is(err, fs.ErrNotExist) {
			continue // 并发删除：对象在遍历途中没了
		}
		if err != nil {
			return nil, err
		}
		want := hashKey(m.Key)
		if m.Key == "" || want != e.Name() || want[:2] != shardName {
			return nil, fmt.Errorf("%w: 目录 %s/%s 与元数据 key %q 不一致",
				ErrCorrupt, shardName, e.Name(), m.Key)
		}
		out = append(out, m)
	}
	return out, nil
}

// readListMetaAt 按路径读取 LIST 需要的字段。
//
// 错误语义与 readObjectMetaAt 一致：文件不存在时满足
// errors.Is(err, fs.ErrNotExist)（并发删除，由调用方跳过），
// 解析失败一律是 ErrCorrupt。
func (s *Store) readListMetaAt(path string) (listMeta, error) {
	data, err := readFile(s.fs, path)
	if err != nil {
		return listMeta{}, err
	}
	var m listMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return listMeta{}, fmt.Errorf("%w: 对象元数据 %s 不是合法 JSON: %v", ErrCorrupt, path, err)
	}
	return m, nil
}
