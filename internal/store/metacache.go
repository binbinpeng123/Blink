package store

import (
	"container/list"
	"sync"
)

// objectMetaCache 是对象元数据的有界 LRU 缓存。
//
// 为什么需要它：每个 GET/HEAD 都要打开并解析 meta.json，每个覆盖写还要
// 再读一次旧版本。边端推理服务会反复读取同一批模型与配置，稳定热点的
// meta 是一段纯重复劳动；命中缓存后一次 1 KiB GET 不再触碰 meta 文件。
//
// 边界：
//   - 只缓存成功加载的元数据，不做负缓存 —— "对象不存在"的验证仍走盘，
//     避免删除/新建后需要一套额外的失效协议。
//   - 按估算字节数与条目数双重限界，防止大块表对象把内存撑爆。
//   - 返回的 *ObjectMeta 是**只读快照**。写入路径不会修改已发布的 meta，
//     读取路径也只读；调用方不得修改返回对象。
const (
	defaultMetaCacheBytes   = 4 << 20
	defaultMetaCacheEntries = 1024
	// maxCachedMetaBytes 单条上限：超大块表（如 5 GiB 对象的 1300 个块）
	// 仍可缓存；超过 128 KiB 的极端元数据直接绕过缓存，避免一条就把
	// 整个缓存挤空。
	maxCachedMetaBytes = 128 << 10
)

type metaCacheKey struct {
	bucket string
	key    string
}

type metaCacheEntry struct {
	key  metaCacheKey
	meta *ObjectMeta
	cost int64
}

// objectMetaCache 是非并发安全的 LRU 的并发封装。
type objectMetaCache struct {
	mu         sync.Mutex
	maxBytes   int64
	maxEntries int
	bytes      int64
	ll         *list.List
	items      map[metaCacheKey]*list.Element
}

func newObjectMetaCache(maxBytes int64, maxEntries int) *objectMetaCache {
	if maxBytes <= 0 {
		maxBytes = defaultMetaCacheBytes
	}
	if maxEntries <= 0 {
		maxEntries = defaultMetaCacheEntries
	}
	return &objectMetaCache{
		maxBytes:   maxBytes,
		maxEntries: maxEntries,
		ll:         list.New(),
		items:      make(map[metaCacheKey]*list.Element),
	}
}

// get 返回缓存命中；命中时把条目移到队首。
func (c *objectMetaCache) get(k metaCacheKey) (*ObjectMeta, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[k]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(el)
	return el.Value.(*metaCacheEntry).meta, true
}

// put 写入或替换一条元数据，并按需淘汰。
func (c *objectMetaCache) put(k metaCacheKey, m *ObjectMeta) {
	if c == nil || m == nil {
		return
	}
	cost := objectMetaCost(m)
	if cost > c.maxBytes || cost > maxCachedMetaBytes {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[k]; ok {
		old := el.Value.(*metaCacheEntry)
		c.bytes -= old.cost
		old.meta = m
		old.cost = cost
		c.bytes += cost
		c.ll.MoveToFront(el)
	} else {
		el := c.ll.PushFront(&metaCacheEntry{key: k, meta: m, cost: cost})
		c.items[k] = el
		c.bytes += cost
	}

	for c.bytes > c.maxBytes || c.ll.Len() > c.maxEntries {
		back := c.ll.Back()
		if back == nil {
			break
		}
		entry := back.Value.(*metaCacheEntry)
		c.ll.Remove(back)
		delete(c.items, entry.key)
		c.bytes -= entry.cost
	}
}

// delete 让某个 key 的缓存立即失效。
func (c *objectMetaCache) delete(k metaCacheKey) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[k]
	if !ok {
		return
	}
	entry := el.Value.(*metaCacheEntry)
	c.ll.Remove(el)
	delete(c.items, k)
	c.bytes -= entry.cost
}

// objectMetaCost 估算一条元数据在堆上的开销。
//
// 不需要精确：它只用于限界。字符串长度、块表与用户元数据是大头；
// 固定开销把 slice/map/结构体头也算进去。
func objectMetaCost(m *ObjectMeta) int64 {
	cost := int64(len(m.Key) + len(m.ETag) + len(m.ContentType) +
		len(m.Checksum) + len(m.ChecksumAlgorithm) + len(m.ChecksumType) +
		len(m.NodeID) + len(m.Layout) + 256)
	for k, v := range m.UserMeta {
		cost += int64(len(k) + len(v) + 32)
	}
	cost += int64(len(m.Blocks)) * 96
	// 内联内容驻留在 meta 里：不计入成本会让缓存远超 maxBytes
	// （4 KiB 上限下每条多出 4 KiB 数据，默认 4 MiB 缓存会被迅速挤空）。
	cost += int64(len(m.InlineData))
	if m.EC != nil {
		cost += int64(64 + len(m.EC.Group)*24)
	}
	return cost
}
