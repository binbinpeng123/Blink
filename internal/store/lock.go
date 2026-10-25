package store

import (
	"sort"
	"sync"
)

// keyLocker 按 key 提供互斥，避免不同对象互相阻塞。
//
// 为什么不用一把全局互斥锁：大对象写入可能持续数秒（GB 级模型权重），
// 全局锁会让所有其他对象的读写排队 —— 边端并发上传几个模型时直接退化成串行。
//
// 为什么不用固定的锁数组（striped locks）：锁数量固定时，
// 两个不同 key 仍会因哈希冲突而互相等待，且冲突概率随对象数上升。
// 这里改为**按需创建、引用计数归零即回收**，既无冲突也无泄漏。
type keyLocker struct {
	mu    sync.Mutex
	locks map[string]*lockEntry
}

type lockEntry struct {
	mu   sync.Mutex
	refs int
}

func newKeyLocker() *keyLocker {
	return &keyLocker{locks: make(map[string]*lockEntry)}
}

// Lock 获取 key 的锁并返回解锁函数。
func (l *keyLocker) Lock(key string) func() {
	l.mu.Lock()
	e, ok := l.locks[key]
	if !ok {
		e = &lockEntry{}
		l.locks[key] = e
	}
	e.refs++
	l.mu.Unlock()

	e.mu.Lock()

	return func() {
		e.mu.Unlock()

		l.mu.Lock()
		e.refs--
		if e.refs == 0 {
			// 引用计数归零：没人持有或等待，可以安全删除条目。
			// 必须在持有 l.mu 时判断并删除，否则可能与新来的 Lock 竞争。
			delete(l.locks, key)
		}
		l.mu.Unlock()
	}
}

// sortStrings 按字节序排序，供 LIST 使用。
//
// 用字节序而非字典序：S3 规定 LIST 结果按 key 的 UTF-8 字节序排列，
// 两者对 ASCII 一致，对非 ASCII 则可能不同（例如中文）。
// sort.Strings 恰好就是字节序比较，这里包一层以便集中说明该约定。
func sortStrings(s []string) { sort.Strings(s) }
