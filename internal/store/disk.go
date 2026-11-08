package store

import (
	"errors"
	"sync"
	"time"
)

// errZeroTotal 表示文件系统对总容量报 0，无法计算水位。
var errZeroTotal = errors.New("store: 文件系统报告总容量为 0")

// DiskSpace 测量 path 所在文件系统的总容量与可用容量（字节）。
//
// 抽成函数类型是为了测试：真实实现走 syscall.Statfs（disk_unix.go），
// 测试注入固定数值，不必真的把盘写满。
type DiskSpace func(path string) (total, free uint64, err error)

// DiskUsage 是一次容量快照。
//
// Available 为 false 表示 statfs 失败；此时其余字段为 0。调用方
// （/readyz、/metrics）必须据此区分"盘没满"与"量不出来"。
type DiskUsage struct {
	Total     uint64
	Free      uint64
	UsedRatio float64
	Available bool
	// Err 是上次 statfs 的错误，仅在 Available=false 时非 nil。
	Err error
}

// diskCheckInterval 是容量检查的缓存时长。
//
// 每次 PUT 都 statfs 在边端是多余的系统调用；而磁盘从"未满"到"满"
// 至少需要写完剩余空间，1 秒的滞后不会让写请求撞上 ENOSPC 之外的新情况
// —— 真撞上了 ENOSPC 仍会映射为 507。
const diskCheckInterval = time.Second

// diskState 把水位配置与最近一次测量结果放在一起。
type diskState struct {
	mu      sync.Mutex
	high    float64
	low     float64
	space   DiskSpace
	path    string
	usage   DiskUsage
	checked time.Time
	blocked bool
}

// initDisk 在 Open 中建立水位状态。high<=0 表示不启用容量保护。
func (s *Store) initDisk(high, low float64, space DiskSpace) {
	if space == nil {
		space = defaultDiskSpace
	}
	s.disk = &diskState{
		high:  high,
		low:   low,
		space: space,
		path:  s.root,
	}
}

// refreshLocked 重新测量容量。调用方必须持有 d.mu。
//
// statfs 失败时保留 Available=false 并**清空阻断标志**：fail-open 的
// 语义是"量不出来就放行"，若继续按旧状态拒写，监控故障会变成写入故障。
func (d *diskState) refreshLocked() {
	d.checked = time.Now()
	total, free, err := d.space(d.path)
	if err != nil {
		d.usage = DiskUsage{Available: false, Err: err}
		d.blocked = false
		return
	}
	if total == 0 {
		// 部分文件系统（如某些网络盘）对总容量报 0，无法计算比例。
		d.usage = DiskUsage{Available: false, Err: errZeroTotal}
		d.blocked = false
		return
	}
	used := total - free
	if free > total {
		// 理论上前者不应发生；真出现时按"已满"处理，宁可拦住写入。
		used = total
		free = 0
	}
	d.usage = DiskUsage{
		Total:     total,
		Free:      free,
		UsedRatio: float64(used) / float64(total),
		Available: true,
	}
}

// updateBlockedLocked 应用滞回：达到高水位进入阻断，回落到低水位才解除。
func (d *diskState) updateBlockedLocked() {
	if !d.usage.Available {
		d.blocked = false
		return
	}
	if d.blocked {
		if d.usage.UsedRatio <= d.low {
			d.blocked = false
		}
		return
	}
	if d.usage.UsedRatio >= d.high {
		d.blocked = true
	}
}

// checkWriteAllowed 在写入口调用；命中水位时返回 ErrStorageFull。
func (s *Store) checkWriteAllowed() error {
	d := s.disk
	if d == nil || d.high <= 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if time.Since(d.checked) >= diskCheckInterval {
		d.refreshLocked()
		d.updateBlockedLocked()
	}
	if d.blocked {
		return ErrStorageFull
	}
	return nil
}

// DiskUsage 返回容量快照，最多 diskCheckInterval 旧。
//
// 过期时刷新一次，使 /readyz 在"没有写入"的时段也能反映删除释放的空间，
// 同时避免高频抓取把监控流量变成 statfs 风暴。
func (s *Store) DiskUsage() DiskUsage {
	d := s.disk
	if d == nil {
		return DiskUsage{}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if time.Since(d.checked) >= diskCheckInterval {
		d.refreshLocked()
		d.updateBlockedLocked()
	}
	return d.usage
}

// WriteBlocked 报告当前是否因水位阻断写入。
func (s *Store) WriteBlocked() bool {
	d := s.disk
	if d == nil || d.high <= 0 {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.blocked
}
