package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/binbinpeng123/Blink/internal/store"
)

// 本文件实现故障修复（hinted handoff）：写入或读取发现某个副本落后/
// 缺块时，把"该给谁补什么"记成任务，后台按指数退避重试。
//
// 为什么需要它：W 个 ack 就向客户端确认成功，意味着总有副本没跟上。
// 如果只在读路径上"顺手修"，从未被读到的冷数据会永远缺副本；
// 如果只在删除时修复，一次长故障会让墓碑永远缺失。修复队列是
// 三道防线中的第二道（读时修复 → 修复队列 → 周期反熵）。
//
// 任务持久化在 <data-dir>/cluster/hints/<target>/<hash>.json：
// 进程重启后修复不会丢失，而节点恢复通常正是重启的结果。
// 文件名由 (target, bucket, key) 确定性生成，重复入队天然幂等。

// repairKind 标记任务来源，只用于日志与排障。
//
// 执行语义是统一的：确保目标拥有最新版本的**全部块**，再提交 meta。
// 不按 kind 走两套逻辑，是因为"有 meta 缺块"是修复最危险的中间态，
// 统一走"先块后 meta"从结构上排除了它。
type repairKind string

const (
	// repairMeta 表示调用方确认块已齐全，修复只需补 meta
	// （典型场景：删除墓碑、读路径发现的落后副本）。
	repairMeta repairKind = "meta"
	// repairBlocks 表示目标可能缺块，修复需先补齐块再提交 meta
	// （典型场景：写路径有副本没拿到全部块）。
	repairBlocks repairKind = "blocks"
)

// repairTask 是一份待补齐的副本缺陷。
type repairTask struct {
	Bucket      string     `json:"bucket"`
	Key         string     `json:"key"`
	Version     uint64     `json:"version"`
	TargetNode  string     `json:"target_node"`
	Kind        repairKind `json:"kind"`
	CreatedAt   time.Time  `json:"created_at"`
	Attempts    int        `json:"attempts"`
	NextAttempt time.Time  `json:"next_attempt"`

	// queued 表示任务已被投递给 worker。只在内存中，不持久化：
	// 重启后所有任务都应当被重新投递。
	queued bool
}

// repairBackoff 是重试间隔序列；超过长度后停在最后一档。
//
// 从 1s 开始而不是 0：节点刚重启时第一个修复请求很可能赶上它还在
// 建立监听，立即重试只会把失败率刷进日志。
var repairBackoff = []time.Duration{
	time.Second,
	5 * time.Second,
	30 * time.Second,
	5 * time.Minute,
	time.Hour,
}

// backoffFor 返回第 attempts 次失败后的等待时间，带 ±30% 抖动。
//
// 抖动是必需的：一次节点重启会让所有它负责的 key 同时入队，
// 固定间隔会在每个退避档位上制造同步的重试脉冲。
func backoffFor(attempts int) time.Duration {
	i := attempts - 1
	if i < 0 {
		i = 0
	}
	if i >= len(repairBackoff) {
		i = len(repairBackoff) - 1
	}
	d := repairBackoff[i]
	return time.Duration(float64(d) * (0.7 + 0.6*rand.Float64()))
}

// repairer 是每个节点一个的修复执行器。
type repairer struct {
	c      *Cluster
	dir    string
	ttl    time.Duration
	conc   int
	period time.Duration
	scrub  time.Duration

	mu     sync.Mutex
	tasks  map[string]*repairTask
	closed bool

	jobs chan *repairTask
	wake chan struct{}
	stop chan struct{}
	wg   sync.WaitGroup

	closeOnce sync.Once
}

// newRepairer 构造修复器；start 之前不产生任何 goroutine。
func newRepairer(c *Cluster) *repairer {
	return &repairer{
		c:      c,
		dir:    filepath.Join(c.Store.Root(), "cluster", "hints"),
		ttl:    c.cfg.HintTTL,
		conc:   c.cfg.RepairMaxConcurrency,
		period: c.cfg.RepairInterval,
		scrub:  c.cfg.ScrubInterval,
		tasks:  make(map[string]*repairTask),
		jobs:   make(chan *repairTask, c.cfg.RepairMaxConcurrency*4),
		wake:   make(chan struct{}, 1),
		stop:   make(chan struct{}),
	}
}

// start 加载持久化任务并启动调度器、worker 与反熵扫描。
func (r *repairer) start() {
	r.loadHints()

	r.wg.Add(1)
	go r.schedule()

	for i := 0; i < r.conc; i++ {
		r.wg.Add(1)
		go r.worker()
	}

	// ScrubInterval < 0 表示禁用后台扫描（例如磁盘紧张或压测时）。
	if r.scrub >= 0 {
		r.wg.Add(1)
		go r.scrubLoop()
	}
}

// close 停止全部 goroutine，并等待正在执行的修复结束。
//
// 必须先等 worker 退出再关闭 PeerClient 的空闲连接：反过来的话，
// 正在传输的修复会以"连接被关闭"失败，在日志里留下一批假告警。
func (r *repairer) close() {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()

		close(r.stop)
		r.wg.Wait()
	})
}

// enqueue 记录一个需要修复的副本缺陷。
//
// 幂等：同一 (target, bucket, key) 已有任务时，只有观察到**更高版本**
// 才重置退避 —— 否则每次 GET 都会把退避打回 1s，一个持续缺块的
// 副本会以读请求的频率被反复重试，变成自伤的修复风暴。
//
// 调用方在请求路径上，本方法只做内存操作与小文件写，不等待任何网络。
func (r *repairer) enqueue(targetID string, kind repairKind, bucket, key string, version uint64) {
	if targetID == "" {
		return
	}
	if err := store.ValidateObjectPath(bucket, key); err != nil {
		return
	}
	if !r.c.isDataTarget(targetID, bucket, key) {
		// 目标已不在这个 key 的副本集或分片组里（成员表变更后），
		// 补它没有意义。
		return
	}

	name := hintName(targetID, bucket, key)
	now := r.c.now()

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	existing := r.tasks[name]
	if existing != nil && version <= existing.Version {
		r.mu.Unlock()
		return
	}
	if existing != nil {
		existing.Version = version
		existing.Kind = kind
		existing.Attempts = 0
		existing.NextAttempt = now
		existing.CreatedAt = now
		// 落盘用锁内快照：任务可能正在被 worker 执行，finish 也会
		// 在锁内改写 Attempts/NextAttempt，直接传指针出去就是数据竞争。
		snapshot := *existing
		r.mu.Unlock()
		r.persist(&snapshot)
		r.signal()
		return
	}
	t := &repairTask{
		Bucket:      bucket,
		Key:         key,
		Version:     version,
		TargetNode:  targetID,
		Kind:        kind,
		CreatedAt:   now,
		NextAttempt: now,
	}
	r.tasks[name] = t
	r.mu.Unlock()

	r.persist(t)
	r.signal()
}

// signal 唤醒调度器重新计算到期任务；channel 已满时无需重复唤醒。
func (r *repairer) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// schedule 是唯一挑选到期任务的 goroutine。
//
// 到期判定放在这里而不是每个 worker 里，是为了让"哪些任务正在执行"
// 只在一个 goroutine 的视角下变化，不需要给每个任务加锁。
func (r *repairer) schedule() {
	defer r.wg.Done()

	timer := time.NewTimer(r.period)
	defer timer.Stop()

	for {
		next := r.dispatchDue()

		// 唤醒时间取"最近一个到期任务"与兜底间隔中较早者：
		// 只依赖 period 会让退避 1s 的任务实际等 30s 才重试，
		// 而只依赖下一个到期时间会让空队列时的兜底心跳失效。
		wait := r.period
		if !next.IsZero() {
			if until := next.Sub(r.c.now()); until < wait {
				wait = until
			}
		}
		if wait < minRepairTick {
			// 已到期（或时钟回拨）时不要 0 超时自旋。
			wait = minRepairTick
		}

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(wait)

		select {
		case <-r.stop:
			return
		case <-r.wake:
		case <-timer.C:
		}
	}
}

// minRepairTick 是调度循环的最小间隔，避免到期任务造成忙等。
const minRepairTick = 10 * time.Millisecond

// dispatchDue 把已到期且未在执行的任务投递给 worker，并返回下一个
// 尚未到期任务的到期时间（没有则返回零值）。
func (r *repairer) dispatchDue() (next time.Time) {
	now := r.c.now()

	type expired struct{ target, name string }
	var dropped []expired

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return time.Time{}
	}
	for name, t := range r.tasks {
		// 超过保留期的任务不再重试：长期不可达的节点交给反熵重新发现，
		// 让 hint 目录不会随故障时长无限增长。
		if now.Sub(t.CreatedAt) > r.ttl {
			delete(r.tasks, name)
			dropped = append(dropped, expired{target: t.TargetNode, name: name})
			continue
		}
		if t.queued {
			// 正在执行：完成时 finish 会重新 signal，不参与本轮计时。
			continue
		}
		if t.NextAttempt.After(now) {
			if next.IsZero() || t.NextAttempt.Before(next) {
				next = t.NextAttempt
			}
			continue
		}
		t.queued = true
		select {
		case r.jobs <- t:
		default:
			// worker 全忙：保持未排队状态，下一轮再试。
			t.queued = false
		}
	}
	r.mu.Unlock()

	// 文件操作放在锁外：删 hint 可能遇到慢盘，不能阻塞 enqueue。
	for _, e := range dropped {
		r.removeHint(e.target, e.name)
	}
	return next
}

// worker 执行修复任务并回报结果。
func (r *repairer) worker() {
	defer r.wg.Done()
	for {
		select {
		case <-r.stop:
			return
		case t := <-r.jobs:
			err := r.execute(t)
			r.finish(t, err)
		}
	}
}

// finish 处理一次修复的结果：成功删除任务与 hint，失败按退避重排。
func (r *repairer) finish(t *repairTask, err error) {
	name := hintName(t.TargetNode, t.Bucket, t.Key)
	now := r.c.now()

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	t.queued = false

	if err == nil {
		delete(r.tasks, name)
		r.mu.Unlock()
		r.removeHint(t.TargetNode, name)
		return
	}

	t.Attempts++
	if now.Sub(t.CreatedAt) > r.ttl {
		delete(r.tasks, name)
		r.mu.Unlock()
		r.removeHint(t.TargetNode, name)
		r.c.logger.Warn("repair_abandoned",
			"peer", t.TargetNode, "bucket", t.Bucket, "key", t.Key,
			"attempts", t.Attempts, "err", err.Error())
		return
	}
	t.NextAttempt = now.Add(backoffFor(t.Attempts))
	snapshot := *t
	r.mu.Unlock()

	r.persist(&snapshot)
	r.c.logger.Warn("repair_failed",
		"peer", t.TargetNode, "bucket", t.Bucket, "key", t.Key,
		"attempt", t.Attempts, "next", t.NextAttempt.Format(time.RFC3339), "err", err.Error())
	r.signal()
}

// execute 让目标节点拥有该 key 最新版本的**全部块**与 meta。
//
// 每次执行都重新探测：任务入队时看到的版本可能已经过时，
// 而"修复"的正确含义永远是"补齐当前最新版本"，不是"补上当初那个版本"。
func (r *repairer) execute(t *repairTask) error {
	// worker 执行期间 enqueue 可能原地重置同一个任务的 Version/Attempts，
	// 因此只在锁内取一次快照，后续全部读快照。
	r.mu.Lock()
	task := *t
	r.mu.Unlock()

	ctx := context.Background()

	// 先探测、再定位目标。不能先按副本集定位：EC 分片组里可能有节点
	// 不在副本集上，那样一进来就会被判成 repair_target_moved 而永久漏修。
	ts := r.c.targetsFor(task.Bucket, task.Key)
	probe := r.c.probeMetas(task.Bucket, task.Key, ts)
	responded, _ := countResponses(probe)
	if responded < r.c.cfg.ReadQuorum {
		return fmt.Errorf("%w: 修复前探测仅 %d/%d 个副本响应，需要 %d",
			store.ErrUnavailable, responded, len(ts), r.c.cfg.ReadQuorum)
	}
	latest := latestMeta(probe)
	if latest == nil {
		// 所有响应都说没有这份副本：要么对象从未提交，要么任务基于
		// 一次失败的写。没有可修的东西。
		return nil
	}
	if latest.ObjVersion < task.Version {
		// 任务基于更旧的版本，且当前没有任何副本持有它 —— 已无关紧要。
		return nil
	}

	// 目标可能同时是副本成员与分片组成员，两个身份分别处理：
	//   - 副本成员负责 meta（以及副本布局下的整块）；
	//   - 分片组成员负责自己那一片。
	replicaIdx := -1
	for i := range ts {
		if ts[i].id == task.TargetNode {
			replicaIdx = i
			break
		}
	}
	var group []target
	groupIdx := -1
	if latest.Layout == store.LayoutEC && latest.EC != nil {
		g, err := r.c.ecTargetsFor(latest)
		if err != nil {
			return err
		}
		group = g
		for i := range group {
			if group[i].id == task.TargetNode {
				groupIdx = i
				break
			}
		}
	}
	if replicaIdx < 0 && groupIdx < 0 {
		r.c.logger.Info("repair_target_moved",
			"peer", task.TargetNode, "bucket", task.Bucket, "key", task.Key)
		return nil
	}

	var current *store.ObjectMeta
	if replicaIdx >= 0 {
		for _, res := range probe {
			if res.id == task.TargetNode && res.err == nil && res.found {
				current = res.meta
			}
		}
	}
	if current != nil && newerMeta(current, latest) {
		return nil
	}

	if !latest.Deleted {
		if groupIdx >= 0 {
			if err := r.ensureECShards(ctx, latest, groupIdx, group, group[groupIdx]); err != nil {
				return err
			}
		}
		// EC 对象的整块并不存在于任何节点，块表里也没有可搬运的整块：
		// 副本成员只承担 meta，数据由上面的分片补齐负责。
		if replicaIdx >= 0 && latest.Layout != store.LayoutEC {
			if err := r.ensureBlocks(ctx, ts, ts[replicaIdx], latest.Blocks); err != nil {
				return err
			}
		}
	}

	if replicaIdx < 0 {
		// 只是分片组成员，不持有 meta：分片补齐即完成。
		return nil
	}
	if current != nil && sameVersion(current, latest) {
		return nil
	}
	if err := ts[replicaIdx].putMeta(ctx, task.Bucket, task.Key, latest); err != nil {
		if isStaleError(err) {
			// 对端已有更新版本：本任务的目标已被别处满足。
			return nil
		}
		return err
	}
	return nil
}

// ensureBlocks 把缺失的块从其它副本搬到 target。
//
// 先查存在性再传输：块按内容寻址，绝大多数修复只需要一次 HEAD
// （本地是一条 stat），真正的数据搬运只在确实缺块时发生。
func (r *repairer) ensureBlocks(ctx context.Context, ts []target, target target, blocks []store.BlockDesc) error {
	for _, b := range blocks {
		ok, err := target.blockExists(ctx, b.Hash)
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		if err := r.copyBlock(ctx, ts, target, b); err != nil {
			return err
		}
	}
	return nil
}

// copyBlock 从任一健康副本读取整块并写入 target。
func (r *repairer) copyBlock(ctx context.Context, ts []target, target target, b store.BlockDesc) error {
	var lastErr error
	for _, src := range ts {
		if src.id == target.id {
			continue
		}
		var buf bytes.Buffer
		if b.Length > 0 && b.Length < 64<<20 {
			buf.Grow(int(b.Length))
		}
		if err := src.getBlock(ctx, b, 0, b.Length, &buf); err != nil {
			lastErr = err
			continue
		}
		data := buf.Bytes()
		if int64(len(data)) != b.Length {
			lastErr = fmt.Errorf("%w: 块 %s 从节点 %s 读到 %d 字节，期望 %d",
				store.ErrCorrupt, b.Hash, src.id, len(data), b.Length)
			continue
		}
		// 来源可能已经损坏。PutBlock 不重算哈希，所以这里必须算：
		// 把一个损坏的块以合法哈希写进 CAS，就再没有自愈机会。
		if store.BlockHash(data) != b.Hash {
			lastErr = fmt.Errorf("%w: 块 %s 从节点 %s 读到的内容与哈希不符",
				store.ErrCorrupt, b.Hash, src.id)
			continue
		}
		if _, err := target.putBlock(ctx, b.Hash, data); err != nil {
			return err
		}
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("%w: 块 %s 没有任何可用来源", store.ErrCorrupt, b.Hash)
	}
	return lastErr
}

// isStaleError 判断错误是否表示"对端已有更新版本"。
//
// 这类失败是收敛的正常结果，不是故障：重试永远不会成功。
func isStaleError(err error) bool {
	if errors.Is(err, store.ErrStaleMeta) {
		return true
	}
	var pe *PeerError
	if errors.As(err, &pe) {
		return pe.Status == 409
	}
	return false
}

// hintName 返回 (target, bucket, key) 的确定性文件名。
func hintName(targetID, bucket, key string) string {
	return fmt.Sprintf("%016x", hashString(targetID+"\x00"+bucket+"\x00"+key))
}

// hintPath 返回 hint 文件路径。
func (r *repairer) hintPath(targetID, name string) string {
	return filepath.Join(r.dir, targetID, name+".json")
}

// persist 原子写入 hint 文件。
//
// 失败只记警告：内存中的任务仍然有效，只是这个缺陷活不过进程重启。
// 反熵扫描是它的兜底，因此不值得为此让请求路径失败。
func (r *repairer) persist(t *repairTask) {
	path := r.hintPath(t.TargetNode, hintName(t.TargetNode, t.Bucket, t.Key))
	data, err := json.Marshal(t)
	if err != nil {
		r.c.logger.Warn("repair_hint_encode_failed", "err", err.Error())
		return
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		r.c.logger.Warn("repair_hint_write_failed", "path", dir, "err", err.Error())
		return
	}
	tmp, err := os.CreateTemp(dir, ".hint-*")
	if err != nil {
		r.c.logger.Warn("repair_hint_write_failed", "path", dir, "err", err.Error())
		return
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		r.c.logger.Warn("repair_hint_write_failed", "path", tmpName, "err", err.Error())
		return
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		r.c.logger.Warn("repair_hint_write_failed", "path", tmpName, "err", err.Error())
		return
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		r.c.logger.Warn("repair_hint_write_failed", "path", path, "err", err.Error())
	}
}

// removeHint 删除 hint 文件；不存在不是错误。
func (r *repairer) removeHint(targetID, name string) {
	if err := os.Remove(r.hintPath(targetID, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		r.c.logger.Warn("repair_hint_remove_failed", "peer", targetID, "err", err.Error())
	}
}

// loadHints 在启动时恢复持久化任务。
//
// 损坏或过期的文件直接删除：hint 是尽力而为的加速层，不能因为一个
// 写了一半的文件让整个节点起不来。
func (r *repairer) loadHints() {
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			r.c.logger.Warn("repair_hints_read_failed", "path", r.dir, "err", err.Error())
		}
		return
	}

	now := r.c.now()
	loaded := 0
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		targetID := ent.Name()
		if _, ok := r.c.cfg.PeerByID(targetID); !ok {
			// 节点已不在成员表里，它的 hint 永远无法执行。
			_ = os.RemoveAll(filepath.Join(r.dir, targetID))
			continue
		}
		files, err := os.ReadDir(filepath.Join(r.dir, targetID))
		if err != nil {
			r.c.logger.Warn("repair_hints_read_failed", "peer", targetID, "err", err.Error())
			continue
		}
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			path := filepath.Join(r.dir, targetID, f.Name())
			data, err := os.ReadFile(path)
			if err != nil {
				r.c.logger.Warn("repair_hint_read_failed", "path", path, "err", err.Error())
				continue
			}
			var t repairTask
			if err := json.Unmarshal(data, &t); err != nil {
				r.c.logger.Warn("repair_hint_corrupt", "path", path, "err", err.Error())
				_ = os.Remove(path)
				continue
			}
			if t.TargetNode != targetID || t.Bucket == "" || t.Key == "" {
				_ = os.Remove(path)
				continue
			}
			if err := store.ValidateObjectPath(t.Bucket, t.Key); err != nil {
				_ = os.Remove(path)
				continue
			}
			if now.Sub(t.CreatedAt) > r.ttl {
				_ = os.Remove(path)
				continue
			}
			if !r.c.isDataTarget(t.TargetNode, t.Bucket, t.Key) {
				_ = os.Remove(path)
				continue
			}
			if t.Attempts > 0 {
				t.NextAttempt = now.Add(backoffFor(t.Attempts))
			} else if t.NextAttempt.IsZero() {
				t.NextAttempt = now
			}
			r.tasks[hintName(t.TargetNode, t.Bucket, t.Key)] = &t
			loaded++
		}
	}
	if loaded > 0 {
		r.c.logger.Info("repair_hints_loaded", "count", loaded)
		r.signal()
	}
}

// isReplicaTarget 判断 id 是否是该 key 的副本目标之一。
func (c *Cluster) isReplicaTarget(id, bucket, key string) bool {
	for _, rid := range c.ring.Replicas(ReplicaKey(bucket, key), c.cfg.ReplicaCount) {
		if rid == id {
			return true
		}
	}
	return false
}
