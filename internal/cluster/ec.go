package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/binbinpeng123/Blink/internal/checksum"
	"github.com/binbinpeng123/Blink/internal/ec"
	"github.com/binbinpeng123/Blink/internal/store"
)

// ecEnabled 报告本节点是否按 EC 布局写新对象。
func (c *Cluster) ecEnabled() bool { return c.cfg.ECEnabled }

// 本文件把 internal/ec 的编解码能力接到集群放置上。
//
// 与多副本路径的分工：
//   - meta 仍然提交给环上选出的 N 个副本（W 个 ack），读路径的版本仲裁
//     与墓碑语义完全不变；
//   - 数据改为 k+m 个分片，落到按同一个 ReplicaKey 选出的 k+m 个节点。
//     每个分片单独作为 CAS 块存放，因此去重、存在性探测、修复搬运都能
//     直接复用块层能力，不需要第二套分片存储格式。
//
// EC 只覆盖单次 PUT。分片上传（MPU）仍走副本路径：Complete 只串接块表，
// 若中途改成 EC 就要在 Complete 时重新读一遍全部数据，与"不搬数据"的
// 设计目标冲突。这条限制写在 PR 的已知限制里。

// ecGroupTargets 返回某个 key 的分片组（k+m 个**不同**节点，下标即分片号）。
//
// 组由对象 key 的环位置一次决定，所有块共享：逐块重新放置会让同一对象
// 的分片散落在不同节点集合上，修复与反熵都要按块记录节点，成本远大于
// 收益。环上不同节点不足 k+m 时返回 ErrUnavailable —— 组大小是布局的
// 一部分，凑不够就不能降级成"少几个分片"。
func (c *Cluster) ecGroupTargets(bucket, key string) ([]target, error) {
	k, m := c.cfg.ECK, c.cfg.ECM
	n := k + m
	ids := c.ring.Replicas(ReplicaKey(bucket, key), n)
	if len(ids) < n {
		return nil, fmt.Errorf("%w: EC 需要 %d 个不同节点，环上只有 %d 个",
			store.ErrUnavailable, n, len(ids))
	}
	ts := make([]target, n)
	for i, id := range ids {
		ts[i] = c.targetFor(id)
	}
	return ts, nil
}

// ecTargetsFor 把 meta 中记录的分片组还原成目标列表。
//
// 与 ecGroupTargets 的区别：后者按**当前**环重新计算，前者只信 meta。
// 读与修复必须用前者 —— 对象写在成员表变更之前时，重新计算可能得到
// 另一组节点，而分片就在旧组里。
func (c *Cluster) ecTargetsFor(meta *store.ObjectMeta) ([]target, error) {
	if meta == nil || meta.EC == nil {
		return nil, fmt.Errorf("%w: EC 对象缺少分片组", store.ErrCorrupt)
	}
	if len(meta.EC.Group) != meta.EC.K+meta.EC.M {
		return nil, fmt.Errorf("%w: EC 分片组有 %d 个节点，期望 %d",
			store.ErrCorrupt, len(meta.EC.Group), meta.EC.K+meta.EC.M)
	}
	ts := make([]target, len(meta.EC.Group))
	for i, id := range meta.EC.Group {
		ts[i] = c.targetFor(id)
	}
	return ts, nil
}

// ecWriteState 是一次 EC 写入的组内状态。
//
// failed 跨块累积：某个节点只要有一块分片写失败，就在对象提交后入队
// 修复。入队按 (target, bucket, key) 去重，重复登记不会放大任务数。
type ecWriteState struct {
	codec  *ec.Codec
	group  []target
	names  []string
	failed map[string]struct{}
}

// newECWrite 为一个对象构造编码器并固定分片组。
func (c *Cluster) newECWrite(bucket, key string) (*ecWriteState, error) {
	codec, err := ec.NewCodec(c.cfg.ECK, c.cfg.ECM)
	if err != nil {
		return nil, fmt.Errorf("cluster: EC 参数 k=%d m=%d 非法: %w", c.cfg.ECK, c.cfg.ECM, err)
	}
	group, err := c.ecGroupTargets(bucket, key)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(group))
	for i, t := range group {
		names[i] = t.id
	}
	return &ecWriteState{
		codec:  codec,
		group:  group,
		names:  names,
		failed: make(map[string]struct{}),
	}, nil
}

// distributeECBlock 把一个块编码成 k+m 个分片并并行写入组内节点。
//
// 返回的分片描述**无论部分写入是否失败都要完整返回**：meta 描述的是
// 对象的目标布局，缺的分片由修复队列补齐。只有成功分片少于 k 时才
// 让整次写入失败 —— 少于 k 片的对象根本不可读，提交 meta 只会制造
// 一个"存在但读不出来"的对象。
//
// 必须等全部 goroutine 结束才能返回：shards 是 Encode 分配的连续内存，
// 回调返回后 b.Data 也不再属于本次写入。
func (c *Cluster) distributeECBlock(w *ecWriteState, b store.Block) ([]store.ShardDesc, bool, error) {
	shards, err := w.codec.Encode(b.Data)
	if err != nil {
		return nil, false, fmt.Errorf("%w: 块 %s 编码失败: %v", store.ErrCorrupt, b.Desc.Hash, err)
	}

	descs := make([]store.ShardDesc, len(shards))
	for i, s := range shards {
		descs[i] = store.ShardDesc{
			Hash:   store.BlockHash(s),
			Length: int64(len(s)),
			CRC32C: checksum.BlockCRC32C(s),
		}
	}

	type outcome struct {
		wrote bool
		err   error
	}
	out := make([]outcome, len(w.group))
	var wg sync.WaitGroup
	for i, t := range w.group {
		wg.Add(1)
		go func(i int, t target) {
			defer wg.Done()
			wrote, err := t.putBlock(context.Background(), descs[i].Hash, shards[i])
			out[i] = outcome{wrote: wrote, err: err}
		}(i, t)
	}
	wg.Wait()

	ok := 0
	deduped := true
	for i, o := range out {
		if o.err != nil {
			w.failed[w.group[i].id] = struct{}{}
			c.logger.Warn("replica_shard_failed",
				"peer", w.group[i].id, "shard", i,
				"hash", descs[i].Hash, "err", o.err.Error())
			continue
		}
		ok++
		if o.wrote {
			deduped = false
		}
	}
	if ok < w.codec.K() {
		return nil, false, fmt.Errorf("%w: 块 %s 仅 %d/%d 个分片写入成功，需要 %d",
			store.ErrUnavailable, b.Desc.Hash, ok, len(w.group), w.codec.K())
	}
	return descs, deduped, nil
}

// fetchECShard 把一片分片完整读入内存并校验哈希。
//
// 分片在 CAS 里就是普通块，所以复用 getBlock 的本地零拷贝/远端流式路径。
// 只读整片而不读片段：分片的完整性凭据是整片哈希，读一半无法验证。
func fetchECShard(ctx context.Context, t target, sd store.ShardDesc) ([]byte, error) {
	if sd.Length <= 0 {
		return nil, fmt.Errorf("%w: 分片 %s 长度 %d 非正", store.ErrCorrupt, sd.Hash, sd.Length)
	}
	var buf bytes.Buffer
	buf.Grow(int(sd.Length))
	desc := store.BlockDesc{Hash: sd.Hash, Length: sd.Length, CRC32C: sd.CRC32C}
	if err := t.getBlock(ctx, desc, 0, sd.Length, &buf); err != nil {
		return nil, err
	}
	data := buf.Bytes()
	if int64(len(data)) != sd.Length {
		return nil, fmt.Errorf("%w: 分片 %s 读到 %d 字节，期望 %d",
			store.ErrCorrupt, sd.Hash, len(data), sd.Length)
	}
	if store.BlockHash(data) != sd.Hash {
		return nil, fmt.Errorf("%w: 分片 %s 的内容与哈希不符", store.ErrCorrupt, sd.Hash)
	}
	return data, nil
}

// fetchSomeShards 并行读取 idxs 指定的分片，错误按位置返回。
func fetchSomeShards(ctx context.Context, group []target, b store.BlockDesc, idxs []int) ([][]byte, []error) {
	data := make([][]byte, len(idxs))
	errs := make([]error, len(idxs))
	var wg sync.WaitGroup
	for j, i := range idxs {
		wg.Add(1)
		go func(j, i int) {
			defer wg.Done()
			data[j], errs[j] = fetchECShard(ctx, group[i], b.Shards[i])
		}(j, i)
	}
	wg.Wait()
	return data, errs
}

// gatherECShards 从组内取回足够重建一个块的分片。
//
// 顺序刻意让健康路径只读 k 片：先并行读全部数据片，全部成功时直接返回，
// 不看校验片；有数据片失败才继续读校验片。这样正常读的带宽与副本路径
// 相同，而不是固定 (k+m)/k 倍。
//
// exclude 用于修复：调用方正在补第 exclude 片，它本来就不该出现在来源里。
// 返回的 missing 是**尝试过且失败**的下标，读路径据此登记修复；没尝试
// 过的分片不算缺失（健康路径上就是校验片），交给反熵兜底。
func gatherECShards(ctx context.Context, codec *ec.Codec, group []target, b store.BlockDesc, exclude int) ([][]byte, []bool, []int, error) {
	n := codec.Shards()
	shards := make([][]byte, n)
	present := make([]bool, n)
	var missing []int

	avail := func() int {
		c := 0
		for _, ok := range present {
			if ok {
				c++
			}
		}
		return c
	}

	dataIdx := make([]int, 0, codec.K())
	for i := 0; i < codec.K(); i++ {
		if i != exclude {
			dataIdx = append(dataIdx, i)
		}
	}
	data, errs := fetchSomeShards(ctx, group, b, dataIdx)
	for j, i := range dataIdx {
		if errs[j] != nil {
			missing = append(missing, i)
			continue
		}
		shards[i] = data[j]
		present[i] = true
	}
	if exclude >= 0 {
		missing = append(missing, exclude)
	}

	if avail() < codec.K() {
		parityIdx := make([]int, 0, codec.M())
		for i := codec.K(); i < n; i++ {
			if i != exclude {
				parityIdx = append(parityIdx, i)
			}
		}
		pdata, perrs := fetchSomeShards(ctx, group, b, parityIdx)
		for j, i := range parityIdx {
			if perrs[j] != nil {
				missing = append(missing, i)
				continue
			}
			shards[i] = pdata[j]
			present[i] = true
		}
	}

	if avail() < codec.K() {
		return nil, nil, missing, fmt.Errorf("%w: 块 %s 仅取得 %d/%d 个分片，需要 %d",
			store.ErrUnavailable, b.Hash, avail(), n, codec.K())
	}
	return shards, present, missing, nil
}

// reconstructECBlock 还原整块内容并校验整块哈希。
//
// 分片本身已经逐片校验过，整块哈希是第二道防线：它同时覆盖"编码规则
// 与 meta 一致"与"分片拼回的顺序正确"，正是分片级校验看不到的部分。
func reconstructECBlock(codec *ec.Codec, b store.BlockDesc, shards [][]byte, present []bool) ([]byte, error) {
	// 健康路径（k 个数据片全在）不需要解码，也不该重算校验片：
	// Reconstruct 会为 m 个缺失校验片各做 k 次 mulAdd，即 m/k 倍的额外
	// CPU。数据片齐全时它们的字节就是原文的连续切分，直接拼接即可。
	allData := true
	for i := 0; i < codec.K(); i++ {
		if !present[i] {
			allData = false
			break
		}
	}
	if !allData {
		if err := codec.Reconstruct(shards, present); err != nil {
			return nil, fmt.Errorf("%w: 块 %s 重建失败: %v", store.ErrCorrupt, b.Hash, err)
		}
	}
	out := make([]byte, 0, int(b.Length))
	for i := 0; i < codec.K(); i++ {
		out = append(out, shards[i]...)
	}
	if int64(len(out)) < b.Length {
		return nil, fmt.Errorf("%w: 块 %s 重建得到 %d 字节，期望 %d",
			store.ErrCorrupt, b.Hash, len(out), b.Length)
	}
	out = out[:b.Length]
	if store.BlockHash(out) != b.Hash {
		return nil, fmt.Errorf("%w: 块 %s 重建后的内容与哈希不符", store.ErrCorrupt, b.Hash)
	}
	return out, nil
}

// ecReader 是一个 EC 对象的分片视图。
type ecReader struct {
	codec *ec.Codec
	group []target
}

// readECSegment 读取 EC 块内的一段。
//
// 无法只读分片的一段：分片没有独立的块边界，任取一片都无法验证完整性。
// 因此总是取回整块、校验，再切片写出 —— Range 请求有超额读取，这是
// 用带宽换"绝不返回未校验内容"的取舍。
func (r *clusterReader) readECSegment(w io.Writer, b store.BlockDesc, offset, length int64) error {
	if r.ecr == nil {
		return fmt.Errorf("%w: 块 %s 缺少 EC 解码器", store.ErrCorrupt, b.Hash)
	}
	shards, present, missing, err := gatherECShards(context.Background(), r.ecr.codec, r.ecr.group, b, -1)
	if r.repair != nil {
		for _, i := range missing {
			r.repair(r.ecr.group[i], b)
		}
	}
	if err != nil {
		return err
	}
	data, err := reconstructECBlock(r.ecr.codec, b, shards, present)
	if err != nil {
		return err
	}
	_, err = w.Write(data[offset : offset+length])
	return err
}

// ensureECShards 让目标节点持有第 idx 片分片的全部块。
//
// 先查存在性再重建：分片按内容寻址，绝大多数修复只需要一次 stat/HEAD，
// 真正的重建只在确实缺片时发生。
func (r *repairer) ensureECShards(ctx context.Context, meta *store.ObjectMeta, idx int, group []target, tgt target) error {
	codec, err := ec.NewCodec(meta.EC.K, meta.EC.M)
	if err != nil {
		return fmt.Errorf("%w: EC 参数 k=%d m=%d 非法: %v",
			store.ErrCorrupt, meta.EC.K, meta.EC.M, err)
	}
	for _, b := range meta.Blocks {
		if idx < 0 || idx >= len(b.Shards) {
			return fmt.Errorf("%w: 块 %s 缺少分片 %d", store.ErrCorrupt, b.Hash, idx)
		}
		sd := b.Shards[idx]
		ok, err := tgt.blockExists(ctx, sd.Hash)
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		if err := r.rebuildShard(ctx, codec, group, b, idx, tgt); err != nil {
			return err
		}
	}
	return nil
}

// rebuildShard 从其余 k 片重建第 idx 片并写入 target。
func (r *repairer) rebuildShard(ctx context.Context, codec *ec.Codec, group []target, b store.BlockDesc, idx int, tgt target) error {
	shards, present, _, err := gatherECShards(ctx, codec, group, b, idx)
	if err != nil {
		return err
	}
	if err := codec.Reconstruct(shards, present); err != nil {
		return fmt.Errorf("%w: 块 %s 分片 %d 重建失败: %v", store.ErrCorrupt, b.Hash, idx, err)
	}
	got := shards[idx]
	sd := b.Shards[idx]
	// 校验重建出的分片本身，而不是整块：分片哈希相等即内容相等，
	// 省掉一次 4 MiB 拼接；整块一致性由每次读路径的 reconstructECBlock 兜底。
	if int64(len(got)) != sd.Length || store.BlockHash(got) != sd.Hash {
		return fmt.Errorf("%w: 块 %s 分片 %d 的重建结果与元数据不符", store.ErrCorrupt, b.Hash, idx)
	}
	if _, err := tgt.putBlock(ctx, sd.Hash, got); err != nil {
		return err
	}
	return nil
}

// isDataTarget 判断 id 是否可能持有该 key 的数据。
//
// 副本目标持有 meta 与整块，EC 分片组成员只持有分片，两者都需要接受
// 修复任务；只按副本集过滤会把 EC 组里独有的节点排除掉，导致缺片
// 永远不修。
func (c *Cluster) isDataTarget(id, bucket, key string) bool {
	if c.isReplicaTarget(id, bucket, key) {
		return true
	}
	if !c.ecEnabled() {
		return false
	}
	for _, gid := range c.ring.Replicas(ReplicaKey(bucket, key), c.cfg.ECK+c.cfg.ECM) {
		if gid == id {
			return true
		}
	}
	return false
}
