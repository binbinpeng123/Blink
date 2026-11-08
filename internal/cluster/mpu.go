package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/binbinpeng123/Blink/internal/checksum"
	"github.com/binbinpeng123/Blink/internal/store"
)

// 本文件把分片上传（MPU）分布化。核心设计只有一句话：
//
//	会话放在 S，数据块放在 O，Complete 只搬块表不搬数据。
//
//   - 会话副本集 S 由 uploadId 推导：Ring.Replicas("__mpu__/"+uploadId, |S|)；
//   - 分片数据块属于对象，按 bucket/key 放到 O = Ring.Replicas(bucket+"/"+key, N)。
//
// 好处是 UploadPart 与普通 PUT 走同一条块分发路径（去重、纠删的前置接口
// 都在 distributeBlock 上），而 Complete 的成本只与"块表条目数"有关，
// 与对象体积无关：20 GiB 对象的 Complete 和 20 MiB 对象一样便宜。
//
// |S| 取 max(min(N,3), W, R) 而不是计划原文的 min(N,3)。后者在
// N=5/W=2/R=4 这类合法配置下给出 |S|=3 < R，"从 S 读 R 份"直接不成立。
// 取最大值后恒有 |S| ≥ W 且 |S| ≥ R；又因配置保证 W+R > N ≥ |S|，
// 整个式子给出 W+R > |S|。这就是 Complete 不需要任何共识的全部依据：
// 任意 R 份会话副本的响应集合，必然与写过同一状态的 W 个副本相交。

// sessionReplicaCount 返回会话的副本数。
func (c *Cluster) sessionReplicaCount() int {
	n := c.cfg.ReplicaCount
	if n > 3 {
		n = 3
	}
	if n < c.cfg.WriteQuorum {
		n = c.cfg.WriteQuorum
	}
	if n < c.cfg.ReadQuorum {
		n = c.cfg.ReadQuorum
	}
	return n
}

// mpuReplicaKey 是会话在环上的放置键。
//
// 桶名不允许下划线（bucketNameRe），因此这个前缀不可能与任何真实对象的
// ReplicaKey（"bucket/key"）冲突 —— 会话与数据天然隔离在不同的键空间。
func mpuReplicaKey(uploadID string) string { return "__mpu__/" + uploadID }

// sessionTargets 返回会话副本集 S。
func (c *Cluster) sessionTargets(uploadID string) []target {
	ids := c.ring.Replicas(mpuReplicaKey(uploadID), c.sessionReplicaCount())
	ts := make([]target, 0, len(ids))
	for _, id := range ids {
		ts = append(ts, c.targetFor(id))
	}
	return ts
}

// CreateMultipartUpload 创建会话并提交到 S（W ack）。
//
// 必须在建会话之前确认桶存在：单机路径同样先查桶，否则客户端会拿到一个
// 永远无法 Complete 的 uploadId。
func (c *Cluster) CreateMultipartUpload(bucket, key string, opt store.CreateMultipartOptions) (*store.MultipartUpload, error) {
	if err := store.ValidateObjectPath(bucket, key); err != nil {
		return nil, err
	}
	exists, err := c.BucketExists(bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, store.ErrBucketNotFound
	}

	uploadID := store.NewUploadID()
	st, err := store.NewMultipartUploadState(bucket, key, uploadID, opt, c.now(), c.cfg.NodeID)
	if err != nil {
		return nil, err
	}

	ok, errs := c.putUploadStateToAll(st, c.sessionTargets(uploadID))
	if ok < c.cfg.WriteQuorum {
		return nil, quorumError("创建分片上传会话", ok, c.cfg.WriteQuorum, errs)
	}
	return st.Info(), nil
}

// UploadPart 上传一个分片：块写 O，分片状态写 S。
//
// 校验顺序与错误类型逐项对齐单机实现（store.UploadPart）。这不是重复代码
// 的问题，而是契约问题：同一份客户端请求在单机与集群下必须得到同样的
// 结果，否则排障时无法判断问题出在哪一层。
func (c *Cluster) UploadPart(bucket, key, uploadID string, partNumber int, body io.Reader, opt store.UploadPartOptions) (*store.PartInfo, error) {
	if partNumber < 1 || partNumber > store.MaxPartNumber {
		return nil, fmt.Errorf("%w: %d 不在 1–%d 之间", store.ErrInvalidPartNumber, partNumber, store.MaxPartNumber)
	}
	if opt.ExpectedSize < 0 {
		return nil, fmt.Errorf("%w: 缺少 Content-Length", store.ErrIncompleteBody)
	}

	st, err := c.loadUploadState(uploadID)
	if err != nil {
		return nil, err
	}
	if st.Bucket != bucket || st.Key != key {
		// 与 AWS 一致：uploadId 不属于这个 bucket/key 时按"不存在"处理。
		return nil, store.ErrNoSuchUpload
	}

	alg := st.ChecksumAlgorithm
	adopting := false
	if opt.ChecksumAlgorithm != "" && opt.ChecksumAlgorithm != alg {
		if alg != "" {
			return nil, fmt.Errorf("%w: 本次声明 %s，会话为 %s",
				store.ErrInvalidChecksum, opt.ChecksumAlgorithm, alg)
		}
		// 会话没声明算法而分片带了校验和：采用首个分片声明的算法。
		// 理由见 store.UploadPart 的同名分支（aws-cli 手工 MPU 依赖它）。
		alg = opt.ChecksumAlgorithm
		adopting = true
		if _, ok := checksum.New(alg); !ok {
			return nil, fmt.Errorf("%w: 未知的校验和算法 %q", store.ErrInvalidChecksum, alg)
		}
	}

	tag := fmt.Sprintf("upload %s part %d", uploadID, partNumber)
	ts := c.targetsFor(bucket, key)
	alive := make(map[string]bool, len(ts))
	for _, t := range ts {
		alive[t.id] = true
	}

	dedupedBlocks := 0
	summary, err := c.Store.StreamBlocksSized(body, alg, tag, opt.ExpectedSize, func(b store.Block) error {
		deduped, err := c.distributeBlock(alive, ts, b)
		if err != nil {
			return err
		}
		if len(alive) < c.cfg.WriteQuorum {
			return fmt.Errorf("%w: 分片 %d 的块 %s 仅落在 %d 个副本上，需要 %d",
				store.ErrUnavailable, partNumber, b.Desc.Hash, len(alive), c.cfg.WriteQuorum)
		}
		if deduped {
			dedupedBlocks++
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if summary.Size != opt.ExpectedSize {
		return nil, fmt.Errorf("%w: 收到 %d 字节，Content-Length 声明 %d",
			store.ErrIncompleteBody, summary.Size, opt.ExpectedSize)
	}
	if opt.ContentMD5 != "" && !strings.EqualFold(opt.ContentMD5, summary.ETag) {
		return nil, fmt.Errorf("%w: Content-MD5 声明 %s，实际 %s",
			store.ErrBadDigest, opt.ContentMD5, summary.ETag)
	}
	declared := opt.Checksum
	if declared == "" && alg != "" && opt.LateChecksum != nil {
		if v, ok := opt.LateChecksum(); ok {
			declared = v
		}
	}
	if declared != "" && declared != summary.Checksum {
		return nil, fmt.Errorf("%w: %s 声明 %s，实际 %s",
			store.ErrBadDigest, alg, declared, summary.Checksum)
	}

	ps := &store.PartState{
		Version:    store.FormatVersion,
		PartNumber: partNumber,
		ETag:       summary.ETag,
		Size:       summary.Size,
		Modified:   c.now().UTC().Truncate(time.Second),
		Blocks:     summary.Blocks,
		Checksum:   summary.Checksum,
		Deduped:    dedupedBlocks,
	}

	unlock := c.locks.Lock("mpu:" + uploadID)
	defer unlock()

	// 锁内复核：读请求体期间可能发生了 Abort 或 Complete。锁只在本协调者
	// 进程内生效，跨协调者的竞态由会话 Revision 与 S 的 W 写屏障兜住。
	cur, err := c.loadUploadState(uploadID)
	if err != nil {
		return nil, err
	}
	if cur.Bucket != bucket || cur.Key != key {
		return nil, store.ErrNoSuchUpload
	}
	if cur.ChecksumAlgorithm != "" && cur.ChecksumAlgorithm != alg {
		return nil, fmt.Errorf("%w: 会话已被设为 %s，本次声明 %s",
			store.ErrInvalidChecksum, cur.ChecksumAlgorithm, alg)
	}

	sessions := c.sessionTargets(uploadID)
	if adopting && cur.ChecksumAlgorithm == "" {
		checksumType, err := store.NormalizeChecksumType(alg, "")
		if err != nil {
			return nil, err
		}
		cur.ChecksumAlgorithm = alg
		cur.ChecksumType = checksumType
		cur.Revision++

		ok, errs := c.putUploadStateToAll(cur, sessions)
		// 任一副本以"同 Revision 不同算法"拒绝，说明并发协调者已经先采用了
		// 另一种算法。此时本请求必须失败 —— 与单机"先落盘者赢"一致。
		if err := firstMatching(errs, store.ErrInvalidChecksum); err != nil {
			return nil, fmt.Errorf("%w: 会话已被并发设为其他算法（%v）", store.ErrInvalidChecksum, err)
		}
		if ok < c.cfg.WriteQuorum {
			return nil, quorumError("采用分片校验和算法", ok, c.cfg.WriteQuorum, errs)
		}
	}

	ok, errs := c.putPartStateToAll(uploadID, ps, sessions)
	if ok < c.cfg.WriteQuorum {
		return nil, quorumError("提交分片状态", ok, c.cfg.WriteQuorum, errs)
	}

	return &store.PartInfo{
		PartNumber:        partNumber,
		ETag:              ps.ETag,
		Size:              ps.Size,
		Modified:          ps.Modified,
		Checksum:          ps.Checksum,
		ChecksumAlgorithm: alg,
	}, nil
}

// CompleteMultipartUpload 串接分片块表并提交对象元数据。
//
// 锁顺序与单机一致：先 uploadId 锁、再 key 锁。
func (c *Cluster) CompleteMultipartUpload(bucket, key, uploadID string, parts []store.CompletePart, declaredChecksum string, ifNoneMatchAny bool) (*store.ObjectInfo, error) {
	if len(parts) == 0 {
		return nil, fmt.Errorf("%w: 分片列表为空", store.ErrInvalidPartOrder)
	}

	unlockMPU := c.locks.Lock("mpu:" + uploadID)
	defer unlockMPU()

	st, err := c.loadUploadState(uploadID)
	if err != nil {
		return nil, err
	}
	if st.Bucket != bucket || st.Key != key {
		return nil, store.ErrNoSuchUpload
	}

	merged, err := c.mergedPartStates(uploadID)
	if err != nil {
		return nil, err
	}
	out, err := store.AssembleMultipart(st, parts, func(n int) (*store.PartState, error) {
		ps, ok := merged[n]
		if !ok {
			return nil, store.ErrInvalidPart
		}
		return ps, nil
	}, declaredChecksum)
	if err != nil {
		return nil, err
	}

	unlockKey := c.locks.Lock(bucket + "\x00" + key)
	defer unlockKey()

	ts := c.targetsFor(bucket, key)
	probe := c.probeMetas(bucket, key, ts)
	if responded, _ := countResponses(probe); responded < c.cfg.ReadQuorum {
		return nil, fmt.Errorf("%w: 只有 %d/%d 个对象副本响应，需要 %d",
			store.ErrUnavailable, responded, len(ts), c.cfg.ReadQuorum)
	}
	if ifNoneMatchAny && liveCount(probe) > 0 {
		return nil, store.ErrPreconditionFailed
	}
	var seen uint64
	if m := latestMeta(probe); m != nil {
		seen = m.ObjVersion
	}

	meta := &store.ObjectMeta{
		Version:           store.FormatVersion,
		Key:               key,
		Size:              out.Size,
		ETag:              out.ETag,
		ContentType:       st.ContentType,
		Modified:          c.now().UTC().Truncate(time.Second),
		UserMeta:          st.UserMeta,
		Blocks:            out.Blocks,
		Checksum:          out.Checksum,
		ChecksumAlgorithm: string(out.ChecksumAlgorithm),
		ChecksumType:      out.ChecksumType,
		NodeID:            c.cfg.NodeID,
		ObjVersion:        c.clock.Next(seen),
		Parts:             out.Parts,
	}

	// meta 发给全部 O 目标而不是挑 W 个：读路径的 readSegment 会逐块回退，
	// 多一份 meta 就多一个可读入口，成本只是一次小 JSON 写。
	ok, errs := c.putMetaToAll(bucket, key, meta, ts)
	if ok < c.cfg.WriteQuorum {
		return nil, quorumError("提交对象元数据", ok, c.cfg.WriteQuorum, errs)
	}

	// Complete 成功后补齐缺 meta 的 O 目标。块的检查交给修复执行器：
	// 分片数据在 UploadPart 时已按 O 分发，正常路径下它们都在，
	// 逐个 HEAD 只会把 Complete 的延迟从"串接块表"拖成"遍历所有分片"。
	for i, err := range errs {
		if err != nil && !isStaleError(err) {
			c.repair.enqueue(ts[i].id, repairBlocks, bucket, key, meta.ObjVersion)
		}
	}

	// 对象已提交：会话清理是尽力而为。失败只会在 S 上留下过期状态，
	// 由保留期清扫回收；此时向客户端报错反而会导致"对象已存在却收到失败"。
	sessions := c.sessionTargets(uploadID)
	if ok, errs := c.deleteUploadStateAll(uploadID, sessions); ok < len(sessions) {
		for i, err := range errs {
			if err != nil {
				c.logger.Warn("mpu_session_cleanup_failed",
					"upload", uploadID, "peer", sessions[i].id, "err", err.Error())
			}
		}
	}

	return &store.ObjectInfo{
		Key:               key,
		Size:              out.Size,
		ETag:              out.ETag,
		Modified:          meta.Modified,
		ContentType:       st.ContentType,
		UserMeta:          st.UserMeta,
		Checksum:          out.Checksum,
		ChecksumAlgorithm: st.ChecksumAlgorithm,
		ChecksumType:      st.ChecksumType,
		BlockCount:        out.BlockCount,
		DedupedBlocks:     out.Deduped,
		Version:           meta.ObjVersion,
		Parts:             store.PartBounds(out.Parts),
	}, nil
}

// AbortMultipartUpload 删除 S 上的会话。
//
// 分片数据块不在这里删除：它们已进入 CAS，可能被同内容的其他对象/分片
// 引用，由块 GC 统一回收（与单机行为一致）。
func (c *Cluster) AbortMultipartUpload(bucket, key, uploadID string) error {
	unlock := c.locks.Lock("mpu:" + uploadID)
	defer unlock()

	st, err := c.loadUploadState(uploadID)
	if err != nil {
		return err
	}
	if st.Bucket != bucket || st.Key != key {
		return store.ErrNoSuchUpload
	}

	sessions := c.sessionTargets(uploadID)
	ok, errs := c.deleteUploadStateAll(uploadID, sessions)
	if ok < c.cfg.WriteQuorum {
		return quorumError("删除分片上传会话", ok, c.cfg.WriteQuorum, errs)
	}
	return nil
}

// ListParts 合并 S 上的分片状态后分页。
//
// 分页语义必须与单机逐项一致：NextPartNumberMarker 是"本页最后一个分片号"，
// 未截断时归零 —— 客户端取消分页时依赖它判断是否还有下一页。
func (c *Cluster) ListParts(bucket, key, uploadID string, opt store.ListPartsOptions) (*store.ListPartsResult, error) {
	st, err := c.loadUploadState(uploadID)
	if err != nil {
		return nil, err
	}
	if st.Bucket != bucket || st.Key != key {
		return nil, store.ErrNoSuchUpload
	}

	res := &store.ListPartsResult{Upload: *st.Info()}
	if opt.MaxParts <= 0 {
		return res, nil
	}

	merged, err := c.mergedPartStates(uploadID)
	if err != nil {
		return nil, err
	}

	numbers := make([]int, 0, len(merged))
	for n := range merged {
		if n > opt.PartNumberMarker {
			numbers = append(numbers, n)
		}
	}
	sort.Ints(numbers)

	for _, n := range numbers {
		if len(res.Parts) >= opt.MaxParts {
			res.IsTruncated = true
			break
		}
		ps := merged[n]
		res.Parts = append(res.Parts, store.PartInfo{
			PartNumber:        ps.PartNumber,
			ETag:              ps.ETag,
			Size:              ps.Size,
			Modified:          ps.Modified,
			Checksum:          ps.Checksum,
			ChecksumAlgorithm: st.ChecksumAlgorithm,
		})
		res.NextPartNumberMarker = ps.PartNumber
	}
	if !res.IsTruncated {
		res.NextPartNumberMarker = 0
	}
	return res, nil
}

// ListMultipartUploads 扇形到全部节点后按 uploadId 归并去重。
//
// 不能只问 S：会话的放置键是 uploadId 本身，而列举请求只给了 bucket。
// 任何节点都可能持有属于该桶的会话，漏掉一个就会让客户端看到不完整列表。
func (c *Cluster) ListMultipartUploads(bucket string, opt store.ListMultipartUploadsOptions) (*store.ListMultipartUploadsResult, error) {
	exists, err := c.BucketExists(bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, store.ErrBucketNotFound
	}

	states, err := c.allUploadStates(bucket)
	if err != nil {
		return nil, err
	}
	infos := make([]store.MultipartUploadInfo, 0, len(states))
	for _, st := range states {
		infos = append(infos, store.MultipartUploadInfo{
			Key:               st.Key,
			UploadID:          st.UploadID,
			Initiated:         st.Initiated,
			ChecksumAlgorithm: st.ChecksumAlgorithm,
			ChecksumType:      st.ChecksumType,
		})
	}
	return store.PageUploads(infos, opt), nil
}

// ---------------------------------------------------------------------------
// 会话读取与合并
// ---------------------------------------------------------------------------

type uploadStateResult struct {
	id    string
	state *store.UploadState
	found bool
	err   error
}

// probeUploadStates 并行读取 S 上的会话。返回顺序与 ts 一致。
func (c *Cluster) probeUploadStates(uploadID string, ts []target) []uploadStateResult {
	results := make([]uploadStateResult, len(ts))
	var wg sync.WaitGroup
	for i, t := range ts {
		wg.Add(1)
		go func(i int, t target) {
			defer wg.Done()
			results[i] = uploadStateResult{id: t.id}
			st, found, err := t.getUploadState(context.Background(), uploadID)
			results[i].state, results[i].found, results[i].err = st, found, err
		}(i, t)
	}
	wg.Wait()
	return results
}

// loadUploadState 从 S 读出最新会话。
//
// 合并规则只依赖 W+R>|S|：更高 Revision 覆盖更低；同 Revision 上出现两个
// 非空且不同的算法，说明并发采用真的发生了，必须拒绝而不是随便挑一个。
func (c *Cluster) loadUploadState(uploadID string) (*store.UploadState, error) {
	ts := c.sessionTargets(uploadID)
	results := c.probeUploadStates(uploadID, ts)

	responded := 0
	var latest *store.UploadState
	var conflict error
	var firstErr error
	for _, r := range results {
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		responded++
		if !r.found {
			continue
		}
		if latest == nil {
			latest = r.state
			continue
		}
		switch {
		case r.state.Revision > latest.Revision:
			latest = r.state
			conflict = nil
		case r.state.Revision == latest.Revision:
			if latest.ChecksumAlgorithm != "" && r.state.ChecksumAlgorithm != "" &&
				latest.ChecksumAlgorithm != r.state.ChecksumAlgorithm {
				conflict = fmt.Errorf("%w: 会话 %s 在 revision %d 上出现冲突算法 %s/%s",
					store.ErrInvalidChecksum, uploadID, latest.Revision,
					latest.ChecksumAlgorithm, r.state.ChecksumAlgorithm)
			}
		}
	}

	if responded < c.cfg.ReadQuorum {
		return nil, fmt.Errorf("%w: 会话 %s 仅 %d/%d 个副本可响应（%v）",
			store.ErrUnavailable, uploadID, responded, len(ts), firstErr)
	}
	if latest == nil {
		return nil, store.ErrNoSuchUpload
	}
	if conflict != nil {
		return nil, conflict
	}
	return latest, nil
}

// mergedPartStates 合并 S 上的全部分片状态，按 partNumber 取最新。
//
// 分片可能被重传（同编号覆盖），因此胜负由 (Modified, ETag) 决定；
// Modified 只到秒，ETag 只是同秒内的确定性决胜，不承载"谁更新"的语义。
func (c *Cluster) mergedPartStates(uploadID string) (map[int]*store.PartState, error) {
	ts := c.sessionTargets(uploadID)
	results := make([][]*store.PartState, len(ts))
	errs := make([]error, len(ts))

	var wg sync.WaitGroup
	for i, t := range ts {
		wg.Add(1)
		go func(i int, t target) {
			defer wg.Done()
			results[i], errs[i] = t.listPartStates(context.Background(), uploadID)
		}(i, t)
	}
	wg.Wait()

	merged := make(map[int]*store.PartState)
	responded := 0
	var firstErr error
	for i := range ts {
		if errs[i] != nil {
			if firstErr == nil {
				firstErr = errs[i]
			}
			continue
		}
		responded++
		for _, ps := range results[i] {
			if ps == nil {
				continue
			}
			if cur, ok := merged[ps.PartNumber]; !ok || partStateNewer(ps, cur) {
				merged[ps.PartNumber] = ps
			}
		}
	}
	if responded < c.cfg.ReadQuorum {
		return nil, fmt.Errorf("%w: 会话 %s 的分片状态仅 %d/%d 个副本可读（%v）",
			store.ErrUnavailable, uploadID, responded, len(ts), firstErr)
	}
	return merged, nil
}

// partStateNewer 判定 a 是否应该覆盖 b。
func partStateNewer(a, b *store.PartState) bool {
	if !a.Modified.Equal(b.Modified) {
		return a.Modified.After(b.Modified)
	}
	return a.ETag > b.ETag
}

// allUploadStates 汇总全部节点上属于该桶的会话，按 uploadId 去重取最高 Revision。
func (c *Cluster) allUploadStates(bucket string) ([]*store.UploadState, error) {
	ts := c.allTargets()
	results := make([][]*store.UploadState, len(ts))
	errs := make([]error, len(ts))

	var wg sync.WaitGroup
	for i, t := range ts {
		wg.Add(1)
		go func(i int, t target) {
			defer wg.Done()
			results[i], errs[i] = t.listUploadStates(context.Background(), bucket)
		}(i, t)
	}
	wg.Wait()

	// 任何一个节点不可达都返回 503：残缺的列表比错误更危险 ——
	// 客户端可能据此判断"没有进行中的上传"。
	for i := range ts {
		if errs[i] != nil {
			return nil, fmt.Errorf("%w: 读取节点 %s 的分片上传列表失败：%v",
				store.ErrUnavailable, ts[i].id, errs[i])
		}
	}

	latest := make(map[string]*store.UploadState)
	for _, states := range results {
		for _, st := range states {
			if st == nil {
				continue
			}
			if cur, ok := latest[st.UploadID]; !ok || st.Revision > cur.Revision {
				latest[st.UploadID] = st
			}
		}
	}

	out := make([]*store.UploadState, 0, len(latest))
	for _, st := range latest {
		if st.Bucket == bucket {
			out = append(out, st)
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// 扇出原语
// ---------------------------------------------------------------------------

// putUploadStateToAll 并行写会话。errs 的顺序与 ts 一致，便于定位失败节点。
func (c *Cluster) putUploadStateToAll(st *store.UploadState, ts []target) (int, []error) {
	errs := make([]error, len(ts))
	var ok int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i, t := range ts {
		wg.Add(1)
		go func(i int, t target) {
			defer wg.Done()
			if err := t.putUploadState(context.Background(), st); err != nil {
				errs[i] = err
				return
			}
			mu.Lock()
			ok++
			mu.Unlock()
		}(i, t)
	}
	wg.Wait()
	return ok, errs
}

// putPartStateToAll 并行提交分片状态。
func (c *Cluster) putPartStateToAll(uploadID string, ps *store.PartState, ts []target) (int, []error) {
	errs := make([]error, len(ts))
	var ok int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i, t := range ts {
		wg.Add(1)
		go func(i int, t target) {
			defer wg.Done()
			if err := t.putPartState(context.Background(), uploadID, ps); err != nil {
				errs[i] = err
				return
			}
			mu.Lock()
			ok++
			mu.Unlock()
		}(i, t)
	}
	wg.Wait()
	return ok, errs
}

// deleteUploadStateAll 并行删除会话。删除是幂等的，缺会话也算成功。
func (c *Cluster) deleteUploadStateAll(uploadID string, ts []target) (int, []error) {
	errs := make([]error, len(ts))
	var ok int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i, t := range ts {
		wg.Add(1)
		go func(i int, t target) {
			defer wg.Done()
			if err := t.deleteUploadState(context.Background(), uploadID); err != nil {
				errs[i] = err
				return
			}
			mu.Lock()
			ok++
			mu.Unlock()
		}(i, t)
	}
	wg.Wait()
	return ok, errs
}

// quorumError 把"只拿到 ok/need 个确认"整理成 ErrUnavailable。
//
// 只带第一个失败原因：足够定位，又不会把整份成员表刷进客户端响应。
func quorumError(op string, ok, need int, errs []error) error {
	for _, err := range errs {
		if err != nil {
			return fmt.Errorf("%w: %s仅 %d/%d 个节点确认：%v", store.ErrUnavailable, op, ok, need, err)
		}
	}
	return fmt.Errorf("%w: %s仅 %d/%d 个节点确认", store.ErrUnavailable, op, ok, need)
}

// firstMatching 返回第一个满足 errors.Is(err, target) 的错误。
func firstMatching(errs []error, target error) error {
	for _, err := range errs {
		if errors.Is(err, target) {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// target 的会话操作：本地走 *store.Store，远端走 PeerClient
// ---------------------------------------------------------------------------

func (t target) getUploadState(ctx context.Context, uploadID string) (*store.UploadState, bool, error) {
	if t.local != nil {
		st, err := t.local.LoadUploadState(uploadID)
		if errors.Is(err, store.ErrNoSuchUpload) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		return st, true, nil
	}
	return t.client.GetUploadState(ctx, uploadID)
}

func (t target) putUploadState(ctx context.Context, st *store.UploadState) error {
	if t.local != nil {
		return t.local.PutUploadState(st)
	}
	return t.client.PutUploadState(ctx, st)
}

func (t target) deleteUploadState(ctx context.Context, uploadID string) error {
	if t.local != nil {
		_, err := t.local.DeleteUploadState(uploadID)
		return err
	}
	_, err := t.client.DeleteUploadState(ctx, uploadID)
	return err
}

func (t target) putPartState(ctx context.Context, uploadID string, ps *store.PartState) error {
	if t.local != nil {
		return t.local.PutPartState(uploadID, ps)
	}
	return t.client.PutPartState(ctx, uploadID, ps)
}

func (t target) listPartStates(ctx context.Context, uploadID string) ([]*store.PartState, error) {
	if t.local != nil {
		parts, err := t.local.ListPartStates(uploadID)
		// 会话不在这个副本上：它只是没参与这次会话，不是错误。
		if errors.Is(err, store.ErrNoSuchUpload) {
			return nil, nil
		}
		return parts, err
	}
	return t.client.ListPartStates(ctx, uploadID)
}

func (t target) listUploadStates(ctx context.Context, bucket string) ([]*store.UploadState, error) {
	if t.local != nil {
		return t.local.ListUploadStates(bucket)
	}
	return t.client.ListUploadStates(ctx, bucket)
}
