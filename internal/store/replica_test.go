package store

import (
	"bytes"
	"errors"
	"io/fs"
	"testing"
	"time"

	"github.com/binbinpeng123/Blink/internal/checksum"
)

// replicaMeta 构造一份结构自洽的副本元数据，供各用例改写其中一项。
func replicaMeta(key string, version uint64, nodeID string, blocks ...BlockDesc) *ObjectMeta {
	var size int64
	for _, b := range blocks {
		size += b.Length
	}
	return &ObjectMeta{
		Version:     FormatVersion,
		Key:         key,
		Size:        size,
		ETag:        "d41d8cd98f00b204e9800998ecf8427e",
		ContentType: DefaultContentType,
		Modified:    time.Unix(1_700_000_000, 0).UTC(),
		Blocks:      blocks,
		NodeID:      nodeID,
		ObjVersion:  version,
	}
}

func singleBlock(hash string, length int64) BlockDesc {
	return BlockDesc{Hash: hash, Offset: 0, Length: length}
}

// descOf 构造与块表一致的块描述：读路径要拿 Length 与 CRC32C 校验盘上内容。
func descOf(data []byte) BlockDesc {
	return BlockDesc{
		Hash:   BlockHash(data),
		Length: int64(len(data)),
		CRC32C: checksum.BlockCRC32C(data),
	}
}

// TestPutReplicaMetaVersionOrder 验证副本按 (ObjVersion, NodeID) 拒旧版本。
//
// 这是并发覆盖写的收敛保证：无论旧版本多晚到达，副本都不会回退。
func TestPutReplicaMetaVersionOrder(t *testing.T) {
	s := newTestStore(t)
	const bucket, key = "test-bucket", "obj"
	hash := BlockHash([]byte("hello"))

	if err := s.PutReplicaMeta(bucket, key, replicaMeta(key, 2, "node-b", singleBlock(hash, 5))); err != nil {
		t.Fatalf("首次写入失败: %v", err)
	}

	// 版本号更小 — 必须拒绝。
	if err := s.PutReplicaMeta(bucket, key, replicaMeta(key, 1, "node-c", singleBlock(hash, 5))); !errors.Is(err, ErrStaleMeta) {
		t.Fatalf("低版本写入应返回 ErrStaleMeta，实际 %v", err)
	}

	// 同版本但 node_id 字典序更小 — 同样拒绝，保证平局有确定赢家。
	if err := s.PutReplicaMeta(bucket, key, replicaMeta(key, 2, "node-a", singleBlock(hash, 5))); !errors.Is(err, ErrStaleMeta) {
		t.Fatalf("同版本低 node_id 应返回 ErrStaleMeta，实际 %v", err)
	}

	// 同版本同 node_id — 幂等重写，必须成功。
	if err := s.PutReplicaMeta(bucket, key, replicaMeta(key, 2, "node-b", singleBlock(hash, 5))); err != nil {
		t.Fatalf("幂等重写应成功，实际 %v", err)
	}

	// 更高版本 — 成功。
	if err := s.PutReplicaMeta(bucket, key, replicaMeta(key, 3, "node-a", singleBlock(hash, 5))); err != nil {
		t.Fatalf("高版本写入应成功，实际 %v", err)
	}

	m, found, err := s.LoadReplicaMeta(bucket, key)
	if err != nil || !found {
		t.Fatalf("LoadReplicaMeta 失败: found=%v err=%v", found, err)
	}
	if m.ObjVersion != 3 || m.NodeID != "node-a" {
		t.Fatalf("最终版本应为 (3,node-a)，实际 (%d,%s)", m.ObjVersion, m.NodeID)
	}
}

// TestPutReplicaMetaRejectsInconsistent 验证结构自相矛盾的元数据不会落盘。
func TestPutReplicaMetaRejectsInconsistent(t *testing.T) {
	s := newTestStore(t)
	const bucket, key = "test-bucket", "obj"
	hash := BlockHash([]byte("hello"))

	cases := []struct {
		name string
		meta *ObjectMeta
	}{
		{
			name: "key 不符",
			meta: replicaMeta("other", 1, "n", singleBlock(hash, 5)),
		},
		{
			name: "块哈希非法",
			meta: replicaMeta(key, 1, "n", singleBlock("abc", 5)),
		},
		{
			name: "偏移不连续",
			meta: replicaMeta(key, 1, "n", BlockDesc{Hash: hash, Offset: 1, Length: 5}),
		},
		{
			name: "块总长与大小不符",
			meta: func() *ObjectMeta {
				m := replicaMeta(key, 1, "n", singleBlock(hash, 5))
				m.Size = 99
				return m
			}(),
		},
		{
			name: "缺少 node_id",
			meta: replicaMeta(key, 1, "", singleBlock(hash, 5)),
		},
		{
			name: "版本号为 0",
			meta: replicaMeta(key, 0, "n", singleBlock(hash, 5)),
		},
		{
			name: "格式版本过高",
			meta: func() *ObjectMeta {
				m := replicaMeta(key, 1, "n", singleBlock(hash, 5))
				m.Version = FormatVersion + 1
				return m
			}(),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := s.PutReplicaMeta(bucket, key, c.meta)
			if !errors.Is(err, ErrCorrupt) && !errors.Is(err, ErrInvalidBlock) {
				t.Fatalf("应拒绝非法元数据，实际 %v", err)
			}
			if _, found, _ := s.LoadReplicaMeta(bucket, key); found {
				t.Fatal("被拒绝的元数据不能落盘")
			}
		})
	}
}

// TestLoadReplicaMetaMissing 验证"没有副本"与"副本损坏"的区别。
func TestLoadReplicaMetaMissing(t *testing.T) {
	s := newTestStore(t)
	m, found, err := s.LoadReplicaMeta("nobucket", "nokey")
	if err != nil || found || m != nil {
		t.Fatalf("缺失副本应返回 (nil,false,nil)，实际 (%v,%v,%v)", m, found, err)
	}
}

// TestBlockRoundTrip 验证块写入、整块读取与部分区间读取。
func TestBlockRoundTrip(t *testing.T) {
	s := newTestStore(t)
	data := []byte("0123456789abcdef")
	hash := BlockHash(data)

	wrote, err := s.PutBlock(hash, data)
	if err != nil || !wrote {
		t.Fatalf("PutBlock 失败: wrote=%v err=%v", wrote, err)
	}
	// 同内容第二次写入应命中去重。
	wrote, err = s.PutBlock(hash, data)
	if err != nil || wrote {
		t.Fatalf("重复 PutBlock 应去重: wrote=%v err=%v", wrote, err)
	}

	size, err := s.BlockSize(hash)
	if err != nil || size != int64(len(data)) {
		t.Fatalf("BlockSize = %d, %v；期望 %d", size, err, len(data))
	}

	desc := descOf(data)

	var full bytes.Buffer
	if err := s.ReadBlockRange(&full, desc, 0, size); err != nil {
		t.Fatalf("整块读取失败: %v", err)
	}
	if !bytes.Equal(full.Bytes(), data) {
		t.Fatalf("整块内容不符: %q", full.Bytes())
	}

	var mid bytes.Buffer
	if err := s.ReadBlockRange(&mid, desc, 4, 6); err != nil {
		t.Fatalf("区间读取失败: %v", err)
	}
	if got := mid.String(); got != "456789" {
		t.Fatalf("区间内容 = %q，期望 456789", got)
	}

	// 越界必须报损坏，而不是静默返回短数据。
	if err := s.ReadBlockRange(&bytes.Buffer{}, desc, 10, 100); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("越界读取应返回 ErrCorrupt，实际 %v", err)
	}

	// 块表长度与盘上文件不符同样报损坏：这是元数据与块文件分叉的证据。
	short := desc
	short.Length = size + 1
	if err := s.ReadBlockRange(&bytes.Buffer{}, short, 0, 1); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("长度不符应返回 ErrCorrupt，实际 %v", err)
	}
}

// TestBlockHashValidation 验证非法哈希在触碰文件系统之前就被拒绝。
//
// 短哈希尤其重要：casPath 用 hash[:2] 拼目录，不校验会直接 panic。
func TestBlockHashValidation(t *testing.T) {
	s := newTestStore(t)
	bad := []string{"", "ab", "abzz", string(bytes.Repeat([]byte("A"), 64))}
	for _, h := range bad {
		if _, err := s.PutBlock(h, []byte("x")); !errors.Is(err, ErrInvalidBlock) {
			t.Fatalf("PutBlock(%q) 应返回 ErrInvalidBlock，实际 %v", h, err)
		}
		if _, err := s.BlockSize(h); !errors.Is(err, ErrInvalidBlock) {
			t.Fatalf("BlockSize(%q) 应返回 ErrInvalidBlock，实际 %v", h, err)
		}
		if err := s.ReadBlockRange(&bytes.Buffer{}, BlockDesc{Hash: h, Length: 1}, 0, 1); !errors.Is(err, ErrInvalidBlock) {
			t.Fatalf("ReadBlockRange(%q) 应返回 ErrInvalidBlock，实际 %v", h, err)
		}
	}

	// 合法但不存在的块：错误必须能被 errors.Is(err, fs.ErrNotExist) 识别，
	// 读路径据此换源。
	missing := BlockHash([]byte("never written"))
	if _, err := s.BlockSize(missing); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("缺失块应可识别为 fs.ErrNotExist，实际 %v", err)
	}
}

// tombstoneMeta 构造一条删除墓碑：只有版本与节点，没有内容。
func tombstoneMeta(key string, version uint64, nodeID string) *ObjectMeta {
	return &ObjectMeta{
		Version:    FormatVersion,
		Key:        key,
		Modified:   time.Unix(1_700_000_000, 0).UTC(),
		NodeID:     nodeID,
		ObjVersion: version,
		Deleted:    true,
	}
}

// TestTombstoneLifecycle 验证墓碑的完整语义：
//
//	活副本 → 墓碑（接受）→ 旧活版本写回（拒绝）→ 更高版本活副本（接受）
//
// 第一步与第二步是"删除后不复活"的全部保证；第三步保证删除之后
// 同一个 key 还能重新上传。
func TestTombstoneLifecycle(t *testing.T) {
	s := newTestStore(t)
	const bucket, key = "test-bucket", "obj"
	hash := BlockHash([]byte("hello"))
	if err := s.CreateBucket(bucket); err != nil {
		t.Fatalf("建桶失败: %v", err)
	}

	if err := s.PutReplicaMeta(bucket, key, replicaMeta(key, 2, "node-b", singleBlock(hash, 5))); err != nil {
		t.Fatalf("写入活副本失败: %v", err)
	}
	if err := s.PutReplicaMeta(bucket, key, tombstoneMeta(key, 3, "node-a")); err != nil {
		t.Fatalf("写入墓碑失败: %v", err)
	}

	m, found, err := s.LoadReplicaMeta(bucket, key)
	if err != nil || !found {
		t.Fatalf("读取墓碑失败: found=%v err=%v", found, err)
	}
	if !m.Deleted || m.Size != 0 || len(m.Blocks) != 0 {
		t.Fatalf("墓碑内容 = %+v，期望 Deleted 且无内容", m)
	}

	// 删除时不可达的节点恢复后写回旧副本：必须被墓碑挡住。
	if err := s.PutReplicaMeta(bucket, key, replicaMeta(key, 2, "node-c", singleBlock(hash, 5))); !errors.Is(err, ErrStaleMeta) {
		t.Fatalf("旧活版本写回应返回 ErrStaleMeta，实际 %v", err)
	}
	// 同版本但 node_id 更小同样被拒（版本相同时的确定性裁决对墓碑生效）。
	if err := s.PutReplicaMeta(bucket, key, tombstoneMeta(key, 3, "node-0")); !errors.Is(err, ErrStaleMeta) {
		t.Fatalf("同版本低 node_id 应为 ErrStaleMeta，实际 %v", err)
	}

	// 删除后重新上传：版本更高，必须成功，墓碑被覆盖。
	if err := s.PutReplicaMeta(bucket, key, replicaMeta(key, 4, "node-a", singleBlock(hash, 5))); err != nil {
		t.Fatalf("删除后重新上传失败: %v", err)
	}
	m, found, err = s.LoadReplicaMeta(bucket, key)
	if err != nil || !found || m.Deleted {
		t.Fatalf("重新上传后状态 = %+v found=%v err=%v", m, found, err)
	}

	// 整桶扫描必须能把墓碑与活对象区分开。
	if err := s.PutReplicaMeta(bucket, "gone", tombstoneMeta("gone", 1, "node-a")); err != nil {
		t.Fatalf("写入第二个墓碑失败: %v", err)
	}
	entries, err := s.ScanReplicaEntries(bucket)
	if err != nil {
		t.Fatalf("扫描副本失败: %v", err)
	}
	got := make(map[string]bool, len(entries))
	for _, e := range entries {
		got[e.Key] = e.Deleted
	}
	if deleted, ok := got["gone"]; !ok || !deleted {
		t.Fatalf("扫描结果缺少 gone 的墓碑: %+v", entries)
	}
	if deleted, ok := got[key]; !ok || deleted {
		t.Fatalf("扫描结果中 %s 应为活对象: %+v", key, entries)
	}
}

// TestPutReplicaMetaRejectsInvalidTombstone 验证"墓碑"不是绕过内容校验的
// 后门：带大小或块表的墓碑是自相矛盾的元数据，必须拒绝。
func TestPutReplicaMetaRejectsInvalidTombstone(t *testing.T) {
	s := newTestStore(t)
	const bucket, key = "test-bucket", "obj"
	hash := BlockHash([]byte("hello"))

	cases := []struct {
		name string
		meta *ObjectMeta
	}{
		{
			name: "墓碑带大小",
			meta: func() *ObjectMeta {
				m := tombstoneMeta(key, 1, "n")
				m.Size = 5
				return m
			}(),
		},
		{
			name: "墓碑带块表",
			meta: func() *ObjectMeta {
				m := tombstoneMeta(key, 1, "n")
				m.Blocks = []BlockDesc{singleBlock(hash, 5)}
				return m
			}(),
		},
		{
			name: "墓碑缺少 node_id",
			meta: tombstoneMeta(key, 1, ""),
		},
		{
			name: "墓碑版本号为 0",
			meta: tombstoneMeta(key, 0, "n"),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := s.PutReplicaMeta(bucket, key, c.meta); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("应拒绝非法墓碑，实际 %v", err)
			}
			if _, found, _ := s.LoadReplicaMeta(bucket, key); found {
				t.Fatal("被拒绝的墓碑不能落盘")
			}
		})
	}
}

// TestDeleteReplicaMeta 验证删除只移除对象元数据，不动 CAS 块。
func TestDeleteReplicaMeta(t *testing.T) {
	s := newTestStore(t)
	const bucket, key = "test-bucket", "obj"
	data := []byte("payload")
	hash := BlockHash(data)

	if wrote, err := s.PutBlock(hash, data); err != nil || !wrote {
		t.Fatalf("PutBlock 失败: %v", err)
	}
	if err := s.PutReplicaMeta(bucket, key, replicaMeta(key, 1, "n", singleBlock(hash, int64(len(data))))); err != nil {
		t.Fatalf("PutReplicaMeta 失败: %v", err)
	}

	found, err := s.DeleteReplicaMeta(bucket, key)
	if err != nil || !found {
		t.Fatalf("首次删除应 found=true，实际 (%v,%v)", found, err)
	}
	// 幂等：第二次删除不再报告存在。
	found, err = s.DeleteReplicaMeta(bucket, key)
	if err != nil || found {
		t.Fatalf("重复删除应 found=false，实际 (%v,%v)", found, err)
	}
	if _, exists, _ := s.LoadReplicaMeta(bucket, key); exists {
		t.Fatal("删除后不应还能读到副本元数据")
	}
	// 块必须保留：它可能被其他 key 去重引用。
	if ok, err := s.BlockExists(hash); err != nil || !ok {
		t.Fatalf("删除元数据不应删除 CAS 块: ok=%v err=%v", ok, err)
	}
}
