package store

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
)

// readObject 按对象读回全部内容，供复制测试比对字节。
func readObject(t *testing.T, s *Store, bucket, key string) []byte {
	t.Helper()
	r, err := s.OpenObject(bucket, key)
	if err != nil {
		t.Fatalf("打开 %s 失败: %v", key, err)
	}
	var buf bytes.Buffer
	if err := r.WriteRangeTo(&buf, 0, r.Info().Size); err != nil {
		t.Fatalf("读取 %s 失败: %v", key, err)
	}
	return buf.Bytes()
}

// TestCopyObjectSharesBlocks 验证复制的核心承诺：O(1) 元数据复制。
//
// 判据不是"耗时很短"（那不可靠），而是"CAS 里的块文件数一个都没增加"。
// 块数量增加就说明实现偷偷复制了数据，边端上复制大模型会立刻退化成
// 一次完整的读写往返。
func TestCopyObjectSharesBlocks(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	const body = "hello world, this is a copy source for blink"
	putString(t, s, "bkt", "src.txt", body)

	casDir := filepath.Join(s.Root(), casDirName)
	before := countFiles(t, casDir)

	dst, err := s.CopyObject("bkt", "dst.txt", "bkt", "src.txt", CopyOptions{})
	if err != nil {
		t.Fatalf("CopyObject 失败: %v", err)
	}
	if after := countFiles(t, casDir); after != before {
		t.Errorf("复制后 CAS 文件数 %d → %d，说明数据被重新写了一遍", before, after)
	}
	if dst.Size != int64(len(body)) {
		t.Errorf("Size = %d，期望 %d", dst.Size, len(body))
	}
	if dst.BlockCount == 0 {
		t.Error("BlockCount 不应为 0：块是从源对象继承的")
	}
	if got := readObject(t, s, "bkt", "dst.txt"); string(got) != body {
		t.Errorf("复制后内容 = %q，期望 %q", got, body)
	}

	srcMeta, err := s.loadObjectMeta("bkt", "src.txt")
	if err != nil {
		t.Fatalf("读取源 meta 失败: %v", err)
	}
	dstMeta, err := s.loadObjectMeta("bkt", "dst.txt")
	if err != nil {
		t.Fatalf("读取目标 meta 失败: %v", err)
	}
	if len(srcMeta.Blocks) != len(dstMeta.Blocks) {
		t.Fatalf("块数不一致：源 %d，目标 %d", len(srcMeta.Blocks), len(dstMeta.Blocks))
	}
	for i := range srcMeta.Blocks {
		a, b := srcMeta.Blocks[i], dstMeta.Blocks[i]
		if a.Hash != b.Hash || a.Offset != b.Offset || a.Length != b.Length || a.CRC32C != b.CRC32C {
			t.Errorf("第 %d 块描述不一致：源 %+v，目标 %+v", i, a, b)
		}
	}
	// 块表必须是独立副本：共享底层数组会让将来的原地修改（如修复）
	// 同时改到源对象。
	if len(srcMeta.Blocks) > 0 {
		srcMeta.Blocks[0].Offset = -1
		if dstMeta.Blocks[0].Offset == -1 {
			t.Error("源与目标的块表共享了底层数组")
		}
	}
}

// TestCopyObjectAcrossBuckets 验证跨桶复制。
//
// 跨桶是 CAS 去重的最大受益场景：同一个数据集在多个桶里各留一份
// 逻辑副本，磁盘上只存一份字节。
func TestCopyObjectAcrossBuckets(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	if err := s.CreateBucket("mirror"); err != nil {
		t.Fatalf("建桶失败: %v", err)
	}
	const body = "cross bucket payload"
	putString(t, s, "bkt", "model.bin", body)

	casDir := filepath.Join(s.Root(), casDirName)
	before := countFiles(t, casDir)

	if _, err := s.CopyObject("mirror", "model.bin", "bkt", "model.bin", CopyOptions{}); err != nil {
		t.Fatalf("跨桶复制失败: %v", err)
	}
	if after := countFiles(t, casDir); after != before {
		t.Errorf("跨桶复制新增了 %d 个 CAS 文件", after-before)
	}
	if got := readObject(t, s, "mirror", "model.bin"); string(got) != body {
		t.Errorf("目标桶内容 = %q，期望 %q", got, body)
	}
}

// TestCopyObjectMetadataDirective 覆盖 COPY 与 REPLACE 两种元数据语义。
func TestCopyObjectMetadataDirective(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	if _, err := s.PutObject("bkt", "src", bytes.NewReader([]byte("payload")), PutOptions{
		ExpectedSize: 7,
		ContentType:  "image/png",
		UserMeta:     map[string]string{"owner": "edge-01"},
	}); err != nil {
		t.Fatalf("写源对象失败: %v", err)
	}

	t.Run("COPY 保留源元数据", func(t *testing.T) {
		info, err := s.CopyObject("bkt", "copy", "bkt", "src", CopyOptions{})
		if err != nil {
			t.Fatalf("CopyObject 失败: %v", err)
		}
		if info.ContentType != "image/png" {
			t.Errorf("ContentType = %q，期望 image/png", info.ContentType)
		}
		if info.UserMeta["owner"] != "edge-01" {
			t.Errorf("UserMeta = %v，期望保留 owner=edge-01", info.UserMeta)
		}
	})

	t.Run("REPLACE 覆盖元数据", func(t *testing.T) {
		info, err := s.CopyObject("bkt", "replace", "bkt", "src", CopyOptions{
			ReplaceMetadata: true,
			ContentType:     "text/plain",
			UserMeta:        map[string]string{"owner": "edge-02"},
		})
		if err != nil {
			t.Fatalf("CopyObject 失败: %v", err)
		}
		if info.ContentType != "text/plain" {
			t.Errorf("ContentType = %q，期望 text/plain", info.ContentType)
		}
		if info.UserMeta["owner"] != "edge-02" {
			t.Errorf("UserMeta = %v，期望 owner=edge-02", info.UserMeta)
		}
	})

	t.Run("REPLACE 清空 UserMeta 并落默认 ContentType", func(t *testing.T) {
		info, err := s.CopyObject("bkt", "replace-empty", "bkt", "src", CopyOptions{
			ReplaceMetadata: true,
		})
		if err != nil {
			t.Fatalf("CopyObject 失败: %v", err)
		}
		if info.ContentType != defaultContentType {
			t.Errorf("ContentType = %q，期望默认值 %q", info.ContentType, defaultContentType)
		}
		if len(info.UserMeta) != 0 {
			t.Errorf("UserMeta = %v，REPLACE 未提供时应为空", info.UserMeta)
		}
	})
}

// TestCopyObjectSelfCopy 验证"复制到自身"的两条分支。
func TestCopyObjectSelfCopy(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	const body = "self copy payload"
	putString(t, s, "bkt", "self", body)

	if _, err := s.CopyObject("bkt", "self", "bkt", "self", CopyOptions{}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("自复制 + COPY 错误 = %v，期望 ErrInvalidRequest", err)
	}

	info, err := s.CopyObject("bkt", "self", "bkt", "self", CopyOptions{
		ReplaceMetadata: true,
		ContentType:     "application/json",
	})
	if err != nil {
		t.Fatalf("自复制 + REPLACE 失败: %v", err)
	}
	if info.ContentType != "application/json" {
		t.Errorf("ContentType = %q，期望 application/json", info.ContentType)
	}
	// 内容必须原样保留：REPLACE 只改元数据。
	if got := readObject(t, s, "bkt", "self"); string(got) != body {
		t.Errorf("自复制后内容 = %q，期望 %q", got, body)
	}
}

// TestCopyObjectVersionAndCondition 覆盖目标版本递增与 If-None-Match。
func TestCopyObjectVersionAndCondition(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	putString(t, s, "bkt", "src", "payload")

	first, err := s.CopyObject("bkt", "dst", "bkt", "src", CopyOptions{})
	if err != nil {
		t.Fatalf("首次复制失败: %v", err)
	}
	if first.Version != 1 {
		t.Errorf("首次复制 Version = %d，期望 1", first.Version)
	}

	second, err := s.CopyObject("bkt", "dst", "bkt", "src", CopyOptions{})
	if err != nil {
		t.Fatalf("覆盖复制失败: %v", err)
	}
	if second.Version != 2 {
		t.Errorf("覆盖复制 Version = %d，期望 2", second.Version)
	}

	// 目标已存在且不是墓碑 → 条件失败。
	if _, err := s.CopyObject("bkt", "dst", "bkt", "src", CopyOptions{
		IfNoneMatchAny: true,
	}); !errors.Is(err, ErrPreconditionFailed) {
		t.Errorf("目标存在时 IfNoneMatchAny 错误 = %v，期望 ErrPreconditionFailed", err)
	}

	// 单机 DeleteObject 会物理删掉 meta.json（墓碑是集群层的概念），
	// 因此删除之后同名复制等价于"从未存在"：条件写允许，版本从 1 重新开始。
	if err := s.DeleteObject("bkt", "dst"); err != nil {
		t.Fatalf("删除目标失败: %v", err)
	}
	info, err := s.CopyObject("bkt", "dst", "bkt", "src", CopyOptions{
		IfNoneMatchAny: true,
	})
	if err != nil {
		t.Fatalf("删除后的条件复制失败: %v", err)
	}
	if info.Version != 1 {
		t.Errorf("删除后 Version = %d，期望 1（meta 已随删除消失）", info.Version)
	}

	// 集群删除留下的是墓碑（Deleted=true 的 meta），条件写必须把它
	// 当作"不存在"，同时版本继续递增 —— 否则旧副本复活时版本号会倒退。
	writeRawMeta(t, s, "bkt", "tombstoned", objectMeta{
		Deleted:    true,
		Modified:   s.now(),
		ObjVersion: 7,
	})
	info, err = s.CopyObject("bkt", "tombstoned", "bkt", "src", CopyOptions{
		IfNoneMatchAny: true,
	})
	if err != nil {
		t.Fatalf("墓碑之上的条件复制失败: %v", err)
	}
	if info.Version != 8 {
		t.Errorf("墓碑之上 Version = %d，期望 8（墓碑版本 7 + 1）", info.Version)
	}
}

// TestCopyObjectErrors 覆盖源侧的各种错误路径。
func TestCopyObjectErrors(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	putString(t, s, "bkt", "alive", "payload")
	writeRawMeta(t, s, "bkt", "tombstone", objectMeta{
		Deleted:    true,
		Modified:   s.now(),
		ObjVersion: 3,
	})

	cases := []struct {
		name string
		err  error
		want error
	}{
		{"源 key 不存在", copyErr(s, "bkt", "dst", "bkt", "missing"), ErrNoSuchKey},
		{"源是墓碑", copyErr(s, "bkt", "dst", "bkt", "tombstone"), ErrNoSuchKey},
		{"源桶不存在", copyErr(s, "bkt", "dst", "nope", "alive"), ErrBucketNotFound},
		{"目标桶不存在", copyErr(s, "nope", "dst", "bkt", "alive"), ErrBucketNotFound},
		{"目标 key 非法", copyErr(s, "bkt", "", "bkt", "alive"), ErrInvalidKey},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if !errors.Is(c.err, c.want) {
				t.Errorf("错误 = %v，期望 %v", c.err, c.want)
			}
		})
	}
}

// copyErr 执行一次复制并只返回错误，让上面的表驱动保持紧凑。
func copyErr(s *Store, dstBucket, dstKey, srcBucket, srcKey string) error {
	_, err := s.CopyObject(dstBucket, dstKey, srcBucket, srcKey, CopyOptions{})
	return err
}

// writeRawMeta 直接落一份对象元数据，用于构造正常写入路径造不出的状态
// （EC 布局、超过 5 GiB 的引用）。
func writeRawMeta(t *testing.T, s *Store, bucket, key string, meta objectMeta) {
	t.Helper()
	meta.Version = formatVersion
	meta.Key = key
	if meta.ContentType == "" {
		meta.ContentType = defaultContentType
	}
	if err := s.writeFileAtomic(s.objectMetaPath(bucket, key), mustJSON(meta)); err != nil {
		t.Fatalf("写对象元数据失败: %v", err)
	}
}

// TestCopyObjectRejectsECSource 验证单机路径拒绝复制 EC 布局对象。
//
// EC 分片分散在集群节点上，复制出来的 meta 会指向本节点不存在的块：
// 与其产出"复制成功、一读就坏"的对象，不如明确回 501。
func TestCopyObjectRejectsECSource(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	writeRawMeta(t, s, "bkt", "ec-src", objectMeta{
		Size:       16,
		ETag:       "d41d8cd98f00b204e9800998ecf8427e",
		Modified:   s.now(),
		Layout:     LayoutEC,
		EC:         &ECLayout{K: 2, M: 1, Group: []string{"node-1", "node-2", "node-3"}},
		ObjVersion: 1,
	})

	if _, err := s.CopyObject("bkt", "dst", "bkt", "ec-src", CopyOptions{}); !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("复制 EC 源错误 = %v，期望 ErrNotImplemented", err)
	}
}

// TestCopyObjectRejectsOversizeSource 验证 5 GiB 上限。
//
// 上限的来源是 meta.json 的 32 MiB 读取上限：5 GiB / 4 MiB 块约 1300 条
// 块描述，仍在安全范围内；再大就必须走分片上传。
func TestCopyObjectRejectsOversizeSource(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	writeRawMeta(t, s, "bkt", "huge", objectMeta{
		Size:       MaxCopyObjectSize + 1,
		ETag:       "d41d8cd98f00b204e9800998ecf8427e",
		Modified:   s.now(),
		ObjVersion: 1,
	})

	if _, err := s.CopyObject("bkt", "dst", "bkt", "huge", CopyOptions{}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("复制超大源错误 = %v，期望 ErrInvalidRequest", err)
	}
}

// TestCopyObjectIsSnapshot 验证复制之后覆盖源对象不影响目标。
//
// 块是不可变的，覆盖写只会换掉 meta.json 的指向；复制拿到的是源 meta
// 的一次快照，因此两者后续各自独立演化。
func TestCopyObjectIsSnapshot(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	putString(t, s, "bkt", "src", "first version")
	if _, err := s.CopyObject("bkt", "dst", "bkt", "src", CopyOptions{}); err != nil {
		t.Fatalf("CopyObject 失败: %v", err)
	}

	putString(t, s, "bkt", "src", "second version")

	if got := readObject(t, s, "bkt", "dst"); string(got) != "first version" {
		t.Errorf("覆盖源后目标内容 = %q，期望仍为 first version", got)
	}
	if got := readObject(t, s, "bkt", "src"); string(got) != "second version" {
		t.Errorf("源内容 = %q，期望 second version", got)
	}
}

// TestCopyObjectUpdatesMetaCache 验证复制后缓存立即反映新对象。
//
// 缓存不进则退：写盘成功但缓存仍是旧的，随后一次 HEAD 就会报出
// 与磁盘不一致的元数据，而这类 bug 在测试里只有主动比对才抓得到。
func TestCopyObjectUpdatesMetaCache(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	putString(t, s, "bkt", "src", "payload")

	if _, err := s.CopyObject("bkt", "dst", "bkt", "src", CopyOptions{
		ReplaceMetadata: true,
		ContentType:     "application/x-model",
	}); err != nil {
		t.Fatalf("CopyObject 失败: %v", err)
	}

	// 先读一次让缓存命中，再确认读到的是复制写入的那份元数据。
	r, err := s.OpenObject("bkt", "dst")
	if err != nil {
		t.Fatalf("OpenObject 失败: %v", err)
	}
	if got := r.Info().ContentType; got != "application/x-model" {
		t.Errorf("缓存中的 ContentType = %q，期望 application/x-model", got)
	}
}
