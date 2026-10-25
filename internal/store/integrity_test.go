package store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// patternBytes 生成 n 字节的测试数据，保证**不同偏移的块内容不同**。
//
// 不能用 mpuBytes：它的内容只取决于下标对 256 取模，周期正好是 256，
// 而 64 KiB 是 256 的整数倍 —— 同一对象的相邻块会逐字节相同，被 CAS
// 去重成同一个文件。那样"只损坏第二块"实际等于损坏了全部块，
// 完整性测试也就分不清"检测到损坏"和"到处都是损坏"。
func patternBytes(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		// i>>16 而不是 i>>8：byte(i>>8) 的周期是 65536，正好等于默认块大小，
		// 相邻块依旧会重复 —— 这个坑踩过一次。
		b[i] = byte(i*31) ^ byte(i>>8) ^ byte(i>>16) ^ seed
	}
	return b
}

// flipByte 就地翻转文件中的一个字节，模拟闪存位翻转。
//
// 直接改 CAS 目录下的文件，而不是走 Store API：真实损坏就发生在
// Store 不知情的时候，测试必须绕过写入路径制造它。
func flipByte(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("打开 %s 失败: %v", path, err)
	}
	defer func() { _ = f.Close() }()

	var one [1]byte
	if _, err := f.ReadAt(one[:], off); err != nil {
		t.Fatalf("读取 %s 偏移 %d 失败: %v", path, off, err)
	}
	one[0] ^= 0xFF
	if _, err := f.WriteAt(one[:], off); err != nil {
		t.Fatalf("写入 %s 偏移 %d 失败: %v", path, off, err)
	}
}

// readRange 读取对象的 [off, off+n)，把错误交回调用方。
func readRangeNoFatal(s *Store, key string, off, n int64) ([]byte, error) {
	r, err := s.OpenObject("bkt", key)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := r.WriteRangeTo(&buf, off, n); err != nil {
		return buf.Bytes(), err
	}
	return buf.Bytes(), nil
}

// TestScrubDetectsCorruptBlock 验证后台 scrub 能定位到具体块。
func TestScrubDetectsCorruptBlock(t *testing.T) {
	s := openConcurrencyStore(t)

	data := patternBytes(200<<10, 0x77) // 64K + 64K + 64K + 8K
	mustPutBytes(t, s, "victim", data)
	mustPutBytes(t, s, "clean", patternBytes(1<<10, 0x88))

	if err := s.ScrubObject("bkt", "victim"); err != nil {
		t.Fatalf("健康对象 scrub 失败: %v", err)
	}

	m, err := s.loadObjectMeta("bkt", "victim")
	if err != nil {
		t.Fatalf("读取元数据失败: %v", err)
	}
	if len(m.Blocks) != 4 {
		t.Fatalf("块数 = %d，期望 4", len(m.Blocks))
	}
	bad := m.Blocks[1]
	flipByte(t, s.casPath(bad.Hash), 0)

	err = s.ScrubObject("bkt", "victim")
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("scrub 未报 ErrCorrupt，实际: %v", err)
	}
	// 错误信息必须带块哈希：运维要据此定位到具体文件。
	if !strings.Contains(err.Error(), bad.Hash) {
		t.Errorf("错误信息未包含块哈希 %s: %v", bad.Hash, err)
	}
	// 损坏是局部的，其它对象不受影响 —— 否则 scrubbed 报告无法用来
	// 判断"这一个对象坏了"还是"整块盘坏了"。
	if err := s.ScrubObject("bkt", "clean"); err != nil {
		t.Errorf("未损坏的对象 scrub 报错: %v", err)
	}

	// 块文件整个消失也要被当作损坏，而不是"对象不存在"。
	// 用另一个对象做这件事：victim 已经有坏块，scrub 会在第一处错误就返回，
	// 断言不到"缺失"这条分支。
	mustPutBytes(t, s, "gone", patternBytes(200<<10, 0x66))
	gm, err := s.loadObjectMeta("bkt", "gone")
	if err != nil {
		t.Fatalf("读取元数据失败: %v", err)
	}
	missing := gm.Blocks[1]
	if err := os.Remove(s.casPath(missing.Hash)); err != nil {
		t.Fatalf("删除块文件失败: %v", err)
	}
	err = s.ScrubObject("bkt", "gone")
	if !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), missing.Hash) {
		t.Errorf("块缺失时 scrub 应报 ErrCorrupt 并指出 %s，实际: %v", missing.Hash, err)
	}
}

// TestVerifyOnReadDetectsCorruption 验证 --verify-read 的行为边界。
//
// 默认关闭时读取**不会**发现块内位翻转：这是零拷贝读的代价，写在
// 03 文档与 known-limitations 里。打开后校验发生在写出任何字节之前，
// 因此首块损坏时客户端一个字节都收不到。
func TestVerifyOnReadDetectsCorruption(t *testing.T) {
	s := openConcurrencyStore(t)

	data := patternBytes(200<<10, 0x99)
	mustPutBytes(t, s, "victim", data)
	mustPutBytes(t, s, "single", patternBytes(100, 0xAA)) // 单块对象

	m, err := s.loadObjectMeta("bkt", "victim")
	if err != nil {
		t.Fatalf("读取元数据失败: %v", err)
	}
	bad := m.Blocks[1]
	flipByte(t, s.casPath(bad.Hash), 0)

	single, err := s.loadObjectMeta("bkt", "single")
	if err != nil || len(single.Blocks) != 1 {
		t.Fatalf("单块对象元数据异常: blocks=%d err=%v", len(single.Blocks), err)
	}
	flipByte(t, s.casPath(single.Blocks[0].Hash), 0)

	// 默认（不校验）：读不到任何错误。这是在文档里明确接受的取舍。
	got, err := readRangeNoFatal(s, "victim", 0, int64(len(data)))
	if err != nil {
		t.Fatalf("默认读取不应报错: %v", err)
	}
	if bytes.Equal(got, data) {
		t.Fatal("默认读取拿到的内容与原始数据相同，损坏没有被制造出来")
	}

	vs, err := Open(Options{
		Root:            s.Root(),
		BlockSize:       64 << 10,
		InlineThreshold: 16,
		VerifyOnRead:    true,
	})
	if err != nil {
		t.Fatalf("以校验读模式重新打开失败: %v", err)
	}

	// 全量读：损坏块之前的块已经写出（流式读无法预判后面的块），
	// 但必须出错而不是静默返回坏数据。
	got, err = readRangeNoFatal(vs, "victim", 0, int64(len(data)))
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("校验读全量读取未报 ErrCorrupt，实际: %v", err)
	}
	if int64(len(got)) != bad.Offset {
		t.Errorf("损坏块之前应恰好写出 %d 字节，实际 %d", bad.Offset, len(got))
	}
	if !strings.Contains(err.Error(), bad.Hash) {
		t.Errorf("错误信息未包含块哈希 %s: %v", bad.Hash, err)
	}

	// 只读第一块：不触碰损坏块，必须成功且内容逐字节正确。
	got, err = readRangeNoFatal(vs, "victim", 0, 1000)
	if err != nil {
		t.Fatalf("读取健康块失败: %v", err)
	}
	if !bytes.Equal(got, data[:1000]) {
		t.Error("读取健康块的内容不符")
	}

	// 首块损坏：校验发生在写之前，客户端一个字节都收不到。
	got, err = readRangeNoFatal(vs, "single", 0, 100)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("首块损坏未报 ErrCorrupt，实际: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("首块损坏时不应写出任何字节，实际写出 %d 字节", len(got))
	}
}

// TestScrubHealthyStore 是一个防误报的基线：没坏的文件不能被判成坏了。
// 没有它，上面那些"检测到损坏"的断言可能只是因为 scrub 永远报错。
func TestScrubHealthyStore(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")
	s, err := Open(Options{Root: root, BlockSize: 64 << 10, InlineThreshold: 16})
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	if err := s.CreateBucket("bkt"); err != nil {
		t.Fatalf("CreateBucket 失败: %v", err)
	}
	keys := []string{"a", "b", "c"}
	for i, k := range keys {
		mustPutBytes(t, s, k, patternBytes(150<<10, byte(i+1)))
	}
	for _, k := range keys {
		if err := s.ScrubObject("bkt", k); err != nil {
			t.Errorf("健康对象 %s scrub 失败: %v", k, err)
		}
	}
}
