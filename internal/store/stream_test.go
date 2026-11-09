package store

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"runtime"
	"testing"

	"github.com/binbinpeng123/Blink/internal/checksum"
)

// TestStreamBlocksSizedKeepsBlockBoundaries 断言长度提示不改变切块结果。
//
// streamBlocksSized 按 min(expected, blockSize) 分配缓冲，这会把"缓冲大小"
// 与"块边界"绑在一起：一旦提示值与实际长度不一致时切出的块不同，
// 同一份内容就会因为走哪条调用路径（单机 PUT / 集群 PUT / 分片上传）
// 而得到不同的块表，去重与修复的假设随之失效。
//
// 覆盖尺寸刻意选在阈值与块边界的整数倍附近：恰好等于、少一字节、多一字节。
func TestStreamBlocksSizedKeepsBlockBoundaries(t *testing.T) {
	const (
		blockSize = 16
		inline    = 8
	)
	s := newSmallBlockStore(t, blockSize, inline)

	for _, size := range []int{0, 1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 40} {
		size := size
		t.Run(fmt.Sprintf("%dB", size), func(t *testing.T) {
			body := patternBytes(size, 7)

			want, err := s.StreamBlocks(bytes.NewReader(body), "", "hint-test", func(Block) error { return nil })
			if err != nil {
				t.Fatalf("StreamBlocks(无提示) 失败: %v", err)
			}
			got, err := s.StreamBlocksSized(bytes.NewReader(body), "", "hint-test", int64(size), func(Block) error { return nil })
			if err != nil {
				t.Fatalf("StreamBlocksSized 失败: %v", err)
			}

			if got.Size != want.Size || got.Size != int64(size) {
				t.Fatalf("Size = %d，期望 %d（无提示版本 %d）", got.Size, size, want.Size)
			}
			if got.ETag != want.ETag {
				t.Fatalf("ETag = %s，期望 %s", got.ETag, want.ETag)
			}
			if len(got.Blocks) != len(want.Blocks) {
				t.Fatalf("块数 = %d，期望 %d", len(got.Blocks), len(want.Blocks))
			}
			for i := range want.Blocks {
				w, g := want.Blocks[i], got.Blocks[i]
				// BlockDesc 含切片字段，不能整体比较；块表里参与去重与读取的
				// 就是这四个字段。
				if g.Hash != w.Hash || g.Offset != w.Offset || g.Length != w.Length || g.CRC32C != w.CRC32C {
					t.Fatalf("第 %d 块 = %+v，期望 %+v", i, g, w)
				}
			}
		})
	}
}

// TestStreamBlocksParallelHashes 断言并行路径（块长 ≥ parallelHashMinBlock）
// 产出的每块 SHA-256/CRC32C 与 ETag，与对原始内容独立重算的结果一致。
//
// 并行版本让 goroutine 与主 goroutine 同时读同一块缓冲：等待时机错了、
// 或在回调返回前就复用了缓冲，症状都是偶发的哈希错位/ETag 漂移。
// 因此这里逐块重算全部字段，而不是只比较 Size 或块数。
func TestStreamBlocksParallelHashes(t *testing.T) {
	if runtime.GOMAXPROCS(0) < 2 {
		t.Skip("GOMAXPROCS=1 时不会走并行路径")
	}
	const blockSize = int64(parallelHashMinBlock)
	s := newSmallBlockStore(t, blockSize, 64<<10)

	// 3 个满块（走并行）+ 1 个不满块（走串行），两条路径在同一对象里都覆盖。
	body := patternBytes(int(blockSize*3+777), 13)
	got, err := s.StreamBlocks(bytes.NewReader(body), "", "parallel-hash", func(Block) error { return nil })
	if err != nil {
		t.Fatalf("StreamBlocks 失败: %v", err)
	}
	if len(got.Blocks) != 4 {
		t.Fatalf("块数 = %d，期望 4", len(got.Blocks))
	}

	parallel := 0
	for i, b := range got.Blocks {
		if b.Length >= int64(parallelHashMinBlock) {
			parallel++
		}
		seg := body[b.Offset : b.Offset+b.Length]
		sum := sha256.Sum256(seg)
		if want := hex.EncodeToString(sum[:]); b.Hash != want {
			t.Fatalf("第 %d 块 Hash = %s，期望 %s", i, b.Hash, want)
		}
		if want := checksum.BlockCRC32C(seg); b.CRC32C != want {
			t.Fatalf("第 %d 块 CRC32C = %#x，期望 %#x", i, b.CRC32C, want)
		}
	}
	if parallel != 3 {
		t.Fatalf("走并行路径的块数 = %d，期望 3（测试没有覆盖并行路径）", parallel)
	}

	sum := md5.Sum(body)
	if want := hex.EncodeToString(sum[:]); got.ETag != want {
		t.Fatalf("ETag = %s，期望 %s", got.ETag, want)
	}
}

// TestStreamBlocksSizedIgnoresWrongHint 断言提示值与实际长度不符时
// 仍然完整读出请求体：长度校验是调用方的事（见 StreamBlocks 注释），
// 提示只影响缓冲大小。否则一次错误的 Content-Length 会让服务端在
// 读到一半时提前"成功"。
func TestStreamBlocksSizedIgnoresWrongHint(t *testing.T) {
	const blockSize = 16
	s := newSmallBlockStore(t, blockSize, 8)

	body := patternBytes(40, 3) // 实际 40 字节，跨 3 块
	for _, hint := range []int64{1, 16, 64} {
		hint := hint
		t.Run(fmt.Sprintf("hint=%d", hint), func(t *testing.T) {
			got, err := s.StreamBlocksSized(bytes.NewReader(body), "", "hint-test", hint, func(Block) error { return nil })
			if err != nil {
				t.Fatalf("StreamBlocksSized(hint=%d) 失败: %v", hint, err)
			}
			if got.Size != 40 {
				t.Fatalf("Size = %d，期望 40", got.Size)
			}
			var total int64
			for _, b := range got.Blocks {
				total += b.Length
			}
			if total != 40 {
				t.Fatalf("块长度合计 = %d，期望 40", total)
			}
		})
	}
}
