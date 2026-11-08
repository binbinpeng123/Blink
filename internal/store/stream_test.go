package store

import (
	"bytes"
	"fmt"
	"testing"
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
