package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/binbinpeng123/Blink/internal/checksum"
)

// 内联小对象（创新 D0）的校验与辅助逻辑。
//
// 内联对象把内容放进 meta.json，读路径不再有 CAS 文件可 Stat，因此
// "元数据与数据自洽"必须在内存里重新立一遍：长度、块表、内容哈希。
// 这里集中实现，写入、读取、副本校验与完整性巡检共用同一套判断，
// 避免三处各写一遍后悄悄分叉。

// validateInlineMeta 校验内联元数据的**结构**，不重算内容哈希。
//
// 结构校验很便宜（O(1)），可以放在每次读取上；哈希校验是 O(n)，
// 只在 verifyOnRead 或 scrub 时做（见 verifyInlineContent）。
func validateInlineMeta(m *objectMeta) error {
	if m.Layout != LayoutInline {
		return fmt.Errorf("%w: 对象 %q 不是内联布局（layout=%q）",
			ErrCorrupt, m.Key, m.Layout)
	}
	if m.Size <= 0 {
		return fmt.Errorf("%w: 内联对象 %q 大小 %d 非正", ErrCorrupt, m.Key, m.Size)
	}
	if len(m.Blocks) != 1 {
		return fmt.Errorf("%w: 内联对象 %q 应恰好有一个块，实际 %d 个",
			ErrCorrupt, m.Key, len(m.Blocks))
	}
	b := m.Blocks[0]
	if b.Offset != 0 || b.Length != m.Size {
		return fmt.Errorf("%w: 内联对象 %q 的块区间 [%d,%d) 与大小 %d 不符",
			ErrCorrupt, m.Key, b.Offset, b.Offset+b.Length, m.Size)
	}
	if len(b.Shards) != 0 {
		return fmt.Errorf("%w: 内联对象 %q 不应携带 EC 分片表", ErrCorrupt, m.Key)
	}
	if int64(len(m.InlineData)) != m.Size {
		return fmt.Errorf("%w: 内联对象 %q 内容 %d 字节，元数据声明 %d",
			ErrCorrupt, m.Key, len(m.InlineData), m.Size)
	}
	return nil
}

// verifyInlineContent 在结构校验之外重算 SHA-256 与 CRC32C。
//
// 与 CAS 块不同，meta.json 不是内容寻址文件：盘上的位翻转不会改变
// 文件名，因此只有主动重算才能发现。verifyOnRead 与后台巡检都走这里。
func verifyInlineContent(m *objectMeta) error {
	if err := validateInlineMeta(m); err != nil {
		return err
	}
	b := m.Blocks[0]
	sum := sha256.Sum256(m.InlineData)
	if got := hex.EncodeToString(sum[:]); got != b.Hash {
		return fmt.Errorf("%w: 内联对象 %q 内容哈希 %s，块表记录 %s",
			ErrCorrupt, m.Key, got, b.Hash)
	}
	if got := checksum.BlockCRC32C(m.InlineData); got != b.CRC32C {
		return fmt.Errorf("%w: 内联对象 %q 内容 CRC32C %08x，块表记录 %08x",
			ErrCorrupt, m.Key, got, b.CRC32C)
	}
	return nil
}

// hashInline 对已读入内存的内容做与流式写路径完全相同的切块与校验和计算，
// 但不写盘。
//
// 复用 StreamBlocksSized 而不是另写一套 MD5/SHA-256/CRC32C：ETag 与
// 对象级校验和的定义必须与普通对象逐字节一致，否则同一个客户端在
// "内联"与"非内联"两种落盘方式之间迁移时会看到不同的 ETag。
func (s *Store) hashInline(data []byte, alg checksum.Algorithm, tag string) (*streamResult, error) {
	res := &streamResult{}
	summary, err := s.StreamBlocksSized(bytes.NewReader(data), alg, tag,
		int64(len(data)), func(Block) error { return nil })
	if err != nil {
		return nil, err
	}
	res.blocks = summary.Blocks
	res.size = summary.Size
	res.etag = summary.ETag
	res.checksum = summary.Checksum
	return res, nil
}

// scrubInline 校验一个内联对象的完整性。
func (s *Store) scrubInline(m *objectMeta) error {
	return verifyInlineContent(m)
}
