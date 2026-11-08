package store

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/binbinpeng123/Blink/internal/checksum"
)

// Block 是 StreamBlocks 切出的一个数据块。
type Block struct {
	Desc BlockDesc
	// Data 是块内容。底层缓冲在回调之间复用，因此 Data **只在本次回调
	// 期间有效**；需要保留必须自行拷贝。
	Data []byte
}

// StreamSummary 是一次流式切块的汇总。
type StreamSummary struct {
	// Blocks 按写入顺序排列，Offset 已累加为对象内偏移。
	Blocks []BlockDesc
	Size   int64
	// ETag 是内容 MD5 的小写十六进制（普通对象 ETag 与分片 ETag 同义）。
	ETag string
	// Checksum 是 alg 对应的校验和（base64）；alg 为空时为空串。
	Checksum string
}

// StreamBlocks 从 body 读完全部内容，切块并逐块交给 fn，同时算出 ETag
// 与对象级校验和。
//
// 它只做"读、切、算"，不写盘：单机路径在回调里把块写入本地 CAS，
// 集群路径在回调里同时写本地与远端副本。两条路径共用同一个读循环，
// 块边界、MD5 与校验和的语义因此不可能分叉。
//
// fn 返回的错误会立即终止读取并原样返回；调用方据此中止整次写入。
// 长度校验由调用方负责：本函数不知道客户端声明了多少字节。
//
// tag 只用于错误信息（对象 key 或分片标识），不参与任何判断。
func (s *Store) StreamBlocks(body io.Reader, alg checksum.Algorithm, tag string, fn func(Block) error) (*StreamSummary, error) {
	return s.StreamBlocksSized(body, alg, tag, -1, fn)
}

// StreamBlocksSized 是 StreamBlocks 的"已知声明长度"版本。
//
// expected < 0 表示长度未知，缓冲按块大小分配；否则缓冲按
// min(expected, blockSize) 分配。对 1 MiB 对象，这让每次写入的临时缓冲
// 从 4 MiB 降到 1 MiB —— 边端设备上并发上传数十个对象时，这部分内存
// 是实打实的 RSS 差异。
//
// 长度提示只影响缓冲大小，不改变块边界：只要 actual ≤ expected，
// 以 expected 为上限切出的块与按 blockSize 切出的块完全一致（末尾一个
// 不满块）。客户端声明的长度与实际不符时块边界可能不同，但那一次写入
// 必然以 ErrIncompleteBody 失败，块表不会落盘（残块由 GC 回收）。
func (s *Store) StreamBlocksSized(body io.Reader, alg checksum.Algorithm, tag string, expected int64, fn func(Block) error) (*StreamSummary, error) {
	var objSum checksum.Sum
	if alg != "" {
		sum, ok := checksum.New(alg)
		if !ok {
			return nil, fmt.Errorf("%w: 未知的校验和算法 %q", ErrInvalidChecksum, alg)
		}
		objSum = sum
	}

	res := &StreamSummary{Blocks: make([]BlockDesc, 0, 4)}
	md5h := md5.New()

	blockSize := int(s.blockSize)
	inline := int(s.inlineThreshold)

	// 本次流的块长上限：已知声明长度时不超过声明值，未知时就是块大小。
	// expected == 0 时保持 blockSize —— 缓冲长度为 0 会让 readBlock
	// 永远返回 (0, nil)，死循环。
	limit := blockSize
	if expected > 0 && expected < int64(blockSize) {
		limit = int(expected)
	}

	// 小对象快路径：缓冲只按内联阈值分配，而不是无条件分配一个块大小。
	//
	// 边端常见的是几 KiB 的配置/清单文件；旧实现虽然只读阈值字节，
	// 却先 make 了 4 MiB 缓冲，一次 1 KiB PUT 的内存分配超过 4 MiB。
	// 这里先分配 min(阈值, limit)，只有确认流超过阈值时才增长到 limit：
	// 1 MiB 对象因此分配 1 MiB 而不是 4 MiB，块边界不变。
	bufLen := limit
	if inline > 0 && inline < limit {
		bufLen = inline
	}
	buf := make([]byte, bufLen)
	prefill := 0
	if inline > 0 {
		readLen := inline
		if readLen > bufLen {
			readLen = bufLen
		}
		n, err := io.ReadFull(body, buf[:readLen])
		switch {
		case err == nil, errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
			prefill = n
		default:
			return nil, fsErr("读取请求体", tag, err)
		}
		// 阈值恰好读满时，流可能刚好结束，也可能还有数据。
		// 先 peek 一个字节：有数据就立刻增长到本次流的块长上限，把这一
		// 字节放进新缓冲再继续读。必须在这里完成增长，不能等到循环里 ——
		// 否则 >阈值的对象会被切成"阈值 + 块大小"的块，改变块边界，
		// 也破坏 LIST/修复对既有块表的假设。
		if err == nil && bufLen < limit {
			var one [1]byte
			m, perr := io.ReadFull(body, one[:])
			switch {
			case m == 1:
				grown := make([]byte, limit)
				copy(grown, buf[:prefill])
				grown[prefill] = one[0]
				buf = grown
				prefill++
			case errors.Is(perr, io.EOF):
				// 恰好 threshold 字节：保持小缓冲，下一轮在 readBlock
				// 里以 (0, EOF) 结束，只产出一个块。
			case perr != nil:
				return nil, fsErr("读取请求体", tag, perr)
			}
		}
	}

	for {
		n, err := readBlock(body, buf, prefill)
		prefill = 0
		if n > 0 {
			chunk := buf[:n]
			// md5/sha 与校验和计算器都不返回错误，但接口要求处理它。
			_, _ = md5h.Write(chunk)
			if objSum != nil {
				_, _ = objSum.Write(chunk)
			}

			sum := sha256.Sum256(chunk)
			desc := BlockDesc{
				Hash:   hex.EncodeToString(sum[:]),
				Offset: res.Size,
				Length: int64(n),
				CRC32C: checksum.BlockCRC32C(chunk),
			}
			if err := fn(Block{Desc: desc, Data: chunk}); err != nil {
				return nil, err
			}
			res.Blocks = append(res.Blocks, desc)
			res.Size += int64(n)
		}
		if err != nil {
			// EOF 与 ErrUnexpectedEOF 都表示"正常读完，只是最后一块不满"；
			// 其余错误意味着链路出了问题，必须让整次写入失败。
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, fsErr("读取请求体", tag, err)
		}
	}

	res.ETag = hex.EncodeToString(md5h.Sum(nil))
	if objSum != nil {
		res.Checksum = objSum.Encoded()
	}
	return res, nil
}

// readBlock 把一个块读满到 buf，返回块长度。
//
// prefill 表示 buf 前若干字节已在小对象快路径里读入。返回语义：
//
//	err == nil                 读满 buf，可能还有更多数据
//	err == io.EOF              流已结束；n 为最后一块长度（空对象时为 0）
//	err == io.ErrUnexpectedEOF 流在中途结束；n 为最后一块长度
//
// 关键细节：prefill > 0 且流恰好在此结束时，io.ReadFull 返回
// (0, io.EOF)，此时 buf[:prefill] 本身就是最后一个完整块 ——
// 直接丢弃会让每个"长度恰好是阈值整数倍"的对象少一段内容。
func readBlock(r io.Reader, buf []byte, prefill int) (int, error) {
	if prefill == 0 {
		return io.ReadFull(r, buf)
	}
	n, err := io.ReadFull(r, buf[prefill:])
	return prefill + n, err
}
