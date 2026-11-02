// Package checksum 实现 S3 校验和算法。
//
// S3 允许五种：CRC32、CRC32C、CRC64NVME、SHA1、SHA256，统一入口见 alg.go。
// 选择它们的理由不是"更安全"，而是**校验和的用途是发现边端廉价闪存的
// 位翻转**，因此必须便宜到"每个块都算"也不心疼。
//
// 各算法达成"便宜"的手段不同，别混为一谈：
//
//	CRC32C   —— 有硬件指令（SSE4.2 的 crc32、ARMv8 的 crc32c），
//	            标准库直接用，这是它真正的优势所在；块级校验和就用它。
//	CRC64NVME —— **没有**硬件指令，本实现靠 slicing-by-8 查表。
//	SHA 系   —— 速度远低于 CRC，但对恶意篡改有抗性；只在客户端要求时算。
//
// CRC64NVME 是 AWS 在 2025 年起的默认校验和（`aws s3 cp` 已默认发送），
// 因此不是可选项 —— 不支持它，aws-cli 的默认路径就报错。
package checksum

import "encoding/binary"

// CRC64NVME 参数。
//
// 这三个值**不是凭记忆写下的**，而是用两个外部预言机反解出来的：
//
//	reveng 目录的 check 值：CRC64NVME("123456789") == 0xAE8B14860A799888
//	真实 aws-cli 2.36.2 对 "hello blink\n" 实际发送的
//	x-amz-checksum-crc64nvme: 0Y2Qe5BGWvI=（大端解码 = 0xD18D907B90465AF2）
//
// 最初按记忆写成 0xad93d23594c935a9，被这两个值同时否证 ——
// 正确值是 0xad93d23594c93659（末四位 3659，不是 35a9）。
// 保留这段记录是因为：CRC 参数错一位，得到的是一组"看起来正常"的
// 错误校验和，除了与外部对比之外没有任何办法发现。
//
// poly 用反射（LSB-first）形式，因此计算时右移、查表用低位。
const (
	crc64NormalPoly  = 0xad93d23594c93659 // 正常形式
	crc64ReflectPoly = 0x9a6c9329ac4bc9b5 // 反射形式，查表用
	crc64Init        = ^uint64(0)
	crc64XorOut      = ^uint64(0)
)

// crc64Tables 是 slicing-by-8 的 8 张表，在包初始化时建一次并复用。
//
// 为什么不用标准库的 hash/crc64？——**先纠正一个流传的说法**：
// 标准库并非只有逐字节实现，它**已经有 slicing-by-8**，只是内置预设
// 仅 ISO 与 ECMA 两个。所以"我们手写表因此快几倍"是错的。
//
// 真正的差别在表的生命周期：标准库对非内置多项式，在每次调用
// （len(p) >= 2048 时）都现建一张 16 KiB 表，建完即弃。而 CRC64NVME
// 算的是**对象级**校验和，输入来自 HTTP body 的读取，被拆成几十 KiB
// 一块反复喂入 —— 于是"每块重建一张 16 KiB 表"正好落在热路径上。
//
// 实测（Apple M4，4 MiB 总量，分块喂入，5 轮中位）：
//
//	分块      缓存表       标准库       差异
//	8 KiB    2222 MB/s   1788 MB/s   +24%
//	32 KiB   2226 MB/s   2101 MB/s    +6%
//	64 KiB   2214 MB/s   2165 MB/s    +2%
//	4 MiB    2163 MB/s   2163 MB/s    持平
//
// 结论是"赢多少取决于喂入粒度，且在小分块下才显著"，不是"更快"这个
// 笼统说法。4 MiB 持平很重要：本实现的代价至多为零，不会倒亏。
// 这些数字只作本机回归基线，不构成性能承诺（见 05 文档）。
//
// 另需注意：表在栈/静态区，不产生堆分配（基准 B/op 为 0），
// 因此用 testing.AllocsPerRun 是**看不到**这笔开销的，只能靠吞吐看出来。
//
// 表索引写错不会崩，只会静默算错 —— 所以"与标准库逐位相等"的测试
// 是这条优化唯一的护栏（见 crc64nvme_test.go 的 TestMatchesStdlibOracle）。
var crc64Tables = makeCRC64Tables()

func makeCRC64Tables() *[8][256]uint64 {
	var t [8][256]uint64
	for i := range t[0] {
		c := uint64(i)
		for j := 0; j < 8; j++ {
			if c&1 == 1 {
				c = (c >> 1) ^ crc64ReflectPoly
			} else {
				c >>= 1
			}
		}
		t[0][i] = c
	}
	// 后续 7 张表由前一张递推：每前进一字节，就是"先查表再右移 8 位"。
	for k := 1; k < 8; k++ {
		for i := range t[k] {
			t[k][i] = (t[k-1][i] >> 8) ^ t[0][t[k-1][i]&0xff]
		}
	}
	return &t
}

// CRC64NVME 计算 data 的 CRC-64/NVME 校验和。
func CRC64NVME(data []byte) uint64 {
	return UpdateCRC64NVME(crc64Init, data) ^ crc64XorOut
}

// UpdateCRC64NVME 在 crc 基础上继续计算 data。
//
// crc 是**未做 xorout 的中间值**，这样调用方可以分块喂入。
// 直接用 NewCRC64NVME 的 Write 更不容易出错。
func UpdateCRC64NVME(crc uint64, data []byte) uint64 {
	t := crc64Tables
	// 8 字节一步的主循环。
	for len(data) >= 8 {
		crc ^= binary.LittleEndian.Uint64(data)
		crc = t[7][byte(crc)] ^
			t[6][byte(crc>>8)] ^
			t[5][byte(crc>>16)] ^
			t[4][byte(crc>>24)] ^
			t[3][byte(crc>>32)] ^
			t[2][byte(crc>>40)] ^
			t[1][byte(crc>>48)] ^
			t[0][byte(crc>>56)]
		data = data[8:]
	}
	for _, b := range data {
		crc = t[0][byte(crc)^b] ^ (crc >> 8)
	}
	return crc
}

// CRC64NVMEHasher 实现 hash.Hash64，便于边读边算（避免把对象缓冲进内存）。
type CRC64NVMEHasher struct {
	crc uint64
}

// NewCRC64NVMEHasher 返回一个可增量写入的 CRC64NVME 计算器。
func NewCRC64NVMEHasher() *CRC64NVMEHasher {
	return &CRC64NVMEHasher{crc: crc64Init}
}

// Write 实现 io.Writer，永不返回错误。
func (h *CRC64NVMEHasher) Write(p []byte) (int, error) {
	h.crc = UpdateCRC64NVME(h.crc, p)
	return len(p), nil
}

// Sum64 返回校验和。
func (h *CRC64NVMEHasher) Sum64() uint64 { return h.crc ^ crc64XorOut }

// Sum 把校验和按**大端**追加到 b 并返回。
//
// 大端不是随意选择：S3 的 x-amz-checksum-crc64nvme 头是该值的
// base64 编码，而 aws-cli 实际发送的 0Y2Qe5BGWvI= 解码后正是大端序。
// 反向写会让校验和"本身正确但编码不同"，客户端仍然报 BadDigest。
func (h *CRC64NVMEHasher) Sum(b []byte) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], h.Sum64())
	return append(b, buf[:]...)
}

// Reset 复位到初始状态。
func (h *CRC64NVMEHasher) Reset() { h.crc = crc64Init }

// Size 返回校验和字节数。
func (h *CRC64NVMEHasher) Size() int { return 8 }

// BlockSize 返回建议的写入块大小。
func (h *CRC64NVMEHasher) BlockSize() int { return 8 }
