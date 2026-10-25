package checksum

import (
	"hash/crc64"
	"testing"
)

// 本文件回答一个问题：CRC64NVME 要不要自己维护切片表？
//
// 背景更正：标准库的 crc64 **已经实现 slicing-by-8**，只是内置预设
// 只有 ISO 与 ECMA。CRC64NVME 的多项式不在其中，需要自己 MakeTable。
//
// 这条更正的后果是：原本"标准库只有逐字节，我们手写表快 9 倍"的
// 立项理由**不成立**。所以现在必须用数据重新决定，而不是沿用旧理由。
//
// 两个候选实现必须在**同一个逻辑操作**上比：同样的分块、同样的总量。
// 分块大小取 8–64 KiB，因为 io.Copy 的默认缓冲是 32 KiB，
// 这是对象写入时 CRC 实际被喂入的粒度。

const benchTotal = 4 << 20 // 4 MiB = 本项目的块大小

func benchData() []byte {
	b := make([]byte, benchTotal)
	for i := range b {
		b[i] = byte(i*31 + i>>8)
	}
	return b
}

var benchChunks = []int{8 << 10, 32 << 10, 64 << 10, 4 << 20}

func benchStream(b *testing.B, chunk int, write func([]byte)) {
	data := benchData()
	b.SetBytes(benchTotal)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for off := 0; off < len(data); off += chunk {
			end := off + chunk
			if end > len(data) {
				end = len(data)
			}
			write(data[off:end])
		}
	}
}

// BenchmarkCachedTables 用本包维护的表（包初始化时建一次，进程内复用）。
func BenchmarkCachedTables(b *testing.B) {
	for _, chunk := range benchChunks {
		b.Run(chunkName(chunk), func(b *testing.B) {
			benchStream(b, chunk, func(p []byte) {
				h := cachedHasher
				h.Write(p)
			})
		})
	}
}

// BenchmarkStdlibTables 用标准库 + 自定义多项式。
//
// 标准库对非内置多项式在 len(p) >= 2048 时走 slicing，
// 每次调用现建一张 16 KiB 表（实测在栈上，不产生堆分配）。
func BenchmarkStdlibTables(b *testing.B) {
	tbl := crc64.MakeTable(crc64ReflectPoly)
	for _, chunk := range benchChunks {
		b.Run(chunkName(chunk), func(b *testing.B) {
			benchStream(b, chunk, func(p []byte) {
				_ = crc64.Update(0, tbl, p)
			})
		})
	}
}

// cachedHasher 全局复用：真实调用方也是长时间持有 hasher 反复 Write，
// 而不是每次 Write 都新建。基准必须反映这一点。
var cachedHasher = NewCRC64NVMEHasher()

func chunkName(n int) string {
	switch {
	case n >= 4<<20:
		return "chunk_4MiB"
	case n >= 64<<10:
		return "chunk_64KiB"
	case n >= 32<<10:
		return "chunk_32KiB"
	default:
		return "chunk_8KiB"
	}
}
