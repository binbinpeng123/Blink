package bench

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/binbinpeng123/Blink/internal/store"
)

// 写放大基准：量化每次操作真正落盘的字节数，而不是只量延迟/吞吐。
//
// 05 文档要求的 ReportMetric 是 "write-B/op" —— 它把"对象 16 MiB、
// 实际写了多少"直接摆到台面上，也是"去重第二份不落盘""MPU Complete
// 零重写"这类创新点唯一可断言的形式。
//
// 这些基准的默认 -benchtime 会按吞吐自动加迭代，16 MiB 档一轮就能写出
// 数 GiB。写字节数与迭代次数无关，本机应固定用：
//
//	go test ./test/bench -run '^$' -bench 'WriteBytes' -benchtime=1x
//
// 以遵守"本机压测总写入 ≤ 512 MiB"的预算。

// BenchmarkPutWriteBytes 测量不同尺寸 PUT 的写放大。
//
// 每轮内容前缀（每个 4 MiB 块的首 8 字节）都写序号：内容寻址会去重相同
// 块，不去重就有一半以上的块根本不落盘，量到的写放大是假的。
// 除数据块外，meta.json 的两次原子写也会计入 —— 小对象的 write-B/op
// 因此会略大于对象本身，这正是边端小对象需要关注的固定开销。
func BenchmarkPutWriteBytes(b *testing.B) {
	s, cf := openCountedBenchStore(b)

	for _, size := range putSizes {
		size := size
		b.Run(sizeName(size), func(b *testing.B) {
			tmpl := benchBody(int(size))
			body := make([]byte, size)
			b.SetBytes(size)
			b.ReportAllocs()

			var total int64
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				copy(body, tmpl)
				markBlocksUnique(body, i)
				cf.written.Store(0)
				key := fmt.Sprintf("write-bytes/%s/%d", sizeName(size), i)
				if _, err := s.PutObject("bench", key, bytes.NewReader(body), store.PutOptions{
					ExpectedSize: size,
				}); err != nil {
					b.Fatalf("PutObject(%s) 失败: %v", sizeName(size), err)
				}
				total += cf.written.Load()
			}
			b.StopTimer()
			b.ReportMetric(float64(total)/float64(b.N), "write-B/op")
		})
	}
}

// BenchmarkDedupSecondCopyWriteBytes 验证"同内容第二次写入不再落盘数据块"。
//
// 先写一份 16 MiB 对象，再以不同 key 写入完全相同的内容：按内容寻址，
// 第二份应只写 meta，write-B/op 应落在 KB 级。计划中的 1 GiB 版本受
// 本机写入预算限制未实现（05 文档），16 MiB 已足以覆盖 4 个数据块。
func BenchmarkDedupSecondCopyWriteBytes(b *testing.B) {
	const size = 16 << 20
	s, cf := openCountedBenchStore(b)

	body := benchBody(size)
	if _, err := s.PutObject("bench", "dedup/first", bytes.NewReader(body), store.PutOptions{
		ExpectedSize: size,
	}); err != nil {
		b.Fatalf("预写第一份失败: %v", err)
	}

	b.ReportAllocs()
	var total int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cf.written.Store(0)
		key := fmt.Sprintf("dedup/second/%d", i)
		if _, err := s.PutObject("bench", key, bytes.NewReader(body), store.PutOptions{
			ExpectedSize: size,
		}); err != nil {
			b.Fatalf("写入第二份失败: %v", err)
		}
		total += cf.written.Load()
	}
	b.StopTimer()
	b.ReportMetric(float64(total)/float64(b.N), "write-B/op")
}

// BenchmarkMPUCompleteWriteBytes 验证 MPU Complete 只写元数据、不重写数据。
//
// 分片按 5 MiB + 1 MiB 上传（S3 要求除末片外不小于 5 MiB），上传完成后
// 计数器归零，再单独计量 Complete：对象由块引用拼装，Complete 不应搬运
// 任何分片字节 —— 这是边端大文件分片上传相比"先合并再落盘"的核心优势。
func BenchmarkMPUCompleteWriteBytes(b *testing.B) {
	const (
		partSize = 5 << 20
		lastSize = 1 << 20
	)
	s, cf := openCountedBenchStore(b)

	b.ReportAllocs()
	var total int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		up, err := s.CreateMultipartUpload("bench", fmt.Sprintf("mpu-complete/%d", i), store.CreateMultipartOptions{})
		if err != nil {
			b.Fatalf("CreateMultipartUpload 失败: %v", err)
		}
		p1 := benchBody(partSize)
		putSeq(p1, 2*i)
		info1, err := s.UploadPart("bench", up.Key, up.UploadID, 1, bytes.NewReader(p1), store.UploadPartOptions{
			ExpectedSize: partSize,
		})
		if err != nil {
			b.Fatalf("UploadPart(1) 失败: %v", err)
		}
		p2 := benchBody(lastSize)
		putSeq(p2, 2*i+1)
		info2, err := s.UploadPart("bench", up.Key, up.UploadID, 2, bytes.NewReader(p2), store.UploadPartOptions{
			ExpectedSize: lastSize,
		})
		if err != nil {
			b.Fatalf("UploadPart(2) 失败: %v", err)
		}

		cf.written.Store(0)
		if _, err := s.CompleteMultipartUpload("bench", up.Key, up.UploadID, []store.CompletePart{
			{PartNumber: 1, ETag: info1.ETag},
			{PartNumber: 2, ETag: info2.ETag},
		}, store.CompleteOptions{}); err != nil {
			b.Fatalf("CompleteMultipartUpload 失败: %v", err)
		}
		total += cf.written.Load()
	}
	b.StopTimer()
	b.ReportMetric(float64(total)/float64(b.N), "write-B/op")
}

// markBlocksUnique 在每 4 MiB 块的首 8 字节写入序号。
//
// 4 MiB 与 store 默认块大小一致；只改块首而不是整块，避免内容生成成本
// 混进写入路径的计时。
func markBlocksUnique(buf []byte, iter int) {
	const blockSize = 4 << 20
	for off := 0; off+8 <= len(buf); off += blockSize {
		binary.LittleEndian.PutUint64(buf[off:], uint64(iter)*7919+uint64(off))
	}
}
