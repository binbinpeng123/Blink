package bench

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/binbinpeng123/Blink/internal/store"
)

// putSizes 覆盖 05 文档要求的对象尺寸档位。
//
// 1 KiB 是"小对象元数据路径"的代表（验收门槛 P50 < 2 ms）；1 MiB 是
// 常规对象；16 MiB 让一次写入跨过多个 4 MiB 块，暴露每块 fsync 的放大。
// 不取 1 GiB：本机压测总写入必须 ≤ 512 MiB，且大对象吞吐由
// BenchmarkGetRangeSequential 与并发基准覆盖。
var putSizes = []int64{1 << 10, 64 << 10, 1 << 20, 16 << 20}

func sizeName(n int64) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%dMiB", n>>20)
	case n >= 1<<10:
		return fmt.Sprintf("%dKiB", n>>10)
	default:
		return fmt.Sprintf("%dB", n)
	}
}

// BenchmarkPutObjectSizes 测量不同对象的完整 PUT 延迟。
//
// 每次迭代使用**不同的 key**：内容寻址会去重相同内容，重复写同一个 key
// 从第二次起只改 meta，量到的就不再是写入成本。key 不同但内容相同仍会
// 命中 CAS 去重，因此每个对象的内容前缀写入一个序号，确保真正落盘。
//
// 内容模板在计时开始前生成一次，迭代内只做 memcpy + 改前缀：旧写法
// 每轮用字节循环重新生成 16 MiB，生成成本会盖过 PUT 本身（CPU profile
// 里曾占 15%），量到的就不再是存储路径。memcpy 同样计入，但它是任何
// 真实调用方都免不了的一次拷贝。
//
// 迭代结束不清理：CAS 块在测试临时目录里，随 b.TempDir 一起删除。
func BenchmarkPutObjectSizes(b *testing.B) {
	s := openBenchStore(b)

	for _, size := range putSizes {
		size := size
		b.Run(sizeName(size), func(b *testing.B) {
			tmpl := benchBody(int(size))
			body := make([]byte, size)
			b.SetBytes(size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				copy(body, tmpl)
				putSeq(body, i)
				key := fmt.Sprintf("put/%s/%d", sizeName(size), i)
				if _, err := s.PutObject("bench", key, bytes.NewReader(body), store.PutOptions{
					ExpectedSize: size,
				}); err != nil {
					b.Fatalf("PutObject(%d) 失败: %v", size, err)
				}
			}
		})
	}
}

// BenchmarkGetObjectSizes 测量完整 GET（元数据 + 全部块）的吞吐。
//
// 每个尺寸只预写一个对象，迭代内反复 OpenObject 并读取：
// HTTP 下每个 GET 都是独立请求，元数据解析与逐块 Stat 都是真实成本。
func BenchmarkGetObjectSizes(b *testing.B) {
	s := openBenchStore(b)

	for _, size := range putSizes {
		size := size
		key := "get/" + sizeName(size)
		if _, err := s.PutObject("bench", key, bytes.NewReader(sizedBody(size, 0)), store.PutOptions{
			ExpectedSize: size,
		}); err != nil {
			b.Fatalf("预写 %s 失败: %v", sizeName(size), err)
		}

		b.Run(sizeName(size), func(b *testing.B) {
			b.SetBytes(size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, err := s.OpenObject("bench", key)
				if err != nil {
					b.Fatalf("OpenObject 失败: %v", err)
				}
				if err := r.WriteRangeTo(io.Discard, 0, size); err != nil {
					b.Fatalf("WriteRangeTo 失败: %v", err)
				}
			}
		})
	}
}

// BenchmarkConcurrentPut1MiB 测量 64 并发 1 MiB PUT 的聚合吞吐。
//
// 05 文档的验收门槛是 > 300 MB/s。并发度固定 64 是为了让数字可复现，
// 不代表边端设备的推荐值；每轮 b.N 次迭代每个 worker 各写一次。
func BenchmarkConcurrentPut1MiB(b *testing.B) {
	const (
		workers = 64
		size    = 1 << 20
	)
	s := openBenchStore(b)

	// 每个 worker 一块独立缓冲：并发写不能共享 body，而基准要量的是
	// 存储路径，不是 64 个 goroutine 各自填充 1 MiB 的成本。
	tmpl := benchBody(size)
	bodies := make([][]byte, workers)
	for w := range bodies {
		bodies[w] = make([]byte, size)
		copy(bodies[w], tmpl)
	}

	b.SetBytes(size * workers)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		errs := make(chan error, workers)
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w, iter int) {
				defer wg.Done()
				body := bodies[w]
				putSeq(body, iter*workers+w)
				key := fmt.Sprintf("concurrent/%d/%d", iter, w)
				if _, err := s.PutObject("bench", key, bytes.NewReader(body), store.PutOptions{
					ExpectedSize: size,
				}); err != nil {
					errs <- err
				}
			}(w, i)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			b.Fatalf("并发 PutObject 失败: %v", err)
		}
	}
}

// sizedBody 生成 n 字节的确定性内容，并把 i 写进开头，避免 CAS 去重
// 使后续迭代根本不落盘。
func sizedBody(n int64, i int) []byte {
	buf := benchBody(int(n))
	putSeq(buf, i)
	return buf
}

// putSeq 把序号写进内容前缀，让每次 PUT 的块哈希都不同。
//
// 只改 8 字节而不是重新生成整块：内容寻址只关心"有没有出现过"，
// 前缀不同就足以让块落盘，其余字节共用模板可以避开生成成本。
func putSeq(buf []byte, i int) {
	binary.LittleEndian.PutUint64(buf[:8], uint64(i))
}
