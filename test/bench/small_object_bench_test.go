package bench

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/binbinpeng123/Blink/internal/store"
)

const (
	// smallObjectSize 取 1 KiB：03 文档把 4 KiB 以下对象定义为小对象，
	// 1 KiB 是其中最有代表性的档位（配置、prompt 片段、推理结果）。
	smallObjectSize = 1 << 10
	// smallObjectCount 是创新 D 对照组的批量规模。10k 小对象足以暴露
	// "每对象两次 fsync + 两次目录同步"的固定成本，又不会让基线跑上小时级。
	smallObjectCount = 10_000
)

// BenchmarkPutSmallObject10k 测量向空桶连续写入 10,000 个 1 KiB 对象的
// 总耗时与单对象延迟分布，作为创新 D（小对象打包为只读 segment）的基线。
//
// 这是 03 文档"开发纪律"要求的先决数据：没有它，不得决定是否实施 D。
// 对象内容逐条不同（前缀写入序号），因此不会命中 CAS 去重，量到的是真实落盘；
// 写入在计时区内完成，不靠预置文件绕过持久化屏障。
//
// 使用与线上一致的默认：≤4 KiB 内容内联进 meta.json（创新 D0）。
// 读路径与 LIST 的基线分别由 BenchmarkGetObjectSizes/1KiB 与
// BenchmarkList100kObjects 覆盖。
//
// 单次迭代在本机约 2–3 分钟（APFS 上每个 PUT 约 14–18 ms），因此默认
// 只跑一次：
//
//	go test ./test/bench -run '^$' -bench '^BenchmarkPutSmallObject10k$' -benchtime=1x
//
// 一个迭代只写 10 MiB，满足"本机压测总写入 ≤512 MiB"的约束；若把
// -benchtime 调大，请自行确认 b.N 次迭代的总写入量。
func BenchmarkPutSmallObject10k(b *testing.B) {
	s, err := store.Open(store.Options{
		Root:                b.TempDir(),
		InlineObjectMaxSize: 4 << 10,
	})
	if err != nil {
		b.Fatalf("打开存储失败: %v", err)
	}
	if err := s.CreateBucket("bench"); err != nil {
		b.Fatalf("创建桶失败: %v", err)
	}

	// 内容模板只生成一次，迭代内只做 memcpy + 改序号；否则 10k 次重建
	// 1 KiB 缓冲的成本会混进 PUT 延迟。
	tmpl := benchBody(smallObjectSize)
	body := make([]byte, smallObjectSize)
	durations := make([]time.Duration, 0, smallObjectCount*b.N)

	b.SetBytes(smallObjectSize * smallObjectCount)
	b.ReportAllocs()
	b.ResetTimer()

	for iter := 0; iter < b.N; iter++ {
		for i := 0; i < smallObjectCount; i++ {
			copy(body, tmpl)
			putSeq(body, iter*smallObjectCount+i)
			key := fmt.Sprintf("small/%d/%05d", iter, i)

			start := time.Now()
			if _, err := s.PutObject("bench", key, bytes.NewReader(body), store.PutOptions{
				ExpectedSize: smallObjectSize,
			}); err != nil {
				b.Fatalf("PutObject(%s) 失败: %v", key, err)
			}
			durations = append(durations, time.Since(start))
		}
	}

	b.StopTimer()

	// 延迟分布是 D 的关键输入：打包 segment 能摊薄的是每次 PUT 的固定
	// 开销量，若 P50 已被 fsync 主导，再叠加一层 segment 索引未必划算。
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	for _, p := range []struct {
		name string
		q    float64
	}{
		{"p50", 0.50},
		{"p95", 0.95},
		{"p99", 0.99},
	} {
		b.ReportMetric(float64(durations[percentileIndex(len(durations), p.q)])/float64(time.Millisecond), p.name+"-ms")
	}

	var total time.Duration
	for _, d := range durations {
		total += d
	}
	b.ReportMetric(float64(total)/float64(len(durations))/float64(time.Millisecond), "mean-ms")
	b.ReportMetric(float64(len(durations))/total.Seconds(), "objects/s")
}

// percentileIndex 返回有序切片中给定分位数的下标（最近秩法）。
func percentileIndex(n int, q float64) int {
	idx := int(float64(n)*q + 0.5)
	if idx >= n {
		return n - 1
	}
	return idx
}
