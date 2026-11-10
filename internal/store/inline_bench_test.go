package store

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
)

// 内联小对象的基准：同一份 1 KiB 内容分别走 CAS 与内联两条路径。
//
// 关注两件事：
//   - PUT：内联省掉 CAS 块的 fsync + rename，只提交 meta.json；
//   - GET：内联省掉打开/Stat CAS 文件，内容直接从元数据切片写出。
//
// 用 b.N 控制总写入量，避免在开发机上产生大流量。

const benchInlineBody = 1 << 10

func newBenchInlineStore(b *testing.B, inlineMax int64) *Store {
	b.Helper()
	s, err := Open(Options{
		Root:                filepath.Join(b.TempDir(), "data"),
		BlockSize:           4 << 20,
		InlineThreshold:     64 << 10,
		InlineObjectMaxSize: inlineMax,
	})
	if err != nil {
		b.Fatalf("Open 失败: %v", err)
	}
	if err := s.CreateBucket("bkt"); err != nil {
		b.Fatalf("CreateBucket 失败: %v", err)
	}
	return s
}

func benchmarkPut(b *testing.B, inlineMax int64) {
	s := newBenchInlineStore(b, inlineMax)
	body := bytes.Repeat([]byte("a"), benchInlineBody)
	b.SetBytes(benchInlineBody)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k-%d", i%128)
		if _, err := s.PutObject("bkt", key, bytes.NewReader(body), PutOptions{
			ExpectedSize: benchInlineBody,
		}); err != nil {
			b.Fatalf("PutObject 失败: %v", err)
		}
	}
}

func BenchmarkPutSmallCAS(b *testing.B)    { benchmarkPut(b, 0) }
func BenchmarkPutSmallInline(b *testing.B) { benchmarkPut(b, 4<<10) }

func benchmarkGet(b *testing.B, inlineMax int64) {
	s := newBenchInlineStore(b, inlineMax)
	body := bytes.Repeat([]byte("a"), benchInlineBody)
	if _, err := s.PutObject("bkt", "k", bytes.NewReader(body), PutOptions{
		ExpectedSize: benchInlineBody,
	}); err != nil {
		b.Fatalf("PutObject 失败: %v", err)
	}
	b.SetBytes(benchInlineBody)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := s.OpenObject("bkt", "k")
		if err != nil {
			b.Fatalf("OpenObject 失败: %v", err)
		}
		var buf bytes.Buffer
		buf.Grow(benchInlineBody)
		if err := r.WriteRangeTo(&buf, 0, benchInlineBody); err != nil {
			b.Fatalf("Read 失败: %v", err)
		}
	}
}

func BenchmarkGetSmallCAS(b *testing.B)    { benchmarkGet(b, 0) }
func BenchmarkGetSmallInline(b *testing.B) { benchmarkGet(b, 4<<10) }

// benchmarkList 用 1000 个 1 KiB 对象测一次完整 LIST。
//
// 这是内联方案最需要盯住的回归点：LIST 必须读取并解码每一个 meta.json，
// 内联内容以 base64 存在其中（膨胀 4/3），对象越多、内容越大，LIST 的
// 单次耗时就越高。基准给出可比较的数字，而不是靠猜测。
func benchmarkList(b *testing.B, inlineMax int64) {
	s := newBenchInlineStore(b, inlineMax)
	body := bytes.Repeat([]byte("a"), benchInlineBody)
	const objects = 1000
	for i := 0; i < objects; i++ {
		key := fmt.Sprintf("obj-%04d", i)
		if _, err := s.PutObject("bkt", key, bytes.NewReader(body), PutOptions{
			ExpectedSize: benchInlineBody,
		}); err != nil {
			b.Fatalf("PutObject 失败: %v", err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := s.ListObjects("bkt", ListOptions{MaxKeys: objects})
		if err != nil {
			b.Fatalf("ListObjects 失败: %v", err)
		}
		if res.KeyCount != objects {
			b.Fatalf("KeyCount = %d，期望 %d", res.KeyCount, objects)
		}
	}
}

func BenchmarkListSmallCAS(b *testing.B)    { benchmarkList(b, 0) }
func BenchmarkListSmallInline(b *testing.B) { benchmarkList(b, 4<<10) }
