// Package bench 存放需要真实磁盘的性能基线。
//
// 这些数字**只作本机回归基线**（见 docs/05）：它们绑定当前机器的磁盘与
// 文件系统，不可作为对外性能承诺。任何优化合入前必须有前后对照数据。
package bench

import (
	"bytes"
	"io"
	"testing"

	"github.com/binbinpeng123/Blink/internal/store"
)

// benchObjectSize 是基准对象的固定大小。取 64 MiB 是为了让对象
// 明显大于页缓存友好区间之外的块数（默认 4 MiB/块 → 16 块），
// 同时保证基准本身能在数秒内跑完。
const benchObjectSize = 64 << 20

// rangeSize 是每次 Range 请求的字节数，也是 b.SetBytes 的口径。
const rangeSize = 64 << 10

// BenchmarkGetRangeSequential 测量"顺序 Range 读"的端到端成本。
//
// 每次迭代都重新 OpenObject：HTTP 下每个 Range 请求都是一次独立请求，
// 因此元数据读取与块 Stat 校验都属于真实成本，不能摊到循环外。
// 读取写入 io.Discard —— 这里量的是存储读路径，网络发送由部署环境决定。
func BenchmarkGetRangeSequential(b *testing.B) {
	s := openBenchStore(b)
	body := benchBody(benchObjectSize)
	if _, err := s.PutObject("bench", "model.bin", bytes.NewReader(body), store.PutOptions{
		ExpectedSize: int64(len(body)),
	}); err != nil {
		b.Fatalf("写入基准对象失败: %v", err)
	}

	// 顺序游标在迭代之间推进，到末尾回绕，形成"从头到尾再从头"的模式。
	var offset int64

	b.SetBytes(rangeSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if offset+rangeSize > benchObjectSize {
			offset = 0
		}
		r, err := s.OpenObject("bench", "model.bin")
		if err != nil {
			b.Fatalf("OpenObject 失败: %v", err)
		}
		if err := r.WriteRangeTo(io.Discard, offset, rangeSize); err != nil {
			b.Fatalf("WriteRangeTo(%d) 失败: %v", offset, err)
		}
		offset += rangeSize
	}
}

// openBenchStore 创建一个临时数据目录上的存储。临时目录随测试结束删除，
// 不会污染仓库；因此这里不需要 .gitignore 之外的清理逻辑。
func openBenchStore(b *testing.B) *store.Store {
	s, _ := openBenchStoreRoot(b)
	return s
}

// openBenchStoreRoot 与 openBenchStore 相同，但同时返回数据目录。
//
// 需要直接按布局预置海量对象的基准（如 List100kObjects）用它：
// 逐个 PutObject 要为每个对象做两次 fsync，10 万对象会把基准前置时间
// 拉到分钟级，量到的却全是写入成本。
func openBenchStoreRoot(b *testing.B) (*store.Store, string) {
	b.Helper()
	root := b.TempDir()
	s, err := store.Open(store.Options{Root: root})
	if err != nil {
		b.Fatalf("打开存储失败: %v", err)
	}
	if err := s.CreateBucket("bench"); err != nil {
		b.Fatalf("创建桶失败: %v", err)
	}
	return s, root
}

// benchBody 生成确定性的非全零内容，避免文件系统对稀疏/全零块做特殊处理，
// 也让去重不会误判所有块相同。
func benchBody(n int) []byte {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = byte(i*31 + i>>8)
	}
	return buf
}
