package bench

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/binbinpeng123/Blink/internal/store"
)

// listBenchObjects 是 LIST 基准的桶内对象数。
//
// 取 10 万是因为 03 文档把"LIST 必须读全部 meta.json"标为本方案最大的
// 性能风险；风险量级必须在真实规模上量，1 万以内的数字不足以暴露问题。
const listBenchObjects = 100_000

// BenchmarkList100kObjects 测量 10 万对象桶的一次 LIST。
//
// 一次 LIST 无论 max-keys 取多少，存储层都必须扫描并排序全部对象
// （目录名是 key 的哈希，无法做范围定位），因此 max-keys=1000
// 量到的就是全量扫描成本；这正是切片 9 创新 D 要对照的基线。
//
// 数字只作本机回归基线（docs/05）：绑定当前机器/文件系统，
// 不可作为对外性能承诺。
func BenchmarkList100kObjects(b *testing.B) {
	s, root := openBenchStoreRoot(b)
	seedBenchObjects(b, root, listBenchObjects)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		res, err := s.ListObjects("bench", store.ListOptions{MaxKeys: 1000})
		if err != nil {
			b.Fatalf("ListObjects 失败: %v", err)
		}
		// 断言基准数据确实被看到：静默的空结果会让基准"跑得很快"。
		if res.KeyCount != 1000 || !res.IsTruncated {
			b.Fatalf("首屏 KeyCount=%d truncated=%v，期望 1000/true",
				res.KeyCount, res.IsTruncated)
		}
	}
}

// benchMeta 是预置对象时写出的最小元数据。
//
// 刻意不复用 store 的内部类型（test/bench 是外部包）：这里验证的正是
// "磁盘布局是稳定契约"。若布局或必需字段变了，LIST 会以 ErrCorrupt
// 响亮失败，而不是让基准悄悄量到一份空桶。
type benchMeta struct {
	Version     int       `json:"version"`
	Key         string    `json:"key"`
	Size        int64     `json:"size"`
	ETag        string    `json:"etag"`
	ContentType string    `json:"content_type"`
	Modified    time.Time `json:"modified"`
	Blocks      []any     `json:"blocks"`
	NodeID      string    `json:"node_id"`
	ObjVersion  uint64    `json:"obj_version"`
}

// seedBenchObjects 直接按 03 文档的布局写出 n 个对象目录。
//
// 不逐个走 PutObject：那要为每个对象做两次 fsync + 两次目录同步，
// 10 万对象的前置时间会以分钟计，而基准量的是 LIST 而不是写入。
// 这里也刻意不 fsync —— 临时目录在测试结束后即被删除，无需持久性。
func seedBenchObjects(b *testing.B, root string, n int) {
	b.Helper()
	start := time.Now()

	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < n; i += workers {
				if err := writeBenchObject(root, i); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		b.Fatalf("预置对象失败: %v", err)
	}
	b.Logf("预置 %d 个对象耗时 %v", n, time.Since(start))
}

func writeBenchObject(root string, i int) error {
	key := fmt.Sprintf("dataset/2026/part-%08d.bin", i)
	sum := sha256.Sum256([]byte(key))
	hash := hex.EncodeToString(sum[:])
	dir := filepath.Join(root, "buckets", "bench", "objects", hash[:2], hash)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	etag := fmt.Sprintf("%x", md5.Sum([]byte(key)))
	data, err := json.Marshal(benchMeta{
		Version:     1,
		Key:         key,
		Size:        4096,
		ETag:        etag,
		ContentType: "application/octet-stream",
		Modified:    time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		Blocks:      []any{},
		NodeID:      "bench",
		ObjVersion:  1,
	})
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "meta.json"), data, 0o644)
}
