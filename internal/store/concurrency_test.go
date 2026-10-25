package store

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
)

// openConcurrencyStore 打开一个块大小 64 KiB 的存储。
//
// 不共享 newTestStore：并发测试需要自己控制块布局（让测试数据跨越多个块），
// 而且每个测试都应拿到独立目录，避免 -count=N 时互相干扰。
func openConcurrencyStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(Options{
		Root:            filepath.Join(t.TempDir(), "data"),
		BlockSize:       64 << 10,
		InlineThreshold: 16,
	})
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	if err := s.CreateBucket("bkt"); err != nil {
		t.Fatalf("CreateBucket 失败: %v", err)
	}
	return s
}

// readObjectNoFatal 是非测试 goroutine 可用的读取助手。
//
// 测试失败只能在测试主 goroutine 上用 t.Fatalf；后台 goroutine 里
// 调用它会直接终止整个测试进程，掩盖真正的失败原因。
func readObjectNoFatal(s *Store, key string) ([]byte, error) {
	r, err := s.OpenObject("bkt", key)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := r.WriteRangeTo(&buf, 0, r.Info().Size); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// TestConcurrentPutSameKeySerializes 让 32 个写者同时覆盖同一个 key。
//
// 断言分两层：
//  1. 每个写者拿到的 obj_version 恰好是 1..32 的一个排列 —— 说明按 key
//     加锁真的互斥，没有两个写者读到同一个 prev 版本；
//  2. 最终内容是**某一份完整输入**，而不是多份输入的首尾拼贴。
func TestConcurrentPutSameKeySerializes(t *testing.T) {
	if testing.Short() {
		t.Skip("并发压力测试在 -short 下跳过")
	}
	s := openConcurrencyStore(t)

	const writers = 32
	const payloadSize = 200 << 10 // 4 个块
	payloads := make([][]byte, writers)
	for i := range payloads {
		payloads[i] = patternBytes(payloadSize, byte(i+1))
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	versions := make([]uint64, writers)
	errs := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			info, err := s.PutObject("bkt", "shared", bytes.NewReader(payloads[i]), PutOptions{
				ExpectedSize: int64(len(payloads[i])),
			})
			if err != nil {
				errs[i] = err
				return
			}
			versions[i] = info.Version
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("写者 %d 失败: %v", i, err)
		}
	}

	sorted := append([]uint64(nil), versions...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	for i, v := range sorted {
		if v != uint64(i+1) {
			t.Fatalf("obj_version 不是 1..%d 的排列：第 %d 个是 %d", writers, i+1, v)
		}
	}

	// 长度先对：内容比对只有在长度一致时才有意义。
	got := readObjectAll(t, s, "shared")
	if len(got) != payloadSize {
		t.Fatalf("对象长度 = %d，期望 %d", len(got), payloadSize)
	}
	matched := false
	for _, p := range payloads {
		if bytes.Equal(got, p) {
			matched = true
			break
		}
	}
	if !matched {
		t.Fatal("最终内容不是任何一次完整写入的结果（出现了撕裂）")
	}

	r, err := s.OpenObject("bkt", "shared")
	if err != nil {
		t.Fatalf("OpenObject 失败: %v", err)
	}
	if r.Info().Version != writers {
		t.Errorf("最终 obj_version = %d，期望 %d", r.Info().Version, writers)
	}
}

// TestConcurrentReadsNeverSeeTornObject 在持续覆盖写期间保持 8 个读者。
//
// 这是 03 文档"读路径不加锁"的正确性依据：读者拿到的 meta 快照指向
// 不可变的 CAS 块，因此覆盖写只改变可见版本，不会就地改写读者手里的数据。
// 任何一次读到"新旧混合"的内容都说明这个前提被破坏了。
func TestConcurrentReadsNeverSeeTornObject(t *testing.T) {
	if testing.Short() {
		t.Skip("并发压力测试在 -short 下跳过")
	}
	s := openConcurrencyStore(t)

	const readers = 8
	const overwrites = 24
	const payloadSize = 128 << 10 // 2 个块

	payloads := [][]byte{
		patternBytes(payloadSize, 0x31),
		patternBytes(payloadSize, 0x42),
		patternBytes(payloadSize, 0x53),
		patternBytes(payloadSize, 0x64),
	}
	mustPutBytes(t, s, "hot", payloads[0])

	var (
		done    atomic.Bool
		reads   atomic.Int64
		readErr atomic.Int64
		wg      sync.WaitGroup
	)
	start := make(chan struct{})
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for !done.Load() {
				got, err := readObjectNoFatal(s, "hot")
				if err != nil {
					// 只报一次错就收手，避免刷屏淹没真正的失败。
					if readErr.Add(1) == 1 {
						t.Errorf("读者读取失败: %v", err)
					}
					return
				}
				reads.Add(1)
				ok := false
				for _, p := range payloads {
					if bytes.Equal(got, p) {
						ok = true
						break
					}
				}
				if !ok {
					if readErr.Add(1) == 1 {
						t.Errorf("读到撕裂内容：%d 字节，不属于任何一个完整版本", len(got))
					}
					return
				}
			}
		}()
	}

	close(start)
	for i := 0; i < overwrites; i++ {
		mustPutBytes(t, s, "hot", payloads[(i+1)%len(payloads)])
	}
	done.Store(true)
	wg.Wait()

	if errs := readErr.Load(); errs > 0 {
		t.Fatalf("%d 个读者报告了错误", errs)
	}
	// 读者必须在写者完成前真正读过 —— 否则这个测试什么也没验证。
	if n := reads.Load(); n == 0 {
		t.Fatal("读者一次都没读到数据，测试未生效")
	}
	t.Logf("覆盖写 %d 次期间完成 %d 次完整读取", overwrites, reads.Load())
}

// TestConcurrentListSeesOnlyReadableObjects 在持续写入时反复 LIST。
//
// 强一致承诺的一部分：LIST 返回的每个 key 都必须能立刻打开并读全。
// 实现里 LIST 会遍历 meta.json，跳过并发消失的元数据；本测试断言的是
// 另一个方向 —— 不能把**读到一半/写了一半**的元数据当成对象返回。
func TestConcurrentListSeesOnlyReadableObjects(t *testing.T) {
	if testing.Short() {
		t.Skip("并发压力测试在 -short 下跳过")
	}
	s := openConcurrencyStore(t)

	const total = 40
	const payloadSize = 100 << 10

	var (
		done    atomic.Bool
		listed  atomic.Int64
		badOnce atomic.Bool
		wg      sync.WaitGroup
	)
	start := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for !done.Load() {
			res, err := s.ListObjects("bkt", ListOptions{Prefix: "list/", MaxKeys: 1000})
			if err != nil {
				if badOnce.CompareAndSwap(false, true) {
					t.Errorf("LIST 失败: %v", err)
				}
				return
			}
			listed.Add(int64(len(res.Objects)))
			for _, o := range res.Objects {
				got, err := readObjectNoFatal(s, o.Key)
				if err != nil {
					if badOnce.CompareAndSwap(false, true) {
						t.Errorf("LIST 返回的 %s 读不到: %v", o.Key, err)
					}
					return
				}
				if int64(len(got)) != o.Size {
					if badOnce.CompareAndSwap(false, true) {
						t.Errorf("LIST 报告 %s 为 %d 字节，实际读到 %d", o.Key, o.Size, len(got))
					}
					return
				}
			}
		}
	}()

	close(start)
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("list/%03d", i)
		mustPutBytes(t, s, key, patternBytes(payloadSize, byte(i+1)))
	}
	done.Store(true)
	wg.Wait()

	if listed.Load() == 0 {
		t.Fatal("LIST 一次都没返回对象，测试未生效")
	}
}
