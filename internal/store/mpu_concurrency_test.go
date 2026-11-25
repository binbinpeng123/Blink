package store

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"
)

// 本文件补 docs/05「并发冲突」里缺失的分片会话状态机：Complete 与 Abort 互斥、
// 同一个分片编号并发覆盖、ListParts 与在途上传并发。断言全部落在
// "会话要么完整存在、要么彻底消失，对象要么不存在、要么内容完整"上。

// md5Hex 是 UploadPart 写入分片状态的 ETag 形态。
func md5Hex(b []byte) string {
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:])
}

// tryUploadPart 是可在测试 goroutine 之外调用的 UploadPart。
//
// 测试辅助函数（uploadPart 等）内部用 t.Fatalf，只允许在主 goroutine 调用；
// 并发用例必须把错误带回来，否则首个失败会直接终止进程、掩盖真实原因。
func tryUploadPart(s *Store, key, uploadID string, n int, data []byte) (*PartInfo, error) {
	return s.UploadPart("bkt", key, uploadID, n, bytes.NewReader(data), UploadPartOptions{
		ExpectedSize: int64(len(data)),
	})
}

// TestConcurrentUploadPartSameNumber 让 16 个写者并发覆盖同一个分片编号。
//
// 断言分片状态按编号串行提交、没有撕裂：
//   - 每次 UploadPart 都成功，且返回的 ETag 是某一份完整输入；
//   - ListParts 只看到一个分片，大小与 ETag 对得上；
//   - 用它 Complete 后，对象内容与 ETag 指向的那份输入逐字节相等。
func TestConcurrentUploadPartSameNumber(t *testing.T) {
	if testing.Short() {
		t.Skip("并发压力测试在 -short 下跳过")
	}
	s := openMPUStore(t)

	const writers = 16
	payloads := make([][]byte, writers)
	etagOf := make(map[string][]byte, writers)
	for i := range payloads {
		payloads[i] = mpuBytes(1<<10, byte(i+1))
		etagOf[md5Hex(payloads[i])] = payloads[i]
	}

	up := createUpload(t, s, "same-part", "", "")

	start := make(chan struct{})
	var wg sync.WaitGroup
	infos := make([]*PartInfo, writers)
	errs := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			infos[i], errs[i] = tryUploadPart(s, up.Key, up.UploadID, 1, payloads[i])
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("写者 %d 上传失败: %v", i, err)
		}
		if _, ok := etagOf[infos[i].ETag]; !ok {
			t.Fatalf("写者 %d 返回的 ETag %q 不属于任何一份完整输入", i, infos[i].ETag)
		}
	}

	listed, err := s.ListParts("bkt", up.Key, up.UploadID, ListPartsOptions{MaxParts: 100})
	if err != nil {
		t.Fatalf("ListParts 失败: %v", err)
	}
	if len(listed.Parts) != 1 {
		t.Fatalf("ListParts 返回 %d 个分片，期望 1 个:%+v", len(listed.Parts), listed.Parts)
	}
	got := listed.Parts[0]
	want, ok := etagOf[got.ETag]
	if !ok {
		t.Fatalf("最终分片的 ETag %q 不属于任何一份完整输入", got.ETag)
	}
	if got.Size != int64(len(want)) {
		t.Fatalf("最终分片大小 = %d，期望 %d", got.Size, len(want))
	}

	obj, err := s.CompleteMultipartUpload("bkt", up.Key, up.UploadID,
		[]CompletePart{{PartNumber: 1, ETag: got.ETag}}, CompleteOptions{})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload 失败: %v", err)
	}
	if obj.Size != int64(len(want)) {
		t.Fatalf("对象大小 = %d，期望 %d", obj.Size, len(want))
	}
	if body := readObjectAll(t, s, up.Key); !bytes.Equal(body, want) {
		t.Fatalf("对象内容与 ETag 对应的输入不一致（长度 %d vs %d）", len(body), len(want))
	}
}

// TestConcurrentCompleteAndAbort 反复让 Complete 与 Abort 同时抢同一个会话。
//
// 两者在存储层共用 uploadId 锁，因此必须恰好一个成功：
//   - Complete 成功：对象可见且完整，会话消失，Abort 得到 NoSuchUpload；
//   - Abort 成功：会话消失，对象不存在，Complete 得到 NoSuchUpload。
//
// 任何"对象已提交却仍能列出会话"或"两者都失败"的组合都意味着锁或
// 状态清理被绕过。
func TestConcurrentCompleteAndAbort(t *testing.T) {
	if testing.Short() {
		t.Skip("并发压力测试在 -short 下跳过")
	}
	s := openMPUStore(t)

	const rounds = 20
	for i := 0; i < rounds; i++ {
		key := fmt.Sprintf("race-complete/%02d", i)
		up := createUpload(t, s, key, "", "")
		payload := mpuBytes(2<<10, byte(i+1))
		info := uploadPart(t, s, key, up.UploadID, 1, payload, UploadPartOptions{})

		start := make(chan struct{})
		var wg sync.WaitGroup
		var (
			obj       *ObjectInfo
			completeE error
			abortE    error
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			obj, completeE = s.CompleteMultipartUpload("bkt", key, up.UploadID,
				[]CompletePart{{PartNumber: 1, ETag: info.ETag}}, CompleteOptions{})
		}()
		go func() {
			defer wg.Done()
			<-start
			abortE = s.AbortMultipartUpload("bkt", key, up.UploadID)
		}()
		close(start)
		wg.Wait()

		switch {
		case completeE == nil && abortE == nil:
			t.Fatalf("第 %d 轮 Complete 与 Abort 同时成功", i)
		case completeE != nil && abortE != nil:
			t.Fatalf("第 %d 轮两者都失败: complete=%v abort=%v", i, completeE, abortE)

		case completeE == nil:
			if !errors.Is(abortE, ErrNoSuchUpload) {
				t.Fatalf("第 %d 轮 Complete 成功但 Abort 失败原因不是 NoSuchUpload: %v", i, abortE)
			}
			if obj.Size != int64(len(payload)) {
				t.Fatalf("第 %d 轮对象大小 = %d，期望 %d", i, obj.Size, len(payload))
			}
			if body := readObjectAll(t, s, key); !bytes.Equal(body, payload) {
				t.Fatalf("第 %d 轮对象内容不完整（长度 %d vs %d）", i, len(body), len(payload))
			}
			if _, err := s.ListParts("bkt", key, up.UploadID, ListPartsOptions{MaxParts: 10}); !errors.Is(err, ErrNoSuchUpload) {
				t.Fatalf("第 %d 轮 Complete 成功后仍能列出会话: err=%v", i, err)
			}

		default:
			if !errors.Is(completeE, ErrNoSuchUpload) {
				t.Fatalf("第 %d 轮 Abort 成功但 Complete 失败原因不是 NoSuchUpload: %v", i, completeE)
			}
			if _, err := s.OpenObject("bkt", key); !errors.Is(err, ErrNoSuchKey) {
				t.Fatalf("第 %d 轮 Abort 成功后对象仍可见: err=%v", i, err)
			}
			if _, err := s.ListParts("bkt", key, up.UploadID, ListPartsOptions{MaxParts: 10}); !errors.Is(err, ErrNoSuchUpload) {
				t.Fatalf("第 %d 轮 Abort 成功后仍能列出会话: err=%v", i, err)
			}
		}
	}
}

// TestConcurrentUploadPartAndAbort 让分片上传与 Abort 抢同一个会话。
//
// UploadPart 在锁外流式写入、锁内复核会话，因此合法结果只有两种：
// 上传先提交（Abort 随后清掉整个目录）或 Abort 先执行（上传得到 NoSuchUpload）。
// 无论哪种，最终都不能留下可继续使用的会话或可见对象。
func TestConcurrentUploadPartAndAbort(t *testing.T) {
	if testing.Short() {
		t.Skip("并发压力测试在 -short 下跳过")
	}
	s := openMPUStore(t)

	const rounds = 20
	for i := 0; i < rounds; i++ {
		key := fmt.Sprintf("race-abort/%02d", i)
		up := createUpload(t, s, key, "", "")
		payload := mpuBytes(2<<10, byte(i+1))

		start := make(chan struct{})
		var wg sync.WaitGroup
		var (
			partE  error
			abortE error
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, partE = tryUploadPart(s, key, up.UploadID, 1, payload)
		}()
		go func() {
			defer wg.Done()
			<-start
			abortE = s.AbortMultipartUpload("bkt", key, up.UploadID)
		}()
		close(start)
		wg.Wait()

		if abortE != nil {
			t.Fatalf("第 %d 轮 Abort 失败: %v", i, abortE)
		}
		if partE != nil && !errors.Is(partE, ErrNoSuchUpload) {
			t.Fatalf("第 %d 轮 UploadPart 失败原因不是 NoSuchUpload: %v", i, partE)
		}
		if _, err := s.ListParts("bkt", key, up.UploadID, ListPartsOptions{MaxParts: 10}); !errors.Is(err, ErrNoSuchUpload) {
			t.Fatalf("第 %d 轮 Abort 后仍能列出会话: err=%v", i, err)
		}
		if _, err := s.OpenObject("bkt", key); !errors.Is(err, ErrNoSuchKey) {
			t.Fatalf("第 %d 轮 Abort 后对象可见: err=%v", i, err)
		}
	}
}

// TestConcurrentListPartsDuringUploads 在 8 个分片并发上传期间持续 ListParts。
//
// 分片状态以临时文件 + rename 整体替换，ListParts 要么看到旧状态、要么看到
// 新状态。断言每一页都自洽：编号严格升序、大小为正、ETag 是 32 位十六进制。
// 任何半写入的 JSON 都会让 loadPartState 报 ErrCorrupt，从而在这里失败。
func TestConcurrentListPartsDuringUploads(t *testing.T) {
	if testing.Short() {
		t.Skip("并发压力测试在 -short 下跳过")
	}
	s := openMPUStore(t)

	const parts = 8
	up := createUpload(t, s, "list-during-upload", "", "")

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		listErrs []error
		done     = make(chan struct{})
	)
	wg.Add(parts)
	for n := 1; n <= parts; n++ {
		go func(n int) {
			defer wg.Done()
			if _, err := tryUploadPart(s, up.Key, up.UploadID, n, mpuBytes(4<<10, byte(n))); err != nil {
				mu.Lock()
				listErrs = append(listErrs, fmt.Errorf("分片 %d 上传失败: %w", n, err))
				mu.Unlock()
			}
		}(n)
	}
	go func() {
		wg.Wait()
		close(done)
	}()

	for reading := true; reading; {
		select {
		case <-done:
			reading = false
		default:
		}
		listed, err := s.ListParts("bkt", up.Key, up.UploadID, ListPartsOptions{MaxParts: 100})
		if err != nil {
			mu.Lock()
			listErrs = append(listErrs, fmt.Errorf("上传期间 ListParts 失败: %w", err))
			mu.Unlock()
			break
		}
		last := 0
		for _, p := range listed.Parts {
			if p.PartNumber <= last {
				mu.Lock()
				listErrs = append(listErrs, fmt.Errorf("分片编号未严格升序: %d 出现在 %d 之后", p.PartNumber, last))
				mu.Unlock()
				break
			}
			last = p.PartNumber
			if p.Size <= 0 || len(p.ETag) != 32 {
				mu.Lock()
				listErrs = append(listErrs, fmt.Errorf("分片 %d 状态不完整: size=%d etag=%q", p.PartNumber, p.Size, p.ETag))
				mu.Unlock()
				break
			}
		}
	}
	for _, err := range listErrs {
		t.Fatal(err)
	}

	listed, err := s.ListParts("bkt", up.Key, up.UploadID, ListPartsOptions{MaxParts: 100})
	if err != nil {
		t.Fatalf("上传结束后 ListParts 失败: %v", err)
	}
	if len(listed.Parts) != parts {
		t.Fatalf("上传结束后有 %d 个分片，期望 %d 个", len(listed.Parts), parts)
	}
}
