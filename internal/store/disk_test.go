package store

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// fakeDisk 是可变的容量读数，测试通过修改 free 模拟"盘被填满/被清理"。
type fakeDisk struct {
	total uint64
	free  uint64
	err   error
}

func (f *fakeDisk) space(string) (uint64, uint64, error) {
	if f.err != nil {
		return 0, 0, f.err
	}
	return f.total, f.free, nil
}

// newWatermarkStore 返回带注入容量读数的存储与控制器。
func newWatermarkStore(t *testing.T, high, low float64) (*Store, *fakeDisk) {
	t.Helper()
	disk := &fakeDisk{total: 100, free: 50}
	s, err := Open(Options{
		Root:          filepath.Join(t.TempDir(), "data"),
		HighWatermark: high,
		LowWatermark:  low,
		DiskSpace:     disk.space,
	})
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	return s, disk
}

// forceRefresh 让下一次水位检查立即重新测量，避免测试真的等 1 秒。
func forceRefresh(s *Store) {
	s.disk.mu.Lock()
	s.disk.checked = time.Time{}
	s.disk.mu.Unlock()
}

// put 写一个小对象，只关心错误，不关心返回的元数据。
func put(s *Store, key string) error {
	body := []byte("x")
	_, err := s.PutObject("bkt", key, bytes.NewReader(body), PutOptions{ExpectedSize: int64(len(body))})
	return err
}

// TestWatermarkBlocksAndRecovers 验证滞回：高水位阻断、低水位恢复。
//
// 只测"达到高水位拒写"是不够的：如果解除条件写成"低于高水位"，
// 使用率在 0.90 附近抖动时就会反复开关，写入吞吐出现锯齿。
func TestWatermarkBlocksAndRecovers(t *testing.T) {
	s, disk := newWatermarkStore(t, 0.90, 0.85)

	if err := s.CreateBucket("bkt"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := put(s, "before"); err != nil {
		t.Fatalf("未达水位时应可写: %v", err)
	}

	// 95% 已用：进入阻断。
	disk.free = 5
	forceRefresh(s)
	if err := put(s, "blocked"); !errors.Is(err, ErrStorageFull) {
		t.Fatalf("达到高水位应返回 ErrStorageFull，实际 %v", err)
	}
	if !s.WriteBlocked() {
		t.Error("WriteBlocked 应为 true")
	}

	// 88% 已用：高于低水位，仍应保持阻断（滞回）。
	disk.free = 12
	forceRefresh(s)
	if err := put(s, "still-blocked"); !errors.Is(err, ErrStorageFull) {
		t.Fatalf("回落到低水位之前应继续阻断，实际 %v", err)
	}

	// 80% 已用：低于低水位，恢复写入。
	disk.free = 20
	forceRefresh(s)
	if err := put(s, "after"); err != nil {
		t.Fatalf("低于低水位后应恢复写入: %v", err)
	}
	if s.WriteBlocked() {
		t.Error("恢复后 WriteBlocked 应为 false")
	}

	usage := s.DiskUsage()
	if !usage.Available || usage.Total != 100 || usage.Free != 20 {
		t.Errorf("DiskUsage = %+v，期望 total=100 free=20 且可用", usage)
	}
}

// TestWatermarkFailsOpenOnStatError 验证 statfs 失败时放行。
//
// 监控故障不应升级为写入故障：量不出剩余空间时，"可能写满"由
// 真实 ENOSPC 兜底，而拒绝一切写入是确定的服务中断。
func TestWatermarkFailsOpenOnStatError(t *testing.T) {
	s, disk := newWatermarkStore(t, 0.90, 0.85)
	if err := s.CreateBucket("bkt"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	disk.free = 1 // 先进入阻断
	forceRefresh(s)
	if err := put(s, "a"); !errors.Is(err, ErrStorageFull) {
		t.Fatalf("准备条件失败: %v", err)
	}

	disk.err = errors.New("statfs: broken")
	forceRefresh(s)
	if err := put(s, "bkt"); err != nil {
		t.Fatalf("statfs 失败时应 fail-open，实际 %v", err)
	}
	if s.WriteBlocked() {
		t.Error("statfs 失败后 WriteBlocked 应为 false")
	}
	if usage := s.DiskUsage(); usage.Available || usage.Err == nil {
		t.Errorf("statfs 失败时 DiskUsage 应标记不可用: %+v", usage)
	}
}

// TestWatermarkDisabled 验证 high=0 时不启用容量保护。
func TestWatermarkDisabled(t *testing.T) {
	disk := &fakeDisk{total: 100, free: 0}
	s, err := Open(Options{
		Root:      filepath.Join(t.TempDir(), "data"),
		DiskSpace: disk.space,
	})
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	if err := s.CreateBucket("bkt"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := put(s, "k"); err != nil {
		t.Fatalf("未启用保护时不应拦截写入: %v", err)
	}
	if s.WriteBlocked() {
		t.Error("未启用保护时 WriteBlocked 必须为 false")
	}
}

// TestWatermarkOptionValidation 验证启动期暴露非法水位组合。
func TestWatermarkOptionValidation(t *testing.T) {
	cases := []struct {
		name      string
		high, low float64
	}{
		{"高水位越界", 1.5, 0.85},
		{"低水位不小于高水位", 0.9, 0.9},
		{"低水位非正", 0.9, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Open(Options{
				Root:          filepath.Join(t.TempDir(), "data"),
				HighWatermark: c.high,
				LowWatermark:  c.low,
			})
			if err == nil {
				t.Fatal("非法水位组合应拒绝启动")
			}
		})
	}
}
