package store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// sampleCORS 返回一份用于往返测试的配置。
func sampleCORS() *CORSConfiguration {
	maxAge := 3600
	return &CORSConfiguration{Rules: []CORSRule{{
		ID:             "rule-1",
		AllowedOrigins: []string{"https://console.example.com", "http://localhost:5173"},
		AllowedMethods: []string{"GET", "PUT"},
		AllowedHeaders: []string{"*"},
		ExposeHeaders:  []string{"ETag", "x-amz-request-id"},
		MaxAgeSeconds:  &maxAge,
	}}}
}

func TestCORSConfigurationRoundTrip(t *testing.T) {
	s := newTestStore(t)
	if err := s.CreateBucket("bkt"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := s.PutBucketCORS("bkt", sampleCORS()); err != nil {
		t.Fatalf("PutBucketCORS: %v", err)
	}

	got, err := s.GetBucketCORS("bkt")
	if err != nil {
		t.Fatalf("GetBucketCORS: %v", err)
	}
	if len(got.Rules) != 1 || got.Rules[0].ID != "rule-1" {
		t.Fatalf("规则往返失败: %+v", got.Rules)
	}
	if got.Rules[0].MaxAgeSeconds == nil || *got.Rules[0].MaxAgeSeconds != 3600 {
		t.Errorf("MaxAgeSeconds 往返失败: %+v", got.Rules[0].MaxAgeSeconds)
	}

	// 重新打开（模拟重启）后必须仍能读到：证明配置真的落了盘，
	// 而不是只活在进程内缓存里。
	s2, err := Open(Options{Root: s.Root()})
	if err != nil {
		t.Fatalf("重新 Open: %v", err)
	}
	got2, err := s2.GetBucketCORS("bkt")
	if err != nil {
		t.Fatalf("重启后 GetBucketCORS: %v", err)
	}
	if len(got2.Rules[0].AllowedOrigins) != 2 ||
		got2.Rules[0].AllowedOrigins[0] != "https://console.example.com" {
		t.Errorf("重启后配置不一致: %+v", got2.Rules[0])
	}
}

// TestCORSNotConfigured 区分"桶不存在"与"桶存在但没配 CORS"。
func TestCORSNotConfigured(t *testing.T) {
	s := newTestStore(t)
	if err := s.CreateBucket("bkt"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	if _, err := s.GetBucketCORS("bkt"); !errors.Is(err, ErrNoSuchCORSConfiguration) {
		t.Errorf("未配置时应返回 ErrNoSuchCORSConfiguration，实际 %v", err)
	}
	if _, err := s.GetBucketCORS("missing"); !errors.Is(err, ErrBucketNotFound) {
		t.Errorf("桶不存在时应返回 ErrBucketNotFound，实际 %v", err)
	}
	if err := s.PutBucketCORS("missing", sampleCORS()); !errors.Is(err, ErrBucketNotFound) {
		t.Errorf("向不存在的桶写配置应返回 ErrBucketNotFound，实际 %v", err)
	}
}

// TestCORSDeleteIdempotent 验证删除语义：从未配置也成功，删除后回到未配置。
func TestCORSDeleteIdempotent(t *testing.T) {
	s := newTestStore(t)
	if err := s.CreateBucket("bkt"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := s.DeleteBucketCORS("bkt"); err != nil {
		t.Fatalf("首次删除应幂等成功: %v", err)
	}
	if err := s.PutBucketCORS("bkt", sampleCORS()); err != nil {
		t.Fatalf("PutBucketCORS: %v", err)
	}
	if err := s.DeleteBucketCORS("bkt"); err != nil {
		t.Fatalf("DeleteBucketCORS: %v", err)
	}
	if _, err := s.GetBucketCORS("bkt"); !errors.Is(err, ErrNoSuchCORSConfiguration) {
		t.Errorf("删除后应回到未配置，实际 %v", err)
	}
	if _, err := os.Stat(filepath.Join(s.bucketDir("bkt"), bucketCORSFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("配置文件应已删除，stat 结果 %v", err)
	}
}

// TestCORSDeleteBucketDropsCache 验证删桶后 CORS 缓存不再命中。
func TestCORSDeleteBucketDropsCache(t *testing.T) {
	s := newTestStore(t)
	if err := s.CreateBucket("bkt"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := s.PutBucketCORS("bkt", sampleCORS()); err != nil {
		t.Fatalf("PutBucketCORS: %v", err)
	}
	if err := s.DeleteBucket("bkt"); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	// 重建同名桶：旧配置绝不能"复活"。
	if err := s.CreateBucket("bkt"); err != nil {
		t.Fatalf("重建桶: %v", err)
	}
	if _, err := s.GetBucketCORS("bkt"); !errors.Is(err, ErrNoSuchCORSConfiguration) {
		t.Errorf("重建后的桶不应继承旧配置，实际 %v", err)
	}
}

// TestCORSPutIsDeepCopy 验证写入后修改入参不会影响已存的配置。
func TestCORSPutIsDeepCopy(t *testing.T) {
	s := newTestStore(t)
	if err := s.CreateBucket("bkt"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	cfg := sampleCORS()
	if err := s.PutBucketCORS("bkt", cfg); err != nil {
		t.Fatalf("PutBucketCORS: %v", err)
	}
	cfg.Rules[0].AllowedOrigins[0] = "https://evil.example.com"
	*cfg.Rules[0].MaxAgeSeconds = 0

	got, err := s.GetBucketCORS("bkt")
	if err != nil {
		t.Fatalf("GetBucketCORS: %v", err)
	}
	if got.Rules[0].AllowedOrigins[0] != "https://console.example.com" {
		t.Errorf("修改入参影响了存储: %v", got.Rules[0].AllowedOrigins)
	}
	if *got.Rules[0].MaxAgeSeconds != 3600 {
		t.Errorf("MaxAgeSeconds 被入参篡改: %d", *got.Rules[0].MaxAgeSeconds)
	}
}

// TestCORSCorruptFile 验证损坏的配置文件返回 ErrCorrupt 而不是"未配置"。
func TestCORSCorruptFile(t *testing.T) {
	s := newTestStore(t)
	if err := s.CreateBucket("bkt"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := os.WriteFile(filepath.Join(s.bucketDir("bkt"), bucketCORSFileName), []byte("{not json"), 0o640); err != nil {
		t.Fatalf("写坏文件: %v", err)
	}
	if _, err := s.GetBucketCORS("bkt"); !errors.Is(err, ErrCorrupt) {
		t.Errorf("损坏的配置应返回 ErrCorrupt，实际 %v", err)
	}
}
