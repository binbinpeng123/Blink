package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
)

// CORSRule 是桶级 CORS 配置中的一条规则。
//
// 字段与 S3 的 XML 结构一一对应，但这里用 JSON 持久化：配置只在
// PUT/GET ?cors 两个端点上出现，JSON 的可读性更利于运维直接看盘。
// 校验（方法白名单、通配符数量、规则上限）属于协议层，由 internal/s3api
// 负责；存储层只保证"写进去什么、读出来还是什么"。
type CORSRule struct {
	ID             string   `json:"id,omitempty"`
	AllowedOrigins []string `json:"allowed_origins"`
	AllowedMethods []string `json:"allowed_methods"`
	AllowedHeaders []string `json:"allowed_headers,omitempty"`
	ExposeHeaders  []string `json:"expose_headers,omitempty"`
	// MaxAgeSeconds 为 nil 表示未设置，响应中不应出现 Access-Control-Max-Age。
	MaxAgeSeconds *int `json:"max_age_seconds,omitempty"`
}

// CORSConfiguration 是 <bucket>/cors.json 的内容。
type CORSConfiguration struct {
	Rules []CORSRule `json:"rules"`
}

// PutBucketCORS 原子地替换桶的 CORS 配置。
//
// 整份替换而非合并：S3 的 PutBucketCors 语义就是"用请求体覆盖已有配置"，
// 合并会让客户端无法删除某条规则。
func (s *Store) PutBucketCORS(bucket string, cfg *CORSConfiguration) error {
	if cfg == nil {
		return errors.New("store: CORS 配置为空")
	}
	if _, err := s.loadBucketMeta(bucket); err != nil {
		return err
	}
	// 深拷贝后再写入并缓存：调用方（s3api）可能在写完继续复用或修改
	// 这份结构，共享指针会让缓存内容与磁盘内容悄悄分叉。
	saved := cloneCORSConfiguration(cfg)
	if err := s.writeFileAtomic(s.bucketCORSPath(bucket), mustJSON(saved)); err != nil {
		return fsErr("写入 CORS 配置", bucket, err)
	}
	s.cacheBucketCORS(bucket, saved)
	return nil
}

// GetBucketCORS 返回桶的 CORS 配置。
//
// 桶不存在返回 ErrBucketNotFound；桶存在但未配置返回
// ErrNoSuchCORSConfiguration —— 两者在 S3 中是不同的错误码。
// 返回值与缓存共享，调用方只读。
func (s *Store) GetBucketCORS(bucket string) (*CORSConfiguration, error) {
	if _, err := s.loadBucketMeta(bucket); err != nil {
		return nil, err
	}
	if cfg := s.cachedBucketCORS(bucket); cfg != nil {
		return cfg, nil
	}
	data, err := readFile(s.fs, s.bucketCORSPath(bucket))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNoSuchCORSConfiguration
	}
	if err != nil {
		return nil, fsErr("读取 CORS 配置", bucket, err)
	}
	var cfg CORSConfiguration
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("%w: 桶 %s 的 %s 不是合法 JSON: %v",
			ErrCorrupt, bucket, bucketCORSFileName, err)
	}
	s.cacheBucketCORS(bucket, &cfg)
	return &cfg, nil
}

// DeleteBucketCORS 删除桶的 CORS 配置。
//
// 幂等：从未配置过也返回成功（S3 的 DeleteBucketCors 同样如此）。
// 桶不存在仍返回 ErrBucketNotFound —— 这一项不幂等，与 DeleteObject
// 对不存在桶的处理保持一致。
func (s *Store) DeleteBucketCORS(bucket string) error {
	if _, err := s.loadBucketMeta(bucket); err != nil {
		return err
	}
	if err := s.fs.Remove(s.bucketCORSPath(bucket)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fsErr("删除 CORS 配置", bucket, err)
	}
	// 先让缓存失效再 sync：即使 sync 失败，后续请求也不该继续命中
	// 一份已经不在磁盘上的配置。
	s.dropBucketCORS(bucket)
	return s.fs.SyncDir(s.bucketDir(bucket))
}

func (s *Store) cachedBucketCORS(bucket string) *CORSConfiguration {
	s.corsMu.RLock()
	defer s.corsMu.RUnlock()
	return s.cors[bucket]
}

func (s *Store) cacheBucketCORS(bucket string, cfg *CORSConfiguration) {
	s.corsMu.Lock()
	defer s.corsMu.Unlock()
	s.cors[bucket] = cfg
}

// dropBucketCORS 让缓存失效，供 DeleteBucketCORS 与 DeleteBucket 调用。
func (s *Store) dropBucketCORS(bucket string) {
	s.corsMu.Lock()
	defer s.corsMu.Unlock()
	delete(s.cors, bucket)
}

// cloneCORSConfiguration 深拷贝配置，切断调用方与缓存之间的共享切片。
func cloneCORSConfiguration(in *CORSConfiguration) *CORSConfiguration {
	out := &CORSConfiguration{Rules: make([]CORSRule, len(in.Rules))}
	for i, r := range in.Rules {
		out.Rules[i] = CORSRule{
			ID:             r.ID,
			AllowedOrigins: append([]string(nil), r.AllowedOrigins...),
			AllowedMethods: append([]string(nil), r.AllowedMethods...),
			AllowedHeaders: append([]string(nil), r.AllowedHeaders...),
			ExposeHeaders:  append([]string(nil), r.ExposeHeaders...),
		}
		if r.MaxAgeSeconds != nil {
			v := *r.MaxAgeSeconds
			out.Rules[i].MaxAgeSeconds = &v
		}
	}
	return out
}
