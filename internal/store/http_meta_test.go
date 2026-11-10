package store

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// httpMetaFixture 是一组非空的标准 HTTP 元数据，用于各条写路径的往返断言。
func httpMetaFixture() HTTPMeta {
	return HTTPMeta{
		CacheControl:       "public, max-age=3600",
		ContentDisposition: `attachment; filename="weights.bin"`,
		ContentEncoding:    "gzip",
		ContentLanguage:    "zh-CN",
		Expires:            "Wed, 21 Oct 2026 07:28:00 GMT",
	}
}

func requireHTTPMeta(t *testing.T, got, want HTTPMeta) {
	t.Helper()
	if got != want {
		t.Errorf("HTTPMeta = %+v，期望 %+v", got, want)
	}
}

// 标准 HTTP 元数据必须随 meta.json 落盘，并从读取路径原样返回。
func TestPutObjectPersistsHTTPMeta(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)
	want := httpMetaFixture()

	info, err := s.PutObject("bkt", "model.bin", bytes.NewReader([]byte("x")), PutOptions{
		ExpectedSize: 1,
		HTTPMeta:     want,
	})
	if err != nil {
		t.Fatalf("PutObject 失败: %v", err)
	}
	requireHTTPMeta(t, info.HTTPMeta, want)

	meta, err := s.loadObjectMeta("bkt", "model.bin")
	if err != nil {
		t.Fatalf("loadObjectMeta 失败: %v", err)
	}
	requireHTTPMeta(t, meta.HTTPMeta, want)

	r, err := s.OpenObject("bkt", "model.bin")
	if err != nil {
		t.Fatalf("OpenObject 失败: %v", err)
	}
	requireHTTPMeta(t, r.Info().HTTPMeta, want)

	// JSON 必须是扁平字段：分片会话与对象元数据共用同一组字段名，
	// 嵌套结构会让旧版本兼容的前提（未知字段忽略）失效。
	raw, err := os.ReadFile(s.objectMetaPath("bkt", "model.bin"))
	if err != nil {
		t.Fatalf("读取 meta.json 失败: %v", err)
	}
	for _, field := range []string{
		`"cache_control"`, `"content_disposition"`, `"content_encoding"`,
		`"content_language"`, `"expires"`,
	} {
		if !strings.Contains(string(raw), field) {
			t.Errorf("meta.json 缺少字段 %s: %s", field, raw)
		}
	}
}

// 空的标准 HTTP 元数据既不写盘，也不会在旧字段表里留下空键。
func TestPutObjectOmitsEmptyHTTPMeta(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)
	putString(t, s, "bkt", "plain.txt", "x")

	raw, err := os.ReadFile(s.objectMetaPath("bkt", "plain.txt"))
	if err != nil {
		t.Fatalf("读取 meta.json 失败: %v", err)
	}
	for _, field := range []string{
		"cache_control", "content_disposition", "content_encoding",
		"content_language", `"expires"`,
	} {
		if strings.Contains(string(raw), field) {
			t.Errorf("空元数据不应写盘，但 meta.json 含 %s: %s", field, raw)
		}
	}
}

// 旧版本写下的 meta.json 没有这些字段：解析后必须是零值而不是报错。
func TestObjectMetaLegacyWithoutHTTPMeta(t *testing.T) {
	const legacy = `{"version":1,"key":"old","size":0,` +
		`"etag":"d41d8cd98f00b204e9800998ecf8427e","content_type":"text/plain",` +
		`"modified":"2026-01-01T00:00:00Z","blocks":[]}`
	var m objectMeta
	if err := json.Unmarshal([]byte(legacy), &m); err != nil {
		t.Fatalf("解析旧 meta.json 失败: %v", err)
	}
	if !m.HTTPMeta.IsZero() {
		t.Errorf("旧 meta.json 的 HTTPMeta = %+v，期望零值", m.HTTPMeta)
	}
	if m.ContentType != "text/plain" {
		t.Errorf("已有字段被破坏: ContentType = %q", m.ContentType)
	}
}

// COPY 指令保留源对象的标准 HTTP 元数据；REPLACE 完整替换，缺省即清空。
func TestCopyObjectHTTPMetaDirectives(t *testing.T) {
	s := newSmallBlockStore(t, 64, 16)
	src := httpMetaFixture()
	if _, err := s.PutObject("bkt", "src.bin", bytes.NewReader([]byte("payload")), PutOptions{
		ExpectedSize: int64(len("payload")),
		HTTPMeta:     src,
	}); err != nil {
		t.Fatalf("写入源对象失败: %v", err)
	}

	t.Run("COPY 保留源元数据", func(t *testing.T) {
		info, err := s.CopyObject("bkt", "copy.bin", "bkt", "src.bin", CopyOptions{})
		if err != nil {
			t.Fatalf("CopyObject 失败: %v", err)
		}
		requireHTTPMeta(t, info.HTTPMeta, src)
		meta, err := s.loadObjectMeta("bkt", "copy.bin")
		if err != nil {
			t.Fatalf("loadObjectMeta 失败: %v", err)
		}
		requireHTTPMeta(t, meta.HTTPMeta, src)
	})

	t.Run("REPLACE 替换为新值", func(t *testing.T) {
		replaced := HTTPMeta{
			CacheControl:    "no-store",
			ContentEncoding: "br",
		}
		info, err := s.CopyObject("bkt", "replace.bin", "bkt", "src.bin", CopyOptions{
			ReplaceMetadata: true,
			HTTPMeta:        replaced,
		})
		if err != nil {
			t.Fatalf("CopyObject 失败: %v", err)
		}
		requireHTTPMeta(t, info.HTTPMeta, replaced)
		meta, _ := s.loadObjectMeta("bkt", "replace.bin")
		requireHTTPMeta(t, meta.HTTPMeta, replaced)
	})

	t.Run("REPLACE 未提供的头被清空", func(t *testing.T) {
		info, err := s.CopyObject("bkt", "clear.bin", "bkt", "src.bin", CopyOptions{
			ReplaceMetadata: true,
		})
		if err != nil {
			t.Fatalf("CopyObject 失败: %v", err)
		}
		if !info.HTTPMeta.IsZero() {
			t.Errorf("REPLACE 未提供时应清空，得到 %+v", info.HTTPMeta)
		}
		meta, _ := s.loadObjectMeta("bkt", "clear.bin")
		if !meta.HTTPMeta.IsZero() {
			t.Errorf("落盘后仍残留 %+v", meta.HTTPMeta)
		}
	})
}

// Initiate 时声明的标准 HTTP 元数据保存在会话里，Complete 时写入对象。
func TestMultipartPersistsHTTPMeta(t *testing.T) {
	s := newSmallBlockStore(t, 64<<10, 16)
	want := httpMetaFixture()

	up, err := s.CreateMultipartUpload("bkt", "mpu.bin", CreateMultipartOptions{
		ContentType: "application/octet-stream",
		HTTPMeta:    want,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload 失败: %v", err)
	}
	requireHTTPMeta(t, up.HTTPMeta, want)

	// 会话状态同样扁平落盘，集群副本才能原样搬运。
	raw, err := os.ReadFile(s.multipartStatePath(up.UploadID))
	if err != nil {
		t.Fatalf("读取 upload.json 失败: %v", err)
	}
	if !strings.Contains(string(raw), `"cache_control"`) {
		t.Errorf("upload.json 缺少 cache_control: %s", raw)
	}

	part := mpuBytes(minPartSize, 0x33)
	pi := uploadPart(t, s, "mpu.bin", up.UploadID, 1, part, UploadPartOptions{})

	info, err := s.CompleteMultipartUpload("bkt", "mpu.bin", up.UploadID,
		[]CompletePart{{1, pi.ETag}}, "", false)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload 失败: %v", err)
	}
	requireHTTPMeta(t, info.HTTPMeta, want)

	meta, err := s.loadObjectMeta("bkt", "mpu.bin")
	if err != nil {
		t.Fatalf("loadObjectMeta 失败: %v", err)
	}
	requireHTTPMeta(t, meta.HTTPMeta, want)
}

func TestHTTPMetaIsZero(t *testing.T) {
	if !(HTTPMeta{}).IsZero() {
		t.Error("零值应报告 IsZero")
	}
	if httpMetaFixture().IsZero() {
		t.Error("非零值不应报告 IsZero")
	}
}
