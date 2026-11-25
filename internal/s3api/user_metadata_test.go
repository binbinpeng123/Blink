package s3api

import (
	"encoding/xml"
	"net/http"
	"strings"
	"testing"

	"github.com/binbinpeng123/Blink/internal/s3err"
)

// metadataOfSize 构造一个「键 + 值」字节总和恰好为 total 的用户元数据。
func metadataOfSize(key string, total int) map[string]string {
	return map[string]string{key: strings.Repeat("v", total-len(key))}
}

// TestCheckUserMetadataSize 锁定 2 KB 上限的计数口径：UTF-8 字节数，
// 而非字符数；键本身计入总和。按字符计的话，多字节元数据可以把
// 实际存储量放大数倍而"看起来没超"。
func TestCheckUserMetadataSize(t *testing.T) {
	cases := []struct {
		name string
		meta map[string]string
		over bool
	}{
		{"nil", nil, false},
		{"只有键没有值", map[string]string{"owner": ""}, false},
		{"恰好 2048", metadataOfSize("k", maxUserMetadataBytes), false},
		{"多一字节", metadataOfSize("k", maxUserMetadataBytes+1), true},
		// 682 个汉字 = 2046 字节，加键 1 字节仍在上限内；
		// 683 个汉字 = 2049 字节，加键后 2050 必然超限。
		{"多字节恰好内", map[string]string{"k": strings.Repeat("中", 682)}, false},
		{"多字节超限", map[string]string{"k": strings.Repeat("中", 683)}, true},
		{"多键累加恰好", map[string]string{
			"a": strings.Repeat("x", 999),
			"b": strings.Repeat("y", 1047),
		}, false},
		{"多键累加超限", map[string]string{
			"a": strings.Repeat("x", 999),
			"b": strings.Repeat("y", 1048),
		}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := checkUserMetadataSize(c.meta)
			if c.over {
				if err == nil {
					t.Fatalf("应超限却没有报错：%+v", c.meta)
				}
				if err.Code != s3err.CodeMetadataTooLarge || err.HTTPStatus != http.StatusBadRequest {
					t.Fatalf("错误码 = %s/%d，期望 MetadataTooLarge/400", err.Code, err.HTTPStatus)
				}
				return
			}
			if err != nil {
				t.Fatalf("未超限却报错：%v", err)
			}
		})
	}
}

// TestPutObjectUserMetadataLimitHTTP 验证超限请求在落盘之前被拒绝：
// 状态码与错误码对，且不得留下半写入的对象。
func TestPutObjectUserMetadataLimitHTTP(t *testing.T) {
	h := objectTestServer(t)

	rec := putObject(t, h, "/bkt/big.txt", "hello", func(r *http.Request) {
		r.Header.Set("x-amz-meta-blob", strings.Repeat("v", maxUserMetadataBytes))
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，期望 400：%s", rec.Code, rec.Body.String())
	}
	if body := decodeError(t, rec); body.Code != string(s3err.CodeMetadataTooLarge) {
		t.Fatalf("错误码 = %s，期望 MetadataTooLarge", body.Code)
	}
	if got := do(t, h, http.MethodGet, "/bkt/big.txt"); got.Code != http.StatusNotFound {
		t.Fatalf("超限请求后对象仍可读：GET = %d", got.Code)
	}
}

// TestPutObjectUserMetadataBoundaryHTTP 验证边界值恰好 2048 字节时正常写入，
// 且读回时元数据完整——上限不能因为"留余量"而被悄悄改小。
func TestPutObjectUserMetadataBoundaryHTTP(t *testing.T) {
	h := objectTestServer(t)
	value := strings.Repeat("v", maxUserMetadataBytes-1) // 键 "k" 1 字节 + 值 2047 字节

	rec := putObject(t, h, "/bkt/exact.txt", "hello", func(r *http.Request) {
		r.Header.Set("x-amz-meta-k", value)
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("边界值被拒绝：%d %s", rec.Code, rec.Body.String())
	}
	got := do(t, h, http.MethodGet, "/bkt/exact.txt")
	if got.Code != http.StatusOK {
		t.Fatalf("GET = %d", got.Code)
	}
	// 响应用小写头名（见 handler_object_read.go），Header.Get 会规范化成
	// X-Amz-Meta-K 而查不到，必须直接按键取。
	if v := got.Header()["x-amz-meta-k"]; len(v) != 1 || v[0] != value {
		t.Fatalf("读回的元数据 = %q，期望长度 %d", v, len(value))
	}
}

// TestCopyObjectUserMetadataLimitHTTP 覆盖两个方向：
// REPLACE 指令下的请求元数据会落盘，必须校验；COPY 指令下的
// x-amz-meta-* 会被整体丢弃，为它报错只会让复制莫名失败。
func TestCopyObjectUserMetadataLimitHTTP(t *testing.T) {
	h := objectTestServer(t)
	if rec := putObject(t, h, "/bkt/src.txt", "payload", func(r *http.Request) {
		r.Header.Set("x-amz-meta-owner", "edge-01")
	}); rec.Code != http.StatusOK {
		t.Fatalf("准备源对象失败：%d %s", rec.Code, rec.Body.String())
	}

	oversize := strings.Repeat("v", maxUserMetadataBytes*4)
	rec := putObject(t, h, "/bkt/dst-replace.txt", "", func(r *http.Request) {
		r.Header.Set("x-amz-copy-source", "/bkt/src.txt")
		r.Header.Set("x-amz-metadata-directive", "REPLACE")
		r.Header.Set("x-amz-meta-blob", oversize)
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("REPLACE 超限状态码 = %d，期望 400：%s", rec.Code, rec.Body.String())
	}
	if body := decodeError(t, rec); body.Code != string(s3err.CodeMetadataTooLarge) {
		t.Fatalf("错误码 = %s，期望 MetadataTooLarge", body.Code)
	}
	if got := do(t, h, http.MethodGet, "/bkt/dst-replace.txt"); got.Code != http.StatusNotFound {
		t.Fatalf("被拒绝的复制留下了目标对象：GET = %d", got.Code)
	}

	// 缺省 COPY：超限头被忽略，复制成功，目标保留源对象的元数据。
	rec = putObject(t, h, "/bkt/dst-copy.txt", "", func(r *http.Request) {
		r.Header.Set("x-amz-copy-source", "/bkt/src.txt")
		r.Header.Set("x-amz-meta-blob", oversize)
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("COPY 状态码 = %d，期望 200：%s", rec.Code, rec.Body.String())
	}
	got := do(t, h, http.MethodGet, "/bkt/dst-copy.txt")
	if got.Code != http.StatusOK {
		t.Fatalf("GET 目标 = %d", got.Code)
	}
	if v := got.Header()["x-amz-meta-owner"]; len(v) != 1 || v[0] != "edge-01" {
		t.Errorf("COPY 未保留源元数据：x-amz-meta-owner = %q", v)
	}
	if v := got.Header()["x-amz-meta-blob"]; len(v) != 0 {
		t.Errorf("COPY 不应把被丢弃的请求元数据写入目标：x-amz-meta-blob = %d 个值", len(v))
	}
}

// TestCreateMultipartUploadUserMetadataLimitHTTP 验证超限时拒绝创建会话：
// 先建会话再报错，客户端要到 UploadPart 才收到 NoSuchUpload，错误现场
// 与真实原因脱节。
func TestCreateMultipartUploadUserMetadataLimitHTTP(t *testing.T) {
	h := mpuTestServer(t)

	rec := mpuRequest(t, h, http.MethodPost, "/bkt/big.bin?uploads", nil, func(r *http.Request) {
		r.Header.Set("x-amz-meta-blob", strings.Repeat("v", maxUserMetadataBytes))
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，期望 400：%s", rec.Code, rec.Body.String())
	}
	if body := decodeError(t, rec); body.Code != string(s3err.CodeMetadataTooLarge) {
		t.Fatalf("错误码 = %s，期望 MetadataTooLarge", body.Code)
	}

	// 被拒绝的 Initiate 不得留下任何会话。
	rec = mpuRequest(t, h, http.MethodGet, "/bkt?uploads", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("ListMultipartUploads = %d：%s", rec.Code, rec.Body.String())
	}
	var list listMultipartUploadsResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("列表响应不是合法 XML：%v (%s)", err, rec.Body.String())
	}
	if len(list.Uploads) != 0 {
		t.Fatalf("超限的 Initiate 留下了 %d 个会话", len(list.Uploads))
	}
}
