//go:build integration

package integration

import (
	"crypto/md5"
	"encoding/hex"
	"net/http"
	"testing"
)

// TestObjectRoundTrip 覆盖单次 PUT/GET/HEAD/DELETE 的主路径，
// 以及首字节链路上的关键响应头（ETag、Content-Type、用户元数据）。
func TestObjectRoundTrip(t *testing.T) {
	srv := startServer(t, serverOptions{})
	c := newClient(t, srv)

	bucket := uniqueBucket("object")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)

	body := bodyOf(128*1024, 1) // 超过默认内联阈值，走 CAS 块路径
	wantETag := hex.EncodeToString(func() []byte {
		sum := md5.Sum(body)
		return sum[:]
	}())

	res := c.putObject(bucket, "dir/hello.bin", body, map[string]string{
		"Content-Type":       "application/x-blink-test",
		"x-amz-meta-owner":   "integration",
		"x-amz-meta-purpose": "roundtrip",
	})
	requireStatus(t, res, http.StatusOK)
	if got := unquoteETag(res.Header.Get("ETag")); got != wantETag {
		t.Fatalf("PUT ETag = %q，期望 %q", got, wantETag)
	}

	t.Run("GET 内容与响应头", func(t *testing.T) {
		got := c.getObject(bucket, "dir/hello.bin", nil)
		requireStatus(t, got, http.StatusOK)
		if string(got.Body) != string(body) {
			t.Fatalf("GET 内容不一致：长度 %d vs %d", len(got.Body), len(body))
		}
		if ct := got.Header.Get("Content-Type"); ct != "application/x-blink-test" {
			t.Fatalf("Content-Type = %q", ct)
		}
		if mt := got.Header.Get("x-amz-meta-owner"); mt != "integration" {
			t.Fatalf("x-amz-meta-owner = %q", mt)
		}
		if mt := got.Header.Get("x-amz-meta-purpose"); mt != "roundtrip" {
			t.Fatalf("x-amz-meta-purpose = %q", mt)
		}
		if ar := got.Header.Get("Accept-Ranges"); ar != "bytes" {
			t.Fatalf("Accept-Ranges = %q，期望 bytes", ar)
		}
		if sc := got.Header.Get("x-amz-storage-class"); sc != "STANDARD" {
			t.Fatalf("x-amz-storage-class = %q，期望 STANDARD", sc)
		}
		if got.Header.Get("Content-Length") == "" {
			t.Fatalf("缺少显式 Content-Length")
		}
		if unquoteETag(got.Header.Get("ETag")) != wantETag {
			t.Fatalf("GET ETag = %q，期望 %q", got.Header.Get("ETag"), wantETag)
		}
	})

	t.Run("HEAD 与 GET 元数据一致", func(t *testing.T) {
		got := c.headObject(bucket, "dir/hello.bin", nil)
		requireStatus(t, got, http.StatusOK)
		if len(got.Body) != 0 {
			t.Fatalf("HEAD 不应有响应体，得到 %d 字节", len(got.Body))
		}
		if unquoteETag(got.Header.Get("ETag")) != wantETag {
			t.Fatalf("HEAD ETag = %q，期望 %q", got.Header.Get("ETag"), wantETag)
		}
	})

	t.Run("覆盖写生成新版本", func(t *testing.T) {
		replacement := []byte("second version, shorter")
		requireStatus(t, c.putObject(bucket, "dir/hello.bin", replacement, nil), http.StatusOK)

		got := c.getObject(bucket, "dir/hello.bin", nil)
		requireStatus(t, got, http.StatusOK)
		if string(got.Body) != string(replacement) {
			t.Fatalf("覆盖写后内容 = %q", got.Body)
		}
		if unquoteETag(got.Header.Get("ETag")) == wantETag {
			t.Fatalf("覆盖写后 ETag 未变化")
		}
	})

	t.Run("零字节对象 ETag", func(t *testing.T) {
		requireStatus(t, c.putObject(bucket, "empty", nil, nil), http.StatusOK)
		got := c.getObject(bucket, "empty", nil)
		requireStatus(t, got, http.StatusOK)
		if len(got.Body) != 0 {
			t.Fatalf("零字节对象返回 %d 字节", len(got.Body))
		}
		const emptyMD5 = "d41d8cd98f00b204e9800998ecf8427e"
		if unquoteETag(got.Header.Get("ETag")) != emptyMD5 {
			t.Fatalf("空对象 ETag = %q，期望 %q", got.Header.Get("ETag"), emptyMD5)
		}
	})

	t.Run("删除幂等", func(t *testing.T) {
		requireStatus(t, c.deleteObject(bucket, "dir/hello.bin"), http.StatusNoContent)
		// 第二次删除同样 204：S3 的 DeleteObject 是幂等的。
		requireStatus(t, c.deleteObject(bucket, "dir/hello.bin"), http.StatusNoContent)

		got := c.getObject(bucket, "dir/hello.bin", nil)
		requireS3Error(t, got, http.StatusNotFound, "NoSuchKey")
	})
}

// TestObjectErrors 覆盖 PUT 的头部校验与不存在资源的错误码。
func TestObjectErrors(t *testing.T) {
	srv := startServer(t, serverOptions{})
	c := newClient(t, srv)

	bucket := uniqueBucket("objerr")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)

	t.Run("Content-MD5 格式非法", func(t *testing.T) {
		res := c.putObject(bucket, "bad-md5-format", []byte("data"), map[string]string{
			"Content-MD5": "not-a-base64-digest",
		})
		requireS3Error(t, res, http.StatusBadRequest, "InvalidDigest")
	})

	t.Run("Content-MD5 内容不匹配", func(t *testing.T) {
		res := c.putObject(bucket, "bad-md5-value", []byte("actual"), map[string]string{
			"Content-MD5": contentMD5([]byte("expected")),
		})
		requireS3Error(t, res, http.StatusBadRequest, "BadDigest")
	})

	t.Run("Content-MD5 正确时放行", func(t *testing.T) {
		body := []byte("md5 verified")
		res := c.putObject(bucket, "good-md5", body, map[string]string{
			"Content-MD5": contentMD5(body),
		})
		requireStatus(t, res, http.StatusOK)
	})

	t.Run("不支持的存储类型返回 501", func(t *testing.T) {
		res := c.putObject(bucket, "glacier", []byte("x"), map[string]string{
			"x-amz-storage-class": "GLACIER",
		})
		requireS3Error(t, res, http.StatusNotImplemented, "NotImplemented")
	})

	t.Run("If-None-Match: * 对已存在对象返回 412", func(t *testing.T) {
		requireStatus(t, c.putObject(bucket, "existing", []byte("v1"), nil), http.StatusOK)
		res := c.putObject(bucket, "existing", []byte("v2"), map[string]string{
			"If-None-Match": "*",
		})
		requireS3Error(t, res, http.StatusPreconditionFailed, "PreconditionFailed")
	})

	t.Run("If-None-Match 的显式 ETag 条件写", func(t *testing.T) {
		first := c.putObject(bucket, "cond-etag", []byte("v1"), nil)
		requireStatus(t, first, http.StatusOK)
		etag := first.Header.Get("ETag")

		// 当前值正是客户端手里的 ETag → 412，且内容不变。
		res := c.putObject(bucket, "cond-etag", []byte("v2"), map[string]string{
			"If-None-Match": etag,
		})
		requireS3Error(t, res, http.StatusPreconditionFailed, "PreconditionFailed")
		if res := c.getObject(bucket, "cond-etag", nil); string(res.Body) != "v1" {
			t.Errorf("失败的条件写改动了内容: %q", res.Body)
		}

		// ETag 不是当前版本 → 放行。
		requireStatus(t, c.putObject(bucket, "cond-etag", []byte("v2"), map[string]string{
			"If-None-Match": `"deadbeef"`,
		}), http.StatusOK)

		// 对象不存在时不匹配任何 ETag → 放行。
		requireStatus(t, c.putObject(bucket, "cond-fresh", []byte("x"), map[string]string{
			"If-None-Match": etag,
		}), http.StatusOK)
	})

	t.Run("If-Match 条件写按 ETag 生效", func(t *testing.T) {
		first := c.putObject(bucket, "cas", []byte("v1"), nil)
		requireStatus(t, first, http.StatusOK)
		etag := first.Header.Get("ETag")

		requireStatus(t, c.putObject(bucket, "cas", []byte("v2"), map[string]string{
			"If-Match": etag,
		}), http.StatusOK)

		// 覆盖之后旧 ETag 不再成立。
		res := c.putObject(bucket, "cas", []byte("v3"), map[string]string{
			"If-Match": etag,
		})
		requireS3Error(t, res, http.StatusPreconditionFailed, "PreconditionFailed")

		// 对象不存在时 `*` 同样不成立。
		res = c.putObject(bucket, "cas-missing", []byte("x"), map[string]string{
			"If-Match": "*",
		})
		requireS3Error(t, res, http.StatusPreconditionFailed, "PreconditionFailed")
	})

	t.Run("不存在的桶与对象", func(t *testing.T) {
		res := c.getObject(uniqueBucket("missing"), "whatever", nil)
		requireS3Error(t, res, http.StatusNotFound, "NoSuchBucket")

		res = c.getObject(bucket, "does-not-exist", nil)
		requireS3Error(t, res, http.StatusNotFound, "NoSuchKey")

		res = c.headObject(bucket, "does-not-exist", nil)
		requireStatus(t, res, http.StatusNotFound)
	})

	t.Run("未实现子资源返回 501", func(t *testing.T) {
		res := c.do(http.MethodGet, c.objectURL(bucket, "good-md5"), nil, nil, map[string]string{})
		requireStatus(t, res, http.StatusOK)

		res = c.do(http.MethodGet, c.objectURL(bucket, "good-md5"), map[string][]string{"acl": {""}}, nil, nil)
		requireS3Error(t, res, http.StatusNotImplemented, "NotImplemented")
	})
}
