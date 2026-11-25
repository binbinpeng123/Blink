//go:build integration

package integration

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// TestUserMetadataLimit 在真实二进制上验证 2 KB 用户元数据上限：
// 超限的写请求必须在落盘或建会话之前被拒，边界值必须原样接受。
func TestUserMetadataLimit(t *testing.T) {
	srv := startServer(t, serverOptions{})
	c := newClient(t, srv)

	bucket := uniqueBucket("meta")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)

	t.Run("PutObject 超限", func(t *testing.T) {
		res := c.putObject(bucket, "big.txt", []byte("hello"), map[string]string{
			"x-amz-meta-blob": strings.Repeat("v", 2048), // 加键 4 字节后 2052
		})
		requireS3Error(t, res, http.StatusBadRequest, "MetadataTooLarge")
		// 被拒绝的请求不得留下对象。
		requireStatus(t, c.getObject(bucket, "big.txt", nil), http.StatusNotFound)
	})

	t.Run("PutObject 边界值", func(t *testing.T) {
		// 键 "k" 1 字节 + 值 2047 字节 = 恰好 2048。
		value := strings.Repeat("v", 2047)
		res := c.putObject(bucket, "exact.txt", []byte("hello"), map[string]string{
			"x-amz-meta-k": value,
		})
		requireStatus(t, res, http.StatusOK)

		got := c.getObject(bucket, "exact.txt", nil)
		requireStatus(t, got, http.StatusOK)
		// 服务端以小写头名回传；Go 客户端解析响应时会规范化键，
		// 因此这里用 Get 即可（与服务端侧 httptest 直接读 map 不同）。
		if v := got.Header.Get("x-amz-meta-k"); v != value {
			t.Fatalf("读回的 x-amz-meta-k 长度 = %d，期望 %d", len(v), len(value))
		}
	})

	t.Run("CreateMultipartUpload 超限", func(t *testing.T) {
		res := c.do(http.MethodPost, c.objectURL(bucket, "big.bin"),
			url.Values{"uploads": {""}}, nil, map[string]string{
				"x-amz-meta-blob": strings.Repeat("v", 2048),
			})
		requireS3Error(t, res, http.StatusBadRequest, "MetadataTooLarge")

		// 被拒绝的 Initiate 不得留下会话。
		list := c.do(http.MethodGet, c.bucketURL(bucket), url.Values{"uploads": {""}}, nil, nil)
		requireStatus(t, list, http.StatusOK)
		if body := string(list.Body); strings.Contains(body, "<Upload>") {
			t.Fatalf("超限的 Initiate 留下了会话：%s", truncate(list.Body))
		}
	})

	t.Run("CopyObject REPLACE 超限", func(t *testing.T) {
		requireStatus(t, c.putObject(bucket, "src.txt", []byte("payload"), nil), http.StatusOK)

		res := c.putObject(bucket, "dst.txt", nil, map[string]string{
			"x-amz-copy-source":        "/" + bucket + "/src.txt",
			"x-amz-metadata-directive": "REPLACE",
			"x-amz-meta-blob":          strings.Repeat("v", 2048),
		})
		requireS3Error(t, res, http.StatusBadRequest, "MetadataTooLarge")
		requireStatus(t, c.getObject(bucket, "dst.txt", nil), http.StatusNotFound)
	})
}
