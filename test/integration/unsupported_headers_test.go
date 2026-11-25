//go:build integration

package integration

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// TestUnsupportedCapabilityHeaders 在真实二进制上验证「未实现能力对应的
// 请求头」被显式拒绝：501 NotImplemented，且不留下桶、对象或分片会话。
//
// 这是 01/02 契约里"不得静默忽略"的端到端保障：进程内单测覆盖了每条
// 写路径，这里再确认中间件链（CORS、鉴权、审计）不会提前消费或
// 改写这些头。
func TestUnsupportedCapabilityHeaders(t *testing.T) {
	srv := startServer(t, serverOptions{})
	c := newClient(t, srv)

	bucket := uniqueBucket("hdr")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)

	t.Run("PutObject SSE", func(t *testing.T) {
		res := c.putObject(bucket, "sse.txt", []byte("hello"), map[string]string{
			"x-amz-server-side-encryption": "AES256",
		})
		requireS3Error(t, res, http.StatusNotImplemented, "NotImplemented")
		requireStatus(t, c.getObject(bucket, "sse.txt", nil), http.StatusNotFound)
	})

	t.Run("PutObject ACL", func(t *testing.T) {
		res := c.putObject(bucket, "acl.txt", []byte("hello"), map[string]string{
			"x-amz-acl": "public-read",
		})
		requireS3Error(t, res, http.StatusNotImplemented, "NotImplemented")
		requireStatus(t, c.getObject(bucket, "acl.txt", nil), http.StatusNotFound)
	})

	t.Run("CreateBucket Object Lock", func(t *testing.T) {
		locked := uniqueBucket("lock")
		res := c.do(http.MethodPut, c.bucketURL(locked), nil, nil, map[string]string{
			"x-amz-bucket-object-lock-enabled": "true",
		})
		requireS3Error(t, res, http.StatusNotImplemented, "NotImplemented")
		requireStatus(t, c.headBucket(locked), http.StatusNotFound)
	})

	t.Run("CreateMultipartUpload 标签", func(t *testing.T) {
		res := c.do(http.MethodPost, c.objectURL(bucket, "tagged.bin"),
			url.Values{"uploads": {""}}, nil, map[string]string{
				"x-amz-tagging": "env=prod",
			})
		requireS3Error(t, res, http.StatusNotImplemented, "NotImplemented")

		// 被拒绝的 Initiate 不得留下会话。
		list := c.do(http.MethodGet, c.bucketURL(bucket), url.Values{"uploads": {""}}, nil, nil)
		requireStatus(t, list, http.StatusOK)
		if body := string(list.Body); strings.Contains(body, "<Upload>") {
			t.Fatalf("被拒绝的 Initiate 留下了会话：%s", truncate(list.Body))
		}
	})
}
