//go:build integration

package integration

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// TestAuthentication 验证完整的 SigV4 校验链路：
// 匿名拒绝、未知 AK、错误 SK、Presigned URL 放行。
func TestAuthentication(t *testing.T) {
	srv := startServer(t, serverOptions{})
	c := newClient(t, srv)

	bucket := uniqueBucket("auth")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)

	body := []byte("authenticated content")
	requireStatus(t, c.putObject(bucket, "hello.txt", body, nil), http.StatusOK)

	path := c.objectURL(bucket, "hello.txt")

	t.Run("匿名请求被拒绝", func(t *testing.T) {
		res := c.unsigned(http.MethodGet, path, nil, nil, nil)
		requireS3Error(t, res, http.StatusForbidden, "AccessDenied")
	})

	t.Run("未知 Access Key", func(t *testing.T) {
		bad := c.withCredentials("AKIANOTAREALKEY000", testSecretKey)
		res := bad.getObject(bucket, "hello.txt", nil)
		requireS3Error(t, res, http.StatusForbidden, "InvalidAccessKeyId")
	})

	t.Run("错误 Secret Key", func(t *testing.T) {
		bad := c.withCredentials(testAccessKey, "wrong-secret-key")
		res := bad.getObject(bucket, "hello.txt", nil)
		requireS3Error(t, res, http.StatusForbidden, "SignatureDoesNotMatch")
	})

	t.Run("Presigned URL 可读", func(t *testing.T) {
		signed := c.presignedURL(http.MethodGet, path, nil)
		resp, err := c.hc.Get(signed)
		if err != nil {
			t.Fatalf("请求 Presigned URL 失败: %v", err)
		}
		defer resp.Body.Close()
		requireStatus(t, &httpResult{Status: resp.StatusCode, Header: resp.Header}, http.StatusOK)
	})

	t.Run("Presigned URL 篡改参数后签名失效", func(t *testing.T) {
		signed := c.presignedURL(http.MethodGet, path, nil)
		u, err := url.Parse(signed)
		if err != nil {
			t.Fatalf("解析 URL 失败: %v", err)
		}
		// 改一个业务参数，但保留原签名：查询串进入规范化，签名必须失配。
		q := u.Query()
		q.Set("response-content-type", "text/plain")
		u.RawQuery = q.Encode()
		resp, err := c.hc.Get(u.String())
		if err != nil {
			t.Fatalf("请求篡改后的 URL 失败: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatalf("篡改参数后仍返回 200，签名未覆盖查询串")
		}
	})
}

// TestUnauthenticatedEndpoints 验证探针与指标端点免签，且探针字段符合契约。
func TestUnauthenticatedEndpoints(t *testing.T) {
	srv := startServer(t, serverOptions{})
	c := newClient(t, srv)

	t.Run("/healthz", func(t *testing.T) {
		res := c.unsigned(http.MethodGet, "/healthz", nil, nil, nil)
		requireStatus(t, res, http.StatusOK)

		type healthz struct {
			Status string `json:"status"`
		}
		got := mustJSON[healthz](t, res)
		if got.Status != "ok" {
			t.Fatalf("status = %q，期望 ok", got.Status)
		}
	})

	t.Run("/readyz", func(t *testing.T) {
		res := c.unsigned(http.MethodGet, "/readyz", nil, nil, nil)
		requireStatus(t, res, http.StatusOK)

		type readyz struct {
			Status string `json:"status"`
			Ready  bool   `json:"ready"`
			Disk   struct {
				HighWatermark float64 `json:"high_watermark"`
				LowWatermark  float64 `json:"low_watermark"`
			} `json:"disk"`
		}
		got := mustJSON[readyz](t, res)
		if got.Status != "ok" || !got.Ready {
			t.Fatalf("readyz = %+v，期望 status=ok ready=true", got)
		}
		if got.Disk.HighWatermark <= got.Disk.LowWatermark {
			t.Fatalf("水位区间异常: high=%v low=%v", got.Disk.HighWatermark, got.Disk.LowWatermark)
		}
	})

	t.Run("/version", func(t *testing.T) {
		res := c.unsigned(http.MethodGet, "/version", nil, nil, nil)
		requireStatus(t, res, http.StatusOK)
		if !strings.Contains(string(res.Body), "version") {
			t.Fatalf("/version 响应缺少 version 字段: %s", res.Body)
		}
	})

	t.Run("/metrics", func(t *testing.T) {
		res := c.unsigned(http.MethodGet, "/metrics", nil, nil, nil)
		requireStatus(t, res, http.StatusOK)
	})

	t.Run("未知路径仍需签名", func(t *testing.T) {
		res := c.unsigned(http.MethodGet, "/no-such-path", nil, nil, nil)
		requireS3Error(t, res, http.StatusForbidden, "AccessDenied")
	})
}
