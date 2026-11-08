//go:build integration

package integration

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

const testCORSConfig = `<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration>
  <CORSRule>
    <ID>web-app</ID>
    <AllowedOrigin>https://app.example.com</AllowedOrigin>
    <AllowedMethod>GET</AllowedMethod>
    <AllowedMethod>PUT</AllowedMethod>
    <AllowedHeader>content-type</AllowedHeader>
    <AllowedHeader>x-amz-*</AllowedHeader>
    <ExposeHeader>ETag</ExposeHeader>
    <MaxAgeSeconds>3600</MaxAgeSeconds>
  </CORSRule>
</CORSConfiguration>`

// TestBucketCORS 覆盖桶级 CORS 的配置读写、预检与实际请求的响应头注入。
func TestBucketCORS(t *testing.T) {
	srv := startServer(t, serverOptions{})
	c := newClient(t, srv)

	bucket := uniqueBucket("cors")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)

	t.Run("未配置时 GET 返回 404", func(t *testing.T) {
		res := c.do(http.MethodGet, c.bucketURL(bucket), url.Values{"cors": {""}}, nil, nil)
		requireS3Error(t, res, http.StatusNotFound, "NoSuchCORSConfiguration")
	})

	t.Run("PUT 配置成功且可读回", func(t *testing.T) {
		res := c.do(http.MethodPut, c.bucketURL(bucket), url.Values{"cors": {""}},
			[]byte(testCORSConfig), map[string]string{"Content-Type": "application/xml"})
		requireStatus(t, res, http.StatusOK)

		res = c.do(http.MethodGet, c.bucketURL(bucket), url.Values{"cors": {""}}, nil, nil)
		requireStatus(t, res, http.StatusOK)
		body := string(res.Body)
		for _, want := range []string{
			"<CORSConfiguration>",
			"https://app.example.com",
			"<AllowedMethod>GET</AllowedMethod>",
			"<MaxAgeSeconds>3600</MaxAgeSeconds>",
		} {
			if !strings.Contains(body, want) {
				t.Fatalf("GET ?cors 响应缺少 %q：%s", want, truncate(res.Body))
			}
		}
	})

	t.Run("非法 XML 返回 400", func(t *testing.T) {
		res := c.do(http.MethodPut, c.bucketURL(bucket), url.Values{"cors": {""}},
			[]byte("<CORSConfiguration><CORSRule>"), map[string]string{"Content-Type": "application/xml"})
		requireS3Error(t, res, http.StatusBadRequest, "MalformedXML")
	})

	t.Run("预检匹配返回允许的方法与缓存时长", func(t *testing.T) {
		// 预检不带签名：浏览器不会给 OPTIONS 带 Authorization。
		res := c.unsigned(http.MethodOptions, c.bucketURL(bucket), nil, nil, map[string]string{
			"Origin":                         "https://app.example.com",
			"Access-Control-Request-Method":  "GET",
			"Access-Control-Request-Headers": "content-type",
		})
		requireStatus(t, res, http.StatusOK)
		if got := res.Header.Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
			t.Fatalf("Allow-Origin = %q", got)
		}
		if got := res.Header.Get("Access-Control-Allow-Methods"); !strings.Contains(got, "GET") {
			t.Fatalf("Allow-Methods = %q，应包含 GET", got)
		}
		if got := res.Header.Get("Access-Control-Allow-Headers"); !strings.Contains(strings.ToLower(got), "content-type") {
			t.Fatalf("Allow-Headers = %q，应回显 content-type", got)
		}
		if got := res.Header.Get("Access-Control-Max-Age"); got != "3600" {
			t.Fatalf("Max-Age = %q，期望 3600", got)
		}
	})

	t.Run("预检缺 Origin 返回 400", func(t *testing.T) {
		res := c.unsigned(http.MethodOptions, c.bucketURL(bucket), nil, nil, map[string]string{
			"Access-Control-Request-Method": "GET",
		})
		requireS3Error(t, res, http.StatusBadRequest, "InvalidArgument")
	})

	t.Run("预检缺请求方法返回 400", func(t *testing.T) {
		res := c.unsigned(http.MethodOptions, c.bucketURL(bucket), nil, nil, map[string]string{
			"Origin": "https://app.example.com",
		})
		requireS3Error(t, res, http.StatusBadRequest, "InvalidArgument")
	})

	t.Run("预检未授权方法返回 403", func(t *testing.T) {
		res := c.unsigned(http.MethodOptions, c.bucketURL(bucket), nil, nil, map[string]string{
			"Origin":                        "https://app.example.com",
			"Access-Control-Request-Method": "DELETE",
		})
		requireS3Error(t, res, http.StatusForbidden, "AccessForbidden")
	})

	t.Run("预检未授权来源返回 403", func(t *testing.T) {
		res := c.unsigned(http.MethodOptions, c.bucketURL(bucket), nil, nil, map[string]string{
			"Origin":                        "https://evil.example.com",
			"Access-Control-Request-Method": "GET",
		})
		requireS3Error(t, res, http.StatusForbidden, "AccessForbidden")
	})

	t.Run("未配置 CORS 的桶预检返回 403", func(t *testing.T) {
		other := uniqueBucket("cors-none")
		requireStatus(t, c.createBucket(other), http.StatusOK)

		res := c.unsigned(http.MethodOptions, c.bucketURL(other), nil, nil, map[string]string{
			"Origin":                        "https://app.example.com",
			"Access-Control-Request-Method": "GET",
		})
		requireS3Error(t, res, http.StatusForbidden, "AccessForbidden")
	})

	t.Run("实际请求带 Origin 时注入 CORS 头", func(t *testing.T) {
		requireStatus(t, c.putObject(bucket, "cors.txt", []byte("hello"), nil), http.StatusOK)

		res := c.getObject(bucket, "cors.txt", map[string]string{
			"Origin": "https://app.example.com",
		})
		requireStatus(t, res, http.StatusOK)
		if got := res.Header.Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
			t.Fatalf("Allow-Origin = %q", got)
		}
		if got := res.Header.Get("Access-Control-Expose-Headers"); !strings.Contains(got, "ETag") {
			t.Fatalf("Expose-Headers = %q，应包含 ETag", got)
		}
	})

	t.Run("实际请求未授权来源不带 CORS 头", func(t *testing.T) {
		res := c.getObject(bucket, "cors.txt", map[string]string{
			"Origin": "https://evil.example.com",
		})
		// S3 语义：业务照常完成，只是不给 CORS 头。
		requireStatus(t, res, http.StatusOK)
		if got := res.Header.Get("Access-Control-Allow-Origin"); got != "" {
			t.Fatalf("未授权来源仍收到 Allow-Origin = %q", got)
		}
	})

	t.Run("DELETE 幂等且之后 GET 返回 404", func(t *testing.T) {
		requireStatus(t, c.do(http.MethodDelete, c.bucketURL(bucket),
			url.Values{"cors": {""}}, nil, nil), http.StatusNoContent)
		// 对从未配置的桶删除同样 204。
		requireStatus(t, c.do(http.MethodDelete, c.bucketURL(bucket),
			url.Values{"cors": {""}}, nil, nil), http.StatusNoContent)

		res := c.do(http.MethodGet, c.bucketURL(bucket), url.Values{"cors": {""}}, nil, nil)
		requireS3Error(t, res, http.StatusNotFound, "NoSuchCORSConfiguration")
	})

	t.Run("对象级 ?cors 返回 501", func(t *testing.T) {
		res := c.do(http.MethodGet, c.objectURL(bucket, "cors.txt"),
			url.Values{"cors": {""}}, nil, nil)
		requireS3Error(t, res, http.StatusNotImplemented, "NotImplemented")
	})
}
