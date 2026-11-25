//go:build integration

package integration

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/binbinpeng123/Blink/internal/sigv4"
)

// postPolicyDoc 生成 base64 编码的 policy 文档。
func postPolicyDoc(t *testing.T, expiration time.Time, conds ...any) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"expiration": expiration.UTC().Format(time.RFC3339),
		"conditions": conds,
	})
	if err != nil {
		t.Fatalf("编码 policy 失败: %v", err)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

// signedPostFields 生成签名完整的表单字段。
//
// x-amz-* 字段同样受覆盖规则约束，所以必须出现在 policy 条件里；
// policy 里额外包含 {"bucket": …} 与 ["eq", "$key", …] 两条基础条件。
func signedPostFields(t *testing.T, bucket, key string, expiration time.Time, extraConds ...any) [][2]string {
	t.Helper()
	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	credential := testAccessKey + "/" + dateStamp + "/" + testRegion + "/s3/aws4_request"

	conds := append([]any{
		map[string]string{"bucket": bucket},
		[]string{"eq", "$key", key},
		map[string]string{"x-amz-algorithm": "AWS4-HMAC-SHA256"},
		map[string]string{"x-amz-credential": credential},
		map[string]string{"x-amz-date": amzDate},
	}, extraConds...)
	policy := postPolicyDoc(t, expiration, conds...)

	return [][2]string{
		{"key", key},
		{"policy", policy},
		{"x-amz-algorithm", "AWS4-HMAC-SHA256"},
		{"x-amz-credential", credential},
		{"x-amz-date", amzDate},
		{"x-amz-signature", sigv4.SignPostPolicy(testSecretKey, dateStamp, testRegion, policy)},
	}
}

// buildForm 按字段顺序组装 multipart 表单，file part 固定排在最后。
func buildForm(t *testing.T, fields [][2]string, file []byte) (*bytes.Buffer, string) {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	for _, f := range fields {
		if err := w.WriteField(f[0], f[1]); err != nil {
			t.Fatalf("写字段 %s 失败: %v", f[0], err)
		}
	}
	fw, err := w.CreateFormFile("file", "upload.bin")
	if err != nil {
		t.Fatalf("创建 file part 失败: %v", err)
	}
	if _, err := fw.Write(file); err != nil {
		t.Fatalf("写 file part 失败: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("关闭 multipart writer 失败: %v", err)
	}
	return &buf, w.FormDataContentType()
}

// postForm 发送一次真实的表单直传（不经 s3Client，因为它只签 URL）。
//
// 不跟随重定向：303 的 Location 指向调用方自己的页面，
// 跟随会把"服务端是否正确回跳"变成一次外部 DNS 解析。
func postForm(t *testing.T, srv *testServer, bucket string, body *bytes.Buffer, contentType string) *httpResult {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.baseURL+"/"+url.PathEscape(bucket), body)
	if err != nil {
		t.Fatalf("构造请求失败: %v", err)
	}
	req.Header.Set("Content-Type", contentType)
	hc := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatalf("POST 表单失败: %v", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读取响应失败: %v", err)
	}
	return &httpResult{Status: resp.StatusCode, Header: resp.Header, Body: data}
}

// TestPostObjectEndToEnd 覆盖浏览器直传的主链路：
// 表单签名 → 落盘 → 用签名客户端回读内容。
func TestPostObjectEndToEnd(t *testing.T) {
	srv := startServer(t, serverOptions{})
	c := newClient(t, srv)
	bucket := uniqueBucket("post")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)

	const key = "uploads/hello.txt"
	content := []byte("hello from browser form")
	body, ct := buildForm(t, signedPostFields(t, bucket, key, time.Now().Add(time.Hour)), content)

	res := postForm(t, srv, bucket, body, ct)
	requireStatus(t, res, http.StatusNoContent)
	if got := res.Header.Get("ETag"); got == "" {
		t.Error("204 响应缺少 ETag")
	}
	if loc := res.Header.Get("Location"); loc != "/"+bucket+"/"+key {
		t.Errorf("Location = %q", loc)
	}
	if len(res.Body) != 0 {
		t.Errorf("204 响应体应为空，实际 %q", truncate(res.Body))
	}

	got := c.getObject(bucket, key, nil)
	requireStatus(t, got, http.StatusOK)
	if !bytes.Equal(got.Body, content) {
		t.Errorf("回读内容 = %q", truncate(got.Body))
	}
}

// TestPostObjectSuccessActions 验证 201 XML 与 303 重定向两种成功形态。
func TestPostObjectSuccessActions(t *testing.T) {
	srv := startServer(t, serverOptions{})
	c := newClient(t, srv)
	bucket := uniqueBucket("post")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)

	t.Run("201", func(t *testing.T) {
		key := "created.txt"
		fields := signedPostFields(t, bucket, key, time.Now().Add(time.Hour),
			map[string]string{"success_action_status": "201"})
		fields = append(fields, [2]string{"success_action_status", "201"})
		body, ct := buildForm(t, fields, []byte("x"))

		res := postForm(t, srv, bucket, body, ct)
		requireStatus(t, res, http.StatusCreated)
		if !strings.Contains(string(res.Body), "<PostResponse>") {
			t.Errorf("201 响应应为 PostResponse XML: %s", truncate(res.Body))
		}
	})

	t.Run("303", func(t *testing.T) {
		key := "redirected.txt"
		const target = "https://upload.example.test/done?from=form"
		fields := signedPostFields(t, bucket, key, time.Now().Add(time.Hour),
			map[string]string{"success_action_redirect": target})
		fields = append(fields, [2]string{"success_action_redirect", target})
		body, ct := buildForm(t, fields, []byte("x"))

		res := postForm(t, srv, bucket, body, ct)
		requireStatus(t, res, http.StatusSeeOther)
		loc := res.Header.Get("Location")
		for _, want := range []string{"from=form", "bucket=" + bucket, "key=" + key, "etag="} {
			if !strings.Contains(loc, want) {
				t.Errorf("Location = %q，缺少 %q", loc, want)
			}
		}
	})
}

// TestPostObjectPolicyRejections 验证过期、未覆盖字段与长度超限的线上行为，
// 且被拒请求不留下对象。
func TestPostObjectPolicyRejections(t *testing.T) {
	srv := startServer(t, serverOptions{})
	c := newClient(t, srv)
	bucket := uniqueBucket("post")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)

	t.Run("policy 过期", func(t *testing.T) {
		body, ct := buildForm(t, signedPostFields(t, bucket, "expired.txt", time.Now().Add(-time.Hour)), []byte("x"))
		res := postForm(t, srv, bucket, body, ct)
		e := requireS3Error(t, res, http.StatusForbidden, "AccessDenied")
		if !strings.Contains(e.Message, "Policy expired.") {
			t.Errorf("消息 = %q", e.Message)
		}
	})

	t.Run("未覆盖字段", func(t *testing.T) {
		key := "extra.txt"
		fields := append(signedPostFields(t, bucket, key, time.Now().Add(time.Hour)),
			[2]string{"x-amz-meta-sneaky", "v"})
		body, ct := buildForm(t, fields, []byte("x"))

		res := postForm(t, srv, bucket, body, ct)
		e := requireS3Error(t, res, http.StatusForbidden, "AccessDenied")
		if !strings.Contains(e.Message, "Extra input fields: x-amz-meta-sneaky") {
			t.Errorf("消息 = %q", e.Message)
		}
		if got := c.getObject(bucket, key, nil); got.Status != http.StatusNotFound {
			t.Errorf("被拒请求留下了对象：状态码 %d", got.Status)
		}
	})

	t.Run("超过 content-length-range", func(t *testing.T) {
		key := "too-large.bin"
		fields := signedPostFields(t, bucket, key, time.Now().Add(time.Hour),
			[]any{"content-length-range", 0, 4})
		body, ct := buildForm(t, fields, []byte("0123456789"))

		res := postForm(t, srv, bucket, body, ct)
		requireS3Error(t, res, http.StatusBadRequest, "EntityTooLarge")
		if got := c.getObject(bucket, key, nil); got.Status != http.StatusNotFound {
			t.Errorf("超限上传留下了对象：状态码 %d", got.Status)
		}
	})
}
