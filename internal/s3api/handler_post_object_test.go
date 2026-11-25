package s3api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/binbinpeng123/Blink/internal/sigv4"
	"github.com/binbinpeng123/Blink/internal/store"
)

// postPolicy 生成 base64 编码的 policy 文档。
func postPolicy(t *testing.T, exp time.Time, conds ...any) string {
	t.Helper()
	doc := map[string]any{
		"expiration": exp.UTC().Format(time.RFC3339),
		"conditions": conds,
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("编码 policy 失败: %v", err)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

// buildForm 组装 multipart 表单；file 非 nil 时插入 file part，
// tail 里的字段写在 file 之后（用于验证"file 必须是最后一个"）。
func buildForm(t *testing.T, fields [][2]string, file []byte, tail [][2]string) (*bytes.Buffer, string) {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	for _, f := range fields {
		if err := w.WriteField(f[0], f[1]); err != nil {
			t.Fatalf("写字段 %s 失败: %v", f[0], err)
		}
	}
	if file != nil {
		fw, err := w.CreateFormFile("file", "upload.bin")
		if err != nil {
			t.Fatalf("创建 file part 失败: %v", err)
		}
		if _, err := fw.Write(file); err != nil {
			t.Fatalf("写 file part 失败: %v", err)
		}
	}
	for _, f := range tail {
		if err := w.WriteField(f[0], f[1]); err != nil {
			t.Fatalf("写字段 %s 失败: %v", f[0], err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("关闭 multipart writer 失败: %v", err)
	}
	return &buf, w.FormDataContentType()
}

// sendPost 发送表单请求。
func sendPost(t *testing.T, h http.Handler, target string, body *bytes.Buffer, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, target, body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// basicPostFields 是一组覆盖全部表单字段的 policy 条件配套字段。
//
// 每个字段都必须被条件覆盖（否则 403 Extra input fields），
// 所以这里成对给出，避免每个用例都手写一遍。
func basicPostFields(t *testing.T, key string, extra [][2]string) [][2]string {
	t.Helper()
	fields := [][2]string{
		{"key", key},
		{"policy", postPolicy(t, time.Now().Add(time.Hour),
			map[string]string{"bucket": "bkt"},
			[]string{"eq", "$key", key},
		)},
	}
	return append(fields, extra...)
}

// postObjectGet 发 GET 验证对象是否落盘。
func postObjectGet(t *testing.T, h http.Handler, key string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/bkt/"+key, nil))
	return rec
}

func TestPostObjectSuccess(t *testing.T) {
	h := objectTestServer(t)
	content := "hello from a browser form"

	body, ct := buildForm(t, [][2]string{
		{"key", "uploads/hello.txt"},
		{"policy", postPolicy(t, time.Now().Add(time.Hour),
			map[string]string{"bucket": "bkt"},
			[]string{"starts-with", "$key", "uploads/"},
			[]string{"eq", "$Content-Type", "text/plain"},
			map[string]string{"x-amz-meta-owner": "edge-01"},
			[]any{"content-length-range", 0, 1 << 20},
		)},
		{"Content-Type", "text/plain"},
		{"x-amz-meta-owner", "edge-01"},
	}, []byte(content), nil)

	rec := sendPost(t, h, "/bkt", body, ct)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("状态码 = %d，期望 204（%s）", rec.Code, rec.Body.String())
	}
	if got, want := rec.Header().Get("ETag"), `"`+md5Hex(content)+`"`; got != want {
		t.Errorf("ETag = %s，期望 %s", got, want)
	}
	if loc := rec.Header().Get("Location"); loc != "/bkt/uploads/hello.txt" {
		t.Errorf("Location = %q", loc)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("204 响应体应为空，实际 %q", rec.Body.String())
	}

	got := postObjectGet(t, h, "uploads/hello.txt")
	if got.Code != http.StatusOK {
		t.Fatalf("回读状态码 = %d（%s）", got.Code, got.Body.String())
	}
	if got.Body.String() != content {
		t.Errorf("回读内容 = %q", got.Body.String())
	}
	if v := got.Header().Get("Content-Type"); v != "text/plain" {
		t.Errorf("Content-Type = %q", v)
	}
	// GET 侧按原始小写键写元数据头（供 botocore 按前缀匹配），
	// Header().Get 会 canonicalize 成 X-Amz-Meta-Owner 而取不到。
	if v := got.Header()["x-amz-meta-owner"]; len(v) != 1 || v[0] != "edge-01" {
		t.Errorf("x-amz-meta-owner = %q", v)
	}
}

func TestPostObjectSuccessActionStatus201(t *testing.T) {
	h := objectTestServer(t)
	body, ct := buildForm(t, [][2]string{
		{"key", "created.txt"},
		{"policy", postPolicy(t, time.Now().Add(time.Hour),
			map[string]string{"bucket": "bkt"},
			[]string{"eq", "$key", "created.txt"},
			map[string]string{"success_action_status": "201"},
		)},
		{"success_action_status", "201"},
	}, []byte("x"), nil)

	rec := sendPost(t, h, "/bkt", body, ct)
	if rec.Code != http.StatusCreated {
		t.Fatalf("状态码 = %d，期望 201（%s）", rec.Code, rec.Body.String())
	}
	var resp postObjectResponse
	if err := xml.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("解析 PostResponse 失败: %v；响应=%s", err, rec.Body.String())
	}
	if resp.Bucket != "bkt" || resp.Key != "created.txt" || resp.Location != "/bkt/created.txt" {
		t.Errorf("PostResponse = %+v", resp)
	}
}

func TestPostObjectSuccessActionRedirect(t *testing.T) {
	h := objectTestServer(t)
	const redirect = "https://upload.example.test/done?from=form"
	body, ct := buildForm(t, [][2]string{
		{"key", "redirected.txt"},
		{"policy", postPolicy(t, time.Now().Add(time.Hour),
			map[string]string{"bucket": "bkt"},
			[]string{"eq", "$key", "redirected.txt"},
			map[string]string{"success_action_redirect": redirect},
		)},
		{"success_action_redirect", redirect},
	}, []byte("x"), nil)

	rec := sendPost(t, h, "/bkt", body, ct)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("状态码 = %d，期望 303（%s）", rec.Code, rec.Body.String())
	}
	loc := rec.Header().Get("Location")
	for _, want := range []string{"from=form", "bucket=bkt", "key=redirected.txt", "etag="} {
		if !strings.Contains(loc, want) {
			t.Errorf("Location = %q，缺少 %q", loc, want)
		}
	}
}

func TestPostObjectPolicyExpired(t *testing.T) {
	h := objectTestServer(t)
	fields := [][2]string{
		{"key", "expired.txt"},
		{"policy", postPolicy(t, time.Now().Add(-time.Minute),
			map[string]string{"bucket": "bkt"}, []string{"eq", "$key", "expired.txt"})},
	}
	body, ct := buildForm(t, fields, []byte("x"), nil)

	rec := sendPost(t, h, "/bkt", body, ct)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("状态码 = %d，期望 403（%s）", rec.Code, rec.Body.String())
	}
	e := decodeError(t, rec)
	if e.Code != "AccessDenied" || !strings.Contains(e.Message, "Policy expired.") {
		t.Errorf("错误 = %+v", e)
	}
}

func TestPostObjectPolicyConditionFailed(t *testing.T) {
	h := objectTestServer(t)
	fields := [][2]string{
		{"key", "other/prefix.txt"},
		{"policy", postPolicy(t, time.Now().Add(time.Hour),
			[]string{"starts-with", "$key", "uploads/"})},
	}
	body, ct := buildForm(t, fields, []byte("x"), nil)

	rec := sendPost(t, h, "/bkt", body, ct)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("状态码 = %d，期望 403（%s）", rec.Code, rec.Body.String())
	}
	if !strings.Contains(decodeError(t, rec).Message, "Policy Condition failed") {
		t.Errorf("消息 = %q", decodeError(t, rec).Message)
	}
}

func TestPostObjectExtraInputFields(t *testing.T) {
	h := objectTestServer(t)
	fields := [][2]string{
		{"key", "covered.txt"},
		{"policy", postPolicy(t, time.Now().Add(time.Hour),
			[]string{"eq", "$key", "covered.txt"})},
		{"x-amz-meta-sneaky", "value"}, // 未被条件覆盖
	}
	body, ct := buildForm(t, fields, []byte("x"), nil)

	rec := sendPost(t, h, "/bkt", body, ct)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("状态码 = %d，期望 403（%s）", rec.Code, rec.Body.String())
	}
	if !strings.Contains(decodeError(t, rec).Message, "Extra input fields: x-amz-meta-sneaky") {
		t.Errorf("消息 = %q", decodeError(t, rec).Message)
	}
}

func TestPostObjectContentLengthRange(t *testing.T) {
	h := objectTestServer(t)
	policy := func() string {
		return postPolicy(t, time.Now().Add(time.Hour),
			[]string{"eq", "$key", "sized.bin"},
			[]any{"content-length-range", 4, 8},
		)
	}

	t.Run("超过上限", func(t *testing.T) {
		body, ct := buildForm(t, [][2]string{
			{"key", "sized.bin"}, {"policy", policy()},
		}, []byte("0123456789"), nil)
		rec := sendPost(t, h, "/bkt", body, ct)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("状态码 = %d，期望 400（%s）", rec.Code, rec.Body.String())
		}
		if code := decodeError(t, rec).Code; code != "EntityTooLarge" {
			t.Errorf("Code = %s", code)
		}
		if got := postObjectGet(t, h, "sized.bin"); got.Code != http.StatusNotFound {
			t.Errorf("超限上传后对象仍存在：状态码 %d", got.Code)
		}
	})

	t.Run("低于下限", func(t *testing.T) {
		body, ct := buildForm(t, [][2]string{
			{"key", "sized.bin"}, {"policy", policy()},
		}, []byte("ab"), nil)
		rec := sendPost(t, h, "/bkt", body, ct)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("状态码 = %d，期望 400（%s）", rec.Code, rec.Body.String())
		}
		if code := decodeError(t, rec).Code; code != "EntityTooSmall" {
			t.Errorf("Code = %s", code)
		}
	})

	t.Run("在范围内", func(t *testing.T) {
		body, ct := buildForm(t, [][2]string{
			{"key", "sized.bin"}, {"policy", policy()},
		}, []byte("012345"), nil)
		if rec := sendPost(t, h, "/bkt", body, ct); rec.Code != http.StatusNoContent {
			t.Fatalf("状态码 = %d（%s）", rec.Code, rec.Body.String())
		}
	})
}

// 未实现能力字段必须在读取文件之前被拒绝（501），且不留对象。
func TestPostObjectRejectsUnsupportedFields(t *testing.T) {
	h := objectTestServer(t)
	for _, name := range []string{"acl", "tagging", "x-amz-security-token", "x-amz-server-side-encryption"} {
		t.Run(name, func(t *testing.T) {
			key := "rejected-" + name
			body, ct := buildForm(t, basicPostFields(t, key, [][2]string{{name, "v"}}), []byte("x"), nil)
			rec := sendPost(t, h, "/bkt", body, ct)
			if rec.Code != http.StatusNotImplemented {
				t.Fatalf("状态码 = %d，期望 501（%s）", rec.Code, rec.Body.String())
			}
			if got := postObjectGet(t, h, key); got.Code != http.StatusNotFound {
				t.Errorf("被拒请求留下了对象：状态码 %d", got.Code)
			}
		})
	}
}

func TestPostObjectRejectsContentMD5Field(t *testing.T) {
	h := objectTestServer(t)
	key := "with-md5"
	body, ct := buildForm(t, basicPostFields(t, key, [][2]string{{"Content-MD5", md5Hex("x")}}), []byte("x"), nil)
	rec := sendPost(t, h, "/bkt", body, ct)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，期望 400（%s）", rec.Code, rec.Body.String())
	}
	if code := decodeError(t, rec).Code; code != "InvalidArgument" {
		t.Errorf("Code = %s", code)
	}
}

func TestPostObjectFileMustBeLast(t *testing.T) {
	h := objectTestServer(t)
	key := "tail-field"
	body, ct := buildForm(t, basicPostFields(t, key, nil), []byte("x"), [][2]string{{"x-ignore-tail", "1"}})
	rec := sendPost(t, h, "/bkt", body, ct)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，期望 400（%s）", rec.Code, rec.Body.String())
	}
	if code := decodeError(t, rec).Code; code != "MalformedPOSTRequest" {
		t.Errorf("Code = %s", code)
	}
	if got := postObjectGet(t, h, key); got.Code != http.StatusNotFound {
		t.Errorf("file 非最后仍留下了对象：状态码 %d", got.Code)
	}
}

func TestPostObjectMalformedForms(t *testing.T) {
	h := objectTestServer(t)
	cases := map[string]struct {
		fields [][2]string
		file   []byte
		raw    string
	}{
		"缺少 file": {
			fields: basicPostFields(t, "no-file", nil),
		},
		"重复字段": {
			fields: append(basicPostFields(t, "dup", nil), [2]string{"key", "dup"}),
			file:   []byte("x"),
		},
		"无 policy": {
			fields: [][2]string{{"key", "no-policy"}},
			file:   []byte("x"),
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			body, ct := buildForm(t, c.fields, c.file, nil)
			rec := sendPost(t, h, "/bkt", body, ct)
			if rec.Code < 400 || rec.Code >= 500 {
				t.Fatalf("状态码 = %d，期望 4xx（%s）", rec.Code, rec.Body.String())
			}
		})
	}

	t.Run("缺失 boundary", func(t *testing.T) {
		rec := sendPost(t, h, "/bkt", bytes.NewBufferString("x"), "multipart/form-data")
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("状态码 = %d", rec.Code)
		}
		if code := decodeError(t, rec).Code; code != "MalformedPOSTRequest" {
			t.Errorf("Code = %s", code)
		}
	})
}

// postAuthTestServer 与 objectTestServer 相同，但开启签名校验，
// 用于验证表单签名链路（中间件在 handler 之前）。
// 第二个返回值共用同一存储但不校验签名，供测试回读对象。
func postAuthTestServer(t *testing.T) (signed, open http.Handler) {
	t.Helper()
	st, err := store.Open(store.Options{
		Root:            filepath.Join(t.TempDir(), "data"),
		BlockSize:       8,
		InlineThreshold: 4,
	})
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	if err := st.CreateBucket("bkt"); err != nil {
		t.Fatalf("建桶失败: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	signed = New(Options{
		Logger:   logger,
		Region:   testRegion,
		Provider: sigv4.NewStaticProvider(sigv4.Credentials{AccessKeyID: testAK, SecretKey: testSK}),
		Store:    st,
	}).Handler()
	open = New(Options{
		Logger:      logger,
		Region:      testRegion,
		DisableAuth: true,
		Store:       st,
	}).Handler()
	return signed, open
}

// signedPostFields 生成一份签名完整的表单字段。
//
// 签名材料里的每个字段都必须同时出现在 policy 条件里，
// 否则会被判为"未覆盖字段"。
func signedPostFields(t *testing.T, key string) [][2]string {
	t.Helper()
	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	credential := testAK + "/" + dateStamp + "/" + testRegion + "/s3/aws4_request"
	policy := postPolicy(t, now.Add(time.Hour),
		map[string]string{"bucket": "bkt"},
		[]string{"eq", "$key", key},
		map[string]string{"x-amz-algorithm": "AWS4-HMAC-SHA256"},
		map[string]string{"x-amz-credential": credential},
		map[string]string{"x-amz-date": amzDate},
	)
	return [][2]string{
		{"key", key},
		{"policy", policy},
		{"x-amz-algorithm", "AWS4-HMAC-SHA256"},
		{"x-amz-credential", credential},
		{"x-amz-date", amzDate},
		{"x-amz-signature", sigv4.SignPostPolicy(testSK, dateStamp, testRegion, policy)},
	}
}

func TestPostObjectSigned(t *testing.T) {
	h, open := postAuthTestServer(t)
	const key = "signed.txt"
	body, ct := buildForm(t, signedPostFields(t, key), []byte("signed body"), nil)

	rec := sendPost(t, h, "/bkt", body, ct)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("状态码 = %d，期望 204（%s）", rec.Code, rec.Body.String())
	}
	if got := postObjectGet(t, open, key); got.Code != http.StatusOK || got.Body.String() != "signed body" {
		t.Errorf("回读 = %d %q", got.Code, got.Body.String())
	}
}

func TestPostObjectSignatureMismatch(t *testing.T) {
	h, open := postAuthTestServer(t)
	fields := signedPostFields(t, "tampered.txt")
	sig := fields[len(fields)-1][1]
	fields[len(fields)-1][1] = strings.Repeat("0", len(sig))
	body, ct := buildForm(t, fields, []byte("x"), nil)

	rec := sendPost(t, h, "/bkt", body, ct)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("状态码 = %d，期望 403（%s）", rec.Code, rec.Body.String())
	}
	if code := decodeError(t, rec).Code; code != "SignatureDoesNotMatch" {
		t.Errorf("Code = %s", code)
	}
	if got := postObjectGet(t, open, "tampered.txt"); got.Code != http.StatusNotFound {
		t.Errorf("签名失败仍留下了对象：状态码 %d", got.Code)
	}
}

func TestPostObjectMissingSignatureFields(t *testing.T) {
	h, _ := postAuthTestServer(t)
	for _, name := range []string{"x-amz-credential", "x-amz-date", "x-amz-signature"} {
		t.Run(name, func(t *testing.T) {
			var fields [][2]string
			for _, f := range signedPostFields(t, "missing.txt") {
				if f[0] != name {
					fields = append(fields, f)
				}
			}
			body, ct := buildForm(t, fields, []byte("x"), nil)
			rec := sendPost(t, h, "/bkt", body, ct)
			// 与头部签名一致：AuthorizationHeaderMalformed 是 400，
			// 见 02 错误码表。
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("状态码 = %d，期望 400（%s）", rec.Code, rec.Body.String())
			}
			if code := decodeError(t, rec).Code; code != "AuthorizationHeaderMalformed" {
				t.Errorf("Code = %s", code)
			}
		})
	}
}

// 签名有效但 policy 已过期：验签通过后由 policy 的 expiration 拒绝。
func TestPostObjectSignedButExpired(t *testing.T) {
	h, _ := postAuthTestServer(t)
	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	credential := testAK + "/" + dateStamp + "/" + testRegion + "/s3/aws4_request"
	policy := postPolicy(t, now.Add(-time.Minute),
		map[string]string{"bucket": "bkt"},
		[]string{"eq", "$key", "expired.txt"},
		map[string]string{"x-amz-algorithm": "AWS4-HMAC-SHA256"},
		map[string]string{"x-amz-credential": credential},
		map[string]string{"x-amz-date": amzDate},
	)
	body, ct := buildForm(t, [][2]string{
		{"key", "expired.txt"},
		{"policy", policy},
		{"x-amz-algorithm", "AWS4-HMAC-SHA256"},
		{"x-amz-credential", credential},
		{"x-amz-date", amzDate},
		{"x-amz-signature", sigv4.SignPostPolicy(testSK, dateStamp, testRegion, policy)},
	}, []byte("x"), nil)

	rec := sendPost(t, h, "/bkt", body, ct)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("状态码 = %d，期望 403（%s）", rec.Code, rec.Body.String())
	}
	e := decodeError(t, rec)
	if e.Code != "AccessDenied" || !strings.Contains(e.Message, "Policy expired.") {
		t.Errorf("错误 = %+v", e)
	}
}

// 非 multipart 的桶级 POST 仍是 501，不能被 POST Object 分派吞掉。
func TestBucketPostWithoutFormStaysNotImplemented(t *testing.T) {
	h := objectTestServer(t)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/bkt", strings.NewReader("{}")))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("状态码 = %d，期望 501（%s）", rec.Code, rec.Body.String())
	}
}
