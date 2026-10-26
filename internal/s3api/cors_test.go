package s3api

import (
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/binbinpeng123/Blink/internal/s3err"
	"github.com/binbinpeng123/Blink/internal/sigv4"
	"github.com/binbinpeng123/Blink/internal/store"
)

const testCORSXML = `<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration>
  <CORSRule>
    <ID>ui</ID>
    <AllowedOrigin>http://localhost:5173</AllowedOrigin>
    <AllowedOrigin>https://*.example.com</AllowedOrigin>
    <AllowedMethod>GET</AllowedMethod>
    <AllowedMethod>PUT</AllowedMethod>
    <AllowedHeader>x-amz-*</AllowedHeader>
    <ExposeHeader>ETag</ExposeHeader>
    <MaxAgeSeconds>600</MaxAgeSeconds>
  </CORSRule>
</CORSConfiguration>`

// corsTestServer 构造接了真实 store 且已建桶 bkt 的测试服务器。
func corsTestServer(t *testing.T) http.Handler {
	t.Helper()
	st, err := store.Open(store.Options{Root: filepath.Join(t.TempDir(), "data")})
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	if err := st.CreateBucket("bkt"); err != nil {
		t.Fatalf("建桶失败: %v", err)
	}
	return New(Options{
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Region:      "us-east-1",
		DisableAuth: true,
		Store:       st,
	}).Handler()
}

// corsRequest 发一个带自定义头的请求。
func corsRequest(t *testing.T, h http.Handler, method, target string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// putCORS 写入测试用 CORS 配置，失败即终止。
func putCORS(t *testing.T, h http.Handler) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "/bkt?cors", strings.NewReader(testCORSXML))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT ?cors 状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}
}

func TestBucketCORSLifecycleOverHTTP(t *testing.T) {
	h := corsTestServer(t)
	putCORS(t, h)

	// GET 必须原样回传配置。
	rec := do(t, h, http.MethodGet, "/bkt?cors")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET ?cors 状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}
	var got corsConfigurationXML
	if err := xml.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("响应体不是合法 XML: %v (%s)", err, rec.Body.String())
	}
	if len(got.Rules) != 1 || got.Rules[0].ID != "ui" {
		t.Fatalf("规则内容不符: %+v", got.Rules)
	}
	if len(got.Rules[0].AllowedOrigins) != 2 || len(got.Rules[0].AllowedMethods) != 2 {
		t.Errorf("来源/方法往返不一致: %+v", got.Rules[0])
	}
	if got.Rules[0].MaxAgeSeconds == nil || *got.Rules[0].MaxAgeSeconds != 600 {
		t.Errorf("MaxAgeSeconds 往返失败: %+v", got.Rules[0].MaxAgeSeconds)
	}

	// DELETE 返回 204，且可重复调用。
	for i := 0; i < 2; i++ {
		if rec := do(t, h, http.MethodDelete, "/bkt?cors"); rec.Code != http.StatusNoContent {
			t.Fatalf("第 %d 次 DELETE ?cors 状态码 = %d，期望 204", i+1, rec.Code)
		}
	}

	// 删除后再 GET：404 NoSuchCORSConfiguration（不是 NoSuchBucket）。
	rec = do(t, h, http.MethodGet, "/bkt?cors")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("未配置时 GET ?cors 状态码 = %d，期望 404", rec.Code)
	}
	if body := decodeError(t, rec); body.Code != string(s3err.CodeNoSuchCORSConfiguration) {
		t.Errorf("错误码 = %q，期望 NoSuchCORSConfiguration", body.Code)
	}
}

// TestBucketCORSValidation 覆盖 PutBucketCors 的文档校验。
//
// 校验失败必须回 400 MalformedXML 且**不落盘** —— 后者用随后的一次
// GET 断言：坏请求不能把已有配置清掉，也不能留下半份配置。
func TestBucketCORSValidation(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"空文档", ``},
		{"根元素错误", `<CORS><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORS>`},
		{"没有规则", `<CORSConfiguration></CORSConfiguration>`},
		{"缺少 Origin", `<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>`},
		{"缺少 Method", `<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin></CORSRule></CORSConfiguration>`},
		{"方法不在白名单", `<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>PATCH</AllowedMethod></CORSRule></CORSConfiguration>`},
		{"Origin 两个通配符", `<CORSConfiguration><CORSRule><AllowedOrigin>http://*.*.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>`},
		{"MaxAge 为负", `<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod><MaxAgeSeconds>-1</MaxAgeSeconds></CORSRule></CORSConfiguration>`},
		{"根元素后有内容", `<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration><CORSConfiguration></CORSConfiguration>`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := corsTestServer(t)
			putCORS(t, h)

			req := httptest.NewRequest(http.MethodPut, "/bkt?cors", strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("状态码 = %d，期望 400 (%s)", rec.Code, rec.Body.String())
			}
			if body := decodeError(t, rec); body.Code != string(s3err.CodeMalformedXML) {
				t.Errorf("错误码 = %q，期望 MalformedXML", body.Code)
			}
			// 原有配置必须保持不变。
			if rec := do(t, h, http.MethodGet, "/bkt?cors"); rec.Code != http.StatusOK {
				t.Errorf("坏请求清掉了已有配置，GET 状态码 = %d", rec.Code)
			}
		})
	}
}

// TestBucketCORSValidationRuleLimit 验证 100 条上限（单独建文档，避免表驱动里拼字符串）。
func TestBucketCORSValidationRuleLimit(t *testing.T) {
	h := corsTestServer(t)
	var b strings.Builder
	b.WriteString("<CORSConfiguration>")
	for i := 0; i < maxCORSRules+1; i++ {
		b.WriteString("<CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule>")
	}
	b.WriteString("</CORSConfiguration>")

	req := httptest.NewRequest(http.MethodPut, "/bkt?cors", strings.NewReader(b.String()))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("超过 100 条规则时状态码 = %d，期望 400", rec.Code)
	}
}

// TestBucketCORSScopeAndMethod 验证 ?cors 只接受桶级 PUT/GET/DELETE。
func TestBucketCORSScopeAndMethod(t *testing.T) {
	h := corsTestServer(t)

	// 带 key 或空桶名的 ?cors 是未定义组合：501，而不是落到对象读写。
	for _, target := range []string{"/bkt/obj.txt?cors", "/?cors"} {
		if rec := do(t, h, http.MethodPut, target); rec.Code != http.StatusNotImplemented {
			t.Errorf("%s 状态码 = %d，期望 501", target, rec.Code)
		}
	}
	// 桶级但不支持的方法：405。
	if rec := do(t, h, http.MethodPost, "/bkt?cors"); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST ?cors 状态码 = %d，期望 405", rec.Code)
	}
	// 桶不存在：404 NoSuchBucket。
	req := httptest.NewRequest(http.MethodPut, "/missing?cors", strings.NewReader(testCORSXML))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("向不存在的桶写配置状态码 = %d，期望 404", rec.Code)
	}
	if body := decodeError(t, rec); body.Code != string(s3err.CodeNoSuchBucket) {
		t.Errorf("错误码 = %q，期望 NoSuchBucket", body.Code)
	}
}

// TestPreflightSuccess 验证预检成功时的全套响应头。
func TestPreflightSuccess(t *testing.T) {
	h := corsTestServer(t)
	putCORS(t, h)

	rec := corsRequest(t, h, http.MethodOptions, "/bkt/obj.txt", map[string]string{
		"Origin":                         "http://localhost:5173",
		"Access-Control-Request-Method":  "PUT",
		"Access-Control-Request-Headers": "x-amz-date, x-amz-content-sha256",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("预检状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}
	want := map[string]string{
		"Access-Control-Allow-Origin":      "http://localhost:5173",
		"Access-Control-Allow-Credentials": "true",
		"Access-Control-Allow-Methods":     "GET, PUT",
		"Access-Control-Allow-Headers":     "x-amz-date, x-amz-content-sha256",
		"Access-Control-Expose-Headers":    "ETag",
		"Access-Control-Max-Age":           "600",
	}
	for k, v := range want {
		if got := rec.Header().Get(k); got != v {
			t.Errorf("%s = %q，期望 %q", k, got, v)
		}
	}
	if vary := rec.Header().Get("Vary"); !strings.Contains(vary, "Origin") ||
		!strings.Contains(vary, "Access-Control-Request-Method") {
		t.Errorf("Vary = %q", vary)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("预检成功不应有响应体，实际 %d 字节", rec.Body.Len())
	}
}

// TestPreflightWildcardOrigin 验证裸 * 源回 * 且不带凭据声明。
func TestPreflightWildcardOrigin(t *testing.T) {
	h := corsTestServer(t)
	body := `<CORSConfiguration><CORSRule>
		<AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod>
	</CORSRule></CORSConfiguration>`
	req := httptest.NewRequest(http.MethodPut, "/bkt?cors", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT ?cors 状态码 = %d (%s)", rec.Code, rec.Body.String())
	}

	rec = corsRequest(t, h, http.MethodOptions, "/bkt", map[string]string{
		"Origin":                        "https://anywhere.example.org",
		"Access-Control-Request-Method": "GET",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("通配源预检状态码 = %d，期望 200", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Access-Control-Allow-Origin = %q，期望 *", got)
	}
	// 通配源下浏览器禁止携带凭据，因此不得声明 credentials。
	if got := rec.Header().Get("Access-Control-Allow-Credentials"); got != "" {
		t.Errorf("通配源不应回 Access-Control-Allow-Credentials，实际 %q", got)
	}
}

// TestPreflightErrors 覆盖预检的各类拒绝路径。
func TestPreflightErrors(t *testing.T) {
	t.Run("缺 Origin", func(t *testing.T) {
		h := corsTestServer(t)
		rec := corsRequest(t, h, http.MethodOptions, "/bkt", map[string]string{
			"Access-Control-Request-Method": "GET",
		})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("状态码 = %d，期望 400", rec.Code)
		}
		if body := decodeError(t, rec); body.Code != string(s3err.CodeInvalidArgument) {
			t.Errorf("错误码 = %q", body.Code)
		}
	})
	t.Run("缺请求方法", func(t *testing.T) {
		h := corsTestServer(t)
		rec := corsRequest(t, h, http.MethodOptions, "/bkt", map[string]string{
			"Origin": "http://localhost:5173",
		})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("状态码 = %d，期望 400", rec.Code)
		}
	})
	t.Run("未配置 CORS", func(t *testing.T) {
		h := corsTestServer(t)
		rec := corsRequest(t, h, http.MethodOptions, "/bkt", map[string]string{
			"Origin":                        "http://localhost:5173",
			"Access-Control-Request-Method": "GET",
		})
		if rec.Code != http.StatusForbidden {
			t.Fatalf("状态码 = %d，期望 403", rec.Code)
		}
		body := decodeError(t, rec)
		if body.Code != string(s3err.CodeAccessForbidden) {
			t.Errorf("错误码 = %q，期望 AccessForbidden", body.Code)
		}
		if !strings.Contains(body.Message, "not enabled") {
			t.Errorf("消息 = %q，应说明该桶未启用 CORS", body.Message)
		}
	})
	t.Run("源不匹配", func(t *testing.T) {
		h := corsTestServer(t)
		putCORS(t, h)
		rec := corsRequest(t, h, http.MethodOptions, "/bkt", map[string]string{
			"Origin":                        "https://evil.example.net",
			"Access-Control-Request-Method": "GET",
		})
		if rec.Code != http.StatusForbidden {
			t.Fatalf("状态码 = %d，期望 403", rec.Code)
		}
		if body := decodeError(t, rec); !strings.Contains(body.Message, "not allowed") {
			t.Errorf("消息 = %q", body.Message)
		}
	})
	t.Run("方法不匹配", func(t *testing.T) {
		h := corsTestServer(t)
		putCORS(t, h)
		rec := corsRequest(t, h, http.MethodOptions, "/bkt", map[string]string{
			"Origin":                        "http://localhost:5173",
			"Access-Control-Request-Method": "DELETE",
		})
		if rec.Code != http.StatusForbidden {
			t.Fatalf("状态码 = %d，期望 403", rec.Code)
		}
	})
	t.Run("请求头不匹配", func(t *testing.T) {
		h := corsTestServer(t)
		putCORS(t, h)
		rec := corsRequest(t, h, http.MethodOptions, "/bkt", map[string]string{
			"Origin":                         "http://localhost:5173",
			"Access-Control-Request-Method":  "GET",
			"Access-Control-Request-Headers": "authorization",
		})
		if rec.Code != http.StatusForbidden {
			t.Fatalf("状态码 = %d，期望 403", rec.Code)
		}
	})
	t.Run("桶不存在", func(t *testing.T) {
		h := corsTestServer(t)
		rec := corsRequest(t, h, http.MethodOptions, "/missing", map[string]string{
			"Origin":                        "http://localhost:5173",
			"Access-Control-Request-Method": "GET",
		})
		if rec.Code != http.StatusNotFound {
			t.Fatalf("状态码 = %d，期望 404", rec.Code)
		}
	})
}

// TestActualRequestCORSHeaders 验证普通请求上的 CORS 头注入。
func TestActualRequestCORSHeaders(t *testing.T) {
	h := corsTestServer(t)
	putCORS(t, h)

	rec := corsRequest(t, h, http.MethodGet, "/bkt?list-type=2", map[string]string{
		"Origin": "https://console.example.com",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://console.example.com" {
		t.Errorf("Access-Control-Allow-Origin = %q", got)
	}
	if got := rec.Header().Get("Access-Control-Expose-Headers"); got != "ETag" {
		t.Errorf("Access-Control-Expose-Headers = %q", got)
	}
	// 实际请求不应出现预检专用头。
	if got := rec.Header().Get("Access-Control-Allow-Methods"); got != "" {
		t.Errorf("实际请求不应回 Allow-Methods，实际 %q", got)
	}

	// 不匹配的源：照常返回业务结果，只是不带 CORS 头。
	rec = corsRequest(t, h, http.MethodGet, "/bkt?list-type=2", map[string]string{
		"Origin": "https://evil.example.net",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("不匹配源的状态码 = %d，期望仍为 200", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("不匹配源不应回 Access-Control-Allow-Origin，实际 %q", got)
	}
}

// TestCORSBypassesAuthOnlyForPreflight 验证两条边界：
// 预检必须免签（浏览器不会带 Authorization），而真正的配置读写仍须签名。
func TestCORSBypassesAuthOnlyForPreflight(t *testing.T) {
	st, err := store.Open(store.Options{Root: filepath.Join(t.TempDir(), "data")})
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	if err := st.CreateBucket("bkt"); err != nil {
		t.Fatalf("建桶失败: %v", err)
	}
	if err := st.PutBucketCORS("bkt", &store.CORSConfiguration{Rules: []store.CORSRule{{
		AllowedOrigins: []string{"http://localhost:5173"},
		AllowedMethods: []string{"GET"},
	}}}); err != nil {
		t.Fatalf("写配置失败: %v", err)
	}
	h := New(Options{
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Region:   testRegion,
		Provider: sigv4.NewStaticProvider(sigv4.Credentials{AccessKeyID: testAK, SecretKey: testSK}),
		Store:    st,
	}).Handler()

	rec := corsRequest(t, h, http.MethodOptions, "/bkt/obj.txt", map[string]string{
		"Origin":                        "http://localhost:5173",
		"Access-Control-Request-Method": "PUT",
	})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("PUT 未在规则中，预检应 403，实际 %d", rec.Code)
	}
	rec = corsRequest(t, h, http.MethodOptions, "/bkt/obj.txt", map[string]string{
		"Origin":                        "http://localhost:5173",
		"Access-Control-Request-Method": "GET",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("免签预检状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}

	// 配置读写仍然要求签名。
	rec = do(t, h, http.MethodGet, "/bkt?cors")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("未签名 GET ?cors 状态码 = %d，期望 403", rec.Code)
	}
	if body := decodeError(t, rec); body.Code != string(s3err.CodeAccessDenied) {
		t.Errorf("错误码 = %q，期望 AccessDenied", body.Code)
	}
}

// TestCORSPatternMatches 覆盖单通配符匹配的边界。
func TestCORSPatternMatches(t *testing.T) {
	cases := []struct {
		pattern, value string
		fold           bool
		want           bool
	}{
		{"*", "https://a.example.com", false, true},
		{"https://a.example.com", "https://a.example.com", false, true},
		{"https://a.example.com", "https://A.example.com", false, false},
		{"https://A.example.com", "https://a.example.com", true, true},
		{"https://*.example.com", "https://a.example.com", false, true},
		{"https://*.example.com", "https://a.b.example.com", false, true},
		{"https://*.example.com", "https://example.com", false, false},
		{"https://*.example.com", "http://a.example.com", false, false},
		{"https://*.example.com", "https://a.example.com.evil.com", false, false},
		{"x-amz-*", "X-Amz-Date", true, true},
		{"x-amz-*", "authorization", true, false},
		{"*suffix", "xsuffix", false, true},
		{"pre*", "pre", false, true},
	}
	for _, tc := range cases {
		if got := corsPatternMatches(tc.pattern, tc.value, tc.fold); got != tc.want {
			t.Errorf("corsPatternMatches(%q, %q, fold=%v) = %v，期望 %v",
				tc.pattern, tc.value, tc.fold, got, tc.want)
		}
	}
}
