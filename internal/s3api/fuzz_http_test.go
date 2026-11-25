package s3api

import (
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/binbinpeng123/Blink/internal/s3err"
	"github.com/binbinpeng123/Blink/internal/store"
)

// 本文件是 handler 级端到端模糊测试：把完整 HTTP 请求（方法、路径、查询串、
// 头部块、请求体）送进 Server.Handler()，走完中间件链、路由、handler 与存储。
//
// 解析函数的 fuzz 覆盖不到"组合错误"：某个头只在路由之后才被读取、某条错误
// 分支忘了写 XML、某个分支在部分写出响应后 panic。这些只有全链路才能暴露。
//
// 规模护栏（请求体 ≤1 KiB、路径/查询串 ≤512 B、头块 ≤1 KiB）不是装饰：
// 没有上限时 fuzzer 会把整段时长耗在最小化巨大输入上，也会让临时目录按
// 迭代次数无界增长。
//
// 有界 fuzz：
//
//	go test -run=^$ -fuzz=^FuzzServeHTTP$ -fuzztime=20s ./internal/s3api

const (
	fuzzHTTPMaxBody        = 1 << 10
	fuzzHTTPMaxTarget      = 512
	fuzzHTTPMaxHeaderBlock = 1 << 10
)

// fuzzHTTPAllowedStatuses 是 S3 接口可能返回的全部状态码。
//
// 出现集合外的码意味着某个分支写出了契约里没有的状态：要么是漏了错误映射，
// 要么是把 net/http 的兜底响应当成了 S3 响应。
var fuzzHTTPAllowedStatuses = map[int]bool{
	http.StatusOK:                           true, // 200
	http.StatusNoContent:                    true, // 204 DeleteObject / DeleteObjects
	http.StatusPartialContent:               true, // 206 Range
	http.StatusTemporaryRedirect:            true, // 307 仅来自 ServeMux 的路径规范化（保留方法）
	http.StatusNotModified:                  true, // 304 条件请求
	http.StatusBadRequest:                   true, // 400
	http.StatusForbidden:                    true, // 403
	http.StatusNotFound:                     true, // 404
	http.StatusMethodNotAllowed:             true, // 405
	http.StatusConflict:                     true, // 409
	http.StatusLengthRequired:               true, // 411
	http.StatusPreconditionFailed:           true, // 412
	http.StatusRequestEntityTooLarge:        true, // 413
	http.StatusRequestedRangeNotSatisfiable: true, // 416
	http.StatusNotImplemented:               true, // 501
	http.StatusServiceUnavailable:           true, // 503
	http.StatusInsufficientStorage:          true, // 507
}

// fuzzHTTPNonS3Paths 是返回 JSON 的非 S3 端点：它们的错误体不是 S3 XML，
// 不适用"错误体必须是 <Error>"这条不变量。
var fuzzHTTPNonS3Paths = map[string]bool{
	"/healthz": true,
	"/readyz":  true,
	"/version": true,
	"/metrics": true,
}

// FuzzServeHTTP 校验全链路在任意请求下的三条契约：不 panic、状态码在集合内、
// 错误响应始终是合法的 S3 XML 错误体。
func FuzzServeHTTP(f *testing.F) {
	root, err := os.MkdirTemp("", "blink-fuzz-http-*")
	if err != nil {
		f.Fatalf("创建临时存储目录失败: %v", err)
	}
	defer os.RemoveAll(root)

	// 阈值与块大小取同一值：请求体上限 1 KiB 全部走内联，不产生 CAS 块文件。
	// 这让"用随机路径写对象"的迭代只影响元数据，不会把磁盘写入放大成
	// 与迭代次数成正比的块文件。
	st, err := store.Open(store.Options{
		Root:            filepath.Join(root, "data"),
		BlockSize:       64 << 10,
		InlineThreshold: 64 << 10,
	})
	if err != nil {
		f.Fatalf("打开存储失败: %v", err)
	}
	if err := st.CreateBucket("bkt"); err != nil {
		f.Fatalf("建桶失败: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := New(Options{
		Logger:      logger,
		Region:      "us-east-1",
		DisableAuth: true,
		Store:       st,
	}).Handler()

	// 种子语料按"一条主链路一个请求"组织。普通 go test 会把它们当子测试跑一遍，
	// 因此这里也是这些组合的最小回归集。
	for _, s := range []struct {
		name    string
		method  string
		path    string
		query   string
		headers string
		body    string
	}{
		{"列出桶", http.MethodGet, "/", "", "", ""},
		{"列出对象", http.MethodGet, "/bkt", "list-type=2&delimiter=%2F", "", ""},
		{"建桶", http.MethodPut, "/bkt2", "", "", ""},
		{"删桶", http.MethodDelete, "/bkt3", "", "", ""},
		{"桶头", http.MethodHead, "/bkt", "", "", ""},
		{"写对象", http.MethodPut, "/bkt/hello.txt", "", "", "hello"},
		{"读对象", http.MethodGet, "/bkt/hello.txt", "", "", ""},
		{"对象头", http.MethodHead, "/bkt/hello.txt", "", "", ""},
		{"Range", http.MethodGet, "/bkt/hello.txt", "", "Range: bytes=0-1\n", ""},
		{"条件读", http.MethodGet, "/bkt/hello.txt", "", "If-None-Match: *\n", ""},
		{"删对象", http.MethodDelete, "/bkt/hello.txt", "", "", ""},
		{"批量删除", http.MethodPost, "/bkt", "delete",
			"", `<Delete><Object><Key>a.txt</Key></Object></Delete>`},
		{"初始化分片", http.MethodPost, "/bkt/mpu.bin", "uploads", "", ""},
		{"列出分片会话", http.MethodGet, "/bkt", "uploads", "", ""},
		{"不存在的会话", http.MethodPost, "/bkt/mpu.bin", "uploadId=missing", "", ""},
		{"复制", http.MethodPut, "/bkt/copy.txt", "",
			"x-amz-copy-source: /bkt/hello.txt\n", ""},
		{"ACL 头", http.MethodPut, "/bkt/acl.txt", "", "x-amz-acl: public-read\n", ""},
		{"ACL 子资源", http.MethodGet, "/bkt/hello.txt", "acl", "", ""},
		{"版本子资源", http.MethodGet, "/bkt/hello.txt", "versionId=1", "", ""},
		{"CORS 预检", http.MethodOptions, "/bkt/hello.txt", "",
			"Origin: https://example.com\nAccess-Control-Request-Method: GET\n", ""},
		{"读 CORS", http.MethodGet, "/bkt", "cors", "", ""},
		{"写 CORS", http.MethodPut, "/bkt", "cors", "",
			`<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin>` +
				`<AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>`},
		{"aws-chunked", http.MethodPut, "/bkt/chunked.txt", "",
			"Content-Encoding: aws-chunked\n" +
				"x-amz-content-sha256: STREAMING-UNSIGNED-PAYLOAD\n" +
				"x-amz-decoded-content-length: 5\n",
			"5\r\nhello\r\n0\r\n\r\n"},
		{"深层键", http.MethodPut, "/bkt/a/b/c.txt", "", "", "deep"},
		{"健康检查", http.MethodGet, "/healthz", "", "", ""},
		{"版本端点", http.MethodGet, "/version", "", "", ""},
		{"指标端点", http.MethodGet, "/metrics", "", "", ""},
		// 下面三条是 fuzz 挖出的缺陷对应的最小回归语料。它们放在 seed 里而不是
		// testdata 语料文件里：语料文件对单请求无状态，删掉上下文后并不能独立
		// 复现问题，留在仓库只会让人误以为那里有测试。
		{"路径规范化重定向", http.MethodGet, "/..", "", "", ""},
		{"非法桶名 NUL", http.MethodDelete, "/bad\x00bucket", "", "", ""},
		{"非法 UTF-8 对象键", http.MethodPut, "/bkt/bad\xffkey", "", "", "x"},
	} {
		f.Add(s.method, s.path, s.query, s.headers, s.body)
	}

	f.Fuzz(func(t *testing.T, method, path, rawQuery, headerBlock, body string) {
		// 护栏：超限输入直接跳过，避免把时长耗在最小化与磁盘写入上。
		if len(body) > fuzzHTTPMaxBody || len(path) > fuzzHTTPMaxTarget ||
			len(rawQuery) > fuzzHTTPMaxTarget || len(headerBlock) > fuzzHTTPMaxHeaderBlock {
			t.Skip()
		}
		if method == "" {
			method = http.MethodGet
		}
		if !isHTTPToken(method) {
			t.Skip()
		}
		// origin-form 要求路径以 "/" 开头；补上前缀而不是跳过，
		// 否则 fuzzer 在随机路径上永远走不到路由。
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}

		req := &http.Request{
			Method:        method,
			URL:           &url.URL{Path: path, RawQuery: rawQuery},
			Header:        parseFuzzHeaderBlock(headerBlock),
			Body:          io.NopCloser(strings.NewReader(body)),
			ContentLength: int64(len(body)),
			Host:          "blink.test",
			RemoteAddr:    "127.0.0.1:1234",
			Proto:         "HTTP/1.1",
			ProtoMajor:    1,
			ProtoMinor:    1,
		}

		// recoverPanic 中间件应当兜住 handler 内的 panic 并转成 500；
		// 这里兜的是中间件自身（withRequestContext 等）的 panic —— 那说明
		// 兜底链断了。用 Errorf 而不是 Fatalf：defer 里 Fatalf 会再次
		// 触发运行时的退出路径。
		defer func() {
			if p := recover(); p != nil {
				t.Errorf("请求逃逸出未捕获的 panic：%v\nmethod=%q path=%q query=%q headers=%q body=%q",
					p, method, path, rawQuery, headerBlock, body)
			}
		}()

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if !fuzzHTTPAllowedStatuses[rec.Code] {
			t.Fatalf("状态码 %d 不在契约集合内：method=%q path=%q query=%q headers=%q body=%q response=%q",
				rec.Code, method, path, rawQuery, headerBlock, body, rec.Body.String())
		}
		if id := rec.Header().Get(headerRequestID); id == "" {
			t.Fatalf("响应缺少 %s：method=%q path=%q code=%d",
				headerRequestID, method, path, rec.Code)
		}
		if got := rec.Header().Get(headerServer); got != serverName {
			t.Fatalf("Server 头 = %q，期望 %q：path=%q", got, serverName, path)
		}

		// 204/304 按 RFC 9110 不得携带正文。
		if rec.Code == http.StatusNoContent || rec.Code == http.StatusNotModified {
			if rec.Body.Len() != 0 {
				t.Fatalf("状态 %d 携带了正文 %q：path=%q", rec.Code, rec.Body.String(), path)
			}
			return
		}

		if rec.Code < 400 || fuzzHTTPNonS3Paths[path] {
			return
		}
		// HEAD 的响应体在 writeXML 里被跳过（与真实 HTTP 语义一致），
		// 但 Content-Type 仍然会写，所以这里只跳过正文断言。
		if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/xml") {
			t.Fatalf("错误响应的 Content-Type = %q：method=%q path=%q code=%d",
				ct, method, path, rec.Code)
		}
		if method == http.MethodHead {
			return
		}

		var errBody s3ErrorBody
		if err := xml.Unmarshal(rec.Body.Bytes(), &errBody); err != nil {
			t.Fatalf("错误体不是合法 XML：%v\ncode=%d path=%q body=%q",
				err, rec.Code, path, rec.Body.String())
		}
		if errBody.Code == "" {
			t.Fatalf("错误体缺少 <Code>：code=%d path=%q body=%q",
				rec.Code, path, rec.Body.String())
		}
		// InternalError 意味着有 panic 被兜底、或存储未接入等内部故障。
		// 随机请求不应触发内部故障；触发即缺陷。
		if errBody.Code == string(s3err.CodeInternalError) {
			t.Fatalf("随机请求触发 InternalError：method=%q path=%q query=%q headers=%q body=%q message=%q",
				method, path, rawQuery, headerBlock, body, errBody.Message)
		}
		// 错误体里的 RequestId 必须与响应头一致：排障时两者会被交叉引用。
		if errBody.RequestID != rec.Header().Get(headerRequestID) {
			t.Fatalf("错误体 RequestId = %q 与响应头 %q 不一致：path=%q",
				errBody.RequestID, rec.Header().Get(headerRequestID), path)
		}
	})
}

// parseFuzzHeaderBlock 把 "Name: value" 行块解析成 http.Header。
//
// 与 net/http 的解析保持同样宽松：跳过空行、无冒号行与非法头名，值去首尾空白。
// 折行（以空格/Tab 开头）在这里直接丢弃而不是续行 —— 构造请求时没有
// 原始报文可依，续行只会让"哪一行属于哪个头"变得不确定。
func parseFuzzHeaderBlock(block string) http.Header {
	h := make(http.Header)
	for _, line := range strings.Split(block, "\n") {
		line = strings.TrimSuffix(line, "\r")
		if line == "" || line[0] == ' ' || line[0] == '\t' {
			continue
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok || !isHTTPToken(name) {
			continue
		}
		h.Add(name, strings.TrimSpace(value))
	}
	return h
}

// isHTTPToken 报告 s 是否为 RFC 9110 的 token（头名与方法名的字符集）。
func isHTTPToken(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0:
		default:
			return false
		}
	}
	return true
}
