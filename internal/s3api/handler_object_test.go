package s3api

import (
	"crypto/md5"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/binbinpeng123/Blink/internal/checksum"
	"github.com/binbinpeng123/Blink/internal/store"
)

// objectTestServer 构造一个接了真实 store 的测试服务器。
//
// 块大小压到 8 字节、内联阈值 4 字节：这样几十字节的请求体就能覆盖
// "内联快路径 + 多块切分"两条分支，而不必真的写 8 MiB。
func objectTestServer(t *testing.T) http.Handler {
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
	return New(Options{
		Logger:      logger,
		Region:      "us-east-1",
		DisableAuth: true,
		Store:       st,
	}).Handler()
}

// putObject 发一个 PUT 对象请求；mutate 用于按测试需要改写请求。
func putObject(t *testing.T, h http.Handler, target, body string, mutate func(*http.Request)) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, target, strings.NewReader(body))
	if mutate != nil {
		mutate(req)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func md5Hex(s string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(s)))
}

func TestPutObjectOverHTTP(t *testing.T) {
	h := objectTestServer(t)
	const body = "hello"

	rec := putObject(t, h, "/bkt/greeting.txt", body, func(r *http.Request) {
		r.Header.Set("Content-Type", "text/plain")
		r.Header.Set("x-amz-meta-Owner", "edge-01")
	})

	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}
	// ETag 必须是带引号的 MD5：不带引号的条件请求会在后续 If-Match 里失配。
	if got, want := rec.Header().Get("ETag"), `"`+md5Hex(body)+`"`; got != want {
		t.Errorf("ETag = %s，期望 %s", got, want)
	}
	if _, err := time.Parse(http.TimeFormat, rec.Header().Get("Last-Modified")); err != nil {
		t.Errorf("Last-Modified 不是合法 HTTP 时间: %q", rec.Header().Get("Last-Modified"))
	}
	// 没有声明校验和算法时，不得凭空回传一个。
	if v := rec.Header().Get("x-amz-checksum-crc32"); v != "" {
		t.Errorf("未声明算法却回传了校验和: %s", v)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("PUT 响应体应为空，实际 %q", rec.Body.String())
	}
}

// 客户端声明算法但未给值：服务端必须算出来并回传，
// 且回传值要与该算法对内容的真实结果一致。
func TestPutObjectEchoesComputedChecksum(t *testing.T) {
	h := objectTestServer(t)
	const body = "0123456789abcdefghij"

	rec := putObject(t, h, "/bkt/with-checksum", body, func(r *http.Request) {
		r.Header.Set("x-amz-sdk-checksum-algorithm", "CRC32C")
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d (%s)", rec.Code, rec.Body.String())
	}

	sum, _ := checksum.New(checksum.AlgCRC32C)
	if _, err := sum.Write([]byte(body)); err != nil {
		t.Fatalf("计算校验和失败: %v", err)
	}
	if got := rec.Header().Get("x-amz-checksum-crc32c"); got != sum.Encoded() {
		t.Errorf("回传校验和 = %q，期望 %q", got, sum.Encoded())
	}
}

// trailer 里的校验和只有在请求体读完后才可见，因此服务端必须先读体、
// 再从 r.Trailer 取；这里是该分支在 HTTP 层的回归点。
func TestPutObjectChecksumFromTrailer(t *testing.T) {
	h := objectTestServer(t)
	const body = "trailer-payload"

	sum, _ := checksum.New(checksum.AlgSHA256)
	_, _ = sum.Write([]byte(body))

	rec := putObject(t, h, "/bkt/trailer", body, func(r *http.Request) {
		r.Header.Set("x-amz-sdk-checksum-algorithm", "SHA256")
		r.Trailer = http.Header{"X-Amz-Checksum-Sha256": []string{sum.Encoded()}}
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d (%s)", rec.Code, rec.Body.String())
	}

	// 篡改 trailer：内容没变但客户端声明了错的值，必须拒绝。
	rec = putObject(t, h, "/bkt/trailer-bad", body, func(r *http.Request) {
		r.Header.Set("x-amz-sdk-checksum-algorithm", "SHA256")
		r.Trailer = http.Header{"X-Amz-Checksum-Sha256": []string{"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}}
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，期望 400", rec.Code)
	}
	if got := decodeError(t, rec).Code; got != "BadDigest" {
		t.Errorf("错误码 = %q，期望 BadDigest", got)
	}
}

func TestPutObjectWithoutContentLength(t *testing.T) {
	h := objectTestServer(t)
	rec := putObject(t, h, "/bkt/no-length", "abc", func(r *http.Request) {
		// Go 在 chunked 传输或无长度时把 ContentLength 置为 -1。
		r.ContentLength = -1
	})
	if rec.Code != http.StatusLengthRequired {
		t.Fatalf("状态码 = %d，期望 411", rec.Code)
	}
	if got := decodeError(t, rec).Code; got != "LengthRequired" {
		t.Errorf("错误码 = %q", got)
	}
}

func TestPutObjectPreconditionFailed(t *testing.T) {
	h := objectTestServer(t)
	if rec := putObject(t, h, "/bkt/existing", "first", nil); rec.Code != http.StatusOK {
		t.Fatalf("首次写入失败: %d (%s)", rec.Code, rec.Body.String())
	}

	rec := putObject(t, h, "/bkt/existing", "second", func(r *http.Request) {
		r.Header.Set("If-None-Match", "*")
	})
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("状态码 = %d，期望 412 (%s)", rec.Code, rec.Body.String())
	}
	if got := decodeError(t, rec).Code; got != "PreconditionFailed" {
		t.Errorf("错误码 = %q", got)
	}
}

// 不支持的条件写必须明确拒绝，不能静默降级成无条件覆盖。
func TestPutObjectUnsupportedPreconditions(t *testing.T) {
	h := objectTestServer(t)
	for _, hdr := range []string{"If-Match", "If-None-Match"} {
		t.Run(hdr, func(t *testing.T) {
			rec := putObject(t, h, "/bkt/cond", "x", func(r *http.Request) {
				r.Header.Set(hdr, `"some-etag"`)
			})
			if rec.Code != http.StatusNotImplemented {
				t.Fatalf("状态码 = %d，期望 501", rec.Code)
			}
		})
	}
}

func TestPutObjectAWSChunkedUnsigned(t *testing.T) {
	h := objectTestServer(t)
	// 请求体是分块框架；落盘内容必须是解帧后的 "hello"。
	body := "5\r\nhello\r\n0\r\n\r\n"
	rec := putObject(t, h, "/bkt/chunked", body, func(r *http.Request) {
		r.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD")
		r.Header.Set("Content-Encoding", "aws-chunked")
		r.Header.Set("x-amz-decoded-content-length", "5")
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}
	if got, want := rec.Header().Get("ETag"), `"`+md5Hex("hello")+`"`; got != want {
		t.Errorf("ETag = %s，期望 %s", got, want)
	}

	got := httptest.NewRecorder()
	h.ServeHTTP(got, httptest.NewRequest(http.MethodGet, "/bkt/chunked", nil))
	if got.Code != http.StatusOK {
		t.Fatalf("GET 状态码 = %d", got.Code)
	}
	if got.Body.String() != "hello" {
		t.Errorf("对象内容 = %q，期望 %q", got.Body.String(), "hello")
	}
	// Content-Encoding 里已被解掉的 aws-chunked 不得残留到对象元数据。
	if enc := got.Header().Get("Content-Encoding"); enc != "" {
		t.Errorf("Content-Encoding = %q，期望为空", enc)
	}
}

func TestPutObjectAWSChunkedTrailerChecksum(t *testing.T) {
	h := objectTestServer(t)
	body := "5\r\nhello\r\n0\r\nx-amz-checksum-crc32:NhCmhg==\r\n\r\n"
	rec := putObject(t, h, "/bkt/chunked-crc", body, func(r *http.Request) {
		r.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
		r.Header.Set("Content-Encoding", "aws-chunked")
		r.Header.Set("x-amz-decoded-content-length", "5")
		r.Header.Set("x-amz-sdk-checksum-algorithm", "CRC32")
		r.Header.Set("x-amz-trailer", "x-amz-checksum-crc32")
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("x-amz-checksum-crc32"); got != "NhCmhg==" {
		t.Errorf("响应校验和 = %q，期望 NhCmhg==", got)
	}

	// 内容与声明不符时必须拒绝，而不是"算完就忘"。
	bad := "5\r\nhello\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n"
	rec = putObject(t, h, "/bkt/chunked-bad-crc", bad, func(r *http.Request) {
		r.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
		r.Header.Set("Content-Encoding", "aws-chunked")
		r.Header.Set("x-amz-decoded-content-length", "5")
		r.Header.Set("x-amz-sdk-checksum-algorithm", "CRC32")
		r.Header.Set("x-amz-trailer", "x-amz-checksum-crc32")
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("错误校验和状态码 = %d，期望 400", rec.Code)
	}
	if got := decodeError(t, rec).Code; got != "BadDigest" {
		t.Errorf("错误码 = %q，期望 BadDigest", got)
	}
}

func TestPutObjectAWSChunkedRequiresStreamingHash(t *testing.T) {
	h := objectTestServer(t)
	// 只有 decoded-content-length 而没有 STREAMING- 哈希：框架与签名声明
	// 自相矛盾，不能猜客户端意图。
	rec := putObject(t, h, "/bkt/chunked-bad", "x", func(r *http.Request) {
		r.Header.Set("x-amz-decoded-content-length", "1")
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，期望 400", rec.Code)
	}
	if got := decodeError(t, rec).Code; got != "InvalidArgument" {
		t.Errorf("错误码 = %q", got)
	}
}

func TestPutObjectRejectsUnsupportedStorageClass(t *testing.T) {
	h := objectTestServer(t)
	rec := putObject(t, h, "/bkt/glacier", "x", func(r *http.Request) {
		r.Header.Set("x-amz-storage-class", "GLACIER")
	})
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("状态码 = %d，期望 501", rec.Code)
	}

	// STANDARD 是默认值，显式发送也必须接受。
	rec = putObject(t, h, "/bkt/standard", "x", func(r *http.Request) {
		r.Header.Set("x-amz-storage-class", "STANDARD")
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("STANDARD 被拒: %d (%s)", rec.Code, rec.Body.String())
	}
}

func TestPutObjectInvalidContentMD5(t *testing.T) {
	h := objectTestServer(t)
	// "not-base64!" 连解码都过不了：属于"值不合法"（InvalidDigest），
	// 而不是"内容对不上"（BadDigest）。
	rec := putObject(t, h, "/bkt/bad-md5", "x", func(r *http.Request) {
		r.Header.Set("Content-MD5", "not-base64!")
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，期望 400", rec.Code)
	}
	if got := decodeError(t, rec).Code; got != "InvalidDigest" {
		t.Errorf("错误码 = %q，期望 InvalidDigest", got)
	}
}

func TestPutObjectContentMD5Mismatch(t *testing.T) {
	h := objectTestServer(t)
	rec := putObject(t, h, "/bkt/md5-mismatch", "payload", func(r *http.Request) {
		// 合法格式但与内容不符。
		r.Header.Set("Content-MD5", "AAAAAAAAAAAAAAAAAAAAAA==")
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，期望 400 (%s)", rec.Code, rec.Body.String())
	}
	if got := decodeError(t, rec).Code; got != "BadDigest" {
		t.Errorf("错误码 = %q，期望 BadDigest", got)
	}
}

func TestPutObjectUnknownChecksumAlgorithm(t *testing.T) {
	h := objectTestServer(t)
	rec := putObject(t, h, "/bkt/unknown-alg", "x", func(r *http.Request) {
		r.Header.Set("x-amz-sdk-checksum-algorithm", "MD5")
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("状态码 = %d，期望 400", rec.Code)
	}
	if got := decodeError(t, rec).Code; got != "InvalidArgument" {
		t.Errorf("错误码 = %q，期望 InvalidArgument", got)
	}
}

func TestPutObjectNoSuchBucket(t *testing.T) {
	h := objectTestServer(t)
	rec := putObject(t, h, "/missing/key", "x", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("状态码 = %d，期望 404", rec.Code)
	}
	if got := decodeError(t, rec).Code; got != "NoSuchBucket" {
		t.Errorf("错误码 = %q，期望 NoSuchBucket", got)
	}
}

// 切片 7 之后 multipart 已实现，但其余未支持的子资源仍必须明确回 501，
// 而不是掉进泛化 404 或被当成普通对象操作静默执行。
func TestUnsupportedSubresourceStillNotImplemented(t *testing.T) {
	h := objectTestServer(t)
	cases := []struct {
		method string
		target string
	}{
		{http.MethodGet, "/bkt/key?acl"},
		{http.MethodPut, "/bkt/key?tagging"},
		{http.MethodGet, "/bkt?versioning"},
		// 分片下载（GetObject partNumber）仍不在本版本范围内。
		{http.MethodGet, "/bkt/key?partNumber=1"},
		// 单独的 HTTP 方法上没有 uploadId 语义。
		{http.MethodPatch, "/bkt/key?uploadId=abc"},
	}
	for _, c := range cases {
		rec := do(t, h, c.method, c.target)
		if rec.Code != http.StatusNotImplemented {
			t.Errorf("%s %s 状态码 = %d，期望 501", c.method, c.target, rec.Code)
		}
	}
}
