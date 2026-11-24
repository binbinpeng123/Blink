package s3api

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// plainReader 遮住底层类型的 WriteTo，模拟"没有捷径的源"。
//
// io.Copy 先看 src 是否实现 WriterTo，再看 dst 是否实现 ReaderFrom。
// *strings.Reader / *bytes.Buffer / *os.File 都有 WriteTo，直接用它们
// 做源会让测试绕过被测方法 —— 这正是生产路径上 os.File.WriteTo 面对
// 非网络 writer 时退回 io.Copy 的那一段。
type plainReader struct{ io.Reader }

// fakeReaderFrom 是实现了 io.ReaderFrom 的 ResponseWriter 测试替身，
// 记录是否被调用以及拷贝了多少字节。
type fakeReaderFrom struct {
	hdr     http.Header
	body    bytes.Buffer
	status  int
	rfCalls int
}

func (f *fakeReaderFrom) Header() http.Header {
	if f.hdr == nil {
		f.hdr = http.Header{}
	}
	return f.hdr
}

func (f *fakeReaderFrom) Write(p []byte) (int, error) { return f.body.Write(p) }

func (f *fakeReaderFrom) WriteHeader(code int) { f.status = code }

func (f *fakeReaderFrom) ReadFrom(r io.Reader) (int64, error) {
	f.rfCalls++
	n, err := f.body.ReadFrom(r)
	return n, err
}

// TestStatusRecorderReadFromDelegates 验证 statusRecorder 把 ReadFrom
// 交给底层：不透传时 io.Copy 只能退回用户态缓冲拷贝，大对象下载会失去
// sendfile 零拷贝（见 docs/reviews/09 的实测数据）。
func TestStatusRecorderReadFromDelegates(t *testing.T) {
	under := &fakeReaderFrom{}
	rec := &statusRecorder{ResponseWriter: under}

	const payload = "0123456789abcdef"
	n, err := io.Copy(rec, plainReader{strings.NewReader(payload)})
	if err != nil {
		t.Fatalf("io.Copy 失败: %v", err)
	}
	if n != int64(len(payload)) {
		t.Fatalf("拷贝 %d 字节，期望 %d", n, len(payload))
	}
	if under.rfCalls != 1 {
		t.Errorf("底层 ReadFrom 调用 %d 次，期望 1 次", under.rfCalls)
	}
	if got := under.body.String(); got != payload {
		t.Errorf("底层收到 %q，期望 %q", got, payload)
	}
	// 字节数只能记一次：ReadFrom 里已经累加，不能再被 Write 重复计入。
	if rec.written != int64(len(payload)) {
		t.Errorf("written = %d，期望 %d", rec.written, len(payload))
	}
	if rec.statusCode() != http.StatusOK {
		t.Errorf("状态码 = %d，期望 200", rec.statusCode())
	}
}

// TestStatusRecorderReadFromFallback 验证底层不支持 ReadFrom 时退回
// 普通拷贝，且不会因为 io.Copy 重新选中本方法而无限递归。
func TestStatusRecorderReadFromFallback(t *testing.T) {
	under := httptest.NewRecorder() // 不实现 io.ReaderFrom
	rec := &statusRecorder{ResponseWriter: under}

	const payload = "fallback-path"
	n, err := io.Copy(rec, plainReader{strings.NewReader(payload)})
	if err != nil {
		t.Fatalf("io.Copy 失败: %v", err)
	}
	if n != int64(len(payload)) {
		t.Fatalf("拷贝 %d 字节，期望 %d", n, len(payload))
	}
	if got := under.Body.String(); got != payload {
		t.Errorf("响应体 %q，期望 %q", got, payload)
	}
	if rec.written != int64(len(payload)) {
		t.Errorf("written = %d，期望 %d（不能重复计数）", rec.written, len(payload))
	}
}

// TestStatusRecorderReadFromNested 验证两层包装（withRequestContext 与
// withAudit 各一层）时 request 与 audit 的字节数都能拿到，且数据只写一次。
func TestStatusRecorderReadFromNested(t *testing.T) {
	under := &fakeReaderFrom{}
	outer := &statusRecorder{ResponseWriter: under}
	inner := &statusRecorder{ResponseWriter: outer}

	const payload = "nested"
	if _, err := io.Copy(inner, plainReader{strings.NewReader(payload)}); err != nil {
		t.Fatalf("io.Copy 失败: %v", err)
	}
	for name, rec := range map[string]*statusRecorder{"inner": inner, "outer": outer} {
		if rec.written != int64(len(payload)) {
			t.Errorf("%s written = %d，期望 %d", name, rec.written, len(payload))
		}
	}
	if under.body.String() != payload {
		t.Errorf("底层收到 %q，期望 %q", under.body.String(), payload)
	}
	if under.rfCalls != 1 {
		t.Errorf("底层 ReadFrom 调用 %d 次，期望 1 次", under.rfCalls)
	}
}

// failingReader 是读到一半就报错的源，用于验证错误原样传播。
type failingReader struct {
	sent bool
}

func (f *failingReader) Read(p []byte) (int, error) {
	if !f.sent {
		f.sent = true
		return copy(p, "partial"), nil
	}
	return 0, errTestRead
}

var errTestRead = errors.New("测试读错误")

// TestStatusRecorderReadFromPropagatesError 验证 ReadFrom 的错误不被吞掉，
// 且已写出的字节数仍然记账（审计日志需要知道实际发出了多少）。
func TestStatusRecorderReadFromPropagatesError(t *testing.T) {
	under := &fakeReaderFrom{}
	rec := &statusRecorder{ResponseWriter: under}

	n, err := rec.ReadFrom(&failingReader{})
	if !errors.Is(err, errTestRead) {
		t.Fatalf("错误 = %v，期望 %v", err, errTestRead)
	}
	if n != int64(len("partial")) {
		t.Errorf("返回 %d 字节，期望 %d", n, len("partial"))
	}
	if rec.written != int64(len("partial")) {
		t.Errorf("written = %d，期望 %d", rec.written, len("partial"))
	}
}
