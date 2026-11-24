package s3api

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/binbinpeng123/Blink/internal/metrics"
	"github.com/binbinpeng123/Blink/internal/store"
)

// TestGetObjectCountsResponseBytesOnce 验证大对象 GET 的响应字节只被
// 记一次。
//
// 背景：statusRecorder 透传 io.ReaderFrom 后，io.Copy 会把数据直接交给
// 底层 ResponseWriter；如果 ReadFrom 里既累加返回值、又靠 Write 累加，
// 审计与 blink_http_response_bytes_total 就会翻倍。这类错误不会让任何
// 功能测试失败，只有对着数字看才看得出来。
func TestGetObjectCountsResponseBytesOnce(t *testing.T) {
	reg := metrics.NewRegistry()
	st, err := store.Open(store.Options{Root: t.TempDir(), BlockSize: 1024, InlineThreshold: 1024})
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	if err := st.CreateBucket("bkt"); err != nil {
		t.Fatalf("建桶失败: %v", err)
	}
	// 10 KiB 跨多个块，确保走的是 copyBlockSegment 而不是内联快路径。
	body := bytes.Repeat([]byte("0123456789"), 1024)
	if _, err := st.PutObject("bkt", "big.bin", bytes.NewReader(body), store.PutOptions{
		ExpectedSize: int64(len(body)),
	}); err != nil {
		t.Fatalf("写入对象失败: %v", err)
	}

	h := New(Options{
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Region:      testRegion,
		Provider:    testProvider(),
		Metrics:     reg,
		DisableAuth: true,
		Store:       st,
	}).Handler()

	// 先打一次 GET：指标在请求处理之后才记录。
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/bkt/big.bin", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET 状态码 = %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Body.Len() != len(body) {
		t.Fatalf("响应体 %d 字节，期望 %d", rec.Body.Len(), len(body))
	}

	// httptest.ResponseRecorder 不实现 io.ReaderFrom，走的是降级分支；
	// 这里断言的是"降级时也不会重复计数"。
	var buf bytes.Buffer
	reg.Write(&buf)
	want := fmt.Sprintf(`blink_http_response_bytes_total{method="GET"} %d`, len(body))
	if !strings.Contains(buf.String(), want) {
		t.Errorf("响应字节计数不正确，期望包含 %q\n%s", want, buf.String())
	}
}

// TestGetObjectStreamsThroughReaderFrom 用真实 TCP 验证大对象 GET 在
// 中间件链下仍能走 ReadFrom 快路径。
//
// 判定方式：底层 ResponseWriter 是否被调用过 ReadFrom。用 httptest 的
// 真实 server 而不是 ResponseRecorder —— 后者没有 ReadFrom，测不出
// 这条路径。
func TestGetObjectStreamsThroughReaderFrom(t *testing.T) {
	st, err := store.Open(store.Options{Root: t.TempDir(), BlockSize: 1024, InlineThreshold: 1024})
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	if err := st.CreateBucket("bkt"); err != nil {
		t.Fatalf("建桶失败: %v", err)
	}
	body := bytes.Repeat([]byte("abcdefghij"), 2048)
	if _, err := st.PutObject("bkt", "big.bin", bytes.NewReader(body), store.PutOptions{
		ExpectedSize: int64(len(body)),
	}); err != nil {
		t.Fatalf("写入对象失败: %v", err)
	}

	var sawReadFrom bool
	h := New(Options{
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Region:      testRegion,
		Provider:    testProvider(),
		DisableAuth: true,
		Store:       st,
	}).Handler()
	// 在真实 ResponseWriter 之外再套一层探针，记录最外层中间件是否
	// 调用了 ReadFrom。探针自身透传 ReadFrom，不改变被测行为。
	probe := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pw := &readFromProbe{ResponseWriter: w, hit: &sawReadFrom}
		h.ServeHTTP(pw, r)
	})

	ts := httptest.NewServer(probe)
	defer ts.Close()

	resp, err := ts.Client().Get(ts.URL + "/bkt/big.bin")
	if err != nil {
		t.Fatalf("GET 失败: %v", err)
	}
	got, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("读取响应体失败: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("响应体与对象内容不一致：got %d 字节, want %d", len(got), len(body))
	}
	if !sawReadFrom {
		t.Error("响应未经过 ReadFrom：中间件链把零拷贝快路径挡住了")
	}
}

// readFromProbe 记录 ReadFrom 是否被调用并透传。
type readFromProbe struct {
	http.ResponseWriter
	hit *bool
}

func (p *readFromProbe) ReadFrom(r io.Reader) (int64, error) {
	*p.hit = true
	if rf, ok := p.ResponseWriter.(io.ReaderFrom); ok {
		return rf.ReadFrom(r)
	}
	return io.Copy(struct{ io.Writer }{p.ResponseWriter}, r)
}
