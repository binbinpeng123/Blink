package bench

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"

	"github.com/binbinpeng123/Blink/internal/s3api"
	"github.com/binbinpeng123/Blink/internal/store"
)

// httpBenchObjectSize 是 HTTP 端到端基准的对象大小。
//
// 取 16 MiB（4 个默认块）而不是 read_bench 的 64 MiB：这里量的是
// 响应写出路径，块数据必须完整走过 socket；16 MiB 已足够让"用户态
// 拷贝 vs sendfile"的差异明显，同时把临时目录占用控制在数十 MiB。
const httpBenchObjectSize = 16 << 20

// BenchmarkHTTPGetObject 走完整的 s3api 中间件链与真实 TCP 回环，
// 测量整对象 GET 的端到端吞吐。
//
// 为什么不能用 test/bench 里其它基准替代：那些基准把响应写进
// io.Discard，量的是存储读路径。而 `io.Copy(w, f)` 能否走 sendfile
// 零拷贝，取决于中间件包装后的 w 是否**透传 io.ReaderFrom** ——
// 这是 HTTP 层的问题，只有经过真实 ResponseWriter 与真实 socket
// 才能观察到。数据见 docs/reviews/09。
func BenchmarkHTTPGetObject(b *testing.B) {
	st, err := store.Open(store.Options{Root: b.TempDir()})
	if err != nil {
		b.Fatalf("打开存储失败: %v", err)
	}
	if err := st.CreateBucket("bench"); err != nil {
		b.Fatalf("建桶失败: %v", err)
	}
	body := benchBody(httpBenchObjectSize)
	if _, err := st.PutObject("bench", "model.bin", bytes.NewReader(body), store.PutOptions{
		ExpectedSize: int64(len(body)),
	}); err != nil {
		b.Fatalf("写入基准对象失败: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := s3api.New(s3api.Options{
		Logger:      logger,
		Region:      "us-east-1",
		DisableAuth: true,
		Store:       st,
	}).Handler()
	ts := httptest.NewServer(h)
	defer ts.Close()

	client := ts.Client()
	target := fmt.Sprintf("%s/bench/model.bin", ts.URL)

	b.SetBytes(httpBenchObjectSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resp, err := client.Get(target)
		if err != nil {
			b.Fatalf("GET 失败: %v", err)
		}
		n, err := io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if err != nil {
			b.Fatalf("读取响应体失败: %v", err)
		}
		if n != httpBenchObjectSize {
			b.Fatalf("响应体 %d 字节，期望 %d", n, httpBenchObjectSize)
		}
	}
}
