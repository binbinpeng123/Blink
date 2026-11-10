//go:build integration

package integration

import (
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestSIGKILLAfterAcknowledgedWrites 验证崩溃一致性：
// 只要客户端收到 200，SIGKILL 之后重启必须能读到完整内容。
//
// 默认 --fsync-policy=always，200 的语义就是"数据与元数据都已落盘"。
// 这里刻意在写入进行中直接 SIGKILL，而不是优雅关闭：优雅关闭会掩盖
// "依赖退出时刷盘"这类实现缺陷。
func TestSIGKILLAfterAcknowledgedWrites(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	srv := startServer(t, serverOptions{dataDir: dataDir})
	c := newClient(t, srv)

	bucket := uniqueBucket("crash")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)

	type write struct {
		key  string
		body []byte
	}

	var (
		mu     sync.Mutex
		acked  []write
		wg     sync.WaitGroup
		stopCh = make(chan struct{})
	)

	const workers = 8
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stopCh:
					return
				default:
				}
				key := fmt.Sprintf("crash/%02d/%04d.bin", w, i)
				body := bodyOf(64*1024, byte(w*37+i))
				res, err := c.tryRequest(http.MethodPut, c.objectURL(bucket, key), nil, body, nil)
				if err != nil {
					// 崩溃后连接被切断，进行中的请求在这里返回。
					return
				}
				if res.Status != http.StatusOK {
					// 崩溃窗口内的非 200 不是本测试的问题；
					// 真正要保证的是"200 即可读"。
					return
				}
				mu.Lock()
				acked = append(acked, write{key: key, body: body})
				mu.Unlock()
			}
		}(w)
	}

	// 给写入一点时间进入稳定状态，然后不给任何收尾机会直接 SIGKILL。
	time.Sleep(300 * time.Millisecond)
	srv.kill()
	close(stopCh)
	wg.Wait()

	if len(acked) == 0 {
		t.Fatalf("崩溃前没有任何写入被确认，测试没有覆盖到目标场景")
	}
	t.Logf("崩溃前已确认 %d 个对象", len(acked))

	restarted := startServer(t, serverOptions{dataDir: dataDir})
	c2 := newClient(t, restarted)

	for _, w := range acked {
		res := c2.getObject(bucket, w.key, nil)
		requireStatus(t, res, http.StatusOK)
		if string(res.Body) != string(w.body) {
			t.Fatalf("%s 崩溃后内容不一致：长度 %d，期望 %d", w.key, len(res.Body), len(w.body))
		}
	}
}

// TestSIGKILLDuringMultipart 验证崩溃时未完成的分片会话不会让对象出现，
// 且已上传分片的状态在重启后仍可继续使用或安全放弃。
func TestSIGKILLDuringMultipart(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	srv := startServer(t, serverOptions{dataDir: dataDir})
	c := newClient(t, srv)

	bucket := uniqueBucket("crashmpu")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)

	const key = "inflight.bin"
	uploadID := initiateMPU(t, c, bucket, key)
	part1 := bodyOf(5<<20, 41)
	etag1 := uploadPart(t, c, bucket, key, uploadID, 1, part1)

	srv.kill()

	restarted := startServer(t, serverOptions{dataDir: dataDir})
	c2 := newClient(t, restarted)

	t.Run("已确认的分片可继续访问", func(t *testing.T) {
		res := c2.do(http.MethodGet, c2.objectURL(bucket, key),
			url.Values{"uploadId": {uploadID}}, nil, nil)
		requireStatus(t, res, http.StatusOK)
		got := mustXML[listPartsResult](t, res)
		if len(got.Parts) != 1 || unquoteETag(got.Parts[0].ETag) != unquoteETag(etag1) {
			t.Fatalf("崩溃后分片状态异常：%+v", got.Parts)
		}
	})

	t.Run("未完成会话不产生对象", func(t *testing.T) {
		requireS3Error(t, c2.getObject(bucket, key, nil), http.StatusNotFound, "NoSuchKey")
	})
}
