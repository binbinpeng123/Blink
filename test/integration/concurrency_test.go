//go:build integration

package integration

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net/http"
	"sync"
	"testing"
)

// TestConcurrentSameKeyOverwrites 验证同 key 并发覆盖写不会产生撕裂对象：
// 最终读到的必须是某一个完整版本，且 ETag 与内容自洽。
func TestConcurrentSameKeyOverwrites(t *testing.T) {
	srv := startServer(t, serverOptions{})
	c := newClient(t, srv)

	bucket := uniqueBucket("conc")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)

	const (
		key     = "hot/object.bin"
		writers = 8
		rounds  = 6
	)

	// 所有可能写入的内容先在主 goroutine 生成，读者据此判断"没有撕裂"。
	versions := make([][]byte, 0, writers*rounds)
	for w := 0; w < writers; w++ {
		for r := 0; r < rounds; r++ {
			versions = append(versions, append(
				[]byte(fmt.Sprintf("writer-%02d-round-%02d:", w, r)),
				bodyOf(16*1024, byte(w*rounds+r))...,
			))
		}
	}
	valid := make(map[string]bool, len(versions))
	for _, v := range versions {
		valid[string(v)] = true
	}

	var wg sync.WaitGroup
	errCh := make(chan error, writers*rounds)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				body := versions[w*rounds+r]
				res, err := c.tryRequest(http.MethodPut, c.objectURL(bucket, key), nil, body, nil)
				if err != nil {
					errCh <- fmt.Errorf("writer %d round %d: %w", w, r, err)
					return
				}
				if res.Status != http.StatusOK {
					errCh <- fmt.Errorf("writer %d round %d: 状态码 %d", w, r, res.Status)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		res := c.getObject(bucket, key, nil)
		requireStatus(t, res, http.StatusOK)

		if !valid[string(res.Body)] {
			t.Fatalf("第 %d 次读取得到撕裂/未知内容：长度 %d，前 64 字节 %q",
				i, len(res.Body), res.Body[:minInt(64, len(res.Body))])
		}
		want := md5Hex(res.Body)
		if got := unquoteETag(res.Header.Get("ETag")); got != want {
			t.Fatalf("第 %d 次读取 ETag = %q，内容 MD5 = %q", i, got, want)
		}
	}
}

// TestConcurrentReadersDuringOverwrites 验证覆盖写期间并发读始终返回完整版本。
func TestConcurrentReadersDuringOverwrites(t *testing.T) {
	srv := startServer(t, serverOptions{})
	c := newClient(t, srv)

	bucket := uniqueBucket("concread")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)

	const key = "live/object.bin"
	seed := bodyOf(64*1024, 51)
	requireStatus(t, c.putObject(bucket, key, seed, nil), http.StatusOK)

	// 版本内容都基于固定基础体：前缀区分版本，尾部保持一致。
	const writerRounds = 20
	versions := make(map[string]bool, writerRounds+1)
	versions[string(seed)] = true
	for r := 0; r < writerRounds; r++ {
		v := append([]byte(fmt.Sprintf("v%02d:", r)), seed...)
		versions[string(v)] = true
	}

	var (
		wg      sync.WaitGroup
		errCh   = make(chan error, 8)
		done    = make(chan struct{})
		readers = 4
	)

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				res, err := c.tryRequest(http.MethodGet, c.objectURL(bucket, key), nil, nil, nil)
				if err != nil {
					errCh <- fmt.Errorf("reader %d: %w", i, err)
					return
				}
				if res.Status != http.StatusOK {
					errCh <- fmt.Errorf("reader %d: 状态码 %d", i, res.Status)
					return
				}
				if !versions[string(res.Body)] {
					errCh <- fmt.Errorf("reader %d: 读到撕裂内容（长度 %d）", i, len(res.Body))
					return
				}
			}
		}(i)
	}

	for r := 0; r < writerRounds; r++ {
		v := append([]byte(fmt.Sprintf("v%02d:", r)), seed...)
		res := c.putObject(bucket, key, v, nil)
		requireStatus(t, res, http.StatusOK)
	}
	close(done)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

// TestConcurrentDistinctKeys 验证不同 key 的并发写互不串扰。
func TestConcurrentDistinctKeys(t *testing.T) {
	srv := startServer(t, serverOptions{})
	c := newClient(t, srv)

	bucket := uniqueBucket("conckeys")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)

	const workers = 8
	const perWorker = 12

	var wg sync.WaitGroup
	errCh := make(chan error, workers*perWorker)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				key := fmt.Sprintf("keys/%02d/%03d.bin", w, i)
				body := append([]byte(fmt.Sprintf("%02d-%03d:", w, i)), bodyOf(32*1024, byte(w*perWorker+i))...)
				res, err := c.tryRequest(http.MethodPut, c.objectURL(bucket, key), nil, body, nil)
				if err != nil {
					errCh <- fmt.Errorf("%s: %w", key, err)
					return
				}
				if res.Status != http.StatusOK {
					errCh <- fmt.Errorf("%s: 状态码 %d", key, res.Status)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	for w := 0; w < workers; w++ {
		for i := 0; i < perWorker; i++ {
			key := fmt.Sprintf("keys/%02d/%03d.bin", w, i)
			want := append([]byte(fmt.Sprintf("%02d-%03d:", w, i)), bodyOf(32*1024, byte(w*perWorker+i))...)
			res := c.getObject(bucket, key, nil)
			requireStatus(t, res, http.StatusOK)
			if string(res.Body) != string(want) {
				t.Fatalf("%s 内容不一致：长度 %d，期望 %d", key, len(res.Body), len(want))
			}
		}
	}
}

func md5Hex(b []byte) string {
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:])
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
