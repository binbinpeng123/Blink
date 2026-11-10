//go:build integration

package integration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// hashKey 复刻 store 的 key → 目录名映射（sha256 十六进制）。
//
// 集成测试刻意不 import internal/store，因此这里重新实现一次；
// 它是磁盘布局的一部分，改动会让测试立刻失败，正是想要的保护。
func hashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// objectMetaPath 返回对象 meta.json 的磁盘路径。
func objectMetaPath(dataDir, bucket, key string) string {
	h := hashKey(key)
	return filepath.Join(dataDir, "buckets", bucket, "objects", h[:2], h, "meta.json")
}

// TestCorruptObjectMeta 验证损坏的元数据被报成 500 而不是 404/空响应。
func TestCorruptObjectMeta(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	srv := startServer(t, serverOptions{dataDir: dataDir})
	c := newClient(t, srv)

	bucket := uniqueBucket("corrupt")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)
	const key = "corrupt/meta.bin"
	requireStatus(t, c.putObject(bucket, key, bodyOf(128*1024, 61), nil), http.StatusOK)

	srv.stop()

	metaPath := objectMetaPath(dataDir, bucket, key)
	if _, err := os.Stat(metaPath); err != nil {
		t.Fatalf("未找到对象元数据 %s: %v", metaPath, err)
	}
	writeFile(t, metaPath, "{ this is not valid json")

	restarted := startServer(t, serverOptions{dataDir: dataDir})
	c2 := newClient(t, restarted)

	t.Run("GET 返回 500 且说明损坏", func(t *testing.T) {
		res := c2.getObject(bucket, key, nil)
		e := requireS3Error(t, res, http.StatusInternalServerError, "InternalError")
		if !strings.Contains(e.Message, "存储层数据损坏") {
			t.Fatalf("错误消息 = %q，应说明存储层数据损坏", e.Message)
		}
	})

	t.Run("HEAD 返回 500", func(t *testing.T) {
		res := c2.headObject(bucket, key, nil)
		requireStatus(t, res, http.StatusInternalServerError)
	})

	t.Run("LIST 返回 500 而不是静默跳过", func(t *testing.T) {
		res := c2.do(http.MethodGet, c2.bucketURL(bucket),
			url.Values{"list-type": {"2"}}, nil, nil)
		e := requireS3Error(t, res, http.StatusInternalServerError, "InternalError")
		if !strings.Contains(e.Message, "存储层数据损坏") {
			t.Fatalf("错误消息 = %q，应说明存储层数据损坏", e.Message)
		}
	})
}

// TestCorruptCASDetectedByVerifyRead 验证 --verify-read 打开时，
// CAS 块位翻转会在发给客户端前被 CRC32C 检出。
//
// 由于响应头已经发出，HTTP 状态仍是 200，客户端通过
// Content-Length 与实际字节数不符（unexpected EOF）发现传输失败。
func TestCorruptCASDetectedByVerifyRead(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	srv := startServer(t, serverOptions{dataDir: dataDir})
	c := newClient(t, srv)

	bucket := uniqueBucket("cas")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)
	const key = "corrupt/cas.bin"
	requireStatus(t, c.putObject(bucket, key, bodyOf(64*1024, 62), nil), http.StatusOK)

	srv.stop()

	metaPath := objectMetaPath(dataDir, bucket, key)
	raw, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("读取对象元数据失败: %v", err)
	}
	var meta struct {
		Blocks []struct {
			Hash string `json:"hash"`
		} `json:"blocks"`
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatalf("解析对象元数据失败: %v", err)
	}
	if len(meta.Blocks) == 0 {
		t.Fatalf("对象元数据没有块引用: %s", truncate(raw))
	}

	h := meta.Blocks[0].Hash
	casPath := filepath.Join(dataDir, "cas", h[:2], h)
	data, err := os.ReadFile(casPath)
	if err != nil {
		t.Fatalf("读取 CAS 块失败: %v", err)
	}
	data[0] ^= 0xff // 翻转一个位，模拟磁盘位翻转
	if err := os.WriteFile(casPath, data, 0o644); err != nil {
		t.Fatalf("写回 CAS 块失败: %v", err)
	}

	restarted := startServer(t, serverOptions{
		dataDir: dataDir,
		args:    []string{"--verify-read"},
	})
	c2 := newClient(t, restarted)

	// 绕过 harness 的 io.ReadAll 断言：这里预期的就是读取失败。
	signed := c2.presignedURL(http.MethodGet, c2.objectURL(bucket, key), nil)
	resp, err := c2.hc.Get(signed)
	if err != nil {
		t.Fatalf("请求失败（预期先拿到 200 响应头）: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200（错误发生在响应头之后）", resp.StatusCode)
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr == nil {
		t.Fatalf("校验读本应因 CRC32C 不符中断传输，却成功读到 %d 字节", len(body))
	}

	// 服务端必须留下可诊断的日志，而不是静默截断。
	if log := restarted.logTail(200); !strings.Contains(log, "object_read_failed") {
		t.Fatalf("服务端日志缺少 object_read_failed：\n%s", log)
	}
}
