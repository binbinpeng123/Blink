//go:build integration

package integration

import (
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestPersistenceAcrossRestart 验证优雅重启后对象与未完成的分片会话仍然可用。
func TestPersistenceAcrossRestart(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")

	srv := startServer(t, serverOptions{dataDir: dataDir})
	c := newClient(t, srv)

	bucket := uniqueBucket("restart")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)

	body := bodyOf(256*1024, 31) // 走 CAS 块路径
	requireStatus(t, c.putObject(bucket, "persist/obj.bin", body, map[string]string{
		"x-amz-meta-run": "before-restart",
	}), http.StatusOK)

	const mpKey = "persist/in-flight.bin"
	uploadID := initiateMPU(t, c, bucket, mpKey)
	// 首片是完成时的非末片，必须 ≥ 5 MiB。
	part1 := bodyOf(5<<20, 32)
	partETag := uploadPart(t, c, bucket, mpKey, uploadID, 1, part1)

	// 优雅关闭：SIGTERM + 等待进程退出。
	srv.stop()

	restarted := startServer(t, serverOptions{dataDir: dataDir})
	c2 := newClient(t, restarted)

	t.Run("对象内容与元数据完整", func(t *testing.T) {
		res := c2.getObject(bucket, "persist/obj.bin", nil)
		requireStatus(t, res, http.StatusOK)
		if string(res.Body) != string(body) {
			t.Fatalf("重启后对象内容不一致：长度 %d vs %d", len(res.Body), len(body))
		}
		if got := res.Header.Get("x-amz-meta-run"); got != "before-restart" {
			t.Fatalf("重启后用户元数据 = %q", got)
		}
	})

	t.Run("未完成的分片会话保留", func(t *testing.T) {
		res := c2.do(http.MethodGet, c2.objectURL(bucket, mpKey),
			url.Values{"uploadId": {uploadID}}, nil, nil)
		requireStatus(t, res, http.StatusOK)
		got := mustXML[listPartsResult](t, res)
		if len(got.Parts) != 1 {
			t.Fatalf("重启后分片数 = %d，期望 1", len(got.Parts))
		}
		if unquoteETag(got.Parts[0].ETag) != unquoteETag(partETag) {
			t.Fatalf("重启后分片 ETag = %q，期望 %q", got.Parts[0].ETag, partETag)
		}

		// 会话可继续：补一个末片并提交。
		part2 := bodyOf(64*1024, 33)
		etag2 := uploadPart(t, c2, bucket, mpKey, uploadID, 2, part2)
		res = completeMPU(t, c2, bucket, mpKey, uploadID, []completePartReq{
			{PartNumber: 1, ETag: partETag},
			{PartNumber: 2, ETag: etag2},
		})
		requireStatus(t, res, http.StatusOK)

		want := append(append([]byte(nil), part1...), part2...)
		gotObj := c2.getObject(bucket, mpKey, nil)
		requireStatus(t, gotObj, http.StatusOK)
		if string(gotObj.Body) != string(want) {
			t.Fatalf("重启后完成的分片对象内容不一致：长度 %d vs %d", len(gotObj.Body), len(want))
		}
	})
}

// TestStartupRejectsIncompatibleDataDir 验证三类拒绝启动：
// 格式版本过高、元数据不是合法 JSON、非空目录缺少格式文件。
func TestStartupRejectsIncompatibleDataDir(t *testing.T) {
	t.Run("格式版本高于本二进制", func(t *testing.T) {
		dataDir := filepath.Join(t.TempDir(), "data")
		writeFile(t, filepath.Join(dataDir, "blink-meta.json"),
			`{"format_version":99,"created_at":"2026-01-01T00:00:00Z","node_id":"test"}`)

		fail := runServerExpectFailure(t, serverOptions{dataDir: dataDir})
		for _, want := range []string{"磁盘格式版本 99", "高于本二进制支持的 1"} {
			if !strings.Contains(fail.output, want) {
				t.Fatalf("启动失败输出缺少 %q：\n%s", want, fail.output)
			}
		}
	})

	t.Run("格式文件不是合法 JSON", func(t *testing.T) {
		dataDir := filepath.Join(t.TempDir(), "data")
		writeFile(t, filepath.Join(dataDir, "blink-meta.json"), "this is not json")

		fail := runServerExpectFailure(t, serverOptions{dataDir: dataDir})
		if !strings.Contains(fail.output, "不是合法 JSON") {
			t.Fatalf("启动失败输出缺少「不是合法 JSON」：\n%s", fail.output)
		}
	})

	t.Run("非空目录缺少格式文件", func(t *testing.T) {
		dataDir := filepath.Join(t.TempDir(), "data")
		writeFile(t, filepath.Join(dataDir, "someone-elses-file.txt"), "do not touch")

		fail := runServerExpectFailure(t, serverOptions{dataDir: dataDir})
		for _, want := range []string{"非空", "blink-meta.json"} {
			if !strings.Contains(fail.output, want) {
				t.Fatalf("启动失败输出缺少 %q：\n%s", want, fail.output)
			}
		}
	})
}

// writeFile 创建父目录并写入文件，用于构造启动前的磁盘状态。
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("创建目录失败: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("写入 %s 失败: %v", path, err)
	}
}
