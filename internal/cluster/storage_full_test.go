package cluster

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/binbinpeng123/Blink/internal/store"
)

// fullDisk 是一个恒为 99% 已用的容量读数。
func fullDisk(string) (uint64, uint64, error) { return 100, 1, nil }

// TestWriteStorageFullWhenAllReplicasFull 验证满盘写穿透到协调者。
//
// 这条链路有三段：本地目标返回 ErrStorageFull、远端目标经内部协议
// 变 507、协调者把两者重新归并为 ErrStorageFull。任何一段断掉，
// 客户端拿到的都会是 503（"重试可能成功"），而满盘重试永远不会成功。
func TestWriteStorageFullWhenAllReplicasFull(t *testing.T) {
	nodes := newTestNodesFull(t, 3, 1024, nil, func(o *store.Options) {
		o.HighWatermark = 0.90
		o.LowWatermark = 0.85
		o.DiskSpace = fullDisk
	})
	cl := nodes[0].cl

	if err := cl.CreateBucket("bkt"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	body := bytes.Repeat([]byte("x"), 4096)
	_, err := cl.PutObject("bkt", "big.bin", bytes.NewReader(body), store.PutOptions{ExpectedSize: int64(len(body))})
	if !errors.Is(err, store.ErrStorageFull) {
		t.Fatalf("所有副本满盘时写入错误 = %v，期望 ErrStorageFull", err)
	}
}

// TestIsStorageFull 固定"满盘"的识别口径。
func TestIsStorageFull(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"本地满盘", store.ErrStorageFull, true},
		{"包装后的本地满盘", errors.Join(errors.New("上下文"), store.ErrStorageFull), true},
		{"远端 507", &PeerError{Peer: "n2", Status: http.StatusInsufficientStorage}, true},
		{"远端 503", &PeerError{Peer: "n2", Status: http.StatusServiceUnavailable}, false},
		{"普通错误", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := isStorageFull(c.err); got != c.want {
				t.Errorf("isStorageFull(%v) = %v，期望 %v", c.err, got, c.want)
			}
		})
	}
}

// TestInternalStorageErrorStatus 验证内部端口的错误码映射。
//
// 协调者只按状态码分类：507 触发"满盘"判定，503 触发"节点故障"。
// 两者混淆会让客户端在满盘时无限重试。
func TestInternalStorageErrorStatus(t *testing.T) {
	s := &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	cases := []struct {
		name     string
		err      error
		want     int
		wantBody string
	}{
		{"满盘", store.ErrStorageFull, http.StatusInsufficientStorage, "storage_full"},
		{"暂时不可用", store.ErrUnavailable, http.StatusServiceUnavailable, "unavailable"},
		{"数据损坏", store.ErrCorrupt, http.StatusInternalServerError, "corrupt"},
		{"未知错误", errors.New("boom"), http.StatusInternalServerError, "internal_error"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			s.writeStoreError(rec, "put-block", c.err)
			if rec.Code != c.want {
				t.Errorf("状态码 = %d，期望 %d", rec.Code, c.want)
			}
			if !strings.Contains(rec.Body.String(), c.wantBody) {
				t.Errorf("响应体 %q 未包含 %q", rec.Body.String(), c.wantBody)
			}
		})
	}
}
