package cluster

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/binbinpeng123/Blink/internal/store"
)

// testNode 是一个跑在本机上的集群节点：真实 HTTP 监听 + 独立数据目录。
//
// 用 httptest 而不是真实进程，是为了能在同一个测试里断言"某个节点
// 上的本地副本长什么样"；网络路径（签名、块传输、meta JSON）与
// 多进程部署完全一致。
type testNode struct {
	id  string
	cl  *Cluster
	st  *store.Store
	ts  *httptest.Server
	cfg Config
	one sync.Once

	// down 是"进程还在、但不再提供服务"的开关：内部请求直接回 503。
	// 用它而不是 ts.Close 模拟故障，是因为 ts.Close 之后端口不可复用，
	// 而故障恢复测试需要一个节点"原地重启"——内存里的存储必须保留。
	down atomic.Bool
	// handler 保存当前 Cluster 的内部 handler，支持原地重建 Cluster
	// （验证 hint 持久化跨进程重启）。
	handler atomic.Pointer[http.Handler]
}

// Close 幂等关闭监听与连接池；测试里会主动"杀掉"节点模拟故障。
func (n *testNode) Close() {
	n.one.Do(func() {
		n.ts.Close()
		n.cl.Close()
	})
}

// SetDown 模拟节点宕机 / 恢复。存储与监听保持不变：恢复后必须看到
// 宕机期间同一个数据目录，这正是"重启后修复补齐"要验证的场景。
func (n *testNode) SetDown(down bool) {
	n.down.Store(down)
}

// attachCluster 让这个节点对外提供指定 Cluster 的内部协议。
func (n *testNode) attachCluster(cl *Cluster) {
	n.cl = cl
	h := cl.InternalHandler()
	n.handler.Store(&h)
}

// newTestNodes 起 count 个互相认识的节点，N=3 / W=2 / R=2。
func newTestNodes(t *testing.T, count int) []*testNode {
	t.Helper()
	return newTestNodesWithBlockSize(t, count, 1024)
}

// newTestNodesWithBlockSize 与 newTestNodes 相同，但可指定块大小。
//
// MPU 测试必须传 ≥5 MiB 的非末分片；1 KiB 块会让一次分片变成五千多次
// 内部 HTTP 往返，测试时间与覆盖面积不成比例，因此那里改用 MiB 级块。
func newTestNodesWithBlockSize(t *testing.T, count int, blockSize int64) []*testNode {
	t.Helper()
	return newTestNodesWithOptions(t, count, blockSize, nil)
}

// newTestNodesWithOptions 允许在构造前微调集群配置（例如打开降级读、
// 关闭后台反熵扫描），用于验证单机测试不需要感知的修复参数。
func newTestNodesWithOptions(t *testing.T, count int, blockSize int64, tweak func(*Config)) []*testNode {
	t.Helper()
	return newTestNodesFull(t, count, blockSize, tweak, nil)
}

// newTestNodesFull 在 newTestNodesWithOptions 之上再允许微调节点存储选项，
// 用于注入容量读数等无法在运行期修改的构造参数。
func newTestNodesFull(t *testing.T, count int, blockSize int64, tweak func(*Config), storeTweak func(*store.Options)) []*testNode {
	t.Helper()
	if count < 3 {
		t.Fatalf("集成测试至少需要 3 个节点，当前 %d", count)
	}

	nodes := make([]*testNode, count)
	peers := make([]Peer, count)
	// 先把监听端口占住，再用真实地址构造成员表：环与 peer 客户端
	// 使用的地址必须与内部服务实际监听的地址一致。
	for i := 0; i < count; i++ {
		ts := httptest.NewUnstartedServer(http.NotFoundHandler())
		nodes[i] = &testNode{id: fmt.Sprintf("node-%d", i+1), ts: ts}
		peers[i] = Peer{ID: nodes[i].id, Addr: ts.Listener.Addr().String()}
	}

	secret := bytes.Repeat([]byte("cluster-secret-"), 3) // 45 字节
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	for _, n := range nodes {
		// 小块 + 禁用内联，让测试对象必然跨多个块，
		// 从而覆盖块切分、跨块 Range 与远端块拼装。
		stOpts := store.Options{
			Root:            filepath.Join(t.TempDir(), "data"),
			BlockSize:       blockSize,
			InlineThreshold: -1,
			VerifyOnRead:    true,
		}
		if storeTweak != nil {
			storeTweak(&stOpts)
		}
		st, err := store.Open(stOpts)
		if err != nil {
			t.Fatalf("打开节点 %s 的存储失败: %v", n.id, err)
		}
		cfg := Config{
			NodeID:       n.id,
			Peers:        peers,
			InternalAddr: n.ts.Listener.Addr().String(),
			Secret:       secret,
			ReplicaCount: 3,
			WriteQuorum:  2,
			ReadQuorum:   2,
			VNodes:       64,
			PeerTimeout:  time.Second,
			// 后台扫描默认 24h，测试里不会自然触发；显式关掉可以
			// 让"扫描修复"用例通过直接调用 scrubOnce 精确控制时机。
			ScrubInterval: -1,
		}
		if tweak != nil {
			tweak(&cfg)
		}
		cl, err := New(cfg, st, logger)
		if err != nil {
			t.Fatalf("构造节点 %s 的集群后端失败: %v", n.id, err)
		}
		n.st = st
		n.cfg = cfg
		n.ts.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if n.down.Load() {
				// 与真实进程"监听还在但存储不可用"的形态一致：
				// 对端拿到的是一个明确的 503，而不是连接超时。
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error":"node_down"}`))
				return
			}
			(*n.handler.Load()).ServeHTTP(w, r)
		})
		n.attachCluster(cl)
		n.ts.Start()
	}

	t.Cleanup(func() {
		for _, n := range nodes {
			n.Close()
		}
	})
	return nodes
}

// testPayload 生成可复现的测试内容：任何一位差异都能被 bytes.Equal 发现。
func testPayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i/251)
	}
	return b
}

// etagOf 返回与 store 一致的 ETag（内容 MD5 的小写十六进制）。
func etagOf(data []byte) string {
	sum := md5.Sum(data)
	return hex.EncodeToString(sum[:])
}

func putObject(t *testing.T, cl *Cluster, bucket, key string, data []byte) *store.ObjectInfo {
	t.Helper()
	info, err := cl.PutObject(bucket, key, bytes.NewReader(data), store.PutOptions{
		ExpectedSize: int64(len(data)),
		ContentType:  "application/octet-stream",
	})
	if err != nil {
		t.Fatalf("写入 %s 失败: %v", key, err)
	}
	return info
}

// putWithOpts 以额外的前置条件写入并断言成功，供条件写的"应成功"分支使用。
func putWithOpts(t *testing.T, cl *Cluster, bucket, key string, data []byte, opt store.PutOptions) *store.ObjectInfo {
	t.Helper()
	opt.ExpectedSize = int64(len(data))
	info, err := cl.PutObject(bucket, key, bytes.NewReader(data), opt)
	if err != nil {
		t.Fatalf("条件写入 %s 失败: %v", key, err)
	}
	return info
}

func readAll(t *testing.T, cl *Cluster, bucket, key string) []byte {
	t.Helper()
	r, err := cl.OpenObject(bucket, key)
	if err != nil {
		t.Fatalf("打开 %s 失败: %v", key, err)
	}
	var buf bytes.Buffer
	if err := r.WriteRangeTo(&buf, 0, r.Info().Size); err != nil {
		t.Fatalf("读取 %s 失败: %v", key, err)
	}
	if int64(buf.Len()) != r.Info().Size {
		t.Fatalf("读取 %s 得到 %d 字节，期望 %d", key, buf.Len(), r.Info().Size)
	}
	return buf.Bytes()
}

// createBucketOnAll 在第一个节点建桶，并验证它已广播到每个节点。
//
// 桶是集群范围的命名空间：逐个节点建会得到 ErrBucketExists，
// 因此"建一次、处处可见"本身就是本切片要验证的行为。
func createBucketOnAll(t *testing.T, nodes []*testNode, bucket string) {
	t.Helper()
	if err := nodes[0].cl.CreateBucket(bucket); err != nil {
		t.Fatalf("节点 %s 建桶失败: %v", nodes[0].id, err)
	}
	for _, n := range nodes {
		ok, err := n.st.BucketExists(bucket)
		if err != nil || !ok {
			t.Fatalf("节点 %s 未收到桶 %s 的广播（exists=%v err=%v）", n.id, bucket, ok, err)
		}
	}
	if err := nodes[len(nodes)-1].cl.CreateBucket(bucket); !errors.Is(err, store.ErrBucketExists) {
		t.Fatalf("重复建桶错误 = %v，期望 ErrBucketExists", err)
	}
}

// TestReplicatedPutReadAcrossNodes 是切片 3 的核心验收：
// 任意节点写 → 其余任意节点读 → 内容逐字节一致，且每个节点都有副本。
func TestReplicatedPutReadAcrossNodes(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket = "test-bucket"
	createBucketOnAll(t, nodes, bucket)

	for i := 0; i < len(nodes); i++ {
		key := fmt.Sprintf("dir/obj-from-node-%d.bin", i)
		payload := testPayload(5*1024 + 123 + i) // 跨多块，且各 writer 内容不同

		info := putObject(t, nodes[i].cl, bucket, key, payload)
		if info.Size != int64(len(payload)) {
			t.Errorf("%s 写入大小 = %d，期望 %d", key, info.Size, len(payload))
		}
		if info.ETag != etagOf(payload) {
			t.Errorf("%s ETag = %s，期望 %s", key, info.ETag, etagOf(payload))
		}
		if info.BlockCount < 2 {
			t.Errorf("%s 块数 = %d，期望跨多块", key, info.BlockCount)
		}

		// N=3 且成员数=3：每个节点都应当持有该 key 的副本 meta。
		for _, n := range nodes {
			m, found, err := n.st.LoadReplicaMeta(bucket, key)
			if err != nil {
				t.Fatalf("节点 %s 读取本地副本 meta 失败: %v", n.id, err)
			}
			if !found {
				t.Fatalf("节点 %s 缺少 %s 的副本", n.id, key)
			}
			if m.Size != int64(len(payload)) || m.ObjVersion != info.Version {
				t.Errorf("节点 %s 的副本 meta = %+v，期望大小 %d 版本 %d",
					n.id, m, len(payload), info.Version)
			}
		}

		// 从每个节点读回同一份内容（包括不是协调者的节点）。
		for _, n := range nodes {
			if got := readAll(t, n.cl, bucket, key); !bytes.Equal(got, payload) {
				t.Errorf("从节点 %s 读取 %s 内容不一致", n.id, key)
			}
		}
	}
}

// TestReplicatedRangeRead 验证跨块边界与首尾边界的 Range 在远端拼装后正确。
func TestReplicatedRangeRead(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket, key = "test-bucket", "range.bin"
	createBucketOnAll(t, nodes, bucket)

	payload := testPayload(4096 + 77) // 5 块：1024×4 + 77
	putObject(t, nodes[0].cl, bucket, key, payload)

	cases := []struct {
		name   string
		offset int64
		length int64
	}{
		{"first-byte", 0, 1},
		{"inside-first-block", 10, 100},
		{"cross-block", 900, 1500},
		{"last-byte", int64(len(payload)) - 1, 1},
		{"tail", int64(len(payload)) - 500, 500},
		{"whole", 0, int64(len(payload))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// 用非协调者读，确保走的是远端块路径。
			r, err := nodes[2].cl.OpenObject(bucket, key)
			if err != nil {
				t.Fatalf("打开失败: %v", err)
			}
			var buf bytes.Buffer
			if err := r.WriteRangeTo(&buf, tc.offset, tc.length); err != nil {
				t.Fatalf("Range 读取失败: %v", err)
			}
			want := payload[tc.offset : tc.offset+tc.length]
			if !bytes.Equal(buf.Bytes(), want) {
				t.Fatalf("Range [%d,%d) 内容不一致", tc.offset, tc.offset+tc.length)
			}
		})
	}
}

// TestReplicatedReadWithoutLocalReplica 验证读取不依赖本地副本：
// 删掉本地 meta 后仍能从其他副本拼出完整内容。
func TestReplicatedReadWithoutLocalReplica(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket, key = "test-bucket", "remote-only.bin"
	createBucketOnAll(t, nodes, bucket)

	payload := testPayload(3000)
	putObject(t, nodes[0].cl, bucket, key, payload)

	if _, err := nodes[1].st.DeleteReplicaMeta(bucket, key); err != nil {
		t.Fatalf("删除本地副本失败: %v", err)
	}
	if got := readAll(t, nodes[1].cl, bucket, key); !bytes.Equal(got, payload) {
		t.Fatal("本地无副本时读取内容不一致")
	}
}

// TestReplicatedConditionalPut 验证 If-None-Match: * 在跨节点写前
// barrier 上生效：其他协调者写入的对象必须被看见。
func TestReplicatedConditionalPut(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket, key = "test-bucket", "conditional.txt"
	createBucketOnAll(t, nodes, bucket)

	putObject(t, nodes[0].cl, bucket, key, []byte("first"))

	_, err := nodes[1].cl.PutObject(bucket, key, strings.NewReader("second"), store.PutOptions{
		ExpectedSize:   6,
		IfNoneMatchAny: true,
	})
	if !errors.Is(err, store.ErrPreconditionFailed) {
		t.Fatalf("条件写错误 = %v，期望 ErrPreconditionFailed", err)
	}

	// 不加条件的覆盖必须成功，且版本号严格递增。
	info := putObject(t, nodes[1].cl, bucket, key, []byte("second"))
	if info.Version <= 1 {
		t.Errorf("覆盖写版本 = %d，期望 > 1", info.Version)
	}
	for _, n := range nodes {
		if got := string(readAll(t, n.cl, bucket, key)); got != "second" {
			t.Errorf("节点 %s 读到 %q，期望 second", n.id, got)
		}
	}
}

// TestReplicatedIfNoneMatchETagPut 验证具体 ETag 的 If-None-Match
// 同样在跨节点写前 barrier 上求值：其他协调者写入的当前版本必须被看见。
func TestReplicatedIfNoneMatchETagPut(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket, key = "test-bucket", "if-none-match-etag.txt"
	createBucketOnAll(t, nodes, bucket)

	first := putObject(t, nodes[0].cl, bucket, key, []byte("first"))

	_, err := nodes[1].cl.PutObject(bucket, key, strings.NewReader("second"), store.PutOptions{
		ExpectedSize:    6,
		IfNoneMatchETag: first.ETag,
	})
	if !errors.Is(err, store.ErrPreconditionFailed) {
		t.Fatalf("ETag 相等时条件写错误 = %v，期望 ErrPreconditionFailed", err)
	}

	// 给定 ETag 不是当前版本 → 覆盖安全。
	info, err := nodes[1].cl.PutObject(bucket, key, strings.NewReader("second"), store.PutOptions{
		ExpectedSize:    6,
		IfNoneMatchETag: "deadbeef",
	})
	if err != nil {
		t.Fatalf("ETag 不等时条件写失败: %v", err)
	}
	for _, n := range nodes {
		if got := string(readAll(t, n.cl, bucket, key)); got != "second" {
			t.Errorf("节点 %s 读到 %q，期望 second", n.id, got)
		}
	}

	// 目标 key 不存在时条件成立。
	if _, err := nodes[2].cl.PutObject(bucket, "fresh", strings.NewReader("x"), store.PutOptions{
		ExpectedSize:    1,
		IfNoneMatchETag: info.ETag,
	}); err != nil {
		t.Fatalf("对象不存在时条件写失败: %v", err)
	}
}

// TestReplicatedOverwriteConverges 验证两个协调者先后写同一个 key 时，
// 所有节点都收敛到版本更大（后写）的那份。
func TestReplicatedOverwriteConverges(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket, key = "test-bucket", "converge.bin"
	createBucketOnAll(t, nodes, bucket)

	older := testPayload(2048)
	newer := testPayload(1500)
	putObject(t, nodes[0].cl, bucket, key, older)
	info := putObject(t, nodes[2].cl, bucket, key, newer)

	for _, n := range nodes {
		if got := readAll(t, n.cl, bucket, key); !bytes.Equal(got, newer) {
			t.Errorf("节点 %s 未收敛到最新版本", n.id)
		}
		m, found, err := n.st.LoadReplicaMeta(bucket, key)
		if err != nil || !found {
			t.Fatalf("节点 %s 副本 meta 缺失: %v", n.id, err)
		}
		if m.ObjVersion != info.Version {
			t.Errorf("节点 %s 的版本 = %d，期望 %d", n.id, m.ObjVersion, info.Version)
		}
	}
}

// TestReplicatedDelete 验证任一副本删除后，所有节点立即返回 NoSuchKey。
func TestReplicatedDelete(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket, key = "test-bucket", "gone.bin"
	createBucketOnAll(t, nodes, bucket)

	putObject(t, nodes[0].cl, bucket, key, testPayload(500))
	if err := nodes[2].cl.DeleteObject(bucket, key); err != nil {
		t.Fatalf("删除失败: %v", err)
	}
	for _, n := range nodes {
		if _, err := n.cl.OpenObject(bucket, key); !errors.Is(err, store.ErrNoSuchKey) {
			t.Errorf("节点 %s 删除后读取错误 = %v，期望 ErrNoSuchKey", n.id, err)
		}
	}
	// 删除是幂等的：对象已经在墓碑状态下，再删一次仍然成功
	// （S3 的 DELETE 对不存在的 key 返回 204，客户端重试不该失败）。
	if err := nodes[0].cl.DeleteObject(bucket, key); err != nil {
		t.Errorf("重复删除错误 = %v，期望 nil", err)
	}
}

// TestReplicatedMissingKey 验证对象在全部副本上都不存在时返回 NoSuchKey。
func TestReplicatedMissingKey(t *testing.T) {
	nodes := newTestNodes(t, 3)
	createBucketOnAll(t, nodes, "test-bucket")

	if _, err := nodes[1].cl.OpenObject("test-bucket", "never-existed"); !errors.Is(err, store.ErrNoSuchKey) {
		t.Fatalf("错误 = %v，期望 ErrNoSuchKey", err)
	}
}

// TestReplicatedBadDigest 验证 Content-MD5 不匹配时，任何副本都不提交 meta。
func TestReplicatedBadDigest(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket, key = "test-bucket", "bad-digest.bin"
	createBucketOnAll(t, nodes, bucket)

	_, err := nodes[0].cl.PutObject(bucket, key, strings.NewReader("payload"), store.PutOptions{
		ExpectedSize: 7,
		ContentMD5:   strings.Repeat("0", 32),
	})
	if !errors.Is(err, store.ErrBadDigest) {
		t.Fatalf("错误 = %v，期望 ErrBadDigest", err)
	}
	for _, n := range nodes {
		if _, found, err := n.st.LoadReplicaMeta(bucket, key); err != nil || found {
			t.Errorf("节点 %s 不应提交损坏对象的 meta（found=%v err=%v）", n.id, found, err)
		}
	}
}

// TestReplicatedUnavailableWhenQuorumLost 验证副本不足时读写都回
// ErrUnavailable（映射为 S3 503），而不是假装对象不存在。
func TestReplicatedUnavailableWhenQuorumLost(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket, key = "test-bucket", "quorum.bin"
	createBucketOnAll(t, nodes, bucket)
	putObject(t, nodes[0].cl, bucket, key, testPayload(4096))

	// 关掉两个节点，只剩本地节点：R=2 无法满足。
	nodes[1].Close()
	nodes[2].Close()

	if _, err := nodes[0].cl.OpenObject(bucket, key); !errors.Is(err, store.ErrUnavailable) {
		t.Errorf("读错误 = %v，期望 ErrUnavailable", err)
	}
	if _, err := nodes[0].cl.PutObject(bucket, key, strings.NewReader("x"), store.PutOptions{
		ExpectedSize: 1,
	}); !errors.Is(err, store.ErrUnavailable) {
		t.Errorf("写错误 = %v，期望 ErrUnavailable", err)
	}
	if err := nodes[0].cl.DeleteObject(bucket, key); !errors.Is(err, store.ErrUnavailable) {
		t.Errorf("删除错误 = %v，期望 ErrUnavailable", err)
	}
}

// TestReplicatedIfMatchPut 验证 If-Match 在跨节点写前 barrier 上生效：
// 判定依据是 probeMetas 观察到的最新**活**副本，不是本地 meta。
func TestReplicatedIfMatchPut(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket, key = "test-bucket", "cas.txt"
	createBucketOnAll(t, nodes, bucket)

	first := putObject(t, nodes[0].cl, bucket, key, []byte("v1"))

	// 用非写入协调者、拿第一次响应的 ETag 覆盖：必须成功。
	putWithOpts(t, nodes[1].cl, bucket, key, []byte("v2"), store.PutOptions{
		IfMatchETag: first.ETag,
	})
	// 旧 ETag 已被覆盖，再写必须 412。
	if _, err := nodes[2].cl.PutObject(bucket, key, strings.NewReader("v3"), store.PutOptions{
		ExpectedSize: 2,
		IfMatchETag:  first.ETag,
	}); !errors.Is(err, store.ErrPreconditionFailed) {
		t.Fatalf("旧 ETag 条件写错误 = %v，期望 ErrPreconditionFailed", err)
	}
	// `*` 只要求对象存在。
	putWithOpts(t, nodes[0].cl, bucket, key, []byte("v4"), store.PutOptions{IfMatchAny: true})

	if _, err := nodes[1].cl.PutObject(bucket, "absent", strings.NewReader("x"), store.PutOptions{
		ExpectedSize: 1,
		IfMatchAny:   true,
	}); !errors.Is(err, store.ErrPreconditionFailed) {
		t.Fatalf("对象不存在时 If-Match:* 错误 = %v，期望 ErrPreconditionFailed", err)
	}

	for _, n := range nodes {
		if got := string(readAll(t, n.cl, bucket, key)); got != "v4" {
			t.Errorf("节点 %s 读到 %q，期望 v4", n.id, got)
		}
	}
}

// TestReplicatedIfMatchIgnoresTombstone 验证集群条件写把墓碑当成"没有对象"：
// 墓碑的 ETag 是删除前的旧值，用它匹配会与单机语义分叉。
func TestReplicatedIfMatchIgnoresTombstone(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket, key = "test-bucket", "deleted.txt"
	createBucketOnAll(t, nodes, bucket)

	first := putObject(t, nodes[0].cl, bucket, key, []byte("v1"))
	if err := nodes[1].cl.DeleteObject(bucket, key); err != nil {
		t.Fatalf("删除失败: %v", err)
	}

	if _, err := nodes[2].cl.PutObject(bucket, key, strings.NewReader("v2"), store.PutOptions{
		ExpectedSize: 2,
		IfMatchETag:  first.ETag,
	}); !errors.Is(err, store.ErrPreconditionFailed) {
		t.Fatalf("删除后旧 ETag 条件写错误 = %v，期望 ErrPreconditionFailed", err)
	}
	if _, err := nodes[2].cl.PutObject(bucket, key, strings.NewReader("v2"), store.PutOptions{
		ExpectedSize: 2,
		IfMatchAny:   true,
	}); !errors.Is(err, store.ErrPreconditionFailed) {
		t.Fatalf("删除后 If-Match:* 错误 = %v，期望 ErrPreconditionFailed", err)
	}
}
