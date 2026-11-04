package cluster

import (
	"bytes"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/binbinpeng123/Blink/internal/store"
)

// listFn 是 ListObjects 的最小接口，让单机与集群两条路径用同一个
// 翻页断言函数。
type listFn func(bucket string, opt store.ListOptions) (*store.ListResult, error)

// walkPages 按 Cursor 翻完所有页，把条目按返回顺序拼成一个序列。
//
// 序列里区分 obj: 与 pre: 前缀：两者在 S3 响应里分开返回，
// 单机与集群必须产出完全相同的序列与页边界。
func walkPages(t *testing.T, lister listFn, bucket string, opt store.ListOptions) []string {
	t.Helper()
	var out []string
	for page := 0; page < 50; page++ {
		res, err := lister(bucket, opt)
		if err != nil {
			t.Fatalf("LIST 失败: %v", err)
		}
		for _, o := range res.Objects {
			out = append(out, "obj:"+o.Key)
		}
		for _, p := range res.CommonPrefixes {
			out = append(out, "pre:"+p)
		}
		if !res.IsTruncated {
			return out
		}
		if res.Next == nil {
			t.Fatalf("IsTruncated=true 但 Next 为空")
		}
		opt.Cursor = res.Next
	}
	t.Fatalf("翻页 50 次仍未结束")
	return nil
}

// TestClusterListMatchesSingleNode 是本切片的核心验收：
// 同一批对象在单机与集群上按同一组参数翻页，页边界与条目序列必须完全一致。
func TestClusterListMatchesSingleNode(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket = "list-bucket"
	createBucketOnAll(t, nodes, bucket)

	single, err := store.Open(store.Options{
		Root:            filepath.Join(t.TempDir(), "single"),
		BlockSize:       1024,
		InlineThreshold: -1,
		VerifyOnRead:    true,
	})
	if err != nil {
		t.Fatalf("打开参照存储失败: %v", err)
	}
	if err := single.CreateBucket(bucket); err != nil {
		t.Fatalf("参照存储建桶失败: %v", err)
	}

	// 覆盖：普通 key、两级以上目录、前缀互为前缀的 key、空对象。
	keys := []string{
		"a.txt",
		"b-empty.bin",
		"dir1/",
		"dir1/a.bin",
		"dir1/b.bin",
		"dir1/sub/deep.bin",
		"dir1x.txt",
		"dir2/c.bin",
		"dir2/nested/d.bin",
		"zzz.txt",
		"中文/键.txt",
	}
	for i, key := range keys {
		payload := testPayload(100 + i*37)
		// 写往不同协调者：集群 LIST 必须对"副本散落在不同节点"同样正确。
		if _, err := nodes[i%len(nodes)].cl.PutObject(bucket, key, bytes.NewReader(payload), store.PutOptions{
			ExpectedSize: int64(len(payload)),
		}); err != nil {
			t.Fatalf("集群写入 %q 失败: %v", key, err)
		}
		if _, err := single.PutObject(bucket, key, bytes.NewReader(payload), store.PutOptions{
			ExpectedSize: int64(len(payload)),
		}); err != nil {
			t.Fatalf("参照存储写入 %q 失败: %v", key, err)
		}
	}

	cases := []struct {
		name string
		opt  store.ListOptions
	}{
		{"all-in-one-page", store.ListOptions{MaxKeys: 1000}},
		{"page-2", store.ListOptions{MaxKeys: 2}},
		{"page-1", store.ListOptions{MaxKeys: 1}},
		{"page-3", store.ListOptions{MaxKeys: 3}},
		{"max-keys-0", store.ListOptions{MaxKeys: 0}},
		{"prefix-dir1", store.ListOptions{Prefix: "dir1/", MaxKeys: 1000}},
		{"prefix-dir1-paged", store.ListOptions{Prefix: "dir1/", MaxKeys: 2}},
		{"delimiter-root", store.ListOptions{Delimiter: "/", MaxKeys: 1000}},
		{"delimiter-root-paged", store.ListOptions{Delimiter: "/", MaxKeys: 2}},
		{"delimiter-under-dir1", store.ListOptions{Prefix: "dir1/", Delimiter: "/", MaxKeys: 1000}},
		{"start-after", store.ListOptions{StartAfter: "dir1/a.bin", MaxKeys: 1000}},
		{"prefix-no-match", store.ListOptions{Prefix: "nope/", MaxKeys: 1000}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := walkPages(t, single.ListObjects, bucket, tc.opt)
			for _, n := range nodes {
				got := walkPages(t, n.cl.ListObjects, bucket, tc.opt)
				if !equalSeq(got, want) {
					t.Fatalf("节点 %s 的 LIST 序列与单机不一致\n集群: %v\n单机: %v", n.id, got, want)
				}
			}
		})
	}
}

// equalSeq 比较两个字符串序列。
func equalSeq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestClusterListHidesDeletedObjects 验证删除后 LIST 立即不再返回该 key，
// 且每个节点上留下的是墓碑而不是"什么都没有"。
func TestClusterListHidesDeletedObjects(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket = "delete-list"
	createBucketOnAll(t, nodes, bucket)

	for _, key := range []string{"keep-1", "gone", "keep-2"} {
		putObject(t, nodes[0].cl, bucket, key, []byte("payload-"+key))
	}
	if err := nodes[1].cl.DeleteObject(bucket, "gone"); err != nil {
		t.Fatalf("删除失败: %v", err)
	}

	// 删除立即对每个节点的读生效。
	for _, n := range nodes {
		if _, err := n.cl.OpenObject(bucket, "gone"); !errors.Is(err, store.ErrNoSuchKey) {
			t.Errorf("节点 %s 删除后读取错误 = %v，期望 ErrNoSuchKey", n.id, err)
		}
		res, err := n.cl.ListObjects(bucket, store.ListOptions{MaxKeys: 100})
		if err != nil {
			t.Fatalf("节点 %s LIST 失败: %v", n.id, err)
		}
		if got := objKeys(res); !equalSeq(got, []string{"keep-1", "keep-2"}) {
			t.Errorf("节点 %s LIST = %v，期望 [keep-1 keep-2]", n.id, got)
		}
		// 墓碑必须留在本地：物理删除 meta 会让宕机节点恢复后无从比较版本。
		entries, err := n.st.ScanReplicaEntries(bucket)
		if err != nil {
			t.Fatalf("节点 %s 扫描副本失败: %v", n.id, err)
		}
		var tomb bool
		for _, e := range entries {
			if e.Key == "gone" {
				tomb = e.Deleted
			}
		}
		if !tomb {
			t.Errorf("节点 %s 上没有 gone 的墓碑", n.id)
		}
	}
}

// objKeys 取出结果里的对象 key。
func objKeys(res *store.ListResult) []string {
	out := make([]string, len(res.Objects))
	for i, o := range res.Objects {
		out[i] = o.Key
	}
	return out
}

// TestTombstonePreventsResurrection 模拟"删除时不可达的节点带着旧副本回来"：
//
//	node-3 关机 → 经 node-1 删除（W=2 仍满足）→ node-3 在原地址恢复
//	（数据目录里还是删除前的活副本）→ 任意节点读必须是 404，LIST 不得漏出该 key。
//
// 这是墓碑（而不是"物理删除 meta"）存在的全部理由：删除的版本必须能被
// 恢复节点上的副本比较看见，否则旧数据会复活。
func TestTombstonePreventsResurrection(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket, key = "resurrect", "ghost.bin"
	createBucketOnAll(t, nodes, bucket)

	payload := testPayload(2048)
	putObject(t, nodes[0].cl, bucket, key, payload)

	// 旧副本必须在 node-3 上：N=3、成员=3 时每个节点都有一份。
	if _, found, err := nodes[2].st.LoadReplicaMeta(bucket, key); err != nil || !found {
		t.Fatalf("node-3 上缺少旧副本（found=%v err=%v）", found, err)
	}

	addr := nodes[2].ts.Listener.Addr().String()
	nodes[2].ts.Close() // 只停服务，数据目录保持原样

	// W=2：node-3 不可达时删除仍然成功。
	if err := nodes[0].cl.DeleteObject(bucket, key); err != nil {
		t.Fatalf("删除失败: %v", err)
	}

	// 在原地址把 node-3 重新拉起来，数据目录还是删除前的。
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("在 %s 重启 node-3 失败: %v", addr, err)
	}
	srv := &http.Server{Handler: nodes[2].cl.InternalHandler()}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	// 复活候选副本确实还在 node-3 上，且是活的旧版本。
	m, found, err := nodes[2].st.LoadReplicaMeta(bucket, key)
	if err != nil || !found || m.Deleted {
		t.Fatalf("node-3 的旧副本状态 = %+v found=%v err=%v", m, found, err)
	}

	for _, n := range nodes {
		if _, err := n.cl.OpenObject(bucket, key); !errors.Is(err, store.ErrNoSuchKey) {
			t.Errorf("节点 %s 读到复活的对象，错误 = %v，期望 ErrNoSuchKey", n.id, err)
		}
		res, err := n.cl.ListObjects(bucket, store.ListOptions{MaxKeys: 100})
		if err != nil {
			t.Fatalf("节点 %s LIST 失败: %v", n.id, err)
		}
		if got := objKeys(res); len(got) != 0 {
			t.Errorf("节点 %s 的 LIST 漏出了已删除的 key: %v", n.id, got)
		}
	}
}

// TestClusterListBucketNotFound 验证集群 LIST 对不存在的桶返回 NoSuchBucket，
// 而不是空列表。
func TestClusterListBucketNotFound(t *testing.T) {
	nodes := newTestNodes(t, 3)
	if _, err := nodes[0].cl.ListObjects("never-created", store.ListOptions{MaxKeys: 10}); !errors.Is(err, store.ErrBucketNotFound) {
		t.Fatalf("错误 = %v，期望 ErrBucketNotFound", err)
	}
}
