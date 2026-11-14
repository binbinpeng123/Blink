package cluster

import (
	"bytes"
	"errors"
	"testing"

	"github.com/binbinpeng123/Blink/internal/store"
)

// TestCopyObjectAcrossNodes 是集群复制的主验收。
//
// 关键判据不是"coordinator 能读回目标"，而是**每个节点的本地存储都能
// 独立打开目标对象**：Cluster 内嵌 *store.Store，若 CopyObject 忘了
// 显式覆盖，方法提升会让它落到单机实现，目标 meta 只存在于 coordinator
// 上且指向本节点块表 —— 其它节点读一次就是数据缺失。只有逐节点检查
// 本地副本，才能抓住这个错误。
func TestCopyObjectAcrossNodes(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket = "copy-bucket"
	createBucketOnAll(t, nodes, bucket)

	payload := testPayload(5*1024 + 123) // 跨多个 1 KiB 块
	putObject(t, nodes[0].cl, bucket, "models/source.bin", payload)

	// 用与写入不同的节点当协调者，确保复制不依赖"源就在本机"。
	const dstKey = "models/copied.bin"
	info, err := nodes[1].cl.CopyObject(bucket, dstKey, bucket, "models/source.bin", store.CopyOptions{})
	if err != nil {
		t.Fatalf("集群复制失败: %v", err)
	}
	if info.Size != int64(len(payload)) {
		t.Errorf("Size = %d，期望 %d", info.Size, len(payload))
	}
	if info.ETag != etagOf(payload) {
		t.Errorf("ETag = %s，期望 %s", info.ETag, etagOf(payload))
	}

	// 从每个 Cluster 实例读：走 quorum，验证副本元数据齐全。
	for _, n := range nodes {
		if got := readAll(t, n.cl, bucket, dstKey); !bytes.Equal(got, payload) {
			t.Errorf("从 %s 读副本内容不一致（%d 字节）", n.id, len(got))
		}
	}

	// 每个节点的本地存储也必须能直接打开：这条断言把"只写了 coordinator"
	// 的实现挡在门外。
	for _, n := range nodes {
		r, err := n.st.OpenObject(bucket, dstKey)
		if err != nil {
			t.Fatalf("节点 %s 本地缺少目标元数据: %v", n.id, err)
		}
		var buf bytes.Buffer
		if err := r.WriteRangeTo(&buf, 0, r.Info().Size); err != nil {
			t.Fatalf("节点 %s 本地读取目标失败: %v", n.id, err)
		}
		if !bytes.Equal(buf.Bytes(), payload) {
			t.Errorf("节点 %s 本地副本内容不一致", n.id)
		}
	}
}

// TestCopyObjectPreservesMetadataInCluster 验证 COPY 指令跨节点保留元数据。
func TestCopyObjectPreservesMetadataInCluster(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket = "copy-meta"
	createBucketOnAll(t, nodes, bucket)

	if _, err := nodes[0].cl.PutObject(bucket, "src", bytes.NewReader([]byte("payload")), store.PutOptions{
		ExpectedSize: 7,
		ContentType:  "image/png",
		UserMeta:     map[string]string{"owner": "edge-01"},
	}); err != nil {
		t.Fatalf("写源对象失败: %v", err)
	}

	if _, err := nodes[1].cl.CopyObject(bucket, "copy", bucket, "src", store.CopyOptions{}); err != nil {
		t.Fatalf("复制失败: %v", err)
	}

	// 从第三个节点读，确认元数据随副本一起可见。
	r, err := nodes[2].cl.OpenObject(bucket, "copy")
	if err != nil {
		t.Fatalf("打开副本失败: %v", err)
	}
	info := r.Info()
	if info.ContentType != "image/png" {
		t.Errorf("ContentType = %q，期望 image/png", info.ContentType)
	}
	if info.UserMeta["owner"] != "edge-01" {
		t.Errorf("UserMeta = %v，期望保留 owner=edge-01", info.UserMeta)
	}
}

// TestCopyObjectReplaceMetadataInCluster 验证 REPLACE 指令只改元数据。
func TestCopyObjectReplaceMetadataInCluster(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket = "copy-replace"
	createBucketOnAll(t, nodes, bucket)

	const body = "replace me"
	putObject(t, nodes[0].cl, bucket, "src", []byte(body))

	if _, err := nodes[1].cl.CopyObject(bucket, "dst", bucket, "src", store.CopyOptions{
		ReplaceMetadata: true,
		ContentType:     "application/json",
		UserMeta:        map[string]string{"owner": "edge-02"},
	}); err != nil {
		t.Fatalf("REPLACE 复制失败: %v", err)
	}

	r, err := nodes[2].cl.OpenObject(bucket, "dst")
	if err != nil {
		t.Fatalf("打开副本失败: %v", err)
	}
	if got := r.Info().ContentType; got != "application/json" {
		t.Errorf("ContentType = %q，期望 application/json", got)
	}
	if got := r.Info().UserMeta["owner"]; got != "edge-02" {
		t.Errorf("UserMeta owner = %q，期望 edge-02", got)
	}
	// REPLACE 只换元数据，内容必须逐字节不变。
	if got := readAll(t, nodes[2].cl, bucket, "dst"); string(got) != body {
		t.Errorf("内容 = %q，期望 %q", got, body)
	}
}

// TestCopyObjectSelfCopyInCluster 验证自复制的两条分支。
func TestCopyObjectSelfCopyInCluster(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket = "copy-self"
	createBucketOnAll(t, nodes, bucket)

	const body = "self copy payload"
	putObject(t, nodes[0].cl, bucket, "self", []byte(body))

	if _, err := nodes[1].cl.CopyObject(bucket, "self", bucket, "self", store.CopyOptions{}); !errors.Is(err, store.ErrInvalidRequest) {
		t.Fatalf("自复制 + COPY 错误 = %v，期望 ErrInvalidRequest", err)
	}
	if _, err := nodes[1].cl.CopyObject(bucket, "self", bucket, "self", store.CopyOptions{
		ReplaceMetadata: true,
		ContentType:     "text/csv",
	}); err != nil {
		t.Fatalf("自复制 + REPLACE 失败: %v", err)
	}

	r, err := nodes[0].cl.OpenObject(bucket, "self")
	if err != nil {
		t.Fatalf("打开对象失败: %v", err)
	}
	if got := r.Info().ContentType; got != "text/csv" {
		t.Errorf("ContentType = %q，期望 text/csv", got)
	}
	if got := readAll(t, nodes[0].cl, bucket, "self"); string(got) != body {
		t.Errorf("内容 = %q，期望 %q", got, body)
	}
}

// TestCopyObjectCrossBucketInCluster 验证跨桶复制与目标侧条件写。
func TestCopyObjectCrossBucketInCluster(t *testing.T) {
	nodes := newTestNodes(t, 3)
	createBucketOnAll(t, nodes, "source-bucket")
	createBucketOnAll(t, nodes, "mirror-bucket")

	payload := testPayload(3 * 1024)
	putObject(t, nodes[0].cl, "source-bucket", "dataset.bin", payload)

	if _, err := nodes[2].cl.CopyObject("mirror-bucket", "dataset.bin", "source-bucket", "dataset.bin", store.CopyOptions{}); err != nil {
		t.Fatalf("跨桶复制失败: %v", err)
	}
	if got := readAll(t, nodes[1].cl, "mirror-bucket", "dataset.bin"); !bytes.Equal(got, payload) {
		t.Error("跨桶副本内容不一致")
	}

	// 目标已存在时条件写必须失败。
	if _, err := nodes[2].cl.CopyObject("mirror-bucket", "dataset.bin", "source-bucket", "dataset.bin", store.CopyOptions{
		IfNoneMatchAny: true,
	}); !errors.Is(err, store.ErrPreconditionFailed) {
		t.Errorf("目标存在时条件复制错误 = %v，期望 ErrPreconditionFailed", err)
	}
}

// TestCopyObjectErrorsInCluster 覆盖源/桶不存在的错误路径。
func TestCopyObjectErrorsInCluster(t *testing.T) {
	nodes := newTestNodes(t, 3)
	createBucketOnAll(t, nodes, "bkt")
	putObject(t, nodes[0].cl, "bkt", "key", []byte("payload"))

	cases := []struct {
		name string
		err  error
		want error
	}{
		{
			name: "源 key 不存在",
			err: func() error {
				_, err := nodes[0].cl.CopyObject("bkt", "dst", "bkt", "missing", store.CopyOptions{})
				return err
			}(),
			want: store.ErrNoSuchKey,
		},
		{
			name: "源桶不存在",
			err: func() error {
				_, err := nodes[0].cl.CopyObject("bkt", "dst", "nope", "key", store.CopyOptions{})
				return err
			}(),
			want: store.ErrBucketNotFound,
		},
		{
			name: "目标桶不存在",
			err: func() error {
				_, err := nodes[0].cl.CopyObject("nope", "dst", "bkt", "key", store.CopyOptions{})
				return err
			}(),
			want: store.ErrBucketNotFound,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if !errors.Is(c.err, c.want) {
				t.Errorf("错误 = %v，期望 %v", c.err, c.want)
			}
		})
	}
}

// TestCopyObjectDestinationIfNoneMatchETagInCluster 验证 COPY 目标上的
// 具体 ETag 条件没有在 Cluster.CopyObject 里被丢掉。
func TestCopyObjectDestinationIfNoneMatchETagInCluster(t *testing.T) {
	nodes := newTestNodes(t, 3)
	createBucketOnAll(t, nodes, "bkt")

	payload := testPayload(2 * 1024)
	putObject(t, nodes[0].cl, "bkt", "src", payload)
	dst := putObject(t, nodes[0].cl, "bkt", "dst", []byte("old"))

	if _, err := nodes[1].cl.CopyObject("bkt", "dst", "bkt", "src", store.CopyOptions{
		IfNoneMatchETag: dst.ETag,
	}); !errors.Is(err, store.ErrPreconditionFailed) {
		t.Fatalf("目标 ETag 相等时复制错误 = %v，期望 ErrPreconditionFailed", err)
	}

	if _, err := nodes[1].cl.CopyObject("bkt", "dst", "bkt", "src", store.CopyOptions{
		IfNoneMatchETag: "deadbeef",
	}); err != nil {
		t.Fatalf("目标 ETag 不等时复制失败: %v", err)
	}
	if got := readAll(t, nodes[2].cl, "bkt", "dst"); !bytes.Equal(got, payload) {
		t.Error("复制后内容不一致")
	}

	if _, err := nodes[1].cl.CopyObject("bkt", "fresh", "bkt", "src", store.CopyOptions{
		IfNoneMatchETag: dst.ETag,
	}); err != nil {
		t.Fatalf("目标不存在时复制失败: %v", err)
	}
}

// TestCopyObjectDestinationIfMatchInCluster 验证 COPY 目标上的 If-Match
// 在集群路径上同样传到存储层，而不是被 CopyOptions 悄悄丢掉。
func TestCopyObjectDestinationIfMatchInCluster(t *testing.T) {
	nodes := newTestNodes(t, 3)
	createBucketOnAll(t, nodes, "bkt")

	payload := testPayload(2 * 1024)
	putObject(t, nodes[0].cl, "bkt", "src", payload)
	dst := putObject(t, nodes[0].cl, "bkt", "dst", []byte("old"))

	if _, err := nodes[1].cl.CopyObject("bkt", "dst", "bkt", "src", store.CopyOptions{
		IfMatchETag: dst.ETag,
	}); err != nil {
		t.Fatalf("目标 ETag 匹配时复制失败: %v", err)
	}
	if got := readAll(t, nodes[2].cl, "bkt", "dst"); !bytes.Equal(got, payload) {
		t.Error("复制后内容不一致")
	}

	if _, err := nodes[1].cl.CopyObject("bkt", "dst", "bkt", "src", store.CopyOptions{
		IfMatchETag: dst.ETag,
	}); !errors.Is(err, store.ErrPreconditionFailed) {
		t.Errorf("旧 ETag 复制错误 = %v，期望 ErrPreconditionFailed", err)
	}
	if _, err := nodes[1].cl.CopyObject("bkt", "fresh", "bkt", "src", store.CopyOptions{
		IfMatchAny: true,
	}); !errors.Is(err, store.ErrPreconditionFailed) {
		t.Errorf("目标不存在时复制错误 = %v，期望 ErrPreconditionFailed", err)
	}
}
