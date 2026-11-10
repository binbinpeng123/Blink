package cluster

import (
	"bytes"
	"testing"

	"github.com/binbinpeng123/Blink/internal/store"
)

// clusterHTTPMeta 是一组非空的标准 HTTP 元数据。
func clusterHTTPMeta() store.HTTPMeta {
	return store.HTTPMeta{
		CacheControl:       "public, max-age=3600",
		ContentDisposition: `attachment; filename="weights.bin"`,
		ContentEncoding:    "gzip",
		ContentLanguage:    "zh-CN",
		Expires:            "Wed, 21 Oct 2026 07:28:00 GMT",
	}
}

// requireClusterHTTPMeta 断言集群写入的元数据在**每个节点本地副本**上可见。
//
// Cluster 内嵌 *store.Store，方法提升会让漏改的路径悄悄落回单机实现：
// 协调者读得到、其它副本读不到。只查协调者会漏掉这类错误。
func requireClusterHTTPMeta(t *testing.T, nodes []*testNode, bucket, key string, want store.HTTPMeta) {
	t.Helper()
	for _, n := range nodes {
		r, err := n.st.OpenObject(bucket, key)
		if err != nil {
			t.Fatalf("节点 %s 本地缺少 %s 的元数据: %v", n.id, key, err)
		}
		if got := r.Info().HTTPMeta; got != want {
			t.Errorf("节点 %s 的 HTTPMeta = %+v，期望 %+v", n.id, got, want)
		}
	}
}

// 标准 HTTP 元数据必须随三副本一起下发，而不是只存在于协调者。
func TestPutObjectHTTPMetaAcrossNodes(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket = "http-meta-put"
	createBucketOnAll(t, nodes, bucket)

	want := clusterHTTPMeta()
	putWithOpts(t, nodes[0].cl, bucket, "model.bin", []byte("weights"), store.PutOptions{
		HTTPMeta: want,
	})

	// 从另一个节点读，走 quorum 合并后的 ObjectInfo。
	r, err := nodes[1].cl.OpenObject(bucket, "model.bin")
	if err != nil {
		t.Fatalf("打开对象失败: %v", err)
	}
	if got := r.Info().HTTPMeta; got != want {
		t.Errorf("集群读回的 HTTPMeta = %+v，期望 %+v", got, want)
	}
	requireClusterHTTPMeta(t, nodes, bucket, "model.bin", want)
}

// COPY 保留源元数据；REPLACE 替换并清空未提供的头 —— 两台以上协调者结果一致。
func TestCopyObjectHTTPMetaAcrossNodes(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket = "http-meta-copy"
	createBucketOnAll(t, nodes, bucket)

	src := clusterHTTPMeta()
	putWithOpts(t, nodes[0].cl, bucket, "src", []byte("payload"), store.PutOptions{
		HTTPMeta: src,
	})

	if _, err := nodes[1].cl.CopyObject(bucket, "copy", bucket, "src", store.CopyOptions{}); err != nil {
		t.Fatalf("COPY 复制失败: %v", err)
	}
	requireClusterHTTPMeta(t, nodes, bucket, "copy", src)

	replaced := store.HTTPMeta{CacheControl: "no-store", ContentEncoding: "br"}
	if _, err := nodes[2].cl.CopyObject(bucket, "replace", bucket, "src", store.CopyOptions{
		ReplaceMetadata: true,
		HTTPMeta:        replaced,
	}); err != nil {
		t.Fatalf("REPLACE 复制失败: %v", err)
	}
	requireClusterHTTPMeta(t, nodes, bucket, "replace", replaced)

	// 源对象有、REPLACE 请求没给的头必须清空，不能残留源值。
	if _, err := nodes[0].cl.CopyObject(bucket, "clear", bucket, "src", store.CopyOptions{
		ReplaceMetadata: true,
	}); err != nil {
		t.Fatalf("REPLACE 清空复制失败: %v", err)
	}
	requireClusterHTTPMeta(t, nodes, bucket, "clear", store.HTTPMeta{})
}

// Initiate 声明的标准 HTTP 元数据随会话复制，在 Complete 时写进对象三副本。
func TestMultipartHTTPMetaAcrossNodes(t *testing.T) {
	nodes := newTestNodesWithBlockSize(t, 3, mpuBlockSize)
	const bucket = "http-meta-mpu"
	const key = "models/mpu.bin"
	createBucketOnAll(t, nodes, bucket)

	want := store.HTTPMeta{
		CacheControl:       "public, max-age=600",
		ContentDisposition: `attachment; filename="mpu.bin"`,
	}
	up := createMPU(t, nodes[0].cl, bucket, key, store.CreateMultipartOptions{
		ContentType: "application/octet-stream",
		HTTPMeta:    want,
	})
	if up.HTTPMeta != want {
		t.Fatalf("Create 返回的 HTTPMeta = %+v，期望 %+v", up.HTTPMeta, want)
	}

	part := testPayload(5 << 20)
	info := uploadPart(t, nodes[1].cl, bucket, key, up.UploadID, 1, part, store.UploadPartOptions{})

	obj, err := nodes[2].cl.CompleteMultipartUpload(bucket, key, up.UploadID,
		[]store.CompletePart{{PartNumber: 1, ETag: info.ETag}}, "", false)
	if err != nil {
		t.Fatalf("Complete 失败: %v", err)
	}
	if obj.HTTPMeta != want {
		t.Errorf("Complete 返回的 HTTPMeta = %+v，期望 %+v", obj.HTTPMeta, want)
	}
	requireClusterHTTPMeta(t, nodes, bucket, key, want)

	// 内容仍逐字节正确，元数据改动不影响数据路径。
	if got := readAll(t, nodes[0].cl, bucket, key); !bytes.Equal(got, part) {
		t.Errorf("对象内容不一致：%d 字节，期望 %d", len(got), len(part))
	}
}
