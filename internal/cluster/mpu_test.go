package cluster

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"

	"github.com/binbinpeng123/Blink/internal/checksum"
	"github.com/binbinpeng123/Blink/internal/store"
)

// 本文件覆盖分片上传的跨节点行为。分片尺寸必须遵守 S3 的 5 MiB 下限
// （末分片除外），因此这里用 1 MiB 的块而不是默认测试里的 1 KiB ——
// 否则一次 5 MiB 分片会变成五千多次内部 HTTP 往返。

const mpuBlockSize = 1 << 20

// multipartETag 按 S3 规则计算完成后的对象 ETag：
// md5(各分片 ETag 的原始 MD5 字节) + "-" + 分片数。
func multipartETag(parts ...[]byte) string {
	h := md5.New()
	for _, p := range parts {
		sum := md5.Sum(p)
		h.Write(sum[:])
	}
	return hex.EncodeToString(h.Sum(nil)) + fmt.Sprintf("-%d", len(parts))
}

func checksumOf(t *testing.T, alg checksum.Algorithm, data []byte) string {
	t.Helper()
	s, ok := checksum.New(alg)
	if !ok {
		t.Fatalf("未知的校验和算法 %q", alg)
	}
	if _, err := s.Write(data); err != nil {
		t.Fatalf("计算 %s 校验和失败: %v", alg, err)
	}
	return s.Encoded()
}

func createMPU(t *testing.T, cl *Cluster, bucket, key string, opt store.CreateMultipartOptions) *store.MultipartUpload {
	t.Helper()
	up, err := cl.CreateMultipartUpload(bucket, key, opt)
	if err != nil {
		t.Fatalf("创建分片上传 %s 失败: %v", key, err)
	}
	if len(up.UploadID) != 32 {
		t.Fatalf("uploadId = %q，期望 32 位十六进制", up.UploadID)
	}
	return up
}

func uploadPart(t *testing.T, cl *Cluster, bucket, key, uploadID string, n int, data []byte, opt store.UploadPartOptions) *store.PartInfo {
	t.Helper()
	opt.ExpectedSize = int64(len(data))
	info, err := cl.UploadPart(bucket, key, uploadID, n, bytes.NewReader(data), opt)
	if err != nil {
		t.Fatalf("上传分片 %d 失败: %v", n, err)
	}
	if info.Size != int64(len(data)) {
		t.Fatalf("分片 %d 大小 = %d，期望 %d", n, info.Size, len(data))
	}
	return info
}

// TestMultipartUploadAcrossNodes 是切片 5 的核心验收：
// 协调者建会话 → 分片打到不同节点 → 任意节点 Complete → 任意节点读回。
func TestMultipartUploadAcrossNodes(t *testing.T) {
	nodes := newTestNodesWithBlockSize(t, 3, mpuBlockSize)
	const bucket = "mpu-bucket"
	const key = "dir/big.bin"
	createBucketOnAll(t, nodes, bucket)

	up := createMPU(t, nodes[0].cl, bucket, key, store.CreateMultipartOptions{
		ContentType: "application/x-test",
		UserMeta:    map[string]string{"owner": "mpu-test"},
	})

	part1 := testPayload(5<<20 + 321)
	part2 := testPayload(1<<20 + 7)
	// 分片特意由不同节点协调：会话与数据都不应绑定在创建者身上。
	info1 := uploadPart(t, nodes[1].cl, bucket, key, up.UploadID, 1, part1, store.UploadPartOptions{})
	info2 := uploadPart(t, nodes[2].cl, bucket, key, up.UploadID, 2, part2, store.UploadPartOptions{})

	if info1.ETag != etagOf(part1) || info2.ETag != etagOf(part2) {
		t.Fatalf("分片 ETag 不符：%s / %s", info1.ETag, info2.ETag)
	}

	// 任意节点都能看到完整的分片列表（状态合并，而不是只看本地）。
	for _, n := range nodes {
		res, err := n.cl.ListParts(bucket, key, up.UploadID, store.ListPartsOptions{MaxParts: 1000})
		if err != nil {
			t.Fatalf("节点 %s 列举分片失败: %v", n.id, err)
		}
		if len(res.Parts) != 2 || res.Parts[0].Size != int64(len(part1)) || res.Parts[1].Size != int64(len(part2)) {
			t.Fatalf("节点 %s 的分片列表 = %+v", n.id, res.Parts)
		}
		if res.IsTruncated || res.NextPartNumberMarker != 0 {
			t.Fatalf("节点 %s 未截断时 NextPartNumberMarker 应为 0: %+v", n.id, res)
		}
	}

	// 分页语义与单机一致：NextPartNumberMarker 是本页最后一个分片号。
	res, err := nodes[0].cl.ListParts(bucket, key, up.UploadID, store.ListPartsOptions{MaxParts: 1})
	if err != nil {
		t.Fatalf("分页列举分片失败: %v", err)
	}
	if !res.IsTruncated || len(res.Parts) != 1 || res.NextPartNumberMarker != 1 {
		t.Fatalf("第一页 = %+v，期望截断且 NextPartNumberMarker=1", res)
	}
	res, err = nodes[0].cl.ListParts(bucket, key, up.UploadID, store.ListPartsOptions{MaxParts: 1, PartNumberMarker: 1})
	if err != nil {
		t.Fatalf("第二页列举分片失败: %v", err)
	}
	if res.IsTruncated || len(res.Parts) != 1 || res.Parts[0].PartNumber != 2 {
		t.Fatalf("第二页 = %+v，期望仅分片 2 且不截断", res)
	}

	// 会话列表扇形到全部节点后归并。
	for _, n := range nodes {
		list, err := n.cl.ListMultipartUploads(bucket, store.ListMultipartUploadsOptions{MaxUploads: 100})
		if err != nil {
			t.Fatalf("节点 %s 列举会话失败: %v", n.id, err)
		}
		if len(list.Uploads) != 1 || list.Uploads[0].UploadID != up.UploadID || list.Uploads[0].Key != key {
			t.Fatalf("节点 %s 的会话列表 = %+v", n.id, list.Uploads)
		}
	}

	// Complete 由第三个节点发起（既不是建会话的节点，也不是末分片的协调者）。
	// 第一个 ETag 带引号，验证服务端会去掉引号再比对。
	obj, err := nodes[1].cl.CompleteMultipartUpload(bucket, key, up.UploadID, []store.CompletePart{
		{PartNumber: 1, ETag: `"` + info1.ETag + `"`},
		{PartNumber: 2, ETag: info2.ETag},
	}, "", false)
	if err != nil {
		t.Fatalf("Complete 失败: %v", err)
	}
	want := append(append([]byte{}, part1...), part2...)
	if obj.Size != int64(len(want)) {
		t.Fatalf("对象大小 = %d，期望 %d", obj.Size, len(want))
	}
	if wantETag := multipartETag(part1, part2); obj.ETag != wantETag {
		t.Fatalf("对象 ETag = %s，期望 %s", obj.ETag, wantETag)
	}
	if obj.ContentType != "application/x-test" || obj.UserMeta["owner"] != "mpu-test" {
		t.Fatalf("对象属性未从会话继承: %+v", obj)
	}
	if obj.BlockCount < 6 {
		t.Fatalf("对象块数 = %d，期望至少 6", obj.BlockCount)
	}

	for _, n := range nodes {
		if got := readAll(t, n.cl, bucket, key); !bytes.Equal(got, want) {
			t.Errorf("从节点 %s 读取的对象与分片拼接结果不一致", n.id)
		}
	}

	// 每个节点都应有对象副本 meta：Complete 把 meta 发给了全部 O 目标。
	for _, n := range nodes {
		m, found, err := n.st.LoadReplicaMeta(bucket, key)
		if err != nil || !found {
			t.Fatalf("节点 %s 缺少对象副本 meta（found=%v err=%v）", n.id, found, err)
		}
		if m.ETag != obj.ETag || m.Size != obj.Size {
			t.Errorf("节点 %s 的副本 meta = %+v", n.id, m)
		}
	}

	// 会话在 Complete 后必须消失：ListParts / Complete 都应回 NoSuchUpload。
	for _, n := range nodes {
		if _, err := n.cl.ListParts(bucket, key, up.UploadID, store.ListPartsOptions{MaxParts: 10}); !errors.Is(err, store.ErrNoSuchUpload) {
			t.Errorf("节点 %s 在 Complete 后 ListParts 错误 = %v，期望 ErrNoSuchUpload", n.id, err)
		}
		list, err := n.cl.ListMultipartUploads(bucket, store.ListMultipartUploadsOptions{MaxUploads: 100})
		if err != nil {
			t.Fatalf("节点 %s 列举会话失败: %v", n.id, err)
		}
		if len(list.Uploads) != 0 {
			t.Errorf("节点 %s 在 Complete 后仍有 %d 个会话", n.id, len(list.Uploads))
		}
	}
}

// TestMultipartUploadAbort 验证 Abort 在集群范围生效，且重复 Abort 回
// NoSuchUpload（与 AWS 一致）。
func TestMultipartUploadAbort(t *testing.T) {
	nodes := newTestNodesWithBlockSize(t, 3, mpuBlockSize)
	const bucket = "mpu-abort"
	const key = "obj.bin"
	createBucketOnAll(t, nodes, bucket)

	up := createMPU(t, nodes[0].cl, bucket, key, store.CreateMultipartOptions{})
	uploadPart(t, nodes[0].cl, bucket, key, up.UploadID, 1, testPayload(5<<20), store.UploadPartOptions{})

	if err := nodes[1].cl.AbortMultipartUpload(bucket, key, up.UploadID); err != nil {
		t.Fatalf("Abort 失败: %v", err)
	}
	for _, n := range nodes {
		if _, err := n.cl.ListParts(bucket, key, up.UploadID, store.ListPartsOptions{MaxParts: 10}); !errors.Is(err, store.ErrNoSuchUpload) {
			t.Errorf("节点 %s 在 Abort 后 ListParts 错误 = %v，期望 ErrNoSuchUpload", n.id, err)
		}
	}
	if _, err := nodes[2].cl.CompleteMultipartUpload(bucket, key, up.UploadID, []store.CompletePart{
		{PartNumber: 1, ETag: etagOf(testPayload(5 << 20))},
	}, "", false); !errors.Is(err, store.ErrNoSuchUpload) {
		t.Fatalf("Abort 后 Complete 错误 = %v，期望 ErrNoSuchUpload", err)
	}
	if err := nodes[2].cl.AbortMultipartUpload(bucket, key, up.UploadID); !errors.Is(err, store.ErrNoSuchUpload) {
		t.Fatalf("重复 Abort 错误 = %v，期望 ErrNoSuchUpload", err)
	}
}

// TestMultipartUploadSurvivesCoordinatorFailure 验证会话属于 S 而不是
// 创建者：创建者宕机后，别的节点仍能 Complete。
func TestMultipartUploadSurvivesCoordinatorFailure(t *testing.T) {
	nodes := newTestNodesWithBlockSize(t, 3, mpuBlockSize)
	const bucket = "mpu-failover"
	const key = "obj.bin"
	createBucketOnAll(t, nodes, bucket)

	up := createMPU(t, nodes[0].cl, bucket, key, store.CreateMultipartOptions{})
	part1 := testPayload(5 << 20)
	part2 := testPayload(1 << 20)
	info1 := uploadPart(t, nodes[0].cl, bucket, key, up.UploadID, 1, part1, store.UploadPartOptions{})
	info2 := uploadPart(t, nodes[1].cl, bucket, key, up.UploadID, 2, part2, store.UploadPartOptions{})

	// 模拟创建者宕机：会话副本还剩 2 个，恰好等于 R。
	nodes[0].Close()

	obj, err := nodes[1].cl.CompleteMultipartUpload(bucket, key, up.UploadID, []store.CompletePart{
		{PartNumber: 1, ETag: info1.ETag},
		{PartNumber: 2, ETag: info2.ETag},
	}, "", false)
	if err != nil {
		t.Fatalf("创建者宕机后 Complete 失败: %v", err)
	}
	want := append(append([]byte{}, part1...), part2...)
	if obj.Size != int64(len(want)) {
		t.Fatalf("对象大小 = %d，期望 %d", obj.Size, len(want))
	}
	for _, n := range nodes[1:] {
		if got := readAll(t, n.cl, bucket, key); !bytes.Equal(got, want) {
			t.Errorf("从节点 %s 读取的对象不一致", n.id)
		}
	}
}

// TestMultipartUploadChecksumAdoption 验证"首个分片采用算法"的集群版：
// 会话创建时没声明算法，第一个带校验和的分片把它钉住，之后换算法必须失败。
func TestMultipartUploadChecksumAdoption(t *testing.T) {
	nodes := newTestNodesWithBlockSize(t, 3, mpuBlockSize)
	const bucket = "mpu-checksum"
	const key = "obj.bin"
	createBucketOnAll(t, nodes, bucket)

	up := createMPU(t, nodes[0].cl, bucket, key, store.CreateMultipartOptions{})
	if up.ChecksumAlgorithm != "" || up.ChecksumType != "" {
		t.Fatalf("会话初始不应有校验和: %+v", up)
	}

	part1 := testPayload(5 << 20)
	wantSum := checksumOf(t, checksum.AlgCRC32, part1)
	info1 := uploadPart(t, nodes[1].cl, bucket, key, up.UploadID, 1, part1, store.UploadPartOptions{
		ChecksumAlgorithm: checksum.AlgCRC32,
		Checksum:          wantSum,
	})
	if info1.ChecksumAlgorithm != checksum.AlgCRC32 || info1.Checksum != wantSum {
		t.Fatalf("分片 1 校验和信息 = %+v", info1)
	}

	// 采用结果必须写回 S：任意节点看到的会话都应已有算法与类型。
	for _, n := range nodes {
		res, err := n.cl.ListParts(bucket, key, up.UploadID, store.ListPartsOptions{MaxParts: 10})
		if err != nil {
			t.Fatalf("节点 %s 列举分片失败: %v", n.id, err)
		}
		if res.Upload.ChecksumAlgorithm != checksum.AlgCRC32 || res.Upload.ChecksumType != store.ChecksumTypeComposite {
			t.Fatalf("节点 %s 的会话校验和 = %q/%q", n.id, res.Upload.ChecksumAlgorithm, res.Upload.ChecksumType)
		}
	}

	part2 := testPayload(1 << 20)
	// 换一种算法：必须在写入数据块之前就被拒绝。
	if _, err := nodes[2].cl.UploadPart(bucket, key, up.UploadID, 2, bytes.NewReader(part2), store.UploadPartOptions{
		ExpectedSize:      int64(len(part2)),
		ChecksumAlgorithm: checksum.AlgSHA256,
	}); !errors.Is(err, store.ErrInvalidChecksum) {
		t.Fatalf("换算法上传错误 = %v，期望 ErrInvalidChecksum", err)
	}
	// 声明值与实际不符必须失败。
	if _, err := nodes[2].cl.UploadPart(bucket, key, up.UploadID, 2, bytes.NewReader(part2), store.UploadPartOptions{
		ExpectedSize: int64(len(part2)),
		Checksum:     checksumOf(t, checksum.AlgCRC32, []byte("other")),
	}); !errors.Is(err, store.ErrBadDigest) {
		t.Fatalf("错误校验和上传错误 = %v，期望 ErrBadDigest", err)
	}

	info2 := uploadPart(t, nodes[2].cl, bucket, key, up.UploadID, 2, part2, store.UploadPartOptions{
		Checksum: checksumOf(t, checksum.AlgCRC32, part2),
	})
	obj, err := nodes[0].cl.CompleteMultipartUpload(bucket, key, up.UploadID, []store.CompletePart{
		{PartNumber: 1, ETag: info1.ETag},
		{PartNumber: 2, ETag: info2.ETag},
	}, "", false)
	if err != nil {
		t.Fatalf("Complete 失败: %v", err)
	}
	if obj.ChecksumAlgorithm != checksum.AlgCRC32 || obj.ChecksumType != store.ChecksumTypeComposite || obj.Checksum == "" {
		t.Fatalf("对象的校验和信息 = %+v", obj)
	}
	if got := readAll(t, nodes[2].cl, bucket, key); !bytes.Equal(got, append(append([]byte{}, part1...), part2...)) {
		t.Fatal("读取的校验和测试对象内容不一致")
	}
}

// TestMultipartUploadConditionalComplete 验证 Complete 也遵守条件写：
// If-None-Match:* 在对象已存在时失败，且失败后会话仍然可用。
func TestMultipartUploadConditionalComplete(t *testing.T) {
	nodes := newTestNodesWithBlockSize(t, 3, mpuBlockSize)
	const bucket = "mpu-conditional"
	const key = "obj.bin"
	createBucketOnAll(t, nodes, bucket)

	old := testPayload(2 << 20)
	oldInfo := putObject(t, nodes[0].cl, bucket, key, old)

	up := createMPU(t, nodes[0].cl, bucket, key, store.CreateMultipartOptions{})
	data := testPayload(5 << 20)
	info := uploadPart(t, nodes[0].cl, bucket, key, up.UploadID, 1, data, store.UploadPartOptions{})
	parts := []store.CompletePart{{PartNumber: 1, ETag: info.ETag}}

	if _, err := nodes[1].cl.CompleteMultipartUpload(bucket, key, up.UploadID, parts, "", true); !errors.Is(err, store.ErrPreconditionFailed) {
		t.Fatalf("条件 Complete 错误 = %v，期望 ErrPreconditionFailed", err)
	}
	// 失败不能消耗会话。
	if _, err := nodes[2].cl.ListParts(bucket, key, up.UploadID, store.ListPartsOptions{MaxParts: 10}); err != nil {
		t.Fatalf("条件失败后会话不可用: %v", err)
	}

	obj, err := nodes[0].cl.CompleteMultipartUpload(bucket, key, up.UploadID, parts, "", false)
	if err != nil {
		t.Fatalf("Complete 失败: %v", err)
	}
	if obj.Version <= oldInfo.Version {
		t.Fatalf("Complete 后版本 = %d，期望大于 %d", obj.Version, oldInfo.Version)
	}
	if got := readAll(t, nodes[2].cl, bucket, key); !bytes.Equal(got, data) {
		t.Fatal("覆盖后读取的对象内容不一致")
	}
}

// TestListMultipartUploadsFanOut 验证列表扇形到全部节点，并且分页、
// prefix、delimiter 语义与单机一致。
func TestListMultipartUploadsFanOut(t *testing.T) {
	nodes := newTestNodesWithBlockSize(t, 3, mpuBlockSize)
	const bucket = "mpu-list"
	createBucketOnAll(t, nodes, bucket)

	// 会话放在不同协调者上：只查一个节点必然漏。
	upA := createMPU(t, nodes[0].cl, bucket, "a/one", store.CreateMultipartOptions{})
	upB := createMPU(t, nodes[1].cl, bucket, "b/two", store.CreateMultipartOptions{})

	list, err := nodes[2].cl.ListMultipartUploads(bucket, store.ListMultipartUploadsOptions{MaxUploads: 10})
	if err != nil {
		t.Fatalf("列举会话失败: %v", err)
	}
	if len(list.Uploads) != 2 || list.Uploads[0].UploadID != upA.UploadID || list.Uploads[1].UploadID != upB.UploadID {
		t.Fatalf("会话列表 = %+v", list.Uploads)
	}

	page, err := nodes[2].cl.ListMultipartUploads(bucket, store.ListMultipartUploadsOptions{MaxUploads: 1})
	if err != nil {
		t.Fatalf("分页列举会话失败: %v", err)
	}
	if !page.IsTruncated || len(page.Uploads) != 1 || page.NextKeyMarker != "a/one" || page.NextUploadID != upA.UploadID {
		t.Fatalf("第一页 = %+v", page)
	}
	next, err := nodes[2].cl.ListMultipartUploads(bucket, store.ListMultipartUploadsOptions{
		MaxUploads: 10, KeyMarker: page.NextKeyMarker, UploadIDMarker: page.NextUploadID,
	})
	if err != nil {
		t.Fatalf("翻页失败: %v", err)
	}
	if len(next.Uploads) != 1 || next.Uploads[0].UploadID != upB.UploadID {
		t.Fatalf("第二页 = %+v", next.Uploads)
	}

	prefixed, err := nodes[2].cl.ListMultipartUploads(bucket, store.ListMultipartUploadsOptions{MaxUploads: 10, Prefix: "b/"})
	if err != nil {
		t.Fatalf("前缀列举失败: %v", err)
	}
	if len(prefixed.Uploads) != 1 || prefixed.Uploads[0].UploadID != upB.UploadID {
		t.Fatalf("前缀列举结果 = %+v", prefixed.Uploads)
	}

	folded, err := nodes[2].cl.ListMultipartUploads(bucket, store.ListMultipartUploadsOptions{MaxUploads: 10, Delimiter: "/"})
	if err != nil {
		t.Fatalf("delimiter 列举失败: %v", err)
	}
	if len(folded.Uploads) != 0 || len(folded.CommonPrefixes) != 2 ||
		folded.CommonPrefixes[0] != "a/" || folded.CommonPrefixes[1] != "b/" {
		t.Fatalf("delimiter 结果 = uploads %+v, prefixes %+v", folded.Uploads, folded.CommonPrefixes)
	}
}
