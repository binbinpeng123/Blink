package store

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/binbinpeng123/Blink/internal/checksum"
)

// mpuBytes 生成可复现的测试数据：每个位置的值由 seed 与下标决定，
// 这样任意两个分片的边界一旦错位，比对就会在首个不一致处报出来。
func mpuBytes(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31) ^ seed
	}
	return b
}

// sumOf 计算一段数据的校验和（base64），用于模拟客户端声明。
func sumOf(t *testing.T, alg checksum.Algorithm, data []byte) string {
	t.Helper()
	sum, ok := checksum.New(alg)
	if !ok {
		t.Fatalf("未知算法 %s", alg)
	}
	if _, err := sum.Write(data); err != nil {
		t.Fatalf("写入校验和失败: %v", err)
	}
	return sum.Encoded()
}

// openMPUStore 打开一个新存储并建桶，供分片测试使用。
func openMPUStore(t *testing.T) *Store {
	t.Helper()
	return newSmallBlockStore(t, 64<<10, 16)
}

func createUpload(t *testing.T, s *Store, key string, alg checksum.Algorithm, ct string) *MultipartUpload {
	t.Helper()
	up, err := s.CreateMultipartUpload("bkt", key, CreateMultipartOptions{
		ChecksumAlgorithm: alg,
		ChecksumType:      ct,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload(%q) 失败: %v", key, err)
	}
	return up
}

func uploadPart(t *testing.T, s *Store, key, id string, n int, data []byte, opt UploadPartOptions) *PartInfo {
	t.Helper()
	opt.ExpectedSize = int64(len(data))
	info, err := s.UploadPart("bkt", key, id, n, bytes.NewReader(data), opt)
	if err != nil {
		t.Fatalf("UploadPart(%d) 失败: %v", n, err)
	}
	return info
}

// readObjectAll 通过公开读取路径拼回对象内容。
func readObjectAll(t *testing.T, s *Store, key string) []byte {
	t.Helper()
	r, err := s.OpenObject("bkt", key)
	if err != nil {
		t.Fatalf("OpenObject(%q) 失败: %v", key, err)
	}
	var buf bytes.Buffer
	if err := r.WriteRangeTo(&buf, 0, r.Info().Size); err != nil {
		t.Fatalf("读取对象失败: %v", err)
	}
	return buf.Bytes()
}

func TestMultipartFullObjectHappyPath(t *testing.T) {
	s := openMPUStore(t)
	up := createUpload(t, s, "models/weights.bin", checksum.AlgCRC64NVME, "")
	if up.ChecksumType != ChecksumTypeFullObject {
		t.Fatalf("CRC64NVME 未声明类型时应为 FULL_OBJECT，得到 %q", up.ChecksumType)
	}
	if !validUploadID(up.UploadID) {
		t.Fatalf("UploadID %q 不是 32 位十六进制", up.UploadID)
	}

	p1 := mpuBytes(minPartSize, 0x11)
	p2 := mpuBytes(1234, 0x22)
	want := append(append([]byte{}, p1...), p2...)

	i1 := uploadPart(t, s, "models/weights.bin", up.UploadID, 1, p1, UploadPartOptions{
		ChecksumAlgorithm: checksum.AlgCRC64NVME,
		Checksum:          sumOf(t, checksum.AlgCRC64NVME, p1),
	})
	i2 := uploadPart(t, s, "models/weights.bin", up.UploadID, 2, p2, UploadPartOptions{})

	if len(i1.ETag) != 32 || len(i2.ETag) != 32 {
		t.Fatalf("分片 ETag 应为 32 位 MD5 hex，得到 %q / %q", i1.ETag, i2.ETag)
	}

	parts := []CompletePart{{1, i1.ETag}, {2, `"` + i2.ETag + `"`}}
	// 客户端在 Complete 时可以带引号；服务端必须接受。
	full := sumOf(t, checksum.AlgCRC64NVME, want)
	info, err := s.CompleteMultipartUpload("bkt", "models/weights.bin", up.UploadID, parts, full, false)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload 失败: %v", err)
	}

	if info.Size != int64(len(want)) {
		t.Fatalf("对象大小 = %d，期望 %d", info.Size, len(want))
	}
	if info.Checksum != full {
		t.Fatalf("FULL_OBJECT 校验和 = %s，期望 %s（合并必须等于整段直算）", info.Checksum, full)
	}
	if info.ChecksumType != ChecksumTypeFullObject {
		t.Fatalf("ChecksumType = %q，期望 FULL_OBJECT", info.ChecksumType)
	}
	if !strings.HasSuffix(info.ETag, "-2") {
		t.Fatalf("multipart ETag = %q，应以 -2 结尾", info.ETag)
	}
	if info.Version != 1 {
		t.Fatalf("首次完成 obj_version = %d，期望 1", info.Version)
	}

	got := readObjectAll(t, s, "models/weights.bin")
	if !bytes.Equal(got, want) {
		t.Fatalf("读回内容与上传内容不一致（%d vs %d 字节）", len(got), len(want))
	}

	// 完成后临时状态必须被清掉，否则会一直占着"进行中上传"的名额。
	if _, err := os.Stat(s.multipartUploadDir(up.UploadID)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("完成后分片目录仍存在: %v", err)
	}
	if _, err := s.ListParts("bkt", "models/weights.bin", up.UploadID, ListPartsOptions{MaxParts: 10}); !errors.Is(err, ErrNoSuchUpload) {
		t.Fatalf("完成后 ListParts 应返回 ErrNoSuchUpload，得到 %v", err)
	}
}

// 会话创建时没有声明算法、UploadPart 却自带校验和：必须采用该算法，
// 而不是拒绝。aws-cli 的 create-multipart-upload 不带 --checksum-algorithm
// 时正是这条路径（upload-part 仍按默认 CRC64NVME 自动算出并发送），
// 真实 S3 也接受这种组合。
func TestUploadPartAdoptsChecksumAlgorithm(t *testing.T) {
	s := openMPUStore(t)
	up := createUpload(t, s, "adopted.bin", "", "")
	if up.ChecksumType != "" {
		t.Fatalf("未声明算法时 ChecksumType 应为空，得到 %q", up.ChecksumType)
	}

	p1 := mpuBytes(minPartSize, 0x33)
	p2 := mpuBytes(999, 0x44)
	want := append(append([]byte{}, p1...), p2...)
	sum1 := sumOf(t, checksum.AlgCRC64NVME, p1)
	sum2 := sumOf(t, checksum.AlgCRC64NVME, p2)

	i1 := uploadPart(t, s, "adopted.bin", up.UploadID, 1, p1, UploadPartOptions{
		ChecksumAlgorithm: checksum.AlgCRC64NVME,
		Checksum:          sum1,
	})
	if i1.Checksum != sum1 || i1.ChecksumAlgorithm != checksum.AlgCRC64NVME {
		t.Fatalf("分片 1 校验和 = %q/%s，期望 %q/CRC64NVME",
			i1.Checksum, i1.ChecksumAlgorithm, sum1)
	}

	// 算法必须已经写进会话状态，后续 ListParts 也要能看到。
	lp, err := s.ListParts("bkt", "adopted.bin", up.UploadID, ListPartsOptions{MaxParts: 10})
	if err != nil {
		t.Fatalf("ListParts 失败: %v", err)
	}
	if lp.Upload.ChecksumAlgorithm != checksum.AlgCRC64NVME {
		t.Fatalf("会话算法 = %q，期望 CRC64NVME", lp.Upload.ChecksumAlgorithm)
	}
	if lp.Upload.ChecksumType != ChecksumTypeFullObject {
		t.Fatalf("CRC64NVME 默认类型 = %q，期望 FULL_OBJECT", lp.Upload.ChecksumType)
	}

	// 之后的混用算法请求必须被拒绝，不能在同一个对象里混两种校验和。
	if _, err := s.UploadPart("bkt", "adopted.bin", up.UploadID, 2,
		bytes.NewReader(p2), UploadPartOptions{
			ExpectedSize:      int64(len(p2)),
			ChecksumAlgorithm: checksum.AlgSHA256,
		}); !errors.Is(err, ErrInvalidChecksum) {
		t.Fatalf("混用算法应返回 ErrInvalidChecksum，得到 %v", err)
	}

	i2 := uploadPart(t, s, "adopted.bin", up.UploadID, 2, p2, UploadPartOptions{
		ChecksumAlgorithm: checksum.AlgCRC64NVME,
		Checksum:          sum2,
	})
	full := sumOf(t, checksum.AlgCRC64NVME, want)
	info, err := s.CompleteMultipartUpload("bkt", "adopted.bin", up.UploadID,
		[]CompletePart{{1, i1.ETag}, {2, i2.ETag}}, full, false)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload 失败: %v", err)
	}
	if info.Checksum != full {
		t.Fatalf("FULL_OBJECT 校验和 = %s，期望 %s", info.Checksum, full)
	}
	if !bytes.Equal(readObjectAll(t, s, "adopted.bin"), want) {
		t.Fatal("读回内容与上传内容不一致")
	}
}

func TestMultipartCompositeChecksum(t *testing.T) {
	s := openMPUStore(t)
	// CRC32C 未声明类型：AWS 与 02 文档都默认 COMPOSITE。
	up := createUpload(t, s, "k", checksum.AlgCRC32C, "")
	if up.ChecksumType != ChecksumTypeComposite {
		t.Fatalf("CRC32C 未声明类型时应为 COMPOSITE，得到 %q", up.ChecksumType)
	}

	a := mpuBytes(minPartSize, 1)
	b := mpuBytes(17, 2)
	ia := uploadPart(t, s, "k", up.UploadID, 1, a, UploadPartOptions{})
	ib := uploadPart(t, s, "k", up.UploadID, 2, b, UploadPartOptions{})

	want, ok := checksum.MergeComposite(checksum.AlgCRC32C, []checksum.PartChecksum{
		{Encoded: ia.Checksum, Size: ia.Size},
		{Encoded: ib.Checksum, Size: ib.Size},
	})
	if !ok {
		t.Fatal("MergeComposite 失败")
	}
	info, err := s.CompleteMultipartUpload("bkt", "k", up.UploadID,
		[]CompletePart{{1, ia.ETag}, {2, ib.ETag}}, want, false)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload 失败: %v", err)
	}
	if info.Checksum != want || info.ChecksumType != ChecksumTypeComposite {
		t.Fatalf("checksum = %s/%s，期望 %s/COMPOSITE", info.Checksum, info.ChecksumType, want)
	}

	// SHA256 只能 COMPOSITE；声明 FULL_OBJECT 必须被拒。
	if _, err := s.CreateMultipartUpload("bkt", "sha", CreateMultipartOptions{
		ChecksumAlgorithm: checksum.AlgSHA256, ChecksumType: ChecksumTypeFullObject,
	}); !errors.Is(err, ErrInvalidChecksum) {
		t.Fatalf("SHA256 + FULL_OBJECT 应返回 ErrInvalidChecksum，得到 %v", err)
	}
	// 没有算法却声明类型：同样拒绝，而不是猜一个算法。
	if _, err := s.CreateMultipartUpload("bkt", "x", CreateMultipartOptions{
		ChecksumType: ChecksumTypeComposite,
	}); !errors.Is(err, ErrInvalidChecksum) {
		t.Fatalf("无算法 + 类型应返回 ErrInvalidChecksum，得到 %v", err)
	}
}

func TestMultipartCompleteValidation(t *testing.T) {
	s := openMPUStore(t)
	// 用带算法（CRC64NVME）的会话，才能顺带验证"声明的全对象校验和"。
	up := createUpload(t, s, "obj", checksum.AlgCRC64NVME, "")
	big := mpuBytes(minPartSize, 4)
	small := mpuBytes(1024, 3)
	e1 := uploadPart(t, s, "obj", up.UploadID, 1, big, UploadPartOptions{})
	e2 := uploadPart(t, s, "obj", up.UploadID, 2, big, UploadPartOptions{})
	e3 := uploadPart(t, s, "obj", up.UploadID, 3, small, UploadPartOptions{})
	e4 := uploadPart(t, s, "obj", up.UploadID, 4, big, UploadPartOptions{})

	cases := []struct {
		name  string
		parts []CompletePart
		want  error
	}{
		{"非末片过小", []CompletePart{{3, e3.ETag}, {4, e4.ETag}}, ErrEntityTooSmall},
		{"乱序", []CompletePart{{2, e2.ETag}, {1, e1.ETag}}, ErrInvalidPartOrder},
		{"重复编号", []CompletePart{{1, e1.ETag}, {1, e1.ETag}}, ErrInvalidPartOrder},
		{"分片不存在", []CompletePart{{1, e1.ETag}, {5, e4.ETag}}, ErrInvalidPart},
		{"ETag 不符", []CompletePart{{1, strings.Repeat("0", 32)}, {2, e2.ETag}}, ErrInvalidPart},
		{"空列表", nil, ErrInvalidPartOrder},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.CompleteMultipartUpload("bkt", "obj", up.UploadID, tc.parts, "", false)
			if !errors.Is(err, tc.want) {
				t.Fatalf("得到 %v，期望 %v", err, tc.want)
			}
		})
	}

	// 失败之后会话仍可继续使用：校验错误不是破坏性操作。
	if _, err := s.ListParts("bkt", "obj", up.UploadID, ListPartsOptions{MaxParts: 10}); err != nil {
		t.Fatalf("校验失败后 ListParts 不应失败: %v", err)
	}

	// 声明了错误的全对象校验和 → BadDigest。
	_, err := s.CompleteMultipartUpload("bkt", "obj", up.UploadID,
		[]CompletePart{{4, e4.ETag}}, "AAAA", false)
	if !errors.Is(err, ErrBadDigest) {
		t.Fatalf("错误的全对象校验和应返回 ErrBadDigest，得到 %v", err)
	}
}

func TestMultipartUploadValidation(t *testing.T) {
	s := openMPUStore(t)
	up := createUpload(t, s, "obj", checksum.AlgCRC32C, "")
	body := []byte("hello")

	if _, err := s.UploadPart("bkt", "obj", up.UploadID, 0, bytes.NewReader(body), UploadPartOptions{ExpectedSize: 5}); !errors.Is(err, ErrInvalidPartNumber) {
		t.Fatalf("partNumber=0 应返回 ErrInvalidPartNumber，得到 %v", err)
	}
	if _, err := s.UploadPart("bkt", "obj", up.UploadID, 10001, bytes.NewReader(body), UploadPartOptions{ExpectedSize: 5}); !errors.Is(err, ErrInvalidPartNumber) {
		t.Fatalf("partNumber=10001 应返回 ErrInvalidPartNumber，得到 %v", err)
	}
	// 会话是 CRC32C，客户端却声明 SHA256。
	if _, err := s.UploadPart("bkt", "obj", up.UploadID, 1, bytes.NewReader(body), UploadPartOptions{
		ExpectedSize: 5, ChecksumAlgorithm: checksum.AlgSHA256,
	}); !errors.Is(err, ErrInvalidChecksum) {
		t.Fatalf("算法与会话不符应返回 ErrInvalidChecksum，得到 %v", err)
	}
	// 长度与会话无关，必须严格匹配。
	if _, err := s.UploadPart("bkt", "obj", up.UploadID, 1, bytes.NewReader(body), UploadPartOptions{ExpectedSize: 4}); !errors.Is(err, ErrIncompleteBody) {
		t.Fatalf("长度不符应返回 ErrIncompleteBody，得到 %v", err)
	}
	// 分片校验和错误。
	if _, err := s.UploadPart("bkt", "obj", up.UploadID, 1, bytes.NewReader(body), UploadPartOptions{
		ExpectedSize: 5, Checksum: "AAAA",
	}); !errors.Is(err, ErrBadDigest) {
		t.Fatalf("错误的分片校验和应返回 ErrBadDigest，得到 %v", err)
	}
	// 会话不属于这个 key / bucket：按不存在处理，不泄露归属。
	if _, err := s.UploadPart("bkt", "other", up.UploadID, 1, bytes.NewReader(body), UploadPartOptions{ExpectedSize: 5}); !errors.Is(err, ErrNoSuchUpload) {
		t.Fatalf("key 不符应返回 ErrNoSuchUpload，得到 %v", err)
	}

	// UploadId 直接拼进路径，必须只接受自己生成的格式。
	for _, id := range []string{"../../etc/passwd", strings.Repeat("A", 32), "", "0123456789abcdef"} {
		if _, err := s.UploadPart("bkt", "obj", id, 1, bytes.NewReader(body), UploadPartOptions{ExpectedSize: 5}); !errors.Is(err, ErrNoSuchUpload) {
			t.Fatalf("非法 uploadId %q 应返回 ErrNoSuchUpload，得到 %v", id, err)
		}
		if _, err := s.ListParts("bkt", "obj", id, ListPartsOptions{MaxParts: 10}); !errors.Is(err, ErrNoSuchUpload) {
			t.Fatalf("非法 uploadId %q 的 ListParts 应返回 ErrNoSuchUpload，得到 %v", id, err)
		}
	}
}

func TestMultipartAbort(t *testing.T) {
	s := openMPUStore(t)
	up := createUpload(t, s, "obj", "", "")
	uploadPart(t, s, "obj", up.UploadID, 1, []byte("x"), UploadPartOptions{})

	if err := s.AbortMultipartUpload("bkt", "obj", up.UploadID); err != nil {
		t.Fatalf("AbortMultipartUpload 失败: %v", err)
	}
	if err := s.AbortMultipartUpload("bkt", "obj", up.UploadID); !errors.Is(err, ErrNoSuchUpload) {
		t.Fatalf("重复 Abort 应返回 ErrNoSuchUpload，得到 %v", err)
	}
	if _, err := s.ListParts("bkt", "obj", up.UploadID, ListPartsOptions{MaxParts: 10}); !errors.Is(err, ErrNoSuchUpload) {
		t.Fatalf("Abort 后 ListParts 应返回 ErrNoSuchUpload，得到 %v", err)
	}
	if _, err := s.CompleteMultipartUpload("bkt", "obj", up.UploadID,
		[]CompletePart{{1, ""}}, "", false); !errors.Is(err, ErrNoSuchUpload) {
		t.Fatalf("Abort 后 Complete 应返回 ErrNoSuchUpload，得到 %v", err)
	}

	// Abort 不应留下"进行中上传"，否则桶永远删不掉。
	if empty, err := s.bucketIsEmpty("bkt"); err != nil || !empty {
		t.Fatalf("Abort 后桶应为空，empty=%v err=%v", empty, err)
	}
}

func TestListPartsPagination(t *testing.T) {
	s := openMPUStore(t)
	up := createUpload(t, s, "obj", "", "")
	for i := 1; i <= 5; i++ {
		uploadPart(t, s, "obj", up.UploadID, i, []byte{byte(i)}, UploadPartOptions{})
	}

	var got []int
	marker := 0
	for page := 0; page < 5; page++ {
		r, err := s.ListParts("bkt", "obj", up.UploadID, ListPartsOptions{MaxParts: 2, PartNumberMarker: marker})
		if err != nil {
			t.Fatalf("ListParts 失败: %v", err)
		}
		for _, p := range r.Parts {
			got = append(got, p.PartNumber)
		}
		if !r.IsTruncated {
			marker = 0
			break
		}
		if len(r.Parts) == 0 {
			t.Fatal("IsTruncated=true 却没有返回分片")
		}
		marker = r.NextPartNumberMarker
	}
	if fmt.Sprint(got) != "[1 2 3 4 5]" {
		t.Fatalf("分页结果 = %v，期望 [1 2 3 4 5]", got)
	}
	if marker != 0 {
		t.Fatalf("最后一页不应再给出 marker，得到 %d", marker)
	}
}

func TestListMultipartUploads(t *testing.T) {
	s := openMPUStore(t)
	ids := map[string]string{}
	for _, key := range []string{"a/1", "a/2", "b", "c"} {
		ids[key] = createUpload(t, s, key, "", "").UploadID
	}

	// delimiter 折叠：a/1 与 a/2 合成一个公共前缀，其余照常返回。
	r, err := s.ListMultipartUploads("bkt", ListMultipartUploadsOptions{MaxUploads: 100, Delimiter: "/"})
	if err != nil {
		t.Fatalf("ListMultipartUploads 失败: %v", err)
	}
	if fmt.Sprint(r.CommonPrefixes) != "[a/]" {
		t.Fatalf("CommonPrefixes = %v，期望 [a/]", r.CommonPrefixes)
	}
	if len(r.Uploads) != 2 || r.Uploads[0].Key != "b" || r.Uploads[1].Key != "c" {
		t.Fatalf("Uploads = %+v，期望 [b c]", r.Uploads)
	}

	// prefix 过滤。
	r, err = s.ListMultipartUploads("bkt", ListMultipartUploadsOptions{MaxUploads: 100, Prefix: "a/"})
	if err != nil {
		t.Fatalf("ListMultipartUploads 失败: %v", err)
	}
	if len(r.Uploads) != 2 || r.Uploads[0].Key != "a/1" || r.Uploads[1].Key != "a/2" {
		t.Fatalf("prefix 结果 = %+v", r.Uploads)
	}

	// 分页：每页一条，拼接后与全量一致且不重复。
	var seen []string
	opt := ListMultipartUploadsOptions{MaxUploads: 2}
	for page := 0; page < 5; page++ {
		r, err := s.ListMultipartUploads("bkt", opt)
		if err != nil {
			t.Fatalf("ListMultipartUploads 失败: %v", err)
		}
		for _, u := range r.Uploads {
			seen = append(seen, u.Key)
		}
		if !r.IsTruncated {
			break
		}
		opt.KeyMarker, opt.UploadIDMarker = r.NextKeyMarker, r.NextUploadID
	}
	if fmt.Sprint(seen) != "[a/1 a/2 b c]" {
		t.Fatalf("分页拼接 = %v，期望 [a/1 a/2 b c]", seen)
	}

	// 公共前缀作为分页边界时，下一页不能再把同一前缀下的对象吐成独立上传。
	opt = ListMultipartUploadsOptions{MaxUploads: 1, Delimiter: "/"}
	r, err = s.ListMultipartUploads("bkt", opt)
	if err != nil {
		t.Fatalf("ListMultipartUploads 失败: %v", err)
	}
	if len(r.CommonPrefixes) != 1 || !r.IsTruncated {
		t.Fatalf("第一页应为 1 个公共前缀且截断，得到 %+v", r)
	}
	r2, err := s.ListMultipartUploads("bkt", ListMultipartUploadsOptions{
		MaxUploads: 10, Delimiter: "/",
		KeyMarker: r.NextKeyMarker, UploadIDMarker: r.NextUploadID,
	})
	if err != nil {
		t.Fatalf("第二页失败: %v", err)
	}
	if len(r2.CommonPrefixes) != 0 {
		t.Fatalf("第二页不应重复返回公共前缀: %v", r2.CommonPrefixes)
	}
	if len(r2.Uploads) != 2 {
		t.Fatalf("第二页应有 b、c 两条上传，得到 %+v", r2.Uploads)
	}

	// 桶不存在与不存在的标记都不应误报。
	if _, err := s.ListMultipartUploads("nope", ListMultipartUploadsOptions{MaxUploads: 10}); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("不存在的桶应返回 ErrBucketNotFound，得到 %v", err)
	}
}

func TestListMultipartUploadsSameKey(t *testing.T) {
	s := openMPUStore(t)
	u1 := createUpload(t, s, "same", "", "")
	u2 := createUpload(t, s, "same", "", "")

	r, err := s.ListMultipartUploads("bkt", ListMultipartUploadsOptions{MaxUploads: 1})
	if err != nil {
		t.Fatalf("ListMultipartUploads 失败: %v", err)
	}
	if !r.IsTruncated || len(r.Uploads) != 1 {
		t.Fatalf("应截断为 1 条，得到 %+v", r)
	}
	first := r.Uploads[0].UploadID
	if r.NextUploadID != first || r.NextKeyMarker != "same" {
		t.Fatalf("Next markers = %q/%q，期望 same/%s", r.NextKeyMarker, r.NextUploadID, first)
	}
	r2, err := s.ListMultipartUploads("bkt", ListMultipartUploadsOptions{
		MaxUploads: 10, KeyMarker: r.NextKeyMarker, UploadIDMarker: r.NextUploadID,
	})
	if err != nil {
		t.Fatalf("第二页失败: %v", err)
	}
	if len(r2.Uploads) != 1 || r2.Uploads[0].UploadID == first {
		t.Fatalf("第二页应返回另一个 uploadId，得到 %+v", r2.Uploads)
	}
	_ = u1
	_ = u2
}

func TestDeleteBucketWithActiveUpload(t *testing.T) {
	s := openMPUStore(t)
	up := createUpload(t, s, "obj", "", "")
	if err := s.DeleteBucket("bkt"); !errors.Is(err, ErrBucketNotEmpty) {
		t.Fatalf("存在进行中上传时删桶应返回 ErrBucketNotEmpty，得到 %v", err)
	}
	if err := s.AbortMultipartUpload("bkt", "obj", up.UploadID); err != nil {
		t.Fatalf("AbortMultipartUpload 失败: %v", err)
	}
	if err := s.DeleteBucket("bkt"); err != nil {
		t.Fatalf("清理后删桶应成功: %v", err)
	}
}

func TestMultipartIfNoneMatchAndVersion(t *testing.T) {
	s := openMPUStore(t)
	putString(t, s, "bkt", "obj", "old")

	up := createUpload(t, s, "obj", "", "")
	p := mpuBytes(minPartSize, 7)
	ip := uploadPart(t, s, "obj", up.UploadID, 1, p, UploadPartOptions{})
	if _, err := s.CompleteMultipartUpload("bkt", "obj", up.UploadID,
		[]CompletePart{{1, ip.ETag}}, "", true); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("If-None-Match:* 且对象存在时应返回 ErrPreconditionFailed，得到 %v", err)
	}
	// 条件失败不消耗会话。
	info, err := s.CompleteMultipartUpload("bkt", "obj", up.UploadID,
		[]CompletePart{{1, ip.ETag}}, "", false)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload 失败: %v", err)
	}
	if info.Version != 2 {
		t.Fatalf("覆盖后 obj_version = %d，期望 2", info.Version)
	}
	if got := readObjectAll(t, s, "obj"); !bytes.Equal(got, p) {
		t.Fatal("覆盖后的内容不正确")
	}
}

func TestMultipartSurvivesRestart(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")
	open := func(now func() time.Time) *Store {
		s, err := Open(Options{Root: root, BlockSize: 64 << 10, InlineThreshold: 16, Now: now})
		if err != nil {
			t.Fatalf("Open 失败: %v", err)
		}
		return s
	}
	fixed := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	s := open(func() time.Time { return fixed })
	if err := s.CreateBucket("bkt"); err != nil {
		t.Fatalf("CreateBucket 失败: %v", err)
	}
	up := createUpload(t, s, "obj", checksum.AlgCRC64NVME, "")
	p := mpuBytes(minPartSize, 9)
	ip := uploadPart(t, s, "obj", up.UploadID, 1, p, UploadPartOptions{})

	// 重启后会话与分片都应还在（进程重启不该让客户端的上传白白丢掉）。
	s2 := open(func() time.Time { return fixed.Add(time.Hour) })
	r, err := s2.ListParts("bkt", "obj", up.UploadID, ListPartsOptions{MaxParts: 10})
	if err != nil {
		t.Fatalf("重启后 ListParts 失败: %v", err)
	}
	if len(r.Parts) != 1 || r.Parts[0].ETag != ip.ETag {
		t.Fatalf("重启后分片不一致: %+v", r.Parts)
	}
	info, err := s2.CompleteMultipartUpload("bkt", "obj", up.UploadID,
		[]CompletePart{{1, ip.ETag}}, "", false)
	if err != nil {
		t.Fatalf("重启后 Complete 失败: %v", err)
	}
	if got := readObjectAll(t, s2, "obj"); !bytes.Equal(got, p) || info.Size != int64(len(p)) {
		t.Fatal("重启后完成的对象内容不正确")
	}
}

func TestCleanExpiredUploads(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")
	fixed := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	open := func(now time.Time) *Store {
		s, err := Open(Options{
			Root: root, BlockSize: 64 << 10, InlineThreshold: 16,
			Now: func() time.Time { return now },
		})
		if err != nil {
			t.Fatalf("Open 失败: %v", err)
		}
		if s.CleanupError() != nil {
			t.Fatalf("启动清扫报错: %v", s.CleanupError())
		}
		return s
	}

	s := open(fixed)
	if err := s.CreateBucket("bkt"); err != nil {
		t.Fatalf("CreateBucket 失败: %v", err)
	}
	old := createUpload(t, s, "old", "", "")

	// 手工造一个缺 upload.json 的残骸目录。
	orphan := s.multipartUploadDir("0123456789abcdef0123456789abcdef")
	if err := os.MkdirAll(orphan, 0o750); err != nil {
		t.Fatalf("创建残骸目录失败: %v", err)
	}

	// 六天后再建一个会话：与 old 拉开距离，才能区分"过期"与"未过期"。
	s = open(fixed.Add(6 * 24 * time.Hour))
	fresh := createUpload(t, s, "fresh", "", "")

	// 未过期：全都保留。
	s = open(fixed.Add(6*24*time.Hour + time.Hour))
	if _, err := s.ListParts("bkt", "old", old.UploadID, ListPartsOptions{MaxParts: 10}); err != nil {
		t.Fatalf("未过期时不应清理: %v", err)
	}

	// 过期：只删过期的与残骸，保留新会话。
	s = open(fixed.Add(multipartRetention + time.Hour))
	if _, err := s.ListParts("bkt", "old", old.UploadID, ListPartsOptions{MaxParts: 10}); !errors.Is(err, ErrNoSuchUpload) {
		t.Fatalf("过期会话应被清理，得到 %v", err)
	}
	if _, err := s.ListParts("bkt", "fresh", fresh.UploadID, ListPartsOptions{MaxParts: 10}); err != nil {
		t.Fatalf("新会话不应被清理: %v", err)
	}
	if _, err := os.Stat(orphan); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("残骸目录应被清理: %v", err)
	}
}

// BenchmarkCompleteMultipartBlockTable 验证创新 B：Complete 的成本与
// 对象字节数无关，只与块表长度有关。
//
// 它不真的写 20 GiB —— 造那样的对象要几十分钟，CI 跑不动。做法是用
// 4 KiB 块装 4 × 5 MiB 的分片，得到 5120 个块描述，与"20 GiB 对象按
// 4 MiB 切块"的块表长度完全相同；因此这里测到的 JSON 序列化与 rename
// 成本就是 20 GiB 对象 Complete 的成本。实际落盘数据只有 20 MiB，
// 这正是要证明的：Complete 不重读、不复制数据。
//
// 断言：每次 Complete 写入 meta.json 的字节数 < 1 MiB，且不随对象
// 数据量增长（数据量在 b.SetBytes 里只作吞吐参考，不参与成本）。
func BenchmarkCompleteMultipartBlockTable(b *testing.B) {
	const (
		blockSize = 4 << 10
		partSize  = minPartSize
		nParts    = 4
	)
	s, err := Open(Options{
		Root:            filepath.Join(b.TempDir(), "data"),
		BlockSize:       blockSize,
		InlineThreshold: -1,
	})
	if err != nil {
		b.Fatalf("Open 失败: %v", err)
	}
	if err := s.CreateBucket("bkt"); err != nil {
		b.Fatalf("CreateBucket 失败: %v", err)
	}

	// 分片内容各不相同，避免 CAS 去重把块表压得过短。
	data := make([][]byte, nParts)
	for i := range data {
		data[i] = mpuBytes(partSize, byte(i))
	}

	b.SetBytes(partSize * nParts)
	var completeNS int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		up, err := s.CreateMultipartUpload("bkt", "bench", CreateMultipartOptions{})
		if err != nil {
			b.Fatalf("CreateMultipartUpload 失败: %v", err)
		}
		var parts []CompletePart
		for j, d := range data {
			pi, err := s.UploadPart("bkt", "bench", up.UploadID, j+1, bytes.NewReader(d), UploadPartOptions{
				ExpectedSize: int64(len(d)),
			})
			if err != nil {
				b.Fatalf("UploadPart 失败: %v", err)
			}
			parts = append(parts, CompletePart{PartNumber: j + 1, ETag: pi.ETag})
		}
		t0 := time.Now()
		if _, err := s.CompleteMultipartUpload("bkt", "bench", up.UploadID, parts, "", false); err != nil {
			b.Fatalf("Complete 失败: %v", err)
		}
		completeNS += time.Since(t0).Nanoseconds()
	}
	b.StopTimer()

	// 单独报告 Complete 自己的耗时：整轮 ns/op 里绝大部分是 20 MiB
	// 的 UploadPart 写盘，与"完成一个 20 GiB 对象要多久"无关。
	b.ReportMetric(float64(completeNS)/float64(b.N)/1e6, "complete-ms/op")

	// 写入量证据：meta.json 的大小就是 Complete 的写盘字节数。
	fi, err := s.fs.Stat(s.objectMetaPath("bkt", "bench"))
	if err != nil {
		b.Fatalf("Stat meta.json 失败: %v", err)
	}
	if fi.Size() >= 1<<20 {
		b.Fatalf("Complete 写入 %d 字节，超过 1 MiB 上限", fi.Size())
	}
	b.ReportMetric(float64(fi.Size()), "meta-B/op")
}
