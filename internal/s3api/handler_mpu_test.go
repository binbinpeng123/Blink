package s3api

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/binbinpeng123/Blink/internal/checksum"
	"github.com/binbinpeng123/Blink/internal/store"
)

// mpuTestServer 构造接了真实 store 的测试服务器。
//
// 块大小压到 64 KiB：分片下限是 5 MiB，用默认的 4 MiB 块会切出
// 大量 CAS 文件，测试变慢；64 KiB 既覆盖多块切分，又只产生 80 个块。
func mpuTestServer(t *testing.T) http.Handler {
	t.Helper()
	st, err := store.Open(store.Options{
		Root:            filepath.Join(t.TempDir(), "data"),
		BlockSize:       64 << 10,
		InlineThreshold: 16,
	})
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	if err := st.CreateBucket("bkt"); err != nil {
		t.Fatalf("建桶失败: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(Options{
		Logger:      logger,
		Region:      "us-east-1",
		DisableAuth: true,
		Store:       st,
	}).Handler()
}

// mpuRequest 发一个请求；mutate 用于按测试需要改写请求。
func mpuRequest(t *testing.T, h http.Handler, method, target string, body io.Reader, mutate func(*http.Request)) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, body)
	if mutate != nil {
		mutate(req)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// mpuPayload 生成可复现的数据：内容随下标与 seed 变化，
// 分片边界一旦错位，比对会在首个不一致处报出来。
func mpuPayload(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31) ^ seed
	}
	return b
}

// sumOfData 计算数据的 base64 校验和，用于模拟客户端声明。
func sumOfData(t *testing.T, alg checksum.Algorithm, data []byte) string {
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

// createUploadHTTP 发起 CreateMultipartUpload 并返回 uploadId。
func createUploadHTTP(t *testing.T, h http.Handler, key string, mutate func(*http.Request)) string {
	t.Helper()
	rec := mpuRequest(t, h, http.MethodPost, "/bkt/"+key+"?uploads", nil, mutate)
	if rec.Code != http.StatusOK {
		t.Fatalf("CreateMultipartUpload(%q) = %d：%s", key, rec.Code, rec.Body.String())
	}
	var out initiateMultipartUploadResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("解析 Create 响应失败: %v（%s）", err, rec.Body.String())
	}
	if out.UploadID == "" {
		t.Fatalf("Create 响应缺少 UploadId: %s", rec.Body.String())
	}
	return out.UploadID
}

func TestMultipartHTTPFlow(t *testing.T) {
	h := mpuTestServer(t)
	const key = "models/model.bin"

	// aws-cli 2.36.2 的 CreateMultipartUpload 用的是不带 sdk- 前缀的
	// `x-amz-checksum-algorithm`。这条断言守住那个回退分支。
	rec := mpuRequest(t, h, http.MethodPost, "/bkt/"+key+"?uploads", nil, func(r *http.Request) {
		r.Header.Set("x-amz-checksum-algorithm", "CRC64NVME")
		r.Header.Set("Content-Type", "application/octet-stream")
		r.Header.Set("x-amz-meta-owner", "edge-01")
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("Create 状态码 = %d：%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("x-amz-checksum-algorithm"); got != "CRC64NVME" {
		t.Errorf("回传算法 = %q，期望 CRC64NVME", got)
	}
	if got := rec.Header().Get("x-amz-checksum-type"); got != "FULL_OBJECT" {
		t.Errorf("回传类型 = %q，期望 FULL_OBJECT", got)
	}
	var initRes initiateMultipartUploadResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &initRes); err != nil {
		t.Fatalf("解析 Create 响应失败: %v", err)
	}
	if initRes.Bucket != "bkt" || initRes.Key != key {
		t.Errorf("Create 响应 = %+v", initRes)
	}
	uploadID := initRes.UploadID

	// 第一片：5 MiB（非末片下限），客户端按 S3 的隐式规则只带值不带算法头。
	p1 := mpuPayload(5<<20, 0x11)
	p2 := mpuPayload(1024, 0x22)
	sum1 := sumOfData(t, checksum.AlgCRC64NVME, p1)

	rec = mpuRequest(t, h, http.MethodPut, "/bkt/"+key+"?partNumber=1&uploadId="+uploadID,
		bytes.NewReader(p1), func(r *http.Request) {
			r.Header.Set("x-amz-checksum-crc64nvme", sum1)
		})
	if rec.Code != http.StatusOK {
		t.Fatalf("UploadPart(1) = %d：%s", rec.Code, rec.Body.String())
	}
	etag1 := rec.Header().Get("ETag")
	if !strings.HasPrefix(etag1, `"`) || !strings.HasSuffix(etag1, `"`) {
		t.Errorf("ETag 未加引号: %q", etag1)
	}
	if got := rec.Header().Get("x-amz-checksum-crc64nvme"); got != sum1 {
		t.Errorf("分片校验和 = %q，期望 %q", got, sum1)
	}

	// 第二片：末片可以小于 5 MiB；不带校验和头，服务端按会话算法计算。
	rec = mpuRequest(t, h, http.MethodPut, "/bkt/"+key+"?partNumber=2&uploadId="+uploadID,
		bytes.NewReader(p2), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("UploadPart(2) = %d：%s", rec.Code, rec.Body.String())
	}
	etag2 := rec.Header().Get("ETag")
	if got := rec.Header().Get("x-amz-checksum-crc64nvme"); got == "" {
		t.Error("会话有校验和算法时，UploadPart 必须回传分片校验和")
	}

	// ListParts：分片按编号升序，且带上算法与类型。
	rec = mpuRequest(t, h, http.MethodGet, "/bkt/"+key+"?uploadId="+uploadID, nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("ListParts = %d：%s", rec.Code, rec.Body.String())
	}
	var lp listPartsResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &lp); err != nil {
		t.Fatalf("解析 ListParts 响应失败: %v（%s）", err, rec.Body.String())
	}
	if len(lp.Parts) != 2 {
		t.Fatalf("分片数 = %d，期望 2：%s", len(lp.Parts), rec.Body.String())
	}
	if lp.Parts[0].ETag != etag1 || lp.Parts[0].Size != int64(len(p1)) {
		t.Errorf("第 1 片 = %+v", lp.Parts[0])
	}
	if lp.ChecksumAlgorithm != "CRC64NVME" || lp.ChecksumType != "FULL_OBJECT" {
		t.Errorf("ListParts 算法/类型 = %q/%q", lp.ChecksumAlgorithm, lp.ChecksumType)
	}
	// 校验和元素名随算法变化（ChecksumCRC64NVME），无法映射到固定结构体字段，
	// 因此直接检查响应体：元素名和值都必须对。
	if body := rec.Body.String(); !strings.Contains(body, "<ChecksumCRC64NVME>"+sum1+"</ChecksumCRC64NVME>") {
		t.Errorf("第 1 片缺少正确的 ChecksumCRC64NVME: %s", body)
	}

	// Complete：按 aws-cli 的形状提交（ETag 原样带回引号）。
	completeXML := fmt.Sprintf(
		`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part>`+
			`<Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`,
		etag1, etag2)
	rec = mpuRequest(t, h, http.MethodPost, "/bkt/"+key+"?uploadId="+uploadID,
		strings.NewReader(completeXML), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("Complete = %d：%s", rec.Code, rec.Body.String())
	}
	var cr completeMultipartUploadResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &cr); err != nil {
		t.Fatalf("解析 Complete 响应失败: %v（%s）", err, rec.Body.String())
	}
	if !strings.HasSuffix(cr.ETag, `-2"`) {
		t.Errorf("multipart ETag = %q，期望以 -2\" 结尾", cr.ETag)
	}
	if cr.Bucket != "bkt" || cr.Key != key || cr.Location == "" {
		t.Errorf("Complete 响应 = %+v", cr)
	}
	want := append(append([]byte{}, p1...), p2...)
	wantSum := sumOfData(t, checksum.AlgCRC64NVME, want)
	if body := rec.Body.String(); !strings.Contains(body, "<ChecksumCRC64NVME>"+wantSum+"</ChecksumCRC64NVME>") {
		t.Errorf("Complete 响应缺少正确的全对象校验和: %s", body)
	}
	if cr.ChecksumType != "FULL_OBJECT" {
		t.Errorf("Complete ChecksumType = %q", cr.ChecksumType)
	}
	if got := rec.Header().Get("x-amz-checksum-type"); got != "FULL_OBJECT" {
		t.Errorf("Complete 响应头 checksum-type = %q", got)
	}

	// 读回：内容逐字节一致，ChecksumMode 回传同一个全对象校验和。
	rec = mpuRequest(t, h, http.MethodGet, "/bkt/"+key, nil, func(r *http.Request) {
		r.Header.Set("x-amz-checksum-mode", "ENABLED")
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("GET = %d：%s", rec.Code, rec.Body.String())
	}
	if !bytes.Equal(rec.Body.Bytes(), want) {
		t.Error("读回内容与上传内容不一致")
	}
	if got := rec.Header().Get("x-amz-checksum-crc64nvme"); got != wantSum {
		t.Errorf("GET 校验和 = %q，期望 %q", got, wantSum)
	}
	if got := rec.Header().Get("x-amz-checksum-type"); got != "FULL_OBJECT" {
		t.Errorf("GET checksum-type = %q", got)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Errorf("Content-Type = %q", got)
	}
	// CompletMultipartUpload 携带的 x-amz-meta-* 必须延续到对象上，
	// 且与 GET 一致用全小写头名（aws-cli 按原始大小写匹配前缀）。
	if got := rec.Header()["x-amz-meta-owner"]; len(got) != 1 || got[0] != "edge-01" {
		t.Errorf("用户元数据 = %q", got)
	}

	// 完成后会话即消失：ListParts 与 Abort 都必须是 NoSuchUpload。
	rec = mpuRequest(t, h, http.MethodGet, "/bkt/"+key+"?uploadId="+uploadID, nil, nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("Complete 后 ListParts = %d，期望 404", rec.Code)
	}
	rec = mpuRequest(t, h, http.MethodDelete, "/bkt/"+key+"?uploadId="+uploadID, nil, nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("Complete 后 Abort = %d，期望 404", rec.Code)
	}
}

// TestMultipartRoutingAndErrors 覆盖子资源分派的三条非法组合与常见错误。
func TestMultipartRoutingAndErrors(t *testing.T) {
	h := mpuTestServer(t)
	uploadID := createUploadHTTP(t, h, "obj", nil)
	missing := "0123456789abcdef0123456789abcdef"

	cases := []struct {
		name     string
		method   string
		target   string
		body     io.Reader
		mutate   func(*http.Request)
		want     int
		wantCode string
	}{
		{
			name: "桶级 PUT ?uploads", method: http.MethodPut, target: "/bkt?uploads",
			want: http.StatusNotImplemented, wantCode: "NotImplemented",
		},
		{
			name: "带 uploadId 的 PUT 缺 partNumber", method: http.MethodPut,
			target: "/bkt/obj?uploadId=" + uploadID, body: strings.NewReader("x"),
			want: http.StatusBadRequest, wantCode: "InvalidArgument",
		},
		{
			name: "HEAD ?uploadId", method: http.MethodHead, target: "/bkt/obj?uploadId=" + uploadID,
			want: http.StatusNotImplemented,
		},
		{
			name: "单独 partNumber 下载", method: http.MethodGet, target: "/bkt/obj?partNumber=1",
			want: http.StatusNotImplemented, wantCode: "NotImplemented",
		},
		{
			name: "partNumber 为 0", method: http.MethodPut,
			target: "/bkt/obj?partNumber=0&uploadId=" + uploadID, body: strings.NewReader("x"),
			want: http.StatusBadRequest, wantCode: "InvalidArgument",
		},
		{
			name: "partNumber 超上限", method: http.MethodPut,
			target: "/bkt/obj?partNumber=10001&uploadId=" + uploadID, body: strings.NewReader("x"),
			want: http.StatusBadRequest, wantCode: "InvalidArgument",
		},
		{
			name: "partNumber 非数字", method: http.MethodPut,
			target: "/bkt/obj?partNumber=abc&uploadId=" + uploadID, body: strings.NewReader("x"),
			want: http.StatusBadRequest, wantCode: "InvalidArgument",
		},
		{
			name: "缺 Content-Length", method: http.MethodPut,
			target: "/bkt/obj?partNumber=1&uploadId=" + uploadID, body: strings.NewReader("x"),
			mutate: func(r *http.Request) { r.ContentLength = -1 },
			want:   http.StatusLengthRequired, wantCode: "LengthRequired",
		},
		{
			name: "uploadId 不存在", method: http.MethodPut,
			target: "/bkt/obj?partNumber=1&uploadId=" + missing, body: strings.NewReader("x"),
			want: http.StatusNotFound, wantCode: "NoSuchUpload",
		},
		{
			// 非法格式必须在触碰文件系统之前被拒，且表现与"不存在"一致。
			name: "uploadId 含路径穿越", method: http.MethodPut,
			target: "/bkt/obj?partNumber=1&uploadId=../../etc/passwd", body: strings.NewReader("x"),
			want: http.StatusNotFound, wantCode: "NoSuchUpload",
		},
		{
			name: "Abort 不存在的会话", method: http.MethodDelete,
			target: "/bkt/obj?uploadId=" + missing,
			want:   http.StatusNotFound, wantCode: "NoSuchUpload",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := mpuRequest(t, h, tc.method, tc.target, tc.body, tc.mutate)
			if rec.Code != tc.want {
				t.Fatalf("状态码 = %d，期望 %d：%s", rec.Code, tc.want, rec.Body.String())
			}
			if tc.wantCode != "" && tc.method != http.MethodHead {
				if !strings.Contains(rec.Body.String(), "<Code>"+tc.wantCode+"</Code>") {
					t.Errorf("响应缺少错误码 %s：%s", tc.wantCode, rec.Body.String())
				}
			}
		})
	}
}

// TestCompleteMultipartMalformed 覆盖 Complete 请求体解析失败的各条路径。
func TestCompleteMultipartMalformed(t *testing.T) {
	h := mpuTestServer(t)
	uploadID := createUploadHTTP(t, h, "bad", nil)

	cases := []struct {
		name string
		body io.Reader
	}{
		{"空请求体", nil},
		{"非 XML", strings.NewReader("not xml")},
		{"空的 Part 列表", strings.NewReader("<CompleteMultipartUpload></CompleteMultipartUpload>")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := mpuRequest(t, h, http.MethodPost, "/bkt/bad?uploadId="+uploadID, tc.body, nil)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("状态码 = %d，期望 400：%s", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "<Code>MalformedXML</Code>") {
				t.Errorf("错误码不是 MalformedXML：%s", rec.Body.String())
			}
		})
	}

	// 解析失败不得销毁会话：随后的合法 Abort 必须成功。
	rec := mpuRequest(t, h, http.MethodDelete, "/bkt/bad?uploadId="+uploadID, nil, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("Abort = %d，期望 204：%s", rec.Code, rec.Body.String())
	}
	if rec.Body.Len() != 0 {
		t.Errorf("Abort 响应体应为空，实际 %q", rec.Body.String())
	}
}

// TestListMultipartUploadsHTTP 覆盖桶级会话列表的分页、折叠与编码。
func TestListMultipartUploadsHTTP(t *testing.T) {
	h := mpuTestServer(t)

	ids := map[string]string{}
	for _, key := range []string{"dir/a", "dir/b", "top.txt"} {
		ids[key] = createUploadHTTP(t, h, key, nil)
	}

	var all listMultipartUploadsResult
	rec := mpuRequest(t, h, http.MethodGet, "/bkt?uploads", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("ListMultipartUploads = %d：%s", rec.Code, rec.Body.String())
	}
	if err := xml.Unmarshal(rec.Body.Bytes(), &all); err != nil {
		t.Fatalf("解析响应失败: %v（%s）", err, rec.Body.String())
	}
	if len(all.Uploads) != 3 {
		t.Fatalf("会话数 = %d，期望 3：%s", len(all.Uploads), rec.Body.String())
	}
	// 按 key 字节序排列。
	if all.Uploads[0].Key != "dir/a" || all.Uploads[1].Key != "dir/b" || all.Uploads[2].Key != "top.txt" {
		t.Errorf("排序错误: %+v", all.Uploads)
	}
	if all.Uploads[0].UploadID != ids["dir/a"] {
		t.Errorf("dir/a 的 UploadId = %q", all.Uploads[0].UploadID)
	}

	// delimiter 折叠：dir/ 变成 CommonPrefix，只剩顶层会话。
	var folded listMultipartUploadsResult
	rec = mpuRequest(t, h, http.MethodGet, "/bkt?uploads&delimiter=/", nil, nil)
	if err := xml.Unmarshal(rec.Body.Bytes(), &folded); err != nil {
		t.Fatalf("解析响应失败: %v", err)
	}
	if len(folded.Uploads) != 1 || folded.Uploads[0].Key != "top.txt" {
		t.Errorf("折叠后会话 = %+v", folded.Uploads)
	}
	if len(folded.CommonPrefixes) != 1 || folded.CommonPrefixes[0].Prefix != "dir/" {
		t.Errorf("CommonPrefixes = %+v", folded.CommonPrefixes)
	}

	// 分页：第一页 1 条并给出续页 marker，第二页从它之后继续。
	var page1 listMultipartUploadsResult
	rec = mpuRequest(t, h, http.MethodGet, "/bkt?uploads&max-uploads=1", nil, nil)
	if err := xml.Unmarshal(rec.Body.Bytes(), &page1); err != nil {
		t.Fatalf("解析响应失败: %v", err)
	}
	if !page1.IsTruncated || len(page1.Uploads) != 1 || page1.Uploads[0].Key != "dir/a" {
		t.Fatalf("第一页 = %+v", page1)
	}
	if page1.NextKeyMarker != "dir/a" || page1.NextUploadID != ids["dir/a"] {
		t.Errorf("续页 marker = %q/%q", page1.NextKeyMarker, page1.NextUploadID)
	}

	var page2 listMultipartUploadsResult
	target := fmt.Sprintf("/bkt?uploads&max-uploads=1&key-marker=%s&upload-id-marker=%s",
		page1.NextKeyMarker, page1.NextUploadID)
	rec = mpuRequest(t, h, http.MethodGet, target, nil, nil)
	if err := xml.Unmarshal(rec.Body.Bytes(), &page2); err != nil {
		t.Fatalf("解析响应失败: %v", err)
	}
	if len(page2.Uploads) != 1 || page2.Uploads[0].Key != "dir/b" {
		t.Errorf("第二页 = %+v", page2.Uploads)
	}

	// encoding-type=url：key 按 S3 规则编码，客户端才能安全解析。
	const spaced = "a b/c"
	// 路径里的空格必须写成 %20：httptest.NewRequest 拒绝含裸空格的 URL。
	createUploadHTTP(t, h, "a%20b/c", nil)
	var encoded listMultipartUploadsResult
	rec = mpuRequest(t, h, http.MethodGet, "/bkt?uploads&prefix="+s3URLEncode(spaced)+"&encoding-type=url", nil, nil)
	if err := xml.Unmarshal(rec.Body.Bytes(), &encoded); err != nil {
		t.Fatalf("解析响应失败: %v", err)
	}
	if len(encoded.Uploads) != 1 || encoded.Uploads[0].Key != s3URLEncode(spaced) {
		t.Errorf("编码后的 key = %+v", encoded.Uploads)
	}
	if encoded.EncodingType != "url" {
		t.Errorf("EncodingType = %q", encoded.EncodingType)
	}
}

// TestUploadPartAWSChunkedTrailer 覆盖 HTTPS 下 aws-cli 的真实上传形态：
// 分片请求体是 aws-chunked 框架，CRC 校验和只出现在请求体末尾的 trailer 里。
// 解帧、trailer 取值的路径与 PutObject 共用，但 UploadPart 多一层
// LateChecksum —— 回归点必须落在分片接口上。
func TestUploadPartAWSChunkedTrailer(t *testing.T) {
	h := mpuTestServer(t)
	const key = "chunked/part.bin"
	uploadID := createUploadHTTP(t, h, key, func(r *http.Request) {
		r.Header.Set("x-amz-checksum-algorithm", "CRC32")
	})

	data := []byte("chunked upload part payload")
	sum := sumOfData(t, checksum.AlgCRC32, data)
	chunkedBody := func(trailerChecksum string) string {
		return fmt.Sprintf("%x\r\n%s\r\n0\r\nx-amz-checksum-crc32:%s\r\n\r\n",
			len(data), data, trailerChecksum)
	}
	target := "/bkt/" + key + "?partNumber=1&uploadId=" + uploadID

	rec := mpuRequest(t, h, http.MethodPut, target, strings.NewReader(chunkedBody(sum)), func(r *http.Request) {
		r.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
		r.Header.Set("Content-Encoding", "aws-chunked")
		r.Header.Set("x-amz-decoded-content-length", fmt.Sprint(len(data)))
		r.Header.Set("x-amz-sdk-checksum-algorithm", "CRC32")
		r.Header.Set("x-amz-trailer", "x-amz-checksum-crc32")
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("UploadPart(aws-chunked) = %d：%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("x-amz-checksum-crc32"); got != sum {
		t.Errorf("分片校验和 = %q，期望 %q", got, sum)
	}

	// trailer 值错误：内容被正确解帧后才与声明比对，必须报 BadDigest。
	rec = mpuRequest(t, h, http.MethodPut, target, strings.NewReader(chunkedBody("AAAAAA==")), func(r *http.Request) {
		r.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
		r.Header.Set("Content-Encoding", "aws-chunked")
		r.Header.Set("x-amz-decoded-content-length", fmt.Sprint(len(data)))
		r.Header.Set("x-amz-sdk-checksum-algorithm", "CRC32")
		r.Header.Set("x-amz-trailer", "x-amz-checksum-crc32")
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("错误 trailer 的状态码 = %d，期望 400", rec.Code)
	}
	if got := decodeError(t, rec).Code; got != "BadDigest" {
		t.Errorf("错误码 = %q，期望 BadDigest", got)
	}
}
