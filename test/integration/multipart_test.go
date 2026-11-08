//go:build integration

package integration

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

// initiateResult 对应 CreateMultipartUpload 的响应。
type initiateResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

// completeResult 对应 CompleteMultipartUpload 的响应。
type completeResult struct {
	XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
	Bucket  string   `xml:"Bucket"`
	Key     string   `xml:"Key"`
	ETag    string   `xml:"ETag"`
}

// listPartsResult 只取断言关心的字段。
type listPartsResult struct {
	XMLName     xml.Name `xml:"ListPartsResult"`
	Bucket      string   `xml:"Bucket"`
	Key         string   `xml:"Key"`
	UploadID    string   `xml:"UploadId"`
	IsTruncated bool     `xml:"IsTruncated"`
	Parts       []struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
		Size       int64  `xml:"Size"`
	} `xml:"Part"`
}

// listUploadsResult 是 ListMultipartUploads 的响应。
type listUploadsResult struct {
	XMLName     xml.Name `xml:"ListMultipartUploadsResult"`
	Bucket      string   `xml:"Bucket"`
	IsTruncated bool     `xml:"IsTruncated"`
	Uploads     []struct {
		Key      string `xml:"Key"`
		UploadID string `xml:"UploadId"`
	} `xml:"Upload"`
}

type completePartReq struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completeReq struct {
	XMLName xml.Name          `xml:"CompleteMultipartUpload"`
	Parts   []completePartReq `xml:"Part"`
}

// initiateMPU 创建分片上传会话并返回 UploadId。
func initiateMPU(t *testing.T, c *s3Client, bucket, key string) string {
	t.Helper()
	res := c.do(http.MethodPost, c.objectURL(bucket, key),
		url.Values{"uploads": {""}}, nil, nil)
	requireStatus(t, res, http.StatusOK)
	got := mustXML[initiateResult](t, res)
	if got.Bucket != bucket || got.Key != key || got.UploadID == "" {
		t.Fatalf("CreateMultipartUpload 响应不完整: %+v", got)
	}
	return got.UploadID
}

// uploadPart 上传一个分片并返回带引号的 ETag。
func uploadPart(t *testing.T, c *s3Client, bucket, key, uploadID string, number int, body []byte) string {
	t.Helper()
	res := c.do(http.MethodPut, c.objectURL(bucket, key), url.Values{
		"partNumber": {strconv.Itoa(number)},
		"uploadId":   {uploadID},
	}, body, nil)
	requireStatus(t, res, http.StatusOK)
	etag := res.Header.Get("ETag")
	if etag == "" {
		t.Fatalf("UploadPart 缺少 ETag 响应头")
	}
	return etag
}

// completeMPU 提交分片列表。parts 的顺序由调用方决定，用于验证排序校验。
func completeMPU(t *testing.T, c *s3Client, bucket, key, uploadID string, parts []completePartReq) *httpResult {
	t.Helper()
	payload, err := xml.Marshal(completeReq{Parts: parts})
	if err != nil {
		t.Fatalf("序列化 Complete 请求失败: %v", err)
	}
	return c.do(http.MethodPost, c.objectURL(bucket, key),
		url.Values{"uploadId": {uploadID}}, payload,
		map[string]string{"Content-Type": "application/xml"})
}

// TestMultipartUpload 覆盖 MPU 主路径：会话、分片、列举、提交与分片下载。
func TestMultipartUpload(t *testing.T) {
	srv := startServer(t, serverOptions{})
	c := newClient(t, srv)

	bucket := uniqueBucket("mpu")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)

	const key = "videos/big.bin"
	part1 := bodyOf(5<<20, 11) // 非末片必须 ≥ 5 MiB
	part2 := bodyOf(1<<20, 12)

	uploadID := initiateMPU(t, c, bucket, key)

	t.Run("ListMultipartUploads 能看到进行中的会话", func(t *testing.T) {
		res := c.do(http.MethodGet, c.bucketURL(bucket),
			url.Values{"uploads": {""}}, nil, nil)
		requireStatus(t, res, http.StatusOK)
		got := mustXML[listUploadsResult](t, res)
		if got.Bucket != bucket {
			t.Fatalf("Bucket = %q", got.Bucket)
		}
		found := false
		for _, u := range got.Uploads {
			if u.Key == key && u.UploadID == uploadID {
				found = true
			}
		}
		if !found {
			t.Fatalf("ListMultipartUploads 未包含 %q/%q：%+v", key, uploadID, got.Uploads)
		}
	})

	etag1 := uploadPart(t, c, bucket, key, uploadID, 1, part1)
	etag2 := uploadPart(t, c, bucket, key, uploadID, 2, part2)

	t.Run("ListParts 返回已上传分片", func(t *testing.T) {
		res := c.do(http.MethodGet, c.objectURL(bucket, key),
			url.Values{"uploadId": {uploadID}}, nil, nil)
		requireStatus(t, res, http.StatusOK)
		got := mustXML[listPartsResult](t, res)
		if got.Key != key || got.UploadID != uploadID {
			t.Fatalf("ListParts 标识不一致: %+v", got)
		}
		if len(got.Parts) != 2 {
			t.Fatalf("分片数 = %d，期望 2", len(got.Parts))
		}
		for i, want := range []struct {
			number int
			size   int
			etag   string
		}{{1, len(part1), etag1}, {2, len(part2), etag2}} {
			p := got.Parts[i]
			if p.PartNumber != want.number || p.Size != int64(want.size) {
				t.Fatalf("分片 %d = {number:%d size:%d}，期望 %+v", i, p.PartNumber, p.Size, want)
			}
			if unquoteETag(p.ETag) != unquoteETag(want.etag) {
				t.Fatalf("分片 %d ETag = %q，期望 %q", i, p.ETag, want.etag)
			}
		}
	})

	var completeETag string
	t.Run("Complete 返回 -N 后缀 ETag", func(t *testing.T) {
		res := completeMPU(t, c, bucket, key, uploadID, []completePartReq{
			{PartNumber: 1, ETag: etag1},
			{PartNumber: 2, ETag: etag2},
		})
		requireStatus(t, res, http.StatusOK)

		headerETag := unquoteETag(res.Header.Get("ETag"))
		if headerETag == "" {
			t.Fatalf("Complete 响应缺少 ETag 头")
		}
		if !strings.HasSuffix(headerETag, "-2") {
			t.Fatalf("Complete ETag = %q，期望以 -2 结尾", headerETag)
		}
		body := mustXML[completeResult](t, res)
		if body.Bucket != bucket || body.Key != key {
			t.Fatalf("Complete 响应标识不一致: %+v", body)
		}
		if unquoteETag(body.ETag) != headerETag {
			t.Fatalf("XML ETag = %q，头部 ETag = %q", body.ETag, headerETag)
		}
		completeETag = headerETag
	})

	t.Run("GET 拼接后内容与分片一致", func(t *testing.T) {
		res := c.getObject(bucket, key, nil)
		requireStatus(t, res, http.StatusOK)
		want := append(append([]byte(nil), part1...), part2...)
		if string(res.Body) != string(want) {
			t.Fatalf("对象长度 = %d，期望 %d", len(res.Body), len(want))
		}
		if got := unquoteETag(res.Header.Get("ETag")); got != completeETag {
			t.Fatalf("GET ETag = %q，期望 %q", got, completeETag)
		}
		// S3 语义：x-amz-mp-parts-count 只在请求带 partNumber 时返回，
		// 完整 GET 不返回（见 partNumber 子测试）。
	})

	t.Run("partNumber 下载单个分片", func(t *testing.T) {
		res := c.do(http.MethodGet, c.objectURL(bucket, key),
			url.Values{"partNumber": {"1"}}, nil, nil)
		requireStatus(t, res, http.StatusPartialContent)
		if string(res.Body) != string(part1) {
			t.Fatalf("分片 1 内容不一致：长度 %d，期望 %d", len(res.Body), len(part1))
		}
		wantCR := "bytes 0-" + strconv.Itoa(len(part1)-1) + "/" + strconv.Itoa(len(part1)+len(part2))
		if got := res.Header.Get("Content-Range"); got != wantCR {
			t.Fatalf("Content-Range = %q，期望 %q", got, wantCR)
		}
		if got := res.Header.Get("x-amz-mp-parts-count"); got != "2" {
			t.Fatalf("x-amz-mp-parts-count = %q，期望 2", got)
		}
	})

	t.Run("不存在的分片编号返回 416", func(t *testing.T) {
		res := c.do(http.MethodGet, c.objectURL(bucket, key),
			url.Values{"partNumber": {"3"}}, nil, nil)
		requireS3Error(t, res, http.StatusRequestedRangeNotSatisfiable, "InvalidPartNumber")
	})
}

// TestMultipartErrors 覆盖 MPU 的错误语义与 Abort 后的会话失效。
func TestMultipartErrors(t *testing.T) {
	srv := startServer(t, serverOptions{})
	c := newClient(t, srv)

	bucket := uniqueBucket("mpuerr")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)

	t.Run("错误的 UploadId 返回 404", func(t *testing.T) {
		res := c.do(http.MethodGet, c.objectURL(bucket, "missing.bin"),
			url.Values{"uploadId": {"no-such-upload"}}, nil, nil)
		requireS3Error(t, res, http.StatusNotFound, "NoSuchUpload")

		// 空 Parts 会先被 HTTP 层判成 MalformedXML（S3 亦如此），
		// 因此这里必须带一个非空分片列表，才能走到 uploadId 校验。
		res = completeMPU(t, c, bucket, "missing.bin", "no-such-upload", []completePartReq{
			{PartNumber: 1, ETag: `"00000000000000000000000000000000"`},
		})
		requireS3Error(t, res, http.StatusNotFound, "NoSuchUpload")
	})

	t.Run("分片乱序返回 InvalidPartOrder", func(t *testing.T) {
		const key = "order.bin"
		uploadID := initiateMPU(t, c, bucket, key)
		// 校验按元素顺序执行：第 0 个元素（part 2）必须满足非末片尺寸，
		// 第 1 个元素（part 1）才会先撞上乱序校验，而不是 EntityTooSmall。
		etag1 := uploadPart(t, c, bucket, key, uploadID, 1, bodyOf(1<<20, 21))
		etag2 := uploadPart(t, c, bucket, key, uploadID, 2, bodyOf(5<<20, 22))

		res := completeMPU(t, c, bucket, key, uploadID, []completePartReq{
			{PartNumber: 2, ETag: etag2},
			{PartNumber: 1, ETag: etag1},
		})
		requireS3Error(t, res, http.StatusBadRequest, "InvalidPartOrder")

		// 顺序正确后暴露真实问题：首片（非末片）不足 5 MiB。
		res = completeMPU(t, c, bucket, key, uploadID, []completePartReq{
			{PartNumber: 1, ETag: etag1},
			{PartNumber: 2, ETag: etag2},
		})
		requireS3Error(t, res, http.StatusBadRequest, "EntityTooSmall")

		res = c.do(http.MethodDelete, c.objectURL(bucket, key),
			url.Values{"uploadId": {uploadID}}, nil, nil)
		requireStatus(t, res, http.StatusNoContent)
	})

	t.Run("引用未上传的分片返回 InvalidPart", func(t *testing.T) {
		const key = "invalid-part.bin"
		uploadID := initiateMPU(t, c, bucket, key)
		etag1 := uploadPart(t, c, bucket, key, uploadID, 1, bodyOf(5<<20, 23))

		// 分片 2 从未上传。
		res := completeMPU(t, c, bucket, key, uploadID, []completePartReq{
			{PartNumber: 1, ETag: etag1},
			{PartNumber: 2, ETag: etag1},
		})
		requireS3Error(t, res, http.StatusBadRequest, "InvalidPart")

		// ETag 与已上传分片不符同样是 InvalidPart。
		res = completeMPU(t, c, bucket, key, uploadID, []completePartReq{
			{PartNumber: 1, ETag: `"00000000000000000000000000000000"`},
		})
		requireS3Error(t, res, http.StatusBadRequest, "InvalidPart")

		requireStatus(t, c.do(http.MethodDelete, c.objectURL(bucket, key),
			url.Values{"uploadId": {uploadID}}, nil, nil), http.StatusNoContent)
	})

	t.Run("空分片列表返回 MalformedXML", func(t *testing.T) {
		const key = "empty-parts.bin"
		uploadID := initiateMPU(t, c, bucket, key)
		res := completeMPU(t, c, bucket, key, uploadID, nil)
		requireS3Error(t, res, http.StatusBadRequest, "MalformedXML")

		requireStatus(t, c.do(http.MethodDelete, c.objectURL(bucket, key),
			url.Values{"uploadId": {uploadID}}, nil, nil), http.StatusNoContent)
	})

	t.Run("Abort 后会话失效", func(t *testing.T) {
		const key = "aborted.bin"
		uploadID := initiateMPU(t, c, bucket, key)
		uploadPart(t, c, bucket, key, uploadID, 1, bodyOf(1024, 24))

		res := c.do(http.MethodDelete, c.objectURL(bucket, key),
			url.Values{"uploadId": {uploadID}}, nil, nil)
		requireStatus(t, res, http.StatusNoContent)

		res = c.do(http.MethodGet, c.objectURL(bucket, key),
			url.Values{"uploadId": {uploadID}}, nil, nil)
		requireS3Error(t, res, http.StatusNotFound, "NoSuchUpload")

		res = c.do(http.MethodPut, c.objectURL(bucket, key), url.Values{
			"partNumber": {"2"},
			"uploadId":   {uploadID},
		}, bodyOf(1024, 25), nil)
		requireS3Error(t, res, http.StatusNotFound, "NoSuchUpload")

		res = completeMPU(t, c, bucket, key, uploadID, []completePartReq{
			{PartNumber: 1, ETag: `"00000000000000000000000000000000"`},
		})
		requireS3Error(t, res, http.StatusNotFound, "NoSuchUpload")

		// 会话被放弃，对象不应出现。
		res = c.getObject(bucket, key, nil)
		requireS3Error(t, res, http.StatusNotFound, "NoSuchKey")
	})

	t.Run("partNumber 非法返回 400", func(t *testing.T) {
		const key = "bad-part-number.bin"
		uploadID := initiateMPU(t, c, bucket, key)

		for _, raw := range []string{"0", "abc", "10001"} {
			res := c.do(http.MethodPut, c.objectURL(bucket, key), url.Values{
				"partNumber": {raw},
				"uploadId":   {uploadID},
			}, []byte("x"), nil)
			requireS3Error(t, res, http.StatusBadRequest, "InvalidArgument")
		}

		requireStatus(t, c.do(http.MethodDelete, c.objectURL(bucket, key),
			url.Values{"uploadId": {uploadID}}, nil, nil), http.StatusNoContent)
	})
}
