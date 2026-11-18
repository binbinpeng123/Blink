package s3api

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/binbinpeng123/Blink/internal/checksum"
)

// getAttributes 发起 GetObjectAttributes 请求；attributes 为空时不设置请求头。
func getAttributes(t *testing.T, h http.Handler, target, attributes string) *httptest.ResponseRecorder {
	t.Helper()
	return objectRequest(t, h, http.MethodGet, target, func(r *http.Request) {
		if attributes != "" {
			r.Header.Set("x-amz-object-attributes", attributes)
		}
	})
}

func decodeAttributes(t *testing.T, rec *httptest.ResponseRecorder) getObjectAttributesResponse {
	t.Helper()
	var out getObjectAttributesResponse
	if err := xml.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("解析 GetObjectAttributes 响应失败: %v（%s）", err, rec.Body.String())
	}
	return out
}

// TestGetObjectAttributesSinglePut 覆盖普通 PUT 对象的属性投影：
// 只返回请求的属性，未请求的属性不出现。
func TestGetObjectAttributesSinglePut(t *testing.T) {
	h := objectTestServer(t)
	const body = "hello world"
	put := putObject(t, h, "/bkt/greeting.txt", body, nil)
	if put.Code != http.StatusOK {
		t.Fatalf("写入失败: %d (%s)", put.Code, put.Body.String())
	}

	rec := getAttributes(t, h, "/bkt/greeting.txt?attributes", " ETag , ObjectSize,StorageClass ")
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Last-Modified"); got == "" {
		t.Error("缺少 Last-Modified 响应头")
	}

	out := decodeAttributes(t, rec)
	if out.XMLName.Local != "GetObjectAttributesResponse" ||
		out.XMLName.Space != "http://s3.amazonaws.com/doc/2006-03-01/" {
		t.Errorf("根元素 = %s %s", out.XMLName.Space, out.XMLName.Local)
	}
	// ETag 不带引号：与 GET/HEAD 的响应头相反。
	wantETag := strings.Trim(put.Header().Get("ETag"), `"`)
	if out.ETag == nil || *out.ETag != wantETag {
		t.Errorf("ETag = %v，期望 %q", out.ETag, wantETag)
	}
	if out.ObjectSize == nil || *out.ObjectSize != int64(len(body)) {
		t.Errorf("ObjectSize = %v，期望 %d", out.ObjectSize, len(body))
	}
	if out.StorageClass == nil || *out.StorageClass != storageClassStandard {
		t.Errorf("StorageClass = %v，期望 %q", out.StorageClass, storageClassStandard)
	}
	if out.Checksum != nil {
		t.Errorf("未请求 Checksum 却返回了: %+v", out.Checksum)
	}
	if out.ObjectParts != nil {
		t.Errorf("未请求 ObjectParts 却返回了: %+v", out.ObjectParts)
	}
	raw := rec.Body.String()
	if strings.Contains(raw, "<Checksum>") || strings.Contains(raw, "<ObjectParts>") {
		t.Errorf("响应包含未请求的属性: %s", raw)
	}
}

// TestGetObjectAttributesNoChecksum 固定"对象无校验和时整个 Checksum
// 元素省略"的行为：返回空元素会让客户端以为拿到了一个空校验值。
func TestGetObjectAttributesNoChecksum(t *testing.T) {
	h := objectTestServer(t)
	if rec := putObject(t, h, "/bkt/plain.bin", "data", nil); rec.Code != http.StatusOK {
		t.Fatalf("写入失败: %d", rec.Code)
	}

	rec := getAttributes(t, h, "/bkt/plain.bin?attributes", "Checksum")
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d (%s)", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "<Checksum>") {
		t.Errorf("无校验和对象不应返回 Checksum 元素: %s", rec.Body.String())
	}
}

// TestGetObjectAttributesObjectPartsEmpty 固定非分片对象的 ObjectParts：
// 元素存在但分片集合为空、计数为 0，而不是省略元素。
func TestGetObjectAttributesObjectPartsEmpty(t *testing.T) {
	h := objectTestServer(t)
	if rec := putObject(t, h, "/bkt/plain.bin", "data", nil); rec.Code != http.StatusOK {
		t.Fatalf("写入失败: %d", rec.Code)
	}

	rec := getAttributes(t, h, "/bkt/plain.bin?attributes", "ObjectParts")
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d (%s)", rec.Code, rec.Body.String())
	}
	out := decodeAttributes(t, rec)
	if out.ObjectParts == nil {
		t.Fatalf("缺少 ObjectParts: %s", rec.Body.String())
	}
	if out.ObjectParts.TotalPartsCount != 0 || out.ObjectParts.PartsCount != 0 ||
		len(out.ObjectParts.Parts) != 0 || out.ObjectParts.IsTruncated {
		t.Errorf("非分片对象 ObjectParts = %+v", out.ObjectParts)
	}
}

// TestGetObjectAttributesRequestErrors 覆盖请求头与参数校验。
func TestGetObjectAttributesRequestErrors(t *testing.T) {
	h := objectTestServer(t)
	if rec := putObject(t, h, "/bkt/obj", "data", nil); rec.Code != http.StatusOK {
		t.Fatalf("写入失败: %d", rec.Code)
	}

	cases := []struct {
		name       string
		target     string
		attributes string
		want       int
	}{
		{"缺少请求头", "/bkt/obj?attributes", "", http.StatusBadRequest},
		{"空请求头", "/bkt/obj?attributes", " ", http.StatusBadRequest},
		{"未知属性", "/bkt/obj?attributes", "ETag,Magic", http.StatusBadRequest},
		{"空项", "/bkt/obj?attributes", "ETag,,ObjectSize", http.StatusBadRequest},
		{"max-parts 非整数", "/bkt/obj?attributes&max-parts=x", "ObjectParts", http.StatusBadRequest},
		{"max-parts 负数", "/bkt/obj?attributes&max-parts=-1", "ObjectParts", http.StatusBadRequest},
		{"marker 负数", "/bkt/obj?attributes&part-number-marker=-1", "ObjectParts", http.StatusBadRequest},
		{"非对象路径", "/bkt?attributes", "ETag", http.StatusNotImplemented},
		{"带 versionId", "/bkt/obj?attributes&versionId=v1", "ETag", http.StatusNotImplemented},
		{"带 uploadId", "/bkt/obj?attributes&uploadId=u1", "ETag", http.StatusNotImplemented},
		{"带 partNumber", "/bkt/obj?attributes&partNumber=1", "ETag", http.StatusNotImplemented},
		{"对象不存在", "/bkt/missing?attributes", "ETag", http.StatusNotFound},
		{"桶不存在", "/missing/obj?attributes", "ETag", http.StatusNotFound},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := getAttributes(t, h, c.target, c.attributes)
			if rec.Code != c.want {
				t.Fatalf("状态码 = %d，期望 %d（%s）", rec.Code, c.want, rec.Body.String())
			}
		})
	}

	// 非 GET 必须是 405，且不能把请求体当成一次普通上传写进对象。
	for _, method := range []string{http.MethodPut, http.MethodPost, http.MethodDelete, http.MethodHead} {
		rec := objectRequest(t, h, method, "/bkt/obj?attributes", func(r *http.Request) {
			r.Header.Set("x-amz-object-attributes", "ETag")
		})
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s ?attributes = %d，期望 405", method, rec.Code)
		}
	}
}

// TestGetObjectAttributesMultipartParts 覆盖分片对象的对象级与分片级
// 校验和、分页与 NextPartNumberMarker。
func TestGetObjectAttributesMultipartParts(t *testing.T) {
	h := mpuTestServer(t)
	const key = "models/part.bin"

	uploadID := createUploadHTTP(t, h, key, func(r *http.Request) {
		r.Header.Set("x-amz-checksum-algorithm", "CRC64NVME")
	})
	p1 := mpuPayload(5<<20, 0x11)
	p2 := mpuPayload(1024, 0x22)
	sum1 := sumOfData(t, checksum.AlgCRC64NVME, p1)

	rec := mpuRequest(t, h, http.MethodPut, "/bkt/"+key+"?partNumber=1&uploadId="+uploadID,
		bytes.NewReader(p1), func(r *http.Request) {
			r.Header.Set("x-amz-checksum-crc64nvme", sum1)
		})
	if rec.Code != http.StatusOK {
		t.Fatalf("UploadPart(1) = %d：%s", rec.Code, rec.Body.String())
	}
	etag1 := rec.Header().Get("ETag")

	rec = mpuRequest(t, h, http.MethodPut, "/bkt/"+key+"?partNumber=2&uploadId="+uploadID,
		bytes.NewReader(p2), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("UploadPart(2) = %d：%s", rec.Code, rec.Body.String())
	}
	etag2 := rec.Header().Get("ETag")

	completeXML := fmt.Sprintf(
		`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part>`+
			`<Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`,
		etag1, etag2)
	rec = mpuRequest(t, h, http.MethodPost, "/bkt/"+key+"?uploadId="+uploadID,
		strings.NewReader(completeXML), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("Complete = %d：%s", rec.Code, rec.Body.String())
	}

	wantSum := sumOfData(t, checksum.AlgCRC64NVME, append(append([]byte{}, p1...), p2...))
	rec = getAttributes(t, h, "/bkt/"+key+"?attributes&max-parts=1",
		"ETag,Checksum,ObjectParts,ObjectSize")
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d (%s)", rec.Code, rec.Body.String())
	}
	out := decodeAttributes(t, rec)
	if out.ETag == nil || !strings.HasSuffix(*out.ETag, "-2") || strings.HasPrefix(*out.ETag, `"`) {
		t.Errorf("ETag = %v，期望不带引号的 -2 分片 ETag", out.ETag)
	}
	if out.ObjectSize == nil || *out.ObjectSize != int64(len(p1)+len(p2)) {
		t.Errorf("ObjectSize = %v", out.ObjectSize)
	}
	if out.Checksum == nil || out.Checksum.ChecksumCRC64NVME == nil ||
		*out.Checksum.ChecksumCRC64NVME != wantSum {
		t.Errorf("对象级校验和 = %+v，期望 %q", out.Checksum, wantSum)
	}
	if out.Checksum == nil || out.Checksum.ChecksumType != "FULL_OBJECT" {
		t.Errorf("ChecksumType = %+v", out.Checksum)
	}

	parts := out.ObjectParts
	if parts == nil {
		t.Fatalf("缺少 ObjectParts: %s", rec.Body.String())
	}
	if !parts.IsTruncated || parts.MaxParts != 1 || parts.PartNumberMarker != 0 ||
		parts.PartsCount != 1 || parts.TotalPartsCount != 2 {
		t.Errorf("第一页 ObjectParts = %+v", parts)
	}
	if parts.NextPartNumberMarker == nil || *parts.NextPartNumberMarker != 1 {
		t.Errorf("NextPartNumberMarker = %v，期望 1", parts.NextPartNumberMarker)
	}
	if len(parts.Parts) != 1 {
		t.Fatalf("第一页分片数 = %d", len(parts.Parts))
	}
	if p := parts.Parts[0]; p.PartNumber != 1 || p.Size != int64(len(p1)) ||
		p.ChecksumCRC64NVME == nil || *p.ChecksumCRC64NVME != sum1 {
		t.Errorf("第一页分片 = %+v，期望编号 1 大小 %d 校验和 %q", p, len(p1), sum1)
	}

	// 第二页：marker 之后只剩一个分片，不再截断。
	rec = getAttributes(t, h,
		"/bkt/"+key+"?attributes&max-parts=1&part-number-marker=1", "ObjectParts")
	if rec.Code != http.StatusOK {
		t.Fatalf("第二页状态码 = %d (%s)", rec.Code, rec.Body.String())
	}
	out = decodeAttributes(t, rec)
	parts = out.ObjectParts
	if parts == nil || parts.IsTruncated || parts.PartNumberMarker != 1 ||
		parts.PartsCount != 1 || parts.TotalPartsCount != 2 ||
		parts.NextPartNumberMarker != nil {
		t.Errorf("第二页 ObjectParts = %+v", parts)
	}
	if len(parts.Parts) != 1 || parts.Parts[0].PartNumber != 2 ||
		parts.Parts[0].Size != int64(len(p2)) {
		t.Errorf("第二页分片 = %+v", parts.Parts)
	}

	// 请求超过 1000 的 max-parts 按上限回显。
	rec = getAttributes(t, h, "/bkt/"+key+"?attributes&max-parts=5000", "ObjectParts")
	out = decodeAttributes(t, rec)
	if out.ObjectParts == nil || out.ObjectParts.MaxParts != maxMaxParts {
		t.Errorf("max-parts 上限未生效: %+v", out.ObjectParts)
	}
}
