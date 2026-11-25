package s3api

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/binbinpeng123/Blink/internal/s3err"
)

// headerCase 是「请求头 + 代表值」的测试样本。
type headerCase struct {
	name   string
	header string
	value  string
}

// unsupportedWriteHeaderCases 是「能力组 × 请求头」的代表样本：
// ACL、标签、Object Lock、SSE、网站重定向各至少一个。
//
// 用样本而不是穷举列表，是为了让测试挂在契约（表里的每一组）而不是
// 实现细节（列表里的每一个字面量）上；列表本身由
// TestUnsupportedHeaderLists 守住小写与去重。
var unsupportedWriteHeaderCases = []headerCase{
	{"canned ACL", "x-amz-acl", "public-read"},
	{"显式 grant", "x-amz-grant-read", `uri="http://acs.amazonaws.com/groups/global/AllUsers"`},
	{"标签", "x-amz-tagging", "env=prod"},
	{"Object Lock", "x-amz-object-lock-mode", "GOVERNANCE"},
	{"SSE-S3", "x-amz-server-side-encryption", "AES256"},
	{"SSE-KMS", "x-amz-server-side-encryption-aws-kms-key-id", "kms-key"},
	{"SSE-C", "x-amz-server-side-encryption-customer-algorithm", "AES256"},
	{"网站重定向", "x-amz-website-redirect-location", "/edge"},
}

// unsupportedSSECases 是分片级请求（UploadPart、Complete）的反例集合：
// 这些请求上只有 SSE 头有定义。
var unsupportedSSECases = []headerCase{
	{"SSE-S3", "x-amz-server-side-encryption", "AES256"},
	{"SSE-KMS", "x-amz-server-side-encryption-aws-kms-key-id", "kms-key"},
	{"SSE-C", "x-amz-server-side-encryption-customer-algorithm", "AES256"},
	{"SSE 桶密钥", "x-amz-server-side-encryption-bucket-key-enabled", "true"},
}

// requireNotImplemented 断言响应是 501 + NotImplemented。
func requireNotImplemented(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("状态码 = %d，期望 501：%s", rec.Code, rec.Body.String())
	}
	body := decodeError(t, rec)
	if body.Code != string(s3err.CodeNotImplemented) {
		t.Fatalf("错误码 = %s，期望 NotImplemented", body.Code)
	}
	// 消息必须指出是哪个头被拒：只说"未实现"无法让客户端定位。
	if !strings.Contains(body.Message, "暂不支持") {
		t.Errorf("消息 = %q，未说明被拒的能力", body.Message)
	}
}

// TestUnsupportedHeaderLists 守住三件事：头名全小写（消息里按字面量回显，
// 混用大小写会让客户端对不上）、组内无重复、复制组确实比对象组多了
// 标签指令与复制源 SSE-C。
func TestUnsupportedHeaderLists(t *testing.T) {
	lists := map[string][]string{
		"objectWrite": unsupportedObjectWriteHeaders,
		"bucketWrite": unsupportedBucketWriteHeaders,
		"part":        unsupportedPartHeaders,
		"copy":        unsupportedCopyHeaders,
	}
	for name, list := range lists {
		seen := make(map[string]bool, len(list))
		for _, h := range list {
			if h != strings.ToLower(h) {
				t.Errorf("%s 中的头名 %q 不是小写", name, h)
			}
			if seen[h] {
				t.Errorf("%s 中重复出现 %q", name, h)
			}
			seen[h] = true
		}
	}

	for _, h := range []string{
		"x-amz-tagging-directive",
		"x-amz-copy-source-server-side-encryption-customer-key",
	} {
		if !containsHeader(unsupportedCopyHeaders, h) {
			t.Errorf("复制组缺少 %q", h)
		}
	}
	// 标签指令只在复制上有定义，不能混进普通对象写入。
	if containsHeader(unsupportedObjectWriteHeaders, "x-amz-tagging-directive") {
		t.Error("对象写入组不应包含 x-amz-tagging-directive")
	}
	if !containsHeader(unsupportedBucketWriteHeaders, "x-amz-bucket-object-lock-enabled") {
		t.Error("建桶组缺少 x-amz-bucket-object-lock-enabled")
	}
}

func containsHeader(list []string, h string) bool {
	for _, v := range list {
		if v == h {
			return true
		}
	}
	return false
}

// TestPutObjectRejectsUnsupportedHeadersHTTP 验证每个能力组在落盘之前被拒，
// 且不留下对象：客户端看到 501 就应当知道什么都没写。
func TestPutObjectRejectsUnsupportedHeadersHTTP(t *testing.T) {
	h := objectTestServer(t)
	for i, c := range unsupportedWriteHeaderCases {
		t.Run(c.name, func(t *testing.T) {
			key := fmt.Sprintf("/bkt/rejected-%d.txt", i)
			rec := putObject(t, h, key, "hello", func(r *http.Request) {
				r.Header.Set(c.header, c.value)
			})
			requireNotImplemented(t, rec)
			if got := do(t, h, http.MethodGet, key); got.Code != http.StatusNotFound {
				t.Fatalf("被拒绝的请求留下了对象：GET = %d", got.Code)
			}
		})
	}
}

// TestCreateBucketRejectsUnsupportedHeadersHTTP 验证桶级能力（ACL、Object Lock
// 启用、对象所有权、默认加密）被拒时不会真的创建桶。
func TestCreateBucketRejectsUnsupportedHeadersHTTP(t *testing.T) {
	h := objectTestServer(t)
	cases := []struct {
		name   string
		header string
		value  string
	}{
		{"canned ACL", "x-amz-acl", "private"},
		{"Object Lock 启用", "x-amz-bucket-object-lock-enabled", "true"},
		{"对象所有权", "x-amz-object-ownership", "BucketOwnerEnforced"},
		{"默认加密", "x-amz-server-side-encryption", "AES256"},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bucket := fmt.Sprintf("rejected-bucket-%d", i)
			rec := mpuRequest(t, h, http.MethodPut, "/"+bucket, nil, func(r *http.Request) {
				r.Header.Set(c.header, c.value)
			})
			requireNotImplemented(t, rec)
			if got := do(t, h, http.MethodHead, "/"+bucket); got.Code != http.StatusNotFound {
				t.Fatalf("被拒绝的建桶留下了桶：HEAD = %d", got.Code)
			}
		})
	}
}

// TestCreateMultipartUploadRejectsUnsupportedHeadersHTTP 验证 Initiate 被拒时
// 不创建会话：客户端继续 UploadPart 应得到 NoSuchUpload，而不是一个
// 属性已被悄悄丢弃的会话。
func TestCreateMultipartUploadRejectsUnsupportedHeadersHTTP(t *testing.T) {
	h := mpuTestServer(t)
	for _, c := range unsupportedWriteHeaderCases {
		t.Run(c.name, func(t *testing.T) {
			rec := mpuRequest(t, h, http.MethodPost, "/bkt/rejected.bin?uploads", nil, func(r *http.Request) {
				r.Header.Set(c.header, c.value)
			})
			requireNotImplemented(t, rec)
		})
	}

	rec := mpuRequest(t, h, http.MethodGet, "/bkt?uploads", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("ListMultipartUploads = %d：%s", rec.Code, rec.Body.String())
	}
	var list listMultipartUploadsResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("列表响应不是合法 XML：%v (%s)", err, rec.Body.String())
	}
	if len(list.Uploads) != 0 {
		t.Fatalf("被拒绝的 Initiate 留下了 %d 个会话", len(list.Uploads))
	}
}

// TestUploadPartRejectsSSEHeadersHTTP 验证 SSE 头在分片上传上被拒，
// 且被拒的分片不写入、会话保持可用。
func TestUploadPartRejectsSSEHeadersHTTP(t *testing.T) {
	h := mpuTestServer(t)
	const key = "parts.bin"
	uploadID := createUploadHTTP(t, h, key, nil)

	// 先成功传一片，作为"会话本身是好的"的对照。
	rec := mpuRequest(t, h, http.MethodPut, "/bkt/"+key+"?partNumber=1&uploadId="+uploadID,
		strings.NewReader("hello"), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("准备分片失败：%d %s", rec.Code, rec.Body.String())
	}

	for _, c := range unsupportedSSECases {
		t.Run(c.name, func(t *testing.T) {
			rec := mpuRequest(t, h, http.MethodPut, "/bkt/"+key+"?partNumber=2&uploadId="+uploadID,
				strings.NewReader("hello"), func(r *http.Request) {
					r.Header.Set(c.header, c.value)
				})
			requireNotImplemented(t, rec)
		})
	}

	// 被拒请求不得写出分片：会话里应仍然只有第 1 片。
	rec = mpuRequest(t, h, http.MethodGet, "/bkt/"+key+"?uploadId="+uploadID, nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("ListParts = %d：%s", rec.Code, rec.Body.String())
	}
	var parts listPartsResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &parts); err != nil {
		t.Fatalf("ListParts 响应不是合法 XML：%v (%s)", err, rec.Body.String())
	}
	if len(parts.Parts) != 1 || parts.Parts[0].PartNumber != 1 {
		t.Fatalf("被拒的 UploadPart 改动了会话：分片 = %+v", parts.Parts)
	}
}

// TestCompleteMultipartUploadRejectsSSEHeadersHTTP 验证 Complete 被拒时
// 不消耗会话：客户端去掉 SSE 头后可用同一 uploadId 重试。
func TestCompleteMultipartUploadRejectsSSEHeadersHTTP(t *testing.T) {
	h := mpuTestServer(t)
	const key = "complete.bin"
	uploadID := createUploadHTTP(t, h, key, nil)

	rec := mpuRequest(t, h, http.MethodPut, "/bkt/"+key+"?partNumber=1&uploadId="+uploadID,
		strings.NewReader("hello"), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("准备分片失败：%d %s", rec.Code, rec.Body.String())
	}
	etag := strings.Trim(rec.Header().Get("ETag"), `"`)

	body := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + etag +
		`</ETag></Part></CompleteMultipartUpload>`
	for _, c := range unsupportedSSECases {
		t.Run(c.name, func(t *testing.T) {
			rec := mpuRequest(t, h, http.MethodPost, "/bkt/"+key+"?uploadId="+uploadID,
				strings.NewReader(body), func(r *http.Request) {
					r.Header.Set(c.header, c.value)
				})
			requireNotImplemented(t, rec)
		})
	}

	// 会话未被消耗：ListParts 仍能列出分片。
	rec = mpuRequest(t, h, http.MethodGet, "/bkt/"+key+"?uploadId="+uploadID, nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("Complete 被拒后会话不可用：ListParts = %d %s", rec.Code, rec.Body.String())
	}
}

// TestCopyObjectRejectsUnsupportedHeadersHTTP 覆盖复制特有的两个头
// （标签指令、复制源 SSE-C），并验证目标对象不存在。
func TestCopyObjectRejectsUnsupportedHeadersHTTP(t *testing.T) {
	h := objectTestServer(t)
	if rec := putObject(t, h, "/bkt/src.txt", "payload", nil); rec.Code != http.StatusOK {
		t.Fatalf("准备源对象失败：%d %s", rec.Code, rec.Body.String())
	}

	cases := append([]headerCase{}, unsupportedWriteHeaderCases...)
	cases = append(cases,
		headerCase{"标签指令", "x-amz-tagging-directive", "REPLACE"},
		headerCase{"复制源 SSE-C", "x-amz-copy-source-server-side-encryption-customer-algorithm", "AES256"},
	)

	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dst := fmt.Sprintf("/bkt/dst-%d.txt", i)
			rec := putObject(t, h, dst, "", func(r *http.Request) {
				r.Header.Set("x-amz-copy-source", "/bkt/src.txt")
				r.Header.Set(c.header, c.value)
			})
			requireNotImplemented(t, rec)
			if got := do(t, h, http.MethodGet, dst); got.Code != http.StatusNotFound {
				t.Fatalf("被拒绝的复制留下了目标对象：GET = %d", got.Code)
			}
		})
	}
}

// TestUploadPartCopyRejectsUnsupportedHeadersHTTP 验证分片复制与 CopyObject
// 共用同一份拒绝列表，且被拒时不分片写入。
func TestUploadPartCopyRejectsUnsupportedHeadersHTTP(t *testing.T) {
	h := mpuTestServer(t)
	if rec := putObject(t, h, "/bkt/src.bin", "payload", nil); rec.Code != http.StatusOK {
		t.Fatalf("准备源对象失败：%d %s", rec.Code, rec.Body.String())
	}

	const key = "copied.bin"
	uploadID := createUploadHTTP(t, h, key, nil)

	rec := mpuRequest(t, h, http.MethodPut, "/bkt/"+key+"?partNumber=1&uploadId="+uploadID, nil,
		func(r *http.Request) {
			r.Header.Set("x-amz-copy-source", "/bkt/src.bin")
			r.Header.Set("x-amz-acl", "public-read")
		})
	requireNotImplemented(t, rec)

	rec = mpuRequest(t, h, http.MethodGet, "/bkt/"+key+"?uploadId="+uploadID, nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("ListParts = %d：%s", rec.Code, rec.Body.String())
	}
	var parts listPartsResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &parts); err != nil {
		t.Fatalf("ListParts 响应不是合法 XML：%v (%s)", err, rec.Body.String())
	}
	if len(parts.Parts) != 0 {
		t.Fatalf("被拒的分片复制写了 %d 个分片", len(parts.Parts))
	}
}

// TestUnrelatedHeadersStillAcceptedHTTP 守住"不过度拒绝"的边界：
// 账户/计费语义的头不承诺落盘属性，按文档忽略；STANDARD 存储类型照常接受。
// 把这类头也拒掉会让正常客户端莫名失败。
func TestUnrelatedHeadersStillAcceptedHTTP(t *testing.T) {
	h := objectTestServer(t)
	rec := putObject(t, h, "/bkt/ok.txt", "hello", func(r *http.Request) {
		r.Header.Set("x-amz-expected-bucket-owner", "123456789012")
		r.Header.Set("x-amz-request-payer", "requester")
		r.Header.Set("x-amz-storage-class", "STANDARD")
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200：%s", rec.Code, rec.Body.String())
	}
}
