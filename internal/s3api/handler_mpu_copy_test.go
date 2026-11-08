package s3api

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// TestUploadPartCopyFlow 覆盖完整复制与区间复制两条路径，并验证
// Complete 之后的内容与源对象逐字节一致。
func TestUploadPartCopyFlow(t *testing.T) {
	h := mpuTestServer(t)
	src := mpuPayload(5<<20+1024, 0x55)
	if rec := putObject(t, h, "/bkt/src", string(src), nil); rec.Code != http.StatusOK {
		t.Fatalf("写入源对象失败: %d", rec.Code)
	}
	uploadID := createUploadHTTP(t, h, "dst", nil)

	const size1 = 5 << 20
	rec := mpuRequest(t, h, http.MethodPut, "/bkt/dst?partNumber=1&uploadId="+uploadID, nil,
		func(r *http.Request) {
			r.Header.Set("x-amz-copy-source", "/bkt/src")
			r.Header.Set("x-amz-copy-source-range", fmt.Sprintf("bytes=0-%d", size1-1))
		})
	if rec.Code != http.StatusOK {
		t.Fatalf("UploadPartCopy(区间) = %d：%s", rec.Code, rec.Body.String())
	}
	var cpr copyPartResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &cpr); err != nil {
		t.Fatalf("解析 CopyPartResult 失败: %v（%s）", err, rec.Body.String())
	}
	if cpr.LastModified == "" {
		t.Error("CopyPartResult 缺少 LastModified")
	}
	if !strings.HasPrefix(cpr.ETag, `"`) || !strings.HasSuffix(cpr.ETag, `"`) {
		t.Errorf("CopyPartResult.ETag 未加引号: %q", cpr.ETag)
	}
	// 头部 ETag 与 XML 中的必须是同一个值；后续 Complete 依赖它。
	if got := rec.Header().Get("ETag"); got != cpr.ETag {
		t.Errorf("响应头 ETag = %q，XML = %q", got, cpr.ETag)
	}
	etag1 := cpr.ETag

	// 第二片不带区间：复制整个源对象。
	rec = mpuRequest(t, h, http.MethodPut, "/bkt/dst?partNumber=2&uploadId="+uploadID, nil,
		func(r *http.Request) {
			r.Header.Set("x-amz-copy-source", "/bkt/src")
		})
	if rec.Code != http.StatusOK {
		t.Fatalf("UploadPartCopy(完整) = %d：%s", rec.Code, rec.Body.String())
	}
	if err := xml.Unmarshal(rec.Body.Bytes(), &cpr); err != nil {
		t.Fatalf("解析 CopyPartResult 失败: %v（%s）", err, rec.Body.String())
	}
	etag2 := cpr.ETag
	if etag2 == etag1 {
		t.Error("两个区间内容不同，分片 ETag 不应相同")
	}

	// ListParts 的 ETag 必须与 CopyPartResult 一致，否则客户端按它
	// 提交 Complete 会得到 InvalidPart。
	rec = mpuRequest(t, h, http.MethodGet, "/bkt/dst?uploadId="+uploadID, nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("ListParts = %d：%s", rec.Code, rec.Body.String())
	}
	var lp listPartsResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &lp); err != nil {
		t.Fatalf("解析 ListParts 响应失败: %v", err)
	}
	if len(lp.Parts) != 2 || lp.Parts[0].ETag != etag1 || lp.Parts[1].ETag != etag2 {
		t.Errorf("ListParts 分片与 CopyPartResult 不一致: %+v", lp.Parts)
	}
	if lp.Parts[0].Size != size1 || lp.Parts[1].Size != int64(len(src)) {
		t.Errorf("ListParts 分片大小 = %d/%d", lp.Parts[0].Size, lp.Parts[1].Size)
	}

	completeXML := fmt.Sprintf(
		`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part>`+
			`<Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`,
		etag1, etag2)
	if rec := mpuRequest(t, h, http.MethodPost, "/bkt/dst?uploadId="+uploadID,
		strings.NewReader(completeXML), nil); rec.Code != http.StatusOK {
		t.Fatalf("Complete = %d：%s", rec.Code, rec.Body.String())
	}

	want := append(append([]byte{}, src[:size1]...), src...)
	rec = mpuRequest(t, h, http.MethodGet, "/bkt/dst", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET dst = %d：%s", rec.Code, rec.Body.String())
	}
	if !bytes.Equal(rec.Body.Bytes(), want) {
		t.Error("复制出的对象内容与预期不一致")
	}
	// 分片边界来自本次上传，而不是源对象：目标是两个分片。
	if got := rec.Header().Get("x-amz-mp-parts-count"); got != "" {
		t.Errorf("普通 GET 不应带 x-amz-mp-parts-count: %q", got)
	}
	rec = mpuRequest(t, h, http.MethodGet, "/bkt/dst?partNumber=2", nil, nil)
	if rec.Code != http.StatusPartialContent {
		t.Fatalf("分片下载 = %d，期望 206", rec.Code)
	}
	if !bytes.Equal(rec.Body.Bytes(), src) {
		t.Error("第二片内容与源对象不一致")
	}
	if got := rec.Header().Get("x-amz-mp-parts-count"); got != "2" {
		t.Errorf("x-amz-mp-parts-count = %q，期望 2", got)
	}
}

// TestUploadPartCopyErrors 覆盖复制源条件、区间校验与早期失败路径。
func TestUploadPartCopyErrors(t *testing.T) {
	h := mpuTestServer(t)
	const srcBody = "0123456789abcdef"
	if rec := putObject(t, h, "/bkt/src", srcBody, nil); rec.Code != http.StatusOK {
		t.Fatalf("写入源对象失败: %d", rec.Code)
	}
	srcETag := objectRequest(t, h, http.MethodHead, "/bkt/src", nil).Header().Get("ETag")
	uploadID := createUploadHTTP(t, h, "dst", nil)
	target := "/bkt/dst?partNumber=1&uploadId=" + uploadID

	cases := []struct {
		name   string
		mutate func(*http.Request)
		status int
		code   string
		cRange string
	}{
		{
			name:   "源不存在",
			mutate: func(r *http.Request) { r.Header.Set("x-amz-copy-source", "/bkt/missing") },
			status: http.StatusNotFound, code: "NoSuchKey",
		},
		{
			name: "区间越界",
			mutate: func(r *http.Request) {
				r.Header.Set("x-amz-copy-source", "/bkt/src")
				r.Header.Set("x-amz-copy-source-range", "bytes=0-99")
			},
			status: http.StatusRequestedRangeNotSatisfiable, code: "InvalidRange",
			cRange: fmt.Sprintf("bytes */%d", len(srcBody)),
		},
		{
			name: "区间终点早于起点",
			mutate: func(r *http.Request) {
				r.Header.Set("x-amz-copy-source", "/bkt/src")
				r.Header.Set("x-amz-copy-source-range", "bytes=5-1")
			},
			status: http.StatusRequestedRangeNotSatisfiable, code: "InvalidRange",
		},
		{
			// 只接受 bytes=first-last：开放区间在多区间语义下无法表达。
			name: "区间缺终点",
			mutate: func(r *http.Request) {
				r.Header.Set("x-amz-copy-source", "/bkt/src")
				r.Header.Set("x-amz-copy-source-range", "bytes=0-")
			},
			status: http.StatusRequestedRangeNotSatisfiable, code: "InvalidRange",
		},
		{
			name: "源条件不满足",
			mutate: func(r *http.Request) {
				r.Header.Set("x-amz-copy-source", "/bkt/src")
				r.Header.Set("x-amz-copy-source-if-match", `"deadbeef"`)
			},
			status: http.StatusPreconditionFailed, code: "PreconditionFailed",
		},
		{
			// 条件满足时必须继续复制，而不是"带了条件就拒绝"。
			name: "源条件满足",
			mutate: func(r *http.Request) {
				r.Header.Set("x-amz-copy-source", "/bkt/src")
				r.Header.Set("x-amz-copy-source-if-match", srcETag)
			},
			status: http.StatusOK,
		},
		{
			name: "存储类型非 STANDARD",
			mutate: func(r *http.Request) {
				r.Header.Set("x-amz-copy-source", "/bkt/src")
				r.Header.Set("x-amz-storage-class", "GLACIER")
			},
			status: http.StatusNotImplemented, code: "NotImplemented",
		},
		{
			name: "带 SSE 头",
			mutate: func(r *http.Request) {
				r.Header.Set("x-amz-copy-source", "/bkt/src")
				r.Header.Set("x-amz-server-side-encryption", "AES256")
			},
			status: http.StatusNotImplemented, code: "NotImplemented",
		},
		{
			// 会话不存在必须在读源之前失败；同时验证写端 goroutine
			// 不会因为读端提前关闭而泄漏（测试结束前函数必须返回）。
			name: "uploadId 不存在",
			mutate: func(r *http.Request) {
				r.Header.Set("x-amz-copy-source", "/bkt/src")
			},
			status: http.StatusNotFound, code: "NoSuchUpload",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			targetURL := target
			if tc.name == "uploadId 不存在" {
				targetURL = "/bkt/dst?partNumber=1&uploadId=00000000-0000-4000-8000-000000000000"
			}
			rec := mpuRequest(t, h, http.MethodPut, targetURL, nil, tc.mutate)
			if rec.Code != tc.status {
				t.Fatalf("状态码 = %d，期望 %d：%s", rec.Code, tc.status, rec.Body.String())
			}
			if tc.code != "" {
				if got := decodeError(t, rec).Code; got != tc.code {
					t.Errorf("错误码 = %q，期望 %q", got, tc.code)
				}
			}
			if tc.cRange != "" {
				if got := rec.Header().Get("Content-Range"); got != tc.cRange {
					t.Errorf("Content-Range = %q，期望 %q", got, tc.cRange)
				}
			}
		})
	}
}

// TestUploadPartRoutesByCopySource 固定 PUT 同路由的两条分流路径。
func TestUploadPartRoutesByCopySource(t *testing.T) {
	h := mpuTestServer(t)
	if rec := putObject(t, h, "/bkt/src", "source", nil); rec.Code != http.StatusOK {
		t.Fatalf("写入源对象失败: %d", rec.Code)
	}
	uploadID := createUploadHTTP(t, h, "dst", nil)
	target := "/bkt/dst?partNumber=1&uploadId=" + uploadID

	// 无 x-amz-copy-source：普通 UploadPart，回 ETag 头且没有 XML 响应体。
	rec := mpuRequest(t, h, http.MethodPut, target, strings.NewReader("payload"), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("UploadPart = %d：%s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("ETag") == "" {
		t.Error("UploadPart 缺少 ETag 头")
	}
	if strings.Contains(rec.Body.String(), "CopyPartResult") {
		t.Error("普通 UploadPart 不应返回 CopyPartResult")
	}

	// 带 x-amz-copy-source：UploadPartCopy，回 CopyPartResult。
	rec = mpuRequest(t, h, http.MethodPut, target, nil, func(r *http.Request) {
		r.Header.Set("x-amz-copy-source", "/bkt/src")
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("UploadPartCopy = %d：%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "<CopyPartResult>") {
		t.Errorf("UploadPartCopy 缺少 CopyPartResult：%s", rec.Body.String())
	}
}

// TestParseCopyPartRange 固化区间语法与 5 GiB 边界（int64，不得溢出）。
func TestParseCopyPartRange(t *testing.T) {
	cases := []struct {
		name    string
		spec    string
		size    int64
		offset  int64
		length  int64
		wantErr bool
	}{
		{name: "缺省复制整个对象", spec: "", size: 100, offset: 0, length: 100},
		{name: "整段", spec: "bytes=0-99", size: 100, offset: 0, length: 100},
		{name: "中间一段", spec: "bytes=10-19", size: 100, offset: 10, length: 10},
		{name: "单字节", spec: "bytes=99-99", size: 100, offset: 99, length: 1},
		{name: "恰好 5 GiB", spec: "bytes=0-5368709119", size: 5 << 30, offset: 0, length: 5 << 30},
		{name: "超过 5 GiB", spec: "bytes=0-5368709120", size: 5<<30 + 1, offset: 0, length: 5<<30 + 1},
		{name: "起点越界", spec: "bytes=100-100", size: 100, wantErr: true},
		{name: "终点越界", spec: "bytes=0-100", size: 100, wantErr: true},
		{name: "终点早于起点", spec: "bytes=19-10", size: 100, wantErr: true},
		{name: "开放区间", spec: "bytes=0-", size: 100, wantErr: true},
		{name: "后缀区间", spec: "bytes=-10", size: 100, wantErr: true},
		{name: "多区间", spec: "bytes=0-9,20-29", size: 100, wantErr: true},
		{name: "前缀错误", spec: "items=0-9", size: 100, wantErr: true},
		{name: "空对象整段", spec: "", size: 0, offset: 0, length: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			offset, length, e := parseCopyPartRange(tc.spec, tc.size)
			if tc.wantErr {
				if e == nil {
					t.Fatalf("期望错误，得到 offset=%d length=%d", offset, length)
				}
				return
			}
			if e != nil {
				t.Fatalf("意外错误: %v", e)
			}
			if offset != tc.offset || length != tc.length {
				t.Errorf("offset/length = %d/%d，期望 %d/%d", offset, length, tc.offset, tc.length)
			}
		})
	}

	// 5 GiB 边界比较必须在 int64 上完成：用 int 在 32 位平台上会溢出成负数，
	// 使超限的复制被错误放行。
	if maxCopyPartSize != int64(5<<30) {
		t.Errorf("maxCopyPartSize = %d", maxCopyPartSize)
	}
}
