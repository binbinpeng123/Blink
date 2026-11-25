//go:build integration

package integration

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// listBucketsEntry 是 ListBuckets 的单条记录。
type listBucketsEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
	BucketRegion string `xml:"BucketRegion"`
}

// listBucketsResult 是 ListBuckets 响应体里本测试关心的部分。
type listBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Owner   struct {
		ID string `xml:"ID"`
	} `xml:"Owner"`
	Buckets struct {
		Bucket []listBucketsEntry `xml:"Bucket"`
	} `xml:"Buckets"`
	ContinuationToken string `xml:"ContinuationToken"`
	Prefix            string `xml:"Prefix"`
}

// listBucketsVia 发一次带查询参数的 ListBuckets 请求。
func listBucketsVia(t *testing.T, c *s3Client, q url.Values) listBucketsResult {
	t.Helper()
	res := c.do(http.MethodGet, "/", q, nil, nil)
	requireStatus(t, res, http.StatusOK)
	return mustXML[listBucketsResult](t, res)
}

// entryNames 取记录里的桶名。
func entryNames(res listBucketsResult) []string {
	out := make([]string, 0, len(res.Buckets.Bucket))
	for _, b := range res.Buckets.Bucket {
		out = append(out, b.Name)
	}
	return out
}

// TestListBucketsPagination 用真实二进制与签名请求验证 ListBuckets 的
// prefix / max-buckets / continuation-token / bucket-region 契约。
//
// 单测已覆盖参数组合；这里额外验证签名路径（查询串参与签名）与真实 URL
// 编解码 —— continuation-token 在请求 URL 里往返一次后仍是同一个值。
func TestListBucketsPagination(t *testing.T) {
	srv := startServer(t, serverOptions{})
	c := newClient(t, srv)

	base := uniqueBucket("page")
	names := []string{base + "-a", base + "-b", base + "-c"}
	for _, n := range names {
		requireStatus(t, c.createBucket(n), http.StatusOK)
	}

	t.Run("prefix 过滤并回显", func(t *testing.T) {
		res := listBucketsVia(t, c, url.Values{"prefix": {base}})
		if got := strings.Join(entryNames(res), ","); got != strings.Join(names, ",") {
			t.Fatalf("桶列表 = %s，期望 %s", got, strings.Join(names, ","))
		}
		if res.Prefix != base {
			t.Errorf("Prefix = %q，期望回显 %q", res.Prefix, base)
		}
		if res.ContinuationToken != "" {
			t.Errorf("未截断时不应给 token，实际 %q", res.ContinuationToken)
		}
		for _, b := range res.Buckets.Bucket {
			if b.BucketRegion != testRegion {
				t.Errorf("桶 %s 的 BucketRegion = %q，期望 %q", b.Name, b.BucketRegion, testRegion)
			}
			if b.CreationDate == "" {
				t.Errorf("桶 %s 缺少 CreationDate", b.Name)
			}
		}
		if res.Owner.ID == "" {
			t.Error("Owner.ID 不应为空")
		}
	})

	t.Run("max-buckets 翻页取完", func(t *testing.T) {
		first := listBucketsVia(t, c, url.Values{"prefix": {base}, "max-buckets": {"2"}})
		if got := entryNames(first); len(got) != 2 || got[0] != names[0] || got[1] != names[1] {
			t.Fatalf("第一页 = %v", got)
		}
		if first.ContinuationToken == "" {
			t.Fatal("第一页应返回 ContinuationToken")
		}

		second := listBucketsVia(t, c, url.Values{
			"prefix":             {base},
			"max-buckets":        {"2"},
			"continuation-token": {first.ContinuationToken},
		})
		if got := entryNames(second); len(got) != 1 || got[0] != names[2] {
			t.Fatalf("第二页 = %v", got)
		}
		if second.ContinuationToken != "" {
			t.Errorf("最后一页不应再给 token，实际 %q", second.ContinuationToken)
		}
	})

	t.Run("token 与前缀不匹配返回 400", func(t *testing.T) {
		first := listBucketsVia(t, c, url.Values{"prefix": {base}, "max-buckets": {"1"}})
		if first.ContinuationToken == "" {
			t.Fatal("第一页应返回 ContinuationToken")
		}
		res := c.do(http.MethodGet, "/", url.Values{
			"prefix":             {base + "-b"},
			"continuation-token": {first.ContinuationToken},
		}, nil, nil)
		requireS3Error(t, res, http.StatusBadRequest, "InvalidArgument")
	})

	t.Run("跨区域返回空列表", func(t *testing.T) {
		res := listBucketsVia(t, c, url.Values{"prefix": {base}, "bucket-region": {"eu-west-1"}})
		if len(res.Buckets.Bucket) != 0 {
			t.Fatalf("跨区域应返回空列表，实际 %v", entryNames(res))
		}
	})

	t.Run("非法 max-buckets 返回 400", func(t *testing.T) {
		res := c.do(http.MethodGet, "/", url.Values{"max-buckets": {"abc"}}, nil, nil)
		requireS3Error(t, res, http.StatusBadRequest, "InvalidArgument")
	})
}
