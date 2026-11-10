//go:build integration

package integration

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
)

// listResult 是 ListObjectsV2 响应中本测试关心的字段。
type listResult struct {
	XMLName     xml.Name `xml:"ListBucketResult"`
	Name        string   `xml:"Name"`
	Prefix      string   `xml:"Prefix"`
	Delimiter   string   `xml:"Delimiter"`
	MaxKeys     int      `xml:"MaxKeys"`
	KeyCount    int      `xml:"KeyCount"`
	IsTruncated bool     `xml:"IsTruncated"`
	NextToken   string   `xml:"NextContinuationToken"`
	Encoding    string   `xml:"EncodingType"`
	Contents    []struct {
		Key  string `xml:"Key"`
		Size int64  `xml:"Size"`
	} `xml:"Contents"`
	CommonPrefixes []struct {
		Prefix string `xml:"Prefix"`
	} `xml:"CommonPrefixes"`
}

func (r listResult) keys() []string {
	out := make([]string, 0, len(r.Contents))
	for _, c := range r.Contents {
		out = append(out, c.Key)
	}
	return out
}

func listV2(t *testing.T, c *s3Client, bucket string, q url.Values) *httpResult {
	t.Helper()
	v := url.Values{"list-type": {"2"}}
	for k, vals := range q {
		v[k] = vals
	}
	return c.do(http.MethodGet, c.bucketURL(bucket), v, nil, nil)
}

// TestListObjectsV2 覆盖分页、前缀、分隔符、start-after 与 URL 编码。
func TestListObjectsV2(t *testing.T) {
	srv := startServer(t, serverOptions{})
	c := newClient(t, srv)

	bucket := uniqueBucket("list")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)

	keys := []string{
		"a.txt",
		"b/1.txt",
		"b/2.txt",
		"c/d/3.txt",
		"z.txt",
		"中文.txt",
	}
	for i, k := range keys {
		requireStatus(t, c.putObject(bucket, k, []byte{byte(i)}, nil), http.StatusOK)
	}

	t.Run("完整列举按字节序返回", func(t *testing.T) {
		res := listV2(t, c, bucket, nil)
		requireStatus(t, res, http.StatusOK)
		got := mustXML[listResult](t, res)
		if got.Name != bucket {
			t.Fatalf("Name = %q，期望 %q", got.Name, bucket)
		}
		want := append([]string(nil), keys...)
		sort.Strings(want)
		if strings.Join(got.keys(), ",") != strings.Join(want, ",") {
			t.Fatalf("列举结果 = %v，期望 %v", got.keys(), want)
		}
		if got.KeyCount != len(keys) {
			t.Fatalf("KeyCount = %d，期望 %d", got.KeyCount, len(keys))
		}
		if got.IsTruncated {
			t.Fatalf("未分页时 IsTruncated 应为 false")
		}
	})

	t.Run("max-keys 翻页可完整遍历", func(t *testing.T) {
		var all []string
		token := ""
		pages := 0
		for {
			pages++
			if pages > 10 {
				t.Fatalf("分页次数异常，可能存在 token 循环")
			}
			q := url.Values{"max-keys": {"2"}}
			if token != "" {
				q.Set("continuation-token", token)
			}
			res := listV2(t, c, bucket, q)
			requireStatus(t, res, http.StatusOK)
			page := mustXML[listResult](t, res)
			all = append(all, page.keys()...)
			if !page.IsTruncated {
				if page.NextToken != "" {
					t.Fatalf("IsTruncated=false 却返回了 NextContinuationToken")
				}
				break
			}
			if page.NextToken == "" {
				t.Fatalf("IsTruncated=true 但缺少 NextContinuationToken")
			}
			token = page.NextToken
		}

		want := append([]string(nil), keys...)
		sort.Strings(want)
		if strings.Join(all, ",") != strings.Join(want, ",") {
			t.Fatalf("翻页合并结果 = %v，期望 %v", all, want)
		}
	})

	t.Run("prefix 过滤", func(t *testing.T) {
		res := listV2(t, c, bucket, url.Values{"prefix": {"b/"}})
		requireStatus(t, res, http.StatusOK)
		got := mustXML[listResult](t, res)
		if strings.Join(got.keys(), ",") != "b/1.txt,b/2.txt" {
			t.Fatalf("prefix 结果 = %v", got.keys())
		}
	})

	t.Run("delimiter 折叠公共前缀", func(t *testing.T) {
		res := listV2(t, c, bucket, url.Values{"delimiter": {"/"}})
		requireStatus(t, res, http.StatusOK)
		got := mustXML[listResult](t, res)
		if strings.Join(got.keys(), ",") != "a.txt,z.txt,中文.txt" {
			t.Fatalf("分隔符下的对象 = %v", got.keys())
		}
		var prefixes []string
		for _, p := range got.CommonPrefixes {
			prefixes = append(prefixes, p.Prefix)
		}
		if strings.Join(prefixes, ",") != "b/,c/" {
			t.Fatalf("CommonPrefixes = %v，期望 [b/ c/]", prefixes)
		}
		// KeyCount 同时计入对象与公共前缀（S3 语义）。
		if got.KeyCount != 5 {
			t.Fatalf("KeyCount = %d，期望 5（3 对象 + 2 前缀）", got.KeyCount)
		}
	})

	t.Run("start-after", func(t *testing.T) {
		res := listV2(t, c, bucket, url.Values{"start-after": {"b/2.txt"}})
		requireStatus(t, res, http.StatusOK)
		got := mustXML[listResult](t, res)
		if strings.Join(got.keys(), ",") != "c/d/3.txt,z.txt,中文.txt" {
			t.Fatalf("start-after 结果 = %v", got.keys())
		}
	})

	t.Run("encoding-type=url", func(t *testing.T) {
		res := listV2(t, c, bucket, url.Values{"encoding-type": {"url"}})
		requireStatus(t, res, http.StatusOK)
		got := mustXML[listResult](t, res)
		if got.Encoding != "url" {
			t.Fatalf("EncodingType = %q", got.Encoding)
		}
		var found string
		for _, k := range got.keys() {
			if strings.Contains(k, "%E4%B8%AD%E6%96%87") {
				found = k
			}
		}
		if found == "" {
			t.Fatalf("URL 编码结果中未找到中文 key：%v", got.keys())
		}
	})

	t.Run("token 与 prefix 不一致返回 400", func(t *testing.T) {
		res := listV2(t, c, bucket, url.Values{"max-keys": {"1"}, "prefix": {"b/"}})
		requireStatus(t, res, http.StatusOK)
		page := mustXML[listResult](t, res)
		if page.NextToken == "" {
			t.Fatalf("预期分页 token")
		}

		res = listV2(t, c, bucket, url.Values{
			"continuation-token": {page.NextToken},
			"prefix":             {"c/"},
		})
		requireS3Error(t, res, http.StatusBadRequest, "InvalidArgument")
	})

	t.Run("max-keys 负值返回 400", func(t *testing.T) {
		res := listV2(t, c, bucket, url.Values{"max-keys": {"-1"}})
		requireS3Error(t, res, http.StatusBadRequest, "InvalidArgument")
	})

	t.Run("max-keys 非法整数返回 400", func(t *testing.T) {
		res := listV2(t, c, bucket, url.Values{"max-keys": {"abc"}})
		requireS3Error(t, res, http.StatusBadRequest, "InvalidArgument")
	})
}
