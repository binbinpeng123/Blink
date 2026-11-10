//go:build integration

package integration

import (
	"encoding/xml"
	"net/http"
	"testing"
)

// TestBucketLifecycle 覆盖桶的创建、列举、HEAD、非空删除保护与最终删除。
func TestBucketLifecycle(t *testing.T) {
	srv := startServer(t, serverOptions{})
	c := newClient(t, srv)

	bucket := uniqueBucket("bucket")
	requireStatus(t, c.createBucket(bucket), http.StatusOK)

	// 重复创建：单节点单租户语义下是 BucketAlreadyOwnedByYou。
	requireS3Error(t, c.createBucket(bucket), http.StatusConflict, "BucketAlreadyOwnedByYou")

	t.Run("HEAD 存在桶返回区域", func(t *testing.T) {
		res := c.headBucket(bucket)
		requireStatus(t, res, http.StatusOK)
		if got := res.Header.Get("x-amz-bucket-region"); got != testRegion {
			t.Fatalf("x-amz-bucket-region = %q，期望 %q", got, testRegion)
		}
	})

	t.Run("HEAD 不存在桶返回 404", func(t *testing.T) {
		res := c.headBucket(uniqueBucket("missing"))
		requireStatus(t, res, http.StatusNotFound)
	})

	t.Run("ListBuckets 包含已创建的桶", func(t *testing.T) {
		res := c.do(http.MethodGet, "/", nil, nil, nil)
		requireStatus(t, res, http.StatusOK)

		type bucketEntry struct {
			Name string `xml:"Name"`
		}
		type listAll struct {
			XMLName xml.Name `xml:"ListAllMyBucketsResult"`
			Buckets struct {
				Bucket []bucketEntry `xml:"Bucket"`
			} `xml:"Buckets"`
		}
		got := mustXML[listAll](t, res)
		found := false
		for _, b := range got.Buckets.Bucket {
			if b.Name == bucket {
				found = true
			}
		}
		if !found {
			t.Fatalf("ListBuckets 未包含 %q：%+v", bucket, got.Buckets.Bucket)
		}
	})

	t.Run("非法桶名返回 400", func(t *testing.T) {
		res := c.createBucket("Bad_Bucket")
		requireS3Error(t, res, http.StatusBadRequest, "InvalidBucketName")
	})

	t.Run("非空桶拒绝删除", func(t *testing.T) {
		requireStatus(t, c.putObject(bucket, "keep.txt", []byte("x"), nil), http.StatusOK)
		requireS3Error(t, c.deleteBucket(bucket), http.StatusConflict, "BucketNotEmpty")

		// 清空后再删，验证 204 且之后 HEAD 为 404。
		requireStatus(t, c.deleteObject(bucket, "keep.txt"), http.StatusNoContent)
		requireStatus(t, c.deleteBucket(bucket), http.StatusNoContent)
		requireStatus(t, c.headBucket(bucket), http.StatusNotFound)
	})
}
