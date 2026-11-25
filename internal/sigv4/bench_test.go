package sigv4

import (
	"net/http"
	"net/url"
	"testing"
	"time"
)

// BenchmarkSigV4Verify 量化签名校验的每请求成本（解析 Authorization、
// 重建 canonical request、HMAC）。这是每个请求都走的前置开销，
// 与服务端 handler 自身的成本无关，因此两路签名分开计量。
func BenchmarkSigV4Verify(b *testing.B) {
	b.Run("header", func(b *testing.B) {
		r, amzDate := signedReq(b, http.MethodGet, "http://"+vecHost+"/b/obj.bin")
		opt := baseOptions(amzDate)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, serr := Verify(r, opt); serr != nil {
				b.Fatalf("Verify 失败: %v", serr)
			}
		}
	})

	b.Run("presigned", func(b *testing.B) {
		now := mustParseAmzForTest("20261023T120000Z")
		req := mustRequest(b, http.MethodGet, "http://"+vecHost+"/b/obj.bin")
		raw, err := Presign(req, PresignOptions{
			Credentials: Credentials{AccessKeyID: vecAK, SecretKey: vecSK},
			Region:      vecRegion,
			Expires:     time.Hour,
			Now:         fixedNow(now),
		})
		if err != nil {
			b.Fatalf("Presign 失败: %v", err)
		}
		u, err := url.Parse(raw)
		if err != nil {
			b.Fatalf("解析 Presigned URL 失败: %v", err)
		}
		// 与 net/http 交给 handler 的形态一致：查询参数在 URL 上。
		r := &http.Request{Method: http.MethodGet, URL: u, Host: u.Host, Header: http.Header{}}
		opt := Options{
			Provider: NewStaticProvider(Credentials{AccessKeyID: vecAK, SecretKey: vecSK}),
			Region:   vecRegion,
			Now:      fixedNow(now),
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, serr := Verify(r, opt); serr != nil {
				b.Fatalf("Verify 失败: %v", serr)
			}
		}
	})
}
