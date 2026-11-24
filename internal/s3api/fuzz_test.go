package s3api

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/binbinpeng123/Blink/internal/store"
)

// 本文件是协议解析层的模糊测试基线。
//
// 关注的不是行覆盖率，而是把**所有来自客户端的字节**都当作恶意输入：
// 每个目标除了"不 panic"，还断言解析成功时必须成立的不变量
// （区间不越界、编码可逆、token 可逆）。`go test` 每次都会执行种子语料，
// 因此这些断言同时充当回归测试。
//
// 有界 fuzz（纯 CPU，不写磁盘、不联网）：
//
//	go test -run=^$ -fuzz=^FuzzParseRange$ -fuzztime=20s ./internal/s3api
func nonNegative(v int64) int64 {
	if v < 0 {
		// 不用 -v：MinInt64 取反会溢出回负数。
		return -(v + 1)
	}
	return v
}

// FuzzParseRange 校验 Range 解析的两条硬不变量：
//  1. 成功结果的区间必须完全落在对象内（start ≥ 0，length > 0，start+length ≤ size）；
//  2. 结果可逆 —— 把成功结果写成显式区间 `bytes=start-end` 再解析，必须得到同一结果。
//
// 第 2 条能抓住"后缀形式与显式形式换算错位"这类只影响特定输入的缺陷。
func FuzzParseRange(f *testing.F) {
	seeds := []struct {
		spec string
		size int64
	}{
		{"bytes=0-0", 1},
		{"bytes=0-", 100},
		{"bytes=10-19", 100},
		{"bytes=10-", 100},
		{"bytes=-10", 100},
		{"bytes=-200", 100},
		{"bytes=99-", 100},
		{"bytes=100-", 100},
		{"bytes=100-200", 100},
		{"bytes=5-4", 100},
		{"bytes=-0", 100},
		{"bytes=0-1,3-4", 100},
		{"bytes=", 100},
		{"bytes=0", 100},
		{"items=0-1", 100},
		{"  BYTES=-5  ", 100},
		{"bytes=0-9223372036854775807", 100},
		{"bytes=-9223372036854775808", 100},
		{"bytes=0-", 0},
	}
	for _, s := range seeds {
		f.Add(s.spec, s.size)
	}

	f.Fuzz(func(t *testing.T, spec string, size int64) {
		size = nonNegative(size)
		start, length, serr := parseRange(spec, size)
		if serr != nil {
			return
		}
		if size == 0 {
			t.Fatalf("size=0 不应解析成功：spec=%q → (%d,%d)", spec, start, length)
		}
		if start < 0 || length <= 0 || start >= size || length > size-start {
			t.Fatalf("区间越界：spec=%q size=%d → start=%d length=%d", spec, size, start, length)
		}

		explicit := fmt.Sprintf("bytes=%d-%d", start, start+length-1)
		gotStart, gotLength, gotErr := parseRange(explicit, size)
		if gotErr != nil || gotStart != start || gotLength != length {
			t.Fatalf("结果不可逆：spec=%q size=%d → (%d,%d)，重写为 %q 得到 (%d,%d) err=%v",
				spec, size, start, length, explicit, gotStart, gotLength, gotErr)
		}
	})
}

// FuzzParseCopyPartRange 校验复制源区间。
//
// 与 Range 的差别：空串表示"整段复制"，其余情形一律严格，不允许截断，
// 因此成功时区间必须完整落在源对象内。
func FuzzParseCopyPartRange(f *testing.F) {
	seeds := []struct {
		spec string
		size int64
	}{
		{"", 100},
		{"", 0},
		{"bytes=0-0", 100},
		{"bytes=0-99", 100},
		{"bytes=0-100", 100},
		{"bytes=50-49", 100},
		{"bytes=-10", 100},
		{"bytes=10-", 100},
		{"bytes=0-1,3-4", 100},
		{"BYTES=5-9", 100},
		{"  bytes=5-9 ", 100},
		{"bytes=9223372036854775807-9223372036854775807", 100},
	}
	for _, s := range seeds {
		f.Add(s.spec, s.size)
	}

	f.Fuzz(func(t *testing.T, spec string, size int64) {
		size = nonNegative(size)
		offset, length, serr := parseCopyPartRange(spec, size)
		if serr != nil {
			return
		}
		if strings.TrimSpace(spec) == "" {
			if offset != 0 || length != size {
				t.Fatalf("空区间应表示整段：size=%d → (%d,%d)", size, offset, length)
			}
			return
		}
		if offset < 0 || length <= 0 || offset >= size || length > size-offset {
			t.Fatalf("复制区间越界：spec=%q size=%d → offset=%d length=%d",
				spec, size, offset, length)
		}

		explicit := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
		gotOffset, gotLength, gotErr := parseCopyPartRange(explicit, size)
		if gotErr != nil || gotOffset != offset || gotLength != length {
			t.Fatalf("结果不可逆：spec=%q size=%d → (%d,%d)，重写为 %q 得到 (%d,%d) err=%v",
				spec, size, offset, length, explicit, gotOffset, gotLength, gotErr)
		}
	})
}

// FuzzS3URLEncode 断言 encoding-type=url 的编码是**无损且规范**的：
// 输出只含 unreserved 字符、`/` 与合法的百分号转义，且反解码必须逐字节还原。
//
// 客户端会拿编码值与原始 key 做字符串比较，任何一字节偏差都是兼容性事故。
func FuzzS3URLEncode(f *testing.F) {
	for _, s := range []string{
		"",
		"a",
		"models/source.bin",
		"a b",
		"a+b&c,d",
		"中文/键",
		"100%~",
		"\x00\xff\x7f",
		"/",
		"//",
	} {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, s string) {
		enc := s3URLEncode(s)
		for i := 0; i < len(enc); i++ {
			c := enc[i]
			switch {
			case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
			case c == '-', c == '_', c == '.', c == '~', c == '/':
			case c == '%':
				if i+2 >= len(enc) || !isHexByte(enc[i+1]) || !isHexByte(enc[i+2]) {
					t.Fatalf("编码输出含不完整转义：input=%q output=%q 位置 %d", s, enc, i)
				}
				i += 2
			default:
				t.Fatalf("编码输出含未编码字节：input=%q output=%q 字节 0x%02x", s, enc, c)
			}
		}

		dec, err := url.PathUnescape(enc)
		if err != nil {
			t.Fatalf("编码输出无法解码：input=%q output=%q err=%v", s, enc, err)
		}
		if dec != s {
			t.Fatalf("编码不可逆：input=%q → %q → %q", s, enc, dec)
		}
	})
}

func isHexByte(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

// FuzzDecodeListToken 断言 continuation-token 的**可重签**性质：
// 任何能被解码的 token，用其内容重新编码后必须解码出完全相同的语义。
//
// 这条性质覆盖"客户端把 token 在标准/URL 安全字母表之间转换"的兼容路径，
// 同时保证解码器不会接受一个语义上无法表达的位置。
func FuzzDecodeListToken(f *testing.F) {
	seedCursor := store.ListCursor{After: "photos/2026.jpg", IsPrefix: false}
	for _, prefix := range []string{"", "photos/"} {
		f.Add(encodeListToken(seedCursor, prefix, "/"))
	}
	f.Add("")
	f.Add("!!!")
	f.Add(base64.RawURLEncoding.EncodeToString([]byte(`{"after":""}`)))

	f.Fuzz(func(t *testing.T, token string) {
		cursor, tokPrefix, tokDelimiter, err := decodeListToken(token)
		if err != nil {
			return
		}
		if cursor == nil || cursor.After == "" {
			t.Fatalf("解码成功却没有续页位置：token=%q", token)
		}

		resigned := encodeListToken(*cursor, tokPrefix, tokDelimiter)
		again, againPrefix, againDelimiter, err := decodeListToken(resigned)
		if err != nil {
			t.Fatalf("重签 token 无法解码：token=%q resigned=%q err=%v", token, resigned, err)
		}
		if again.After != cursor.After || again.IsPrefix != cursor.IsPrefix ||
			againPrefix != tokPrefix || againDelimiter != tokDelimiter {
			t.Fatalf("重签语义改变：token=%q resigned=%q", token, resigned)
		}
	})
}

// FuzzParseTrailerNames 校验 x-amz-trailer 解析结果：非空、小写、去重、
// 字符集受限，且 hasTrailerName 对每个返回名一致。
func FuzzParseTrailerNames(f *testing.F) {
	for _, v := range []string{
		"",
		"x-amz-checksum-crc32",
		"X-Amz-Checksum-CRC32,x-amz-checksum-crc64nvme",
		" x-amz-checksum-sha256 , x-amz-checksum-sha256 ",
		",,",
		"bad name",
		"x-amz-checksum-crc32:",
	} {
		f.Add(v)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		h := http.Header{}
		h.Set("x-amz-trailer", raw)

		names, serr := parseTrailerNames(h)
		if serr != nil {
			return
		}
		seen := make(map[string]bool, len(names))
		for _, n := range names {
			if n == "" || n != strings.ToLower(n) || !validTrailerName(n) {
				t.Fatalf("返回了非法 trailer 名：raw=%q name=%q", raw, n)
			}
			if seen[n] {
				t.Fatalf("返回了重复 trailer 名：raw=%q name=%q", raw, n)
			}
			seen[n] = true
			if !hasTrailerName(h, n) {
				t.Fatalf("hasTrailerName 与解析结果不一致：raw=%q name=%q", raw, n)
			}
		}
	})
}

// FuzzParseCopySource 断言成功解析的复制源可逆：把 (Bucket, Key) 重新
// 百分号编码拼接后，必须解析出完全相同的值。
//
// 这一条直接覆盖"在解码之前按第一个 `/` 切分"的关键语义 —— 若实现先解码
// 再切分，key 中的 `%2F` 就会把 bucket/key 边界挪位，本性质会立刻失败。
func FuzzParseCopySource(f *testing.F) {
	for _, v := range []string{
		"/bucket/key",
		"bucket/key",
		"/bucket/a%2Fb",
		"/bucket/a b",
		"/bucket/../key",
		"/bucket/key?versionId=1",
		"/bucket/",
		"/bucket",
		"//key",
		"?versionId=1",
	} {
		f.Add(v)
	}

	f.Fuzz(func(t *testing.T, v string) {
		src, serr := parseCopySource(v)
		if serr != nil {
			return
		}
		if src.Bucket == "" || src.Key == "" {
			t.Fatalf("解析成功却有空字段：input=%q → %+v", v, src)
		}
		// 注意：不做"bucket 不含斜杠"的断言。`%2F/0` 这类输入在切分后
		// 解码会得到 Bucket="/"，这是**解码阶段的正常结果**，由下游
		// store.validateBucketName 统一拒绝（400），不在协议解析层判死。

		rebuilt := "/" + url.PathEscape(src.Bucket) + "/" + url.PathEscape(src.Key)
		again, againErr := parseCopySource(rebuilt)
		if againErr != nil || again != src {
			t.Fatalf("复制源不可逆：input=%q → %+v，重写为 %q 得到 %+v err=%v",
				v, src, rebuilt, again, againErr)
		}
	})
}

// FuzzParseContentMD5 断言 Content-MD5 解析：成功时必须是 16 字节 base64
// 对应的 32 位小写十六进制串；失败时不得返回半成品。
func FuzzParseContentMD5(f *testing.F) {
	for _, v := range []string{
		"",
		"1B2M2Y8AsgTpgAmY7PhCfg==",
		"1B2M2Y8AsgTpgAmY7PhCfg",
		"AAAA",
		"not base64!!",
	} {
		f.Add(v)
	}

	f.Fuzz(func(t *testing.T, v string) {
		got, ok := parseContentMD5(v)
		if !ok {
			if got != "" {
				t.Fatalf("失败时返回值应为空：input=%q got=%q", v, got)
			}
			return
		}
		if v == "" {
			if got != "" {
				t.Fatalf("未声明时应返回空：got=%q", got)
			}
			return
		}

		raw, err := base64.StdEncoding.DecodeString(v)
		if err != nil || len(raw) != 16 {
			t.Fatalf("接受了不合法的 Content-MD5：input=%q got=%q", v, got)
		}
		if want := hex.EncodeToString(raw); got != want || got != strings.ToLower(got) || len(got) != 32 {
			t.Fatalf("解码结果不一致：input=%q got=%q want=%q", v, got, want)
		}
	})
}

// FuzzParseDecodedContentLength 断言 x-amz-decoded-content-length 的成功值
// 就是其十进制字面值（去空白后），且非负。
func FuzzParseDecodedContentLength(f *testing.F) {
	for _, v := range []string{"", "0", "1", " 42 ", "-1", "+7", "1.5", "9223372036854775807", "9223372036854775808"} {
		f.Add(v)
	}

	f.Fuzz(func(t *testing.T, v string) {
		n, serr := parseDecodedContentLength(v)
		if serr != nil {
			return
		}
		if n < 0 {
			t.Fatalf("成功解析出负数：input=%q → %d", v, n)
		}
		// 成功值必须与十进制字面值一致（允许前后空白，与 TrimSpace 对齐）。
		if want := strings.TrimSpace(v); want != "" {
			if parsed, err := strconv.ParseInt(want, 10, 64); err != nil || parsed != n {
				t.Fatalf("字面值不一致：input=%q → %d", v, n)
			}
		}
	})
}
