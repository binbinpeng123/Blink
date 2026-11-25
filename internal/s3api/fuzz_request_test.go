package s3api

import (
	"bytes"
	"encoding/xml"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"unicode"

	"github.com/binbinpeng123/Blink/internal/store"
)

// 本文件是"请求体 / 请求参数"解析的模糊测试基线，补齐 fuzz_test.go 未覆盖的
// 入口：CORS 配置文档、CORS 模式匹配、逗号分隔头列表、ListObjects 查询参数、
// 分片编号。这些函数同样直接吃客户端字节，但它们的失败模式不是"内存越界"，
// 而是"接受了不该接受的配置"或"拒绝了自己刚输出的内容"。
//
// 有界 fuzz（纯 CPU，不写磁盘、不联网）：
//
//	go test -run=^$ -fuzz=^FuzzDecodeCORSConfiguration$ -fuzztime=20s ./internal/s3api

// FuzzDecodeCORSConfiguration 校验 PutBucketCors 请求体的解析。
//
// 除规模与必填项外，最关键的断言是**可回写**：GET ?cors 会把这份配置重新编码
// 成 XML 返回。若解码接受的文档无法编码回去，或者编码后再次解码得到不同配置，
// 那就意味着"刚存进去的配置读不出来"，CORS 会在这个桶上静默失效。
func FuzzDecodeCORSConfiguration(f *testing.F) {
	valid := `<CORSConfiguration><CORSRule>` +
		`<AllowedOrigin>https://example.com</AllowedOrigin>` +
		`<AllowedMethod>GET</AllowedMethod>` +
		`</CORSRule></CORSConfiguration>`
	f.Add(valid)
	f.Add(`<?xml version="1.0" encoding="UTF-8"?>` + valid)
	f.Add("")
	f.Add("<CORSConfiguration></CORSConfiguration>")
	f.Add("<CORSConfiguration><CORSRule><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>")
	f.Add("<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>get</AllowedMethod><MaxAgeSeconds>0</MaxAgeSeconds></CORSRule></CORSConfiguration>")
	f.Add(`<CORSConfiguration><CORSRule><AllowedOrigin>a*b*c</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>`)
	f.Add(`<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod><AllowedHeader>a*b*c</AllowedHeader></CORSRule></CORSConfiguration>`)
	f.Add(`<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>PATCH</AllowedMethod></CORSRule></CORSConfiguration>`)
	f.Add(`<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod><MaxAgeSeconds>-1</MaxAgeSeconds></CORSRule></CORSConfiguration>`)
	f.Add(valid + "<CORSConfiguration></CORSConfiguration>") // 双根：只配置一半
	f.Add(valid + "trailing")
	f.Add(`<CORSConfiguration><CORSRule><AllowedOrigin>&#38;</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>`)
	// 不在这里放 64 KiB+1 的超限种子：fuzzer 会为一个巨大的"无意义覆盖"
	// 输入反复最小化，把整个时长耗成 0 execs/sec。超限行为由
	// TestBucketCORSValidationBodyLimit 确定性地覆盖。

	// 101 条规则：正好越过上限，验证是拒绝而不是静默截断。
	var many strings.Builder
	many.WriteString("<CORSConfiguration>")
	rule := "<CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule>"
	for i := 0; i < maxCORSRules+1; i++ {
		many.WriteString(rule)
	}
	many.WriteString("</CORSConfiguration>")
	f.Add(many.String())

	f.Fuzz(func(t *testing.T, doc string) {
		cfg, err := decodeCORSConfiguration(strings.NewReader(doc))
		if err != nil {
			if cfg != nil {
				t.Fatalf("解析失败却返回了配置：input=%q → %+v", doc, cfg)
			}
			return
		}

		if len(cfg.Rules) == 0 || len(cfg.Rules) > maxCORSRules {
			t.Fatalf("规则条数越界：input=%q → %d", doc, len(cfg.Rules))
		}
		for i, r := range cfg.Rules {
			if len(r.AllowedOrigins) == 0 || len(r.AllowedMethods) == 0 {
				t.Fatalf("规则 %d 缺必填项：input=%q → %+v", i, doc, r)
			}
			for _, o := range r.AllowedOrigins {
				if strings.Count(o, "*") > 1 {
					t.Fatalf("AllowedOrigin 含多个通配符：input=%q origin=%q", doc, o)
				}
			}
			for _, h := range r.AllowedHeaders {
				if strings.Count(h, "*") > 1 {
					t.Fatalf("AllowedHeader 含多个通配符：input=%q header=%q", doc, h)
				}
			}
			for _, m := range r.AllowedMethods {
				// 存下来的方法必须是归一化后的大写白名单成员；
				// 否则 Allow-Methods 响应头会与客户端期望的写法不一致。
				if m != strings.ToUpper(m) || !corsAllowedMethods[m] {
					t.Fatalf("规则 %d 含未归一化的方法：input=%q method=%q", i, doc, m)
				}
			}
			if r.MaxAgeSeconds != nil && *r.MaxAgeSeconds < 0 {
				t.Fatalf("规则 %d 的 MaxAgeSeconds 为负：input=%q → %d", i, doc, *r.MaxAgeSeconds)
			}
		}

		// 确定性：同样的字节必须解析出同样的配置。
		again, err2 := decodeCORSConfiguration(strings.NewReader(doc))
		if err2 != nil || !reflect.DeepEqual(again, cfg) {
			t.Fatalf("解析不确定：input=%q first=%+v second=%+v err=%v",
				doc, cfg, again, err2)
		}

		// 可回写：GET ?cors 的响应对这份配置做 XML 编码，客户端可能原样回传。
		data, merr := xml.Marshal(corsConfigToXML(cfg))
		if merr != nil {
			t.Fatalf("配置无法编码回 XML：input=%q cfg=%+v err=%v", doc, cfg, merr)
		}
		back, berr := decodeCORSConfiguration(bytes.NewReader(data))
		if berr != nil || !reflect.DeepEqual(back, cfg) {
			t.Fatalf("配置往返不闭合：input=%q 编码为 %q 后得到 %+v err=%v",
				doc, data, back, berr)
		}
	})
}

// FuzzCorsPatternMatches 校验 CORS 的单通配符匹配语义。
//
// 匹配结果直接决定浏览器能否跨源访问。这里断言与实现无关的性质：
// `*` 匹配一切；无通配符时等价于（可折叠的）全等；有通配符时匹配必然
// 意味着长度足够且首尾对齐，反向也成立 —— 首尾都对上却不匹配就是漏判。
func FuzzCorsPatternMatches(f *testing.F) {
	seeds := []struct {
		pattern string
		value   string
		fold    bool
	}{
		{"*", "https://a", false},
		{"GET", "GET", false},
		{"get", "GET", false},
		{"get", "GET", true},
		{"x-amz-*", "x-amz-date", true},
		{"*example.com", "https://example.com", false},
		{"a*b", "ab", false},
		{"a*b", "axxb", false},
		{"a*b", "a", false},
		{"", "", false},
		{"**", "x", false},
		{"中文*", "中文值", false},
	}
	for _, s := range seeds {
		f.Add(s.pattern, s.value, s.fold)
	}

	f.Fuzz(func(t *testing.T, pattern, value string, fold bool) {
		got := corsPatternMatches(pattern, value, fold)
		i := strings.IndexByte(pattern, '*')
		if i < 0 {
			want := pattern == value
			if fold {
				want = strings.EqualFold(pattern, value)
			}
			if got != want {
				t.Fatalf("无通配符时匹配语义错误：pattern=%q value=%q fold=%v got=%v want=%v",
					pattern, value, fold, got, want)
			}
			return
		}

		prefix, suffix := pattern[:i], pattern[i+1:]
		if got {
			if len(value) < len(prefix)+len(suffix) {
				t.Fatalf("短于前后缀的 value 被匹配：pattern=%q value=%q fold=%v", pattern, value, fold)
			}
			if !fold && (!strings.HasPrefix(value, prefix) || !strings.HasSuffix(value, suffix)) {
				t.Fatalf("首尾不匹配却报告匹配：pattern=%q value=%q", pattern, value)
			}
			return
		}
		// 反向：非折叠比较下，首尾都逐字节对上就必然匹配。
		if !fold && len(value) >= len(prefix)+len(suffix) &&
			strings.HasPrefix(value, prefix) && strings.HasSuffix(value, suffix) {
			t.Fatalf("首尾匹配却报告不匹配：pattern=%q value=%q", pattern, value)
		}
	})
}

// FuzzSplitHeaderList 校验逗号分隔列表的归一化：结果里不得有任何空项或
// 首尾空白，且拼接后再次拆分必须得到同一列表（幂等）。
//
// 这个函数用于 Access-Control-Request-Headers 与配置里的头名列表；
// 非幂等意味着预检与实际请求对同一份配置给出不同判断。
func FuzzSplitHeaderList(f *testing.F) {
	for _, raw := range []string{"", " ", "a", "a,b", " a , b ", ",,", "a,,b", "\t\n", "x-a,x-b , x-a"} {
		f.Add(raw)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		got := splitHeaderList(raw)
		for _, p := range got {
			if p == "" || strings.TrimSpace(p) != p {
				t.Fatalf("列表项未归一化：raw=%q items=%q", raw, got)
			}
		}
		if strings.TrimSpace(raw) == "" {
			if len(got) != 0 {
				t.Fatalf("空白输入应得到空列表：raw=%q items=%q", raw, got)
			}
			return
		}
		if len(got) == 0 {
			// 空列表只在"整串只有分隔符与任意空白"时合法（",," 与
			// ",　" 都是真值 —— 空白判定必须跟 TrimSpace 一样是全 Unicode 的）。
			if strings.TrimFunc(raw, func(r rune) bool { return r == ',' || unicode.IsSpace(r) }) != "" {
				t.Fatalf("含实际内容的输入却得到空列表：raw=%q", raw)
			}
			return
		}
		if again := splitHeaderList(strings.Join(got, ",")); !slices.Equal(again, got) {
			t.Fatalf("拆分不幂等：raw=%q first=%q second=%q", raw, got, again)
		}
	})
}

// FuzzParseListParams 校验 ListObjectsV2 查询参数解析。
//
// 两个最容易出错的点在这里被锁死：`max-keys` 只允许落在 0–1000（超出按上限
// 截断，负值/非数字拒绝），以及 continuation-token 必须与本次请求的
// prefix/delimiter 绑定 —— 用 A 前缀的 token 续 B 前缀会得到语义不明的页。
func FuzzParseListParams(f *testing.F) {
	token := encodeListToken(store.ListCursor{After: "photos/2026.jpg", IsPrefix: false}, "photos/", "/")
	for _, q := range []string{
		"",
		"list-type=2",
		"list-type=2&max-keys=10",
		"list-type=2&max-keys=5000",
		"list-type=2&max-keys=-1",
		"list-type=2&max-keys=abc",
		"list-type=2&max-keys=",
		"prefix=a%2Fb&delimiter=%2F",
		"prefix=%ZZ",
		"encoding-type=url",
		"encoding-type=URL",
		"fetch-owner=true",
		"fetch-owner=false",
		"fetch-owner=yes",
		"continuation-token=!!!",
		"continuation-token=",
		"prefix=photos%2F&delimiter=%2F&continuation-token=" + url.QueryEscape(token),
		"prefix=other%2F&delimiter=%2F&continuation-token=" + url.QueryEscape(token),
		"prefix=photos%2F&continuation-token=" + url.QueryEscape(token),
		"&&&=&=&&",
	} {
		f.Add(q)
	}

	f.Fuzz(func(t *testing.T, rawQuery string) {
		r := &http.Request{URL: &url.URL{RawQuery: rawQuery}}
		p, perr := parseListParams(r)
		if perr != nil {
			if p != nil {
				t.Fatalf("解析失败却返回了参数：query=%q → %+v", rawQuery, p)
			}
			return
		}

		if p.MaxKeys < 0 || p.MaxKeys > maxMaxKeys {
			t.Fatalf("max-keys 越界：query=%q → %d", rawQuery, p.MaxKeys)
		}
		if p.EncodingType != "" && p.EncodingType != "url" {
			t.Fatalf("encoding-type 非法：query=%q → %q", rawQuery, p.EncodingType)
		}
		if (p.ContinuationToken == "") != (p.Cursor == nil) {
			t.Fatalf("token 与游标不一致：query=%q token=%q cursor=%+v",
				rawQuery, p.ContinuationToken, p.Cursor)
		}
		if p.Cursor != nil {
			if p.Cursor.After == "" {
				t.Fatalf("游标没有续页位置：query=%q", rawQuery)
			}
			cursor, tokPrefix, tokDelimiter, err := decodeListToken(p.ContinuationToken)
			if err != nil {
				t.Fatalf("接受的 token 无法解码：query=%q token=%q err=%v",
					rawQuery, p.ContinuationToken, err)
			}
			if cursor.After != p.Cursor.After || cursor.IsPrefix != p.Cursor.IsPrefix {
				t.Fatalf("游标与 token 内容不符：query=%q cursor=%+v token=%+v",
					rawQuery, p.Cursor, cursor)
			}
			if tokPrefix != p.Prefix || tokDelimiter != p.Delimiter {
				t.Fatalf("token 未与 prefix/delimiter 绑定：query=%q prefix=%q delimiter=%q tokenPrefix=%q tokenDelimiter=%q",
					rawQuery, p.Prefix, p.Delimiter, tokPrefix, tokDelimiter)
			}
		}
		if !r.URL.Query().Has("max-keys") && p.MaxKeys != defaultMaxKeys {
			t.Fatalf("缺省 max-keys 应为 %d：query=%q → %d", defaultMaxKeys, rawQuery, p.MaxKeys)
		}

		// 确定性：同一查询串两次解析必须一致。
		p2, perr2 := parseListParams(r)
		if perr2 != nil || !reflect.DeepEqual(p2, p) {
			t.Fatalf("解析不确定：query=%q first=%+v second=%+v err=%v", rawQuery, p, p2, perr2)
		}
	})
}

// FuzzParsePartNumber 校验分片编号：成功值必须在 1–10000，且与十进制字面值一致。
func FuzzParsePartNumber(f *testing.F) {
	for _, v := range []string{
		"1", "2", "10000", "10001", "0", "-1", "", "abc", " 5", "5 ", "+5", "05",
		"1.0", "9223372036854775807", "9223372036854775808",
	} {
		f.Add(v)
	}

	f.Fuzz(func(t *testing.T, v string) {
		n, perr := parsePartNumber(v)
		if perr != nil {
			if n != 0 {
				t.Fatalf("解析失败却返回了编号：input=%q → %d", v, n)
			}
			return
		}
		if n < 1 || n > maxPartNumber {
			t.Fatalf("分片编号越界：input=%q → %d", v, n)
		}
		// 本实现用 Atoi，成功就必须与字面值相等（先取反再比较，避开 MinInt）。
		want, err := strconv.Atoi(v)
		if err != nil || want != n {
			t.Fatalf("与十进制字面值不一致：input=%q → %d want=%d err=%v", v, n, want, err)
		}
		back, perr2 := parsePartNumber(strconv.Itoa(n))
		if perr2 != nil || back != n {
			t.Fatalf("编号不可逆：input=%q → %d 重写后得到 (%d,%v)", v, n, back, perr2)
		}
	})
}
