package s3api

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/binbinpeng123/Blink/internal/s3err"
)

// listTarget 构造 ListObjectsV2 的请求目标。
//
// 统一走 url.Values 编码而不是手工拼串：分页测试要原样回传
// continuation-token，任何一处漏编码都会让测试掩盖真实的客户端问题。
func listTarget(bucket string, q url.Values) string {
	v := url.Values{"list-type": {"2"}}
	for k, vals := range q {
		for _, val := range vals {
			v.Add(k, val)
		}
	}
	return "/" + bucket + "?" + v.Encode()
}

// decodeList 发一次 LIST 请求并解析响应。
func decodeList(t *testing.T, h http.Handler, target string) (int, listBucketResult) {
	t.Helper()
	rec := do(t, h, http.MethodGet, target)
	var body listBucketResult
	if rec.Code == http.StatusOK {
		if err := xml.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("响应不是合法 XML: %v (%s)", err, rec.Body.String())
		}
	}
	return rec.Code, body
}

// listAllPages 反复翻页直到 IsTruncated=false，按页内顺序收集条目。
//
// 每页内部是"先 Contents、后 CommonPrefixes"—— 与客户端解析结果一致；
// 只有把跨页边界也拼起来比对，才能发现"某条在两页交界处丢了/重了"。
func listAllPages(t *testing.T, h http.Handler, bucket string, q url.Values) (entries []string, pages int) {
	t.Helper()
	for {
		code, body := decodeList(t, h, listTarget(bucket, q))
		if code != http.StatusOK {
			t.Fatalf("状态码 = %d，期望 200", code)
		}
		pages++
		if body.KeyCount != len(body.Contents)+len(body.CommonPrefixes) {
			t.Errorf("KeyCount = %d，但 Contents=%d、CommonPrefixes=%d",
				body.KeyCount, len(body.Contents), len(body.CommonPrefixes))
		}
		for _, c := range body.Contents {
			entries = append(entries, c.Key)
		}
		for _, p := range body.CommonPrefixes {
			entries = append(entries, p.Prefix)
		}
		if !body.IsTruncated {
			if body.NextContinuationToken != "" {
				t.Error("未截断却返回了 NextContinuationToken")
			}
			return entries, pages
		}
		if body.NextContinuationToken == "" {
			t.Fatal("IsTruncated 为真却没有 NextContinuationToken")
		}
		q.Set("continuation-token", body.NextContinuationToken)
		if pages > 100 {
			t.Fatal("翻页次数异常，疑似死循环")
		}
	}
}

// escapedPath 把含非 ASCII / 空格 / 特殊字符的 key 编成合法的请求路径。
//
// url.URL{Path: ...}.EscapedPath() 只转义必要的字节，保留 "/" 作为
// key 内的分隔符 —— 这正是 S3 客户端对 key 的处理方式。
func escapedPath(p string) string {
	return (&url.URL{Path: p}).EscapedPath()
}

func TestListObjectsV2EmptyBucket(t *testing.T) {
	h := objectTestServer(t)

	code, body := decodeList(t, h, "/bkt?list-type=2")
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", code)
	}
	if body.Name != "bkt" {
		t.Errorf("Name = %q，期望 bkt", body.Name)
	}
	if body.KeyCount != 0 || len(body.Contents) != 0 || len(body.CommonPrefixes) != 0 {
		t.Errorf("空桶结果 = %+v，期望空", body)
	}
	if body.IsTruncated {
		t.Error("空桶不应截断")
	}
	// 未指定 max-keys 时必须是 1000，而不是 0 或空值 ——
	// SDK 依赖这个字段判断"服务端接受的上限"。
	if body.MaxKeys != defaultMaxKeys {
		t.Errorf("MaxKeys = %d，期望 %d", body.MaxKeys, defaultMaxKeys)
	}
}

func TestListObjectsV2BucketNotFound(t *testing.T) {
	h := objectTestServer(t)

	rec := do(t, h, http.MethodGet, "/nope?list-type=2")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("状态码 = %d，期望 404", rec.Code)
	}
	if got := decodeError(t, rec).Code; got != string(s3err.CodeNoSuchBucket) {
		t.Errorf("Code = %q，期望 NoSuchBucket", got)
	}
}

func TestListObjectsV2EntryFields(t *testing.T) {
	h := objectTestServer(t)
	const body = "hello"
	putObject(t, h, "/bkt/greeting.txt", body, nil)

	code, res := decodeList(t, h, "/bkt?list-type=2")
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", code)
	}
	if len(res.Contents) != 1 {
		t.Fatalf("条目数 = %d，期望 1", len(res.Contents))
	}
	c := res.Contents[0]
	if c.Key != "greeting.txt" || c.Size != int64(len(body)) {
		t.Errorf("条目 = %+v", c)
	}
	// ETag 必须带引号，与 GET/HEAD 的 ETag 逐字节一致。
	if want := `"` + md5Hex(body) + `"`; c.ETag != want {
		t.Errorf("ETag = %s，期望 %s", c.ETag, want)
	}
	if c.StorageClass != storageClassStandard {
		t.Errorf("StorageClass = %q，期望 %q", c.StorageClass, storageClassStandard)
	}
	if _, err := time.Parse("2006-01-02T15:04:05.000Z", c.LastModified); err != nil {
		t.Errorf("LastModified = %q 不是 S3 时间格式: %v", c.LastModified, err)
	}
	// 默认不返回 Owner：它是可选字段，多传会无谓地放大响应体。
	if c.Owner != nil {
		t.Errorf("默认不应返回 Owner，实际 %+v", c.Owner)
	}
}

// TestListObjectsV2PaginationNoLossNoDup 是切片 6 的验收条件：
// --max-keys 2 翻到底，条目集合与一次性列举完全一致，无丢无重。
func TestListObjectsV2PaginationNoLossNoDup(t *testing.T) {
	keys := []string{
		"a.txt",
		"b/1", "b/2", "b/sub/3",
		"c",
		"logs/2026/01.log", "logs/2026/02.log", "logs/2026/03.log",
		"z.txt",
	}

	cases := []struct {
		name string
		q    url.Values
		want []string
	}{
		{
			name: "无 delimiter 全量",
			q:    url.Values{"max-keys": {"2"}},
			want: keys,
		},
		{
			// 折叠后条目：a.txt / b/ / c / logs/ / z.txt，每页 2 条 → 3 页。
			name: "delimiter 折叠",
			q:    url.Values{"max-keys": {"2"}, "delimiter": {"/"}},
			want: []string{"a.txt", "b/", "c", "logs/", "z.txt"},
		},
		{
			name: "prefix + delimiter",
			q:    url.Values{"max-keys": {"2"}, "prefix": {"logs/"}, "delimiter": {"/"}},
			want: []string{"logs/2026/"},
		},
		{
			name: "prefix 内逐个翻页",
			q:    url.Values{"max-keys": {"2"}, "prefix": {"logs/"}},
			want: []string{"logs/2026/01.log", "logs/2026/02.log", "logs/2026/03.log"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := objectTestServer(t)
			for _, k := range keys {
				rec := putObject(t, h, escapedPath("/bkt/"+k), "x", nil)
				if rec.Code != http.StatusOK {
					t.Fatalf("PUT %q 状态码 = %d (%s)", k, rec.Code, rec.Body.String())
				}
			}

			got, pages := listAllPages(t, h, "bkt", c.q)
			if strings.Join(got, "\x00") != strings.Join(c.want, "\x00") {
				t.Errorf("翻页结果 = %v，期望 %v", got, c.want)
			}
			seen := make(map[string]bool, len(got))
			for _, e := range got {
				if seen[e] {
					t.Errorf("条目 %q 重复出现", e)
				}
				seen[e] = true
			}
			wantPages := (len(c.want) + 1) / 2
			if pages != wantPages {
				t.Errorf("页数 = %d，期望 %d", pages, wantPages)
			}
		})
	}
}

func TestListObjectsV2ContinuationTokenIsOpaque(t *testing.T) {
	h := objectTestServer(t)
	for _, k := range []string{"a", "b", "c"} {
		putObject(t, h, escapedPath("/bkt/"+k), "x", nil)
	}

	code, body := decodeList(t, h, "/bkt?list-type=2&max-keys=2")
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", code)
	}
	if !body.IsTruncated || body.NextContinuationToken == "" {
		t.Fatalf("期望截断并返回 token，实际 %+v", body)
	}
	// token 必须能放进查询串原样回传（不含需要额外编码的字符）。
	// 用 RawURL 字母表就是为了这一点：标准 base64 的 "+" 在查询串里
	// 会被解码成空格，续页永远失败。
	if strings.ContainsAny(body.NextContinuationToken, "+/=") {
		t.Errorf("token 含查询串敏感字符: %q", body.NextContinuationToken)
	}
	raw, err := base64.RawURLEncoding.DecodeString(body.NextContinuationToken)
	if err != nil {
		t.Fatalf("token 不是 RawURLEncoding base64: %v", err)
	}
	var tok listToken
	if err := json.Unmarshal(raw, &tok); err != nil {
		t.Fatalf("token 明文不是 JSON: %v", err)
	}
	if tok.After != "b" {
		t.Errorf("token.After = %q，期望 b", tok.After)
	}
}

func TestListObjectsV2InvalidToken(t *testing.T) {
	h := objectTestServer(t)
	putObject(t, h, "/bkt/a", "x", nil)

	cases := []struct {
		name string
		q    url.Values
	}{
		{"不是 base64", url.Values{"continuation-token": {"!!!not-base64!!!"}}},
		{"不是 JSON", url.Values{"continuation-token": {base64.RawURLEncoding.EncodeToString([]byte("plain"))}}},
		{"缺少续页位置", url.Values{"continuation-token": {base64.RawURLEncoding.EncodeToString([]byte(`{"prefix":""}`))}}},
		{"prefix 不一致", url.Values{
			"prefix":             {"dir/"},
			"continuation-token": {base64.RawURLEncoding.EncodeToString([]byte(`{"after":"dir/a","prefix":"other/"}`))},
		}},
		{"delimiter 不一致", url.Values{
			"continuation-token": {base64.RawURLEncoding.EncodeToString([]byte(`{"after":"a","delimiter":"/"}`))},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := do(t, h, http.MethodGet, listTarget("bkt", c.q))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("状态码 = %d，期望 400 (%s)", rec.Code, rec.Body.String())
			}
			if got := decodeError(t, rec).Code; got != string(s3err.CodeInvalidArgument) {
				t.Errorf("Code = %q，期望 InvalidArgument", got)
			}
		})
	}
}

// TestListObjectsV2TokenOverridesStartAfter 验证 S3 的优先级：
// continuation-token 存在时 start-after 被忽略。
func TestListObjectsV2TokenOverridesStartAfter(t *testing.T) {
	h := objectTestServer(t)
	for _, k := range []string{"a", "b", "c", "d"} {
		putObject(t, h, escapedPath("/bkt/"+k), "x", nil)
	}

	_, first := decodeList(t, h, "/bkt?list-type=2&max-keys=2")
	if first.NextContinuationToken == "" {
		t.Fatal("第一页应返回 token")
	}

	// start-after=a 本应从 b 开始；token 指向 b 之后，结果应只剩 c、d。
	q := url.Values{
		"max-keys":           {"2"},
		"start-after":        {"a"},
		"continuation-token": {first.NextContinuationToken},
	}
	code, second := decodeList(t, h, listTarget("bkt", q))
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", code)
	}
	var keys []string
	for _, c := range second.Contents {
		keys = append(keys, c.Key)
	}
	if strings.Join(keys, ",") != "c,d" {
		t.Errorf("第二页 = %v，期望 [c d]（token 优先于 start-after）", keys)
	}
}

func TestListObjectsV2StartAfter(t *testing.T) {
	h := objectTestServer(t)
	for _, k := range []string{"a", "b", "c"} {
		putObject(t, h, escapedPath("/bkt/"+k), "x", nil)
	}

	code, body := decodeList(t, h, "/bkt?list-type=2&start-after=b")
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", code)
	}
	if len(body.Contents) != 1 || body.Contents[0].Key != "c" {
		t.Errorf("start-after=b 结果 = %+v，期望只有 c", body.Contents)
	}
	if body.StartAfter != "b" {
		t.Errorf("StartAfter = %q，期望回显 b", body.StartAfter)
	}
}

// TestListObjectsV2EncodingTypeURL 验证 encoding-type=url。
//
// 这一项不做的话，含空格、中文或 "&" 的 key 在 XML 里会原样出现，
// 部分客户端（尤其是把 XML 再当查询串用的）会解析出错。
func TestListObjectsV2EncodingTypeURL(t *testing.T) {
	h := objectTestServer(t)
	keys := []string{"文档/报告 1.txt", "a+b&c=d.txt", "100%.txt"}
	for _, k := range keys {
		rec := putObject(t, h, escapedPath("/bkt/"+k), "x", nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("PUT %q 状态码 = %d (%s)", k, rec.Code, rec.Body.String())
		}
	}

	q := url.Values{"encoding-type": {"url"}, "prefix": {"文档/"}, "delimiter": {"/"}}
	code, body := decodeList(t, h, listTarget("bkt", q))
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", code)
	}
	if body.EncodingType != "url" {
		t.Errorf("EncodingType = %q，期望 url", body.EncodingType)
	}
	if want := s3URLEncode("文档/"); body.Prefix != want {
		t.Errorf("Prefix = %q，期望 %q", body.Prefix, want)
	}
	if len(body.Contents) != 1 {
		t.Fatalf("Contents = %+v，期望 1 条", body.Contents)
	}
	if want := s3URLEncode("文档/报告 1.txt"); body.Contents[0].Key != want {
		t.Errorf("Key = %q，期望 %q", body.Contents[0].Key, want)
	}
	// 空格必须编成 %20 而不是 "+"：XML 值里 "+" 就是字面加号。
	if strings.Contains(body.Contents[0].Key, "+") {
		t.Errorf("空格被编码成了 +: %q", body.Contents[0].Key)
	}

	// 不带 encoding-type 时原样输出，便于核对上面确实发生了编码。
	_, plain := decodeList(t, h, "/bkt?list-type=2&prefix="+url.QueryEscape("文档/"))
	if plain.Prefix != "文档/" || plain.Contents[0].Key != "文档/报告 1.txt" {
		t.Errorf("未编码时 Prefix/Key = %q/%q，期望原样", plain.Prefix, plain.Contents[0].Key)
	}
}

// TestS3URLEncodeKeepsSlash 固化 encoding-type=url 的字符集。
//
// `/` 必须原样保留：AWS CLI 与各 SDK 会把响应里的 Key 直接和本地记录的
// key 做字符串比较，`models%2Fsource.bin` 会判成"不存在"。其余非
// unreserved 字符（含 `+`、`&`、`%`、空格、非 ASCII）逐字节百分号编码。
func TestS3URLEncodeKeepsSlash(t *testing.T) {
	cases := []struct{ in, want string }{
		{"models/source.bin", "models/source.bin"},
		{"a/b/c", "a/b/c"},
		{"报告 1.txt", "%E6%8A%A5%E5%91%8A%201.txt"},
		{"a+b&c=d.txt", "a%2Bb%26c%3Dd.txt"},
		{"100%.txt", "100%25.txt"},
		{"-_.~/", "-_.~/"},
	}
	for _, c := range cases {
		if got := s3URLEncode(c.in); got != c.want {
			t.Errorf("s3URLEncode(%q) = %q，期望 %q", c.in, got, c.want)
		}
	}
}

func TestListObjectsV2FetchOwner(t *testing.T) {
	h := objectTestServer(t)
	putObject(t, h, "/bkt/a", "x", nil)

	code, body := decodeList(t, h, "/bkt?list-type=2&fetch-owner=true")
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", code)
	}
	if len(body.Contents) != 1 || body.Contents[0].Owner == nil {
		t.Fatalf("fetch-owner=true 应返回 Owner，实际 %+v", body.Contents)
	}
	o := body.Contents[0].Owner
	if o.ID == "" || o.DisplayName == "" {
		t.Errorf("Owner 字段不完整: %+v", o)
	}
}

func TestListObjectsV2ParameterValidation(t *testing.T) {
	h := objectTestServer(t)

	for _, c := range []struct {
		name   string
		target string
	}{
		{"max-keys 非整数", "/bkt?list-type=2&max-keys=abc"},
		{"max-keys 为负", "/bkt?list-type=2&max-keys=-1"},
		{"encoding-type 不支持", "/bkt?list-type=2&encoding-type=base64"},
		{"fetch-owner 非布尔", "/bkt?list-type=2&fetch-owner=yes"},
		{"list-type 非法", "/bkt?list-type=3"},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec := do(t, h, http.MethodGet, c.target)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("状态码 = %d，期望 400 (%s)", rec.Code, rec.Body.String())
			}
			if got := decodeError(t, rec).Code; got != string(s3err.CodeInvalidArgument) {
				t.Errorf("Code = %q，期望 InvalidArgument", got)
			}
		})
	}

	// 超出上限按上限截断而不是报错：--max-keys 5000 的意图就是"尽量多"。
	code, body := decodeList(t, h, "/bkt?list-type=2&max-keys=5000")
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", code)
	}
	if body.MaxKeys != maxMaxKeys {
		t.Errorf("MaxKeys = %d，期望截断到 %d", body.MaxKeys, maxMaxKeys)
	}
}

// TestListObjectsV2IgnoresUnknownParams 验证 SDK 自带的标记参数不会让请求失败。
//
// aws-sdk-js v3 每次请求都带 x-id=ListObjectsV2；把它当"不支持的功能"
// 会让整个 SDK 不可用。02 文档的"未知参数 → 501"针对的是子资源。
func TestListObjectsV2IgnoresUnknownParams(t *testing.T) {
	h := objectTestServer(t)

	rec := do(t, h, http.MethodGet, "/bkt?list-type=2&x-id=ListObjectsV2&foo=bar")
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200 (%s)", rec.Code, rec.Body.String())
	}
}

// v1Target 构造 ListObjects V1 的请求目标；listType 为空表示完全不带
// list-type 参数（老客户端就是这样请求的）。
func v1Target(bucket string, q url.Values, listType string) string {
	v := url.Values{}
	for k, vals := range q {
		for _, val := range vals {
			v.Add(k, val)
		}
	}
	if listType != "" {
		v.Set("list-type", listType)
	}
	if len(v) == 0 {
		return "/" + bucket
	}
	return "/" + bucket + "?" + v.Encode()
}

// decodeListV1 发一次 V1 请求并解析响应。
func decodeListV1(t *testing.T, h http.Handler, target string) (int, listBucketResultV1) {
	t.Helper()
	rec := do(t, h, http.MethodGet, target)
	var body listBucketResultV1
	if rec.Code == http.StatusOK {
		if err := xml.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("响应不是合法 XML: %v (%s)", err, rec.Body.String())
		}
	}
	return rec.Code, body
}

// listAllPagesV1 按 V1 的续页规则翻到底。
//
// 无 delimiter 时客户端用最后一条 Contents 的 Key 当 marker；
// 有 delimiter 时服务端必须回 NextMarker —— 两种情况都覆盖。
func listAllPagesV1(t *testing.T, h http.Handler, bucket string, q url.Values, listType string) (entries []string, pages int) {
	t.Helper()
	q = url.Values(q)
	for {
		code, body := decodeListV1(t, h, v1Target(bucket, q, listType))
		if code != http.StatusOK {
			t.Fatalf("状态码 = %d，期望 200", code)
		}
		pages++
		for _, c := range body.Contents {
			entries = append(entries, c.Key)
		}
		for _, p := range body.CommonPrefixes {
			entries = append(entries, p.Prefix)
		}
		if !body.IsTruncated {
			if body.NextMarker != "" {
				t.Error("未截断却返回了 NextMarker")
			}
			return entries, pages
		}
		if q.Get("delimiter") != "" {
			if body.NextMarker == "" {
				t.Fatal("带 delimiter 且截断时必须返回 NextMarker")
			}
			q.Set("marker", body.NextMarker)
		} else {
			if len(body.Contents) == 0 {
				t.Fatal("无 delimiter 且截断时，页内至少应有一条 Contents")
			}
			q.Set("marker", body.Contents[len(body.Contents)-1].Key)
		}
		if pages > 100 {
			t.Fatal("翻页次数异常，疑似死循环")
		}
	}
}

// TestListObjectsV1Basic 验证不带 list-type 与 list-type=1 的等价性。
func TestListObjectsV1Basic(t *testing.T) {
	keys := []string{"a.txt", "b/1", "c"}
	for _, listType := range []string{"", "1"} {
		t.Run("list-type="+listType, func(t *testing.T) {
			h := objectTestServer(t)
			for _, k := range keys {
				if rec := putObject(t, h, escapedPath("/bkt/"+k), "x", nil); rec.Code != http.StatusOK {
					t.Fatalf("PUT %q 状态码 = %d", k, rec.Code)
				}
			}

			code, body := decodeListV1(t, h, v1Target("bkt", nil, listType))
			if code != http.StatusOK {
				t.Fatalf("状态码 = %d，期望 200", code)
			}
			if body.Name != "bkt" {
				t.Errorf("Name = %q，期望 bkt", body.Name)
			}
			if body.MaxKeys != defaultMaxKeys {
				t.Errorf("MaxKeys = %d，期望 %d", body.MaxKeys, defaultMaxKeys)
			}
			if body.Marker != "" {
				t.Errorf("未传 marker 时 Marker = %q，期望空", body.Marker)
			}
			if body.IsTruncated || body.NextMarker != "" {
				t.Errorf("3 条 key 不应截断: truncated=%v next=%q", body.IsTruncated, body.NextMarker)
			}
			var got []string
			for _, c := range body.Contents {
				got = append(got, c.Key)
				if want := `"` + md5Hex("x") + `"`; c.ETag != want {
					t.Errorf("ETag = %s，期望 %s", c.ETag, want)
				}
			}
			if strings.Join(got, ",") != strings.Join(keys, ",") {
				t.Errorf("列举结果 = %v，期望 %v", got, keys)
			}
		})
	}
}

// TestListObjectsV1PaginationNoLossNoDup 是 V1 的翻页验收：
// 无 delimiter 时靠最后一条 Key 续页，有 delimiter 时靠 NextMarker。
func TestListObjectsV1PaginationNoLossNoDup(t *testing.T) {
	keys := []string{
		"a.txt",
		"b/1", "b/2", "b/sub/3",
		"c",
		"logs/2026/01.log", "logs/2026/02.log", "logs/2026/03.log",
		"z.txt",
	}
	cases := []struct {
		name     string
		q        url.Values
		want     []string
		wantPage int
	}{
		{
			name:     "无 delimiter 全量",
			q:        url.Values{"max-keys": {"2"}},
			want:     keys,
			wantPage: 5,
		},
		{
			name:     "delimiter 折叠",
			q:        url.Values{"max-keys": {"2"}, "delimiter": {"/"}},
			want:     []string{"a.txt", "b/", "c", "logs/", "z.txt"},
			wantPage: 3,
		},
		{
			name:     "prefix + delimiter",
			q:        url.Values{"max-keys": {"2"}, "prefix": {"logs/"}, "delimiter": {"/"}},
			want:     []string{"logs/2026/"},
			wantPage: 1,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := objectTestServer(t)
			for _, k := range keys {
				if rec := putObject(t, h, escapedPath("/bkt/"+k), "x", nil); rec.Code != http.StatusOK {
					t.Fatalf("PUT %q 状态码 = %d (%s)", k, rec.Code, rec.Body.String())
				}
			}

			got, pages := listAllPagesV1(t, h, "bkt", c.q, "")
			if strings.Join(got, "\x00") != strings.Join(c.want, "\x00") {
				t.Errorf("翻页结果 = %v，期望 %v", got, c.want)
			}
			if pages != c.wantPage {
				t.Errorf("页数 = %d，期望 %d", pages, c.wantPage)
			}
			seen := make(map[string]bool, len(got))
			for _, e := range got {
				if seen[e] {
					t.Errorf("条目 %q 重复出现", e)
				}
				seen[e] = true
			}
		})
	}
}

// TestListObjectsV1Marker 覆盖手工 marker 的两类语义。
func TestListObjectsV1Marker(t *testing.T) {
	h := objectTestServer(t)
	for _, k := range []string{"a", "b", "b/1", "b/2", "c"} {
		putObject(t, h, escapedPath("/bkt/"+k), "x", nil)
	}

	t.Run("普通 marker 跳过自身", func(t *testing.T) {
		code, body := decodeListV1(t, h, v1Target("bkt", url.Values{"marker": {"b"}}, "1"))
		if code != http.StatusOK {
			t.Fatalf("状态码 = %d，期望 200", code)
		}
		var got []string
		for _, c := range body.Contents {
			got = append(got, c.Key)
		}
		if want := []string{"b/1", "b/2", "c"}; strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("结果 = %v，期望 %v", got, want)
		}
	})

	t.Run("以 delimiter 结尾的 marker 是公共前缀游标", func(t *testing.T) {
		code, body := decodeListV1(t, h, v1Target("bkt", url.Values{
			"marker":    {"b/"},
			"delimiter": {"/"},
		}, "1"))
		if code != http.StatusOK {
			t.Fatalf("状态码 = %d，期望 200", code)
		}
		if len(body.CommonPrefixes) != 0 {
			t.Errorf("b/ 之后的下一页不应再出现 b/ 前缀: %+v", body.CommonPrefixes)
		}
		var got []string
		for _, c := range body.Contents {
			got = append(got, c.Key)
		}
		if want := []string{"c"}; strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("结果 = %v，期望 %v", got, want)
		}
	})
}

// TestListObjectsV1EncodingType 验证 encoding-type=url 对 Key 与 Marker 的编码。
func TestListObjectsV1EncodingType(t *testing.T) {
	h := objectTestServer(t)
	keys := []string{"a b", "模型.bin"}
	for _, k := range keys {
		if rec := putObject(t, h, escapedPath("/bkt/"+k), "x", nil); rec.Code != http.StatusOK {
			t.Fatalf("PUT %q 状态码 = %d (%s)", k, rec.Code, rec.Body.String())
		}
	}

	code, body := decodeListV1(t, h, v1Target("bkt", url.Values{
		"encoding-type": {"url"},
		"max-keys":      {"1"},
	}, "1"))
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", code)
	}
	if body.EncodingType != "url" {
		t.Errorf("EncodingType = %q，期望 url", body.EncodingType)
	}
	if len(body.Contents) != 1 || body.Contents[0].Key != "a%20b" {
		t.Fatalf("首条 Key = %+v，期望编码后的 a%%20b", body.Contents)
	}
	// 无 delimiter 时 S3 不回 NextMarker：客户端拿最后一条 Contents 的 Key
	// 当下一页的 marker。_encoding-type=url_ 下这个值也是编码过的，客户端
	// 必须解码后才能回传 —— 这里就按客户端的做法走一遍。
	if !body.IsTruncated {
		t.Fatal("max-keys=1 且有 2 条 key，应当截断")
	}
	if body.NextMarker != "" {
		t.Errorf("无 delimiter 时 NextMarker = %q，期望为空", body.NextMarker)
	}
	marker, err := url.PathUnescape(body.Contents[0].Key)
	if err != nil {
		t.Fatalf("解码 Key %q 失败: %v", body.Contents[0].Key, err)
	}

	code, body = decodeListV1(t, h, v1Target("bkt", url.Values{
		"encoding-type": {"url"},
		"marker":        {marker},
	}, "1"))
	if code != http.StatusOK {
		t.Fatalf("第二页状态码 = %d，期望 200", code)
	}
	if want := s3URLEncode("模型.bin"); len(body.Contents) != 1 || body.Contents[0].Key != want {
		t.Fatalf("第二页 Contents = %+v，期望 %s", body.Contents, want)
	}
	// 回显的 Marker 与 Key 一样按 encoding-type 编码。
	if got := body.Marker; got != "a%20b" {
		t.Errorf("回显 Marker = %q，期望编码后的 a%%20b", got)
	}
}

// TestListObjectsV1UnencodedKeyRoundTrip 验证未带 encoding-type 时 Key 原样返回。
func TestListObjectsV1UnencodedKeyRoundTrip(t *testing.T) {
	h := objectTestServer(t)
	const key = "模型 v1.bin"
	putObject(t, h, escapedPath("/bkt/"+key), "x", nil)

	code, body := decodeListV1(t, h, v1Target("bkt", nil, "1"))
	if code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", code)
	}
	if len(body.Contents) != 1 || body.Contents[0].Key != key {
		t.Fatalf("Key = %+v，期望原样返回 %q", body.Contents, key)
	}
}

// TestListObjectsV1ParamValidation 验证 V1 与 V2 对同一参数给同一答复。
func TestListObjectsV1ParamValidation(t *testing.T) {
	h := objectTestServer(t)

	t.Run("非法 max-keys", func(t *testing.T) {
		rec := do(t, h, http.MethodGet, v1Target("bkt", url.Values{"max-keys": {"abc"}}, "1"))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("状态码 = %d，期望 400", rec.Code)
		}
		if got := decodeError(t, rec).Code; got != string(s3err.CodeInvalidArgument) {
			t.Errorf("Code = %q，期望 InvalidArgument", got)
		}
	})

	t.Run("max-keys 超上限按上限截断", func(t *testing.T) {
		code, body := decodeListV1(t, h, v1Target("bkt", url.Values{"max-keys": {"5000"}}, "1"))
		if code != http.StatusOK {
			t.Fatalf("状态码 = %d，期望 200", code)
		}
		if body.MaxKeys != maxMaxKeys {
			t.Errorf("MaxKeys = %d，期望 %d", body.MaxKeys, maxMaxKeys)
		}
	})

	t.Run("非法 encoding-type", func(t *testing.T) {
		rec := do(t, h, http.MethodGet, v1Target("bkt", url.Values{"encoding-type": {"base64"}}, "1"))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("状态码 = %d，期望 400", rec.Code)
		}
	})

	t.Run("桶不存在", func(t *testing.T) {
		rec := do(t, h, http.MethodGet, "/nope")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("状态码 = %d，期望 404", rec.Code)
		}
		if got := decodeError(t, rec).Code; got != string(s3err.CodeNoSuchBucket) {
			t.Errorf("Code = %q，期望 NoSuchBucket", got)
		}
	})

	t.Run("非法 list-type", func(t *testing.T) {
		rec := do(t, h, http.MethodGet, "/bkt?list-type=3")
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("状态码 = %d，期望 400", rec.Code)
		}
	})
}
