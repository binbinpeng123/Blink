package sigv4

import (
	"net/http"
	"net/url"
	"sort"
	"strings"
)

// CanonicalRequest 是规范化请求的各个组成部分，保留分项便于测试与排查。
type CanonicalRequest struct {
	Method        string
	URI           string
	Query         string
	Headers       string
	SignedHeaders string
	PayloadHash   string
}

// String 按协议把各分项拼接为最终文本。
func (c CanonicalRequest) String() string {
	return strings.Join([]string{
		c.Method,
		c.URI,
		c.Query,
		c.Headers,
		c.SignedHeaders,
		c.PayloadHash,
	}, "\n")
}

// Hash 返回规范化请求的 SHA256 十六进制值（即 StringToSign 的第 4 行）。
func (c CanonicalRequest) Hash() string {
	return sha256Hex([]byte(c.String()))
}

// canonicalURI 计算 CanonicalURI。
//
// **易错点 ①**：必须使用**未解码的原始路径**，且**不做 path.Clean**。
//
// 依据 AWS S3 的签名特例：S3 在计算签名时不对路径做规范化，
// 因此 `/bucket//key`、`/bucket/a/../b` 都按字面参与签名；
// 而 `r.URL.Path` 已被 net/http 解码，`path.Clean` 还会吞掉 `//` 与 `..`，
// 二者都会导致签名与客户端不一致 —— aws-cli 会全线 403。
//
// 空路径按协议补为 "/"（例如 "OPTIONS *" 或直接请求 authority 的情况）。
func canonicalURI(u *url.URL) string {
	p := u.EscapedPath() // 取原始（已编码）路径
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return p
}

// queryPair 是查询串中的一个键值对（保留原始编码形态）。
type queryPair struct{ key, val string }

// canonicalQueryString 计算 CanonicalQueryString。
//
// **易错点 ②**：不能直接对 `url.Values` 排序输出 —— 那样会丢失原始编码，
// 也无法表达"有键无值"与"值为空串"的区别。
//
// 做法：自行切分 RawQuery，按键的**原始编码字节序**排序；
// 值逐对保留。对无值的键补成 `key=`（这正是 `?uploads` 的形态，
// 也是客户端签名的形态：aws-cli 把 `?uploads` 规范化成 `uploads=`）。
//
// 注意：键值对按"整对"排序 —— 规范要求先按键排序，键相同时再按值排序，
// 用 sort.SliceStable + 比较键再比较值即可精确表达。
func canonicalQueryString(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}
	pairs := parseRawQuery(rawQuery)
	sort.SliceStable(pairs, func(i, j int) bool {
		if pairs[i].key != pairs[j].key {
			return pairs[i].key < pairs[j].key
		}
		return pairs[i].val < pairs[j].val
	})
	parts := make([]string, len(pairs))
	for i, p := range pairs {
		parts[i] = p.key + "=" + p.val
	}
	return strings.Join(parts, "&")
}

// parseRawQuery 按 `&` 切分原始查询串，保留键值的原始编码形态。
//
// 不使用 url.ParseQuery：它会做解码与去重，破坏签名的字面一致性。
func parseRawQuery(raw string) []queryPair {
	var out []queryPair
	for _, seg := range strings.Split(raw, "&") {
		if seg == "" {
			continue
		}
		k, v, found := strings.Cut(seg, "=")
		if !found {
			// 形如 `?uploads`：规范化为 `uploads=`。
			v = ""
		}
		out = append(out, queryPair{key: k, val: v})
	}
	return out
}

// canonicalHeaders 计算 CanonicalHeaders 文本，并返回 SignedHeaders 列表文本。
//
// **易错点 ③**：必须**严格**按客户端声明的 SignedHeaders 取头，
// 而不能自作主张地塞入额外头（例如把 Host 无条件加入）——
// 签名参与集合由客户端决定，多一个少一个都会导致校验失败。
//
// 每个头的取值需做：小写化头名、去首尾空白、把连续空白折叠为单个空格。
func canonicalHeaders(r *http.Request, signedHeaders []string) string {
	var b strings.Builder
	for _, name := range signedHeaders {
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(canonicalHeaderValue(r, name))
		b.WriteByte('\n')
	}
	return b.String()
}

// canonicalHeaderValue 取指定头的规范化值。
//
// 三处特殊处理：
//   - `host`：net/http 不把它放进 r.Header，需从 r.Host 取；
//   - 多值头：按出现顺序用逗号连接（与客户端一致）；
//   - 空白折叠：去首尾空白，内部连续空白（含 tab）折叠为单空格。
func canonicalHeaderValue(r *http.Request, name string) string {
	var raw string
	switch name {
	case "host":
		raw = r.Host
	default:
		// http.Header.Get 只返回第一个值，这里需要全部值拼接。
		raw = strings.Join(r.Header.Values(http.CanonicalHeaderKey(name)), ",")
	}
	return collapseSpaces(raw)
}

// collapseSpaces 去除首尾空白并把内部连续空白折叠为单个空格。
func collapseSpaces(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// signedHeaderNames 把 SignedHeaders 头（形如 `host;x-amz-date`）拆为切片。
//
// 客户端若声明了空段或重复项会被原样保留 —— 校验时必然失败，
// 这是正确的：签名参与集合本身就是签名的一部分，不该做"善意修正"。
func signedHeaderNames(v string) []string {
	if v == "" {
		return nil
	}
	return strings.Split(v, ";")
}

// buildCanonical 组装完整的规范化请求。
//
// stripSignature 为真时从查询串中剔除 X-Amz-Signature —— Presigned URL
// 必须如此：签名值本身不能参与签名计算，否则形成自指。
// 之所以显式传参而不是"检测到是 presigned 就自动剔除"，
// 是为了让调用点一眼可见这个关键差异。
func buildCanonical(r *http.Request, signedHeaders []string, payloadHash string, stripSignature bool) CanonicalRequest {
	rawQuery := r.URL.RawQuery
	if stripSignature {
		rawQuery = dropRawParam(rawQuery, headerAmzSignature)
	}
	return CanonicalRequest{
		Method:        r.Method,
		URI:           canonicalURI(r.URL),
		Query:         canonicalQueryString(rawQuery),
		Headers:       canonicalHeaders(r, signedHeaders),
		SignedHeaders: strings.Join(signedHeaders, ";"),
		PayloadHash:   payloadHash,
	}
}

// dropRawParam 从原始查询串中移除指定键，保持其余参数的原始编码与顺序。
func dropRawParam(rawQuery, name string) string {
	pairs := parseRawQuery(rawQuery)
	kept := make([]string, 0, len(pairs))
	for _, p := range pairs {
		if p.key == name {
			continue
		}
		kept = append(kept, p.key+"="+p.val)
	}
	return strings.Join(kept, "&")
}
