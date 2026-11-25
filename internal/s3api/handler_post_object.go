package s3api

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/binbinpeng123/Blink/internal/s3err"
	"github.com/binbinpeng123/Blink/internal/sigv4"
	"github.com/binbinpeng123/Blink/internal/store"
)

// POST Object 的解析护栏（docs/02）。
const (
	// maxPostFieldBytes 是单个普通表单字段的上限。
	maxPostFieldBytes = 64 << 10
	// maxPostFields 是普通表单字段的个数上限。
	maxPostFields = 64
	// maxPostPolicyBytes 是 base64 解码后 policy 文档的上限（与 AWS 一致）。
	maxPostPolicyBytes = 20 << 10
)

// postObjectForm 是已解析的 multipart 表单。
//
// 解析发生在鉴权中间件里而不是 handler 里：policy 验签的五个字段就是
// 表单字段，验签之前不可能不解析；解析一次后放进上下文，handler 直接复用，
// 不会出现"验签用的字段"与"落盘用的字段"来自两份不同解析结果的情形。
type postObjectForm struct {
	// fields 的键统一为小写；S3 的表单字段名不区分大小写。
	fields map[string]string
	// file 是名为 file 的 part 的读取器，其内容在 handler 中流式写入存储。
	file io.Reader
	// filename / fileContentType 是 file part 自带的属性。
	filename        string
	fileContentType string
	// reader 用于在 file 读完后确认没有多余 part（file 必须是最后一个）。
	reader *multipart.Reader

	// 以下由 policy 的 content-length-range 条件填充。
	hasSizeRange bool
	minSize      int64
	maxSize      int64
}

type postObjectCtxKey int

const postObjectFormCtxKey postObjectCtxKey = iota

// postObjectFormFrom 从上下文取已解析的表单。
func postObjectFormFrom(r *http.Request) *postObjectForm {
	f, _ := r.Context().Value(postObjectFormCtxKey).(*postObjectForm)
	return f
}

// isMultipartFormData 报告 Content-Type 是否为 multipart/form-data。
func isMultipartFormData(ct string) bool {
	mt, _, err := mime.ParseMediaType(ct)
	return err == nil && strings.EqualFold(mt, "multipart/form-data")
}

// isPostObjectRequest 报告请求是否为 POST Object（表单直传）。
//
// 只看形状不看内容：桶级 POST、不是 DeleteObjects、Content-Type 是
// multipart/form-data。表单本身是否合法由 readPostObjectForm 判定。
func isPostObjectRequest(r *http.Request) bool {
	if r.Method != http.MethodPost || r.URL.Query().Has("delete") {
		return false
	}
	bucket, key := splitPath(r.URL.Path)
	return bucket != "" && key == "" && isMultipartFormData(r.Header.Get("Content-Type"))
}

// malformedPOST 构造 400 MalformedPOSTRequest。
func malformedPOST(msg string) *s3err.Error {
	return s3err.New(s3err.CodeMalformedPOSTRequest).WithMessage(msg)
}

// invalidPolicy 构造 400 InvalidPolicyDocument。
func invalidPolicy(msg string) *s3err.Error {
	return s3err.New(s3err.CodeInvalidPolicyDocument).WithMessage(msg)
}

// readPostObjectForm 解析 POST Object 的 multipart 表单。
//
// 读到 file part 就停止解析并保留其读取器：文件内容可能很大，解析阶段
// 只做"结构是否合法"的判断，字节流留给 handler 边读边写。
func readPostObjectForm(r *http.Request) (*postObjectForm, *s3err.Error) {
	_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return nil, malformedPOST("Content-Type 无法解析：" + err.Error())
	}
	boundary := strings.TrimSpace(params["boundary"])
	if boundary == "" {
		return nil, malformedPOST("multipart/form-data 缺少 boundary。")
	}

	mr := multipart.NewReader(r.Body, boundary)
	form := &postObjectForm{fields: make(map[string]string, 8), reader: mr}

	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			return nil, malformedPOST("表单缺少 file 字段。")
		}
		if err != nil {
			return nil, malformedPOST("multipart 解析失败：" + err.Error())
		}

		name := strings.ToLower(strings.TrimSpace(part.FormName()))
		if name == "" {
			return nil, malformedPOST("表单存在没有名字的字段。")
		}
		if name == "file" {
			form.file = part
			form.filename = part.FileName()
			form.fileContentType = part.Header.Get("Content-Type")
			return form, nil
		}
		if _, dup := form.fields[name]; dup {
			return nil, malformedPOST("表单字段 " + name + " 重复出现。")
		}
		if len(form.fields) >= maxPostFields {
			return nil, malformedPOST("表单字段过多。")
		}
		// 多读 1 字节用来区分"恰好 64 KiB"与"超过 64 KiB"。
		b, err := io.ReadAll(io.LimitReader(part, maxPostFieldBytes+1))
		if err != nil {
			return nil, malformedPOST("读取表单字段 " + name + " 失败：" + err.Error())
		}
		if len(b) > maxPostFieldBytes {
			return nil, malformedPOST("表单字段 " + name + " 超过 64 KiB。")
		}
		form.fields[name] = string(b)
	}
}

// authFields 取出参与 SigV4 POST policy 验签的五个字段。
func (f *postObjectForm) authFields() sigv4.PostPolicyFields {
	return sigv4.PostPolicyFields{
		Algorithm:  f.fields["x-amz-algorithm"],
		Credential: f.fields["x-amz-credential"],
		Date:       f.fields["x-amz-date"],
		Policy:     f.fields["policy"],
		Signature:  f.fields["x-amz-signature"],
	}
}

// header 把表单字段还原成 http.Header，复用 PUT 路径的元数据与校验和解析。
//
// 表单字段与请求头在 S3 里是同一套语义（Policy 条件还要求字段"等价于头"），
// 用同一份解析代码可以保证两条写入路径的元数据规则不会各自漂移。
func (f *postObjectForm) header() http.Header {
	h := make(http.Header, len(f.fields))
	for k, v := range f.fields {
		h.Set(k, v)
	}
	return h
}

// postPolicyDocument 是 policy 文档的顶层结构。
//
// 未知顶层字段显式拒绝：拼错的 "condtions" 会让验证"永远通过"，
// 而调用方以为自己已经约束了上传条件。
type postPolicyDocument struct {
	Expiration string            `json:"expiration"`
	Conditions []json.RawMessage `json:"conditions"`
}

// postCondition 是规范化后的单条条件。
type postCondition struct {
	op    string // eq / starts-with / content-length-range
	field string // 小写、去 $ 前缀；content-length-range 为空
	value string // eq / starts-with 的期望值（${filename} 已替换前保存原值）
	min   int64
	max   int64
}

// parsePostPolicy 解码并校验 policy 文档，返回条件列表与过期时刻。
func parsePostPolicy(encoded string) ([]postCondition, time.Time, *s3err.Error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		// 部分实现省略 padding；标准编码失败后按无 padding 再试一次。
		raw, err = base64.RawStdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, time.Time{}, invalidPolicy("policy 不是合法的 base64。")
		}
	}
	if len(raw) > maxPostPolicyBytes {
		return nil, time.Time{}, invalidPolicy("policy 文档超过 20 KiB。")
	}

	var doc postPolicyDocument
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil {
		return nil, time.Time{}, invalidPolicy("policy 不是合法的 JSON：" + err.Error())
	}
	if doc.Expiration == "" {
		return nil, time.Time{}, invalidPolicy("policy 缺少 expiration。")
	}
	if doc.Conditions == nil {
		return nil, time.Time{}, invalidPolicy("policy 缺少 conditions。")
	}
	exp, err := time.Parse(time.RFC3339Nano, doc.Expiration)
	if err != nil {
		return nil, time.Time{}, invalidPolicy("expiration 不是 RFC3339 时间。")
	}

	conds := make([]postCondition, 0, len(doc.Conditions))
	for _, rc := range doc.Conditions {
		cond, serr := parsePostCondition(rc)
		if serr != nil {
			return nil, time.Time{}, serr
		}
		conds = append(conds, cond)
	}
	return conds, exp, nil
}

// parsePostCondition 解析单条条件：{"field":"value"} 或 ["op","$field",...]。
func parsePostCondition(rc json.RawMessage) (postCondition, *s3err.Error) {
	var cond postCondition

	trimmed := strings.TrimSpace(string(rc))
	if strings.HasPrefix(trimmed, "{") {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(rc, &obj); err != nil || len(obj) != 1 {
			return cond, invalidPolicy("policy 条件对象必须恰好包含一个字段。")
		}
		for k, v := range obj {
			var value string
			if err := json.Unmarshal(v, &value); err != nil {
				return cond, invalidPolicy("policy 条件 " + k + " 的值必须是字符串。")
			}
			return postCondition{op: "eq", field: normalizePolicyField(k), value: value}, nil
		}
	}

	var arr []json.RawMessage
	if err := json.Unmarshal(rc, &arr); err != nil || len(arr) < 1 {
		return cond, invalidPolicy("policy 条件必须是对象或数组。")
	}
	var op string
	if err := json.Unmarshal(arr[0], &op); err != nil {
		return cond, invalidPolicy("policy 条件的第一个元素必须是操作名。")
	}
	switch op {
	case "eq", "starts-with":
		if len(arr) != 3 {
			return cond, invalidPolicy("policy 条件 " + op + " 需要三个元素。")
		}
		var field, value string
		if err := json.Unmarshal(arr[1], &field); err != nil {
			return cond, invalidPolicy("policy 条件 " + op + " 的字段名必须是字符串。")
		}
		if err := json.Unmarshal(arr[2], &value); err != nil {
			return cond, invalidPolicy("policy 条件 " + op + " 的期望值必须是字符串。")
		}
		return postCondition{op: op, field: normalizePolicyField(field), value: value}, nil
	case "content-length-range":
		if len(arr) != 3 {
			return cond, invalidPolicy("content-length-range 需要三个元素。")
		}
		min, ok1 := jsonInt64(arr[1])
		max, ok2 := jsonInt64(arr[2])
		if !ok1 || !ok2 || min < 0 || max < min {
			return cond, invalidPolicy("content-length-range 的下限与上限必须是非负整数且下限不大于上限。")
		}
		return postCondition{op: op, min: min, max: max}, nil
	default:
		return cond, invalidPolicy("不支持的 policy 条件操作：" + op)
	}
}

// jsonInt64 把 JSON 数字解析成 int64；非整数或溢出返回 false。
func jsonInt64(raw json.RawMessage) (int64, bool) {
	var num json.Number
	if err := json.Unmarshal(raw, &num); err != nil {
		return 0, false
	}
	v, err := num.Int64()
	return v, err == nil
}

// normalizePolicyField 统一字段名：去 $ 前缀、去空白、转小写。
func normalizePolicyField(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "$")
	return strings.ToLower(strings.TrimSpace(s))
}

// evaluatePostPolicy 校验 policy 的有效期与全部条件，并记录 content-length-range。
//
// 除 file / policy / x-amz-signature / x-ignore-* 外，表单里出现的每个字段
// 都必须被某条条件覆盖：否则"签给 X 前缀的上传"可以被塞进任意别的字段
// 而不被察觉（例如 x-amz-meta-* 泄露）。
func evaluatePostPolicy(form *postObjectForm, bucket string, now time.Time) *s3err.Error {
	encoded := form.fields["policy"]
	if encoded == "" {
		// 未带任何签名字段按匿名处理（与缺少签名头的普通请求一致）。
		return s3err.New(s3err.CodeAccessDenied).WithMessage("缺少签名信息：请使用签名请求或 Presigned URL。")
	}

	conds, exp, serr := parsePostPolicy(encoded)
	if serr != nil {
		return serr
	}
	if now.After(exp) {
		return s3err.New(s3err.CodeAccessDenied).
			WithMessage("Invalid according to Policy: Policy expired.")
	}

	// 表单里出现 bucket 时，必须与 URL 的桶一致。
	if b, ok := form.fields["bucket"]; ok && b != bucket {
		return policyConditionFailed(postCondition{op: "eq", field: "bucket", value: b})
	}

	covered := make(map[string]bool, len(conds))
	for _, cond := range conds {
		if cond.op == "content-length-range" {
			form.hasSizeRange = true
			form.minSize, form.maxSize = cond.min, cond.max
			continue
		}
		// bucket 是 URL 的一部分而不是表单字段：浏览器表单通常不带它，
		// 但 policy 里的 {"bucket": "..."} 必须能与实际目标桶比对。
		got := form.policyValue(cond.field)
		if cond.field == "bucket" {
			if _, ok := form.fields["bucket"]; !ok {
				got = bucket
			}
		}
		want := strings.ReplaceAll(cond.value, "${filename}", form.filename)
		ok := got == want
		if cond.op == "starts-with" {
			ok = strings.HasPrefix(got, want)
		}
		if !ok {
			return policyConditionFailed(cond)
		}
		covered[cond.field] = true
	}

	// key 是唯一不写入 fields 的特殊字段之外必须存在的字段：
	// 没有 key 就不知道往哪写；而 key 未被条件约束意味着签名对目标无约束。
	if _, ok := form.fields["key"]; !ok {
		return policyConditionFailed(postCondition{op: "eq", field: "key", value: ""})
	}
	if form.fields["key"] == "" {
		return s3err.New(s3err.CodeInvalidArgument).WithMessage("POST 表单的 key 不能为空。")
	}

	for name := range form.fields {
		switch {
		case name == "file", name == "policy", name == "x-amz-signature":
			continue
		case strings.HasPrefix(name, "x-ignore-"):
			continue
		case covered[name]:
			continue
		default:
			return s3err.New(s3err.CodeAccessDenied).
				WithMessage("Invalid according to Policy: Extra input fields: " + name)
		}
	}
	return nil
}

// policyValue 取字段参与条件匹配的值。
//
// 字段不存在时按空串参与匹配（AWS 语义）：这样 starts-with 条件在字段
// 缺失时自然失败，而 eq 条件可以显式要求"字段为空"。
func (f *postObjectForm) policyValue(field string) string {
	v := f.fields[field]
	return strings.ReplaceAll(v, "${filename}", f.filename)
}

// policyConditionFailed 构造条件不满足的 403。
func policyConditionFailed(cond postCondition) *s3err.Error {
	var detail string
	switch cond.op {
	case "content-length-range":
		detail = fmt.Sprintf("[%q, %d, %d]", cond.op, cond.min, cond.max)
	default:
		detail = fmt.Sprintf("[%q, \"$%s\", %q]", cond.op, cond.field, cond.value)
	}
	return s3err.New(s3err.CodeAccessDenied).
		WithMessage("Invalid according to Policy: Policy Condition failed: " + detail)
}

// postObjectSizeReader 在流式读取 file 内容时执行两件策略检查：
//
//  1. content-length-range：超出上限立即中止，读到 EOF 时检查下限；
//  2. file 必须是最后一个 part：EOF 时多探一个 part，存在即拒绝。
//
// 两件事都必须在字节流过存储之前或之中完成 —— 等到写完之后再检查，
// 超限对象已经落盘，"不留下对象"的承诺就失效了。
type postObjectSizeReader struct {
	inner    io.Reader
	reader   *multipart.Reader
	total    int64
	hasRange bool
	min, max int64
	finished bool
	pending  error
}

func (r *postObjectSizeReader) Read(p []byte) (int, error) {
	if r.pending != nil {
		err := r.pending
		r.pending = nil
		return 0, err
	}
	if r.finished {
		return 0, io.EOF
	}

	n, err := r.inner.Read(p)
	r.total += int64(n)

	// 上限检查必须覆盖"一次读完并同时返回 EOF"的情形：
	// 小对象常常在同一个 Read 里既给出全部字节又给出 io.EOF。
	if r.hasRange && r.total > r.max {
		over := s3err.New(s3err.CodeEntityTooLarge).
			WithMessage(fmt.Sprintf("上传内容超过 policy 允许的 %d 字节。", r.max))
		r.finished = true
		if n > 0 {
			r.pending = over
			return n, nil
		}
		return 0, over
	}

	if err != nil && err != io.EOF {
		// 截断或 boundary 损坏：对客户端而言都是"表单不合法"。
		// 不能把底层错误原样上抛，否则会经存储层映射成 500。
		r.finished = true
		return n, malformedPOST("读取 file 字段失败：" + err.Error())
	}

	if err == io.EOF {
		r.finished = true
		stop := r.finish()
		if stop != nil {
			if n > 0 {
				r.pending = stop
				return n, nil
			}
			return 0, stop
		}
	}
	return n, err
}

// finish 在 file 读到 EOF 时做尾部检查。
func (r *postObjectSizeReader) finish() error {
	if r.hasRange && r.total < r.min {
		return s3err.New(s3err.CodeEntityTooSmall).
			WithMessage(fmt.Sprintf("上传内容小于 policy 要求的 %d 字节。", r.min))
	}
	part, err := r.reader.NextPart()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return malformedPOST("multipart 解析失败：" + err.Error())
	}
	// 请求注定被拒，只关闭不读取，避免把后续 part 的字节读进内存。
	part.Close()
	return malformedPOST("file 字段必须是表单的最后一个字段。")
}

// unsupportedPostFields 是 POST 表单里出现即 501 的字段（docs/02）。
//
// 与请求头形态是同一批能力，只是承载位置不同；分组复用能保证新增能力时
// 两条路径一起被拦截。`acl`/`tagging` 是表单里的裸名字，`x-amz-acl` 等
// 是头部形态的写法，两者都要列。
var unsupportedPostFields = concatHeaders(
	unsupportedSSEHeaders,
	unsupportedACLHeaders,
	unsupportedObjectLockHeaders,
	[]string{
		"acl",
		"tagging",
		"x-amz-website-redirect-location",
		"redirect",
		"x-amz-security-token",
		"awsaccesskeyid",
		"signature",
	},
)

// rejectUnsupportedPostFields 在读取文件内容之前拒绝未实现的能力字段。
func rejectUnsupportedPostFields(fields map[string]string) *s3err.Error {
	for _, name := range unsupportedPostFields {
		if fields[name] != "" {
			return notImplemented("POST 上传暂不支持 " + name + " 字段。")
		}
	}
	if sc := fields["x-amz-storage-class"]; sc != "" && !strings.EqualFold(sc, "STANDARD") {
		return notImplemented("本版本只支持 STANDARD 存储类型。")
	}
	return nil
}

// postSuccessTarget 解析 success_action_status 与 success_action_redirect。
//
// redirect 优先于 status（AWS 语义）；两者都必须在写入对象之前校验，
// 否则会出现"对象已写入但客户端拿不到约定的成功响应"。
func postSuccessTarget(fields map[string]string) (status int, redirect string, serr *s3err.Error) {
	status = http.StatusNoContent
	if v, ok := fields["success_action_status"]; ok {
		switch v {
		case "200":
			status = http.StatusOK
		case "201":
			status = http.StatusCreated
		case "204":
			status = http.StatusNoContent
		default:
			return 0, "", s3err.New(s3err.CodeInvalidArgument).
				WithMessage("success_action_status 只支持 \"200\"、\"201\" 或 \"204\"。")
		}
	}

	if v, ok := fields["success_action_redirect"]; ok {
		u, err := url.Parse(v)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return 0, "", s3err.New(s3err.CodeInvalidArgument).
				WithMessage("success_action_redirect 必须是合法的 http(s) 绝对 URL。")
		}
		redirect = v
	}
	return status, redirect, nil
}

// handlePostObject 处理 POST /{bucket}（multipart/form-data）。
func (s *Server) handlePostObject(w http.ResponseWriter, r *http.Request, bucket, reqID string) {
	form := postObjectFormFrom(r)
	if form == nil || form.file == nil {
		malformedPOST("请求体不是合法的 multipart/form-data 表单。").Write(w, r, reqID)
		return
	}

	// 未实现能力字段必须在读 file 之前拒绝：与 PUT 的头部检查同理，
	// 不能让客户端上传完整个大文件才知道被拒。
	if serr := rejectUnsupportedPostFields(form.fields); serr != nil {
		serr.Write(w, r, reqID)
		return
	}

	// 表单内容没有浏览器会生成的 Content-MD5，支持它只会扩大攻击面。
	// 与未实现能力字段同样在读 file 之前拒绝。
	if form.fields["content-md5"] != "" {
		s3err.New(s3err.CodeInvalidArgument).
			WithMessage("POST 上传不支持 Content-MD5 字段。").Write(w, r, reqID)
		return
	}

	status, redirect, serr := postSuccessTarget(form.fields)
	if serr != nil {
		serr.Write(w, r, reqID)
		return
	}

	if serr := evaluatePostPolicy(form, bucket, time.Now()); serr != nil {
		serr.Write(w, r, reqID)
		return
	}
	key := form.fields["key"]

	hdr := form.header()

	userMeta := userMetadata(hdr)
	if me := checkUserMetadataSize(userMeta); me != nil {
		me.Write(w, r, reqID)
		return
	}

	alg, declared, algErr := parseChecksumHeaders(hdr)
	if algErr != nil {
		algErr.Write(w, r, reqID)
		return
	}

	contentType := hdr.Get("Content-Type")
	if contentType == "" {
		contentType = form.fileContentType
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	info, err := s.store.PutObject(bucket, key, &postObjectSizeReader{
		inner:    form.file,
		reader:   form.reader,
		hasRange: form.hasSizeRange,
		min:      form.minSize,
		max:      form.maxSize,
	}, store.PutOptions{
		ContentType:       contentType,
		UserMeta:          userMeta,
		HTTPMeta:          objectHTTPMeta(hdr),
		ChecksumAlgorithm: alg,
		Checksum:          declared,
		ExpectedSize:      -1,
		UnknownSize:       true,
	})
	if err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}

	h := w.Header()
	h.Set("ETag", `"`+info.ETag+`"`)
	h.Set("Last-Modified", info.Modified.UTC().Format(http.TimeFormat))
	if info.ChecksumAlgorithm != "" {
		h.Set(info.ChecksumAlgorithm.Header(), info.Checksum)
		if info.ChecksumType != "" {
			h.Set("x-amz-checksum-type", info.ChecksumType)
		}
	}

	location := "/" + s3URLEncode(bucket) + "/" + s3URLEncode(key)

	if redirect != "" {
		sep := "?"
		if strings.Contains(redirect, "?") {
			sep = "&"
		}
		h.Set("Location", redirect+sep+
			"bucket="+url.QueryEscape(bucket)+
			"&key="+url.QueryEscape(key)+
			"&etag="+url.QueryEscape(info.ETag))
		w.WriteHeader(http.StatusSeeOther)
	} else {
		h.Set("Location", location)
		switch status {
		case http.StatusOK:
			w.WriteHeader(http.StatusOK)
		case http.StatusCreated:
			writeXMLBody(w, r, http.StatusCreated, postObjectResponse{
				Location: location,
				Bucket:   bucket,
				Key:      key,
				ETag:     `"` + info.ETag + `"`,
			})
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}

	loggerFrom(r.Context(), s.logger).Debug("object_post",
		slog.String("bucket", bucket),
		slog.String("key", key),
		slog.Int64("size", info.Size),
	)
}

// postObjectResponse 是 success_action_status=201 时的响应体。
type postObjectResponse struct {
	XMLName  xml.Name `xml:"PostResponse"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}
