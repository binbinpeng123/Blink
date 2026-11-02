package s3api

import (
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/binbinpeng123/Blink/internal/s3err"
	"github.com/binbinpeng123/Blink/internal/store"
)

// 桶级 CORS 的协议层实现。
//
// 分三块：
//   - XML 编解码与校验（PutBucketCors 请求体、GetBucketCors 响应体）；
//   - 预处理请求（OPTIONS）：在鉴权**之前**完成匹配，因为浏览器不会给
//     预检带 Authorization 头；
//   - 实际请求的响应头注入（带 Origin 的普通 S3 请求）。
//
// 语义以 AWS S3 为基准：未配置返回 404 NoSuchCORSConfiguration，
// 预检不匹配返回 403 AccessForbidden，规则按书写顺序取第一条匹配项。

const (
	// maxCORSConfigBytes 是 CORS 配置文档的上限。
	// AWS 的配额是 64 KiB；超过它的配置本身就该被拒绝，而不是写进盘里。
	maxCORSConfigBytes = 64 << 10
	// maxCORSRules 是单桶规则条数上限，与 AWS 一致。
	maxCORSRules = 100

	corsVary = "Origin, Access-Control-Request-Headers, Access-Control-Request-Method"
)

// corsAllowedMethods 是 CORS 规则中允许出现的方法。
//
// 只列出 HTTP 方法而不是 S3 操作名：OPTIONS 本身不能写进规则。
var corsAllowedMethods = map[string]bool{
	http.MethodGet:    true,
	http.MethodPut:    true,
	http.MethodHead:   true,
	http.MethodPost:   true,
	http.MethodDelete: true,
}

// corsConfigurationXML 是 <CORSConfiguration> 文档。
//
// 同时用于请求体解码与响应体编码：字段与 S3 的 XML 结构一一对应，
// 一处定义避免"读"和"写"两份结构漂移。
type corsConfigurationXML struct {
	XMLName xml.Name      `xml:"CORSConfiguration"`
	Rules   []corsRuleXML `xml:"CORSRule"`
}

// corsRuleXML 是单条 <CORSRule>。
type corsRuleXML struct {
	ID             string   `xml:"ID,omitempty"`
	AllowedOrigins []string `xml:"AllowedOrigin"`
	AllowedMethods []string `xml:"AllowedMethod"`
	AllowedHeaders []string `xml:"AllowedHeader,omitempty"`
	ExposeHeaders  []string `xml:"ExposeHeader,omitempty"`
	// MaxAgeSeconds 为 nil 表示未设置 —— 此时响应不得发 Access-Control-Max-Age，
	// 否则等于替客户端编了一个缓存时长。
	MaxAgeSeconds *int `xml:"MaxAgeSeconds,omitempty"`
}

// dispatchCORS 处理 ?cors 子资源。
//
// 返回 true 表示请求已被处理，调用方必须立即返回。
// 与 dispatchMultipart 一样，子资源的语义优先于通用方法分派：
// `GET /{bucket}?cors` 是读配置，不是 ListObjectsV2。
func (s *Server) dispatchCORS(w http.ResponseWriter, r *http.Request, bucket, key, reqID string) bool {
	if !r.URL.Query().Has("cors") {
		return false
	}
	// S3 的桶级 CORS 只存在于 /{bucket}?cors。带 key 的组合没有定义，
	// 明确回 501 而不是让请求落到对象读写 —— 后者会静默忽略子资源，
	// 用户以为配置了跨域，实际什么也没发生。
	if bucket == "" || key != "" {
		notImplemented("?cors 只支持桶级请求：PUT/GET/DELETE /{bucket}?cors。").
			Write(w, r, reqID)
		return true
	}
	if !s.requireStore(w, r, reqID) {
		return true
	}
	switch r.Method {
	case http.MethodPut:
		s.handlePutBucketCORS(w, r, bucket, reqID)
	case http.MethodGet:
		s.handleGetBucketCORS(w, r, bucket, reqID)
	case http.MethodDelete:
		s.handleDeleteBucketCORS(w, r, bucket, reqID)
	default:
		s3err.New(s3err.CodeMethodNotAllowed).Write(w, r, reqID)
	}
	return true
}

// handlePutBucketCORS 处理 PUT /{bucket}?cors。
func (s *Server) handlePutBucketCORS(w http.ResponseWriter, r *http.Request, bucket, reqID string) {
	cfg, err := decodeCORSConfiguration(r.Body)
	if err != nil {
		loggerFrom(r.Context(), s.logger).Debug("cors_config_rejected",
			slog.String("bucket", bucket),
			slog.String("reason", err.Error()),
		)
		s3err.New(s3err.CodeMalformedXML).
			WithMessage("CORS 配置不合法："+err.Error()).
			Write(w, r, reqID)
		return
	}
	if err := s.store.PutBucketCORS(bucket, cfg); err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}
	w.WriteHeader(http.StatusOK)
	loggerFrom(r.Context(), s.logger).Debug("cors_config_put",
		slog.String("bucket", bucket),
		slog.Int("rules", len(cfg.Rules)),
	)
}

// handleGetBucketCORS 处理 GET /{bucket}?cors。
func (s *Server) handleGetBucketCORS(w http.ResponseWriter, r *http.Request, bucket, reqID string) {
	cfg, err := s.store.GetBucketCORS(bucket)
	if err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}
	writeXMLBody(w, r, http.StatusOK, corsConfigToXML(cfg))
}

// handleDeleteBucketCORS 处理 DELETE /{bucket}?cors。
//
// 与 S3 一致返回 204，且对"从未配置"幂等。
func (s *Server) handleDeleteBucketCORS(w http.ResponseWriter, r *http.Request, bucket, reqID string) {
	if err := s.store.DeleteBucketCORS(bucket); err != nil {
		s.writeStoreErr(w, r, reqID, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
	loggerFrom(r.Context(), s.logger).Debug("cors_config_deleted", slog.String("bucket", bucket))
}

// withCORS 是 CORS 中间件。
//
// 必须位于 withAuth **之外侧**（更早执行）：浏览器的预检请求不带
// Authorization 头，如果先鉴权，跨域读取会在预检阶段就被 403 拦下，
// 配置得再对也无法生效。预检本身就是"问能不能跨域"，不涉及数据访问，
// 因此免签是安全的；真正的数据请求依然走完整签名校验。
func (s *Server) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 健康检查与版本端点不参与 CORS：它们面向探针与负载均衡，
		// 不是浏览器跨域访问的对象。
		if unauthenticatedPaths[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}
		bucket, _ := splitPath(r.URL.Path)
		// 未接入存储或根路径（ListBuckets）时没有桶级配置可查。
		if bucket == "" || s.store == nil {
			next.ServeHTTP(w, r)
			return
		}

		if r.Method == http.MethodOptions {
			s.handlePreflight(w, r, bucket, requestIDFrom(r.Context()))
			return
		}

		// 无 Origin 的请求不是跨域请求，不需要（也不应该）添加 CORS 头。
		if r.Header.Get("Origin") == "" {
			next.ServeHTTP(w, r)
			return
		}
		// 头必须在交给 handler 之前设置好：一旦 handler 调用 WriteHeader，
		// 响应头就被冻结了。
		s.applyActualCORSHeaders(w.Header(), r, bucket)
		next.ServeHTTP(w, r)
	})
}

// handlePreflight 处理 OPTIONS 预检请求。
//
// 错误码遵循 S3：缺 Origin / 缺 AC-Request-Method 是 400（请求本身有毛病），
// 桶没配 CORS 或规则不匹配是 403（服务端拒绝跨域）。
func (s *Server) handlePreflight(w http.ResponseWriter, r *http.Request, bucket, reqID string) {
	origin := r.Header.Get("Origin")
	if origin == "" {
		s3err.New(s3err.CodeInvalidArgument).
			WithMessage("Insufficient information. Origin request header needed.").
			Write(w, r, reqID)
		return
	}
	// 预检方法大小写不敏感地归一化：浏览器发大写，但手工调试可能发小写。
	reqMethod := strings.ToUpper(strings.TrimSpace(r.Header.Get("Access-Control-Request-Method")))
	if reqMethod == "" {
		s3err.New(s3err.CodeInvalidArgument).
			WithMessage("Invalid Access-Control-Request-Method: null").
			Write(w, r, reqID)
		return
	}
	reqHeaders := splitHeaderList(r.Header.Get("Access-Control-Request-Headers"))

	cfg, err := s.store.GetBucketCORS(bucket)
	switch {
	case errors.Is(err, store.ErrNoSuchCORSConfiguration):
		// 桶存在但没配 CORS：与"有配置但规则不匹配"用两条消息，
		// 让用户一眼分清该去配配置还是该去改规则。
		s.corsForbidden(w, r, reqID, "CORSResponse: CORS is not enabled for this bucket.")
		return
	case err != nil:
		s.writeStoreErr(w, r, reqID, err)
		return
	}

	rule := matchCORSRule(cfg, origin, reqMethod, reqHeaders)
	if rule == nil {
		s.corsForbidden(w, r, reqID, "CORSResponse: This CORS request is not allowed.")
		return
	}

	setCORSHeaders(w.Header(), rule, origin, true, reqHeaders)
	w.WriteHeader(http.StatusOK)
}

// corsForbidden 返回 403 AccessForbidden，并记录一条 debug 日志。
//
// 用 Debug 而非 Error：跨域被拒是客户端配置问题，属于预期内的正常回流，
// 不该淹没有价值的错误。
func (s *Server) corsForbidden(w http.ResponseWriter, r *http.Request, reqID, msg string) {
	loggerFrom(r.Context(), s.logger).Debug("cors_preflight_denied",
		slog.String("path", r.URL.Path),
		slog.String("origin", r.Header.Get("Origin")),
		slog.String("method", r.Header.Get("Access-Control-Request-Method")),
	)
	s3err.New(s3err.CodeAccessForbidden).WithMessage(msg).Write(w, r, reqID)
}

// applyActualCORSHeaders 为带 Origin 的普通请求注入 CORS 响应头。
//
// 不匹配时**不报错**：实际请求已经完成，回 403 会让浏览器拿到一个
// 与业务语义无关的失败；S3 的做法是照常返回，只是不给 CORS 头，
// 由浏览器自己拦下响应。
func (s *Server) applyActualCORSHeaders(h http.Header, r *http.Request, bucket string) {
	cfg, err := s.store.GetBucketCORS(bucket)
	if err != nil {
		return
	}
	origin := r.Header.Get("Origin")
	rule := matchCORSRule(cfg, origin, r.Method, nil)
	if rule == nil {
		return
	}
	setCORSHeaders(h, rule, origin, false, nil)
}

// matchCORSRule 返回第一条同时匹配 origin、method 与请求头的规则。
//
// 按书写顺序取第一条：S3 的规则是有序的，排在前面的规则优先。
func matchCORSRule(cfg *store.CORSConfiguration, origin, method string, reqHeaders []string) *store.CORSRule {
	for i := range cfg.Rules {
		rule := &cfg.Rules[i]
		if !corsOriginAllowed(rule.AllowedOrigins, origin) {
			continue
		}
		if !containsFold(rule.AllowedMethods, method) {
			continue
		}
		if !corsHeadersAllowed(rule.AllowedHeaders, reqHeaders) {
			continue
		}
		return rule
	}
	return nil
}

// setCORSHeaders 写出响应头。
//
// isPreflight 区分两类响应：预检返回"允许什么方法/头/缓存多久"，
// 实际请求只返回"这个 origin 被允许"与可暴露的响应头。
func setCORSHeaders(h http.Header, rule *store.CORSRule, origin string, isPreflight bool, reqHeaders []string) {
	allowOrigin := origin
	for _, p := range rule.AllowedOrigins {
		if corsPatternMatches(p, origin, false) && p == "*" {
			// 只有规则写的就是裸 * 时才回 *；`http://*.example.com`
			// 这类模式必须回显具体 origin，否则浏览器视为不匹配。
			allowOrigin = "*"
			break
		}
	}
	h.Set("Access-Control-Allow-Origin", allowOrigin)
	if allowOrigin != "*" {
		// 通配 origin 时浏览器禁止带凭据，因此只在回显具体 origin 时声明。
		h.Set("Access-Control-Allow-Credentials", "true")
	}
	if len(rule.ExposeHeaders) > 0 {
		h.Set("Access-Control-Expose-Headers", strings.Join(rule.ExposeHeaders, ", "))
	}
	// 响应随 Origin 与请求方法/头变化，任何共享缓存都必须把它们计入键，
	// 否则会把 A 站的响应喂给 B 站。
	h.Add("Vary", corsVary)
	if !isPreflight {
		return
	}
	h.Set("Access-Control-Allow-Methods", strings.Join(rule.AllowedMethods, ", "))
	if len(reqHeaders) > 0 {
		// 回显请求头而不是回显规则：两者在"规则里写了 *"时并不相同，
		// 回显请求头既最小化授权面，也正是浏览器要的答案。
		h.Set("Access-Control-Allow-Headers", strings.Join(reqHeaders, ", "))
	}
	if rule.MaxAgeSeconds != nil {
		h.Set("Access-Control-Max-Age", strconv.Itoa(*rule.MaxAgeSeconds))
	}
}

// corsOriginAllowed 报告 origin 是否命中允许源列表。
//
// origin 比较大小写敏感：浏览器发送的 Origin 已是规范化的小写序列化结果，
// 放宽比较只会让"看起来一样"的源蒙混过关。
func corsOriginAllowed(patterns []string, origin string) bool {
	for _, p := range patterns {
		if corsPatternMatches(p, origin, false) {
			return true
		}
	}
	return false
}

// corsHeadersAllowed 报告请求头是否全部被规则覆盖。
func corsHeadersAllowed(patterns, requested []string) bool {
	for _, want := range requested {
		if !containsPatternFold(patterns, want) {
			return false
		}
	}
	return true
}

// corsPatternMatches 按 S3 的单通配符语义匹配。
//
// S3 允许在 AllowedOrigin / AllowedHeader 中写一个 `*`，例如
// `http://*.example.com` 或 `x-amz-*`；`*` 只能出现一次。
// fold 为 true 时按 ASCII 大小写不敏感比较（HTTP 头名是大小写不敏感的）。
func corsPatternMatches(pattern, value string, fold bool) bool {
	if pattern == "*" {
		return true
	}
	i := strings.IndexByte(pattern, '*')
	if i < 0 {
		return equalMaybeFold(pattern, value, fold)
	}
	prefix, suffix := pattern[:i], pattern[i+1:]
	if len(value) < len(prefix)+len(suffix) {
		return false
	}
	return equalMaybeFold(prefix, value[:len(prefix)], fold) &&
		equalMaybeFold(suffix, value[len(value)-len(suffix):], fold)
}

func equalMaybeFold(a, b string, fold bool) bool {
	if fold {
		return strings.EqualFold(a, b)
	}
	return a == b
}

// containsPatternFold 报告 names 中是否存在一个模式匹配 name（大小写不敏感）。
func containsPatternFold(patterns []string, name string) bool {
	for _, p := range patterns {
		if corsPatternMatches(p, name, true) {
			return true
		}
	}
	return false
}

// containsFold 报告 list 中是否有与 v 大小写不敏感相等的项。
func containsFold(list []string, v string) bool {
	for _, item := range list {
		if strings.EqualFold(item, v) {
			return true
		}
	}
	return false
}

// splitHeaderList 拆分逗号分隔的头名列表，去掉空白与空项。
func splitHeaderList(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// decodeCORSConfiguration 解析并校验 PutBucketCors 的请求体。
//
// 校验放在协议层而不是存储层：这些规则（100 条上限、方法白名单、
// 单通配符）来自 S3 的接口契约，换个协议前端时不该被继承。
func decodeCORSConfiguration(body io.Reader) (*store.CORSConfiguration, error) {
	// 多读 1 字节用于判定超限：直接截断到 64 KiB 会把一个超大文档
	// 当成合法文档的一部分接受，结果是"配置被静默截短"。
	data, err := io.ReadAll(io.LimitReader(body, maxCORSConfigBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxCORSConfigBytes {
		return nil, errors.New("文档超过 64 KiB 上限。")
	}

	var doc corsConfigurationXML
	dec := xml.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	// 根元素之后不得再有内容：Append 型客户端（如部分 SDK 的幂等重放）
	// 可能拼出两个根元素，接受它等于只配置了一半。
	if err := ensureEOF(dec); err != nil {
		return nil, err
	}
	if err := validateCORSConfiguration(&doc); err != nil {
		return nil, err
	}
	return corsConfigFromXML(&doc), nil
}

// ensureEOF 确认解码器已经读到文档末尾。
func ensureEOF(dec *xml.Decoder) error {
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch t := tok.(type) {
		case xml.CharData:
			if strings.TrimSpace(string(t)) == "" {
				continue
			}
		}
		return errors.New("根元素之后存在多余内容。")
	}
}

// validateCORSConfiguration 校验 S3 的 CORS 文档约束。
func validateCORSConfiguration(doc *corsConfigurationXML) error {
	if len(doc.Rules) == 0 {
		return errors.New("至少需要一条 CORSRule。")
	}
	if len(doc.Rules) > maxCORSRules {
		return errors.New("规则条数超过 100 条上限。")
	}
	for i := range doc.Rules {
		rule := &doc.Rules[i]
		if len(rule.AllowedOrigins) == 0 {
			return errors.New("每条规则至少需要一个 AllowedOrigin。")
		}
		if len(rule.AllowedMethods) == 0 {
			return errors.New("每条规则至少需要一个 AllowedMethod。")
		}
		for _, o := range rule.AllowedOrigins {
			if strings.Count(o, "*") > 1 {
				return errors.New("AllowedOrigin 中最多只能有一个通配符 *。")
			}
		}
		for _, m := range rule.AllowedMethods {
			if !corsAllowedMethods[strings.ToUpper(strings.TrimSpace(m))] {
				return errors.New("AllowedMethod 只能是 GET/PUT/HEAD/POST/DELETE。")
			}
		}
		for _, hd := range rule.AllowedHeaders {
			if strings.Count(hd, "*") > 1 {
				return errors.New("AllowedHeader 中最多只能有一个通配符 *。")
			}
		}
		if rule.MaxAgeSeconds != nil && *rule.MaxAgeSeconds < 0 {
			return errors.New("MaxAgeSeconds 不能为负数。")
		}
	}
	return nil
}

// corsConfigFromXML 把校验过的 XML 文档转成存储层类型。
func corsConfigFromXML(doc *corsConfigurationXML) *store.CORSConfiguration {
	out := &store.CORSConfiguration{Rules: make([]store.CORSRule, len(doc.Rules))}
	for i, r := range doc.Rules {
		// 方法归一化为大写：校验时已确认它们在白名单内，
		// 存成小写会让 Allow-Methods 响应头与客户端期望的大小写不一致。
		methods := make([]string, len(r.AllowedMethods))
		for j, m := range r.AllowedMethods {
			methods[j] = strings.ToUpper(strings.TrimSpace(m))
		}
		out.Rules[i] = store.CORSRule{
			ID:             r.ID,
			AllowedOrigins: append([]string(nil), r.AllowedOrigins...),
			AllowedMethods: methods,
			AllowedHeaders: append([]string(nil), r.AllowedHeaders...),
			ExposeHeaders:  append([]string(nil), r.ExposeHeaders...),
			MaxAgeSeconds:  r.MaxAgeSeconds,
		}
	}
	return out
}

// corsConfigToXML 把存储层配置转成响应文档。
func corsConfigToXML(cfg *store.CORSConfiguration) *corsConfigurationXML {
	out := &corsConfigurationXML{Rules: make([]corsRuleXML, len(cfg.Rules))}
	for i, r := range cfg.Rules {
		out.Rules[i] = corsRuleXML{
			ID:             r.ID,
			AllowedOrigins: r.AllowedOrigins,
			AllowedMethods: r.AllowedMethods,
			AllowedHeaders: r.AllowedHeaders,
			ExposeHeaders:  r.ExposeHeaders,
			MaxAgeSeconds:  r.MaxAgeSeconds,
		}
	}
	return out
}
