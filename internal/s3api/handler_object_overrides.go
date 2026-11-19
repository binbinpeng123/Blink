package s3api

import (
	"fmt"
	"net/http"

	"github.com/binbinpeng123/Blink/internal/s3err"
)

// responseOverrideParams 是 GetObject 的 response-* 查询参数与目标响应头。
//
// 顺序固定，错误消息才不会随 map 迭代顺序变化，测试也才能钉住
// "先校验哪一个"的行为。
var responseOverrideParams = []struct{ param, header string }{
	{"response-cache-control", "Cache-Control"},
	{"response-content-disposition", "Content-Disposition"},
	{"response-content-encoding", "Content-Encoding"},
	{"response-content-language", "Content-Language"},
	{"response-content-type", "Content-Type"},
	{"response-expires", "Expires"},
}

// parseResponseOverrides 解析 response-* 覆盖参数，返回"目标头 → 值"。
//
// 只有 GET 生效：HEAD 没有响应体，覆盖实体头没有意义（S3 也只对 GET
// 定义该行为）。空值按未提供处理，保留对象自身元数据 —— 返回空
// Content-Type 会让客户端按未知类型处理甚至触发内容嗅探。
//
// 值含控制字符直接 400：net/http 写响应头时会把 CR/LF 换成空格，
// 静默清洗的结果是客户端以为覆盖按原值生效。
func parseResponseOverrides(r *http.Request) (map[string]string, *s3err.Error) {
	if r.Method != http.MethodGet {
		return nil, nil
	}
	q := r.URL.Query()
	var out map[string]string
	for _, p := range responseOverrideParams {
		if !q.Has(p.param) {
			continue
		}
		v := q.Get(p.param)
		if v == "" {
			continue
		}
		if !validHeaderValue(v) {
			return nil, invalidArg(fmt.Sprintf("%s 的值含非法控制字符。", p.param))
		}
		if out == nil {
			out = make(map[string]string, len(responseOverrideParams))
		}
		out[p.header] = v
	}
	return out, nil
}

// applyResponseOverrides 把覆盖值写入响应头。
func applyResponseOverrides(h http.Header, overrides map[string]string) {
	for header, v := range overrides {
		h.Set(header, v)
	}
}

// validHeaderValue 报告值能否安全地作为 HTTP 头值写出。
//
// 允许 HTAB（RFC 9110 的 field-value 允许），拒绝其余控制字符与 DEL。
func validHeaderValue(v string) bool {
	for i := 0; i < len(v); i++ {
		c := v[i]
		if (c < 0x20 && c != '\t') || c == 0x7f {
			return false
		}
	}
	return true
}
