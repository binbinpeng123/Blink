// Package s3err 定义 S3 错误码表与统一 XML 错误渲染。
//
// 错误码与 HTTP 状态码的对应关系以评审文档 02（S3 接口与数据模型）为准。
// 任何新增错误码必须同步更新该文档，避免实现与契约漂移。
package s3err

import (
	"encoding/xml"
	"net/http"
)

// Code 是 S3 错误码，通常是驼峰命名的字符串，直接出现在响应体 <Code> 中。
type Code string

// 错误码常量。
const (
	CodeNoSuchBucket                 Code = "NoSuchBucket"
	CodeNoSuchKey                    Code = "NoSuchKey"
	CodeBucketAlreadyOwnedByYou      Code = "BucketAlreadyOwnedByYou"
	CodeBucketAlreadyExists          Code = "BucketAlreadyExists"
	CodeBucketNotEmpty               Code = "BucketNotEmpty"
	CodeInvalidBucketName            Code = "InvalidBucketName"
	CodeInvalidArgument              Code = "InvalidArgument"
	CodeInvalidRange                 Code = "InvalidRange"
	CodeNoSuchUpload                 Code = "NoSuchUpload"
	CodeEntityTooSmall               Code = "EntityTooSmall"
	CodeInvalidPart                  Code = "InvalidPart"
	CodeInvalidPartOrder             Code = "InvalidPartOrder"
	CodeBadDigest                    Code = "BadDigest"
	CodeInvalidDigest                Code = "InvalidDigest"
	CodeMalformedXML                 Code = "MalformedXML"
	CodeMethodNotAllowed             Code = "MethodNotAllowed"
	CodePreconditionFailed           Code = "PreconditionFailed"
	CodeNotImplemented               Code = "NotImplemented"
	CodeAccessDenied                 Code = "AccessDenied"
	CodeInvalidAccessKeyID           Code = "InvalidAccessKeyId"
	CodeSignatureDoesNotMatch        Code = "SignatureDoesNotMatch"
	CodeRequestTimeTooSkewed         Code = "RequestTimeTooSkewed"
	CodeMissingSecurityHeader        Code = "MissingSecurityHeader"
	CodeAuthorizationHeaderMalformed Code = "AuthorizationHeaderMalformed"
	CodeSlowDown                     Code = "SlowDown"
	CodeInternalError                Code = "InternalError"
	CodeLengthRequired               Code = "LengthRequired"
)

// Error 是一条可返回给客户端的 S3 错误。
//
// Message 在表中集中定义；个别场景可以复制一份并覆盖消息（见 WithMessage），
// 但不得改变 Code 与 HTTP 状态码。
type Error struct {
	Code       Code
	Message    string
	HTTPStatus int
	// Details 是追加在 Message 之后的额外 XML 元素。
	//
	// AWS 在部分错误上用这些字段给出可比对的事实，最典型的是
	// RequestTimeTooSkewed 回传 RequestTime / ServerTime，
	// 让用户一眼看出两端时钟差了多少。用切片而非 map 是为了固定元素顺序。
	Details []Detail
}

// Detail 是错误响应体中的一个附加元素。
//
// XMLName 决定元素名，因此用 D 构造而不是直接写字面量 ——
// 后者要求调用方 import encoding/xml，噪音大于收益。
type Detail struct {
	XMLName xml.Name
	Value   string `xml:",chardata"`
}

// D 构造一个附加元素，元素名为 name、文本内容为 value。
//
// 例：s3err.D(s3err.DetailServerTime, "20261023T090000Z")
func D(name, value string) Detail {
	return Detail{XMLName: xml.Name{Local: name}, Value: value}
}

// Error 实现 error 接口，便于在内部以 error 传递。
func (e *Error) Error() string {
	return string(e.Code) + ": " + e.Message
}

// WithMessage 返回带自定义消息的副本，Code 与状态码保持不变。
func (e *Error) WithMessage(msg string) *Error {
	cp := *e
	cp.Message = msg
	return &cp
}

// WithDetails 返回带附加字段的副本。
//
// 传空切片时代替 WithMessage("")，用于需要给出结构化事实而非文案的场景。
func (e *Error) WithDetails(details ...Detail) *Error {
	cp := *e
	cp.Details = append(append([]Detail(nil), e.Details...), details...)
	return &cp
}

// table 是错误码的唯一事实来源，按文档 02 的错误码表逐条转录。
var table = map[Code]*Error{
	CodeNoSuchBucket:                 {Code: CodeNoSuchBucket, Message: "The specified bucket does not exist.", HTTPStatus: http.StatusNotFound},
	CodeNoSuchKey:                    {Code: CodeNoSuchKey, Message: "The specified key does not exist.", HTTPStatus: http.StatusNotFound},
	CodeBucketAlreadyOwnedByYou:      {Code: CodeBucketAlreadyOwnedByYou, Message: "Your previous request to create the named bucket succeeded and you already own it.", HTTPStatus: http.StatusConflict},
	CodeBucketAlreadyExists:          {Code: CodeBucketAlreadyExists, Message: "The requested bucket name is not available.", HTTPStatus: http.StatusConflict},
	CodeBucketNotEmpty:               {Code: CodeBucketNotEmpty, Message: "The bucket you tried to delete is not empty.", HTTPStatus: http.StatusConflict},
	CodeInvalidBucketName:            {Code: CodeInvalidBucketName, Message: "The specified bucket is not valid.", HTTPStatus: http.StatusBadRequest},
	CodeInvalidArgument:              {Code: CodeInvalidArgument, Message: "Invalid Argument.", HTTPStatus: http.StatusBadRequest},
	CodeInvalidRange:                 {Code: CodeInvalidRange, Message: "The requested range is not satisfiable.", HTTPStatus: http.StatusRequestedRangeNotSatisfiable},
	CodeNoSuchUpload:                 {Code: CodeNoSuchUpload, Message: "The specified upload does not exist.", HTTPStatus: http.StatusNotFound},
	CodeEntityTooSmall:               {Code: CodeEntityTooSmall, Message: "Your proposed upload is smaller than the minimum allowed object size.", HTTPStatus: http.StatusBadRequest},
	CodeInvalidPart:                  {Code: CodeInvalidPart, Message: "One or more of the specified parts could not be found.", HTTPStatus: http.StatusBadRequest},
	CodeInvalidPartOrder:             {Code: CodeInvalidPartOrder, Message: "The list of parts was not in ascending order.", HTTPStatus: http.StatusBadRequest},
	CodeBadDigest:                    {Code: CodeBadDigest, Message: "The Content-MD5 or checksum you specified did not match what we received.", HTTPStatus: http.StatusBadRequest},
	CodeInvalidDigest:                {Code: CodeInvalidDigest, Message: "The Content-MD5 you specified is not valid.", HTTPStatus: http.StatusBadRequest},
	CodeMalformedXML:                 {Code: CodeMalformedXML, Message: "The XML you provided was not well-formed or did not validate against our published schema.", HTTPStatus: http.StatusBadRequest},
	CodeMethodNotAllowed:             {Code: CodeMethodNotAllowed, Message: "The specified method is not allowed against this resource.", HTTPStatus: http.StatusMethodNotAllowed},
	CodePreconditionFailed:           {Code: CodePreconditionFailed, Message: "At least one of the preconditions you specified did not hold.", HTTPStatus: http.StatusPreconditionFailed},
	CodeNotImplemented:               {Code: CodeNotImplemented, Message: "A header or feature you provided implies functionality that is not implemented.", HTTPStatus: http.StatusNotImplemented},
	CodeAccessDenied:                 {Code: CodeAccessDenied, Message: "Access Denied.", HTTPStatus: http.StatusForbidden},
	CodeInvalidAccessKeyID:           {Code: CodeInvalidAccessKeyID, Message: "The AWS Access Key Id you provided does not exist in our records.", HTTPStatus: http.StatusForbidden},
	CodeSignatureDoesNotMatch:        {Code: CodeSignatureDoesNotMatch, Message: "The request signature we calculated does not match the signature you provided.", HTTPStatus: http.StatusForbidden},
	CodeRequestTimeTooSkewed:         {Code: CodeRequestTimeTooSkewed, Message: "The difference between the request time and the current time is too large.", HTTPStatus: http.StatusForbidden},
	CodeMissingSecurityHeader:        {Code: CodeMissingSecurityHeader, Message: "Your request is missing a required header.", HTTPStatus: http.StatusBadRequest},
	CodeAuthorizationHeaderMalformed: {Code: CodeAuthorizationHeaderMalformed, Message: "The authorization header you provided is invalid.", HTTPStatus: http.StatusBadRequest},
	CodeSlowDown:                     {Code: CodeSlowDown, Message: "Please reduce your request rate.", HTTPStatus: http.StatusServiceUnavailable},
	CodeInternalError:                {Code: CodeInternalError, Message: "We encountered an internal error. Please try again.", HTTPStatus: http.StatusInternalServerError},
	CodeLengthRequired:               {Code: CodeLengthRequired, Message: "You must provide a Content-Length header.", HTTPStatus: http.StatusLengthRequired},
}

// Get 返回错误码对应的规范错误；未知错误码回落到 InternalError，避免把
// 未注册的错误码泄露给客户端。
func Get(code Code) *Error {
	if e, ok := table[code]; ok {
		return e
	}
	return table[CodeInternalError]
}

// New 是 Get 的别名，提供更自然的调用写法 s3err.New(s3err.CodeNoSuchKey)。
func New(code Code) *Error { return Get(code) }

// Codes 返回表中全部错误码，供测试与文档校验使用。
func Codes() []Code {
	codes := make([]Code, 0, len(table))
	for c := range table {
		codes = append(codes, c)
	}
	return codes
}

// errorXML 是统一的 <Error> 响应体结构。
//
// Details 放在 RequestId 之前，与 AWS 的字段顺序一致
// （RequestTimeTooSkewed 的 RequestTime/ServerTime 出现在 RequestId 之前）。
type errorXML struct {
	XMLName  xml.Name `xml:"Error"`
	Code     Code     `xml:"Code"`
	Message  string   `xml:"Message"`
	Resource string   `xml:"Resource,omitempty"`
	Details  []Detail `xml:",omitempty"`
	// RequestID 与 HostID 用于排障定位，值由中间件注入。
	RequestID string `xml:"RequestId,omitempty"`
	HostID    string `xml:"HostId,omitempty"`
}

// HostID 是所有错误响应共用的固定值，标识服务实现。
const HostID = "blink"

// Write 将错误写入 HTTP 响应。
//
// HEAD 请求按 HTTP 语义不返回响应体，但仍写入状态码与 Content-Type。
// 调用方应在写响应头之前确定最终状态。
func (e *Error) Write(w http.ResponseWriter, r *http.Request, requestID string) {
	writeXML(w, r, e.HTTPStatus, errorXML{
		Code:      e.Code,
		Message:   e.Message,
		Resource:  r.URL.Path,
		Details:   e.Details,
		RequestID: requestID,
		HostID:    HostID,
	})
}

// writeXML 序列化并写出 XML 体，HEAD 请求只写头。
func writeXML(w http.ResponseWriter, r *http.Request, status int, body any) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(status)
	if r != nil && r.Method == http.MethodHead {
		return
	}
	_ = xml.NewEncoder(w).Encode(body)
}
