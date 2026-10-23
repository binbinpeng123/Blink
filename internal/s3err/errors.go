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

// table 是错误码的唯一事实来源，按文档 02 的错误码表逐条转录。
var table = map[Code]*Error{
	CodeNoSuchBucket:                 {CodeNoSuchBucket, "The specified bucket does not exist.", http.StatusNotFound},
	CodeNoSuchKey:                    {CodeNoSuchKey, "The specified key does not exist.", http.StatusNotFound},
	CodeBucketAlreadyOwnedByYou:      {CodeBucketAlreadyOwnedByYou, "Your previous request to create the named bucket succeeded and you already own it.", http.StatusConflict},
	CodeBucketAlreadyExists:          {CodeBucketAlreadyExists, "The requested bucket name is not available.", http.StatusConflict},
	CodeBucketNotEmpty:               {CodeBucketNotEmpty, "The bucket you tried to delete is not empty.", http.StatusConflict},
	CodeInvalidBucketName:            {CodeInvalidBucketName, "The specified bucket is not valid.", http.StatusBadRequest},
	CodeInvalidArgument:              {CodeInvalidArgument, "Invalid Argument.", http.StatusBadRequest},
	CodeInvalidRange:                 {CodeInvalidRange, "The requested range is not satisfiable.", http.StatusRequestedRangeNotSatisfiable},
	CodeNoSuchUpload:                 {CodeNoSuchUpload, "The specified upload does not exist.", http.StatusNotFound},
	CodeEntityTooSmall:               {CodeEntityTooSmall, "Your proposed upload is smaller than the minimum allowed object size.", http.StatusBadRequest},
	CodeInvalidPart:                  {CodeInvalidPart, "One or more of the specified parts could not be found.", http.StatusBadRequest},
	CodeInvalidPartOrder:             {CodeInvalidPartOrder, "The list of parts was not in ascending order.", http.StatusBadRequest},
	CodeBadDigest:                    {CodeBadDigest, "The Content-MD5 or checksum you specified did not match what we received.", http.StatusBadRequest},
	CodeInvalidDigest:                {CodeInvalidDigest, "The Content-MD5 you specified is not valid.", http.StatusBadRequest},
	CodeMalformedXML:                 {CodeMalformedXML, "The XML you provided was not well-formed or did not validate against our published schema.", http.StatusBadRequest},
	CodeMethodNotAllowed:             {CodeMethodNotAllowed, "The specified method is not allowed against this resource.", http.StatusMethodNotAllowed},
	CodePreconditionFailed:           {CodePreconditionFailed, "At least one of the preconditions you specified did not hold.", http.StatusPreconditionFailed},
	CodeNotImplemented:               {CodeNotImplemented, "A header or feature you provided implies functionality that is not implemented.", http.StatusNotImplemented},
	CodeAccessDenied:                 {CodeAccessDenied, "Access Denied.", http.StatusForbidden},
	CodeInvalidAccessKeyID:           {CodeInvalidAccessKeyID, "The AWS Access Key Id you provided does not exist in our records.", http.StatusForbidden},
	CodeSignatureDoesNotMatch:        {CodeSignatureDoesNotMatch, "The request signature we calculated does not match the signature you provided.", http.StatusForbidden},
	CodeRequestTimeTooSkewed:         {CodeRequestTimeTooSkewed, "The difference between the request time and the current time is too large.", http.StatusForbidden},
	CodeMissingSecurityHeader:        {CodeMissingSecurityHeader, "Your request is missing a required header.", http.StatusBadRequest},
	CodeAuthorizationHeaderMalformed: {CodeAuthorizationHeaderMalformed, "The authorization header you provided is invalid.", http.StatusBadRequest},
	CodeSlowDown:                     {CodeSlowDown, "Please reduce your request rate.", http.StatusServiceUnavailable},
	CodeInternalError:                {CodeInternalError, "We encountered an internal error. Please try again.", http.StatusInternalServerError},
	CodeLengthRequired:               {CodeLengthRequired, "You must provide a Content-Length header.", http.StatusLengthRequired},
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
type errorXML struct {
	XMLName   xml.Name `xml:"Error"`
	Code      Code     `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId,omitempty"`
	HostID    string   `xml:"HostId,omitempty"`
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
