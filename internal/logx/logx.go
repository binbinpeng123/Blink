// Package logx 提供结构化日志与敏感信息脱敏。
//
// 日志统一为单行 JSON，便于边缘环境下被日志采集器直接消费。
// 所有可能包含凭据的字段必须经过 Redact 处理后再写入日志。
package logx

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// Redacted 是脱敏后的占位符。
const Redacted = "***REDACTED***"

// New 构造一个输出单行 JSON 的 slog.Logger。
//
// level 取值：debug、info、warn、error；无法识别时回落到 info。
// w 为 nil 时输出到 stderr。
func New(level string, w io.Writer) *slog.Logger {
	if w == nil {
		w = os.Stderr
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: parseLevel(level),
	})
	return slog.New(h)
}

// parseLevel 将配置中的级别字符串映射为 slog.Level，未知级别回落到 Info。
func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// Redact 对敏感值脱敏。
//
// 空值返回空字符串（"未配置"本身不是秘密）；非空值一律替换为占位符，
// 不保留长度与前缀，避免通过长度或前缀推断密钥。
func Redact(s string) string {
	if s == "" {
		return ""
	}
	return Redacted
}

// RedactKeyID 对 Access Key 一类的标识符做部分脱敏：保留前 4 位便于排障，
// 其余替换。标识符本身不是密钥，但也不应完整出现在日志中。
func RedactKeyID(s string) string {
	if s == "" {
		return ""
	}
	if len(s) <= 4 {
		return Redacted
	}
	return s[:4] + strings.Repeat("*", len(s)-4)
}
