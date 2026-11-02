package logx

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

// TestNewWritesSingleLineJSON 验证日志是单行 JSON，可被采集器直接解析。
func TestNewWritesSingleLineJSON(t *testing.T) {
	var buf bytes.Buffer
	logger := New("info", &buf)
	logger.Info("hello", slog.String("k", "v"))

	line := strings.TrimSpace(buf.String())
	if strings.Contains(line, "\n") {
		t.Fatalf("日志应为单行，实际含换行: %q", line)
	}

	var got map[string]any
	if err := json.Unmarshal([]byte(line), &got); err != nil {
		t.Fatalf("日志不是合法 JSON: %v (%q)", err, line)
	}
	if got["msg"] != "hello" {
		t.Errorf("msg = %v", got["msg"])
	}
	if got["k"] != "v" {
		t.Errorf("k = %v", got["k"])
	}
	if got["level"] != "INFO" {
		t.Errorf("level = %v", got["level"])
	}
}

func TestLevelFiltering(t *testing.T) {
	cases := []struct {
		level   string
		expects bool
	}{
		{"debug", true},
		{"info", false},
		{"warn", false},
		{"error", false},
		{"", false},
		{"nonsense", false},
	}
	for _, c := range cases {
		t.Run(c.level, func(t *testing.T) {
			var buf bytes.Buffer
			New(c.level, &buf).Debug("d")
			if got := buf.Len() > 0; got != c.expects {
				t.Errorf("level=%q 时 debug 是否输出 = %v, 期望 %v", c.level, got, c.expects)
			}
		})
	}
}

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug":   slog.LevelDebug,
		"DEBUG":   slog.LevelDebug,
		" info ":  slog.LevelInfo,
		"warn":    slog.LevelWarn,
		"warning": slog.LevelWarn,
		"error":   slog.LevelError,
		"":        slog.LevelInfo,
		"trace":   slog.LevelInfo,
	}
	for in, want := range cases {
		if got := parseLevel(in); got != want {
			t.Errorf("parseLevel(%q) = %v, 期望 %v", in, got, want)
		}
	}
}

func TestRedact(t *testing.T) {
	if got := Redact(""); got != "" {
		t.Errorf("空值应返回空串，实际 %q", got)
	}
	// 即使很短的密钥也必须整体脱敏，不能因为"太短所以看不出"而放行。
	for _, s := range []string{"a", "short", "a-very-long-secret-key-value"} {
		got := Redact(s)
		if got != Redacted {
			t.Errorf("Redact(%q) = %q, 期望 %q", s, got, Redacted)
		}
		if strings.Contains(got, s) && s != "" {
			t.Errorf("脱敏结果泄露了原值: %q", got)
		}
	}
}

func TestRedactKeyID(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"abc", Redacted},
		{"abcd", Redacted},
		{"AKIATESTTEST1234", "AKIA************"},
	}
	for _, c := range cases {
		if got := RedactKeyID(c.in); got != c.want {
			t.Errorf("RedactKeyID(%q) = %q, 期望 %q", c.in, got, c.want)
		}
	}
}
