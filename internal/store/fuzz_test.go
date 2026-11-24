package store

import (
	"strconv"
	"strings"
	"testing"
)

// 本文件是存储层"外部输入"解析的模糊测试基线。桶名来自 URL 路径，
// 分片文件名来自数据目录 —— 前者完全由客户端控制，后者在磁盘损坏或
// 人为改动后也不可信，二者都必须做到"拒绝而不崩溃"。
//
// 有界 fuzz：
//
//	go test -run=^$ -fuzz=^FuzzValidateBucketName$ -fuzztime=20s ./internal/store
func FuzzValidateBucketName(f *testing.F) {
	for _, name := range []string{
		"abc",
		"123",
		"1.2.3",
		"a-b",
		"a.b.c",
		"a.b-c.d",
		"xn--abc",
		"192.168.1.1",
		"0.0.0.0",
		"255.255.255.255",
		"256.1.1.1",
		"01.2.3.4",
		"1.2.3.4.5",
		"ab",
		"a",
		"",
		"..",
		"a..b",
		"a_b",
		"Aabc",
		"abc-",
		"-abc",
		"a.-b",
		strings.Repeat("a", 62),
		strings.Repeat("a", 63),
		strings.Repeat("a", 64),
		"a" + strings.Repeat(".", 61) + "b",
	} {
		f.Add(name)
	}

	f.Fuzz(func(t *testing.T, name string) {
		err := validateBucketName(name)
		want := bucketNameAcceptableByRules(name)
		if (err == nil) != want {
			t.Fatalf("校验结果与规则不符：name=%q err=%v want=%v", name, err, want)
		}
		if err != nil {
			if err != ErrInvalidBucketName {
				t.Fatalf("非法桶名应返回 ErrInvalidBucketName：name=%q err=%v", name, err)
			}
			return
		}
		if err := validateBucketName(name); err != nil {
			t.Fatalf("同一名字两次校验结果不同：name=%q err=%v", name, err)
		}
	})
}

// bucketNameAcceptableByRules 是按 AWS 桶名规则**独立**写的对照实现，
// 不复用被测代码里的正则或 isIPv4。差分对照的价值在于：
// 被测实现若被改松或改紧，这里会立刻以"与规则不符"失败。
func bucketNameAcceptableByRules(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '.', c == '-':
		default:
			return false
		}
	}
	if !isLowerAlnum(name[0]) || !isLowerAlnum(name[len(name)-1]) {
		return false
	}
	if strings.Contains(name, "..") {
		return false
	}
	if strings.HasPrefix(name, "xn--") {
		return false
	}
	return !dottedIPv4ByRules(name)
}

func isLowerAlnum(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= '0' && c <= '9'
}

func dottedIPv4ByRules(s string) bool {
	parts := strings.Split(s, ".")
	if len(parts) != 4 {
		return false
	}
	for _, p := range parts {
		if p == "" || len(p) > 3 || !allDecimalDigits(p) {
			return false
		}
		if len(p) > 1 && p[0] == '0' {
			return false
		}
		if n, err := strconv.Atoi(p); err != nil || n > 255 {
			return false
		}
	}
	return true
}

func allDecimalDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// FuzzParsePartFileName 断言分片文件名的解析不越界，且编号可重写为
// 规范文件名后再次解析得到同一编号。
func FuzzParsePartFileName(f *testing.F) {
	for _, name := range []string{
		"1.json",
		"2.json",
		"10000.json",
		"10001.json",
		"0.json",
		"01.json",
		"00001.json",
		"99999.json",
		"123456.json",
		"1.json.bak",
		".json",
		"json",
		"abc.json",
		"1.JSON",
		" 1.json",
	} {
		f.Add(name)
	}

	f.Fuzz(func(t *testing.T, name string) {
		n, ok := parsePartFileName(name)
		if !ok {
			return
		}
		if n < 1 || n > maxPartNumber {
			t.Fatalf("解析出越界分片编号：name=%q → %d", name, n)
		}
		want, wantOK := parsePartFileName(strconv.Itoa(n) + multipartPartSuffixName)
		if !wantOK || want != n {
			t.Fatalf("分片编号不可逆：name=%q → %d，重写解析得到 (%d,%v)",
				name, n, want, wantOK)
		}
	})
}
