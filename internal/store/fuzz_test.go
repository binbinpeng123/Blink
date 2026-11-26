package store

import (
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"testing"
)

// 本文件是存储层"外部输入"解析的模糊测试基线。桶名来自 URL 路径，
// 分片文件名来自数据目录，meta.json 的内容在磁盘损坏或人为改动后同样
// 不可信 —— 三者都必须做到"拒绝而不崩溃"。
//
// 有界 fuzz：
//
//	go test -run=^$ -fuzz=^FuzzValidateBucketName$ -fuzztime=20s ./internal/store
//	go test -run=^$ -fuzz=^FuzzDecodeObjectMeta$ -fuzztime=20s ./internal/store
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

// FuzzDecodeObjectMeta 把任意字节当作 meta.json 喂给解析入口。
//
// 断言三条：
//  1. 不 panic —— 盘上损坏或被对手改写的元数据不能拖垮进程；
//  2. 失败必须是 ErrCorrupt —— 损坏绝不能降级成"正常"或未分类错误；
//  3. 成功必须真的自洽 —— 块表与分片总长用 math/big 独立复算，不复用
//     被测实现的 int64 累加，溢出伪造出的"总长恰好匹配"会立刻暴露。
func FuzzDecodeObjectMeta(f *testing.F) {
	const (
		hashA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		hashB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	block := func(hash string, off, length int64) string {
		return fmt.Sprintf(`{"hash":%q,"offset":%d,"length":%d}`, hash, off, length)
	}
	valid := `{"version":1,"key":"obj","size":12,"etag":"etag","content_type":"text/plain","node_id":"n1","obj_version":1,"blocks":[` +
		block(hashA, 0, 8) + `,` + block(hashB, 8, 4) + `]}`
	for _, seed := range []string{
		valid,
		// 墓碑：只有版本，没有内容。
		`{"version":1,"key":"obj","deleted":true,"node_id":"n1","obj_version":2}`,
		// 墓碑误带内容。
		strings.Replace(valid, `"key":"obj"`, `"key":"obj","deleted":true`, 1),
		`{"size":-1}`,
		// 块表出现空洞。
		`{"size":12,"blocks":[` + block(hashA, 0, 8) + `,` + block(hashB, 9, 4) + `]}`,
		// 块表被截断：JSON 合法，但总长对不上。
		`{"size":12,"blocks":[` + block(hashA, 0, 8) + `]}`,
		// 溢出攻击：4 个 2^62 的块让 int64 累加绕回 0，恰好"等于" size=0。
		`{"size":0,"blocks":[` +
			block(hashA, 0, 1<<62) + `,` +
			block(hashA, 1<<62, 1<<62) + `,` +
			block(hashA, -(1<<63), 1<<62) + `,` +
			block(hashA, -(1<<63)+(1<<62), 1<<62) + `]}`,
		// 超出 int64 的 size。
		`{"size":9223372036854775808}`,
		// 合法内联对象："abc"，哈希为 SHA-256("abc")。
		`{"layout":"inline","size":3,"key":"obj","blocks":[{"hash":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad","offset":0,"length":3}],"inline_data":"YWJj"}`,
		// EC 布局：单机校验按设计不复核（08 边界），解析成功本身合法。
		`{"layout":"ec","size":3,"blocks":[` + block(hashA, 0, 3) + `]}`,
		`{}`,
		`null`,
		`[]`,
		``,
		`{"blocks":null,"size":0}`,
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data string) {
		m, err := decodeObjectMeta([]byte(data), "fuzz")
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("解析失败必须归类为 ErrCorrupt：err=%v", err)
			}
			return
		}
		if err := checkDecodedMeta(m); err != nil {
			t.Fatalf("解析成功的元数据不自洽：%v（输入 %q）", err, data)
		}
		// 合法元数据序列化后必须仍能被接受，否则"写进去的读不回来"。
		if _, err := decodeObjectMeta(mustJSON(m), "fuzz-roundtrip"); err != nil {
			t.Fatalf("合法元数据重新序列化后被拒绝：%v", err)
		}
	})
}

// checkDecodedMeta 用 math/big 独立复算解析结果的结构约束。
//
// 与被测实现走不同的数值路径，才能发现"实现按 int64 环绕后自洽，
// 但按无界整数并不自洽"这一类溢出伪造。EC 布局除外：单机校验按设计
// 不解析 EC 元数据（08 边界），这里也不越权断言。
func checkDecodedMeta(m *ObjectMeta) error {
	if m.Size < 0 {
		return fmt.Errorf("size 为负：%d", m.Size)
	}
	if m.Deleted {
		if m.Size != 0 || len(m.Blocks) != 0 || len(m.Parts) != 0 {
			return fmt.Errorf("墓碑携带内容：size=%d blocks=%d parts=%d",
				m.Size, len(m.Blocks), len(m.Parts))
		}
		return nil
	}
	if m.Layout == LayoutEC {
		return nil
	}

	var cursor big.Int
	for i, b := range m.Blocks {
		if b.Length <= 0 {
			return fmt.Errorf("块 %d 长度 %d 非正", i, b.Length)
		}
		if big.NewInt(b.Offset).Cmp(&cursor) != 0 {
			return fmt.Errorf("块 %d 偏移 %d，期望 %s", i, b.Offset, cursor.String())
		}
		cursor.Add(&cursor, big.NewInt(b.Length))
	}
	if cursor.Cmp(big.NewInt(m.Size)) != 0 {
		return fmt.Errorf("块表总长 %s 与 size %d 不符", cursor.String(), m.Size)
	}
	if m.Size > 0 && len(m.Blocks) == 0 {
		return fmt.Errorf("非空对象没有块表")
	}

	var (
		partTotal big.Int
		last      int
	)
	for _, p := range m.Parts {
		if p.Number <= last {
			return fmt.Errorf("分片编号 %d 未严格递增", p.Number)
		}
		if p.Size < 0 {
			return fmt.Errorf("分片 %d 长度 %d 为负", p.Number, p.Size)
		}
		last = p.Number
		partTotal.Add(&partTotal, big.NewInt(p.Size))
	}
	if len(m.Parts) > 0 && partTotal.Cmp(big.NewInt(m.Size)) != 0 {
		return fmt.Errorf("分片总长 %s 与 size %d 不符", partTotal.String(), m.Size)
	}

	if m.Layout == LayoutInline {
		if m.Size <= 0 || len(m.Blocks) != 1 {
			return fmt.Errorf("内联对象 size=%d blocks=%d", m.Size, len(m.Blocks))
		}
		if int64(len(m.InlineData)) != m.Size {
			return fmt.Errorf("内联内容 %d 字节，声明 %d", len(m.InlineData), m.Size)
		}
	}
	return nil
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
