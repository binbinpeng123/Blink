package checksum

import (
	"encoding/base64"
	"encoding/hex"
	"hash/crc32"
	"testing"
)

// 五种算法对固定输入 "123456789" 的检查值。
//
// 这些是各自规范里公开的 check value，与实现无关，因此能真正验证
// "多项式选对了""字节序排对了"—— 用自己的输出当期望值什么也证明不了。
var checkValues = map[Algorithm]string{
	AlgCRC32:     "CBF43926",
	AlgCRC32C:    "E3069283",
	AlgCRC64NVME: "AE8B14860A799888",
	AlgSHA1:      "F7C3BC1D808E04732ADF679965CCC34CA7AE3441",
	AlgSHA256:    "15E2B0D3C33891EBB0F1EF609EC419420C20E320CE94C65FBC8C3312448EB225",
}

// encodedOf 把十六进制检查值按 S3 的要求编码成 base64。
//
// 单独写成函数是为了让"大端 + base64"这一步在测试里也是显式的：
// 如果实现里写成了小端，这里比对就会失败。
func encodedOf(t *testing.T, hexValue string) string {
	t.Helper()
	raw, err := hex.DecodeString(hexValue)
	if err != nil {
		t.Fatalf("测试数据不是合法十六进制: %v", err)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func TestNewMatchesCheckValues(t *testing.T) {
	for _, alg := range AllAlgorithms {
		t.Run(string(alg), func(t *testing.T) {
			sum, ok := New(alg)
			if !ok {
				t.Fatalf("New(%s) 返回 false", alg)
			}
			if _, err := sum.Write([]byte("123456789")); err != nil {
				t.Fatalf("Write 失败: %v", err)
			}
			want := encodedOf(t, checkValues[alg])
			if got := sum.Encoded(); got != want {
				t.Errorf("Encoded() = %s，期望 %s", got, want)
			}
		})
	}
}

// 分多次写入必须与一次写入等价：边读边算是 PutObject 的常态，
// 一旦计算器把 Write 当成"整块覆盖"而非"累加"，大对象的结果就会错。
func TestIncrementalWritesMatchOneShot(t *testing.T) {
	chunks := [][]byte{[]byte("1234"), []byte("567"), []byte("89")}
	for _, alg := range AllAlgorithms {
		t.Run(string(alg), func(t *testing.T) {
			oneShot, _ := New(alg)
			_, _ = oneShot.Write([]byte("123456789"))

			inc, _ := New(alg)
			for _, c := range chunks {
				if _, err := inc.Write(c); err != nil {
					t.Fatalf("Write 失败: %v", err)
				}
			}
			if inc.Encoded() != oneShot.Encoded() {
				t.Errorf("分块写入 = %s，一次写入 = %s", inc.Encoded(), oneShot.Encoded())
			}
		})
	}
}

// CRC32 与 CRC32C 是不同的算法，同一个输入必须给出不同的值 ——
// 这条断言防的是"两个常量误写成同一张表"这种一眼看不出来的错误。
func TestCRC32AndCRC32CAreDifferent(t *testing.T) {
	a, _ := New(AlgCRC32)
	b, _ := New(AlgCRC32C)
	_, _ = a.Write([]byte("123456789"))
	_, _ = b.Write([]byte("123456789"))
	if a.Encoded() == b.Encoded() {
		t.Fatal("CRC32 与 CRC32C 得到了相同结果，说明用错了查找表")
	}

	got := BlockCRC32C([]byte("123456789"))
	if want := crc32.Checksum([]byte("123456789"), crc32.MakeTable(crc32.Castagnoli)); got != want {
		t.Errorf("BlockCRC32C = %#x，期望 %#x", got, want)
	}
}

func TestParseAlgorithm(t *testing.T) {
	tests := []struct {
		in   string
		want Algorithm
		ok   bool
	}{
		{"CRC32", AlgCRC32, true},
		{"crc32", AlgCRC32, true},
		{"  CRC32C ", AlgCRC32C, true},
		{"crc64nvme", AlgCRC64NVME, true},
		{"SHA1", AlgSHA1, true},
		{"sha256", AlgSHA256, true},
		{"MD5", "", false},
		{"", "", false},
		{"CRC64", "", false},
	}
	for _, tt := range tests {
		got, ok := ParseAlgorithm(tt.in)
		if got != tt.want || ok != tt.ok {
			t.Errorf("ParseAlgorithm(%q) = (%q, %v)，期望 (%q, %v)", tt.in, got, ok, tt.want, tt.ok)
		}
	}
}

// Header 的拼写直接决定 aws-cli 能否读到回传的校验和，写错等于没实现。
func TestAlgorithmHeader(t *testing.T) {
	tests := map[Algorithm]string{
		AlgCRC32:     "x-amz-checksum-crc32",
		AlgCRC32C:    "x-amz-checksum-crc32c",
		AlgCRC64NVME: "x-amz-checksum-crc64nvme",
		AlgSHA1:      "x-amz-checksum-sha1",
		AlgSHA256:    "x-amz-checksum-sha256",
	}
	for alg, want := range tests {
		if got := alg.Header(); got != want {
			t.Errorf("%s.Header() = %q，期望 %q", alg, got, want)
		}
	}
}

func TestNewRejectsUnknownAlgorithm(t *testing.T) {
	if _, ok := New(Algorithm("MD5")); ok {
		t.Fatal("New 接受了不支持的算法")
	}
	if _, ok := New(Algorithm("")); ok {
		t.Fatal("New 接受了空算法名")
	}
}
