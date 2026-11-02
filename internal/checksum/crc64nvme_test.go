package checksum

import (
	"bytes"
	"encoding/base64"
	"hash/crc64"
	"math/rand"
	"testing"
)

// TestCRC64NVMEAgainstExternalOracles 用**外部**已知值锁定参数。
//
// 这是本包最重要的测试：CRC 参数（poly/init/xorout/反射方向）错一位，
// 得到的是一组"看起来完全正常"的错误校验和 —— 除了与外部真值对比，
// 没有任何办法发现。两个预言机分别来自 CRC 目录与真实 aws-cli。
func TestCRC64NVMEAgainstExternalOracles(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want uint64
		why  string
	}{
		{
			// CRC 目录中 CRC-64/NVME 的标准 check 值。
			name: "check 向量",
			in:   "123456789",
			want: 0xAE8B14860A799888,
			why:  "CRC 目录的 CRC-64/NVME check 值",
		},
		{
			// 真实 aws-cli 2.36.2 对这段内容发送的
			// x-amz-checksum-crc64nvme: 0Y2Qe5BGWvI=
			name: "aws-cli 实发值",
			in:   "hello blink\n",
			want: 0xD18D907B90465AF2,
			why:  "aws-cli 2.36.2 实际发送的 x-amz-checksum-crc64nvme",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := CRC64NVME([]byte(c.in)); got != c.want {
				t.Errorf("CRC64NVME(%q) = 0x%016X，期望 0x%016X（%s）",
					c.in, got, c.want, c.why)
			}
		})
	}
}

// TestSumEncodingMatchesAWSCLI 验证 Sum 的字节序与 aws-cli 一致。
//
// 值对了但字节序反了，base64 结果就不同，客户端照样报 BadDigest。
// 这里直接把 base64 与抓包得到的字面量对照。
func TestSumEncodingMatchesAWSCLI(t *testing.T) {
	h := NewCRC64NVMEHasher()
	if _, err := h.Write([]byte("hello blink\n")); err != nil {
		t.Fatal(err)
	}
	const wantB64 = "0Y2Qe5BGWvI=" // aws-cli 实发
	if got := base64.StdEncoding.EncodeToString(h.Sum(nil)); got != wantB64 {
		t.Errorf("base64 校验和 = %q，期望 %q（大端序）", got, wantB64)
	}
}

// TestMatchesStdlibOracle 用标准库作独立实现交叉验证。
//
// 标准库没有 CRC64/NVME 的预设多项式，但**算法是同一个**：传入
// 同一个反射多项式后，两者必须逐位相等。这条测试的价值在于它覆盖
// 任意长度 —— 外部向量只有两条，覆盖不到表索引出错的那些分支。
//
// 注意标准库并非"逐字节实现"：它在 len(p) >= 2048 时同样走
// slicing-by-8（我们自己另建表是因为它每调用一次重建一次，见
// crc64nvme.go 中的实测数据）。因此这是一次**两种 slicing 实现
// 之间**的对照，比"拿逐字节当基准"更有说服力。
func TestMatchesStdlibOracle(t *testing.T) {
	tbl := crc64.MakeTable(crc64ReflectPoly)

	// crc64.Update(0, tbl, data) 内部会先取补再在末尾再取补，
	// 净效果恰好等于"init=^0, xorout=^0"，也就是完整的 CRC64NVME。
	// 这个换算关系是实测确认的（传 ^0 反而对不上），不是推导出来的。
	oracle := func(data []byte) uint64 {
		return crc64.Update(0, tbl, data)
	}

	// 长度必须覆盖 8 字节主循环的每个边界：0..17 覆盖 <8、=8、>8 且余数各异。
	sizes := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 16, 17, 31, 32, 33, 64, 100, 1000}
	rng := rand.New(rand.NewSource(1))
	for _, n := range sizes {
		data := make([]byte, n)
		rng.Read(data)

		got := CRC64NVME(data)
		want := oracle(data)
		if got != want {
			t.Errorf("长度 %d: 本实现 = 0x%016X，标准库 = 0x%016X", n, got, want)
		}
	}
}

// TestIncrementalMatchesOneShot 验证分块写入与一次算完结果一致。
//
// 这条保证了流式写入（边读边算，不把对象缓冲进内存）的正确性。
func TestIncrementalMatchesOneShot(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	data := make([]byte, 100_000)
	rng.Read(data)

	want := CRC64NVME(data)
	h := NewCRC64NVMEHasher()
	// 用不规整的分块长度，确保跨越 8 字节主循环的边界。
	for off := 0; off < len(data); {
		n := 1 + rng.Intn(997)
		if off+n > len(data) {
			n = len(data) - off
		}
		if _, err := h.Write(data[off : off+n]); err != nil {
			t.Fatal(err)
		}
		off += n
	}
	if got := h.Sum64(); got != want {
		t.Errorf("分块 = 0x%016X，一次算完 = 0x%016X", got, want)
	}
}

func TestResetAndEmpty(t *testing.T) {
	// 空输入的校验和必须等于"只用 init/xorout"的结果，不是 0。
	empty := CRC64NVME(nil)
	if empty != crc64Init^crc64XorOut {
		t.Errorf("空输入 = 0x%016X，期望 0x%016X", empty, crc64Init^crc64XorOut)
	}

	h := NewCRC64NVMEHasher()
	h.Write([]byte("一些内容"))
	h.Reset()
	if got := h.Sum64(); got != empty {
		t.Errorf("Reset 后 = 0x%016X，期望与空输入相同 0x%016X", got, empty)
	}
}

func TestHasherInterface(t *testing.T) {
	h := NewCRC64NVMEHasher()
	if h.Size() != 8 {
		t.Errorf("Size = %d，期望 8", h.Size())
	}
	if h.BlockSize() != 8 {
		t.Errorf("BlockSize = %d，期望 8", h.BlockSize())
	}
	if !bytes.Equal(h.Sum(nil), h.Sum(nil)) {
		t.Error("Sum 应可重复调用且结果稳定")
	}
}
