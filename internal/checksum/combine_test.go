package checksum

import (
	"encoding/base64"
	"math/rand"
	"testing"
)

// splitParts 把 data 按 sizes 切开，并算出每片的校验和。
func splitParts(t *testing.T, a Algorithm, data []byte, sizes []int) []PartChecksum {
	t.Helper()
	parts := make([]PartChecksum, 0, len(sizes))
	off := 0
	for _, n := range sizes {
		if off+n > len(data) {
			t.Fatalf("分片尺寸 %v 超出数据长度 %d", sizes, len(data))
		}
		sum, ok := New(a)
		if !ok {
			t.Fatalf("New(%s) 失败", a)
		}
		if _, err := sum.Write(data[off : off+n]); err != nil {
			t.Fatalf("写入分片失败: %v", err)
		}
		parts = append(parts, PartChecksum{Encoded: sum.Encoded(), Size: int64(n)})
		off += n
	}
	if off != len(data) {
		t.Fatalf("分片尺寸之和 %d != 数据长度 %d", off, len(data))
	}
	return parts
}

// wholeChecksum 直接对整段数据算一次校验和，作为合并结果的对照。
func wholeChecksum(t *testing.T, a Algorithm, data []byte) string {
	t.Helper()
	sum, ok := New(a)
	if !ok {
		t.Fatalf("New(%s) 失败", a)
	}
	if _, err := sum.Write(data); err != nil {
		t.Fatalf("写入失败: %v", err)
	}
	return sum.Encoded()
}

// TestMergeFullObjectMatchesDirect 验证 CRC 合并与"拼起来再算一次"逐位一致。
//
// 这是切片 7 的核心数学：CompleteMultipartUpload 不能重读数据（创新 B），
// 只能靠各分片的 CRC 折算整体值；算错时 aws-cli 的 FULL_OBJECT 校验会报
// BadDigest，而且错误值看上去同样"像校验和"，线上极难定位。
func TestMergeFullObjectMatchesDirect(t *testing.T) {
	rng := rand.New(rand.NewSource(20261024))

	// 尺寸刻意包含 1 字节、奇数、跨越常见块边界与 5 MiB 的值。
	sizes := []int{1, 2, 3, 7, 8, 9, 63, 64, 65, 255, 256, 257, 4095, 4096, 4097,
		65535, 65536, 65537, 1 << 20, 5 << 20}
	data := make([]byte, 0, 8<<20)
	for _, n := range sizes {
		chunk := make([]byte, n)
		rng.Read(chunk)
		data = append(data, chunk...)
	}

	for _, alg := range []Algorithm{AlgCRC32, AlgCRC32C, AlgCRC64NVME} {
		t.Run(string(alg), func(t *testing.T) {
			parts := splitParts(t, alg, data, sizes)

			// 整体合并。
			got, ok := MergeFullObject(alg, parts)
			if !ok {
				t.Fatal("MergeFullObject 失败")
			}
			want := wholeChecksum(t, alg, data)
			if got != want {
				t.Errorf("合并结果 = %s，期望 %s", got, want)
			}

			// 任意切分点都必须得到同一个值：分片边界不是数学的一部分。
			for _, cut := range [][]int{
				{len(data)},
				{1, len(data) - 1},
				{len(data) / 3, len(data) - len(data)/3},
				{0, len(data)},
			} {
				if cut[0] < 0 || len(cut) == 2 && cut[1] < 0 {
					continue
				}
				p := splitParts(t, alg, data, cut)
				merged, ok := MergeFullObject(alg, p)
				if !ok {
					t.Fatalf("切分 %v: MergeFullObject 失败", cut)
				}
				if merged != want {
					t.Errorf("切分 %v: 合并结果 = %s，期望 %s", cut, merged, want)
				}
			}
		})
	}
}

// TestMergeFullObjectEmptyInput 覆盖空对象与零长度分片。
func TestMergeFullObjectEmptyInput(t *testing.T) {
	for _, alg := range []Algorithm{AlgCRC32, AlgCRC32C, AlgCRC64NVME} {
		sum, _ := New(alg)
		empty := sum.Encoded()

		// 零长度分片的校验和就是空内容的校验和。
		parts := []PartChecksum{{Encoded: empty, Size: 0}, {Encoded: empty, Size: 0}}
		got, ok := MergeFullObject(alg, parts)
		if !ok {
			t.Fatalf("%s: MergeFullObject 失败", alg)
		}
		if got != empty {
			t.Errorf("%s: 合并零长度分片 = %s，期望 %s", alg, got, empty)
		}
	}

	if _, ok := MergeFullObject(AlgCRC32, nil); ok {
		t.Error("空分片列表不应成功")
	}
	for _, alg := range []Algorithm{AlgSHA1, AlgSHA256} {
		if _, ok := MergeFullObject(alg, []PartChecksum{{Encoded: "x", Size: 1}}); ok {
			t.Errorf("%s 不支持 FULL_OBJECT，却返回成功", alg)
		}
	}
}

// TestMergeComposite 验证 COMPOSITE 的"校验和的校验和 + -N"定义。
func TestMergeComposite(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	data := make([]byte, 10000)
	rng.Read(data)
	sizes := []int{1000, 2000, 7, 6993}

	for _, alg := range AllAlgorithms {
		t.Run(string(alg), func(t *testing.T) {
			parts := splitParts(t, alg, data, sizes)

			// 独立实现一遍定义：把各分片校验和的原文字节拼起来再算一次。
			sum, _ := New(alg)
			for _, p := range parts {
				raw, err := base64.StdEncoding.DecodeString(p.Encoded)
				if err != nil {
					t.Fatalf("分片校验和不是 base64: %v", err)
				}
				if _, err := sum.Write(raw); err != nil {
					t.Fatalf("写入失败: %v", err)
				}
			}
			want := sum.Encoded() + "-4"

			got, ok := MergeComposite(alg, parts)
			if !ok {
				t.Fatal("MergeComposite 失败")
			}
			if got != want {
				t.Errorf("合并结果 = %s，期望 %s", got, want)
			}
		})
	}

	if _, ok := MergeComposite(AlgCRC32, nil); ok {
		t.Error("空分片列表不应成功")
	}
	if _, ok := MergeComposite(Algorithm("CRC111"), []PartChecksum{{Encoded: "x"}}); ok {
		t.Error("未知算法不应成功")
	}
}

// TestSupportsFullObject 固定住"哪些算法能合并"的契约。
func TestSupportsFullObject(t *testing.T) {
	want := map[Algorithm]bool{
		AlgCRC32: true, AlgCRC32C: true, AlgCRC64NVME: true,
		AlgSHA1: false, AlgSHA256: false, Algorithm("MD5"): false,
	}
	for a, w := range want {
		if got := SupportsFullObject(a); got != w {
			t.Errorf("SupportsFullObject(%s) = %v，期望 %v", a, got, w)
		}
	}
}
