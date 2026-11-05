package ec

import (
	"bytes"
	"math/rand"
	"testing"
)

var testProfiles = []struct {
	k, m int
}{
	{2, 1},
	{3, 2},
	{4, 2},
}

// combinations 返回从 n 个下标中选 r 个的全部组合。
func combinations(n, r int) [][]int {
	if r == 0 {
		return [][]int{{}}
	}
	var out [][]int
	pick := make([]int, r)
	var walk func(start, depth int)
	walk = func(start, depth int) {
		if depth == r {
			out = append(out, append([]int(nil), pick...))
			return
		}
		for i := start; i <= n-(r-depth); i++ {
			pick[depth] = i
			walk(i+1, depth+1)
		}
	}
	walk(0, 0)
	return out
}

func randomData(seed int64, n int) []byte {
	r := rand.New(rand.NewSource(seed))
	buf := make([]byte, n)
	if _, err := r.Read(buf); err != nil {
		panic(err)
	}
	return buf
}

// TestEncodeReconstructExhaustive 对每种 profile、每种长度，穷举所有
// "丢失 1..m 片"的组合，断言恢复后的每个分片与原始编码逐字节一致。
func TestEncodeReconstructExhaustive(t *testing.T) {
	lengths := []int{1, 2, 5, 33, 1024, 5000}
	for _, p := range testProfiles {
		codec, err := NewCodec(p.k, p.m)
		if err != nil {
			t.Fatalf("NewCodec(%d,%d): %v", p.k, p.m, err)
		}
		for _, n := range lengths {
			data := randomData(int64(p.k*1000+n), n)
			orig, err := codec.Encode(data)
			if err != nil {
				t.Fatalf("Encode(%d,%d,len=%d): %v", p.k, p.m, n, err)
			}
			wantSize := (n + p.k - 1) / p.k
			for i, s := range orig {
				if len(s) != wantSize {
					t.Fatalf("(%d,%d) 分片 %d 长度 %d，期望 %d", p.k, p.m, i, len(s), wantSize)
				}
			}
			if err := codec.Verify(orig); err != nil {
				t.Fatalf("(%d,%d,len=%d) Verify 失败: %v", p.k, p.m, n, err)
			}

			for lost := 1; lost <= p.m; lost++ {
				for _, combo := range combinations(codec.Shards(), lost) {
					shards := make([][]byte, len(orig))
					present := make([]bool, len(orig))
					for i := range orig {
						present[i] = true
					}
					for _, idx := range combo {
						present[idx] = false
						shards[idx] = nil
					}
					for i := range orig {
						if present[i] {
							shards[i] = append([]byte(nil), orig[i]...)
						}
					}
					if err := codec.Reconstruct(shards, present); err != nil {
						t.Fatalf("(%d,%d,len=%d) 丢失 %v 恢复失败: %v", p.k, p.m, n, combo, err)
					}
					for i := range orig {
						if !present[i] {
							t.Fatalf("(%d,%d,len=%d) 分片 %d 未被标记为已恢复", p.k, p.m, n, i)
						}
						if !bytes.Equal(shards[i], orig[i]) {
							t.Fatalf("(%d,%d,len=%d) 丢失 %v 后分片 %d 与原始不一致", p.k, p.m, n, combo, i)
						}
					}
				}
			}
		}
	}
}

// TestReconstructTooFewShards 断言只剩 k-1 片时拒绝恢复。
func TestReconstructTooFewShards(t *testing.T) {
	codec, err := NewCodec(4, 2)
	if err != nil {
		t.Fatal(err)
	}
	shards, err := codec.Encode(randomData(7, 4096))
	if err != nil {
		t.Fatal(err)
	}
	present := []bool{true, true, true, false, false, false}
	shards[3], shards[4], shards[5] = nil, nil, nil
	if err := codec.Reconstruct(shards, present); err == nil {
		t.Fatal("3 个可用分片不足 k=4，应返回错误")
	}
}

// TestReconstructRejectsNilPresent 断言"标记可用但内容为空"直接报错。
func TestReconstructRejectsNilPresent(t *testing.T) {
	codec, err := NewCodec(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	shards, err := codec.Encode(randomData(8, 64))
	if err != nil {
		t.Fatal(err)
	}
	present := []bool{true, true, false}
	shards[0] = nil
	if err := codec.Reconstruct(shards, present); err == nil {
		t.Fatal("present=true 但分片为空，应返回错误")
	}
}

// TestVerifyDetectsCorruption 断言校验片被篡改时 Verify 失败。
func TestVerifyDetectsCorruption(t *testing.T) {
	codec, err := NewCodec(3, 2)
	if err != nil {
		t.Fatal(err)
	}
	shards, err := codec.Encode(randomData(9, 333))
	if err != nil {
		t.Fatal(err)
	}
	if err := codec.Verify(shards); err != nil {
		t.Fatalf("原始分片应通过校验: %v", err)
	}
	shards[4][10] ^= 0x01
	if err := codec.Verify(shards); err == nil {
		t.Fatal("校验片被篡改，Verify 应失败")
	}
}

// TestNewCodecValidation 覆盖参数边界。
func TestNewCodecValidation(t *testing.T) {
	bad := [][2]int{{0, 1}, {1, 0}, {-1, 2}, {2, -1}, {255, 2}, {4, 253}}
	for _, p := range bad {
		if _, err := NewCodec(p[0], p[1]); err == nil {
			t.Fatalf("NewCodec(%d,%d) 应返回错误", p[0], p[1])
		}
	}
	for _, p := range [][2]int{{1, 1}, {4, 2}, {254, 2}, {128, 128}} {
		if _, err := NewCodec(p[0], p[1]); err != nil {
			t.Fatalf("NewCodec(%d,%d) 不应失败: %v", p[0], p[1], err)
		}
	}
}

// TestEncodeRejectsEmpty 空数据无法确定分片边界，应拒绝。
func TestEncodeRejectsEmpty(t *testing.T) {
	codec, err := NewCodec(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Encode(nil); err == nil {
		t.Fatal("Encode(nil) 应返回错误")
	}
}

// BenchmarkEncode 固定 4 MiB 块、4+2 profile，对应生产环境的块大小。
func BenchmarkEncode(b *testing.B) {
	codec, err := NewCodec(4, 2)
	if err != nil {
		b.Fatal(err)
	}
	data := randomData(1, 4<<20)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := codec.Encode(data); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReconstruct 丢 2 片（k=4,m=2 的最坏情况）的重建吞吐。
func BenchmarkReconstruct(b *testing.B) {
	codec, err := NewCodec(4, 2)
	if err != nil {
		b.Fatal(err)
	}
	data := randomData(2, 4<<20)
	orig, err := codec.Encode(data)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		shards := make([][]byte, len(orig))
		present := make([]bool, len(orig))
		for j := range orig {
			present[j] = true
		}
		// 丢一个数据片和一个校验片：既走矩阵求逆，也走校验片重算。
		shards[0] = nil
		present[0] = false
		shards[5] = nil
		present[5] = false
		for j := 1; j < len(orig)-1; j++ {
			shards[j] = append([]byte(nil), orig[j]...)
		}
		b.StartTimer()
		if err := codec.Reconstruct(shards, present); err != nil {
			b.Fatal(err)
		}
	}
}
