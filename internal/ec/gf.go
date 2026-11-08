package ec

// 本文件实现 GF(2^8) 的查表算术。
//
// 本原多项式取 0x11d（x^8+x^4+x^3+x^2+1），与 Linux 内核的 RAID6、
// Backblaze Reed-Solomon 等实现一致：同一份分片数据在不同实现之间可以
// 交叉验证，也便于用已知向量做单测。
//
// 全部运算在 uint8 上封闭：加法与减法都是异或，乘除走对数/反对数表。
// 选 2^8 而不是 2^16 是因为 8 位域一次处理一个字节，查表开销最小；
// 代价是 k+m ≤ 256，对边端集群绰绰有余。

const (
	// gfPoly 是本原多项式 x^8+x^4+x^3+x^2+1。
	gfPoly = 0x11d
	// gfOrder 是乘法群的阶：非零元素共 255 个。
	gfOrder = 255
)

var (
	// gfExp 是对数表的反向表。长度 512 而不是 255：gfMul 直接相加两个
	// 对数（最大 254+254=508），越界处理会让内层循环多一次分支。
	gfExp [512]byte
	// gfLog 是 gfExp 的逆表；gfLog[0] 永不使用。
	gfLog [256]byte
	// gfMulTable[a][b] = a·b。64 KiB 静态表换掉编码内层循环的
	// 两次查表 + 一次加法：编码吞吐只受限于内存带宽。
	gfMulTable [256][256]byte
)

func init() {
	x := 1
	for i := 0; i < gfOrder; i++ {
		gfExp[i] = byte(x)
		gfLog[byte(x)] = byte(i)
		x <<= 1
		if x&0x100 != 0 {
			x ^= gfPoly
		}
	}
	for i := gfOrder; i < len(gfExp); i++ {
		gfExp[i] = gfExp[i-gfOrder]
	}
	for a := 0; a < 256; a++ {
		for b := 0; b < 256; b++ {
			gfMulTable[a][b] = gfMul(byte(a), byte(b))
		}
	}
}

// gfMul 返回 a·b。
func gfMul(a, b byte) byte {
	if a == 0 || b == 0 {
		return 0
	}
	return gfExp[int(gfLog[a])+int(gfLog[b])]
}

// gfInv 返回乘法逆元，gfInv(0) 返回 0。
//
// 柯西矩阵的系数是 1/(x_i ⊕ y_j)，构造时保证分母非零；返回 0 让
// "意外出现零元"在下游表现为矩阵不可逆，而不是一个看似合法的分片。
func gfInv(a byte) byte {
	if a == 0 {
		return 0
	}
	return gfExp[gfOrder-int(gfLog[a])]
}

// mulAdd 计算 dst ^= coeff · src。
//
// src 与 dst 必须等长。coeff=1 时走异或快路径，避免查表。
func mulAdd(dst, src []byte, coeff byte) {
	switch coeff {
	case 0:
		return
	case 1:
		for i, v := range src {
			dst[i] ^= v
		}
		return
	}
	table := &gfMulTable[coeff]
	for i, v := range src {
		dst[i] ^= table[v]
	}
}
