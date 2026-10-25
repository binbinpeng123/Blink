package checksum

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strconv"
)

// PartChecksum 是分片上传中一个分片的校验和。
//
// Size 必须包含在内：CRC 的合并结果与"每个分片后面跟着多少个零字节"
// 有关，丢了分片长度就只能重读数据，创新 B（Complete 不重写数据）
// 也就不成立了。
type PartChecksum struct {
	// Encoded 是分片校验和的 base64 文本，与 x-amz-checksum-* 头的值同构。
	Encoded string
	Size    int64
}

// SupportsFullObject 报告某个算法能否计算 FULL_OBJECT 校验和。
//
// 只有三种 CRC 可以：它们的合并是纯代数运算（GF(2) 矩阵），
// 能在不重读数据的前提下由各分片的 CRC 直接折算出整体值。
// SHA1/SHA256 没有这种性质，只能走 COMPOSITE。
func SupportsFullObject(a Algorithm) bool {
	switch a {
	case AlgCRC32, AlgCRC32C, AlgCRC64NVME:
		return true
	}
	return false
}

// MergeFullObject 把各分片的 CRC 合并为整个对象的 CRC。
//
// 语义（02 文档）：与"把所有分片按序拼起来再算一次"逐位相等。
// 这正是 aws-cli 的默认行为 —— s3transfer 为每个分片声明 CRC64NVME，
// CompleteMultipartUpload 时客户端按同一规则合并并校验服务端的复算结果；
// 我们合并错了，客户端会报 BadDigest。
//
// 实现是 zlib crc32_combine 的 GF(2) 矩阵法，对三种 CRC 结构相同：
// 反射多项式、init 全 1、xorout 全 1。输入输出都是**最终 CRC**
// （即已做过 xorout 的值），因此可以直接用 S3 头部的值。
func MergeFullObject(a Algorithm, parts []PartChecksum) (string, bool) {
	dim, poly, width, ok := crcParams(a)
	if !ok || len(parts) == 0 {
		return "", false
	}

	// combine(0, c2, n) == c2（矩阵乘以零向量得零），
	// 因此可以从 0 开始折叠，不必特判第一片。
	var crc uint64
	for _, p := range parts {
		if p.Size < 0 {
			return "", false
		}
		v, err := decodeCRC(p.Encoded, width)
		if err != nil {
			return "", false
		}
		crc = combineCRC(crc, v, p.Size, dim, poly)
	}

	var buf [8]byte
	if width == 4 {
		binary.BigEndian.PutUint32(buf[:4], uint32(crc))
		return base64.StdEncoding.EncodeToString(buf[:4]), true
	}
	binary.BigEndian.PutUint64(buf[:8], crc)
	return base64.StdEncoding.EncodeToString(buf[:8]), true
}

// MergeComposite 计算 COMPOSITE 校验和。
//
// 定义（02 文档）：对"各分片原始校验和字节"按序拼接后再算一次，
// 结果追加 "-<分片数>"。SHA 系只能这样；CRC32/CRC32C 在客户端
// 未声明类型时也默认 COMPOSITE —— 与 AWS 的行为一致。
func MergeComposite(a Algorithm, parts []PartChecksum) (string, bool) {
	if len(parts) == 0 {
		return "", false
	}
	_, _, _, known := crcParams(a)
	if !known {
		switch a {
		case AlgSHA1, AlgSHA256:
		default:
			return "", false
		}
	}

	sum, ok := New(a)
	if !ok {
		return "", false
	}
	for _, p := range parts {
		raw, err := base64.StdEncoding.DecodeString(p.Encoded)
		if err != nil {
			return "", false
		}
		// 不校验 raw 的长度：这里的原则是"客户端给什么字节就合并什么"，
		// 长度异常的输入会算出一个错误的合并值，随后与客户端声明比对时
		// 以 BadDigest 暴露出来，而不是在这里猜客户的意图。
		if _, err := sum.Write(raw); err != nil {
			return "", false
		}
	}
	return sum.Encoded() + "-" + strconv.Itoa(len(parts)), true
}

// crcParams 返回某种 CRC 的矩阵维度、反射多项式与编码宽度（字节）。
func crcParams(a Algorithm) (dim int, poly uint64, width int, ok bool) {
	switch a {
	case AlgCRC32:
		return 32, 0xEDB88320, 4, true
	case AlgCRC32C:
		return 32, 0x82F63B78, 4, true
	case AlgCRC64NVME:
		return 64, 0x9A6C9329AC4BC9B5, 8, true
	}
	return 0, 0, 0, false
}

// decodeCRC 把 base64 的 CRC 值解码为大端整数。
func decodeCRC(encoded string, width int) (uint64, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return 0, err
	}
	if len(raw) != width {
		return 0, fmt.Errorf("checksum: CRC 长度为 %d 字节，期望 %d", len(raw), width)
	}
	if width == 4 {
		return uint64(binary.BigEndian.Uint32(raw)), nil
	}
	return binary.BigEndian.Uint64(raw), nil
}

// combineCRC 把 crc1 再追加 len2 个零字节后与 crc2 合并。
//
// 直接移植 zlib 的 crc32_combine：先用多项式构造"移位一位"与
// "移位两位"的矩阵，不断平方得到"移位 8 位"的算子，再按 len2 的
// 二进制位逐位应用到 crc1，最后 xor crc2。
func combineCRC(crc1, crc2 uint64, len2 int64, dim int, poly uint64) uint64 {
	if len2 <= 0 {
		return crc1
	}

	odd := make([]uint64, dim)
	even := make([]uint64, dim)
	odd[0] = poly
	row := uint64(1)
	for n := 1; n < dim; n++ {
		odd[n] = row
		row <<= 1
	}
	matrixSquare(even, odd) // 两位零
	matrixSquare(odd, even) // 四位零

	for {
		// 每轮先平方得到"八位零"算子，处理 len2 的最低位。
		matrixSquare(even, odd)
		if len2&1 != 0 {
			crc1 = matrixTimes(even, crc1)
		}
		len2 >>= 1
		if len2 == 0 {
			break
		}
		matrixSquare(odd, even)
		if len2&1 != 0 {
			crc1 = matrixTimes(odd, crc1)
		}
		len2 >>= 1
		if len2 == 0 {
			break
		}
	}
	return crc1 ^ crc2
}

// matrixTimes 计算矩阵与向量的 GF(2) 乘积。
func matrixTimes(mat []uint64, vec uint64) uint64 {
	var sum uint64
	for i := 0; vec != 0; i++ {
		if vec&1 != 0 {
			sum ^= mat[i]
		}
		vec >>= 1
	}
	return sum
}

// matrixSquare 计算矩阵的 GF(2) 平方：square = mat × mat。
func matrixSquare(square, mat []uint64) {
	for n := range square {
		square[n] = matrixTimes(mat, mat[n])
	}
}
