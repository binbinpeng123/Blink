package checksum

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"strings"
)

// Algorithm 是 S3 的对象级校验和算法名。
//
// S3 只认这五种，名字就是 x-amz-sdk-checksum-algorithm 头与
// x-amz-checksum-* 头后缀里出现的字面量（大写形式）。
//
// 注意 CRC32 与 CRC32C 是**两种不同算法**（多项式不同、结果不同）：
// 客户端声明 CRC32C 而我们按 CRC32 算，值看起来同样"像校验和"，
// 但客户端会报 BadDigest。这类错误只能靠按名字精确分派来避免。
type Algorithm string

const (
	AlgCRC32     Algorithm = "CRC32"
	AlgCRC32C    Algorithm = "CRC32C"
	AlgCRC64NVME Algorithm = "CRC64NVME"
	AlgSHA1      Algorithm = "SHA1"
	AlgSHA256    Algorithm = "SHA256"
)

// AllAlgorithms 按 S3 文档顺序列出全部算法，供错误信息与测试引用。
var AllAlgorithms = []Algorithm{AlgCRC32, AlgCRC32C, AlgCRC64NVME, AlgSHA1, AlgSHA256}

// crc32cTable 是 Castagnoli 多项式查表，包初始化时建一次。
//
// 标准库在支持 SSE4.2 / ARMv8 CRC 扩展的 CPU 上会用硬件指令执行，
// 因此它既做对象级校验和（客户端声明 CRC32C 时）也做块级校验和
// （见 BlockCRC32C），两条路径都不该重复建表。
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// ParseAlgorithm 把客户端声明的算法名解析为 Algorithm。
//
// 大小写不敏感：HTTP 头值本就不区分大小写，aws-cli 发大写，
// 但把 "crc64nvme" 当成未知算法只会制造无谓的互操作问题。
func ParseAlgorithm(name string) (Algorithm, bool) {
	switch Algorithm(strings.ToUpper(strings.TrimSpace(name))) {
	case AlgCRC32:
		return AlgCRC32, true
	case AlgCRC32C:
		return AlgCRC32C, true
	case AlgCRC64NVME:
		return AlgCRC64NVME, true
	case AlgSHA1:
		return AlgSHA1, true
	case AlgSHA256:
		return AlgSHA256, true
	}
	return "", false
}

// Header 返回该算法对应的 HTTP 头名（全小写）。
//
// 例：x-amz-checksum-crc64nvme。从头名反向推导而不是手写五份字符串，
// 因为"请求头名"与"响应头名"一旦不一致，客户端就取不到校验和。
func (a Algorithm) Header() string {
	return "x-amz-checksum-" + strings.ToLower(string(a))
}

// Sum 按 S3 的编码要求累积对象级校验和。
//
// 刻意不直接暴露 hash.Hash：S3 头部值是 base64 编码，而每种算法的
// "原始字节"含义不同（CRC 是大端整数，SHA 是摘要本身）。把编码收进来，
// 调用方就没有机会用错字节序。
type Sum interface {
	io.Writer
	// Encoded 返回 S3 头部使用的 base64 值。
	Encoded() string
}

// New 返回某个算法的校验和计算器；算法未知时返回 false。
func New(a Algorithm) (Sum, bool) {
	switch a {
	case AlgCRC32:
		return &crcSum{table: crc32.IEEETable}, true
	case AlgCRC32C:
		return &crcSum{table: crc32cTable}, true
	case AlgCRC64NVME:
		return &crc64Sum{h: NewCRC64NVMEHasher()}, true
	case AlgSHA1:
		return &digestSum{h: sha1.New()}, true
	case AlgSHA256:
		return &digestSum{h: sha256.New()}, true
	}
	return nil, false
}

// BlockCRC32C 计算块级校验和（Castagnoli）。
//
// 块级校验和固定用 CRC32C、不给客户端选：它服务于"后台 scrub 发现
// 边端闪存的位翻转"，要求"每块都算也不拖慢写入"，因此必须走硬件指令。
// 它与客户端声明的对象级校验和是两件独立的事，两者同时记录（03 文档）。
func BlockCRC32C(data []byte) uint32 {
	return crc32.Checksum(data, crc32cTable)
}

// crcSum 累积 32 位 CRC。
//
// 两个多项式（IEEE / Castagnoli）共用一个类型 —— 唯一的差别是查表。
type crcSum struct {
	table *crc32.Table
	crc   uint32
}

func (c *crcSum) Write(p []byte) (int, error) {
	c.crc = crc32.Update(c.crc, c.table, p)
	return len(p), nil
}

// Encoded 按**大端**编码。
//
// 大端是 S3 的规定而非习惯：aws-cli 发出的 base64 解码后就是大端整数。
// 小端会让"值算对了但编码不同"，客户端照样报 BadDigest —— 这类错误
// 在线上极难定位，因为本地看校验和"非空且长度正确"。
func (c *crcSum) Encoded() string {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], c.crc)
	return base64.StdEncoding.EncodeToString(b[:])
}

// digestSum 把标准库的 hash.Hash 适配成 Sum。SHA 系摘要天然是大端，
// 不需要额外转换。
type digestSum struct{ h hash.Hash }

func (d *digestSum) Write(p []byte) (int, error) { return d.h.Write(p) }

func (d *digestSum) Encoded() string {
	return base64.StdEncoding.EncodeToString(d.h.Sum(nil))
}

// crc64Sum 适配 CRC64NVME。Sum 已按大端编码（见 crc64nvme.go）。
type crc64Sum struct{ h *CRC64NVMEHasher }

func (c *crc64Sum) Write(p []byte) (int, error) { return c.h.Write(p) }

func (c *crc64Sum) Encoded() string {
	return base64.StdEncoding.EncodeToString(c.h.Sum(nil))
}
