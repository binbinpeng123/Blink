package store

import (
	"errors"
	"fmt"
	"io"
	"io/fs"

	"github.com/binbinpeng123/Blink/internal/checksum"
)

// 本文件实现数据完整性检查的两种入口：
//
//  1. 读取时校验（Options.VerifyOnRead，默认关闭）：读取路径每块先重算
//     CRC32C 再写出，发现位翻转立即返回 ErrCorrupt，绝不把错误数据发给客户端。
//  2. 显式 scrub（ScrubObject）：按块重算 CRC32C，供运维巡检与后续 GC 使用。
//
// 为什么默认关闭：校验必须把数据读进用户态，与 sendfile 零拷贝互斥
// （03 文档"真实张力：校验读 vs 零拷贝读"）。默认保障模型加载吞吐，
// 对可靠性要求更高的部署用 --verify-read 打开。
//
// 两种入口都只校验**块级 CRC32C**，不重算 SHA256：CRC32C 有硬件指令、
// 每秒可校验数 GB，足以发现"盘上的位翻转"；SHA256 用于防篡改，
// 在写入时已经算过并体现在 CAS 路径里，没有必要在读路径上重复付出代价。

// readBlockVerified 读满一个块并核对 CRC32C。
//
// 必须先读完再返回：块级校验和覆盖整块，只读其中一段无法验证。
// 调用方拿到已校验的字节后才可以写向客户端 —— 边读边写会让损坏的
// 数据先一步到达网络上，而 HTTP 响应发出后无法撤回。
func (s *Store) readBlockVerified(f io.Reader, b blockDesc) ([]byte, error) {
	data := make([]byte, b.Length)
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, fmt.Errorf("%w: 读取块 %s 失败: %v", ErrCorrupt, b.Hash, err)
	}
	if got := checksum.BlockCRC32C(data); got != b.CRC32C {
		return nil, fmt.Errorf("%w: 块 %s 的 CRC32C 为 %08x，块表记录 %08x",
			ErrCorrupt, b.Hash, got, b.CRC32C)
	}
	return data, nil
}

// ScrubObject 重算对象全部块的 CRC32C 并与块表比对。
//
// 返回 ErrCorrupt 时错误信息里带块哈希：运维据此定位具体是哪个 CAS
// 文件坏了，而不是只知道"这个对象有问题"。
//
// 只读 meta.json 与 CAS 块，不修改任何数据，因此可以安全地在服务运行中调用。
func (s *Store) ScrubObject(bucket, key string) error {
	if err := validateBucketName(bucket); err != nil {
		return err
	}
	if err := validateKey(key); err != nil {
		return err
	}
	// 与读取路径一致：先确认桶存在，避免桶缺失被报成"对象不存在"。
	if _, err := s.loadBucketMeta(bucket); err != nil {
		return err
	}
	m, err := s.loadObjectMeta(bucket, key)
	if err != nil {
		return err
	}
	for _, b := range m.Blocks {
		if err := s.scrubBlock(b); err != nil {
			return err
		}
	}
	return nil
}

// scrubBlock 校验单个块。
func (s *Store) scrubBlock(b blockDesc) error {
	path := s.casPath(b.Hash)
	f, err := s.fs.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: 块 %s 缺失（对象元数据仍引用它）", ErrCorrupt, b.Hash)
		}
		return fsErr("打开块", b.Hash, err)
	}
	defer func() { _ = f.Close() }()

	st, err := s.fs.Stat(path)
	if err != nil {
		return fsErr("检查块", b.Hash, err)
	}
	if st.Size() != b.Length {
		return fmt.Errorf("%w: 块 %s 长度 %d，块表记录 %d",
			ErrCorrupt, b.Hash, st.Size(), b.Length)
	}
	// 复用同一个"读满并校验"实现：读取时校验与 scrub 若各写一套，
	// 迟早会出现"一种路径检出了、另一种漏掉了"的分叉。
	if _, err := s.readBlockVerified(f, b); err != nil {
		return err
	}
	return nil
}
