// Package ec 实现零依赖的纠删码编解码。
//
// 布局固定为 k 数据片 + m 校验片（系统码）：数据片就是原始数据的连续
// 切分，校验片由柯西矩阵线性组合得到。任意 ≤m 片丢失（数据片或校验片
// 均可）都能从剩下的片逐字节还原。
//
// 编解码按"一个块"为单位工作：调用方把 4 MiB 块交给 Encode，得到
// k+m 个等长分片；读时把拿到的分片交给 Reconstruct，缺失位置就地填充。
// 分片长度 S=ceil(len/k)，最后一片不足的部分以零填充 —— 这条规则必须
// 与 meta 中的校验规则保持一致，否则不同节点会算出不同的分片边界。
package ec

import (
	"bytes"
	"errors"
	"fmt"
)

// Codec 是一个 (k, m) 纠删码实例，创建后不可变，可被并发使用。
type Codec struct {
	k, m int
	// gen 是 (k+m)×k 生成矩阵：前 k 行是单位阵，后 m 行是柯西矩阵。
	gen [][]byte
}

// NewCodec 构造一个 k+m 的纠删码实例。
//
// 限制 k+m ≤256 来自 GF(2^8) 的乘法群大小；k、m 至少为 1。
func NewCodec(k, m int) (*Codec, error) {
	if k < 1 {
		return nil, fmt.Errorf("ec: 数据片数 k=%d 必须 ≥1", k)
	}
	if m < 1 {
		return nil, fmt.Errorf("ec: 校验片数 m=%d 必须 ≥1", m)
	}
	if k+m > 256 {
		return nil, fmt.Errorf("ec: k+m=%d 超过 GF(2^8) 上限 256", k+m)
	}
	return &Codec{k: k, m: m, gen: buildGenerator(k, m)}, nil
}

// K 返回数据片数。
func (c *Codec) K() int { return c.k }

// M 返回校验片数。
func (c *Codec) M() int { return c.m }

// Shards 返回分片总数 k+m。
func (c *Codec) Shards() int { return c.k + c.m }

// ShardSize 返回长度为 n 的数据编码后的分片长度 ceil(n/k)。
func (c *Codec) ShardSize(n int) int {
	if n <= 0 {
		return 0
	}
	return (n + c.k - 1) / c.k
}

// Encode 把 data 编码为 k+m 个等长分片。
//
// 返回的分片共享同一块连续内存，调用方不得单独释放或扩容其中一片。
// 编码是纯 CPU 操作，不修改 data。
func (c *Codec) Encode(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("ec: 不能编码空数据")
	}
	size := c.ShardSize(len(data))
	buf := make([]byte, size*c.Shards())
	shards := make([][]byte, c.Shards())
	for i := range shards {
		shards[i] = buf[i*size : (i+1)*size]
	}
	for i := 0; i < c.k; i++ {
		start := i * size
		if start >= len(data) {
			break
		}
		end := start + size
		if end > len(data) {
			end = len(data)
		}
		copy(shards[i], data[start:end])
	}
	for r := 0; r < c.m; r++ {
		row := shards[c.k+r]
		coeffs := c.gen[c.k+r]
		for j := 0; j < c.k; j++ {
			mulAdd(row, shards[j], coeffs[j])
		}
	}
	return shards, nil
}

// Reconstruct 用现存分片恢复缺失分片，就地写回 shards，并把 present 中
// 恢复出的位置标记为 true。
//
// 要求 len(shards)==len(present)==k+m，present[i]=true 的槽位必须非 nil
// 且长度一致。现存分片少于 k 个时返回错误。
//
// 实现分两步：先解出缺失的数据片（k×k 逆矩阵作用于前 k 个现存行），
// 再用完整的数据片重算缺失的校验片。这样数据片与校验片丢失都能处理。
func (c *Codec) Reconstruct(shards [][]byte, present []bool) error {
	n := c.Shards()
	if len(shards) != n || len(present) != n {
		return fmt.Errorf("ec: 分片数 %d/%d，期望 %d", len(shards), len(present), n)
	}
	var (
		alive   []int
		shardSz = -1
	)
	for i, ok := range present {
		if !ok {
			continue
		}
		if shards[i] == nil {
			return fmt.Errorf("ec: 分片 %d 标记为可用但内容为空", i)
		}
		if shardSz < 0 {
			shardSz = len(shards[i])
		} else if len(shards[i]) != shardSz {
			return fmt.Errorf("ec: 分片 %d 长度 %d 与其它分片 %d 不一致", i, len(shards[i]), shardSz)
		}
		alive = append(alive, i)
	}
	if shardSz < 0 {
		return errors.New("ec: 没有可用分片")
	}
	if len(alive) < c.k {
		return fmt.Errorf("ec: 仅 %d 个可用分片，需要 %d 个才能恢复", len(alive), c.k)
	}

	// 取前 k 个现存行的生成矩阵并求逆：data = P^-1 · 现存分片。
	alive = alive[:c.k]
	rows := make([][]byte, c.k)
	for i, idx := range alive {
		rows[i] = c.gen[idx]
	}
	inv, err := invertMatrix(rows)
	if err != nil {
		return fmt.Errorf("ec: 恢复矩阵不可逆: %w", err)
	}
	for j := 0; j < c.k; j++ {
		if present[j] {
			continue
		}
		recovered := make([]byte, shardSz)
		for t := 0; t < c.k; t++ {
			mulAdd(recovered, shards[alive[t]], inv[j][t])
		}
		shards[j] = recovered
		present[j] = true
	}

	// 数据片齐全后重算缺失的校验片。
	for r := 0; r < c.m; r++ {
		idx := c.k + r
		if present[idx] {
			continue
		}
		recovered := make([]byte, shardSz)
		for j := 0; j < c.k; j++ {
			mulAdd(recovered, shards[j], c.gen[idx][j])
		}
		shards[idx] = recovered
		present[idx] = true
	}
	return nil
}

// Verify 校验所有校验片与数据片一致，用于测试与诊断。
func (c *Codec) Verify(shards [][]byte) error {
	n := c.Shards()
	if len(shards) != n {
		return fmt.Errorf("ec: 分片数 %d，期望 %d", len(shards), n)
	}
	size := len(shards[0])
	if size == 0 {
		return errors.New("ec: 分片为空")
	}
	for i, s := range shards {
		if len(s) != size {
			return fmt.Errorf("ec: 分片 %d 长度 %d，期望 %d", i, len(s), size)
		}
	}
	want := make([]byte, size)
	for r := 0; r < c.m; r++ {
		for i := range want {
			want[i] = 0
		}
		for j := 0; j < c.k; j++ {
			mulAdd(want, shards[j], c.gen[c.k+r][j])
		}
		if !bytes.Equal(want, shards[c.k+r]) {
			return fmt.Errorf("ec: 校验片 %d 与数据片不一致", c.k+r)
		}
	}
	return nil
}
