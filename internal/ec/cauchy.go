package ec

import (
	"fmt"
)

// 本文件构造系统码生成矩阵，并在丢失分片时求恢复矩阵。
//
// 生成矩阵 G 是 (k+m)×k 的柯西矩阵：
//
//	       ┌            ┐
//	       │   I_k      │   ← k 个数据分片原样保留（系统码）
//	G  =   │            │
//	       │ C (m×k)    │   ← m 个校验分片
//	       └            ┘
//
//	C[i][j] = 1 / (x_i ⊕ y_j)，取 x_i = i（0≤i<m）、y_j = m+j（0≤j<k）。
//	x_i 两两不同、y_j 两两不同、且 x 与 y 不相交，因此 C 是柯西矩阵，
//	[I; C] 的任意 k 行线性无关 —— 这正是"任意丢 m 片仍可恢复"的依据。
//
// 相比范德蒙德矩阵，柯西矩阵的系数只依赖下标，不需要求幂；
// 求逆时矩阵规模也只有 k×k，边端设备上开销可以忽略。

// buildGenerator 返回 (k+m)×k 的生成矩阵。
//
// k+m 必须 ≤256，调用方（NewCodec）已校验。
func buildGenerator(k, m int) [][]byte {
	g := make([][]byte, k+m)
	for i := 0; i < k; i++ {
		row := make([]byte, k)
		row[i] = 1
		g[i] = row
	}
	for r := 0; r < m; r++ {
		row := make([]byte, k)
		x := byte(r)
		for j := 0; j < k; j++ {
			// x_i ≠ y_j 由 i<m≤m+j 保证，分母非零。
			row[j] = gfInv(x ^ byte(m+j))
		}
		g[k+r] = row
	}
	return g
}

// invertMatrix 用高斯-约当消元求 n×n 方阵的逆。
//
// 只用于恢复路径（读时丢失分片、修复重建），不在编码热路径上，
// 因此直接做 O(n^3) 消元而不预计算更多表。
func invertMatrix(a [][]byte) ([][]byte, error) {
	n := len(a)
	if n == 0 {
		return nil, fmt.Errorf("ec: 空矩阵不可逆")
	}
	m := make([][]byte, n)
	inv := make([][]byte, n)
	for i := 0; i < n; i++ {
		if len(a[i]) != n {
			return nil, fmt.Errorf("ec: 矩阵第 %d 行长度为 %d，期望 %d", i, len(a[i]), n)
		}
		m[i] = append([]byte(nil), a[i]...)
		inv[i] = make([]byte, n)
		inv[i][i] = 1
	}

	for col := 0; col < n; col++ {
		pivot := -1
		for row := col; row < n; row++ {
			if m[row][col] != 0 {
				pivot = row
				break
			}
		}
		if pivot < 0 {
			return nil, fmt.Errorf("ec: 矩阵奇异（第 %d 列无主元）", col)
		}
		m[col], m[pivot] = m[pivot], m[col]
		inv[col], inv[pivot] = inv[pivot], inv[col]

		if pv := m[col][col]; pv != 1 {
			scale := gfInv(pv)
			for j := 0; j < n; j++ {
				m[col][j] = gfMul(m[col][j], scale)
				inv[col][j] = gfMul(inv[col][j], scale)
			}
		}
		for row := 0; row < n; row++ {
			if row == col {
				continue
			}
			factor := m[row][col]
			if factor == 0 {
				continue
			}
			for j := 0; j < n; j++ {
				m[row][j] ^= gfMul(factor, m[col][j])
				inv[row][j] ^= gfMul(factor, inv[col][j])
			}
		}
	}
	return inv, nil
}
