package store

import (
	"errors"
	"testing"
)

// TestOpenObjectRejectsStructurallyCorruptMeta 验证"JSON 合法但结构不自洽"
// 的元数据在读取入口就被拦下。
//
// 过去读路径只做 JSON 解析：块表被截断后，WriteRangeTo 会按剩下的块
// 少写一段就正常返回，HTTP 层只表现为连接被提前关闭，排查时看不到
// 任何"损坏"信号。现在必须得到 ErrCorrupt。
func TestOpenObjectRejectsStructurallyCorruptMeta(t *testing.T) {
	const body = "0123456789abcdef0123" // 20 字节：8 字节块 → 3 块
	cases := []struct {
		name   string
		damage func(m *ObjectMeta)
	}{
		{
			name: "块表被截断",
			damage: func(m *ObjectMeta) {
				m.Blocks = m.Blocks[:len(m.Blocks)-1]
			},
		},
		{
			name: "块偏移出现空洞",
			damage: func(m *ObjectMeta) {
				m.Blocks[1].Offset++
			},
		},
		{
			name: "块长度非正",
			damage: func(m *ObjectMeta) {
				m.Blocks[0].Length = 0
			},
		},
		{
			name: "分片边界总长不符",
			damage: func(m *ObjectMeta) {
				m.Parts = []PartDesc{{Number: 1, Size: m.Size - 1}}
			},
		},
		{
			name: "未知数据布局",
			damage: func(m *ObjectMeta) {
				m.Layout = "weird"
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newSmallBlockStore(t, 8, 4)
			putString(t, s, "bkt", "obj", body)

			// 基线：健康对象必须能完整读回，证明后续失败来自注入的损坏。
			if got := string(readObject(t, s, "bkt", "obj")); got != body {
				t.Fatalf("基线读取 = %q", got)
			}

			m, err := s.loadObjectMeta("bkt", "obj")
			if err != nil {
				t.Fatalf("读取 meta 失败: %v", err)
			}
			c.damage(m)
			if err := s.writeFileAtomic(s.objectMetaPath("bkt", "obj"), mustJSON(m)); err != nil {
				t.Fatalf("写入损坏 meta 失败: %v", err)
			}
			s.metaCache.delete(metaCacheKey{bucket: "bkt", key: "obj"})

			if _, err := s.OpenObject("bkt", "obj"); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("OpenObject 错误 = %v，期望 ErrCorrupt", err)
			}
			// COPY 源也走同一入口：拒绝复制出指向残缺块表的对象。
			if _, err := s.CopyObject("bkt", "dst", "bkt", "obj", CopyOptions{}); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("CopyObject 错误 = %v，期望 ErrCorrupt", err)
			}
		})
	}
}
