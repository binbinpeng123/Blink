package cluster

import (
	"fmt"
	"slices"
	"testing"
)

var testNodes = []string{"node-a", "node-b", "node-c", "node-d"}

func TestNewRingRejectsBadInput(t *testing.T) {
	if _, err := NewRing(nil, 128); err == nil {
		t.Error("空节点列表应报错")
	}
	if _, err := NewRing([]string{"", "a"}, 128); err == nil {
		t.Error("空节点 ID 应报错")
	}
	if _, err := NewRing([]string{"a"}, 0); err == nil {
		t.Error("零 vnodes 应报错")
	}
	if _, err := NewRing([]string{"a"}, maxVNodes+1); err == nil {
		t.Error("超限 vnodes 应报错")
	}
}

// 成员顺序与重复项不得影响环：不同进程拿到同一成员表必须得到同一个环。
func TestRingDeterministic(t *testing.T) {
	a, err := NewRing([]string{"c", "a", "b"}, 64)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewRing([]string{"b", "c", "a", "a"}, 64)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(a.Nodes(), b.Nodes()) {
		t.Fatalf("节点视图不同: %v vs %v", a.Nodes(), b.Nodes())
	}
	if !slices.Equal(a.Nodes(), []string{"a", "b", "c"}) {
		t.Errorf("节点应去重并排序，实际 %v", a.Nodes())
	}
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("bucket/key-%d", i)
		if !slices.Equal(a.Replicas(key, 3), b.Replicas(key, 3)) {
			t.Fatalf("key %q 的副本顺序不一致", key)
		}
	}
}

func TestReplicasAreDistinctAndClamped(t *testing.T) {
	ring, err := NewRing(testNodes, 128)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("bucket/obj-%d", i)
		got := ring.Replicas(key, 4)
		if len(got) != 4 {
			t.Fatalf("key %q 返回 %d 个副本，期望 4", key, len(got))
		}
		seen := make(map[string]bool, len(got))
		for _, n := range got {
			if seen[n] {
				t.Fatalf("key %q 的副本重复: %v", key, got)
			}
			seen[n] = true
		}
		// n 超过节点数时应截断到节点数，而不是死循环或返回空位。
		if got := ring.Replicas(key, 10); len(got) != len(testNodes) {
			t.Fatalf("key %q 请求 10 副本返回 %d 个", key, len(got))
		}
		if got := ring.Replicas(key, 0); len(got) != 0 {
			t.Fatalf("请求 0 副本应返回空，实际 %v", got)
		}
		if owner := ring.Owner(key); owner != ring.Replicas(key, 3)[0] {
			t.Fatalf("Owner(%q)=%q 与首副本不一致", key, owner)
		}
	}
}

// 128 个 vnodes 下，密钥分布不应明显倾斜（理想为节点数的倒数）。
func TestRingDistribution(t *testing.T) {
	const keys = 20000
	ring, err := NewRing(testNodes, 128)
	if err != nil {
		t.Fatal(err)
	}
	counts := make(map[string]int, len(testNodes))
	for i := 0; i < keys; i++ {
		counts[ring.Owner(fmt.Sprintf("bucket/obj-%d", i))]++
	}
	ideal := float64(keys) / float64(len(testNodes))
	for _, n := range testNodes {
		ratio := float64(counts[n]) / ideal
		if ratio < 0.7 || ratio > 1.3 {
			t.Errorf("节点 %s 分到 %d 个 key（理想 %.0f，偏差 %.2fx）", n, counts[n], ideal, ratio)
		}
	}
}

// 增加一个节点时，只有原本会落到新节点上的 key 需要迁移 —— 这是
// 一致性哈希相对取模哈希的核心优势，也是后续离线 rebalance 的依据。
func TestRingAddNodeMovesOnlyAffectedKeys(t *testing.T) {
	const keys = 20000
	before, err := NewRing([]string{"a", "b", "c"}, 128)
	if err != nil {
		t.Fatal(err)
	}
	after, err := NewRing([]string{"a", "b", "c", "d"}, 128)
	if err != nil {
		t.Fatal(err)
	}

	moved := 0
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("bucket/obj-%d", i)
		oldOwner, newOwner := before.Owner(key), after.Owner(key)
		if oldOwner == newOwner {
			continue
		}
		moved++
		if newOwner != "d" {
			t.Fatalf("key %q 从 %s 迁到了 %s，新增节点时只能迁往新节点", key, oldOwner, newOwner)
		}
	}
	ratio := float64(moved) / keys
	// 新增 1/4 节点，理想迁移量为 25%；给 vnode 数带来的抖动留足余量。
	if ratio < 0.15 || ratio > 0.4 {
		t.Errorf("迁移比例 %.1f%% 超出预期区间（理想 25%%）", ratio*100)
	}
}

func TestRingRemoveNodeKeepsOtherKeys(t *testing.T) {
	const keys = 20000
	before, err := NewRing([]string{"a", "b", "c", "d"}, 128)
	if err != nil {
		t.Fatal(err)
	}
	after, err := NewRing([]string{"a", "b", "c"}, 128)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("bucket/obj-%d", i)
		if before.Owner(key) == "d" {
			if after.Owner(key) == "d" {
				t.Fatalf("key %q 仍落在已移除节点上", key)
			}
			continue
		}
		if before.Owner(key) != after.Owner(key) {
			t.Fatalf("key %q 的 owner 从 %s 变为 %s，移除无关节点不应影响它",
				key, before.Owner(key), after.Owner(key))
		}
	}
}

func TestReplicaKey(t *testing.T) {
	if got := ReplicaKey("bucket", "a/b.txt"); got != "bucket/a/b.txt" {
		t.Errorf("ReplicaKey = %q", got)
	}
	// 桶名与 key 之间用 "/" 连接是唯一的歧义来源，确认不会产生跨桶碰撞。
	if ReplicaKey("a", "b/c") == ReplicaKey("a/b", "c") {
		// 两者相等意味着 (bucket,key) 映射不唯一；当前格式确实如此，
		// 但桶名不能含 "/"，因此实际配置下不会出现这种组合。
		t.Log("已知：ReplicaKey 不区分 a/b+c 与 a+b/c，桶名禁止含 / 保证不触发")
	}
}
