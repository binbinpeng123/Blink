package cluster

import (
	"errors"
	"testing"

	"github.com/binbinpeng123/Blink/internal/store"
)

// TestBucketVisibleAcrossNodes 验证"在 A 建桶、经 B 写入、从 C 读取"，
// 这是桶控制面广播的直接验收。
func TestBucketVisibleAcrossNodes(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket = "shared-bucket"

	if err := nodes[0].cl.CreateBucket(bucket); err != nil {
		t.Fatalf("建桶失败: %v", err)
	}

	for _, n := range nodes {
		ok, err := n.cl.BucketExists(bucket)
		if err != nil || !ok {
			t.Fatalf("节点 %s 看不到桶（exists=%v err=%v）", n.id, ok, err)
		}
		names, err := n.cl.ListBuckets()
		if err != nil {
			t.Fatalf("节点 %s 列举桶失败: %v", n.id, err)
		}
		if len(names) != 1 || names[0] != bucket {
			t.Fatalf("节点 %s 列举桶 = %v，期望 [%s]", n.id, names, bucket)
		}
	}

	// 桶只在 node-1 上由协调者创建：经 node-2 写入、从 node-3 读取。
	putObject(t, nodes[1].cl, bucket, "dir/obj.bin", testPayload(777))
	if got := readAll(t, nodes[2].cl, bucket, "dir/obj.bin"); len(got) != 777 {
		t.Fatalf("读取长度 = %d，期望 777", len(got))
	}

	// 重复建桶必须冲突，"第一个建的人拥有这个桶"。
	if err := nodes[2].cl.CreateBucket(bucket); !errors.Is(err, store.ErrBucketExists) {
		t.Fatalf("重复建桶错误 = %v，期望 ErrBucketExists", err)
	}
}

// TestBucketCreatedAtFallback 验证本地没有桶元数据时能向邻居取值。
//
// 场景：建桶时本节点不可达，之后它参加了读流量。
func TestBucketCreatedAtFallback(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket = "created-at"
	createBucketOnAll(t, nodes, bucket)

	want, err := nodes[0].cl.BucketCreatedAt(bucket)
	if err != nil {
		t.Fatalf("读取本地创建时间失败: %v", err)
	}

	// 抹掉 node-3 的本地桶元数据（广播本应写下它），
	// 让 BucketCreatedAt 必然走远端分支。
	if err := nodes[2].st.DeleteBucket(bucket); err != nil {
		t.Fatalf("删除 node-3 的本地桶失败: %v", err)
	}
	got, err := nodes[2].cl.BucketCreatedAt(bucket)
	if err != nil {
		t.Fatalf("回退查询创建时间失败: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("创建时间 = %v，期望 %v", got, want)
	}
}

// TestDeleteBucketRequiresEmptyCluster 验证"对象未清空时删桶"返回
// ErrBucketNotEmpty；对象全部删除（只剩墓碑）后可以删桶。
func TestDeleteBucketRequiresEmptyCluster(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket = "non-empty"
	createBucketOnAll(t, nodes, bucket)

	putObject(t, nodes[0].cl, bucket, "a.bin", testPayload(64))
	// 对象可能只落在部分节点：任何一个节点非空都必须拒绝删桶。
	if err := nodes[1].cl.DeleteBucket(bucket); !errors.Is(err, store.ErrBucketNotEmpty) {
		t.Fatalf("非空桶删除错误 = %v，期望 ErrBucketNotEmpty", err)
	}

	// 删除对象只留下墓碑：墓碑不是对象，不能挡住桶的删除。
	if err := nodes[2].cl.DeleteObject(bucket, "a.bin"); err != nil {
		t.Fatalf("删除对象失败: %v", err)
	}
	if err := nodes[0].cl.DeleteBucket(bucket); err != nil {
		t.Fatalf("清空后删桶失败: %v", err)
	}
	for _, n := range nodes {
		if ok, err := n.cl.BucketExists(bucket); err != nil || ok {
			t.Errorf("节点 %s 删除后仍看到桶（exists=%v err=%v）", n.id, ok, err)
		}
	}

	// 删除不存在的桶：与对象删除不同，这里必须报 NoSuchBucket。
	if err := nodes[0].cl.DeleteBucket(bucket); !errors.Is(err, store.ErrBucketNotFound) {
		t.Fatalf("重复删桶错误 = %v，期望 ErrBucketNotFound", err)
	}
}

// TestDeleteBucketFailsWhenNodeUnreachable 验证删桶要求全部节点可达。
//
// 与对象数据面相反：宁可回 503，也不接受"宕机节点带着旧桶和旧对象回来"。
func TestDeleteBucketFailsWhenNodeUnreachable(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket = "no-quorum-delete"
	createBucketOnAll(t, nodes, bucket)

	nodes[2].Close()
	if err := nodes[0].cl.DeleteBucket(bucket); !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("节点不可达时删桶错误 = %v，期望 ErrUnavailable", err)
	}
}

// TestCORSBroadcast 验证 CORS 配置写入任意节点后其余节点都能读到，
// 并且在任意节点删除后其余节点也读不到。
func TestCORSBroadcast(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket = "cors-bucket"
	createBucketOnAll(t, nodes, bucket)

	cfg := &store.CORSConfiguration{Rules: []store.CORSRule{{
		ID:             "browser",
		AllowedOrigins: []string{"https://example.com"},
		AllowedMethods: []string{"GET", "PUT"},
		AllowedHeaders: []string{"*"},
	}}}
	if err := nodes[0].cl.PutBucketCORS(bucket, cfg); err != nil {
		t.Fatalf("写入 CORS 失败: %v", err)
	}
	for _, n := range nodes {
		got, err := n.cl.GetBucketCORS(bucket)
		if err != nil {
			t.Fatalf("节点 %s 读取 CORS 失败: %v", n.id, err)
		}
		if len(got.Rules) != 1 || got.Rules[0].ID != "browser" {
			t.Fatalf("节点 %s 的 CORS 配置 = %+v", n.id, got)
		}
	}

	if err := nodes[2].cl.DeleteBucketCORS(bucket); err != nil {
		t.Fatalf("删除 CORS 失败: %v", err)
	}
	for _, n := range nodes {
		if _, err := n.cl.GetBucketCORS(bucket); !errors.Is(err, store.ErrNoSuchCORSConfiguration) {
			t.Errorf("节点 %s 删除后读取 CORS 错误 = %v，期望 ErrNoSuchCORSConfiguration", n.id, err)
		}
	}

	// 不存在的桶不能接受 CORS 写入。
	if err := nodes[0].cl.PutBucketCORS("never-created", cfg); !errors.Is(err, store.ErrBucketNotFound) {
		t.Fatalf("不存在的桶写入 CORS 错误 = %v，期望 ErrBucketNotFound", err)
	}
}
