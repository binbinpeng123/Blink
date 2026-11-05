package cluster

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/binbinpeng123/Blink/internal/store"
)

// newECNodes 起一组开启 EC 的节点。ECMinSize 设为 1，让测试用的小对象
// 也走分片路径；块大小保持 1 KiB，使对象自然跨多块。
func newECNodes(t *testing.T, count int, k, m int) []*testNode {
	t.Helper()
	return newTestNodesWithOptions(t, count, 1024, func(c *Config) {
		c.ECEnabled = true
		c.ECK, c.ECM = k, m
		c.ECMinSize = 1
	})
}

// shardPath 返回分片（CAS 块）在节点数据目录中的路径。
// 分片与整块共用同一套内容寻址布局，这是 EC 复用块层能力的全部前提。
func shardPath(n *testNode, hash string) string {
	return filepath.Join(n.st.Root(), "cas", hash[:2], hash)
}

// nodeByID 在测试节点集合中按 ID 查找。
func nodeByID(t *testing.T, nodes []*testNode, id string) *testNode {
	t.Helper()
	for _, n := range nodes {
		if n.id == id {
			return n
		}
	}
	t.Fatalf("找不到节点 %s", id)
	return nil
}

// TestECObjectRoundTripAcrossNodes 验证 EC 写入后任意节点可读、Range 正确，
// 且整块不再落在任何节点上（否则就退化成"分片 + 整块"的双份存储）。
func TestECObjectRoundTripAcrossNodes(t *testing.T) {
	nodes := newECNodes(t, 3, 2, 1)
	const bucket, key = "ec-bucket", "round-trip.bin"
	createBucketOnAll(t, nodes, bucket)

	payload := testPayload(5000)
	info := putObject(t, nodes[0].cl, bucket, key, payload)

	meta, found := localMeta(t, nodes[0], bucket, key)
	if !found {
		t.Fatal("节点未持有 meta")
	}
	if meta.Layout != store.LayoutEC || meta.EC == nil {
		t.Fatalf("布局 = %q ec=%v，期望 EC", meta.Layout, meta.EC)
	}
	if meta.EC.K != 2 || meta.EC.M != 1 || len(meta.EC.Group) != 3 {
		t.Fatalf("EC 参数 = %d+%d group=%v，期望 2+1 / 3 个成员",
			meta.EC.K, meta.EC.M, meta.EC.Group)
	}
	if len(meta.Blocks) < 2 {
		t.Fatalf("块数 = %d，期望跨多块", len(meta.Blocks))
	}

	for i, b := range meta.Blocks {
		if len(b.Shards) != 3 {
			t.Fatalf("块 %d 的分片数 = %d，期望 3", i, len(b.Shards))
		}
		for _, n := range nodes {
			ok, err := n.st.BlockExists(b.Hash)
			if err != nil {
				t.Fatalf("节点 %s 检查块 %s 失败: %v", n.id, b.Hash, err)
			}
			if ok {
				t.Fatalf("节点 %s 上存在整块 %s：EC 路径不应写整块", n.id, b.Hash)
			}
		}
	}

	// 每个节点都能读：meta 三副本、分片按组还原。
	for _, n := range nodes {
		if got := readAll(t, n.cl, bucket, key); !bytes.Equal(got, payload) {
			t.Fatalf("节点 %s 读回的 EC 对象内容不一致", n.id)
		}
	}

	// Range 读跨越两个块，验证"整块重建后切片输出"。
	r, err := nodes[1].cl.OpenObject(bucket, key)
	if err != nil {
		t.Fatalf("打开对象失败: %v", err)
	}
	var buf bytes.Buffer
	if err := r.WriteRangeTo(&buf, 1500, 2500); err != nil {
		t.Fatalf("Range 读失败: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), payload[1500:4000]) {
		t.Fatal("Range 读内容不一致")
	}
	if info.Version == 0 {
		t.Fatal("写入返回的版本号为 0")
	}
}

// TestECReadAfterShardLoss 验证丢 1 片（m=1）仍可读，并且读路径把缺片
// 交给修复队列后分片会被重建回磁盘。
func TestECReadAfterShardLoss(t *testing.T) {
	nodes := newECNodes(t, 3, 2, 1)
	const bucket, key = "ec-bucket", "shard-loss.bin"
	createBucketOnAll(t, nodes, bucket)

	payload := testPayload(4000)
	putObject(t, nodes[0].cl, bucket, key, payload)

	meta, _ := localMeta(t, nodes[0], bucket, key)
	b := meta.Blocks[0]
	// 下标 1 是数据片：丢掉它必须靠校验片重建，才真正覆盖解码路径。
	loser := nodeByID(t, nodes, meta.EC.Group[1])
	path := shardPath(loser, b.Shards[1].Hash)
	if err := os.Remove(path); err != nil {
		t.Fatalf("删除分片失败: %v", err)
	}

	for _, n := range nodes {
		if got := readAll(t, n.cl, bucket, key); !bytes.Equal(got, payload) {
			t.Fatalf("丢片后节点 %s 读回内容不一致", n.id)
		}
	}

	waitFor(t, 15*time.Second, "读修复重建分片", func() bool {
		ok, err := loser.st.BlockExists(b.Shards[1].Hash)
		return err == nil && ok
	})
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("分片未恢复: %v", err)
	}
}

// TestECTwoShardsLostUnavailable 验证丢 2 片超过 m=1 的容错能力时明确
// 报 ErrUnavailable，而不是返回一段残缺内容。
func TestECTwoShardsLostUnavailable(t *testing.T) {
	nodes := newECNodes(t, 3, 2, 1)
	const bucket, key = "ec-bucket", "two-shards.bin"
	createBucketOnAll(t, nodes, bucket)

	payload := testPayload(3000)
	putObject(t, nodes[0].cl, bucket, key, payload)

	meta, _ := localMeta(t, nodes[0], bucket, key)
	b := meta.Blocks[0]
	for i := 0; i < 2; i++ {
		owner := nodeByID(t, nodes, meta.EC.Group[i])
		if err := os.Remove(shardPath(owner, b.Shards[i].Hash)); err != nil {
			t.Fatalf("删除分片 %d 失败: %v", i, err)
		}
	}

	r, err := nodes[0].cl.OpenObject(bucket, key)
	if err != nil {
		t.Fatalf("打开对象失败: %v", err)
	}
	var buf bytes.Buffer
	err = r.WriteRangeTo(&buf, 0, r.Info().Size)
	if !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("读取错误 = %v，期望 ErrUnavailable", err)
	}
}

// TestECScrubRebuildsForeignShard 验证反熵能发现"只做分片存储、不在副本集
// 上"的节点缺片并补齐 —— 这类节点没有 meta，按副本探测的循环看不到它，
// 只能靠分片组扫描。
func TestECScrubRebuildsForeignShard(t *testing.T) {
	// 6 节点 4+2：组成员 6 个、副本 3 个，下标 3–5 的分片组员天然不在副本集。
	nodes := newECNodes(t, 6, 4, 2)
	const bucket, key = "ec-bucket", "scrub.bin"
	createBucketOnAll(t, nodes, bucket)

	payload := testPayload(3000)
	putObject(t, nodes[0].cl, bucket, key, payload)

	meta, _ := localMeta(t, nodes[0], bucket, key)
	b := meta.Blocks[0]
	groupOnly := nodes[len(nodes)-1].id
	found := false
	for i, gid := range meta.EC.Group {
		if gid == groupOnly {
			found = true
			if err := os.Remove(shardPath(nodeByID(t, nodes, gid), b.Shards[i].Hash)); err != nil {
				t.Fatalf("删除分片 %d 失败: %v", i, err)
			}
			break
		}
	}
	if !found {
		t.Fatalf("节点 %s 不在分片组 %v 中", groupOnly, meta.EC.Group)
	}

	primary := nodes[0].cl.ring.Replicas(ReplicaKey(bucket, key), 1)[0]
	if primary == groupOnly {
		t.Fatalf("测试前提不成立：primary 与分片目标相同（%s）", primary)
	}
	owner := nodeByID(t, nodes, primary)
	owner.cl.repair.scrubOnce()

	// 反熵只入队；真正的重建由后台 worker 完成。
	target := nodeByID(t, nodes, groupOnly)
	waitFor(t, 20*time.Second, "反熵发现并修复缺片", func() bool {
		ok, err := target.st.BlockExists(b.Shards[indexInGroup(t, meta, groupOnly)].Hash)
		return err == nil && ok
	})

	if got := readAll(t, nodes[2].cl, bucket, key); !bytes.Equal(got, payload) {
		t.Fatal("修复后读回内容不一致")
	}
}

// indexInGroup 返回节点在分片组中的下标。
func indexInGroup(t *testing.T, meta *store.ObjectMeta, id string) int {
	t.Helper()
	for i, gid := range meta.EC.Group {
		if gid == id {
			return i
		}
	}
	t.Fatalf("节点 %s 不在分片组中", id)
	return -1
}
