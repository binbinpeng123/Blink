package cluster

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/binbinpeng123/Blink/internal/store"
)

// 本文件覆盖切片 6 的验收点：单节点故障不影响读写、故障恢复后修复
// 队列把副本补齐、以及三道防线（读修复 / hint 队列 / 反熵扫描）各自的
// 触发条件。所有用例都通过真实 HTTP 内部协议进行节点间通信。

// waitFor 轮询直到条件成立或超时。
//
// 修复是异步的：断言"某件事最终会发生"时轮询是唯一不依赖内部实现的
// 做法 —— 用 sleep 固定等待时长，在慢机器上会随机失败。
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待 %s 超时（%s）", what, timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// localMeta 读取节点本地副本元数据；读取失败直接终止测试。
func localMeta(t *testing.T, n *testNode, bucket, key string) (*store.ObjectMeta, bool) {
	t.Helper()
	m, found, err := n.st.LoadReplicaMeta(bucket, key)
	if err != nil {
		t.Fatalf("节点 %s 读取 %s 的本地副本 meta 失败: %v", n.id, key, err)
	}
	return m, found
}

// hintFiles 列出数据目录下所有修复 hint 文件。
func hintFiles(t *testing.T, root string) []string {
	t.Helper()
	dir := filepath.Join(root, "cluster", "hints")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		t.Fatalf("读取 hint 目录 %s 失败: %v", dir, err)
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		files, err := os.ReadDir(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("读取 hint 子目录失败: %v", err)
		}
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			out = append(out, filepath.Join(dir, e.Name(), f.Name()))
		}
	}
	return out
}

// TestSurvivesOneNodeDownAndRepairsAfterRestart 验证切片 6 的核心：
// 1 个节点宕机时读写照常，节点恢复后缺的副本与块被自动补齐。
func TestSurvivesOneNodeDownAndRepairsAfterRestart(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket, key = "test-bucket", "during-outage.bin"
	createBucketOnAll(t, nodes, bucket)

	payload := testPayload(3*1024 + 17) // 跨多个块
	putObject(t, nodes[0].cl, bucket, "before.bin", payload)

	nodes[2].SetDown(true)

	// W=2 由其余两个节点满足：宕机节点既不该阻塞写入，也不该阻塞读取。
	info := putObject(t, nodes[0].cl, bucket, key, payload)
	if _, found := localMeta(t, nodes[2], bucket, key); found {
		t.Fatal("宕机节点不应持有宕机期间写入的对象")
	}
	for _, n := range nodes[:2] {
		if got := readAll(t, n.cl, bucket, key); !bytes.Equal(got, payload) {
			t.Errorf("从节点 %s 读取内容不一致", n.id)
		}
	}

	// 恢复：不需要任何人工干预，hint 队列应当把 meta 与全部块补齐。
	nodes[2].SetDown(false)

	waitFor(t, 15*time.Second, "宕机节点补齐副本", func() bool {
		m, found := localMeta(t, nodes[2], bucket, key)
		if !found || m.ObjVersion != info.Version {
			return false
		}
		for _, b := range m.Blocks {
			ok, err := nodes[2].st.BlockExists(b.Hash)
			if err != nil || !ok {
				return false
			}
		}
		return true
	})

	if got := readAll(t, nodes[2].cl, bucket, key); !bytes.Equal(got, payload) {
		t.Fatal("恢复后的节点读取内容不一致")
	}

	// 修复成功后 hint 必须被删除：它是一次性的待办，不是长期状态。
	waitFor(t, 5*time.Second, "修复 hint 清空", func() bool {
		return len(hintFiles(t, nodes[0].st.Root())) == 0
	})
}

// TestDeleteWhileNodeDownRepairsTombstone 验证删除墓碑也会进入修复队列。
//
// 这条路径容易被忽略：删除只需要 W 个 ack，宕机节点恢复后会带着旧副本
// 回来。没有墓碑修复，"删掉的对象"会在故障节点上复活。
func TestDeleteWhileNodeDownRepairsTombstone(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket, key = "test-bucket", "deleted-during-outage.bin"
	createBucketOnAll(t, nodes, bucket)
	putObject(t, nodes[0].cl, bucket, key, testPayload(1200))

	nodes[2].SetDown(true)
	if err := nodes[0].cl.DeleteObject(bucket, key); err != nil {
		t.Fatalf("宕机期间的删除失败: %v", err)
	}
	nodes[2].SetDown(false)

	waitFor(t, 15*time.Second, "墓碑补齐到恢复节点", func() bool {
		m, found := localMeta(t, nodes[2], bucket, key)
		return found && m.Deleted
	})

	for _, n := range nodes {
		if _, err := n.cl.OpenObject(bucket, key); !errors.Is(err, store.ErrNoSuchKey) {
			t.Errorf("节点 %s 读取已删除对象错误 = %v，期望 ErrNoSuchKey", n.id, err)
		}
	}
}

// TestReadRepairRestoresLostMeta 验证读路径顺带修复：某副本 meta 丢失时，
// 一次普通读取就会把它补回来，不需要等反熵扫描。
func TestReadRepairRestoresLostMeta(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket, key = "test-bucket", "lost-meta.bin"
	createBucketOnAll(t, nodes, bucket)

	payload := testPayload(2500)
	info := putObject(t, nodes[0].cl, bucket, key, payload)

	if _, err := nodes[2].st.DeleteReplicaMeta(bucket, key); err != nil {
		t.Fatalf("删除本地副本 meta 失败: %v", err)
	}
	if _, found := localMeta(t, nodes[2], bucket, key); found {
		t.Fatal("删除本地副本 meta 未生效")
	}

	// 从协调者读一次：探测发现节点 3 缺 meta，读修复入队。
	if got := readAll(t, nodes[0].cl, bucket, key); !bytes.Equal(got, payload) {
		t.Fatal("读取内容不一致")
	}

	waitFor(t, 15*time.Second, "读修复补齐 meta", func() bool {
		m, found := localMeta(t, nodes[2], bucket, key)
		return found && m.ObjVersion == info.Version && !m.Deleted
	})
}

// TestReadRepairRestoresMissingBlock 验证读路径发现块文件丢失时，
// 会从其它副本搬回缺失的块，而读取本身仍然成功。
func TestReadRepairRestoresMissingBlock(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket, key = "test-bucket", "lost-block.bin"
	createBucketOnAll(t, nodes, bucket)

	payload := testPayload(4096 + 77) // 5 块
	putObject(t, nodes[0].cl, bucket, key, payload)

	meta, found := localMeta(t, nodes[1], bucket, key)
	if !found || len(meta.Blocks) < 2 {
		t.Fatalf("节点 2 的副本异常: found=%v blocks=%d", found, len(meta.Blocks))
	}
	missing := meta.Blocks[0].Hash
	// 直接删掉 CAS 文件，模拟磁盘损坏/被清理。meta 仍在，这正是
	// "有 meta 缺块"的形态 —— 读路径必须能从别的副本兜底。
	path := filepath.Join(nodes[1].st.Root(), "cas", missing[:2], missing)
	if err := os.Remove(path); err != nil {
		t.Fatalf("删除块文件失败: %v", err)
	}

	// 从缺块节点自己读：本地块读失败 → 回落到远端副本 → 读修复入队。
	if got := readAll(t, nodes[1].cl, bucket, key); !bytes.Equal(got, payload) {
		t.Fatal("缺块时的读取内容不一致")
	}

	waitFor(t, 15*time.Second, "缺失块被搬回", func() bool {
		ok, err := nodes[1].st.BlockExists(missing)
		return err == nil && ok
	})
	if got := readAll(t, nodes[1].cl, bucket, key); !bytes.Equal(got, payload) {
		t.Fatal("修复后读取内容不一致")
	}
}

// TestScrubRepairsOnlyOwnedKeys 验证反熵扫描的确定性 owner 约束：
// 每个 key 只由其环上 primary 负责扫描，避免所有节点同时扫同一份数据。
func TestScrubRepairsOnlyOwnedKeys(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket, key = "test-bucket", "scrub-owned.bin"
	createBucketOnAll(t, nodes, bucket)
	putObject(t, nodes[0].cl, bucket, key, testPayload(700))

	byID := make(map[string]*testNode, len(nodes))
	for _, n := range nodes {
		byID[n.id] = n
	}
	ownerID := nodes[0].cl.ring.Replicas(ReplicaKey(bucket, key), 1)[0]
	owner := byID[ownerID]
	if owner == nil {
		t.Fatalf("primary %s 不在测试节点中", ownerID)
	}

	var missing, nonOwner *testNode
	for _, n := range nodes {
		if n.id == owner.id {
			continue
		}
		if missing == nil {
			missing = n
			continue
		}
		nonOwner = n
	}
	if missing == nil || nonOwner == nil {
		t.Fatalf("未能从 3 个节点中选出 owner=%s 之外的节点", owner.id)
	}

	if _, err := missing.st.DeleteReplicaMeta(bucket, key); err != nil {
		t.Fatalf("删除 %s 的副本 meta 失败: %v", missing.id, err)
	}

	// 非 owner 扫描：即使它本地看得到同一个 key，也不该为它做修复。
	nonOwner.cl.repair.scrubOnce()
	time.Sleep(300 * time.Millisecond)
	if _, found := localMeta(t, missing, bucket, key); found {
		t.Fatalf("非 owner 节点 %s 不应修复 key 的副本", nonOwner.id)
	}

	// owner 扫描：发现缺 meta 的副本并入队修复。
	owner.cl.repair.scrubOnce()
	waitFor(t, 15*time.Second, "owner 扫描补齐副本", func() bool {
		_, found := localMeta(t, missing, bucket, key)
		return found
	})
}

// TestRepairHintsPersistAcrossRestart 验证 hint 落盘：进程重启后，
// 未完成的修复任务仍然存在并能继续执行。节点恢复通常正是重启的结果，
// 只在内存里记待办会让修复在重启时整体丢失。
func TestRepairHintsPersistAcrossRestart(t *testing.T) {
	nodes := newTestNodes(t, 3)
	const bucket, key = "test-bucket", "persisted-hint.bin"
	createBucketOnAll(t, nodes, bucket)

	nodes[2].SetDown(true)
	info := putObject(t, nodes[0].cl, bucket, key, testPayload(2048))

	hints := hintFiles(t, nodes[0].st.Root())
	if len(hints) != 1 {
		t.Fatalf("hint 文件数 = %d，期望 1（%v）", len(hints), hints)
	}
	data, err := os.ReadFile(hints[0])
	if err != nil {
		t.Fatalf("读取 hint 文件失败: %v", err)
	}
	if !strings.Contains(string(data), `"target_node":"node-3"`) {
		t.Fatalf("hint 内容异常: %s", data)
	}

	// 模拟进程重启：停掉旧 Cluster，用同一个数据目录与配置重建。
	nodes[0].cl.Close()
	cl, err := New(nodes[0].cfg, nodes[0].st, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("重建 Cluster 失败: %v", err)
	}
	nodes[0].attachCluster(cl)

	nodes[2].SetDown(false)

	waitFor(t, 15*time.Second, "重启后继续修复", func() bool {
		m, found := localMeta(t, nodes[2], bucket, key)
		return found && m.ObjVersion == info.Version
	})
	waitFor(t, 5*time.Second, "重启后 hint 清空", func() bool {
		return len(hintFiles(t, nodes[0].st.Root())) == 0
	})
}

// TestReadDegradedServesWithTwoNodesDown 验证降级读：默认关闭时副本不足
// 回 503（见 TestReplicatedUnavailableWhenQuorumLost），显式打开后可以
// 从唯一可用的副本读到数据。
func TestReadDegradedServesWithTwoNodesDown(t *testing.T) {
	nodes := newTestNodesWithOptions(t, 3, 1024, func(c *Config) {
		c.ReadDegraded = true
	})
	const bucket, key = "test-bucket", "degraded.bin"
	createBucketOnAll(t, nodes, bucket)

	payload := testPayload(1800)
	putObject(t, nodes[0].cl, bucket, key, payload)

	nodes[1].Close()
	nodes[2].Close()

	// 只剩 1 个副本响应：read_degraded 允许读出这份数据。
	if got := readAll(t, nodes[0].cl, bucket, key); !bytes.Equal(got, payload) {
		t.Fatal("降级读内容不一致")
	}
}
