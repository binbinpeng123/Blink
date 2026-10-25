package store

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// crashChildEnv 把测试二进制切换成"崩溃写手"模式。
//
// 真杀进程是 faultFS 之外的第二种故障手段：faultFS 验证的是"某个步骤返回
// 错误时代码怎么处理"，而 SIGKILL 验证的是"进程在任意指令处消失后，
// 盘上留下什么"。后者无法靠注入错误模拟 —— 提交流程中的 rename 已经完成、
// 但后续代码一行都没跑的情况，只有真杀才能复现。
const crashChildEnv = "BLINK_CRASH_CHILD_DIR"

// crashPayload 生成第 i 个对象的确定内容。
// 长度 150 KiB 在 64 KiB 块大小下切出 3 个块，单次写入会经过多次
// 写块 → fsync → rename → fsync(dir) 序列。
// 注意 j>>16 这一项：只用 j*31 的话内容周期是 256 字节，而 64 KiB 是
// 256 的整数倍，同一个对象的相邻块会完全重复并被 CAS 去重成同一个文件，
// 崩溃窗口就少了一大半。byte(j>>8) 同样不行 —— 它的周期是 65536，
// 恰好等于块大小。
func crashPayload(i int) []byte {
	b := make([]byte, 150<<10)
	for j := range b {
		b[j] = byte(i*17+j*31) ^ byte(j>>8) ^ byte(j>>16)
	}
	return b
}

// TestCrashChild 是被父测试反复 SIGKILL 的子进程主体。
//
// 它按 crash/<序号> 连续写入并每完成一个就打印 READY <序号>；
// 父进程看到 READY 后再随机延迟一小段时间开枪，因此击杀点散布在
// "对象已提交""块写到一半""rename 前后"等不同位置。
//
// 每次都从磁盘上已有的最大序号之后继续写：这样每轮注入的都是全新内容，
// 不会因为 CAS 去重而退化成"只写元数据"，从而持续覆盖数据块的写入窗口。
func TestCrashChild(t *testing.T) {
	root := os.Getenv(crashChildEnv)
	if root == "" {
		t.Skip("仅在崩溃测试的子进程中运行")
	}
	const blockSize = 64 << 10
	s, err := Open(Options{Root: root, BlockSize: blockSize, InlineThreshold: 16})
	if err != nil {
		fmt.Fprintf(os.Stderr, "CHILD: Open 失败: %v\n", err)
		os.Exit(2)
	}
	if err := s.CreateBucket("bkt"); err != nil && !errors.Is(err, ErrBucketExists) {
		fmt.Fprintf(os.Stderr, "CHILD: CreateBucket 失败: %v\n", err)
		os.Exit(2)
	}

	start := 0
	res, err := s.ListObjects("bkt", ListOptions{Prefix: "crash/", MaxKeys: 1 << 20})
	if err != nil {
		fmt.Fprintf(os.Stderr, "CHILD: ListObjects 失败: %v\n", err)
		os.Exit(2)
	}
	for _, o := range res.Objects {
		var n int
		if _, err := fmt.Sscanf(o.Key, "crash/%d", &n); err == nil && n+1 > start {
			start = n + 1
		}
	}

	for i := start; ; i++ {
		data := crashPayload(i)
		if err := putBytes(s, fmt.Sprintf("crash/%06d", i), data); err != nil {
			// 子进程的写入错误不能被静默吞掉：父进程看到 ERROR 会失败，
			// 而不是把它误认成"对象没写成而已"。
			fmt.Printf("ERROR %d %v\n", i, err)
			os.Exit(3)
		}
		fmt.Printf("READY %d\n", i)
	}
}

// TestCrashLeavesOnlyCompleteObjects 反复 SIGKILL 写入进程，每次重启后
// 校验：**每个对象要么不存在，要么与写入内容逐字节相等**。
//
// 这是 05 文档对崩溃一致性的核心断言。残缺对象（长度对但内容是旧的，
// 或块表指向一半的块）会让上层拿到"看起来正常"的错误数据，是比丢数据
// 更糟的故障模式。
func TestCrashLeavesOnlyCompleteObjects(t *testing.T) {
	if testing.Short() {
		t.Skip("SIGKILL 子进程测试在 -short 下跳过")
	}
	root := filepath.Join(t.TempDir(), "data")
	const blockSize = 64 << 10

	// 固定种子：击杀延迟可复现，失败时能按同一轨迹重跑。
	rnd := rand.New(rand.NewSource(20261024))
	target := 3
	// 统计"击杀时子进程已经提交了对象、但父进程还没读到对应的 READY"的轮数。
	// 这是最危险的一类窗口：rename 完成，进程随即消失。如果实现把提交
	// 拆成多步且顺序有误，这里就会出现残缺对象。
	committedUnacked := 0
	for round := 0; round < 8; round++ {
		ready := runChildUntilKilled(t, root, target, rnd)

		// 重新打开存储 = 模拟进程重启后的恢复流程。
		s, err := Open(Options{Root: root, BlockSize: blockSize, InlineThreshold: 16})
		if err != nil {
			t.Fatalf("第 %d 轮重启 Open 失败: %v", round, err)
		}

		// 以 LIST 的结果为唯一事实来源：管道里可能还缓冲着父进程没来得及
		// 读的 READY 行，按 ready 推断提交进度会漏掉对象。
		maxIdx := -1
		res, err := s.ListObjects("bkt", ListOptions{Prefix: "crash/", MaxKeys: 1 << 20})
		if err != nil {
			t.Fatalf("第 %d 轮 LIST 失败: %v", round, err)
		}
		for i, o := range res.Objects {
			var idx int
			if _, err := fmt.Sscanf(o.Key, "crash/%d", &idx); err != nil || idx != i {
				t.Fatalf("第 %d 轮：对象序号不连续，第 %d 个是 %q", round, i, o.Key)
			}
			got, exists := readIfExists(t, s, o.Key)
			if !exists {
				t.Fatalf("第 %d 轮：LIST 返回的对象 %s 读不到", round, o.Key)
			}
			if !bytes.Equal(got, crashPayload(idx)) {
				t.Fatalf("第 %d 轮：对象 %s 内容不完整（%d 字节）", round, o.Key, len(got))
			}
			maxIdx = idx
		}

		// READY 已经打印 = 对象在打印前就已提交，重启后它不能消失。
		if maxIdx < ready {
			t.Fatalf("第 %d 轮：已 READY 的对象在重启后消失（最大序号 %d，READY 到 %d）", round, maxIdx, ready)
		}
		if maxIdx < target {
			t.Fatalf("第 %d 轮：子进程未写到 %d（最大序号 %d）", round, target, maxIdx)
		}
		if maxIdx > ready {
			committedUnacked++
		}
		target = maxIdx + 2 // 下一轮至少再写两个新对象
	}
	t.Logf("8 轮中有 %d 轮在击杀瞬间存在已提交但未确认的对象", committedUnacked)
	// 不变量只要求"对象完整"，并不要求"不留垃圾"。临时文件与目标同目录
	// （rename 需要同文件系统），而启动清扫只处理 tmp/，所以 SIGKILL
	// 必然留下一些 .tmp-* 孤儿。记录数量，作为 known-limitations 里
	// 那条条目的实测依据；不设为断言 —— 它取决于击杀时机，属偶然结果。
	t.Logf("残留临时文件 %d 个", countOrphanTmp(t, root))
}

// countOrphanTmp 统计数据目录里残留的 .tmp-* 文件数。
func countOrphanTmp(t *testing.T, root string) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.Contains(d.Name(), ".tmp-") {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("遍历 %s 失败: %v", root, err)
	}
	return n
}

// runChildUntilKilled 启动子进程，等到它写完 target 号对象后 SIGKILL，
// 返回最后一次 READY 的序号。
func runChildUntilKilled(t *testing.T, root string, target int, rnd *rand.Rand) int {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$")
	cmd.Env = append(os.Environ(), crashChildEnv+"="+root)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("获取子进程管道失败: %v", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动子进程失败: %v", err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()

	last := -1
	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "READY "):
			var n int
			if _, err := fmt.Sscanf(line, "READY %d", &n); err != nil {
				t.Fatalf("无法解析子进程输出 %q: %v", line, err)
			}
			last = n
		case strings.HasPrefix(line, "ERROR "):
			t.Fatalf("子进程写入失败: %s", line)
		}
		if last >= target {
			break
		}
	}
	if last < target {
		t.Fatalf("子进程未到达对象 %d 就退出（最后 READY=%d）", target, last)
	}

	// 随机延迟 0–45 ms，覆盖一次 150 KiB 写入的耗时（M4/APFS 上实测约 31 ms）。
	// 延迟从读到 READY 之后开始计时，也就是子进程刚开始写下一个对象：
	// 因此击杀点会均匀散布在"写块 / fsync / rename / fsync(dir)"整条路径上，
	// 而不是每次都停在同一个位置。
	time.Sleep(time.Duration(rnd.Int63n(int64(45 * time.Millisecond))))
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("SIGKILL 失败: %v", err)
	}
	waited = true
	// 子进程是被信号杀掉的，Wait 必定返回错误，忽略即可。
	_ = cmd.Wait()
	return last
}
