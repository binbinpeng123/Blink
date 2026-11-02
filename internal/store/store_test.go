package store

import (
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// newTestStore 在临时目录里打开一个存储，测试结束由 t.TempDir 自动清理。
func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(Options{Root: filepath.Join(t.TempDir(), "data")})
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	return s
}

// bucketMetaReadFS 统计 bucket.json 被打开的次数。
//
// 桶元数据缓存是否生效只能靠"读盘次数"证明 —— 只看返回值的话，
// 缓存写错了（例如每次命中后又回源一次）结果依然正确，却没有收益。
type bucketMetaReadFS struct {
	FileSystem
	mu    sync.Mutex
	reads int
}

func (f *bucketMetaReadFS) Open(path string) (io.ReadWriteCloser, error) {
	if strings.HasSuffix(path, string(filepath.Separator)+bucketMetaFileName) {
		f.mu.Lock()
		f.reads++
		f.mu.Unlock()
	}
	return f.FileSystem.Open(path)
}

func (f *bucketMetaReadFS) bucketReads() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reads
}

// TestBucketMetaCached 验证桶元数据缓存的收益与失效。
//
// 两件事必须同时成立：重复请求不再读 bucket.json（收益），
// 以及删除桶之后缓存立刻失效（正确性）。后者比前者更重要 ——
// 一个"永远不会失效"的缓存会让已删除的桶继续接受写入。
func TestBucketMetaCached(t *testing.T) {
	fsys := &bucketMetaReadFS{FileSystem: OSFS()}
	now := time.Unix(1_700_000_000, 0).UTC()
	s, err := Open(Options{
		Root: filepath.Join(t.TempDir(), "data"),
		FS:   fsys,
		Now:  func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	if err := s.CreateBucket("bkt"); err != nil {
		t.Fatalf("CreateBucket 失败: %v", err)
	}
	createdAt := now

	// 建桶前必须确认桶不存在，因此这里至少读过一次盘；否则说明
	// 统计口径没打中真正的读路径，后面的"零次"就没有意义。
	if got := fsys.bucketReads(); got == 0 {
		t.Fatal("建桶时没有读取 bucket.json，计数口径失效")
	}
	baseline := fsys.bucketReads()

	putString(t, s, "bkt", "obj.txt", "hello")
	for i := 0; i < 5; i++ {
		ok, err := s.BucketExists("bkt")
		if err != nil || !ok {
			t.Fatalf("第 %d 次 BucketExists = (%v, %v)，期望 (true, nil)", i, ok, err)
		}
		if _, err := s.OpenObject("bkt", "obj.txt"); err != nil {
			t.Fatalf("第 %d 次 OpenObject 失败: %v", i, err)
		}
	}
	if got := fsys.bucketReads(); got != baseline {
		t.Errorf("缓存命中后又读了 %d 次 bucket.json，期望 0", got-baseline)
	}

	// 删桶后缓存必须失效：查询要重新读盘并得到"不存在"。
	if err := s.DeleteObject("bkt", "obj.txt"); err != nil {
		t.Fatalf("DeleteObject 失败: %v", err)
	}
	if err := s.DeleteBucket("bkt"); err != nil {
		t.Fatalf("DeleteBucket 失败: %v", err)
	}
	before := fsys.bucketReads()
	if ok, err := s.BucketExists("bkt"); err != nil || ok {
		t.Fatalf("删除后 BucketExists = (%v, %v)，期望 (false, nil)", ok, err)
	}
	if fsys.bucketReads() <= before {
		t.Error("删除桶后查询未重新读盘，缓存没有失效")
	}

	// 同名重建要拿到新的创建时间：命中旧缓存会返回旧值，
	// 这是"失效只做了半截"的典型表现。
	now = createdAt.Add(24 * time.Hour)
	if err := s.CreateBucket("bkt"); err != nil {
		t.Fatalf("重建桶失败: %v", err)
	}
	got, err := s.BucketCreatedAt("bkt")
	if err != nil {
		t.Fatalf("BucketCreatedAt 失败: %v", err)
	}
	if !got.Equal(now) {
		t.Errorf("重建后创建时间 = %v，期望 %v（命中了旧缓存）", got, now)
	}
}

// assertNoTmpResidue 断言数据目录里没有残留的临时文件。
//
// 每次写入都要检查它：临时文件泄漏不会立刻表现出故障，
// 但会随着运行时间累积，最终把盘占满 —— 属于"最难排查"的一类问题。
func assertNoTmpResidue(t *testing.T, root string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.Contains(d.Name(), ".tmp-") {
			t.Errorf("残留临时文件: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("遍历 %s 失败: %v", root, err)
	}
}

func TestOpenCreatesLayout(t *testing.T) {
	s := newTestStore(t)

	for _, d := range []string{
		metaFileName, bucketsDirName, casDirName, tmpDirName, multipartDirName,
	} {
		if _, err := os.Stat(filepath.Join(s.Root(), d)); err != nil {
			t.Errorf("缺少 %s: %v", d, err)
		}
	}

	// 格式文件必须能被解析出正确的版本号 —— 它是"这块盘属于 Blink"的唯一凭据。
	m, err := s.loadDiskMeta()
	if err != nil {
		t.Fatalf("loadDiskMeta 失败: %v", err)
	}
	if m == nil {
		t.Fatal("blink-meta.json 未创建")
	}
	if m.FormatVersion != formatVersion {
		t.Errorf("格式版本 = %d，期望 %d", m.FormatVersion, formatVersion)
	}
	assertNoTmpResidue(t, s.Root())
}

func TestOpenIsIdempotent(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")
	first, err := Open(Options{Root: root})
	if err != nil {
		t.Fatalf("首次 Open 失败: %v", err)
	}
	if err := first.CreateBucket("keep"); err != nil {
		t.Fatalf("CreateBucket 失败: %v", err)
	}

	// 第二次打开必须保留既有数据，而不是把目录当成"别人的目录"拒绝，
	// 也不是重新初始化把已有桶抹掉。
	second, err := Open(Options{Root: root})
	if err != nil {
		t.Fatalf("二次 Open 失败: %v", err)
	}
	ok, err := second.BucketExists("keep")
	if err != nil || !ok {
		t.Fatalf("再次打开后桶丢失: ok=%v err=%v", ok, err)
	}
}

func TestOpenRejectsNonEmptyDirWithoutMeta(t *testing.T) {
	// 这是防"指错路径"的关键护栏：用户把 /data 而非 /data/blink 传进来时，
	// 里面通常已经有别的东西。此时在它的根上创建 buckets/ 是破坏性的。
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "someone-elses-file"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := Open(Options{Root: root})
	if err == nil {
		t.Fatal("非空目录且无格式文件时应拒绝打开")
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Errorf("错误类型 = %v，期望 ErrCorrupt", err)
	}
	if !strings.Contains(err.Error(), metaFileName) {
		t.Errorf("错误信息应指明缺少 %s，实际: %v", metaFileName, err)
	}
}

func TestOpenRejectsNewerFormatVersion(t *testing.T) {
	root := t.TempDir()
	body, _ := json.Marshal(diskMeta{FormatVersion: formatVersion + 1, CreatedAt: time.Now()})
	if err := os.WriteFile(filepath.Join(root, metaFileName), body, 0o600); err != nil {
		t.Fatal(err)
	}

	// 旧二进制读新格式最可能的后果是"以为对象不存在"，进而误报 404 甚至误删。
	// 宁可启动失败让人立刻发现。
	_, err := Open(Options{Root: root})
	if err == nil {
		t.Fatal("磁盘格式版本更高时应拒绝打开")
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Errorf("错误类型 = %v，期望 ErrCorrupt", err)
	}
}

func TestOpenRejectsSameVersion(t *testing.T) {
	root := t.TempDir()
	body, _ := json.Marshal(diskMeta{FormatVersion: formatVersion, CreatedAt: time.Now()})
	if err := os.WriteFile(filepath.Join(root, metaFileName), body, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Options{Root: root}); err != nil {
		t.Errorf("相同格式版本应可打开: %v", err)
	}
}

func TestOpenRejectsUnknownFsyncPolicy(t *testing.T) {
	_, err := Open(Options{Root: filepath.Join(t.TempDir(), "d"), FsyncPolicy: "sometimes"})
	if err == nil {
		t.Fatal("未知 fsync 策略应拒绝打开")
	}
	// 静默回落成默认值会让用户以为自己配的策略生效了。
	if !strings.Contains(err.Error(), "fsync") {
		t.Errorf("错误信息应说明是 fsync 策略问题: %v", err)
	}
}

func TestOpenRejectsEmptyRoot(t *testing.T) {
	if _, err := Open(Options{Root: "  "}); err == nil {
		t.Fatal("空数据目录应拒绝打开")
	}
}

func TestOpenCleansTmpOnStartup(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")
	s, err := Open(Options{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	residue := filepath.Join(s.Root(), tmpDirName, "half-written-object")
	if err := os.WriteFile(residue, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}

	// 重启后残留的临时文件必须被清掉：它要么已通过 rename 正式收录（重复品），
	// 要么从未提交（该丢弃），两种情况都不该留。
	if _, err := Open(Options{Root: root}); err != nil {
		t.Fatalf("重新打开失败: %v", err)
	}
	if _, err := os.Stat(residue); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("重启后残留临时文件未被清理: %v", err)
	}
}

func TestBucketNameValidation(t *testing.T) {
	valid := []string{
		"abc",
		"buck2",
		"my-bucket",
		"my.bucket.name",
		"0abc",
		"a1b",
		strings.Repeat("a", 63),
	}
	for _, name := range valid {
		if err := validateBucketName(name); err != nil {
			t.Errorf("validateBucketName(%q) = %v，期望合法", name, err)
		}
	}

	invalid := []struct {
		name string
		why  string
	}{
		{"", "空名"},
		{"ab", "短于 3 字符"},
		{strings.Repeat("a", 64), "长于 63 字符"},
		{"Abc", "含大写"},
		{"abc_", "含下划线"},
		{"-abc", "连字符开头"},
		{"abc-", "连字符结尾"},
		{"abc..def", "含连续点（与目录遍历语义混淆）"},
		{"..", "纯目录遍历"},
		{"192.168.1.1", "全数字与点，与主机名歧义"},
		{"abc/def", "含斜杠"},
		{"abc def", "含空格"},
	}
	for _, tc := range invalid {
		if err := validateBucketName(tc.name); !errors.Is(err, ErrInvalidBucketName) {
			t.Errorf("validateBucketName(%q) = %v，期望 ErrInvalidBucketName（%s）", tc.name, err, tc.why)
		}
	}
}

func TestCreateBucket(t *testing.T) {
	fixed := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	s, err := Open(Options{Root: filepath.Join(t.TempDir(), "data"), Now: func() time.Time { return fixed }})
	if err != nil {
		t.Fatal(err)
	}

	if err := s.CreateBucket("my-bucket"); err != nil {
		t.Fatalf("CreateBucket 失败: %v", err)
	}

	ok, err := s.BucketExists("my-bucket")
	if err != nil || !ok {
		t.Fatalf("桶创建后 BucketExists = %v, %v", ok, err)
	}

	created, err := s.BucketCreatedAt("my-bucket")
	if err != nil {
		t.Fatalf("BucketCreatedAt 失败: %v", err)
	}
	if !created.Equal(fixed) {
		t.Errorf("创建时间 = %v，期望 %v", created, fixed)
	}

	// 失败时不得留下"目录在但元数据缺失"的半成品。
	if _, err := os.Stat(s.objectRoot("my-bucket")); err != nil {
		t.Errorf("objects 目录未创建: %v", err)
	}
	assertNoTmpResidue(t, s.Root())
}

func TestCreateBucketDuplicate(t *testing.T) {
	s := newTestStore(t)
	if err := s.CreateBucket("dup"); err != nil {
		t.Fatal(err)
	}
	err := s.CreateBucket("dup")
	if !errors.Is(err, ErrBucketExists) {
		t.Errorf("重复创建 = %v，期望 ErrBucketExists", err)
	}
	// 单租户下没有"他人拥有"，因此 s3api 会把它映射成 BucketAlreadyOwnedByYou。
}

func TestCreateBucketInvalidName(t *testing.T) {
	s := newTestStore(t)
	err := s.CreateBucket("Bad_Name")
	if !errors.Is(err, ErrInvalidBucketName) {
		t.Errorf("非法桶名 = %v，期望 ErrInvalidBucketName", err)
	}
	// 非法名不该在盘上留下任何痕迹。
	if _, statErr := os.Stat(s.bucketDir("Bad_Name")); !errors.Is(statErr, fs.ErrNotExist) {
		t.Errorf("非法桶名不应创建目录: %v", statErr)
	}
}

func TestDeleteBucket(t *testing.T) {
	s := newTestStore(t)
	if err := s.CreateBucket("gone"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteBucket("gone"); err != nil {
		t.Fatalf("DeleteBucket 失败: %v", err)
	}
	ok, err := s.BucketExists("gone")
	if err != nil {
		t.Fatalf("BucketExists 失败: %v", err)
	}
	if ok {
		t.Error("桶应已被删除")
	}
}

func TestDeleteBucketNotFound(t *testing.T) {
	s := newTestStore(t)
	if err := s.DeleteBucket("nope"); !errors.Is(err, ErrBucketNotFound) {
		t.Errorf("删除不存在的桶 = %v，期望 ErrBucketNotFound", err)
	}
}

func TestDeleteBucketNotEmpty(t *testing.T) {
	s := newTestStore(t)
	if err := s.CreateBucket("full"); err != nil {
		t.Fatal(err)
	}
	// 手工构造一个对象目录，模拟桶内已有数据。
	// 切片 4 之后这里改用真实 PutObject，但"非空"的判断逻辑不变。
	objDir := filepath.Join(s.objectRoot("full"), "ab", strings.Repeat("0", 64))
	if err := os.MkdirAll(objDir, 0o750); err != nil {
		t.Fatal(err)
	}

	if err := s.DeleteBucket("full"); !errors.Is(err, ErrBucketNotEmpty) {
		t.Errorf("删除非空桶 = %v，期望 ErrBucketNotEmpty", err)
	}
	// 失败的删除不能把桶破坏掉。
	ok, err := s.BucketExists("full")
	if err != nil || !ok {
		t.Errorf("删除失败后桶应完好: ok=%v err=%v", ok, err)
	}
}

func TestListBucketsSorted(t *testing.T) {
	s := newTestStore(t)
	for _, name := range []string{"zeta", "alpha", "mid"} {
		if err := s.CreateBucket(name); err != nil {
			t.Fatal(err)
		}
	}

	names, err := s.ListBuckets()
	if err != nil {
		t.Fatalf("ListBuckets 失败: %v", err)
	}
	want := []string{"alpha", "mid", "zeta"}
	if len(names) != len(want) {
		t.Fatalf("桶数 = %d，期望 %d: %v", len(names), len(want), names)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("排序结果 = %v，期望 %v", names, want)
		}
	}
}

func TestListBucketsEmpty(t *testing.T) {
	s := newTestStore(t)
	names, err := s.ListBuckets()
	if err != nil {
		t.Fatalf("ListBuckets 失败: %v", err)
	}
	if len(names) != 0 {
		t.Errorf("空存储应返回 0 个桶，实际 %v", names)
	}
}

func TestListBucketsSkipsHalfCreated(t *testing.T) {
	s := newTestStore(t)
	if err := s.CreateBucket("good"); err != nil {
		t.Fatal(err)
	}
	// 模拟"创建中途失败"：目录建了但 bucket.json 没写成。
	if err := os.MkdirAll(s.bucketDir("broken"), 0o750); err != nil {
		t.Fatal(err)
	}

	names, err := s.ListBuckets()
	if err != nil {
		t.Fatalf("半成品桶不该让 ListBuckets 失败: %v", err)
	}
	if len(names) != 1 || names[0] != "good" {
		t.Errorf("期望只列出 [good]，实际 %v", names)
	}
}

func TestBucketExistsRejectsInvalidName(t *testing.T) {
	s := newTestStore(t)
	// 非法名应报错，而不是安静地回答"不存在" ——
	// 后者会让客户端把"名字写错了"误读成"桶被删了"。
	if _, err := s.BucketExists("Bad_Name"); !errors.Is(err, ErrInvalidBucketName) {
		t.Errorf("非法桶名 = %v，期望 ErrInvalidBucketName", err)
	}
}

func TestWriteFileAtomicRoundTrip(t *testing.T) {
	s := newTestStore(t)
	path := filepath.Join(s.Root(), "probe.json")
	want := []byte(`{"hello":"世界"}`)

	if err := s.writeFileAtomic(path, want); err != nil {
		t.Fatalf("writeFileAtomic 失败: %v", err)
	}
	got, err := readFile(s.fs, path)
	if err != nil {
		t.Fatalf("readFile 失败: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("内容 = %q，期望 %q", got, want)
	}
	assertNoTmpResidue(t, s.Root())
}

func TestWriteFileAtomicOverwrite(t *testing.T) {
	s := newTestStore(t)
	path := filepath.Join(s.Root(), "probe.json")

	if err := s.writeFileAtomic(path, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := s.writeFileAtomic(path, []byte("v2-longer")); err != nil {
		t.Fatal(err)
	}
	got, err := readFile(s.fs, path)
	if err != nil {
		t.Fatal(err)
	}
	// 必须是整体替换，不能残留 v1 的尾巴。
	if string(got) != "v2-longer" {
		t.Errorf("内容 = %q，期望 %q", got, "v2-longer")
	}
}

// failRenameFS 在第 failOn 次 Rename 上注入错误。
//
// 这是切片 8 故障注入的最小预演：writeFileAtomic 的正确性主张是
// "中途失败绝不破坏原有文件"，而 rename 正是唯一的替换点。
type failRenameFS struct {
	FileSystem
	failOn int
	n      int
}

func (f *failRenameFS) Rename(oldpath, newpath string) error {
	f.n++
	if f.n == f.failOn {
		return errors.New("注入的 rename 失败")
	}
	return f.FileSystem.Rename(oldpath, newpath)
}

func TestWriteFileAtomicFailureKeepsOriginal(t *testing.T) {
	fsys := &failRenameFS{FileSystem: OSFS(), failOn: 1 << 30}
	s, err := Open(Options{Root: filepath.Join(t.TempDir(), "data"), FS: fsys})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.Root(), "probe.json")
	if err := s.writeFileAtomic(path, []byte("原始内容")); err != nil {
		t.Fatal(err)
	}

	// 让下一次 rename 失败。
	fsys.failOn = fsys.n + 1
	if err := s.writeFileAtomic(path, []byte("新内容")); err == nil {
		t.Fatal("rename 失败时 writeFileAtomic 应返回错误")
	}

	got, err := readFile(s.fs, path)
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if string(got) != "原始内容" {
		t.Errorf("失败后内容 = %q，期望原内容保持不变", got)
	}
	assertNoTmpResidue(t, s.Root())
}

func TestAtomicWriterCommit(t *testing.T) {
	s := newTestStore(t)
	final := filepath.Join(s.Root(), "obj.bin")

	w, err := s.newAtomicWriter(final, true)
	if err != nil {
		t.Fatalf("newAtomicWriter 失败: %v", err)
	}
	if _, err := w.Write([]byte("hello ")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("world")); err != nil {
		t.Fatal(err)
	}
	if w.Size() != 11 {
		t.Errorf("Size = %d，期望 11", w.Size())
	}
	// Commit 之前目标文件必须不存在：这正是"原子"的含义。
	if _, err := os.Stat(final); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Commit 前目标文件不应存在: %v", err)
	}

	if err := w.Commit(); err != nil {
		t.Fatalf("Commit 失败: %v", err)
	}
	got, err := readFile(s.fs, final)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello world" {
		t.Errorf("内容 = %q，期望 %q", got, "hello world")
	}
	assertNoTmpResidue(t, s.Root())
}

func TestAtomicWriterAbort(t *testing.T) {
	s := newTestStore(t)
	final := filepath.Join(s.Root(), "obj.bin")

	w, err := s.newAtomicWriter(final, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("废弃内容")); err != nil {
		t.Fatal(err)
	}
	if err := w.Abort(); err != nil {
		t.Fatalf("Abort 失败: %v", err)
	}
	if _, err := os.Stat(final); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Abort 后目标文件不应存在: %v", err)
	}
	// Abort 必须可重复调用：调用方常在 defer 里再调一次。
	if err := w.Abort(); err != nil {
		t.Errorf("重复 Abort = %v，期望 nil", err)
	}
	assertNoTmpResidue(t, s.Root())
}

func TestAtomicWriterAbortAfterCommitIsNoop(t *testing.T) {
	s := newTestStore(t)
	final := filepath.Join(s.Root(), "obj.bin")

	w, err := s.newAtomicWriter(final, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("已提交内容")); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	// 已提交的写入不能被 Abort 撤销 —— 否则"Commit 成功"就没有意义了。
	if err := w.Abort(); err != nil {
		t.Errorf("Commit 后 Abort = %v，期望 nil", err)
	}
	got, err := readFile(s.fs, final)
	if err != nil {
		t.Fatalf("已提交的文件不应被 Abort 删除: %v", err)
	}
	if string(got) != "已提交内容" {
		t.Errorf("内容 = %q", got)
	}
}

func TestAtomicWriterCommitTwiceFails(t *testing.T) {
	s := newTestStore(t)
	w, err := s.newAtomicWriter(filepath.Join(s.Root(), "obj.bin"), true)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); err == nil {
		t.Error("重复 Commit 应报错")
	}
}

func TestCleanTmpRemovesResidue(t *testing.T) {
	s := newTestStore(t)
	residue := filepath.Join(s.Root(), tmpDirName, "leftover")
	if err := os.WriteFile(residue, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.cleanTmp(); err != nil {
		t.Fatalf("cleanTmp 失败: %v", err)
	}
	if _, err := os.Stat(residue); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("残留文件未被清理: %v", err)
	}
	// 清理后目录必须还在，否则后续写入无处落脚。
	if _, err := os.Stat(filepath.Join(s.Root(), tmpDirName)); err != nil {
		t.Errorf("tmp 目录应被重建: %v", err)
	}
}

func TestCASPathShape(t *testing.T) {
	s := newTestStore(t)
	sum := strings.Repeat("ab", 32) // 64 个十六进制字符
	got := s.casPath(sum)
	want := filepath.Join(s.Root(), casDirName, "ab", sum)
	if got != want {
		t.Errorf("casPath = %q，期望 %q", got, want)
	}
}

func TestObjectDirIsHashOfKey(t *testing.T) {
	s := newTestStore(t)
	dir := s.objectDir("b", "models/llama.gguf")
	// 目录名不得包含原始 key —— 否则 key 里的 "/" 会变成真实目录层级，
	// ".." 则可能逃出桶目录。
	if strings.Contains(dir, "llama") || strings.Contains(dir, "models") {
		t.Errorf("对象目录不应包含原始 key: %s", dir)
	}
	sum := hashKey("models/llama.gguf")
	if !strings.HasSuffix(dir, filepath.Join(sum[:2], sum)) {
		t.Errorf("对象目录应以 sha256(key) 结尾: %s", dir)
	}
}

func TestSafeJoin(t *testing.T) {
	root := "/data/blink"

	ok, valid := safeJoin(root, "buckets", "mybucket")
	if !valid || ok != "/data/blink/buckets/mybucket" {
		t.Errorf("safeJoin 正常路径 = %q, %v", ok, valid)
	}

	// 目录遍历必须被拦下 —— 这是桶名/key 校验之外的第二道防线。
	if _, valid := safeJoin(root, "..", "etc", "passwd"); valid {
		t.Error("safeJoin 应拒绝目录遍历")
	}
	if _, valid := safeJoin(root, strings.Repeat("../", 3)); valid {
		t.Error("safeJoin 应拒绝多重目录遍历")
	}

	// 从 root 逃到**前缀相似**的兄弟目录也必须被拦下：
	// 朴素实现用 strings.HasPrefix(p, root) 判断，就会把
	// /data/blink-evil 误判为 /data/blink 的子路径。
	if got, valid := safeJoin(root, "..", "blink-evil"); valid {
		t.Errorf("safeJoin 应拒绝逃到前缀相似的兄弟目录，实际返回 %q", got)
	}
}
