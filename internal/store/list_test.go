package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// listRun 是一次"翻到底"的结果。
//
// Entries 按页序、且每页内部按"先对象、后公共前缀"排列 —— 这正是
// 客户端在 XML 里看到的顺序，也是唯一能检验"跨页边界有没有丢条/重条"的
// 全局视图；Keys 与 Prefixes 则保留类型信息。
type listRun struct {
	Entries  []string
	Keys     []string
	Prefixes []string
	Pages    int
}

// collectAll 反复翻页直到结束。
//
// 分页正确性只能靠"翻到底再比对全集"来验证：单看某一页是否合理，
// 恰好漏掉"某一条在两页交界处被吞掉或重复"这类最常见的分页 bug。
func collectAll(t *testing.T, s *Store, bucket string, opt ListOptions) listRun {
	t.Helper()
	var run listRun
	for {
		res, err := s.ListObjects(bucket, opt)
		if err != nil {
			t.Fatalf("ListObjects 失败: %v", err)
		}
		run.Pages++
		for _, o := range res.Objects {
			run.Entries = append(run.Entries, o.Key)
			run.Keys = append(run.Keys, o.Key)
		}
		for _, p := range res.CommonPrefixes {
			run.Entries = append(run.Entries, p)
			run.Prefixes = append(run.Prefixes, p)
		}
		if !res.IsTruncated {
			if res.Next != nil {
				t.Fatal("未截断却返回了 Next 游标")
			}
			return run
		}
		if res.Next == nil {
			t.Fatal("IsTruncated 为真却没有 Next 游标 —— 客户端将无法继续翻页")
		}
		opt.Cursor = res.Next
		if run.Pages > 1000 {
			t.Fatal("翻页次数异常，疑似游标未推进的死循环")
		}
	}
}

func TestListEmptyBucket(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	if err := s.CreateBucket("empty"); err != nil {
		t.Fatalf("CreateBucket 失败: %v", err)
	}

	res, err := s.ListObjects("empty", ListOptions{MaxKeys: 10})
	if err != nil {
		t.Fatalf("ListObjects 失败: %v", err)
	}
	if res.KeyCount != 0 || len(res.Objects) != 0 || len(res.CommonPrefixes) != 0 {
		t.Errorf("空桶结果 = %+v，期望空", res)
	}
	if res.IsTruncated || res.Next != nil {
		t.Errorf("空桶不应截断: %+v", res)
	}
}

func TestListBucketNotFound(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)

	if _, err := s.ListObjects("nope", ListOptions{MaxKeys: 10}); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("错误 = %v，期望 ErrBucketNotFound", err)
	}
	if _, err := s.ListObjects("Bad_Name", ListOptions{MaxKeys: 10}); !errors.Is(err, ErrInvalidBucketName) {
		t.Errorf("错误 = %v，期望 ErrInvalidBucketName", err)
	}
}

// TestListMaxKeysZero 验证 max-keys=0 返回空页且不截断。
//
// 若这里错误地返回 IsTruncated=true，客户端会带着同一个游标无限翻页；
// 若返回了条目，则违背了 S3 明文的 max-keys 语义。
func TestListMaxKeysZero(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	putString(t, s, "bkt", "a.txt", "a")

	res, err := s.ListObjects("bkt", ListOptions{MaxKeys: 0})
	if err != nil {
		t.Fatalf("ListObjects 失败: %v", err)
	}
	if res.KeyCount != 0 || len(res.Objects) != 0 {
		t.Errorf("max-keys=0 应返回空页，实际 %+v", res)
	}
	if res.IsTruncated || res.Next != nil {
		t.Errorf("max-keys=0 不应截断，实际 %+v", res)
	}

	// 负数按 0 处理，而不是"不限量"—— 后者会让一个笔误的客户端
	// 拿到整桶数据并以为分页生效了。
	res, err = s.ListObjects("bkt", ListOptions{MaxKeys: -1})
	if err != nil {
		t.Fatalf("ListObjects 失败: %v", err)
	}
	if res.KeyCount != 0 || res.IsTruncated {
		t.Errorf("负 max-keys 应按 0 处理，实际 %+v", res)
	}
}

func TestListPrefix(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	for _, k := range []string{"logs/a.log", "logs/b.log", "logs.txt", "other.txt"} {
		putString(t, s, "bkt", k, "x")
	}

	res, err := s.ListObjects("bkt", ListOptions{Prefix: "logs/", MaxKeys: 100})
	if err != nil {
		t.Fatalf("ListObjects 失败: %v", err)
	}
	if got := keysOf(res); !slices.Equal(got, []string{"logs/a.log", "logs/b.log"}) {
		t.Errorf("prefix=logs/ 结果 = %v", got)
	}

	// 前缀是纯字节匹配，不是"按目录"匹配：`logs` 也能匹配 `logs.txt`。
	res, err = s.ListObjects("bkt", ListOptions{Prefix: "logs", MaxKeys: 100})
	if err != nil {
		t.Fatalf("ListObjects 失败: %v", err)
	}
	if got := keysOf(res); !slices.Equal(got, []string{"logs.txt", "logs/a.log", "logs/b.log"}) {
		t.Errorf("prefix=logs 结果 = %v（字节序）", got)
	}

	// 匹配不到任何对象是正常结果，不是错误。
	res, err = s.ListObjects("bkt", ListOptions{Prefix: "absent/", MaxKeys: 100})
	if err != nil {
		t.Fatalf("ListObjects 失败: %v", err)
	}
	if res.KeyCount != 0 || res.IsTruncated {
		t.Errorf("无匹配前缀应返回空页，实际 %+v", res)
	}
}

func TestListDelimiterFolding(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	for _, k := range []string{"b", "a/1", "a/2", "a/sub/3", "c/x", "d/", "e.txt"} {
		putString(t, s, "bkt", k, "x")
	}

	res, err := s.ListObjects("bkt", ListOptions{Delimiter: "/", MaxKeys: 100})
	if err != nil {
		t.Fatalf("ListObjects 失败: %v", err)
	}
	if got := keysOf(res); !slices.Equal(got, []string{"b", "e.txt"}) {
		t.Errorf("对象 = %v，期望 [b e.txt]", got)
	}
	// "d/" 自身也折叠成公共前缀 —— 与 AWS S3 一致：
	// 公共前缀代表"这一层目录"，而 key 恰好以分隔符结尾时它就是目录本身。
	if !slices.Equal(res.CommonPrefixes, []string{"a/", "c/", "d/"}) {
		t.Errorf("公共前缀 = %v，期望 [a/ c/ d/]", res.CommonPrefixes)
	}
	if res.KeyCount != 5 {
		t.Errorf("KeyCount = %d，期望 5（对象 + 公共前缀）", res.KeyCount)
	}

	// prefix 与 delimiter 组合：只看 a/ 这一层，子目录折叠。
	res, err = s.ListObjects("bkt", ListOptions{Prefix: "a/", Delimiter: "/", MaxKeys: 100})
	if err != nil {
		t.Fatalf("ListObjects 失败: %v", err)
	}
	if got := keysOf(res); !slices.Equal(got, []string{"a/1", "a/2"}) {
		t.Errorf("prefix=a/ 对象 = %v，期望 [a/1 a/2]", got)
	}
	// 公共前缀必须拼回完整路径，而不是只给 "/sub/"。
	if !slices.Equal(res.CommonPrefixes, []string{"a/sub/"}) {
		t.Errorf("prefix=a/ 公共前缀 = %v，期望 [a/sub/]", res.CommonPrefixes)
	}
}

// TestListUTF8ByteOrder 验证排序是 UTF-8 字节序，而非 rune 序或 locale 序。
//
// 三者在这组 key 上结果不同：按 rune 序 "é"(U+00E9) 会排在 "~"(U+007E) 之后
// 但与 "中"(U+4E2D) 的相对位置不变；按 locale 序则取决于环境。
// 02 文档要求的是可直接用于游标续页的字节序，只有它才有全局确定性。
func TestListUTF8ByteOrder(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	want := []string{"0", "A", "Z", "a", "z", "~", "é", "€", "中"}
	// 逆序写入，确保返回顺序来自排序而不是写入顺序。
	for i := len(want) - 1; i >= 0; i-- {
		putString(t, s, "bkt", want[i], "x")
	}

	res, err := s.ListObjects("bkt", ListOptions{MaxKeys: 100})
	if err != nil {
		t.Fatalf("ListObjects 失败: %v", err)
	}
	if got := keysOf(res); !slices.Equal(got, want) {
		t.Errorf("排序 = %v，期望 UTF-8 字节序 %v", got, want)
	}
}

func TestListStartAfter(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	for _, k := range []string{"a", "b", "c", "d"} {
		putString(t, s, "bkt", k, "x")
	}

	// 严格大于：start-after=b 不包含 b 本身。
	res, err := s.ListObjects("bkt", ListOptions{StartAfter: "b", MaxKeys: 100})
	if err != nil {
		t.Fatalf("ListObjects 失败: %v", err)
	}
	if got := keysOf(res); !slices.Equal(got, []string{"c", "d"}) {
		t.Errorf("start-after=b 结果 = %v，期望 [c d]", got)
	}

	// 不存在的 key 按字典序定位，不报错。
	res, err = s.ListObjects("bkt", ListOptions{StartAfter: "bb", MaxKeys: 100})
	if err != nil {
		t.Fatalf("ListObjects 失败: %v", err)
	}
	if got := keysOf(res); !slices.Equal(got, []string{"c", "d"}) {
		t.Errorf("start-after=bb 结果 = %v，期望 [c d]", got)
	}

	// Cursor 优先于 StartAfter（S3 规定 continuation-token 优先）。
	res, err = s.ListObjects("bkt", ListOptions{
		StartAfter: "a",
		Cursor:     &ListCursor{After: "c"},
		MaxKeys:    100,
	})
	if err != nil {
		t.Fatalf("ListObjects 失败: %v", err)
	}
	if got := keysOf(res); !slices.Equal(got, []string{"d"}) {
		t.Errorf("Cursor 应优先于 StartAfter，结果 = %v，期望 [d]", got)
	}
}

// TestListCursorSkipsWholePrefix 守住分页里最容易写错的一处：
// 上一页最后一条是公共前缀时，下一页必须跳过该前缀下的全部 key。
func TestListCursorSkipsWholePrefix(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	for _, k := range []string{"a/1", "a/2", "a/3", "b"} {
		putString(t, s, "bkt", k, "x")
	}

	res, err := s.ListObjects("bkt", ListOptions{
		Delimiter: "/",
		Cursor:    &ListCursor{After: "a/", IsPrefix: true},
		MaxKeys:   100,
	})
	if err != nil {
		t.Fatalf("ListObjects 失败: %v", err)
	}
	if res.KeyCount != 1 || !slices.Equal(keysOf(res), []string{"b"}) {
		t.Errorf("游标 a/ 之后结果 = %+v，期望只剩 b", res)
	}
}

// TestListPaginationNoLossNoDup 是切片 6 的核心验收：
// max-keys=2 翻到底，结果必须与一次性列举完全一致。
func TestListPaginationNoLossNoDup(t *testing.T) {
	keys := []string{
		"a.txt",
		"b/1", "b/2", "b/sub/3",
		"c",
		"logs/2026/01.log", "logs/2026/02.log", "logs/2026/03.log",
		"z.txt",
	}

	cases := []struct {
		name string
		opt  ListOptions
		want []string
	}{
		{
			name: "无 delimiter 全量",
			opt:  ListOptions{MaxKeys: 2},
			want: keys,
		},
		{
			// 排序后的条目：a.txt / b/ / c / logs/ / z.txt
			// 每页 2 条 → 3 页，两个公共前缀分别落在不同页的边界上。
			name: "delimiter 折叠",
			opt:  ListOptions{Delimiter: "/", MaxKeys: 2},
			want: []string{"a.txt", "b/", "c", "logs/", "z.txt"},
		},
		{
			name: "prefix + delimiter",
			opt:  ListOptions{Prefix: "logs/", Delimiter: "/", MaxKeys: 2},
			want: []string{"logs/2026/"},
		},
		{
			// prefix 内的对象逐个返回，max-keys 小于前缀内对象数。
			name: "prefix logs/ 不折叠",
			opt:  ListOptions{Prefix: "logs/", MaxKeys: 2},
			want: []string{"logs/2026/01.log", "logs/2026/02.log", "logs/2026/03.log"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newSmallBlockStore(t, 8, 4)
			for i := len(keys) - 1; i >= 0; i-- {
				putString(t, s, "bkt", keys[i], "x")
			}

			run := collectAll(t, s, "bkt", c.opt)
			if !slices.Equal(run.Entries, c.want) {
				t.Errorf("翻页结果 = %v，期望 %v", run.Entries, c.want)
			}
			// 无丢无重：条目总数必须等于上一页数之和，且全集无重复。
			seen := make(map[string]bool, len(run.Entries))
			for _, e := range run.Entries {
				if seen[e] {
					t.Errorf("条目 %q 重复出现", e)
				}
				seen[e] = true
			}
			// 页数也必须符合预期：多一页意味着末页空返回（客户端多打一次
			// 请求），少一页意味着最后一页超量。
			wantPages := (len(c.want) + c.opt.MaxKeys - 1) / c.opt.MaxKeys
			if run.Pages != wantPages {
				t.Errorf("页数 = %d，期望 %d", run.Pages, wantPages)
			}
		})
	}
}

// TestListPageSizeRespected 验证每页 KeyCount 都不超过 max-keys。
func TestListPageSizeRespected(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	for _, k := range []string{"a/1", "a/2", "a/3", "b", "c/1", "c/2", "d"} {
		putString(t, s, "bkt", k, "x")
	}

	opt := ListOptions{Delimiter: "/", MaxKeys: 2}
	var pages int
	for {
		res, err := s.ListObjects("bkt", opt)
		if err != nil {
			t.Fatalf("ListObjects 失败: %v", err)
		}
		pages++
		if res.KeyCount > 2 {
			t.Fatalf("第 %d 页 KeyCount = %d，超过 max-keys=2", pages, res.KeyCount)
		}
		if !res.IsTruncated {
			break
		}
		opt.Cursor = res.Next
		if pages > 20 {
			t.Fatal("疑似死循环")
		}
	}
	// a/ b c/ d 共 4 条 → 2 页。
	if pages != 2 {
		t.Errorf("页数 = %d，期望 2", pages)
	}
}

func TestListObjectFields(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	info := putString(t, s, "bkt", "a.txt", "hello")

	res, err := s.ListObjects("bkt", ListOptions{MaxKeys: 10})
	if err != nil {
		t.Fatalf("ListObjects 失败: %v", err)
	}
	if len(res.Objects) != 1 {
		t.Fatalf("条目数 = %d，期望 1", len(res.Objects))
	}
	o := res.Objects[0]
	if o.Key != "a.txt" || o.Size != 5 || o.ETag != info.ETag || o.Modified.IsZero() {
		t.Errorf("条目 = %+v，与写入结果 %+v 不符", o, info)
	}
}

// TestListSkipsConcurrentlyDeleted 验证遍历途中的删除表现为"少一条"，
// 而不是让整个 LIST 500。
func TestListSkipsConcurrentlyDeleted(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	putString(t, s, "bkt", "gone.txt", "x")
	putString(t, s, "bkt", "kept.txt", "x")

	// 连目录一起删掉：模拟并发 DeleteObject 的最终状态。
	if err := os.RemoveAll(s.objectDir("bkt", "gone.txt")); err != nil {
		t.Fatalf("删除对象目录失败: %v", err)
	}
	// 只删 meta.json：模拟删除进行到一半（数据目录还在）。
	if err := os.Remove(s.objectMetaPath("bkt", "kept.txt")); err != nil {
		t.Fatalf("删除 meta.json 失败: %v", err)
	}

	res, err := s.ListObjects("bkt", ListOptions{MaxKeys: 10})
	if err != nil {
		t.Fatalf("并发删除不应让 LIST 失败: %v", err)
	}
	if res.KeyCount != 0 {
		t.Errorf("两个对象都已删除，结果 = %+v", res)
	}
}

// TestListParallelShardScan 覆盖"多分片并行扫描"路径，断言并行不改变结果。
//
// 并行扫描的风险不是慢，而是丢条、重条与顺序错乱：结果若按 worker 完成
// 顺序拼接，key 序就会随调度抖动；若共用同一个 append 目标，就会有竞态。
// 因此这里让 key 均匀落到多个分片，先用一次全量读取比对全集，
// 再用小页翻到底 —— 两种读法必须给出同一个序列。
func TestListParallelShardScan(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	const n = 400
	bodies := make(map[string]string, n)
	want := make([]string, 0, n)
	for i := 0; i < n; i++ {
		// 前缀打散是为了让 key 均匀落进 256 个分片；全部堆在一个分片下
		// 并行路径根本不会被触发。
		key := fmt.Sprintf("p%d/%04d", i%7, i)
		body := strings.Repeat("x", i%5+1)
		putString(t, s, "bkt", key, body)
		bodies[key] = body
		want = append(want, key)
	}
	slices.Sort(want)

	full, err := s.ListObjects("bkt", ListOptions{MaxKeys: n})
	if err != nil {
		t.Fatalf("ListObjects 失败: %v", err)
	}
	if got := keysOf(full); !slices.Equal(got, want) {
		t.Fatalf("全量 LIST 与写入集合不一致：得到 %d 条，期望 %d 条", len(got), len(want))
	}
	// 部分解码只为省成本，字段值必须与完整元数据一致。
	for _, o := range full.Objects {
		if wantSize := int64(len(bodies[o.Key])); o.Size != wantSize {
			t.Fatalf("%q 的 Size = %d，期望 %d", o.Key, o.Size, wantSize)
		}
		if len(o.ETag) != 32 {
			t.Fatalf("%q 的 ETag = %q，期望 32 位十六进制 MD5", o.Key, o.ETag)
		}
		if o.Modified.IsZero() {
			t.Fatalf("%q 的 Modified 为零值，LIST 没有解出时间字段", o.Key)
		}
	}

	run := collectAll(t, s, "bkt", ListOptions{MaxKeys: 37})
	if !slices.Equal(run.Keys, want) {
		t.Fatalf("分页 LIST 与全量 LIST 不一致：得到 %d 条，期望 %d 条", len(run.Keys), len(want))
	}
}

// TestListCorruptMetaFailsLoudly 验证损坏绝不被静默跳过。
//
// "跳过损坏对象"看起来更友好，但它会让 LIST 悄悄少返回数据：
// 用户只有在核对账目时才会发现，而那时已经无法判断丢了什么。
func TestListCorruptMetaFailsLoudly(t *testing.T) {
	cases := []struct {
		name   string
		damage func(t *testing.T, s *Store)
	}{
		{
			name: "meta.json 不是合法 JSON",
			damage: func(t *testing.T, s *Store) {
				t.Helper()
				path := s.objectMetaPath("bkt", "obj.txt")
				if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
					t.Fatalf("写入损坏 meta 失败: %v", err)
				}
			},
		},
		{
			name: "目录哈希与 meta 里的 key 不一致",
			damage: func(t *testing.T, s *Store) {
				t.Helper()
				path := s.objectMetaPath("bkt", "obj.txt")
				m, err := s.readObjectMetaAt(path)
				if err != nil {
					t.Fatalf("读取 meta 失败: %v", err)
				}
				m.Key = "another.txt"
				if err := os.WriteFile(path, mustJSON(m), 0o644); err != nil {
					t.Fatalf("写入 meta 失败: %v", err)
				}
			},
		},
		{
			name: "meta 里的 key 为空",
			damage: func(t *testing.T, s *Store) {
				t.Helper()
				path := s.objectMetaPath("bkt", "obj.txt")
				m, err := s.readObjectMetaAt(path)
				if err != nil {
					t.Fatalf("读取 meta 失败: %v", err)
				}
				m.Key = ""
				if err := os.WriteFile(path, mustJSON(m), 0o644); err != nil {
					t.Fatalf("写入 meta 失败: %v", err)
				}
			},
		},
		{
			// LIST 只解码 key/size/etag/modified，但"少解码"不等于
			// "放过损坏"：块表所在的位置语法坏了，整份 JSON 都不合法，
			// 必须照样报 ErrCorrupt，而不是把残缺文件当成有效元数据。
			name: "blocks 字段语法损坏",
			damage: func(t *testing.T, s *Store) {
				t.Helper()
				path := s.objectMetaPath("bkt", "obj.txt")
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("读取 meta 失败: %v", err)
				}
				broken := strings.Replace(string(data), `"blocks":`, `"blocks":notjson,`, 1)
				if broken == string(data) {
					t.Fatal("meta 中没有找到 blocks 字段，损坏注入失败")
				}
				if err := os.WriteFile(path, []byte(broken), 0o644); err != nil {
					t.Fatalf("写入损坏 meta 失败: %v", err)
				}
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newSmallBlockStore(t, 8, 4)
			putString(t, s, "bkt", "obj.txt", "x")
			c.damage(t, s)

			if _, err := s.ListObjects("bkt", ListOptions{MaxKeys: 10}); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("错误 = %v，期望 ErrCorrupt", err)
			}
		})
	}
}

// TestListMissingObjectsDir 验证"桶元数据在、objects/ 不在"时按空桶处理。
//
// 这是创建中途失败的残骸，对客户端而言与空桶无异；
// 让它 500 会把一次可自愈的残留放大成整个桶不可用。
func TestListMissingObjectsDir(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	putString(t, s, "bkt", "a.txt", "x")
	if err := os.RemoveAll(s.objectRoot("bkt")); err != nil {
		t.Fatalf("删除 objects 目录失败: %v", err)
	}

	res, err := s.ListObjects("bkt", ListOptions{MaxKeys: 10})
	if err != nil {
		t.Fatalf("ListObjects 失败: %v", err)
	}
	if res.KeyCount != 0 || res.IsTruncated {
		t.Errorf("结果 = %+v，期望空桶", res)
	}
}

// TestListIgnoresStrayFiles 验证 objects/ 下的非目录条目不会导致失败。
//
// 真实环境里编辑器备份、`.DS_Store`、运维手工放的东西都可能是文件；
// LIST 只认 <shard>/<hash>/meta.json 这一种结构，其余一律忽略。
func TestListIgnoresStrayFiles(t *testing.T) {
	s := newSmallBlockStore(t, 8, 4)
	putString(t, s, "bkt", "a.txt", "x")

	if err := os.WriteFile(filepath.Join(s.objectRoot("bkt"), ".DS_Store"), []byte("junk"), 0o644); err != nil {
		t.Fatalf("写入多余文件失败: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(s.objectRoot("bkt"), "zz"), 0o755); err != nil {
		t.Fatalf("创建空分片目录失败: %v", err)
	}

	res, err := s.ListObjects("bkt", ListOptions{MaxKeys: 10})
	if err != nil {
		t.Fatalf("ListObjects 失败: %v", err)
	}
	if got := keysOf(res); !slices.Equal(got, []string{"a.txt"}) {
		t.Errorf("结果 = %v，期望 [a.txt]", got)
	}
}

func keysOf(res *ListResult) []string {
	keys := make([]string, len(res.Objects))
	for i, o := range res.Objects {
		keys[i] = o.Key
	}
	return keys
}
