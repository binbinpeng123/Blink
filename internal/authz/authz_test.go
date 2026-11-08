package authz

import "testing"

func mustPolicy(t *testing.T, rules ...Rule) *Policy {
	t.Helper()
	p, err := New(rules)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

func TestAllowMatrix(t *testing.T) {
	p := mustPolicy(t,
		Rule{Bucket: "models", Prefix: "llama/", Actions: []Action{ActionRead, ActionWrite}},
		Rule{Bucket: "artifacts", Actions: []Action{ActionRead, ActionList}},
	)

	cases := []struct {
		name   string
		action Action
		bucket string
		key    string
		want   bool
	}{
		{"前缀内读", ActionRead, "models", "llama/7b.bin", true},
		{"前缀内写", ActionWrite, "models", "llama/7b.bin", true},
		{"前缀外读", ActionRead, "models", "qwen/7b.bin", false},
		{"前缀边界不匹配", ActionRead, "models", "llamax/7b.bin", false},
		{"无 delete", ActionDelete, "models", "llama/7b.bin", false},
		{"无 list", ActionList, "models", "", false},
		{"桶级读取预检", ActionRead, "artifacts", "", true},
		{"桶级列举", ActionList, "artifacts", "", true},
		{"其他桶", ActionRead, "other", "x", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := p.Allow(c.action, c.bucket, c.key); got != c.want {
				t.Fatalf("Allow(%s,%s,%q) = %v，期望 %v", c.action, c.bucket, c.key, got, c.want)
			}
		})
	}
}

func TestWildcardBucket(t *testing.T) {
	p := mustPolicy(t, Rule{Bucket: "*", Actions: []Action{ActionRead}})
	if !p.Allow(ActionRead, "any", "k") {
		t.Fatal("通配桶应允许读取")
	}
	if p.Allow(ActionWrite, "any", "k") {
		t.Fatal("通配桶不应凭空允许写")
	}
	if !p.VisibleBucket("any") {
		t.Fatal("通配桶应可见")
	}
}

func TestBucketScopedCheck(t *testing.T) {
	p := mustPolicy(t, Rule{Bucket: "b", Prefix: "p/", Actions: []Action{ActionDelete}})
	// 桶级预检（key 为空）通过，交给逐 key 检查。
	if !p.Allow(ActionDelete, "b", "") {
		t.Fatal("桶级 delete 预检应通过")
	}
	if p.Allow(ActionDelete, "b", "other/key") {
		t.Fatal("前缀外不应允许删除")
	}
}

func TestAllowAll(t *testing.T) {
	p := AllowAll()
	if !p.Allow(ActionAdmin, "b", "") || !p.Allow(ActionDelete, "*", "k") {
		t.Fatal("AllowAll 应允许一切")
	}
	if !p.VisibleBucket("anything") {
		t.Fatal("AllowAll 应看到全部桶")
	}
}

func TestNilPolicyDenies(t *testing.T) {
	var p *Policy //nolint:staticcheck // 刻意验证 nil 策略的拒绝语义
	if p.Allow(ActionRead, "b", "k") || p.VisibleBucket("b") {
		t.Fatal("nil 策略必须拒绝")
	}
}

func TestInvalidRules(t *testing.T) {
	cases := []struct {
		name string
		rule Rule
	}{
		{"空规则集", Rule{}},
		{"空桶", Rule{Bucket: "", Actions: []Action{ActionRead}}},
		{"桶含斜杠", Rule{Bucket: "a/b", Actions: []Action{ActionRead}}},
		{"空动作", Rule{Bucket: "b"}},
		{"未知动作", Rule{Bucket: "b", Actions: []Action{"root"}}},
		{"重复动作", Rule{Bucket: "b", Actions: []Action{ActionRead, ActionRead}}},
		{"list 带前缀", Rule{Bucket: "b", Prefix: "p/", Actions: []Action{ActionList}}},
		{"admin 带前缀", Rule{Bucket: "b", Prefix: "p/", Actions: []Action{ActionAdmin}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var err error
			if c.name == "空规则集" {
				_, err = New(nil)
			} else {
				_, err = New([]Rule{c.rule})
			}
			if err == nil {
				t.Fatal("期望校验失败")
			}
		})
	}
}

func TestVisibleBucket(t *testing.T) {
	p := mustPolicy(t,
		Rule{Bucket: "models", Prefix: "llama/", Actions: []Action{ActionRead}},
		Rule{Bucket: "ci", Actions: []Action{ActionRead}},
	)
	if !p.VisibleBucket("models") || !p.VisibleBucket("ci") {
		t.Fatal("有规则的桶应可见")
	}
	if p.VisibleBucket("secret") {
		t.Fatal("无规则的桶不应可见")
	}
}
