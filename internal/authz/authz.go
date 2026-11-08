// Package authz 实现 Blink 的静态授权模型：动作 + 桶 + 前缀。
//
// 设计目标与边界：
//
//   - 只服务"一组 Access Key 能做什么"的最小集合，不实现 IAM 条件键、
//     显式 Deny 与资源级 ACL（见 06 文档"明确不做"）。
//   - 默认拒绝：凭据没有对应规则时不允许；不存在"没配规则=全权限"的
//     隐式语义。全权限只能通过 AllowAll 显式构造。
//   - 规则里的桶可以是 "*"，前缀为空表示桶内全部对象。
//   - list 与 admin 没有前缀语义，构造策略时直接拒绝带前缀的规则，
//     避免"看似生效、实际永远匹配不上"的配置。
package authz

import (
	"errors"
	"fmt"
	"strings"
)

// Action 是一次操作被归类后的动作。
type Action string

const (
	// ActionRead 读取对象与其元数据。
	ActionRead Action = "read"
	// ActionWrite 写入对象、分片上传与复制目标。
	ActionWrite Action = "write"
	// ActionDelete 删除对象。
	ActionDelete Action = "delete"
	// ActionList 列举桶与对象、列举分片上传。
	ActionList Action = "list"
	// ActionAdmin 桶级管理：创建/删除桶、读写 CORS 配置。
	ActionAdmin Action = "admin"
)

// AllActions 返回模型支持的全部动作，供文档与测试使用。
func AllActions() []Action {
	return []Action{ActionRead, ActionWrite, ActionDelete, ActionList, ActionAdmin}
}

// Valid 报告动作是否属于模型。
func (a Action) Valid() bool {
	switch a {
	case ActionRead, ActionWrite, ActionDelete, ActionList, ActionAdmin:
		return true
	}
	return false
}

// bucketScoped 报告动作是否只能作用于整个桶（不允许前缀）。
func (a Action) bucketScoped() bool {
	return a == ActionList || a == ActionAdmin
}

// Rule 是一条授权规则。
//
// JSON 形态用于凭据文件；字段名与 06 文档一致。
type Rule struct {
	// Bucket 是桶名，或 "*" 表示全部桶。
	Bucket string `json:"bucket"`
	// Prefix 是对象 key 前缀；为空表示桶内全部 key。
	Prefix string `json:"prefix,omitempty"`
	// Actions 是允许的动作集合。
	Actions []Action `json:"actions"`
}

// compiledRule 是校验后的规则，用 map 做动作查找。
type compiledRule struct {
	bucket  string
	prefix  string
	actions map[Action]bool
}

// Policy 是某个凭据的授权策略。
//
// 零值不可用；必须通过 New 或 AllowAll 构造。
type Policy struct {
	allowAll bool
	rules    []compiledRule
}

// AllowAll 返回全权限策略，供环境变量中的根凭据使用。
//
// 这是**唯一**产生全权限的入口，且必须由配置层显式选择；
// 凭据文件中的凭据不能走这里。
func AllowAll() *Policy { return &Policy{allowAll: true} }

// New 编译并校验规则。空规则集返回错误而不是全权限。
func New(rules []Rule) (*Policy, error) {
	if len(rules) == 0 {
		return nil, errors.New("authz: 至少需要一条规则；全权限必须显式使用 AllowAll")
	}
	p := &Policy{rules: make([]compiledRule, 0, len(rules))}
	for i, r := range rules {
		if err := validateRule(r); err != nil {
			return nil, fmt.Errorf("authz: 第 %d 条规则: %w", i+1, err)
		}
		actions := make(map[Action]bool, len(r.Actions))
		for _, a := range r.Actions {
			actions[a] = true
		}
		p.rules = append(p.rules, compiledRule{
			bucket:  r.Bucket,
			prefix:  r.Prefix,
			actions: actions,
		})
	}
	return p, nil
}

// validateRule 在启动期一次性暴露配置错误。
func validateRule(r Rule) error {
	if strings.TrimSpace(r.Bucket) == "" {
		return errors.New("bucket 不能为空（全部桶请写 \"*\"）")
	}
	if r.Bucket != "*" && strings.ContainsAny(r.Bucket, "/ \t\r\n") {
		return fmt.Errorf("bucket %q 不合法：不能包含空格或 /", r.Bucket)
	}
	if strings.ContainsAny(r.Prefix, "\x00") {
		return errors.New("prefix 不能包含 NUL")
	}
	if len(r.Actions) == 0 {
		return errors.New("actions 不能为空")
	}
	seen := make(map[Action]bool, len(r.Actions))
	for _, a := range r.Actions {
		if !a.Valid() {
			return fmt.Errorf("未知动作 %q（可选：read/write/delete/list/admin）", a)
		}
		if seen[a] {
			return fmt.Errorf("动作 %q 重复", a)
		}
		seen[a] = true
		if a.bucketScoped() && r.Prefix != "" {
			return fmt.Errorf("动作 %q 只作用于整个桶，不能带 prefix", a)
		}
	}
	return nil
}

// Allow 判断策略是否允许对 bucket/key 执行 action。
//
// key 为空表示**桶级检查**：只要该桶上有对应动作的任意规则即通过，
// 前缀不参与匹配。这用于 DeleteObjects 的请求级预检与桶级动作；
// DeleteObjects 随后会逐 key 再检查一次。
func (p *Policy) Allow(action Action, bucket, key string) bool {
	if p == nil {
		// nil 策略表示"没有授权信息"，必须拒绝：调用方漏传策略时
		// 不能退化成放行。
		return false
	}
	if p.allowAll {
		return true
	}
	if !action.Valid() || bucket == "" {
		return false
	}
	for _, r := range p.rules {
		if r.bucket != "*" && r.bucket != bucket {
			continue
		}
		if !r.actions[action] {
			continue
		}
		if key == "" || strings.HasPrefix(key, r.prefix) {
			return true
		}
	}
	return false
}

// VisibleBucket 报告该凭据是否能看到桶（用于 ListBuckets 过滤）。
//
// "看得到"不等同于"能列举"：只要有任何一条规则涉及该桶（含前缀级
// read/write），桶就出现在 ListBuckets 中。这与 AWS 的行为一致：
// 列出桶只反映资源可达性，不代表有 ListObjects 权限。
func (p *Policy) VisibleBucket(bucket string) bool {
	if p == nil {
		return false
	}
	if p.allowAll {
		return true
	}
	for _, r := range p.rules {
		if r.bucket == "*" || r.bucket == bucket {
			return true
		}
	}
	return false
}
