// Package cluster 实现 Blink 的跨节点协调层：静态成员表、一致性哈希环、
// 节点间内部 HTTP 协议与 HMAC 认证。
//
// 单机模式（cluster_mode=single）完全绕过本包：不建环、不监听内部端口、
// 不产生额外 goroutine。集群模式的所有参数都在启动时一次性校验，
// 校验失败拒绝启动 —— 不静默退化成弱一致的单机行为。
package cluster

import (
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// 集群相关默认值。与 docs/reviews/08-distributed-storage.md 的配置表一致。
const (
	DefaultReplicaCount = 3
	DefaultWriteQuorum  = 2
	DefaultReadQuorum   = 2
	DefaultVNodes       = 128
	DefaultPeerTimeout  = 2 * time.Second

	// 故障修复与反熵的默认值。
	//
	// RepairInterval 是调度器的兜底唤醒间隔，不是修复延迟：任务到点会被
	// 精确唤醒，这个值只决定"没有事件时最多睡多久"。
	DefaultRepairInterval = 30 * time.Second
	// DefaultScrubInterval 是后台全量扫描的周期。24h 足够让"从未被读到
	// 的冷数据"也被发现；扫描是顺序的，天然限速。
	DefaultScrubInterval = 24 * time.Hour
	// DefaultRepairMaxConcurrency 是每节点的修复并发上限。
	// 修复是后台流量，压满 IO 会直接伤害在线请求。
	DefaultRepairMaxConcurrency = 2
	// DefaultHintTTL 是修复任务（hinted handoff）的最长保留时间。
	// 超过之后仍失败的副本不再重试，交给反熵的周期扫描重新发现。
	DefaultHintTTL = 72 * time.Hour

	// DefaultECMinSize 是启用 EC 后走纠删码的最小对象大小。
	//
	// 小对象继续走副本：k+m 个分片即使每个只有几百字节，也会在 meta 中
	// 留下 k+m 份描述与 k+m 个 CAS 文件，修复与 LIST 的放大不划算。
	DefaultECMinSize = 32 << 20

	// MinSecretLen 是集群共享密钥的最小长度（字节）。
	//
	// HMAC-SHA256 的密钥短于 32 字节不降低理论强度，但短密钥通常意味着
	// 人选的弱口令；这里用长度阈值把"随手填个 secret"挡在启动之前。
	MinSecretLen = 32

	// maxNodeIDLen 限制节点 ID 长度。节点 ID 会进入 HMAC 头与 JSON，
	// 限制长度与字符集可以避免日志注入与畸形头。
	maxNodeIDLen = 64
	// maxVNodes 是每节点虚拟节点数的上限：它决定环的规模，
	// 过大只会增加启动开销而不改善分布。
	maxVNodes = 4096
	// maxPeers 是成员表规模上限。静态成员表面向中小规模集群，
	// 超过这个数量应当改用真正的成员发现，而不是继续拉长配置。
	maxPeers = 1024
)

// Peer 是成员表中的一个节点。
type Peer struct {
	// ID 是节点标识，须在集群内唯一。
	ID string
	// Addr 是内部协议的 host:port。
	Addr string
}

// URL 返回内部协议的基地址。
func (p Peer) URL() *url.URL {
	return &url.URL{Scheme: "http", Host: p.Addr}
}

// Config 是集群层的完整配置。
//
// 它与 internal/config.Config 刻意分开：后者负责环境变量/命令行解析，
// 本结构只承载集群层自身需要的东西，便于单独测试与复用。
type Config struct {
	// NodeID 是本节点 ID，必须出现在 Peers 中。
	NodeID string
	// Peers 是静态成员表，包含自身。
	Peers []Peer
	// InternalAddr 是节点间内部协议的监听地址。
	InternalAddr string
	// Secret 是节点间 HMAC 共享密钥。
	Secret []byte
	// ReplicaCount 是副本数 N。
	ReplicaCount int
	// WriteQuorum 是写法定人数 W。
	WriteQuorum int
	// ReadQuorum 是读法定人数 R。
	ReadQuorum int
	// VNodes 是每节点的虚拟节点数。
	VNodes int
	// PeerTimeout 是单次节点间请求的超时。
	PeerTimeout time.Duration
	// RepairInterval 是修复调度器的兜底唤醒间隔。
	//
	// 零值表示使用 DefaultRepairInterval（New 会归一化），因此零值 Config
	// 的测试无需感知修复参数。
	RepairInterval time.Duration
	// ScrubInterval 是反熵扫描周期；<0 表示禁用后台扫描。
	ScrubInterval time.Duration
	// RepairMaxConcurrency 是每节点的修复并发上限。
	RepairMaxConcurrency int
	// HintTTL 是修复任务的最长保留期。
	HintTTL time.Duration
	// ReadDegraded 允许在可用副本不足 R 时，从任一可用副本读取。
	//
	// 默认 false：副本不足时回 503 而不是给出"可能是旧版本"的数据。
	// 打开后读可用性优先于一致性，错误日志会标明降级读。
	ReadDegraded bool

	// ECEnabled 开启对象级纠删码：对象 ≥ECMinSize 时按 k+m 分片存放。
	//
	// 默认关闭。EC 改变的是对象布局，滚动升级期间必须全集群同时启用。
	ECEnabled bool
	// ECK / ECM 是数据片与校验片数。
	//
	// 两者都为 0 时按成员数选默认 profile：3 节点 2+1，≥6 节点 4+2。
	// 4–5 节点没有默认值，必须显式指定，避免"以为开了 EC 其实还是副本"
	// 或反之的部署歧义。
	ECK int
	ECM int
	// ECMinSize 是走 EC 的最小对象字节数；0 表示 DefaultECMinSize。
	ECMinSize int64
}

// ParsePeers 解析 id@host:port,... 形式的成员表。
//
// 解析是严格的：空项、重复 ID、重复地址、非法端口全部报错。
// 成员表是启动期配置，任何一项写错都应该在启动时暴露，
// 而不是等到某个 key 落到一个不存在的节点上才发现。
func ParsePeers(s string) ([]Peer, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("集群成员表为空，请设置 BLINK_CLUSTER_PEERS=id@host:port,...")
	}
	items := strings.Split(s, ",")
	if len(items) > maxPeers {
		return nil, fmt.Errorf("集群成员数 %d 超过上限 %d", len(items), maxPeers)
	}

	peers := make([]Peer, 0, len(items))
	seenID := make(map[string]struct{}, len(items))
	seenAddr := make(map[string]struct{}, len(items))
	for i, raw := range items {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return nil, fmt.Errorf("集群成员表第 %d 项为空（多余的逗号？）", i+1)
		}
		id, addr, ok := strings.Cut(raw, "@")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("集群成员表第 %d 项 %q 不是 id@host:port 形式", i+1, raw)
		}
		if err := validateNodeID(id); err != nil {
			return nil, fmt.Errorf("集群成员表第 %d 项: %w", i+1, err)
		}
		host, port, err := net.SplitHostPort(addr)
		if err != nil || host == "" {
			return nil, fmt.Errorf("集群成员表第 %d 项 %q 的地址无效（需要 host:port，IPv6 用 [::1]:9100）", i+1, addr)
		}
		if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
			return nil, fmt.Errorf("集群成员表第 %d 项 %q 的端口无效", i+1, addr)
		}
		if _, dup := seenID[id]; dup {
			return nil, fmt.Errorf("集群成员表存在重复节点 ID %q", id)
		}
		if _, dup := seenAddr[addr]; dup {
			return nil, fmt.Errorf("集群成员表存在重复地址 %q", addr)
		}
		seenID[id] = struct{}{}
		seenAddr[addr] = struct{}{}
		peers = append(peers, Peer{ID: id, Addr: addr})
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("集群成员表为空")
	}
	return peers, nil
}

// Validate 校验集群配置。
//
// 所有问题一次性汇总返回：边端部署时往往只能看到一行启动错误，
// 一次修一个参数会让部署变成多轮试探。
func (c Config) Validate() error {
	var problems []string

	if err := validateNodeID(c.NodeID); err != nil {
		problems = append(problems, err.Error())
	}
	if len(c.Peers) == 0 {
		problems = append(problems, "集群成员表不能为空")
	}

	seenID := make(map[string]struct{}, len(c.Peers))
	seenAddr := make(map[string]struct{}, len(c.Peers))
	selfFound := false
	for _, p := range c.Peers {
		if err := validateNodeID(p.ID); err != nil {
			problems = append(problems, "成员表: "+err.Error())
			continue
		}
		if p.ID == c.NodeID {
			selfFound = true
		}
		if _, dup := seenID[p.ID]; dup {
			problems = append(problems, "成员表存在重复节点 ID "+p.ID)
		}
		if _, dup := seenAddr[p.Addr]; dup {
			problems = append(problems, "成员表存在重复地址 "+p.Addr)
		}
		seenID[p.ID] = struct{}{}
		seenAddr[p.Addr] = struct{}{}
	}
	if c.NodeID != "" && !selfFound {
		problems = append(problems, "node-id "+c.NodeID+" 未出现在成员表中")
	}

	if len(c.Secret) < MinSecretLen {
		problems = append(problems, fmt.Sprintf("cluster secret 至少 %d 字节，当前 %d 字节", MinSecretLen, len(c.Secret)))
	}

	if strings.TrimSpace(c.InternalAddr) == "" {
		problems = append(problems, "内部监听地址不能为空")
	} else if host, port, err := net.SplitHostPort(c.InternalAddr); err != nil || host == "" || port == "" {
		problems = append(problems, "内部监听地址 "+c.InternalAddr+" 无效（需要 host:port）")
	}

	n := len(c.Peers)
	if c.ReplicaCount < 1 || c.ReplicaCount > n {
		problems = append(problems, fmt.Sprintf("replica-count 必须落在 [1, %d]（成员数），当前 %d", n, c.ReplicaCount))
	}
	if c.WriteQuorum < 1 || c.WriteQuorum > c.ReplicaCount {
		problems = append(problems, fmt.Sprintf("write-quorum 必须落在 [1, %d]，当前 %d", c.ReplicaCount, c.WriteQuorum))
	}
	if c.ReadQuorum < 1 || c.ReadQuorum > c.ReplicaCount {
		problems = append(problems, fmt.Sprintf("read-quorum 必须落在 [1, %d]，当前 %d", c.ReplicaCount, c.ReadQuorum))
	}
	// W+R>N 是"读到的副本集合必然与最近一次成功写入的副本集合相交"的充要条件。
	// 不满足时读可能看不到刚写成功的数据，这属于静默弱一致，必须拒绝启动。
	if c.WriteQuorum+c.ReadQuorum <= c.ReplicaCount {
		problems = append(problems, fmt.Sprintf("write-quorum(%d)+read-quorum(%d) 必须大于 replica-count(%d)", c.WriteQuorum, c.ReadQuorum, c.ReplicaCount))
	}

	if c.VNodes < 1 || c.VNodes > maxVNodes {
		problems = append(problems, fmt.Sprintf("vnodes 必须落在 [1, %d]，当前 %d", maxVNodes, c.VNodes))
	}
	if c.PeerTimeout <= 0 {
		problems = append(problems, "peer-timeout 必须为正数")
	}

	// 修复参数：零值表示"用默认值"（见 normalize），负数一律拒绝。
	if c.RepairInterval < 0 {
		problems = append(problems, "repair-interval 不能为负数（0 表示使用默认值）")
	}
	if c.RepairMaxConcurrency < 0 {
		problems = append(problems, "repair-max-concurrency 不能为负数（0 表示使用默认值）")
	}
	if c.HintTTL < 0 {
		problems = append(problems, "hint-ttl 不能为负数（0 表示使用默认值）")
	}

	if c.ECEnabled {
		if _, _, err := c.ecParams(); err != nil {
			problems = append(problems, err.Error())
		}
		if c.ECMinSize < 0 {
			problems = append(problems, "ec-min-size 不能为负数（0 表示使用默认值）")
		}
	}

	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("集群配置校验失败: %s", strings.Join(problems, "; "))
}

// normalize 把零值字段填成默认值。
//
// 零值必须等价于"未配置"：集群层会被大量测试与单机复用代码以
// Config{...} 字面量构造，要求每个调用点都写全修复参数只会制造
// "测试与生产默认值不一致"的漂移。
func (c *Config) normalize() {
	if c.RepairInterval <= 0 {
		c.RepairInterval = DefaultRepairInterval
	}
	if c.ScrubInterval == 0 {
		c.ScrubInterval = DefaultScrubInterval
	}
	if c.RepairMaxConcurrency <= 0 {
		c.RepairMaxConcurrency = DefaultRepairMaxConcurrency
	}
	if c.HintTTL <= 0 {
		c.HintTTL = DefaultHintTTL
	}
	if c.ECEnabled {
		if k, m, err := c.ecParams(); err == nil {
			c.ECK, c.ECM = k, m
		}
		if c.ECMinSize == 0 {
			c.ECMinSize = DefaultECMinSize
		}
	}
}

// ecParams 返回生效的 (k, m)，必要时按成员数选择默认 profile。
//
// 只做纯计算：normalize 与 Validate 共用同一份规则，避免"校验通过但
// 运行时用了另一组参数"这类只在生产才暴露的分歧。
func (c Config) ecParams() (int, int, error) {
	if !c.ECEnabled {
		return 0, 0, nil
	}
	k, m := c.ECK, c.ECM
	if k == 0 && m == 0 {
		switch n := len(c.Peers); {
		case n == 3:
			k, m = 2, 1
		case n >= 6:
			k, m = 4, 2
		default:
			return 0, 0, fmt.Errorf("ec 已启用但成员数 %d 没有默认 profile，请显式设置 ec-k/ec-m（3 节点 2+1，≥6 节点 4+2）", n)
		}
	} else if k == 0 || m == 0 {
		return 0, 0, fmt.Errorf("ec-k 与 ec-m 必须同时设置（当前 k=%d m=%d）", k, m)
	}
	if k < 1 || m < 1 {
		return 0, 0, fmt.Errorf("ec-k 与 ec-m 必须 ≥1（当前 k=%d m=%d）", k, m)
	}
	if k+m > len(c.Peers) {
		return 0, 0, fmt.Errorf("ec-k(%d)+ec-m(%d)=%d 超过成员数 %d", k, m, k+m, len(c.Peers))
	}
	if k+m > 256 {
		return 0, 0, fmt.Errorf("ec-k(%d)+ec-m(%d)=%d 超过 GF(2^8) 上限 256", k, m, k+m)
	}
	return k, m, nil
}

// PeerByID 返回指定 ID 的成员。
func (c Config) PeerByID(id string) (Peer, bool) {
	for _, p := range c.Peers {
		if p.ID == id {
			return p, true
		}
	}
	return Peer{}, false
}

// PeerIDs 返回排序后的成员 ID 列表。
//
// 排序保证同一个成员集合无论配置书写顺序如何，构造出的环完全一致。
func (c Config) PeerIDs() []string {
	ids := make([]string, 0, len(c.Peers))
	for _, p := range c.Peers {
		ids = append(ids, p.ID)
	}
	sort.Strings(ids)
	return ids
}

// validateNodeID 校验节点 ID：非空、不超过 64 字节、只含安全字符。
//
// 节点 ID 会进入 HMAC 头与日志，限制字符集可以防止头注入与日志伪造。
// 该字符集与 K8s 的 Pod 名兼容，便于直接用 downward API 注入。
func validateNodeID(id string) error {
	if id == "" {
		return fmt.Errorf("节点 ID 不能为空")
	}
	if len(id) > maxNodeIDLen {
		return fmt.Errorf("节点 ID %q 超过 %d 字节", id, maxNodeIDLen)
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '_', c == '-':
		default:
			return fmt.Errorf("节点 ID %q 含非法字符 %q（只允许字母、数字、. _ -）", id, string(c))
		}
	}
	return nil
}
