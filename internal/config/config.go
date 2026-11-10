// Package config 定义 Blink 的启动配置。
//
// 配置来源优先级：命令行参数 > 环境变量 > 内置默认值。
//
// 凭据（Access Key / Secret Key）**没有内置默认值**：未提供时拒绝启动，
// 避免发布带有硬编码凭据的二进制。这是 06 文档"默认配置不得泄露凭据"的
// 前置要求。
package config

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/binbinpeng123/Blink/internal/authz"
	"github.com/binbinpeng123/Blink/internal/cluster"
	"github.com/binbinpeng123/Blink/internal/logx"
	"github.com/binbinpeng123/Blink/internal/store"
)

// 默认值集中在此处，便于文档与测试引用。
const (
	DefaultAddr            = "127.0.0.1:9000"
	DefaultDataDir         = "./blink-data"
	DefaultRegion          = "us-east-1"
	DefaultLogLevel        = "info"
	DefaultFsyncPolicy     = "always"
	DefaultMaxClockSkew    = 15 * time.Minute
	DefaultBlockSize       = 4 << 20 // 4 MiB
	DefaultInlineThreshold = 64 << 10
	// DefaultInlineObjectMaxSize 是内容内联的默认上限（创新 D0）。
	//
	// 4 KiB 覆盖边端最常见的配置、清单、小 JSON；再大就会明显放大
	// LIST 的读放大（meta.json 内联后按 base64 膨胀 4/3），
	// 基准与取舍见 docs/03 与 docs/09。
	DefaultInlineObjectMaxSize = 4 << 10
	DefaultHighWatermark       = 0.90
	DefaultLowWatermark        = 0.85
	DefaultMaxBucketCount      = 1000

	// 集群默认值。单机模式下这些字段不参与任何行为。
	DefaultInternalAddr = "0.0.0.0:9100"
	DefaultReplicaCount = cluster.DefaultReplicaCount
	DefaultWriteQuorum  = cluster.DefaultWriteQuorum
	DefaultReadQuorum   = cluster.DefaultReadQuorum
	DefaultVNodes       = cluster.DefaultVNodes
	DefaultPeerTimeout  = cluster.DefaultPeerTimeout

	// 故障修复与反熵的默认值（与 cluster 包保持一致）。
	DefaultRepairInterval       = cluster.DefaultRepairInterval
	DefaultScrubInterval        = cluster.DefaultScrubInterval
	DefaultRepairMaxConcurrency = cluster.DefaultRepairMaxConcurrency
	DefaultHintTTL              = cluster.DefaultHintTTL
	// DefaultReadDegraded 默认关闭：副本不足时回 503 而不是给出可能
	// 过期的数据。
	DefaultReadDegraded = false

	// DefaultECEnabled 默认关闭纠删码：单机与既有集群的行为不变，
	// 是否启用属于部署决策而不是升级副作用。
	DefaultECEnabled = false
	// DefaultECMinSize 与 cluster 包保持一致。
	DefaultECMinSize = cluster.DefaultECMinSize

	// DefaultIntegrityInterval 是后台全量完整性巡检（scrub）的默认周期。
	//
	// 巡检要把每个对象完整读一遍，默认一天一次即可覆盖静默损坏的发现
	// 时效；调短会持续占用磁盘读带宽，属于部署侧的权衡。
	DefaultIntegrityInterval = 24 * time.Hour

	// DefaultTTLSweepInterval 是过期对象后台清扫的默认周期（创新 F'）。
	//
	// 惰性过期已经保证客户端看不到过期对象，清扫只负责回收磁盘；
	// 一分钟一次足以把边端中间结果的磁盘占用控制在一个可接受的窗口，
	// 同时每轮只做一次目录扫描，不读块数据。
	DefaultTTLSweepInterval = time.Minute

	// DefaultGCInterval 是后台 CAS 回收的周期。单机与集群一致：
	// 每个节点只回收本机不再被元数据引用的块。
	DefaultGCInterval = time.Hour
	// DefaultGCGrace 是块在"失去最后一个元数据引用"之后的保留期。
	//
	// 必须覆盖"块已落盘、meta.json 尚未提交"的窗口，以及集群远端
	// 节点提交块的延迟；取 1 小时与 03 文档一致。
	DefaultGCGrace = time.Hour

	// 集群模式取值。
	ClusterModeSingle  = "single"
	ClusterModeCluster = "cluster"
	// DefaultClusterMode 保持单机默认：升级二进制不会改变既有部署的语义。
	DefaultClusterMode = ClusterModeSingle

	// FsyncAlways 表示数据与元数据均落盘后 fsync，可靠性优先。
	FsyncAlways = "always"
	// FsyncMeta 表示仅 fsync 元数据，吞吐优先但掉电窗口更大。
	FsyncMeta = "meta"
)

// 环境变量名。
const (
	EnvAddr      = "BLINK_ADDR"
	EnvDataDir   = "BLINK_DATA_DIR"
	EnvRegion    = "BLINK_REGION"
	EnvAccessKey = "BLINK_ACCESS_KEY"
	EnvSecretKey = "BLINK_SECRET_KEY"
	// EnvCredentialsFile 指向多凭据 JSON 文件；文件中的凭据必须带 rules。
	EnvCredentialsFile = "BLINK_CREDENTIALS_FILE"
	EnvLogLevel        = "BLINK_LOG_LEVEL"
	EnvFsyncPolicy     = "BLINK_FSYNC_POLICY"
	EnvClusterMode     = "BLINK_CLUSTER_MODE"
	EnvVerifyRead      = "BLINK_VERIFY_READ"
	// EnvIntegrityInterval 是后台全量完整性巡检周期；<=0 禁用。
	//
	// 与 EnvScrubInterval（集群反熵，按环扫描分片）是两件事：
	// 这里巡检的是**本节点上已存在的对象数据**。
	EnvIntegrityInterval = "BLINK_INTEGRITY_INTERVAL"
	// EnvInlineObjectMaxSize 控制"内容内联"的小对象上限；0 关闭。
	EnvInlineObjectMaxSize = "BLINK_INLINE_OBJECT_MAX_SIZE"
	// EnvBucketTTL 是按桶配置的对象存活时长，格式 bucket=1h,bucket2=24h。
	EnvBucketTTL = "BLINK_BUCKET_TTL"
	// EnvTTLSweepInterval 是过期对象后台清扫周期；<=0 只禁用清扫。
	EnvTTLSweepInterval = "BLINK_TTL_SWEEP_INTERVAL"
	EnvTLSCert          = "BLINK_TLS_CERT"
	EnvTLSKey           = "BLINK_TLS_KEY"

	// 集群成员与内部协议。密钥只从环境变量读取，不提供命令行参数：
	// 命令行会出现在进程列表与 shell 历史里，而共享密钥泄露等于全集群失守。
	EnvNodeID        = "BLINK_NODE_ID"
	EnvClusterPeers  = "BLINK_CLUSTER_PEERS"
	EnvInternalAddr  = "BLINK_INTERNAL_ADDR"
	EnvClusterSecret = "BLINK_CLUSTER_SECRET"
	EnvReplicaCount  = "BLINK_REPLICA_COUNT"
	EnvWriteQuorum   = "BLINK_WRITE_QUORUM"
	EnvReadQuorum    = "BLINK_READ_QUORUM"
	EnvVNodes        = "BLINK_VNODES"
	EnvPeerTimeout   = "BLINK_PEER_TIMEOUT"

	// 故障修复与反熵。
	EnvRepairInterval       = "BLINK_REPAIR_INTERVAL"
	EnvScrubInterval        = "BLINK_SCRUB_INTERVAL"
	EnvRepairMaxConcurrency = "BLINK_REPAIR_MAX_CONCURRENCY"
	EnvHintTTL              = "BLINK_HINT_TTL"
	EnvReadDegraded         = "BLINK_READ_DEGRADED"

	// 纠删码。k/m 留空（0）时按成员数选 profile。
	EnvECEnabled = "BLINK_EC_ENABLED"
	EnvECK       = "BLINK_EC_K"
	EnvECM       = "BLINK_EC_M"
	EnvECMinSize = "BLINK_EC_MIN_SIZE"

	// CAS 垃圾回收。
	EnvGCInterval = "BLINK_GC_INTERVAL"
	EnvGCGrace    = "BLINK_GC_GRACE"
)

// Config 是服务的完整启动配置。
//
// 该结构体会被整体传入各子系统，因此不得通过 fmt 直接打印（会泄露凭据）；
// 需要输出时使用 Redacted。
type Config struct {
	// Addr 是 HTTP 监听地址，形如 127.0.0.1:9000。
	Addr string
	// DataDir 是数据目录的根路径。
	DataDir string
	// Region 是 S3 签名校验所使用的区域。
	Region string
	// AccessKey 是 S3 访问密钥 ID。
	AccessKey string
	// SecretKey 是 S3 私钥。
	SecretKey string
	// CredentialsFile 是多凭据 JSON 文件路径；为空表示只用环境变量凭据。
	//
	// 文件内容在 Load 中解析并校验，Secret 只保留在内存；日志只输出路径
	// 与凭据数量。
	CredentialsFile string
	// FileCredentials 是凭据文件解析后的结果，仅在 Load 成功后非空。
	//
	// 环境变量凭据（AccessKey/SecretKey）不在其中：它保留全权限语义。
	FileCredentials []FileCredential
	// LogLevel 是日志级别：debug、info、warn、error。
	LogLevel string
	// MaxClockSkew 是签名校验允许的客户端时钟偏移。
	MaxClockSkew time.Duration
	// FsyncPolicy 取值 always 或 meta。
	FsyncPolicy string
	// BlockSize 是大对象的块大小。
	BlockSize int64
	// InlineThreshold 是小对象免分片阈值。
	InlineThreshold int64
	// InlineObjectMaxSize 是内容内联的对象大小上限；0 关闭。
	//
	// 开启后，不超过该值的小对象把内容写进 meta.json，由一次 rename
	// 同时提交数据与元数据，省掉一次块文件持久化。上限不得大于
	// InlineThreshold（一个对象必须是一个块）。
	InlineObjectMaxSize int64
	// VerifyOnRead 开启读取时逐块 CRC32C 校验。
	//
	// 默认关闭以保留 sendfile 零拷贝读路径；打开后每块先校验再写出，
	// 代价是数据要经过用户态。见 03 文档"校验读 vs 零拷贝读"。
	VerifyOnRead bool
	// IntegrityInterval 是后台全量完整性巡检周期；<=0 表示禁用。
	//
	// 与 ScrubInterval（集群反熵）相互独立：后者在集群环上检查分片
	// 归属并触发修复，这里只在本节点上对已有对象重算块校验和。
	IntegrityInterval time.Duration
	// BucketTTL 是按桶配置的对象存活时长（创新 F'）。
	//
	// 写入时换算成绝对过期时刻存进对象元数据；读取/LIST/COPY 把过期
	// 对象视为不存在，后台清扫回收其元数据目录。只对单机模式生效：
	// 集群写路径不经过 Store.PutObject，Validate 在集群模式下显式拒绝。
	BucketTTL map[string]time.Duration
	// TTLSweepInterval 是过期对象后台清扫周期；<=0 只禁用清扫，
	// 惰性过期仍然生效（客户端不会读到过期对象，只是磁盘暂不回收）。
	TTLSweepInterval time.Duration
	// HighWatermark 是拒绝写入的磁盘使用率上限。
	HighWatermark float64
	// LowWatermark 是恢复接受写入的磁盘使用率下限。
	LowWatermark float64
	// MaxBucketCount 是单实例桶数量软上限。
	MaxBucketCount int
	// ClusterMode 取值 single 或 cluster。
	//
	// single（默认）完全绕过集群层：不建环、不监听内部端口、
	// 不产生额外 goroutine。cluster 模式的参数在 Validate 中一次性校验。
	ClusterMode string
	// NodeID 是本节点 ID，集群模式必填且必须出现在 ClusterPeers 中。
	NodeID string
	// ClusterPeers 是静态成员表，格式 id@host:port,...，必须包含自身。
	ClusterPeers string
	// InternalAddr 是节点间内部协议监听地址，不得与 Addr 冲突。
	InternalAddr string
	// ClusterSecret 是节点间 HMAC 共享密钥，集群模式必填且 ≥32 字节。
	//
	// 与 TLSKey 一样，这里只保存值本身而不参与日志输出：Redacted 会替换它。
	ClusterSecret string
	// ReplicaCount 是副本数 N。
	ReplicaCount int
	// WriteQuorum 是写法定人数 W。
	WriteQuorum int
	// ReadQuorum 是读法定人数 R。W+R 必须大于 N。
	ReadQuorum int
	// VNodes 是每节点的虚拟节点数。
	VNodes int
	// PeerTimeout 是单次节点间请求超时。
	PeerTimeout time.Duration
	// RepairInterval 是修复调度器的兜底唤醒间隔。
	RepairInterval time.Duration
	// ScrubInterval 是反熵扫描周期；负值禁用后台扫描。
	ScrubInterval time.Duration
	// RepairMaxConcurrency 是每节点的修复并发上限。
	RepairMaxConcurrency int
	// HintTTL 是修复任务的最长保留期。
	HintTTL time.Duration
	// ReadDegraded 允许在响应副本不足 R 时仍从可用副本读取。
	ReadDegraded bool
	// ECEnabled 开启对象级纠删码；仅对 ≥ECMinSize 的对象生效。
	ECEnabled bool
	// ECK / ECM 是数据片与校验片数；都为 0 时按成员数选择默认 profile。
	ECK int
	ECM int
	// ECMinSize 是走 EC 的最小对象字节数。
	ECMinSize int64
	// GCInterval 是后台 CAS 回收周期；<=0 表示禁用后台回收。
	//
	// 禁用后写入路径的块仍然只增不减，只有显式调用 SweepGarbage
	// 才会回收；这适合由外部调度器按需触发的部署。
	GCInterval time.Duration
	// GCGrace 是块在失去引用后的宽限期，必须为正。
	GCGrace time.Duration
	// TLSCert 是 PEM 证书链文件路径；为空表示不启用 TLS。
	TLSCert string
	// TLSKey 是 PEM 私钥文件路径；必须与 TLSCert 同时提供。
	//
	// 这里只存**路径**而不是密钥内容：日志与配置输出只应出现路径，
	// 私钥字节永远不进入 Config、日志或错误信息。
	TLSKey string

	// envErr 记录环境变量解析失败，由 Validate 转为启动错误。
	//
	// 不能静默忽略：BLINK_VERIFY_READ=yes（拼错的值）如果被当成 false，
	// 运维会以为校验已经打开，而这恰恰是"以为有保护、实际没有"的一类错误。
	envErr error

	// clusterEnvErr 单独记录集群参数的环境变量解析失败。
	//
	// 单机模式必须完全忽略集群配置（老部署残留的变量不应改变行为、
	// 更不应导致启动失败），因此这些错误只在 cluster 模式下并入启动错误；
	// 单机模式通过 ClusterEnvWarnings 输出警告，不静默吞掉笔误。
	clusterEnvErr error
}

// FileCredential 是凭据文件中的一个凭据。
//
// AccessKeyID/SecretKey 从不出现在日志、错误响应或 String() 中。
type FileCredential struct {
	// Name 是可读标识，仅用于日志与审计，不参与鉴权。
	Name string
	// AccessKeyID 是签名用的 Access Key。
	AccessKeyID string
	// SecretKey 是签名用的 Secret Key。
	SecretKey string
	// Policy 是该凭据的授权策略，构造时已校验。
	Policy *authz.Policy
}

// 凭据文件的 JSON 形态（与 06 文档一致）：
//
//	{"credentials":[{"name":"ci","access_key":"…","secret_key":"…",
//	  "rules":[{"bucket":"models","prefix":"llama/","actions":["read","write"]}]}]}
//
// 刻意不提供 YAML/TOML：标准库没有对应解析器，而多引入一个依赖对边端
// 二进制体积的代价高于配置可读性的收益。
type credentialsFile struct {
	Credentials []rawCredential `json:"credentials"`
}

type rawCredential struct {
	Name      string       `json:"name"`
	AccessKey string       `json:"access_key"`
	SecretKey string       `json:"secret_key"`
	Rules     []authz.Rule `json:"rules"`
}

const (
	// maxCredentialsFile 限制配置文件大小，防止把内存作为攻击面。
	maxCredentialsFile = 1 << 20
	// minSecretKeyLen 是 SigV4 HMAC 密钥的工程下限；更短的 secret
	// 可被离线穷举，而 Blink 无法知道上游是否还有额外保护。
	minSecretKeyLen = 16
)

// loadCredentialsFile 解析并校验 --credentials-file。
func loadCredentialsFile(c *Config) error {
	path := strings.TrimSpace(c.CredentialsFile)
	if path == "" {
		return nil
	}

	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("读取凭据文件 %s 失败: %w", path, err)
	}
	if info.IsDir() {
		return fmt.Errorf("凭据文件 %s 是目录", path)
	}
	// 权限检查作用于解析后的目标文件：K8s Secret 卷中的 credentials.json
	// 通常是指向 ..data 的符号链接，直接检查链接本身没有意义。
	//
	// Windows 没有等价的 POSIX 权限位，跳过检查（部署文档要求 ACL 限制）。
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf(
			"凭据文件 %s 权限为 %#o，必须仅所有者可读写（0400 或 0600）；"+
				"Kubernetes Secret 请设置 defaultMode: 0400",
			path, info.Mode().Perm())
	}

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("读取凭据文件 %s 失败: %w", path, err)
	}
	defer f.Close()

	dec := json.NewDecoder(io.LimitReader(f, maxCredentialsFile+1))
	dec.DisallowUnknownFields()
	var raw credentialsFile
	if err := dec.Decode(&raw); err != nil {
		return fmt.Errorf("解析凭据文件 %s 失败: %w", path, err)
	}
	// 只允许一个顶层 JSON 值：多余内容通常意味着拼接错误或文件被截断后
	// 又追加了内容，静默忽略会让"实际生效的凭据"与文件内容不一致。
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		return fmt.Errorf("凭据文件 %s 含多余内容（只允许一个 JSON 对象）", path)
	}
	if len(raw.Credentials) == 0 {
		return fmt.Errorf("凭据文件 %s 至少需要一组 credentials", path)
	}

	seen := make(map[string]string, len(raw.Credentials)+1)
	if c.AccessKey != "" {
		seen[c.AccessKey] = "环境变量/命令行"
	}
	for i, rc := range raw.Credentials {
		if strings.TrimSpace(rc.AccessKey) == "" {
			return fmt.Errorf("凭据文件 %s 第 %d 组缺少 access_key", path, i+1)
		}
		if strings.TrimSpace(rc.SecretKey) == "" {
			return fmt.Errorf("凭据文件 %s 第 %d 组缺少 secret_key", path, i+1)
		}
		if len(rc.SecretKey) < minSecretKeyLen {
			return fmt.Errorf("凭据文件 %s 第 %d 组的 secret_key 少于 %d 字节", path, i+1, minSecretKeyLen)
		}
		if prev, dup := seen[rc.AccessKey]; dup {
			return fmt.Errorf("凭据文件 %s 中的 access_key %q 与%s重复",
				path, logx.RedactKeyID(rc.AccessKey), prev)
		}
		seen[rc.AccessKey] = "本文件中的其他凭据"
		// 文件凭据必须显式授权；authz.New 对空规则集返回错误，
		// 从而避免"忘了写 rules 就得到全权限"。
		policy, err := authz.New(rc.Rules)
		if err != nil {
			return fmt.Errorf("凭据文件 %s 第 %d 组（%s）: %w",
				path, i+1, nameOrIndex(rc.Name, i), err)
		}
		c.FileCredentials = append(c.FileCredentials, FileCredential{
			Name:        strings.TrimSpace(rc.Name),
			AccessKeyID: rc.AccessKey,
			SecretKey:   rc.SecretKey,
			Policy:      policy,
		})
	}
	return nil
}

func nameOrIndex(name string, i int) string {
	if strings.TrimSpace(name) != "" {
		return name
	}
	return fmt.Sprintf("#%d", i+1)
}

// CredentialCount 返回生效的凭据总数（环境变量凭据 + 文件凭据）。
func (c Config) CredentialCount() int {
	n := len(c.FileCredentials)
	if c.AccessKey != "" {
		n++
	}
	return n
}

// Default 返回全部使用默认值的配置（凭据为空）。
func Default() Config {
	return Config{
		Addr:                DefaultAddr,
		DataDir:             DefaultDataDir,
		Region:              DefaultRegion,
		LogLevel:            DefaultLogLevel,
		MaxClockSkew:        DefaultMaxClockSkew,
		FsyncPolicy:         DefaultFsyncPolicy,
		IntegrityInterval:   DefaultIntegrityInterval,
		TTLSweepInterval:    DefaultTTLSweepInterval,
		BlockSize:           DefaultBlockSize,
		InlineThreshold:     DefaultInlineThreshold,
		InlineObjectMaxSize: DefaultInlineObjectMaxSize,
		HighWatermark:       DefaultHighWatermark,
		LowWatermark:        DefaultLowWatermark,
		MaxBucketCount:      DefaultMaxBucketCount,
		ClusterMode:         DefaultClusterMode,
		InternalAddr:        DefaultInternalAddr,
		ReplicaCount:        DefaultReplicaCount,
		WriteQuorum:         DefaultWriteQuorum,
		ReadQuorum:          DefaultReadQuorum,
		VNodes:              DefaultVNodes,
		PeerTimeout:         DefaultPeerTimeout,

		RepairInterval:       DefaultRepairInterval,
		ScrubInterval:        DefaultScrubInterval,
		RepairMaxConcurrency: DefaultRepairMaxConcurrency,
		HintTTL:              DefaultHintTTL,
		ReadDegraded:         DefaultReadDegraded,
		ECEnabled:            DefaultECEnabled,
		ECMinSize:            DefaultECMinSize,

		GCInterval: DefaultGCInterval,
		GCGrace:    DefaultGCGrace,
	}
}

// Load 按"参数 > 环境变量 > 默认值"的优先级解析配置并校验。
//
// args 通常传入 os.Args[1:]；out 用于输出参数用法，nil 表示丢弃。
func Load(args []string, out io.Writer) (Config, error) {
	cfg := Default()

	// 先应用环境变量，再由命令行参数覆盖。
	applyEnv(&cfg)

	fs := flag.NewFlagSet("blink", flag.ContinueOnError)
	if out != nil {
		fs.SetOutput(out)
	} else {
		fs.SetOutput(io.Discard)
	}

	fs.StringVar(&cfg.Addr, "addr", cfg.Addr, "HTTP 监听地址（环境变量 "+EnvAddr+"）")
	fs.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "数据目录（环境变量 "+EnvDataDir+"）")
	fs.StringVar(&cfg.Region, "region", cfg.Region, "S3 区域（环境变量 "+EnvRegion+"）")
	fs.StringVar(&cfg.AccessKey, "access-key", cfg.AccessKey, "S3 Access Key（环境变量 "+EnvAccessKey+"）")
	fs.StringVar(&cfg.SecretKey, "secret-key", cfg.SecretKey, "S3 Secret Key（环境变量 "+EnvSecretKey+"）")
	fs.StringVar(&cfg.CredentialsFile, "credentials-file", cfg.CredentialsFile, "多凭据 JSON 文件路径；文件凭据必须带 rules（环境变量 "+EnvCredentialsFile+"）")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "日志级别：debug|info|warn|error（环境变量 "+EnvLogLevel+"）")
	fs.StringVar(&cfg.FsyncPolicy, "fsync-policy", cfg.FsyncPolicy, "落盘策略：always|meta（环境变量 "+EnvFsyncPolicy+"）")
	fs.StringVar(&cfg.ClusterMode, "cluster-mode", cfg.ClusterMode, "运行模式：single|cluster（环境变量 "+EnvClusterMode+"）")
	fs.StringVar(&cfg.NodeID, "node-id", cfg.NodeID, "本节点 ID，须出现在 cluster-peers 中（环境变量 "+EnvNodeID+"）")
	fs.StringVar(&cfg.ClusterPeers, "cluster-peers", cfg.ClusterPeers, "静态成员表 id@host:port,...，含自身（环境变量 "+EnvClusterPeers+"）")
	fs.StringVar(&cfg.InternalAddr, "internal-addr", cfg.InternalAddr, "节点间内部协议监听地址（环境变量 "+EnvInternalAddr+"）")
	fs.IntVar(&cfg.ReplicaCount, "replica-count", cfg.ReplicaCount, "副本数 N（环境变量 "+EnvReplicaCount+"）")
	fs.IntVar(&cfg.WriteQuorum, "write-quorum", cfg.WriteQuorum, "写法定人数 W（环境变量 "+EnvWriteQuorum+"）")
	fs.IntVar(&cfg.ReadQuorum, "read-quorum", cfg.ReadQuorum, "读法定人数 R（环境变量 "+EnvReadQuorum+"）")
	fs.IntVar(&cfg.VNodes, "vnodes", cfg.VNodes, "每节点虚拟节点数（环境变量 "+EnvVNodes+"）")
	fs.DurationVar(&cfg.PeerTimeout, "peer-timeout", cfg.PeerTimeout, "节点间请求超时（环境变量 "+EnvPeerTimeout+"）")
	fs.DurationVar(&cfg.RepairInterval, "repair-interval", cfg.RepairInterval, "修复调度器兜底唤醒间隔（环境变量 "+EnvRepairInterval+"）")
	fs.DurationVar(&cfg.ScrubInterval, "scrub-interval", cfg.ScrubInterval, "反熵扫描周期，负值禁用（环境变量 "+EnvScrubInterval+"）")
	fs.DurationVar(&cfg.IntegrityInterval, "integrity-interval", cfg.IntegrityInterval, "后台完整性巡检周期，<=0 禁用（环境变量 "+EnvIntegrityInterval+"）")
	fs.IntVar(&cfg.RepairMaxConcurrency, "repair-max-concurrency", cfg.RepairMaxConcurrency, "每节点修复并发上限（环境变量 "+EnvRepairMaxConcurrency+"）")
	fs.DurationVar(&cfg.HintTTL, "hint-ttl", cfg.HintTTL, "修复任务最长保留期（环境变量 "+EnvHintTTL+"）")
	fs.BoolVar(&cfg.ECEnabled, "ec-enabled", cfg.ECEnabled, "开启对象级纠删码（环境变量 "+EnvECEnabled+"）")
	fs.IntVar(&cfg.ECK, "ec-k", cfg.ECK, "EC 数据片数，0 表示按成员数选 profile（环境变量 "+EnvECK+"）")
	fs.IntVar(&cfg.ECM, "ec-m", cfg.ECM, "EC 校验片数，0 表示按成员数选 profile（环境变量 "+EnvECM+"）")
	fs.Var(sizeValue{&cfg.ECMinSize}, "ec-min-size", "走 EC 的最小对象大小，如 32MiB（环境变量 "+EnvECMinSize+"）")
	fs.DurationVar(&cfg.GCInterval, "gc-interval", cfg.GCInterval, "后台 CAS 回收周期，<=0 禁用（环境变量 "+EnvGCInterval+"）")
	fs.DurationVar(&cfg.GCGrace, "gc-grace", cfg.GCGrace, "块失去引用后的保留期，必须为正（环境变量 "+EnvGCGrace+"）")
	fs.DurationVar(&cfg.MaxClockSkew, "max-clock-skew", cfg.MaxClockSkew, "签名校验允许的时钟偏移")
	fs.Int64Var(&cfg.BlockSize, "block-size", cfg.BlockSize, "大对象块大小（字节）")
	fs.Int64Var(&cfg.InlineThreshold, "inline-threshold", cfg.InlineThreshold, "小对象内联阈值（字节）")
	fs.Var(sizeValue{&cfg.InlineObjectMaxSize}, "inline-object-max-size", "内容内联的对象大小上限，0 关闭（环境变量 "+EnvInlineObjectMaxSize+"）")
	fs.Func("bucket-ttl", "按桶配置对象存活时长，格式 bucket=1h,bucket2=24h（环境变量 "+EnvBucketTTL+"；仅单机模式）", func(v string) error {
		m, err := parseBucketTTL(v)
		if err != nil {
			return err
		}
		cfg.BucketTTL = m
		return nil
	})
	fs.DurationVar(&cfg.TTLSweepInterval, "ttl-sweep-interval", cfg.TTLSweepInterval, "过期对象后台清扫周期，<=0 只禁用清扫（环境变量 "+EnvTTLSweepInterval+"）")
	fs.Float64Var(&cfg.HighWatermark, "disk-high-watermark", cfg.HighWatermark, "拒绝写入的磁盘水位")
	fs.Float64Var(&cfg.LowWatermark, "disk-low-watermark", cfg.LowWatermark, "恢复写入的磁盘水位")
	fs.IntVar(&cfg.MaxBucketCount, "max-buckets", cfg.MaxBucketCount, "桶数量软上限")
	fs.BoolVar(&cfg.VerifyOnRead, "verify-read", cfg.VerifyOnRead, "读取时逐块校验 CRC32C（环境变量 "+EnvVerifyRead+"；关闭时走 sendfile 零拷贝）")
	fs.StringVar(&cfg.TLSCert, "tls-cert", cfg.TLSCert, "TLS 证书链 PEM 路径（环境变量 "+EnvTLSCert+"；与 --tls-key 成对提供）")
	fs.StringVar(&cfg.TLSKey, "tls-key", cfg.TLSKey, "TLS 私钥 PEM 路径（环境变量 "+EnvTLSKey+"；与 --tls-cert 成对提供）")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	// 凭据文件在参数解析之后读取：重复 Access Key 检查需要最终生效的
	// 环境变量凭据，而配置文件里的凭据不能在此之前被加载。
	if err := loadCredentialsFile(&cfg); err != nil {
		return Config{}, err
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// applyEnv 从环境变量读取配置，未设置的项保持原值。
func applyEnv(cfg *Config) {
	setIfPresent(EnvAddr, &cfg.Addr)
	setIfPresent(EnvDataDir, &cfg.DataDir)
	setIfPresent(EnvRegion, &cfg.Region)
	setIfPresent(EnvAccessKey, &cfg.AccessKey)
	setIfPresent(EnvSecretKey, &cfg.SecretKey)
	setIfPresent(EnvCredentialsFile, &cfg.CredentialsFile)
	setIfPresent(EnvLogLevel, &cfg.LogLevel)
	setIfPresent(EnvFsyncPolicy, &cfg.FsyncPolicy)
	setIfPresent(EnvClusterMode, &cfg.ClusterMode)
	setIfPresent(EnvNodeID, &cfg.NodeID)
	setIfPresent(EnvClusterPeers, &cfg.ClusterPeers)
	setIfPresent(EnvInternalAddr, &cfg.InternalAddr)
	setIfPresent(EnvClusterSecret, &cfg.ClusterSecret)
	setIfPresent(EnvTLSCert, &cfg.TLSCert)
	setIfPresent(EnvTLSKey, &cfg.TLSKey)

	cfg.applyIntEnv(EnvReplicaCount, &cfg.ReplicaCount, &cfg.clusterEnvErr)
	cfg.applyIntEnv(EnvWriteQuorum, &cfg.WriteQuorum, &cfg.clusterEnvErr)
	cfg.applyIntEnv(EnvReadQuorum, &cfg.ReadQuorum, &cfg.clusterEnvErr)
	cfg.applyIntEnv(EnvVNodes, &cfg.VNodes, &cfg.clusterEnvErr)
	cfg.applyDurationEnv(EnvPeerTimeout, &cfg.PeerTimeout, &cfg.clusterEnvErr)
	cfg.applyDurationEnv(EnvRepairInterval, &cfg.RepairInterval, &cfg.clusterEnvErr)
	cfg.applyDurationEnv(EnvScrubInterval, &cfg.ScrubInterval, &cfg.clusterEnvErr)
	// 完整性巡检在单机模式下同样生效，因此错误必须记入 envErr；
	// 写进 clusterEnvErr 会让单机部署静默忽略一个写错的周期。
	cfg.applyDurationEnv(EnvIntegrityInterval, &cfg.IntegrityInterval, &cfg.envErr)
	cfg.applyIntEnv(EnvRepairMaxConcurrency, &cfg.RepairMaxConcurrency, &cfg.clusterEnvErr)
	cfg.applyDurationEnv(EnvHintTTL, &cfg.HintTTL, &cfg.clusterEnvErr)
	cfg.applyIntEnv(EnvECK, &cfg.ECK, &cfg.clusterEnvErr)
	cfg.applyIntEnv(EnvECM, &cfg.ECM, &cfg.clusterEnvErr)
	cfg.applySizeEnv(EnvECMinSize, &cfg.ECMinSize, &cfg.clusterEnvErr)
	// 内容内联在单机模式下同样生效，错误必须记入 envErr。
	cfg.applySizeEnv(EnvInlineObjectMaxSize, &cfg.InlineObjectMaxSize, &cfg.envErr)
	// TTL 只用于单机模式；在集群模式下配置本身会被 Validate 拒绝，
	// 但格式错误仍应记入 envErr，先于"不支持的配置"暴露出来。
	if v := strings.TrimSpace(os.Getenv(EnvBucketTTL)); v != "" {
		m, err := parseBucketTTL(v)
		if err != nil {
			cfg.addEnvErr(fmt.Errorf("%s: %w", EnvBucketTTL, err))
		} else {
			cfg.BucketTTL = m
		}
	}
	cfg.applyDurationEnv(EnvTTLSweepInterval, &cfg.TTLSweepInterval, &cfg.envErr)
	// GC 在单机与集群模式都生效，因此解析失败记入 envErr 而不是
	// clusterEnvErr：单机模式下写错的 GC 参数不应被静默忽略。
	cfg.applyDurationEnv(EnvGCInterval, &cfg.GCInterval, &cfg.envErr)
	cfg.applyDurationEnv(EnvGCGrace, &cfg.GCGrace, &cfg.envErr)

	if v := os.Getenv("BLINK_MAX_CLOCK_SKEW"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.MaxClockSkew = d
		}
	}
	// 布尔值只接受 strconv.ParseBool 认识的写法（1/0、t/f、true/false、TRUE/FALSE）。
	// 解析失败记入 envErr，而不是悄悄当作 false。
	if v := os.Getenv(EnvVerifyRead); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			cfg.addEnvErr(fmt.Errorf("%s 取值 %q 不是合法的布尔值", EnvVerifyRead, v))
		} else {
			cfg.VerifyOnRead = b
		}
	}
	if v := os.Getenv(EnvReadDegraded); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			cfg.clusterEnvErr = errors.Join(cfg.clusterEnvErr,
				fmt.Errorf("%s 取值 %q 不是合法的布尔值", EnvReadDegraded, v))
		} else {
			cfg.ReadDegraded = b
		}
	}
	if v := os.Getenv(EnvECEnabled); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			cfg.clusterEnvErr = errors.Join(cfg.clusterEnvErr,
				fmt.Errorf("%s 取值 %q 不是合法的布尔值", EnvECEnabled, v))
		} else {
			cfg.ECEnabled = b
		}
	}
}

// applyIntEnv 解析整数环境变量；失败记入 sink 而不是保留默认值 ——
// "BLINK_VNODES=12O（字母 O）"被静默忽略会让人以为参数已经生效。
func (c *Config) applyIntEnv(key string, dst *int, sink *error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		*sink = errors.Join(*sink, fmt.Errorf("%s 取值 %q 不是合法的整数", key, v))
		return
	}
	*dst = n
}

// applyDurationEnv 解析时长环境变量，规则同 applyIntEnv。
func (c *Config) applyDurationEnv(key string, dst *time.Duration, sink *error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		*sink = errors.Join(*sink, fmt.Errorf("%s 取值 %q 不是合法的时长（如 2s、500ms）", key, v))
		return
	}
	*dst = d
}

// applySizeEnv 解析带单位的大小环境变量（如 32MiB、1GiB，或纯字节数）。
func (c *Config) applySizeEnv(key string, dst *int64, sink *error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return
	}
	n, err := parseSize(v)
	if err != nil {
		*sink = errors.Join(*sink, fmt.Errorf("%s 取值 %q 无效: %v", key, v, err))
		return
	}
	*dst = n
}

// sizeValue 让 flag 包接受 "32MiB" 这类大小写法。
type sizeValue struct{ dst *int64 }

func (v sizeValue) String() string {
	if v.dst == nil {
		return "0"
	}
	return strconv.FormatInt(*v.dst, 10)
}

func (v sizeValue) Set(s string) error {
	n, err := parseSize(s)
	if err != nil {
		return err
	}
	if v.dst != nil {
		*v.dst = n
	}
	return nil
}

// parseSize 解析大小，支持 B/KiB/MiB/GiB/TiB。
//
// KB/MB/GB/TB 也按 1024 进制解释：这是存储配置里的常见写法，
// 与其纠结 SI 与 IEC 的差异，不如让同一后缀永远表示同一个数值。
func parseSize(raw string) (int64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, errors.New("大小为空")
	}
	upper := strings.ToUpper(s)
	var unit int64 = 1
	switch {
	case strings.HasSuffix(upper, "TIB"):
		unit, s = 1<<40, s[:len(s)-3]
	case strings.HasSuffix(upper, "GIB"):
		unit, s = 1<<30, s[:len(s)-3]
	case strings.HasSuffix(upper, "MIB"):
		unit, s = 1<<20, s[:len(s)-3]
	case strings.HasSuffix(upper, "KIB"):
		unit, s = 1<<10, s[:len(s)-3]
	case strings.HasSuffix(upper, "TB"):
		unit, s = 1<<40, s[:len(s)-2]
	case strings.HasSuffix(upper, "GB"):
		unit, s = 1<<30, s[:len(s)-2]
	case strings.HasSuffix(upper, "MB"):
		unit, s = 1<<20, s[:len(s)-2]
	case strings.HasSuffix(upper, "KB"):
		unit, s = 1<<10, s[:len(s)-2]
	case strings.HasSuffix(upper, "B"):
		s = s[:len(s)-1]
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("无法解析大小 %q（示例：32MiB、1GiB、33554432）", raw)
	}
	if n < 0 {
		return 0, errors.New("大小不能为负数")
	}
	if n > (1<<63-1)/unit {
		return 0, fmt.Errorf("大小 %q 超出 int64 范围", raw)
	}
	return n * unit, nil
}

// parseBucketTTL 解析 "bucket=1h,bucket2=24h" 形式的桶级 TTL 配置。
//
// 空串返回 nil（等于不启用），而不是空 map：调用方据此判断"是否配置"。
// 重复桶与非法桶名都直接报错 —— TTL 配错会让数据提前消失或永不消失，
// 静默取第一个值不是可接受的行为。
func parseBucketTTL(raw string) (map[string]time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	out := make(map[string]time.Duration)
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			return nil, fmt.Errorf("bucket-ttl %q 含空条目", raw)
		}
		bucket, ttlText, ok := strings.Cut(item, "=")
		if !ok {
			return nil, fmt.Errorf("bucket-ttl 条目 %q 缺少 '='（示例：models=24h）", item)
		}
		bucket = strings.TrimSpace(bucket)
		ttlText = strings.TrimSpace(ttlText)
		if err := store.ValidateBucketName(bucket); err != nil {
			return nil, fmt.Errorf("bucket-ttl 中的桶名 %q 非法: %w", bucket, err)
		}
		ttl, err := time.ParseDuration(ttlText)
		if err != nil {
			return nil, fmt.Errorf("bucket-ttl 中桶 %s 的时长 %q 无法解析（示例：1h、24h、30m）", bucket, ttlText)
		}
		if ttl <= 0 {
			return nil, fmt.Errorf("bucket-ttl 中桶 %s 的时长必须为正数，当前 %v", bucket, ttl)
		}
		if _, dup := out[bucket]; dup {
			return nil, fmt.Errorf("bucket-ttl 重复配置了桶 %s", bucket)
		}
		out[bucket] = ttl
	}
	return out, nil
}

// addEnvErr 累积环境变量解析错误。
//
// 用 errors.Join 而不是覆盖：多个变量同时写错时，运维应当一次看到全部问题。
func (c *Config) addEnvErr(err error) {
	c.envErr = errors.Join(c.envErr, err)
}

// setIfPresent 仅在环境变量存在且非空时覆盖目标字段。
func setIfPresent(key string, dst *string) {
	if v := os.Getenv(key); v != "" {
		*dst = v
	}
}

// Validate 校验配置的完整性与一致性。
func (c Config) Validate() error {
	var problems []string

	if strings.TrimSpace(c.Addr) == "" {
		problems = append(problems, "监听地址不能为空")
	}
	if strings.TrimSpace(c.DataDir) == "" {
		problems = append(problems, "数据目录不能为空")
	}
	if strings.TrimSpace(c.Region) == "" {
		problems = append(problems, "区域不能为空")
	}

	// 凭据没有默认值：一个都没有即拒绝启动。
	//
	// 环境变量凭据必须成对：只给 Access Key 意味着缺少 Secret；
	// 只给 Secret Key 则永远无法用于签名。这与"误以为自己配好了"
	// 的失败模式相比，启动期直接报错更可控。
	envAK := strings.TrimSpace(c.AccessKey) != ""
	envSK := strings.TrimSpace(c.SecretKey) != ""
	switch {
	case envAK && !envSK:
		problems = append(problems, "提供了 Access Key 但缺少 Secret Key，请设置 --secret-key 或 "+EnvSecretKey)
	case !envAK && envSK:
		problems = append(problems, "提供了 Secret Key 但缺少 Access Key，请设置 --access-key 或 "+EnvAccessKey)
	}
	if c.CredentialsFile != "" && len(c.FileCredentials) == 0 {
		// Load 已负责解析文件；走到这里说明 Config 被手工构造或加载失败后
		// 仍继续启动，两种情况下都没有可用凭据。
		problems = append(problems, "credentials-file 已设置但没有加载到任何凭据")
	}
	if !envAK && len(c.FileCredentials) == 0 {
		problems = append(problems, "缺少 S3 凭据：请设置 --access-key（Access Key）与 --secret-key（Secret Key），或通过 --credentials-file 提供至少一组凭据")
	}

	switch c.FsyncPolicy {
	case FsyncAlways, FsyncMeta:
	default:
		problems = append(problems, "fsync-policy 取值非法，必须是 "+FsyncAlways+" 或 "+FsyncMeta)
	}

	// 集群模式：single 与 cluster 之外的值拒绝启动，不静默降级。
	//
	// 单机模式刻意**忽略**全部集群字段：升级时遗留的 BLINK_NODE_ID
	// 之类的环境变量不应改变单机行为，也不应因为写错而拒绝启动。
	switch c.ClusterMode {
	case ClusterModeSingle:
	case ClusterModeCluster:
		cc, err := c.ClusterConfig()
		if err != nil {
			problems = append(problems, err.Error())
		} else if err := cc.Validate(); err != nil {
			problems = append(problems, err.Error())
		}
		if strings.TrimSpace(c.InternalAddr) != "" && sameEndpoint(c.Addr, c.InternalAddr) {
			problems = append(problems, "internal-addr 不能与 addr 使用同一端口，内部协议必须独立监听")
		}
		if c.clusterEnvErr != nil {
			problems = append(problems, c.clusterEnvErr.Error())
		}
	default:
		problems = append(problems, "cluster-mode 必须是 "+ClusterModeSingle+" 或 "+ClusterModeCluster+"，当前 "+strconv.Quote(c.ClusterMode))
	}

	if c.HighWatermark <= 0 || c.HighWatermark > 1 {
		problems = append(problems, "disk-high-watermark 必须落在 (0, 1] 区间")
	}
	if c.LowWatermark <= 0 || c.LowWatermark > 1 {
		problems = append(problems, "disk-low-watermark 必须落在 (0, 1] 区间")
	}
	if c.LowWatermark >= c.HighWatermark {
		problems = append(problems, "disk-low-watermark 必须小于 disk-high-watermark，否则水位会抖动")
	}

	if c.BlockSize <= 0 {
		problems = append(problems, "block-size 必须为正数")
	}
	if c.InlineThreshold < 0 {
		problems = append(problems, "inline-threshold 不能为负数")
	}
	if c.InlineThreshold > c.BlockSize {
		problems = append(problems, "inline-threshold 不应大于 block-size")
	}
	if c.InlineObjectMaxSize < 0 {
		problems = append(problems, "inline-object-max-size 不能为负数（0 表示关闭）")
	}
	// InlineThreshold==0 在 store 层会取默认 64 KiB，因此比较对象是
	// "实际生效的阈值"，而不是字面上的 0。
	inlineLimit := c.InlineThreshold
	if inlineLimit == 0 {
		inlineLimit = DefaultInlineThreshold
	}
	if c.InlineObjectMaxSize > inlineLimit {
		problems = append(problems, "inline-object-max-size 不应大于 inline-threshold")
	}
	// 桶级 TTL：桶名与时长在这里再校验一次（Config 可能被手工构造，
	// 不经过 parseBucketTTL）。集群模式显式拒绝而不是静默忽略：
	// 集群写路径不经过 Store.PutObject，配置了也不会生效，静默接受
	// 会让运维以为中间结果正在自动过期。
	if len(c.BucketTTL) > 0 {
		if c.ClusterMode == ClusterModeCluster {
			problems = append(problems, "bucket-ttl 只支持单机模式：集群写路径不经过本地 PutObject，无法保证副本按同一策略过期")
		}
		buckets := make([]string, 0, len(c.BucketTTL))
		for bucket := range c.BucketTTL {
			buckets = append(buckets, bucket)
		}
		sort.Strings(buckets)
		for _, bucket := range buckets {
			if err := store.ValidateBucketName(bucket); err != nil {
				problems = append(problems, fmt.Sprintf("bucket-ttl 中的桶名 %q 非法: %v", bucket, err))
				continue
			}
			if ttl := c.BucketTTL[bucket]; ttl <= 0 {
				problems = append(problems, fmt.Sprintf("bucket-ttl 中桶 %s 的时长必须为正数，当前 %v", bucket, ttl))
			}
		}
	}
	if c.MaxClockSkew <= 0 {
		problems = append(problems, "max-clock-skew 必须为正数")
	}
	if c.MaxBucketCount <= 0 {
		problems = append(problems, "max-buckets 必须为正数")
	}

	// GC 宽限期在单机与集群模式都必须为正：它覆盖"块已落盘、
	// meta.json 尚未提交"的窗口。0 或负值会让并发的写入被本轮回收
	// 误删，属于数据损坏级别的配置错误，直接拒绝启动。
	//
	// GCInterval 允许 <=0（禁用后台回收），由外部调度器按需触发，
	// 因此这里不校验。
	if c.GCGrace <= 0 {
		problems = append(problems, "gc-grace 必须为正（如 1h）：宽限期覆盖块落盘到元数据提交之间的窗口")
	}

	// 证书与私钥必须成对出现。只给其中一个时，若静默退回明文 HTTP，
	// 运维会以为服务已经在 TLS 上 —— 这正是"以为有保护、实际没有"。
	if (c.TLSCert == "") != (c.TLSKey == "") {
		problems = append(problems, "tls-cert 与 tls-key 必须同时提供或同时为空")
	}

	if c.envErr != nil {
		problems = append(problems, c.envErr.Error())
	}

	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("配置校验失败: %s", strings.Join(problems, "; "))
}

// ClusterEnvWarnings 返回单机模式下被忽略的集群环境变量解析错误。
//
// 单机模式不读取这些参数，但笔误仍应出现在启动日志里 —— 否则运维会以为
// BLINK_VNODES=12O 已经生效，切到集群模式时才发现配置从未被接受。
func (c Config) ClusterEnvWarnings() []string {
	if c.ClusterMode == ClusterModeCluster || c.clusterEnvErr == nil {
		return nil
	}
	return strings.Split(c.clusterEnvErr.Error(), "\n")
}

// ClusterConfig 把集群相关字段转换成 cluster 层的强类型配置。
//
// 只在 cluster 模式下调用；single 模式不解析成员表，避免无关配置影响单机启动。
func (c Config) ClusterConfig() (cluster.Config, error) {
	peers, err := cluster.ParsePeers(c.ClusterPeers)
	if err != nil {
		return cluster.Config{}, err
	}
	return cluster.Config{
		NodeID:       c.NodeID,
		Peers:        peers,
		InternalAddr: c.InternalAddr,
		Secret:       []byte(c.ClusterSecret),
		ReplicaCount: c.ReplicaCount,
		WriteQuorum:  c.WriteQuorum,
		ReadQuorum:   c.ReadQuorum,
		VNodes:       c.VNodes,
		PeerTimeout:  c.PeerTimeout,

		RepairInterval:       c.RepairInterval,
		ScrubInterval:        c.ScrubInterval,
		RepairMaxConcurrency: c.RepairMaxConcurrency,
		HintTTL:              c.HintTTL,
		ReadDegraded:         c.ReadDegraded,

		ECEnabled: c.ECEnabled,
		ECK:       c.ECK,
		ECM:       c.ECM,
		ECMinSize: c.ECMinSize,
	}, nil
}

// sameEndpoint 判断两个监听地址是否可能争用同一端口。
//
// 只做保守判断：端口不同一定不冲突；主机相同一定冲突；
// 任一方是通配地址（0.0.0.0 / :: / 空）时同端口也视为冲突。
// 主机名解析到同一 IP 的情况不在检查范围内 —— 启动期不做 DNS，
// 那种冲突会让第二次 Listen 直接失败，错误信息已经足够明确。
func sameEndpoint(a, b string) bool {
	aHost, aPort, errA := net.SplitHostPort(a)
	bHost, bPort, errB := net.SplitHostPort(b)
	if errA != nil || errB != nil || aPort != bPort {
		return false
	}
	unspecified := func(host string) bool {
		return host == "" || host == "0.0.0.0" || host == "::"
	}
	return aHost == bHost || unspecified(aHost) || unspecified(bHost)
}

// Redacted 返回可安全写入日志的配置副本，凭据字段被替换为占位符。
func (c Config) Redacted() map[string]string {
	return map[string]string{
		"addr":       c.Addr,
		"data_dir":   c.DataDir,
		"region":     c.Region,
		"access_key": logx.RedactKeyID(c.AccessKey),
		"secret_key": logx.Redact(c.SecretKey),
		// 凭据文件只输出路径与数量：文件内容（含全部 secret）永远不进日志。
		"credentials_file":       c.CredentialsFile,
		"credential_count":       strconv.Itoa(c.CredentialCount()),
		"full_access_credential": strconv.FormatBool(c.AccessKey != ""),
		"log_level":              c.LogLevel,
		"fsync_policy":           c.FsyncPolicy,
		"cluster_mode":           c.ClusterMode,
		"block_size":             strconv.FormatInt(c.BlockSize, 10),
		"inline_threshold":       strconv.FormatInt(c.InlineThreshold, 10),
		"inline_object_max_size": strconv.FormatInt(c.InlineObjectMaxSize, 10),
		"verify_read":            strconv.FormatBool(c.VerifyOnRead),
		"bucket_ttl":             formatBucketTTL(c.BucketTTL),
		"ttl_sweep_interval_sec": strconv.FormatInt(int64(c.TTLSweepInterval.Seconds()), 10),
		"high_watermark":         strconv.FormatFloat(c.HighWatermark, 'f', 2, 64),
		"low_watermark":          strconv.FormatFloat(c.LowWatermark, 'f', 2, 64),
		"max_bucket_count":       strconv.Itoa(c.MaxBucketCount),
		"max_clock_skew_sec":     strconv.FormatInt(int64(c.MaxClockSkew.Seconds()), 10),
		// 集群字段：node_id / 成员表 / 端口都不是秘密，可用于排障；
		// 共享密钥必须脱敏，且永远不输出长度或前缀。
		"node_id":          c.NodeID,
		"cluster_peers":    c.ClusterPeers,
		"internal_addr":    c.InternalAddr,
		"cluster_secret":   logx.Redact(c.ClusterSecret),
		"replica_count":    strconv.Itoa(c.ReplicaCount),
		"write_quorum":     strconv.Itoa(c.WriteQuorum),
		"read_quorum":      strconv.Itoa(c.ReadQuorum),
		"vnodes":           strconv.Itoa(c.VNodes),
		"peer_timeout_sec": strconv.FormatInt(int64(c.PeerTimeout.Seconds()), 10),

		"repair_interval_sec":    strconv.FormatInt(int64(c.RepairInterval.Seconds()), 10),
		"scrub_interval_sec":     strconv.FormatInt(int64(c.ScrubInterval.Seconds()), 10),
		"integrity_interval_sec": strconv.FormatInt(int64(c.IntegrityInterval.Seconds()), 10),
		"repair_max_concurrency": strconv.Itoa(c.RepairMaxConcurrency),
		"hint_ttl_hours":         strconv.FormatInt(int64(c.HintTTL.Hours()), 10),
		"read_degraded":          strconv.FormatBool(c.ReadDegraded),
		"ec_enabled":             strconv.FormatBool(c.ECEnabled),
		"ec_k":                   strconv.Itoa(c.ECK),
		"ec_m":                   strconv.Itoa(c.ECM),
		"ec_min_size_bytes":      strconv.FormatInt(c.ECMinSize, 10),
		// GC 参数不是秘密，排障时需要确认宽限期是否被调过。
		"gc_interval_sec": strconv.FormatInt(int64(c.GCInterval.Seconds()), 10),
		"gc_grace_sec":    strconv.FormatInt(int64(c.GCGrace.Seconds()), 10),
		// TLS 只输出开关与文件路径：路径不是秘密，但私钥内容永远不出现。
		"tls_enabled": strconv.FormatBool(c.TLSCert != ""),
		"tls_cert":    c.TLSCert,
		"tls_key":     c.TLSKey,
	}
}

// formatBucketTTL 把 TTL 配置渲染成稳定的 "bucket=duration,..." 串。
// 按桶名排序：map 迭代顺序随机，日志与配置输出必须可复现。
func formatBucketTTL(m map[string]time.Duration) string {
	if len(m) == 0 {
		return ""
	}
	buckets := make([]string, 0, len(m))
	for bucket := range m {
		buckets = append(buckets, bucket)
	}
	sort.Strings(buckets)
	parts := make([]string, 0, len(buckets))
	for _, bucket := range buckets {
		parts = append(parts, bucket+"="+m[bucket].String())
	}
	return strings.Join(parts, ",")
}

// String 实现 fmt.Stringer，输出脱敏后的配置，防止意外打印泄露凭据。
func (c Config) String() string {
	m := c.Redacted()
	keys := []string{
		"addr", "data_dir", "region", "access_key", "secret_key",
		"log_level", "fsync_policy", "cluster_mode", "node_id", "verify_read", "tls_enabled",
	}
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+m[k])
	}
	return "config{" + strings.Join(parts, " ") + "}"
}

// ErrNoCredentials 在缺少凭据时返回，便于调用方区分错误类型。
var ErrNoCredentials = errors.New("缺少 S3 凭据")
