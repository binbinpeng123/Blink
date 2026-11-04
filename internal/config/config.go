// Package config 定义 Blink 的启动配置。
//
// 配置来源优先级：命令行参数 > 环境变量 > 内置默认值。
//
// 凭据（Access Key / Secret Key）**没有内置默认值**：未提供时拒绝启动，
// 避免发布带有硬编码凭据的二进制。这是 06 文档"默认配置不得泄露凭据"的
// 前置要求。
package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/binbinpeng123/Blink/internal/cluster"
	"github.com/binbinpeng123/Blink/internal/logx"
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
	DefaultHighWatermark   = 0.90
	DefaultLowWatermark    = 0.85
	DefaultMaxBucketCount  = 1000

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
	EnvAddr        = "BLINK_ADDR"
	EnvDataDir     = "BLINK_DATA_DIR"
	EnvRegion      = "BLINK_REGION"
	EnvAccessKey   = "BLINK_ACCESS_KEY"
	EnvSecretKey   = "BLINK_SECRET_KEY"
	EnvLogLevel    = "BLINK_LOG_LEVEL"
	EnvFsyncPolicy = "BLINK_FSYNC_POLICY"
	EnvClusterMode = "BLINK_CLUSTER_MODE"
	EnvVerifyRead  = "BLINK_VERIFY_READ"
	EnvTLSCert     = "BLINK_TLS_CERT"
	EnvTLSKey      = "BLINK_TLS_KEY"

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
	// VerifyOnRead 开启读取时逐块 CRC32C 校验。
	//
	// 默认关闭以保留 sendfile 零拷贝读路径；打开后每块先校验再写出，
	// 代价是数据要经过用户态。见 03 文档"校验读 vs 零拷贝读"。
	VerifyOnRead bool
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

// Default 返回全部使用默认值的配置（凭据为空）。
func Default() Config {
	return Config{
		Addr:            DefaultAddr,
		DataDir:         DefaultDataDir,
		Region:          DefaultRegion,
		LogLevel:        DefaultLogLevel,
		MaxClockSkew:    DefaultMaxClockSkew,
		FsyncPolicy:     DefaultFsyncPolicy,
		BlockSize:       DefaultBlockSize,
		InlineThreshold: DefaultInlineThreshold,
		HighWatermark:   DefaultHighWatermark,
		LowWatermark:    DefaultLowWatermark,
		MaxBucketCount:  DefaultMaxBucketCount,
		ClusterMode:     DefaultClusterMode,
		InternalAddr:    DefaultInternalAddr,
		ReplicaCount:    DefaultReplicaCount,
		WriteQuorum:     DefaultWriteQuorum,
		ReadQuorum:      DefaultReadQuorum,
		VNodes:          DefaultVNodes,
		PeerTimeout:     DefaultPeerTimeout,

		RepairInterval:       DefaultRepairInterval,
		ScrubInterval:        DefaultScrubInterval,
		RepairMaxConcurrency: DefaultRepairMaxConcurrency,
		HintTTL:              DefaultHintTTL,
		ReadDegraded:         DefaultReadDegraded,
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
	fs.IntVar(&cfg.RepairMaxConcurrency, "repair-max-concurrency", cfg.RepairMaxConcurrency, "每节点修复并发上限（环境变量 "+EnvRepairMaxConcurrency+"）")
	fs.DurationVar(&cfg.HintTTL, "hint-ttl", cfg.HintTTL, "修复任务最长保留期（环境变量 "+EnvHintTTL+"）")
	fs.DurationVar(&cfg.MaxClockSkew, "max-clock-skew", cfg.MaxClockSkew, "签名校验允许的时钟偏移")
	fs.Int64Var(&cfg.BlockSize, "block-size", cfg.BlockSize, "大对象块大小（字节）")
	fs.Int64Var(&cfg.InlineThreshold, "inline-threshold", cfg.InlineThreshold, "小对象内联阈值（字节）")
	fs.Float64Var(&cfg.HighWatermark, "disk-high-watermark", cfg.HighWatermark, "拒绝写入的磁盘水位")
	fs.Float64Var(&cfg.LowWatermark, "disk-low-watermark", cfg.LowWatermark, "恢复写入的磁盘水位")
	fs.IntVar(&cfg.MaxBucketCount, "max-buckets", cfg.MaxBucketCount, "桶数量软上限")
	fs.BoolVar(&cfg.VerifyOnRead, "verify-read", cfg.VerifyOnRead, "读取时逐块校验 CRC32C（环境变量 "+EnvVerifyRead+"；关闭时走 sendfile 零拷贝）")
	fs.StringVar(&cfg.TLSCert, "tls-cert", cfg.TLSCert, "TLS 证书链 PEM 路径（环境变量 "+EnvTLSCert+"；与 --tls-key 成对提供）")
	fs.StringVar(&cfg.TLSKey, "tls-key", cfg.TLSKey, "TLS 私钥 PEM 路径（环境变量 "+EnvTLSKey+"；与 --tls-cert 成对提供）")

	if err := fs.Parse(args); err != nil {
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
	cfg.applyIntEnv(EnvRepairMaxConcurrency, &cfg.RepairMaxConcurrency, &cfg.clusterEnvErr)
	cfg.applyDurationEnv(EnvHintTTL, &cfg.HintTTL, &cfg.clusterEnvErr)

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

	// 凭据没有默认值：缺失即拒绝启动。
	if strings.TrimSpace(c.AccessKey) == "" {
		problems = append(problems, "缺少 Access Key，请设置 --access-key 或 "+EnvAccessKey)
	}
	if strings.TrimSpace(c.SecretKey) == "" {
		problems = append(problems, "缺少 Secret Key，请设置 --secret-key 或 "+EnvSecretKey)
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
	if c.MaxClockSkew <= 0 {
		problems = append(problems, "max-clock-skew 必须为正数")
	}
	if c.MaxBucketCount <= 0 {
		problems = append(problems, "max-buckets 必须为正数")
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
		"addr":               c.Addr,
		"data_dir":           c.DataDir,
		"region":             c.Region,
		"access_key":         logx.RedactKeyID(c.AccessKey),
		"secret_key":         logx.Redact(c.SecretKey),
		"log_level":          c.LogLevel,
		"fsync_policy":       c.FsyncPolicy,
		"cluster_mode":       c.ClusterMode,
		"block_size":         strconv.FormatInt(c.BlockSize, 10),
		"inline_threshold":   strconv.FormatInt(c.InlineThreshold, 10),
		"verify_read":        strconv.FormatBool(c.VerifyOnRead),
		"high_watermark":     strconv.FormatFloat(c.HighWatermark, 'f', 2, 64),
		"low_watermark":      strconv.FormatFloat(c.LowWatermark, 'f', 2, 64),
		"max_bucket_count":   strconv.Itoa(c.MaxBucketCount),
		"max_clock_skew_sec": strconv.FormatInt(int64(c.MaxClockSkew.Seconds()), 10),
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
		"repair_max_concurrency": strconv.Itoa(c.RepairMaxConcurrency),
		"hint_ttl_hours":         strconv.FormatInt(int64(c.HintTTL.Hours()), 10),
		"read_degraded":          strconv.FormatBool(c.ReadDegraded),
		// TLS 只输出开关与文件路径：路径不是秘密，但私钥内容永远不出现。
		"tls_enabled": strconv.FormatBool(c.TLSCert != ""),
		"tls_cert":    c.TLSCert,
		"tls_key":     c.TLSKey,
	}
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
