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
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/binbinpeng123/Blink/internal/logx"
)

// 默认值集中在此处，便于文档与测试引用。
const (
	DefaultAddr            = "127.0.0.1:9000"
	DefaultDataDir         = "./blink-data"
	DefaultRegion          = "us-east-1"
	DefaultLogLevel        = "info"
	DefaultFsyncPolicy     = "always"
	DefaultClusterMode     = "single"
	DefaultMaxClockSkew    = 15 * time.Minute
	DefaultBlockSize       = 4 << 20 // 4 MiB
	DefaultInlineThreshold = 64 << 10
	DefaultHighWatermark   = 0.90
	DefaultLowWatermark    = 0.85
	DefaultMaxBucketCount  = 1000

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
	// ClusterMode 当前仅支持 single；为分布式预留。
	ClusterMode string
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
	fs.StringVar(&cfg.ClusterMode, "cluster-mode", cfg.ClusterMode, "集群模式：当前仅支持 single（环境变量 "+EnvClusterMode+"）")
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
	setIfPresent(EnvTLSCert, &cfg.TLSCert)
	setIfPresent(EnvTLSKey, &cfg.TLSKey)

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
			cfg.envErr = fmt.Errorf("%s 取值 %q 不是合法的布尔值", EnvVerifyRead, v)
		} else {
			cfg.VerifyOnRead = b
		}
	}
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

	// 当前仅支持单节点；未知模式宁可拒绝启动，也不要静默降级成单机行为。
	if c.ClusterMode != DefaultClusterMode {
		problems = append(problems, "cluster-mode 当前仅支持 "+DefaultClusterMode+"，分布式模式尚未实现")
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
		"log_level", "fsync_policy", "cluster_mode", "verify_read", "tls_enabled",
	}
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+m[k])
	}
	return "config{" + strings.Join(parts, " ") + "}"
}

// ErrNoCredentials 在缺少凭据时返回，便于调用方区分错误类型。
var ErrNoCredentials = errors.New("缺少 S3 凭据")
