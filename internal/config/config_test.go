package config

import (
	"io"
	"strings"
	"testing"
	"time"
)

// clearEnv 清空全部 BLINK_* 环境变量，避免宿主机环境干扰测试。
// t.Setenv 会在测试结束时自动恢复。
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		EnvAddr, EnvDataDir, EnvRegion, EnvAccessKey,
		EnvSecretKey, EnvLogLevel, EnvFsyncPolicy, EnvClusterMode,
		EnvNodeID, EnvClusterPeers, EnvInternalAddr, EnvClusterSecret,
		EnvReplicaCount, EnvWriteQuorum, EnvReadQuorum, EnvVNodes,
		EnvPeerTimeout, EnvVerifyRead, "BLINK_MAX_CLOCK_SKEW",
	} {
		t.Setenv(k, "")
	}
}

const (
	testAK = "AKIATESTTESTTEST1234"
	testSK = "test-secret-key-not-a-real-credential"
)

func TestLoadDefaults(t *testing.T) {
	clearEnv(t)

	cfg, err := Load([]string{"--access-key", testAK, "--secret-key", testSK}, io.Discard)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"Addr", cfg.Addr, DefaultAddr},
		{"DataDir", cfg.DataDir, DefaultDataDir},
		{"Region", cfg.Region, DefaultRegion},
		{"LogLevel", cfg.LogLevel, DefaultLogLevel},
		{"FsyncPolicy", cfg.FsyncPolicy, FsyncAlways},
		{"ClusterMode", cfg.ClusterMode, DefaultClusterMode},
		{"MaxClockSkew", cfg.MaxClockSkew, 15 * time.Minute},
		{"BlockSize", cfg.BlockSize, int64(4 << 20)},
		{"InlineThreshold", cfg.InlineThreshold, int64(64 << 10)},
		{"HighWatermark", cfg.HighWatermark, 0.90},
		{"LowWatermark", cfg.LowWatermark, 0.85},
		{"MaxBucketCount", cfg.MaxBucketCount, 1000},
		// 校验读默认关闭：保留 sendfile 零拷贝路径。
		{"VerifyOnRead", cfg.VerifyOnRead, false},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, 期望 %v", c.name, c.got, c.want)
		}
	}
}

// TestCredentialsRequired 是本切片最关键的一条：凭据没有默认值。
func TestCredentialsRequired(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"两者都缺", nil, "Access Key"},
		{"缺 Secret Key", []string{"--access-key", testAK}, "Secret Key"},
		{"缺 Access Key", []string{"--secret-key", testSK}, "Access Key"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)
			_, err := Load(c.args, io.Discard)
			if err == nil {
				t.Fatal("期望拒绝启动，实际通过校验")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("错误信息 %q 未包含 %q", err.Error(), c.want)
			}
		})
	}
}

func TestEnvThenFlagPrecedence(t *testing.T) {
	clearEnv(t)
	t.Setenv(EnvAccessKey, testAK)
	t.Setenv(EnvSecretKey, testSK)
	t.Setenv(EnvAddr, "0.0.0.0:8000")
	t.Setenv(EnvRegion, "cn-north-1")

	// 仅环境变量。
	cfg, err := Load(nil, io.Discard)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}
	if cfg.Addr != "0.0.0.0:8000" || cfg.Region != "cn-north-1" {
		t.Fatalf("环境变量未生效: addr=%s region=%s", cfg.Addr, cfg.Region)
	}

	// 命令行参数覆盖环境变量。
	cfg, err = Load([]string{"--addr", "127.0.0.1:1234"}, io.Discard)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}
	if cfg.Addr != "127.0.0.1:1234" {
		t.Errorf("命令行参数未覆盖环境变量: %s", cfg.Addr)
	}
	if cfg.Region != "cn-north-1" {
		t.Errorf("未被覆盖的字段应保留环境变量值: %s", cfg.Region)
	}
}

func TestValidateRejectsBadValues(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"水位倒挂", []string{"--disk-high-watermark", "0.8", "--disk-low-watermark", "0.9"}, "low-watermark"},
		{"水位越界", []string{"--disk-high-watermark", "1.5"}, "high-watermark"},
		{"fsync 策略非法", []string{"--fsync-policy", "sometimes"}, "fsync-policy"},
		{"集群模式非法", []string{"--cluster-mode", "gossip"}, "cluster-mode"},
		{"块大小非正", []string{"--block-size", "0"}, "block-size"},
		{"内联阈值大于块大小", []string{"--inline-threshold", "8388608", "--block-size", "1048576"}, "inline-threshold"},
		{"时钟容差非正", []string{"--max-clock-skew", "-1s"}, "max-clock-skew"},
		{"监听地址为空", []string{"--addr", ""}, "监听地址"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)
			args := append([]string{"--access-key", testAK, "--secret-key", testSK}, c.args...)
			_, err := Load(args, io.Discard)
			if err == nil {
				t.Fatal("期望校验失败，实际通过")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("错误信息 %q 未包含 %q", err.Error(), c.want)
			}
		})
	}
}

func TestValidateAcceptsFsyncMeta(t *testing.T) {
	clearEnv(t)
	cfg, err := Load([]string{
		"--access-key", testAK, "--secret-key", testSK,
		"--fsync-policy", FsyncMeta,
	}, io.Discard)
	if err != nil {
		t.Fatalf("fsync-policy=meta 应被接受: %v", err)
	}
	if cfg.FsyncPolicy != FsyncMeta {
		t.Errorf("FsyncPolicy = %s, 期望 %s", cfg.FsyncPolicy, FsyncMeta)
	}
}

// TestVerifyReadSources 覆盖三条来源：默认关闭、环境变量打开、命令行覆盖环境变量。
func TestVerifyReadSources(t *testing.T) {
	t.Run("环境变量打开", func(t *testing.T) {
		clearEnv(t)
		t.Setenv(EnvAccessKey, testAK)
		t.Setenv(EnvSecretKey, testSK)
		t.Setenv(EnvVerifyRead, "true")

		cfg, err := Load(nil, io.Discard)
		if err != nil {
			t.Fatalf("Load 失败: %v", err)
		}
		if !cfg.VerifyOnRead {
			t.Error("BLINK_VERIFY_READ=true 应打开校验读")
		}
	})

	t.Run("命令行覆盖环境变量", func(t *testing.T) {
		clearEnv(t)
		t.Setenv(EnvAccessKey, testAK)
		t.Setenv(EnvSecretKey, testSK)
		t.Setenv(EnvVerifyRead, "true")

		cfg, err := Load([]string{"--verify-read=false"}, io.Discard)
		if err != nil {
			t.Fatalf("Load 失败: %v", err)
		}
		if cfg.VerifyOnRead {
			t.Error("--verify-read=false 应覆盖 BLINK_VERIFY_READ=true")
		}
	})

	// 拼错的值不能静默降级成 false —— 那会让运维以为保护已经打开。
	t.Run("非法布尔值拒绝启动", func(t *testing.T) {
		clearEnv(t)
		t.Setenv(EnvAccessKey, testAK)
		t.Setenv(EnvSecretKey, testSK)
		t.Setenv(EnvVerifyRead, "yes")

		_, err := Load(nil, io.Discard)
		if err == nil {
			t.Fatal("BLINK_VERIFY_READ=yes 应拒绝启动")
		}
		if !strings.Contains(err.Error(), EnvVerifyRead) {
			t.Errorf("错误信息 %q 未指明 %s", err.Error(), EnvVerifyRead)
		}
	})
}

// TestNoCredentialLeakInString 确保任何把 Config 打进日志的写法都不会泄露凭据。
func TestNoCredentialLeakInString(t *testing.T) {
	clearEnv(t)
	cfg, err := Load([]string{"--access-key", testAK, "--secret-key", testSK}, io.Discard)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}

	if s := cfg.String(); strings.Contains(s, testSK) {
		t.Errorf("String() 泄露了 Secret Key: %s", s)
	}

	// 遍历 Redacted 的所有值，任何一个值都不得包含完整凭据。
	for k, v := range cfg.Redacted() {
		if strings.Contains(v, testSK) {
			t.Errorf("Redacted()[%s] 泄露了 Secret Key", k)
		}
		if strings.Contains(v, testAK) {
			t.Errorf("Redacted()[%s] 泄露了完整 Access Key", k)
		}
	}
}

const testClusterSecret = "0123456789abcdef0123456789abcdef"

// clusterArgs 返回一份合法的最小集群配置（密钥走环境变量，不提供命令行参数）。
func clusterArgs() []string {
	return []string{
		"--access-key", testAK, "--secret-key", testSK,
		"--cluster-mode", ClusterModeCluster,
		"--node-id", "n1",
		"--cluster-peers", "n1@127.0.0.1:9101,n2@127.0.0.1:9102,n3@127.0.0.1:9103",
		"--internal-addr", "127.0.0.1:9100",
	}
}

// without 移除一个形如 "--flag value" 的参数对。
func without(args []string, flag string) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		if args[i] == flag {
			i++ // 跳过值
			continue
		}
		out = append(out, args[i])
	}
	return out
}

func TestLoadClusterConfig(t *testing.T) {
	clearEnv(t)
	t.Setenv(EnvClusterSecret, testClusterSecret)

	cfg, err := Load(clusterArgs(), io.Discard)
	if err != nil {
		t.Fatalf("合法集群配置应通过: %v", err)
	}
	if cfg.ClusterMode != ClusterModeCluster || cfg.NodeID != "n1" {
		t.Fatalf("集群字段未生效: mode=%s node=%s", cfg.ClusterMode, cfg.NodeID)
	}

	cc, err := cfg.ClusterConfig()
	if err != nil {
		t.Fatalf("ClusterConfig 失败: %v", err)
	}
	if cc.NodeID != "n1" || len(cc.Peers) != 3 {
		t.Errorf("ClusterConfig = %+v", cc)
	}
	if string(cc.Secret) != testClusterSecret {
		t.Error("集群密钥未被传递")
	}
	if cc.ReplicaCount != DefaultReplicaCount || cc.WriteQuorum != DefaultWriteQuorum || cc.ReadQuorum != DefaultReadQuorum {
		t.Errorf("法定人数默认值错误: N=%d W=%d R=%d", cc.ReplicaCount, cc.WriteQuorum, cc.ReadQuorum)
	}
}

func TestClusterModeRejectsBadConfig(t *testing.T) {
	cases := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"缺 node-id", without(clusterArgs(), "--node-id"), nil, "节点 ID 不能为空"},
		{"缺成员表", without(clusterArgs(), "--cluster-peers"), nil, "成员表为空"},
		{"node-id 不在成员表", append(clusterArgs(), "--node-id", "n9"), nil, "未出现在成员表中"},
		{"成员表格式非法", append(clusterArgs(), "--cluster-peers", "n1:9101"), nil, "id@host:port"},
		{"密钥过短", clusterArgs(), map[string]string{EnvClusterSecret: "short"}, "至少 32 字节"},
		{"W+R 不大于 N", append(clusterArgs(), "--write-quorum", "1"), nil, "必须大于"},
		{"副本数超成员数", append(clusterArgs(), "--replica-count", "5"), nil, "replica-count"},
		{"vnodes 非法", append(clusterArgs(), "--vnodes", "0"), nil, "vnodes"},
		{"内部端口与 S3 端口冲突", append(clusterArgs(), "--internal-addr", "127.0.0.1:9000"), nil, "同一端口"},
		{"vnodes 环境变量非法", clusterArgs(), map[string]string{EnvVNodes: "12O"}, EnvVNodes},
		{"超时环境变量非法", clusterArgs(), map[string]string{EnvPeerTimeout: "soon"}, EnvPeerTimeout},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)
			t.Setenv(EnvClusterSecret, testClusterSecret)
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			_, err := Load(c.args, io.Discard)
			if err == nil {
				t.Fatal("期望拒绝启动，实际通过")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("错误信息 %q 未包含 %q", err.Error(), c.want)
			}
		})
	}
}

// 单机模式必须完全忽略集群字段：残留的坏变量既不能改变行为，也不能拒绝启动。
// 解析失败仍要以警告形式暴露，避免笔误在切到集群模式时才被发现。
func TestSingleModeIgnoresClusterEnv(t *testing.T) {
	clearEnv(t)
	t.Setenv(EnvNodeID, "bad id!")
	t.Setenv(EnvClusterPeers, "garbage")
	t.Setenv(EnvClusterSecret, "short")
	t.Setenv(EnvReplicaCount, "12O")
	t.Setenv(EnvPeerTimeout, "soon")

	cfg, err := Load([]string{"--access-key", testAK, "--secret-key", testSK}, io.Discard)
	if err != nil {
		t.Fatalf("单机模式不应因集群变量拒绝启动: %v", err)
	}
	if cfg.ClusterMode != ClusterModeSingle {
		t.Errorf("ClusterMode = %s, 期望 single", cfg.ClusterMode)
	}

	warnings := strings.Join(cfg.ClusterEnvWarnings(), "\n")
	if !strings.Contains(warnings, EnvReplicaCount) || !strings.Contains(warnings, EnvPeerTimeout) {
		t.Errorf("单机模式的集群变量笔误应产生警告，实际 %q", warnings)
	}
}

// cluster 模式下同一批变量必须让启动失败，证明上一条测试的"忽略"是模式相关的。
func TestClusterModeSurfacesClusterEnvErrors(t *testing.T) {
	clearEnv(t)
	t.Setenv(EnvClusterSecret, testClusterSecret)
	t.Setenv(EnvReplicaCount, "12O")

	_, err := Load(clusterArgs(), io.Discard)
	if err == nil || !strings.Contains(err.Error(), EnvReplicaCount) {
		t.Fatalf("cluster 模式应报告环境变量错误，实际 %v", err)
	}
}

func TestClusterSecretNotLeaked(t *testing.T) {
	clearEnv(t)
	t.Setenv(EnvClusterSecret, testClusterSecret)

	cfg, err := Load(clusterArgs(), io.Discard)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}
	for k, v := range cfg.Redacted() {
		if strings.Contains(v, testClusterSecret) {
			t.Errorf("Redacted()[%s] 泄露了集群密钥", k)
		}
	}
	if s := cfg.String(); strings.Contains(s, testClusterSecret) {
		t.Errorf("String() 泄露了集群密钥: %s", s)
	}
	if redacted := cfg.Redacted()["cluster_secret"]; redacted == "" {
		t.Error("Redacted() 应包含 cluster_secret 占位项")
	}
}

func TestUsageOutputOnBadFlag(t *testing.T) {
	clearEnv(t)
	var buf strings.Builder
	_, err := Load([]string{"--no-such-flag"}, &buf)
	if err == nil {
		t.Fatal("未知参数应报错")
	}
	if !strings.Contains(buf.String(), "fsync-policy") {
		t.Errorf("用法输出应列出全部参数，实际为: %s", buf.String())
	}
}
