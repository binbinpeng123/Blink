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
		EnvVerifyRead, "BLINK_MAX_CLOCK_SKEW",
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
		{"集群模式未实现", []string{"--cluster-mode", "cluster"}, "cluster-mode"},
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
