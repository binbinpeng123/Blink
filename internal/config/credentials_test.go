package config

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/binbinpeng123/Blink/internal/authz"
)

const (
	fileAKOne = "AKIAFILECREDENTIAL01"
	fileSKOne = "file-secret-key-0123456789"
	fileAKTwo = "AKIAFILECREDENTIAL02"
)

// writeCredentialsFile 写入凭据文件并返回路径。
//
// 默认 0600：除权限专项测试外，其余用例都应先满足安全检查，
// 否则它们会因为权限错误而"碰巧通过"，掩盖真正要验证的规则。
func writeCredentialsFile(t *testing.T, content string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "credentials.json")
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatalf("写入凭据文件失败: %v", err)
	}
	// os.WriteFile 受 umask 影响，显式 Chmod 保证权限断言稳定。
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("设置凭据文件权限失败: %v", err)
	}
	return path
}

func validCredentialJSON() string {
	return `{"credentials":[{"name":"ci","access_key":"` + fileAKOne + `",
		"secret_key":"` + fileSKOne + `",
		"rules":[{"bucket":"models","prefix":"llama/","actions":["read","write"]}]}]}`
}

// TestCredentialsFileLoads 验证只提供凭据文件即可启动，且规则按预期生效。
func TestCredentialsFileLoads(t *testing.T) {
	clearEnv(t)
	path := writeCredentialsFile(t, validCredentialJSON(), 0o600)

	cfg, err := Load([]string{"--credentials-file", path}, io.Discard)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}
	if cfg.CredentialCount() != 1 {
		t.Fatalf("CredentialCount = %d，期望 1", cfg.CredentialCount())
	}
	fc := cfg.FileCredentials[0]
	if fc.Name != "ci" || fc.AccessKeyID != fileAKOne || fc.SecretKey != fileSKOne {
		t.Errorf("凭据字段解析错误: %+v", FileCredential{Name: fc.Name, AccessKeyID: fc.AccessKeyID})
	}
	if !fc.Policy.Allow(authz.ActionRead, "models", "llama/7b.bin") {
		t.Error("前缀内的 read 应被允许")
	}
	if fc.Policy.Allow(authz.ActionRead, "models", "other/7b.bin") {
		t.Error("前缀外的 read 应被拒绝")
	}
	if fc.Policy.Allow(authz.ActionDelete, "models", "llama/7b.bin") {
		t.Error("未声明的 delete 应被拒绝")
	}
	if fc.Policy.VisibleBucket("other") {
		t.Error("未在规则中出现的桶不应可见")
	}
}

// TestCredentialsFileFromEnv 验证环境变量入口与 flag 等价。
func TestCredentialsFileFromEnv(t *testing.T) {
	clearEnv(t)
	path := writeCredentialsFile(t, validCredentialJSON(), 0o600)
	t.Setenv(EnvCredentialsFile, path)

	cfg, err := Load(nil, io.Discard)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}
	if cfg.CredentialsFile != path || cfg.CredentialCount() != 1 {
		t.Errorf("环境变量未生效: path=%q count=%d", cfg.CredentialsFile, cfg.CredentialCount())
	}
}

// TestCredentialsFileCombinesWithEnvCredential 验证两类凭据共存。
func TestCredentialsFileCombinesWithEnvCredential(t *testing.T) {
	clearEnv(t)
	path := writeCredentialsFile(t, validCredentialJSON(), 0o600)

	cfg, err := Load([]string{
		"--access-key", testAK, "--secret-key", testSK,
		"--credentials-file", path,
	}, io.Discard)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}
	if cfg.CredentialCount() != 2 {
		t.Errorf("CredentialCount = %d，期望 2", cfg.CredentialCount())
	}
	if len(cfg.FileCredentials) != 1 {
		t.Errorf("文件凭据数 = %d，期望 1", len(cfg.FileCredentials))
	}
}

// TestCredentialsFileRejectsLoosePermissions 验证权限过宽时拒绝启动。
//
// 凭据文件是明文的；0644 意味着同机任何用户都能读到 Secret Key。
// 这里拒绝启动而不是打印警告：警告在长期运行的部署里等于没有。
func TestCredentialsFileRejectsLoosePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows 没有 POSIX 权限位")
	}
	clearEnv(t)
	path := writeCredentialsFile(t, validCredentialJSON(), 0o644)

	_, err := Load([]string{"--credentials-file", path}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "权限") {
		t.Fatalf("0644 凭据文件应被拒绝，实际 err=%v", err)
	}
}

// TestCredentialsFileRejectsBadJSON 覆盖结构层面的失败模式。
func TestCredentialsFileRejectsBadJSON(t *testing.T) {
	cases := []struct {
		name    string
		content string
	}{
		{"未知字段", `{"credentials":[{"access_key":"` + fileAKOne + `","secret_key":"` + fileSKOne + `","rules":[{"bucket":"b","actions":["read"]}],"extra":1}]}`},
		{"空 credentials", `{"credentials":[]}`},
		{"缺少 access_key", `{"credentials":[{"secret_key":"` + fileSKOne + `","rules":[{"bucket":"b","actions":["read"]}]}]}`},
		{"缺少 secret_key", `{"credentials":[{"access_key":"` + fileAKOne + `","rules":[{"bucket":"b","actions":["read"]}]}]}`},
		{"secret 过短", `{"credentials":[{"access_key":"` + fileAKOne + `","secret_key":"short","rules":[{"bucket":"b","actions":["read"]}]}]}`},
		{"缺少 rules", `{"credentials":[{"access_key":"` + fileAKOne + `","secret_key":"` + fileSKOne + `"}]}`},
		{"list 规则带前缀", `{"credentials":[{"access_key":"` + fileAKOne + `","secret_key":"` + fileSKOne + `","rules":[{"bucket":"b","prefix":"p/","actions":["list"]}]}]}`},
		{"文件内重复 AK", `{"credentials":[
			{"access_key":"` + fileAKOne + `","secret_key":"` + fileSKOne + `","rules":[{"bucket":"b","actions":["read"]}]},
			{"access_key":"` + fileAKOne + `","secret_key":"` + fileSKOne + `","rules":[{"bucket":"b","actions":["read"]}]}]}`},
		{"多余 JSON", validCredentialJSON() + validCredentialJSON()},
		{"非对象", `[]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clearEnv(t)
			path := writeCredentialsFile(t, c.content, 0o600)
			if _, err := Load([]string{"--credentials-file", path}, io.Discard); err == nil {
				t.Fatal("非法凭据文件应拒绝启动")
			}
		})
	}
}

// TestCredentialsFileRejectsDuplicateWithEnv 验证文件凭据不能覆盖环境变量凭据。
//
// 若允许重复，后加载的一方会静默覆盖策略表里同名的另一组凭据，
// 实际权限取决于加载顺序 —— 这是最难在事故中复原的一类配置错误。
func TestCredentialsFileRejectsDuplicateWithEnv(t *testing.T) {
	clearEnv(t)
	path := writeCredentialsFile(t, validCredentialJSON(), 0o600)

	_, err := Load([]string{
		"--access-key", fileAKOne, "--secret-key", testSK,
		"--credentials-file", path,
	}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "重复") {
		t.Fatalf("与环境变量凭据重复应被拒绝，实际 err=%v", err)
	}
}

// TestCredentialsFileRejectsOversize 验证 1 MiB 上限。
func TestCredentialsFileRejectsOversize(t *testing.T) {
	clearEnv(t)
	// name 是一段 1 MiB+ 的合法 JSON 字符串；解码器只读到上限即 EOF。
	big := validCredentialJSON()
	big = strings.Replace(big, `"name":"ci"`, `"name":"`+strings.Repeat("a", maxCredentialsFile+1)+`"`, 1)
	path := writeCredentialsFile(t, big, 0o600)

	if _, err := Load([]string{"--credentials-file", path}, io.Discard); err == nil {
		t.Fatal("超过 1 MiB 的凭据文件应被拒绝")
	}
}

// TestCredentialsFileUnpairedEnvCredential 验证凭据必须成对出现。
func TestCredentialsFileUnpairedEnvCredential(t *testing.T) {
	clearEnv(t)
	path := writeCredentialsFile(t, validCredentialJSON(), 0o600)

	_, err := Load([]string{"--access-key", testAK, "--credentials-file", path}, io.Discard)
	if err == nil {
		t.Fatal("只给 Access Key 没有 Secret Key 时应拒绝启动")
	}
}
